package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// The legacy probe tells an old listener (v28–v30, no session v2) from a new
// one when a TLS attempt to a node of unknown version has failed. A failed TLS
// attempt or a timeout proves nothing by itself — a man in the middle can cut
// TLS to push a dialler down to v1 — so the only evidence that legacy is the
// right protocol is a CORRECT old-protocol answer to an identity-less hello.
//
// The probe reveals nothing about the dialler: it carries no identity and no
// address, only what an old listener needs to answer it (its identity-less
// hello branch replies with a challenge-less welcome). The answer is an
// UNPROVEN hint: it names an identity, but nothing on this connection proves
// the listener holds that key, so it must never be recorded as a proven
// identity. Whether a probe may be sent at all (never for an identity or
// endpoint that requires v2) is the caller's decision, not this file's.

// legacyProbeOutcome is what a probe answer permits.
type legacyProbeOutcome int

const (
	// legacyProbeOutcomeNotLegacy: the answer is not a correct old-protocol
	// welcome, so it permits nothing — no legacy session, no hint.
	legacyProbeOutcomeNotLegacy legacyProbeOutcome = iota + 1
	// legacyProbeOutcomeLegacyAllowed: a correct welcome of a version this
	// node serves and that predates v2 — the listener may be spoken to in v1.
	legacyProbeOutcomeLegacyAllowed
	// legacyProbeOutcomeV2Hint: a welcome of a version that speaks v2, or a
	// TLS alert as the first byte of the answer. The listener is new, so the
	// failed TLS attempt was not "old node" — legacy stays closed, and the
	// hint is as unproven as the rest of the answer.
	legacyProbeOutcomeV2Hint
)

func (o legacyProbeOutcome) String() string {
	switch o {
	case legacyProbeOutcomeNotLegacy:
		return "not_legacy"
	case legacyProbeOutcomeLegacyAllowed:
		return "legacy_allowed"
	case legacyProbeOutcomeV2Hint:
		return "v2_hint"
	default:
		return "unknown"
	}
}

// Reasons a probe permits nothing. Each is its own sentinel so a caller (and
// a metric) can tell a timeout from a forged answer without reading text, and
// none carries bytes of the answer: those are chosen by the remote side, and
// an error travels into logs. That is why parser and key errors are not
// wrapped — their text quotes the input (json repeats the offending number or
// character) — and why the checks report no values from the answer.
var (
	errLegacyProbeOptionsInvalid   = errors.New("legacy probe: options invalid")
	errLegacyProbeEncode           = errors.New("legacy probe: encode probe")
	errLegacyProbeWrite            = errors.New("legacy probe: write probe")
	errLegacyProbeDeadline         = errors.New("legacy probe: arm the socket deadline")
	errLegacyProbeAborted          = errors.New("legacy probe: aborted by context")
	errLegacyProbeTimeout          = errors.New("legacy probe: no answer before the deadline")
	errLegacyProbeNoAnswer         = errors.New("legacy probe: no complete answer line")
	errLegacyProbeLineTooLong      = errors.New("legacy probe: answer line exceeds the unauthenticated frame limit")
	errLegacyProbeMalformed        = errors.New("legacy probe: answer is not a frame")
	errLegacyProbeErrorFrame       = errors.New("legacy probe: answer is an error frame")
	errLegacyProbeUnexpectedFrame  = errors.New("legacy probe: answer is not a welcome")
	errLegacyProbeVersionMissing   = errors.New("legacy probe: welcome carries no version")
	errLegacyProbeVersionTooOld    = errors.New("legacy probe: welcome version below the floor")
	errLegacyProbeNetworkMismatch  = errors.New("legacy probe: welcome names another network")
	errLegacyProbeChallengePresent = errors.New("legacy probe: welcome carries a challenge")
	errLegacyProbeIdentityMissing  = errors.New("legacy probe: welcome lacks identity fields")
	errLegacyProbeAddressMismatch  = errors.New("legacy probe: welcome address is not derived from its pubkey")
	errLegacyProbeBoxSigInvalid    = errors.New("legacy probe: welcome box key binding does not verify")
)

// legacyProbeSystemClock reads the operating system's wall clock. The probe
// turns it into an ABSOLUTE socket deadline, and net.Conn compares that
// deadline with the system clock, so any other source — the trusted clock the
// endpoint bindings run on, a clock with an offset — would move the deadline
// by its own disagreement with the system. Tests substitute a skewed reading
// of the same clock, never a different one.
type legacyProbeSystemClock func() time.Time

// legacyProbeOptions bounds one probe. Both fields are required; there is no
// zero-value default, defaultLegacyProbeOptions is the default.
type legacyProbeOptions struct {
	// timeout covers the write of the probe and the read of its answer.
	timeout time.Duration
	// systemNow is the clock the absolute deadline is computed from.
	systemNow legacyProbeSystemClock
}

// legacyProbeLineLimit bounds the answer line. The answer is unauthenticated,
// so it gets the limit a listener gives a peer before authentication
// (maxCommandLineBytes, equal to protocol.MaxFrameLine), not the 8 MiB of an
// authenticated session reply: the largest welcome a release can send — every
// capability, eight dtypes of the longest name — is a few KiB.
const legacyProbeLineLimit = protocol.MaxFrameLine

// defaultLegacyProbeOptions bounds the probe like any v1 handshake step.
// handshakeTimeout is a placeholder, not a decision: the final value is chosen
// from measured first-connection latency against real old releases and over
// Tor, where 2 s is likely too short for a cold circuit.
func defaultLegacyProbeOptions() legacyProbeOptions {
	return legacyProbeOptions{timeout: handshakeTimeout, systemNow: time.Now}
}

func (o legacyProbeOptions) validate() error {
	switch {
	case o.timeout <= 0:
		return fmt.Errorf("%w: timeout %v", errLegacyProbeOptionsInvalid, o.timeout)
	case o.systemNow == nil:
		return fmt.Errorf("%w: no clock", errLegacyProbeOptionsInvalid)
	default:
		return nil
	}
}

// legacyProbeAnswer is the result of one probe.
type legacyProbeAnswer struct {
	outcome legacyProbeOutcome
	// reply is the parsed answer — an unproven claim, see the file comment —
	// and nil when nothing parsable arrived.
	reply *protocol.Frame
	// reader holds whatever the listener sent after the answer line, so a
	// caller that continues on the same connection loses no bytes.
	reader *bufio.Reader
}

// legacyProbeLine is the probe: a hello with exactly type, version, client
// and network. Without identity fields an old listener skips authentication
// and answers with a welcome that has no challenge.
func legacyProbeLine() ([]byte, error) {
	line, err := protocol.MarshalFrameLineBytes(protocol.Frame{
		Type:    "hello",
		Version: config.ProtocolVersion,
		Client:  "node",
		Network: networkName,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errLegacyProbeEncode, err)
	}
	return line, nil
}

// probeLegacy writes the probe on conn and classifies the one line that
// answers it. conn stays open and owned by the caller on every outcome; the
// deadline the probe set is cleared before it returns.
func probeLegacy(ctx context.Context, conn net.Conn, opts legacyProbeOptions) (legacyProbeAnswer, error) {
	reader := bufio.NewReader(conn)
	refused := legacyProbeAnswer{outcome: legacyProbeOutcomeNotLegacy, reader: reader}
	if err := opts.validate(); err != nil {
		return refused, err
	}
	probe, err := legacyProbeLine()
	if err != nil {
		return refused, err
	}
	release, err := armLegacyProbeDeadline(ctx, conn, opts)
	if err != nil {
		return refused, err
	}
	defer release()

	if _, err := conn.Write(probe); err != nil {
		return refused, legacyProbeIOError(ctx, err, errLegacyProbeWrite)
	}
	// Only the first byte tells a TLS listener from a v1 one: an alert is
	// seven bytes without a newline followed by a close, which a line reader
	// would report as a cut answer. A 0x15 later in a line is just JSON.
	first, err := reader.Peek(1)
	if err != nil {
		return refused, legacyProbeIOError(ctx, err, errLegacyProbeNoAnswer)
	}
	if first[0] == sessionv2.TLSRecordAlert {
		return legacyProbeAnswer{outcome: legacyProbeOutcomeV2Hint, reader: reader}, nil
	}
	line, err := readFrameLine(reader, legacyProbeLineLimit)
	if err != nil {
		return refused, legacyProbeIOError(ctx, err, errLegacyProbeNoAnswer)
	}
	outcome, reply, err := classifyLegacyProbeLine(line)
	return legacyProbeAnswer{outcome: outcome, reply: reply, reader: reader}, err
}

// legacyProbeCancelled is a deadline far enough in the past that every
// pending read or write on the connection fails at once.
var legacyProbeCancelled = time.Unix(1, 0)

// armLegacyProbeDeadline bounds the probe by opts and by ctx. ctx is honoured
// through the socket deadline because a blocked net.Conn read observes
// nothing else; the connection is not closed, since it is the caller's. The
// returned release waits out a cancellation already in flight, so it can never
// re-arm the deadline after release cleared it.
func armLegacyProbeDeadline(ctx context.Context, conn net.Conn, opts legacyProbeOptions) (func(), error) {
	if err := conn.SetDeadline(opts.systemNow().Add(opts.timeout)); err != nil {
		return nil, fmt.Errorf("%w: %w", errLegacyProbeDeadline, err)
	}
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelled)
		// Best effort: a conn that cannot take a deadline is already broken,
		// and the probe's own timeout still bounds the wait.
		_ = conn.SetDeadline(legacyProbeCancelled)
	})
	release := func() {
		if !stop() {
			<-cancelled
		}
		// Best effort: the caller sets its own deadline before its next step,
		// and a failure here means the connection is unusable anyway.
		_ = conn.SetDeadline(time.Time{})
	}
	return release, nil
}

// legacyProbeIOError names why a socket step of the probe failed. The context
// is asked first: its cancellation surfaces as a deadline error on the socket,
// and reporting it as a timeout would blame the listener for our own abort.
func legacyProbeIOError(ctx context.Context, err, fallback error) error {
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", errLegacyProbeAborted, context.Cause(ctx))
	case errors.Is(err, errFrameTooLarge):
		return fmt.Errorf("%w: %w", errLegacyProbeLineTooLong, err)
	case errors.Is(err, os.ErrDeadlineExceeded):
		return fmt.Errorf("%w: %w", errLegacyProbeTimeout, err)
	default:
		return fmt.Errorf("%w: %w", fallback, err)
	}
}

// classifyLegacyProbeLine is the validator of a probe answer line. reply is
// nil exactly when the line does not parse as a frame.
func classifyLegacyProbeLine(line string) (legacyProbeOutcome, *protocol.Frame, error) {
	reply, err := protocol.ParseFrameLine(strings.TrimSpace(line))
	if err != nil {
		return legacyProbeOutcomeNotLegacy, nil, legacyProbeMalformedError(err)
	}
	outcome, err := classifyLegacyProbeReply(reply)
	return outcome, &reply, err
}

// legacyProbeMalformedError reports an unparsable answer without the parser's
// text, which quotes the input; a syntax error keeps its offset, a position
// rather than bytes, so a garbled answer can still be located.
func legacyProbeMalformedError(err error) error {
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return fmt.Errorf("%w at offset %d", errLegacyProbeMalformed, syntax.Offset)
	}
	return errLegacyProbeMalformed
}

// legacyProbeReplyCheck is one condition a probe answer must meet.
type legacyProbeReplyCheck func(protocol.Frame) error

// legacyProbeWelcomeChecks hold for every answer that is evidence of
// anything, v2 hint included: a welcome of this network that names a version
// and answers an identity-less hello the only way a listener may — without a
// challenge. A challenge here is a listener that took the probe for an
// authenticating hello, which no release does.
var legacyProbeWelcomeChecks = []legacyProbeReplyCheck{
	requireLegacyProbeNotErrorFrame,
	requireLegacyProbeWelcome,
	requireLegacyProbeVersionPresent,
	requireLegacyProbeNetwork,
	requireLegacyProbeNoChallenge,
}

// legacyProbeLegacyChecks additionally hold for an answer that permits v1:
// a version this node serves, and a self-certifying identity — the address is
// the fingerprint of the pubkey and the pubkey signs the box key. That makes
// the welcome internally consistent, not proven: anyone can replay a real
// node's welcome, which is why the answer stays a hint.
var legacyProbeLegacyChecks = append([]legacyProbeReplyCheck{requireLegacyProbeVersionAtFloor}, legacyProbeIdentityChecks...)

// legacyProbeIdentityChecks hold for a frame whose identity fields bind: the
// address is the fingerprint of the pubkey, and the pubkey signs the box key.
var legacyProbeIdentityChecks = []legacyProbeReplyCheck{
	requireLegacyProbeIdentityFields,
	requireLegacyProbeAddressFromPubKey,
	requireLegacyProbeBoxKeyBinding,
}

// legacyWelcomeCorrectChecks hold for a correct v1 welcome — the probe's
// answer or a full one, which may carry a challenge: what the probe's
// validator requires of any answer, then an identity that binds. A full
// welcome on a v1-only node is read without a probe, so it is held to the
// same before its version may count as a claim.
var legacyWelcomeCorrectChecks = append([]legacyProbeReplyCheck{
	requireLegacyProbeNotErrorFrame,
	requireLegacyProbeWelcome,
	requireLegacyProbeVersionPresent,
	requireLegacyProbeNetwork,
}, legacyProbeIdentityChecks...)

// legacyWelcomeCorrect reports whether a v1 welcome is correct: well formed,
// of this network and self-consistent in its identity — though not proven.
func legacyWelcomeCorrect(frame protocol.Frame) bool {
	return firstFailedLegacyProbeCheck(frame, legacyWelcomeCorrectChecks) == nil
}

func classifyLegacyProbeReply(reply protocol.Frame) (legacyProbeOutcome, error) {
	if err := firstFailedLegacyProbeCheck(reply, legacyProbeWelcomeChecks); err != nil {
		return legacyProbeOutcomeNotLegacy, err
	}
	if domain.ProtocolVersion(reply.Version) >= domain.ProtocolVersion(config.ProtocolVersionSecureSession) {
		return legacyProbeOutcomeV2Hint, nil
	}
	if err := firstFailedLegacyProbeCheck(reply, legacyProbeLegacyChecks); err != nil {
		return legacyProbeOutcomeNotLegacy, err
	}
	return legacyProbeOutcomeLegacyAllowed, nil
}

func firstFailedLegacyProbeCheck(reply protocol.Frame, checks []legacyProbeReplyCheck) error {
	for _, check := range checks {
		if err := check(reply); err != nil {
			return err
		}
	}
	return nil
}

func requireLegacyProbeNotErrorFrame(reply protocol.Frame) error {
	if reply.Type == "error" {
		return errLegacyProbeErrorFrame
	}
	return nil
}

func requireLegacyProbeWelcome(reply protocol.Frame) error {
	if reply.Type != "welcome" {
		return errLegacyProbeUnexpectedFrame
	}
	return nil
}

func requireLegacyProbeVersionPresent(reply protocol.Frame) error {
	if reply.Version <= 0 {
		return errLegacyProbeVersionMissing
	}
	return nil
}

func requireLegacyProbeNetwork(reply protocol.Frame) error {
	if domain.NetworkID(reply.Network) != domain.NetworkID(networkName) {
		return errLegacyProbeNetworkMismatch
	}
	return nil
}

func requireLegacyProbeNoChallenge(reply protocol.Frame) error {
	if reply.Challenge != "" {
		return errLegacyProbeChallengePresent
	}
	return nil
}

func requireLegacyProbeVersionAtFloor(reply protocol.Frame) error {
	if domain.ProtocolVersion(reply.Version) < domain.ProtocolVersion(config.MinimumProtocolVersion) {
		return errLegacyProbeVersionTooOld
	}
	return nil
}

func requireLegacyProbeIdentityFields(reply protocol.Frame) error {
	if !connauth.HasIdentityFields(reply) {
		return errLegacyProbeIdentityMissing
	}
	return nil
}

// requireLegacyProbeAddressFromPubKey repeats what VerifyBoxKeyBinding checks
// first, on purpose: it gives "the address is not this key's" a sentinel of
// its own, apart from "the key does not sign this box key".
func requireLegacyProbeAddressFromPubKey(reply protocol.Frame) error {
	publicKey, err := identity.ParsePublicKeyBase64(reply.PubKey)
	if err != nil {
		return errLegacyProbeAddressMismatch
	}
	if publicKey.Fingerprint() != reply.Address {
		return errLegacyProbeAddressMismatch
	}
	return nil
}

func requireLegacyProbeBoxKeyBinding(reply protocol.Frame) error {
	if err := identity.VerifyBoxKeyBinding(reply.Address, reply.PubKey, reply.BoxKey, reply.BoxSig); err != nil {
		return errLegacyProbeBoxSigInvalid
	}
	return nil
}
