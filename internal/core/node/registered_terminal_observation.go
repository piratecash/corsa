package node

import (
	"errors"
	"io"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Inactive, restricted registry-lifecycle source. No installer, clock, worker,
// raw-attempt population, physical-first-cause claim or public observation API.
type k10TerminalKind uint8

const (
	k10Missing k10TerminalKind = iota
	k10OwnerStopped
	k10OwnerError
	k10OwnerEOF
	k10CommandStopped
	k10LoopbackCondemned
	k10LocalEviction
	k10LegacyRefusal
	k10KindCount
)

type k10CloseRequest uint8

const (
	k10GenericClose k10CloseRequest = 1 << iota
	k10HandlerCleanup
	k10ShutdownRequest
)

// Compile-time closed protocol schema, in original ErrorCode priority order.
// Code0 means no exact code; EOF has its own owner class, not a wire code.
var k10ProtocolCodes = [...]string{
	"", protocol.ErrCodeInvalidJSON, protocol.ErrCodeEncodeFailed,
	protocol.ErrCodeUnknownCommand, protocol.ErrCodeInvalidSendMessage,
	protocol.ErrCodeInvalidImportMessage, protocol.ErrCodeInvalidFetchMessages,
	protocol.ErrCodeInvalidFetchMessageIDs, protocol.ErrCodeInvalidFetchMessage,
	protocol.ErrCodeInvalidFetchInbox, protocol.ErrCodeInvalidSendDeliveryReceipt,
	protocol.ErrCodeInvalidFetchReceipts, protocol.ErrCodeInvalidPublishNotice,
	protocol.ErrCodeUnknownSenderKey, protocol.ErrCodeUnknownMessageID,
	protocol.ErrCodeInvalidDirectMessageSig, protocol.ErrCodeMessageTimestampOutOfRange,
	protocol.ErrCodeIncompatibleProtocol, protocol.ErrCodeRead, protocol.ErrCodeAuthRequired,
	protocol.ErrCodeInvalidAuthSignature, protocol.ErrCodeBlacklisted,
	protocol.ErrCodeInvalidAckDelete, protocol.ErrCodeFrameTooLarge,
	protocol.ErrCodeDuplicateConnection, protocol.ErrCodeRateLimited,
	protocol.ErrCodeHelloAfterAuth, protocol.ErrCodeSenderIdentityNotVerified,
	protocol.ErrCodePeerBanned, protocol.ErrCodeSelfIdentity,
	protocol.ErrCodeStoreDeferred, protocol.ErrCodeStoreFailed,
}

type k10Outcome struct {
	kind k10TerminalKind
	code uint8
}

// Only owners call this outside application AND observer locks. ErrorCode and
// errors.Is may invoke custom error methods; neither belongs in a leaf section.
func k10ReportedOutcome(err error) k10Outcome {
	if err == nil {
		return k10Outcome{kind: k10OwnerStopped}
	}
	code := protocol.ErrorCode(err)
	for i := 1; i < len(k10ProtocolCodes); i++ {
		if code == k10ProtocolCodes[i] {
			return k10Outcome{kind: k10OwnerError, code: uint8(i)}
		}
	}
	if errors.Is(err, io.EOF) {
		return k10Outcome{kind: k10OwnerEOF}
	}
	return k10Outcome{kind: k10OwnerError}
}

type k10TerminalSource struct {
	mu                                                        sync.Mutex // application locks -> this leaf; never the reverse
	instance                                                  [16]byte
	frozen                                                    string
	registered, active, removed, classified, pending, missing uint64
	registrationSerial, terminalSerial, version               uint64
	coarse                                                    uint64
	buckets                                                   [2][k10KindCount][len(k10ProtocolCodes)]uint64
}
type k10TerminalCapsule struct {
	source                    *k10TerminalSource
	entry                     *connEntry
	id                        domain.ConnID
	serial                    uint64
	generation                uint64 // optional captured CM slot generation, never address lookup
	direction                 uint8
	removed, final, published bool
	terminalSerial            uint64
	winner                    peerSessionCloser
	requests                  k10CloseRequest
	outcome                   k10Outcome
	reported                  bool
	reportedOutcome           k10Outcome
}
type k10TerminalSnapshot struct {
	population, status, codeStatus, reason                    string
	instance                                                  [16]byte
	registered, active, removed, classified, pending, missing uint64
	registrationSerial, terminalSerial, version, coarse       uint64
	buckets                                                   [2][k10KindCount][len(k10ProtocolCodes)]uint64
}

const k10Max = ^uint64(0)

func newK10TerminalSource(instance [16]byte) *k10TerminalSource {
	o := &k10TerminalSource{instance: instance}
	if instance == ([16]byte{}) {
		o.frozen = "invalid-source-instance"
	}
	return o
}
func (o *k10TerminalSource) failLocked(reason string) {
	if o.frozen == "" {
		o.frozen = reason
	}
}
func (o *k10TerminalSource) fail(reason string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failLocked(reason)
}
func (s *Service) k10IssuedLocked(previous, current domain.ConnID) {
	if s.terminalObservation != nil && (current == 0 || current <= previous) {
		s.terminalObservation.fail("original-connid-wrap")
	}
}
func (c *k10TerminalCapsule) bindGeneration(generation uint64) {
	if c == nil {
		return
	}
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if generation == 0 || (c.generation != 0 && c.generation != generation) {
		o.failLocked("foreign-slot-generation")
		return
	}
	c.generation = generation
}
func (c *k10TerminalCapsule) request(request k10CloseRequest) {
	if c == nil {
		return
	}
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if request == 0 || request & ^(k10GenericClose|k10HandlerCleanup|k10ShutdownRequest) != 0 {
		o.failLocked("invalid-close-request")
		return
	}
	c.requests |= request // metadata only, never finalizes or overrides cause
}
func (c *k10TerminalCapsule) remove(entry *connEntry) {
	if c == nil {
		return
	}
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if c.entry != entry || c.serial == 0 || c.id == 0 {
		o.failLocked("foreign-terminal-owner")
		return
	}
	if c.removed {
		return
	}
	if o.active == 0 || o.removed == k10Max || o.pending == k10Max || o.terminalSerial == k10Max || o.version == k10Max {
		o.failLocked("removal-overflow-or-underflow")
		return
	}
	c.removed = true
	o.active--
	o.removed++
	o.pending++
	o.terminalSerial++
	o.version++
	c.terminalSerial = o.terminalSerial
	c.publishLocked()
}
func (c *k10TerminalCapsule) publishLocked() {
	o := c.source
	if o.frozen != "" || !c.removed || !c.final || c.published {
		return
	}
	if o.pending == 0 || o.version == k10Max {
		o.failLocked("publication-overflow-or-underflow")
		return
	}
	v := c.outcome
	if v.kind == k10Missing {
		if o.missing == k10Max {
			o.failLocked("missing-overflow")
			return
		}
		o.pending--
		o.missing++
		o.version++
		c.published = true
		o.failLocked("missing-original-outcome")
		return
	}
	if v.kind >= k10KindCount || int(v.code) >= len(k10ProtocolCodes) || c.direction > 1 {
		o.failLocked("invalid-terminal-class")
		return
	}
	bucket := &o.buckets[c.direction][v.kind][v.code]
	coarse := v.code == 0 && v.kind != k10OwnerEOF && v.kind != k10LocalEviction && v.kind != k10LegacyRefusal
	if o.classified == k10Max || *bucket == k10Max || (coarse && o.coarse == k10Max) {
		o.failLocked("histogram-overflow")
		return
	}
	o.pending--
	o.classified++
	*bucket++
	if coarse {
		o.coarse++
	}
	o.version++
	c.published = true
}
func (c *k10TerminalCapsule) finish(outcome k10Outcome, onlyMissing bool) {
	if c == nil {
		return
	}
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if c.final {
		if !onlyMissing && c.outcome != outcome {
			o.failLocked("contradictory-terminal-outcome")
		}
		return
	}
	if outcome.kind >= k10KindCount || int(outcome.code) >= len(k10ProtocolCodes) {
		o.failLocked("invalid-terminal-class")
		return
	}
	c.final = true
	c.outcome = outcome
	c.publishLocked()
}
func (ps *peerSession) k10CloseWinner(closer peerSessionCloser) {
	if ps == nil || ps.terminal == nil {
		return
	}
	c := ps.terminal
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if c.winner != 0 {
		o.failLocked("duplicate-close-winner")
		return
	}
	c.winner = closer
	switch closer {
	case peerSessionClosedByOwner: // its original setup/serve owner still owes outcome
	case peerSessionClosedByLocalEviction:
		c.final = true
		c.outcome = k10Outcome{kind: k10LocalEviction}
		c.publishLocked()
	case peerSessionClosedByLegacyRefusal:
		c.final = true
		c.outcome = k10Outcome{kind: k10LegacyRefusal}
		c.publishLocked()
	default:
		o.failLocked("unknown-close-winner")
	}
}
func (ps *peerSession) k10FinishReported(err error) {
	if ps == nil || ps.terminal == nil {
		return
	}
	outcome := k10ReportedOutcome(err) // outside all application/observer locks
	c := ps.terminal
	o := c.source
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen != "" {
		return
	}
	if c.reported {
		if c.reportedOutcome != outcome {
			o.failLocked("contradictory-owner-report")
		}
		return
	}
	c.reported = true
	c.reportedOutcome = outcome
	if c.winner == peerSessionClosedByLocalEviction || c.winner == peerSessionClosedByLegacyRefusal {
		return
	}
	if c.final && c.outcome != outcome {
		o.failLocked("contradictory-terminal-outcome")
		return
	}
	c.final = true
	c.outcome = outcome
	c.publishLocked()
}
func (ps *peerSession) k10MissingIfUnfinished() {
	if ps != nil {
		ps.terminal.finish(k10Outcome{kind: k10Missing}, true)
	}
}
func (o *k10TerminalSource) snapshot() k10TerminalSnapshot {
	if o == nil {
		return k10TerminalSnapshot{population: "RegisteredTransportInstance", status: "INACTIVE", codeStatus: "UNKNOWN"}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	status, codeStatus := "COMPLETE", "COMPLETE_RESTRICTED_OWNER_CLASSIFICATION"
	if o.frozen != "" {
		status = "UNKNOWN"
	} else if o.pending != 0 {
		status = "IN_FLIGHT"
	}
	if status != "COMPLETE" || o.coarse != 0 {
		codeStatus = "UNKNOWN"
	}
	return k10TerminalSnapshot{population: "RegisteredTransportInstance", status: status, codeStatus: codeStatus, reason: o.frozen, instance: o.instance, registered: o.registered, active: o.active, removed: o.removed, classified: o.classified, pending: o.pending, missing: o.missing, registrationSerial: o.registrationSerial, terminalSerial: o.terminalSerial, version: o.version, coarse: o.coarse, buckets: o.buckets}
}
