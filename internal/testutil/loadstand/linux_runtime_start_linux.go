//go:build linux

package loadstand

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"golang.org/x/sys/unix"
)

// LinuxOwnedRuntimeObservation is historical OS provenance with scoped RPC
// testimony, only for the reviewed nonspawning SDK in a fresh single-use NS.
// It is not RPC-server PID attestation, MemberStart, Wait or finality authority.
// There is no deserialization or conversion to a start/final capability.
type LinuxOwnedRuntimeObservation struct {
	known   bool
	nonce   LaunchNonce // outer helper request binding, NOT the inner FPA nonce
	ordinal LinuxLaunchOrdinal
	facts   linuxRuntimeFacts
}

func (o LinuxOwnedRuntimeObservation) Format(s fmt.State, _ rune) {
	_, _ = fmt.Fprint(s, "LinuxOwnedRuntimeObservation(historical-scoped-correlation)")
}
func (o LinuxOwnedRuntimeObservation) available() bool {
	return o.known && o.nonce != (LaunchNonce{}) && o.ordinal == 1
}
func (o LinuxOwnedRuntimeObservation) Public() (PublicIdentity, bool) {
	return o.facts.Public, o.available()
}
func (o LinuxOwnedRuntimeObservation) Provenance() (LinuxProcessProvenance, bool) {
	return o.facts.Provenance, o.available()
}
func (o LinuxOwnedRuntimeObservation) Interval() (time.Time, time.Time, bool) {
	return o.facts.BeganAt, o.facts.EndedAt, o.available()
}
func (o LinuxOwnedRuntimeObservation) StartedAt() (time.Time, bool) {
	return o.facts.StartedAt, o.available()
}

type linuxRuntimeOutcome struct {
	observation LinuxOwnedRuntimeObservation
	err         error
}

// ObserveOwnedRuntime shares Sample's single work slot. The target program is
// trusted not to delegate RPC: TCP/status have no PID-bound launch challenge.
func (o *LinuxFixture) ObserveOwnedRuntime(ctx context.Context) (LinuxOwnedRuntimeObservation, error) {
	if _, ok := ctx.Deadline(); !ok {
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return LinuxOwnedRuntimeObservation{}, err
	}
	o.mu.Lock()
	if !o.running || o.stopping || o.targetKnown || o.helperWaitKnown || o.correlation.poisoned {
		o.mu.Unlock()
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerPoisoned
	}
	if o.workBusy {
		o.mu.Unlock()
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerBusy
	}
	if err := ctx.Err(); err != nil {
		o.mu.Unlock()
		return LinuxOwnedRuntimeObservation{}, err
	}
	id, err := o.correlation.allocate()
	if err != nil {
		o.mu.Unlock()
		o.poison()
		return LinuxOwnedRuntimeObservation{}, err
	}
	o.workBusy = true
	o.mu.Unlock()
	exchange := linuxSampleExchange{observe: true, ctx: ctx, id: id, runtimeResult: make(chan linuxRuntimeOutcome)}
	select {
	case o.exchanges <- exchange:
	case <-ctx.Done():
		o.mu.Lock()
		o.workBusy = false
		o.mu.Unlock()
		return LinuxOwnedRuntimeObservation{}, ctx.Err()
	case <-o.stoppingDone:
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerPoisoned
	}
	select {
	case result := <-exchange.runtimeResult:
		return result.observation, result.err
	case <-ctx.Done():
		o.poison()
		return LinuxOwnedRuntimeObservation{}, ctx.Err()
	case <-o.stoppingDone:
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerPoisoned
	}
}

func (o *LinuxFixture) observeOnWorkOwner(exchange linuxSampleExchange) {
	result := o.readRuntimeObservation(exchange)
	if errors.Is(result.err, ErrLinuxOwnerProtocol) {
		o.poison()
	}
	select {
	case exchange.runtimeResult <- result:
	case <-exchange.ctx.Done():
		o.poison()
	case <-o.stoppingDone:
	}
}
func (o *LinuxFixture) readRuntimeObservation(exchange linuxSampleExchange) linuxRuntimeOutcome {
	reply, err := o.readRuntimeReply(exchange)
	if err != nil {
		return linuxRuntimeOutcome{err: err}
	}
	observation, err := o.publishRuntimeObservation(exchange.ctx, exchange.id, reply)
	return linuxRuntimeOutcome{observation: observation, err: err}
}

// The sole production reader uses this exact prefix before publication. A
// validated statement is not itself an opaque observation or start authority.
func (o *LinuxFixture) readRuntimeReply(exchange linuxSampleExchange) (linuxRuntimeReply, error) {
	deadline, _ := exchange.ctx.Deadline()
	raw, err := encodeLinuxPacket(linuxObserveRuntime, exchange.id, 1, linuxSampleRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(time.Until(deadline))})
	if err == nil {
		err = o.work.write(raw, deadline)
	}
	var packet linuxPacket
	if err == nil {
		packet, err = o.work.read(deadline)
	}
	if err != nil {
		return linuxRuntimeReply{}, ErrLinuxOwnerProtocol
	}
	reply, err := decodeLinuxBody[linuxRuntimeReply](packet)
	if err != nil || packet.kind != linuxRuntimeResult || packet.id != exchange.id || packet.ordinal != 1 || reply.Nonce != o.correlation.nonce {
		return linuxRuntimeReply{}, ErrLinuxOwnerProtocol
	}
	if reply.Code == linuxResultUnavailable {
		return linuxRuntimeReply{}, ErrProcessObservation
	}
	if reply.Code != linuxResultReady || !validLinuxRuntimeFacts(reply.Facts) {
		return linuxRuntimeReply{}, ErrLinuxOwnerProtocol
	}
	return reply, nil
}

// Lock order namespace.mu -> fixture.mu. No I/O or wait while either is held.
// Root's original expectations are never replaced from helper testimony.
func (o *LinuxFixture) publishRuntimeObservation(ctx context.Context, id LinuxRequestID, reply linuxRuntimeReply) (LinuxOwnedRuntimeObservation, error) {
	if o.namespace == nil {
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerPoisoned
	}
	o.namespace.mu.Lock()
	defer o.namespace.mu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return LinuxOwnedRuntimeObservation{}, err
	}
	if !o.namespace.claimed || o.namespace.closed || o.namespace.poisoned || o.namespace.fd < 0 || !o.running || o.stopping || o.targetKnown || o.helperWaitKnown || o.correlation.poisoned {
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerPoisoned
	}
	expectedNS := linuxRuntimeNamespace{Device: o.namespace.identity.device, Inode: o.namespace.identity.inode}
	if !o.workBusy || id != o.correlation.lastID || reply.Nonce != o.correlation.nonce || reply.Facts.Provenance != o.readyProvenance || reply.Facts.Namespace != expectedNS {
		return LinuxOwnedRuntimeObservation{}, ErrLinuxOwnerProtocol
	}
	result := LinuxOwnedRuntimeObservation{known: true, nonce: o.correlation.nonce, ordinal: o.correlation.ordinal, facts: reply.Facts}
	o.runtimeObservation = result // one copied historical row, never cached-return authority
	return result, nil
}

func validLinuxRuntimeFacts(f linuxRuntimeFacts) bool {
	p := f.Provenance
	digest, err := hex.DecodeString(string(p.RunningExecutableSHA256))
	return err == nil && len(digest) == 32 && hex.EncodeToString(digest) == string(p.RunningExecutableSHA256) && p.Incarnation.PID > 0 && p.Incarnation.StartTicks > 0 && p.UID > 0 && p.RunningExecutableVerified && p.KernelStartTimeAvailable && f.Namespace.Device != 0 && f.Namespace.Inode != 0 && !f.BeganAt.IsZero() && !f.EndedAt.Before(f.BeganAt) && !f.StartedAt.IsZero() && validLinuxRuntimePublic(f.Public)
}
func canonicalLinuxBase64(value string, size int, encoding *base64.Encoding) bool {
	raw, err := encoding.DecodeString(value)
	return err == nil && len(raw) == size && encoding.EncodeToString(raw) == value
}
func validLinuxRuntimePublic(p PublicIdentity) bool {
	return !p.Identity.IsZero() && canonicalLinuxBase64(p.PublicKey, ed25519.PublicKeySize, base64.StdEncoding) && canonicalLinuxBase64(p.BoxPublicKey, 32, base64.StdEncoding) && canonicalLinuxBase64(p.BoxSignature, ed25519.SignatureSize, base64.RawURLEncoding) && identity.VerifyPublicKeyFingerprint(p.Identity.String(), p.PublicKey) == nil && identity.VerifyBoxKeyBinding(p.Identity.String(), p.PublicKey, p.BoxPublicKey, p.BoxSignature) == nil
}
func linuxRuntimePublic(before, after domain.NodeStatus, contacts []protocol.ContactFrame, diagnostics ...*linuxBindRuntimeDiagnostic) (PublicIdentity, error) {
	var diagnostic *linuxBindRuntimeDiagnostic
	if len(diagnostics) != 0 {
		diagnostic = diagnostics[0]
	}
	id, err := domain.ParsePeerIdentity(before.Identity)
	if err != nil || id.IsZero() || before.Address != before.Identity || before.StartedAt.IsZero() || before.Identity != after.Identity || before.Address != after.Address || before.PublicKey != after.PublicKey || before.BoxPublicKey != after.BoxPublicKey || !before.StartedAt.Equal(after.StartedAt) {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = "public-status-pair", ErrProcessObservation
		}
		return PublicIdentity{}, ErrProcessObservation
	}
	var found PublicIdentity
	count := 0
	for _, row := range contacts {
		if row.Address != before.Address {
			continue
		}
		count++
		if row.PubKey != before.PublicKey || row.BoxKey != before.BoxPublicKey {
			if diagnostic != nil {
				diagnostic.detail, diagnostic.err = "public-contact-keys", ErrProcessObservation
			}
			return PublicIdentity{}, ErrProcessObservation
		}
		found = PublicIdentity{Identity: id, PublicKey: row.PubKey, BoxPublicKey: row.BoxKey, BoxSignature: row.BoxSig}
	}
	if count != 1 || !validLinuxRuntimePublic(found) {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = "public-self-count-or-proof", ErrProcessObservation
		}
		return PublicIdentity{}, ErrProcessObservation
	}
	return found, nil
}

// Full fixtures deliberately omit their box binding from the contact plane.
// Only the fixed, trusted Full role may use its fixture-local signed witness.
// It must agree with both existing status snapshots and one signing-only self
// contact. This is scoped RPC testimony, not PID/boot/launch attestation.
func linuxRuntimeFullPublic(role MemberRole, before, after domain.NodeStatus, contacts []protocol.ContactFrame, witness PublicIdentity, diagnostics ...*linuxBindRuntimeDiagnostic) (PublicIdentity, error) {
	var diagnostic *linuxBindRuntimeDiagnostic
	if len(diagnostics) != 0 {
		diagnostic = diagnostics[0]
	}
	refuse := func(detail string) (PublicIdentity, error) {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = detail, ErrProcessObservation
		}
		return PublicIdentity{}, ErrProcessObservation
	}
	if role != MemberFull {
		return refuse("full-witness-role")
	}
	id, err := domain.ParsePeerIdentity(before.Identity)
	if err != nil || id.IsZero() || before.Address != before.Identity || before.StartedAt.IsZero() || before.Identity != after.Identity || before.Address != after.Address || before.PublicKey != after.PublicKey || before.BoxPublicKey != after.BoxPublicKey || !before.StartedAt.Equal(after.StartedAt) {
		return refuse("full-witness-status-pair")
	}
	if witness.Identity != id || witness.PublicKey != before.PublicKey || witness.BoxPublicKey != before.BoxPublicKey || !validLinuxRuntimePublic(witness) {
		return refuse("full-witness-public-proof")
	}
	count := 0
	for _, row := range contacts {
		if row.Address != before.Address {
			continue
		}
		count++
		if row.PubKey != before.PublicKey || row.BoxKey != "" || row.BoxSig != "" {
			return refuse("full-witness-self-contact")
		}
	}
	if count != 1 {
		return refuse("full-witness-self-count")
	}
	return witness, nil
}

// readTargetRuntimeNS uses only the original pinned sampler directory. It
// intentionally follows proc's ns/net magic link, then validates the NS FD.
func (r *linuxHelperRuntime) readTargetRuntimeNS(ctx context.Context, diagnostics ...*linuxBindRuntimeDiagnostic) (value linuxRuntimeNamespace, resultErr error) {
	var diagnostic *linuxBindRuntimeDiagnostic
	if len(diagnostics) != 0 {
		diagnostic = diagnostics[0]
	}
	s := r.sampler
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != processSamplerOpen {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = "ns-sampler-state", ErrProcessObservation
		}
		return value, ErrProcessObservation
	}
	before, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || before.ID != s.expected.Incarnation {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = "ns-process-before", err
		}
		return value, ErrProcessObservation
	}
	fd, err := unix.Openat(s.directory, "ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		if diagnostic != nil {
			diagnostic.detail, diagnostic.err = "ns-open", err
		}
		return value, ErrProcessObservation
	}
	defer func() {
		if err := unix.Close(fd); err != nil {
			if diagnostic != nil {
				diagnostic.detail, diagnostic.err = "ns-close", err
			}
			value = linuxRuntimeNamespace{}
			resultErr = ErrProcessObservation
		}
	}()
	ns, err := namespaceIdentity(fd)
	after, _, afterErr := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || afterErr != nil || before.ID != after.ID || ns.device != r.init.NSDevice || ns.inode != r.init.NSInode {
		if diagnostic != nil {
			switch {
			case err != nil:
				diagnostic.detail, diagnostic.err = "ns-stat", err
			case afterErr != nil:
				diagnostic.detail, diagnostic.err = "ns-process-after", afterErr
			case before.ID != after.ID:
				diagnostic.detail, diagnostic.err = "ns-process-changed", ErrProcessObservation
			default:
				diagnostic.detail, diagnostic.err = "ns-identity", ErrProcessObservation
			}
		}
		return value, ErrProcessObservation
	}
	return linuxRuntimeNamespace{Device: ns.device, Inode: ns.inode}, nil
}
func (r *linuxHelperRuntime) observeOriginalRuntime(ctx context.Context, diagnostics ...*linuxBindRuntimeDiagnostic) (linuxRuntimeFacts, error) {
	var diagnostic *linuxBindRuntimeDiagnostic
	if len(diagnostics) != 0 {
		diagnostic = diagnostics[0]
	}
	var empty linuxRuntimeFacts
	select {
	case <-r.process.Done():
		if diagnostic != nil {
			diagnostic.stage, diagnostic.err = "process-before", ErrProcessObservation
		}
		return empty, ErrProcessObservation
	default:
	}
	before, err := r.sampler.Sample(ctx)
	if err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.err = "sampler-before", err
		}
		return empty, ErrProcessObservation
	}
	if diagnostic != nil {
		diagnostic.stage, diagnostic.detail = "namespace-before", ""
	}
	ns, err := r.readTargetRuntimeNS(ctx, diagnostic)
	if err != nil {
		return empty, ErrProcessObservation
	}
	source := NewProcessSource(r.process.RPC())
	first, err := source.Status(ctx)
	if err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "status1", "", err
		}
		return empty, ErrProcessObservation
	}
	var contacts protocol.Frame
	if err = r.process.RPC().Call(ctx, "fetchContacts", nil, &contacts); err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "contacts", "", err
		}
		return empty, ErrProcessObservation
	}
	// Role comes from the already validated, fixed planned init, never the
	// witness response or a failed contact lookup. SDK uses its original path.
	fullWitness := r.init.Planned != nil && r.init.Planned.Plan.Role == MemberFull
	var witness PublicIdentity
	if fullWitness {
		if err = r.process.RPC().Call(ctx, "loadstandFullPublicIdentity", nil, &witness); err != nil {
			if diagnostic != nil {
				diagnostic.stage, diagnostic.detail, diagnostic.err = "full-witness", "rpc", err
			}
			return empty, ErrProcessObservation
		}
	}
	last, err := source.Status(ctx)
	if err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "status2", "", err
		}
		return empty, ErrProcessObservation
	}
	if diagnostic != nil {
		diagnostic.stage, diagnostic.detail = "public", ""
	}
	var public PublicIdentity
	if fullWitness {
		public, err = linuxRuntimeFullPublic(r.init.Planned.Plan.Role, first, last, contacts.Contacts, witness, diagnostic)
	} else {
		public, err = linuxRuntimePublic(first, last, contacts.Contacts, diagnostic)
	}
	if err != nil {
		return empty, err
	}
	if diagnostic != nil {
		diagnostic.stage, diagnostic.detail = "namespace-after", ""
	}
	afterNS, err := r.readTargetRuntimeNS(ctx, diagnostic)
	if err != nil || ns != afterNS {
		if diagnostic != nil && err == nil {
			diagnostic.detail, diagnostic.err = "ns-changed", ErrProcessObservation
		}
		return empty, ErrProcessObservation
	}
	after, err := r.sampler.Sample(ctx)
	if err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "sampler-after", "", err
		}
		return empty, ErrProcessObservation
	}
	if before.Provenance != after.Provenance {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "provenance-changed", "", ErrProcessObservation
		}
		return empty, ErrProcessObservation
	}
	if err := ctx.Err(); err != nil {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "context-after", "", err
		}
		return empty, ErrProcessObservation
	}
	select {
	case <-r.process.Done():
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "process-after", "", ErrProcessObservation
		}
		return empty, ErrProcessObservation
	default:
	}
	facts := linuxRuntimeFacts{Provenance: after.Provenance, Namespace: ns, Public: public, StartedAt: first.StartedAt.UTC(), BeganAt: before.ReadStartedAt.UTC(), EndedAt: after.ReadCompletedAt.UTC()}
	if !validLinuxRuntimeFacts(facts) {
		if diagnostic != nil {
			diagnostic.stage, diagnostic.detail, diagnostic.err = "facts", "", ErrProcessObservation
		}
		return empty, ErrProcessObservation
	}
	return facts, nil
}
func (r *linuxHelperRuntime) writeRuntimeObservation(ctx context.Context, packet linuxPacket) error {
	facts, err := r.observeOriginalRuntime(ctx)
	code := linuxResultReady
	if err != nil {
		code = linuxResultUnavailable
		facts = linuxRuntimeFacts{}
	}
	raw, err := encodeLinuxPacket(linuxRuntimeResult, packet.id, 1, linuxRuntimeReply{Nonce: r.init.Nonce, Code: code, Facts: facts})
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	return r.work.write(raw, minLinuxPacketDeadline(deadline))
}
