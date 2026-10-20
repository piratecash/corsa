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
func linuxRuntimePublic(before, after domain.NodeStatus, contacts []protocol.ContactFrame) (PublicIdentity, error) {
	id, err := domain.ParsePeerIdentity(before.Identity)
	if err != nil || id.IsZero() || before.Address != before.Identity || before.StartedAt.IsZero() || before.Identity != after.Identity || before.Address != after.Address || before.PublicKey != after.PublicKey || before.BoxPublicKey != after.BoxPublicKey || !before.StartedAt.Equal(after.StartedAt) {
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
			return PublicIdentity{}, ErrProcessObservation
		}
		found = PublicIdentity{Identity: id, PublicKey: row.PubKey, BoxPublicKey: row.BoxKey, BoxSignature: row.BoxSig}
	}
	if count != 1 || !validLinuxRuntimePublic(found) {
		return PublicIdentity{}, ErrProcessObservation
	}
	return found, nil
}

// readTargetRuntimeNS uses only the original pinned sampler directory. It
// intentionally follows proc's ns/net magic link, then validates the NS FD.
func (r *linuxHelperRuntime) readTargetRuntimeNS(ctx context.Context) (value linuxRuntimeNamespace, resultErr error) {
	s := r.sampler
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != processSamplerOpen {
		return value, ErrProcessObservation
	}
	before, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || before.ID != s.expected.Incarnation {
		return value, ErrProcessObservation
	}
	fd, err := unix.Openat(s.directory, "ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return value, ErrProcessObservation
	}
	defer func() {
		if unix.Close(fd) != nil {
			value = linuxRuntimeNamespace{}
			resultErr = ErrProcessObservation
		}
	}()
	ns, err := namespaceIdentity(fd)
	after, _, afterErr := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || afterErr != nil || before.ID != after.ID || ns.device != r.init.NSDevice || ns.inode != r.init.NSInode {
		return value, ErrProcessObservation
	}
	return linuxRuntimeNamespace{Device: ns.device, Inode: ns.inode}, nil
}
func (r *linuxHelperRuntime) observeOriginalRuntime(ctx context.Context) (linuxRuntimeFacts, error) {
	var empty linuxRuntimeFacts
	select {
	case <-r.process.Done():
		return empty, ErrProcessObservation
	default:
	}
	before, err := r.sampler.Sample(ctx)
	if err != nil {
		return empty, ErrProcessObservation
	}
	ns, err := r.readTargetRuntimeNS(ctx)
	if err != nil {
		return empty, ErrProcessObservation
	}
	source := NewProcessSource(r.process.RPC())
	first, err := source.Status(ctx)
	if err != nil {
		return empty, ErrProcessObservation
	}
	var contacts protocol.Frame
	if err = r.process.RPC().Call(ctx, "fetchContacts", nil, &contacts); err != nil {
		return empty, ErrProcessObservation
	}
	last, err := source.Status(ctx)
	if err != nil {
		return empty, ErrProcessObservation
	}
	public, err := linuxRuntimePublic(first, last, contacts.Contacts)
	if err != nil {
		return empty, err
	}
	afterNS, err := r.readTargetRuntimeNS(ctx)
	if err != nil || ns != afterNS {
		return empty, ErrProcessObservation
	}
	after, err := r.sampler.Sample(ctx)
	if err != nil || before.Provenance != after.Provenance || ctx.Err() != nil {
		return empty, ErrProcessObservation
	}
	select {
	case <-r.process.Done():
		return empty, ErrProcessObservation
	default:
	}
	facts := linuxRuntimeFacts{Provenance: after.Provenance, Namespace: ns, Public: public, StartedAt: first.StartedAt.UTC(), BeganAt: before.ReadStartedAt.UTC(), EndedAt: after.ReadCompletedAt.UTC()}
	if !validLinuxRuntimeFacts(facts) {
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
