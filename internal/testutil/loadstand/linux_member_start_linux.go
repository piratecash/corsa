//go:build linux

package loadstand

import (
	"context"
	"errors"
	"time"
)

// New typed Member operation on the original private workchannel; no historical
// LinuxOwnedRuntimeObservation value is accepted or converted here.
type linuxMemberBindRequest struct {
	Nonce       LaunchNonce
	BudgetNanos int64
	Ordinal     IncarnationOrdinal
}
type linuxMemberBindReply struct {
	Nonce      LaunchNonce
	Code       linuxResultCode
	Receipt    StartReceipt
	Provenance LinuxProcessProvenance
}

type linuxMemberStartOutcome struct {
	receipt StartReceipt
	err     error
}

func (o *LinuxFixture) completePlannedMemberStart(ctx context.Context) (StartReceipt, error) {
	if _, ok := ctx.Deadline(); !ok {
		return StartReceipt{}, ErrLinuxOwnerBound
	}
	o.mu.Lock()
	if o.spec.planned == nil || !o.running || o.stopping || o.targetKnown || o.helperWaitKnown || o.correlation.poisoned || o.workBusy {
		o.mu.Unlock()
		return StartReceipt{}, ErrMemberState
	}
	id, err := o.correlation.allocate()
	if err != nil {
		o.mu.Unlock()
		return StartReceipt{}, err
	}
	o.workBusy = true
	o.mu.Unlock()
	exchange := linuxSampleExchange{bindMember: true, ctx: ctx, id: id, memberResult: make(chan linuxMemberStartOutcome)}
	select {
	case o.exchanges <- exchange:
	case <-ctx.Done():
		o.mu.Lock()
		o.workBusy = false
		o.mu.Unlock()
		return StartReceipt{}, ctx.Err()
	case <-o.stoppingDone:
		return StartReceipt{}, ErrLinuxOwnerPoisoned
	}
	select {
	case result := <-exchange.memberResult:
		return result.receipt, result.err
	case <-ctx.Done():
		o.poison()
		return StartReceipt{}, ctx.Err()
	case <-o.stoppingDone:
		return StartReceipt{}, ErrLinuxOwnerPoisoned
	}
}
func (o *LinuxFixture) bindMemberOnWorkOwner(exchange linuxSampleExchange) {
	deadline, _ := exchange.ctx.Deadline()
	request := linuxMemberBindRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(time.Until(deadline)), Ordinal: o.spec.planned.ordinal}
	raw, err := encodeLinuxPacket(linuxMemberBind, exchange.id, 1, request)
	if err == nil {
		err = o.work.write(raw, deadline)
	}
	var packet linuxPacket
	var reply linuxMemberBindReply
	if err == nil {
		packet, err = o.work.read(deadline)
	}
	if err == nil {
		reply, err = decodeLinuxBody[linuxMemberBindReply](packet)
	}
	if err == nil && (packet.kind != linuxMemberBindResult || packet.id != exchange.id || packet.ordinal != 1 || reply.Nonce != request.Nonce || reply.Code != linuxResultReady || reply.Provenance != o.readyProvenance || reply.Receipt.Incarnation.Ordinal != request.Ordinal || reply.Receipt.Incarnation.StartedAt.IsZero() || !validLinuxRuntimePublic(reply.Receipt.Public)) {
		err = ErrLinuxOwnerProtocol
	}
	o.mu.Lock()
	if err == nil && (!o.running || o.stopping || o.targetKnown || o.helperWaitKnown || o.correlation.poisoned) {
		err = ErrMemberState
	}
	o.mu.Unlock()
	if err != nil {
		o.poison()
	}
	result := linuxMemberStartOutcome{err: err}
	if err == nil {
		result.receipt = reply.Receipt
	}
	o.mu.Lock()
	o.workBusy = false
	o.mu.Unlock()
	select {
	case exchange.memberResult <- result:
	case <-exchange.ctx.Done():
		o.poison()
	case <-o.stoppingDone:
	}
}
func (r *linuxHelperRuntime) writeMemberBind(ctx context.Context, packet linuxPacket, request linuxMemberBindRequest) error {
	reply := linuxMemberBindReply{Nonce: r.init.Nonce, Code: linuxResultUnavailable}
	if r.init.Planned != nil && r.membership != nil && r.memberBinding == nil && request.Ordinal == r.init.Planned.Ordinal {
		// Reuse the existing bounded readiness poll; the subsequent joint OS/RPC
		// read is fresh and is the sole receipt input, not this readiness hint.
		readyErr := r.process.awaitReady(ctx, r.init.Config.Listener, r.init.Config.Listen, nil)
		var facts linuxRuntimeFacts
		err := readyErr
		if err == nil {
			facts, err = r.observeOriginalRuntime(ctx)
		}
		if err == nil {
			receipt := StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: request.Ordinal, StartedAt: facts.StartedAt}}
			binding, err := r.process.CompletedStart(receipt)
			if err == nil && binding.launch == (launchBinding{nonce: r.init.Planned.FinalNonce, fingerprint: r.init.Planned.FinalFingerprint}) {
				r.memberBinding = &binding
				reply.Code = linuxResultReady
				reply.Receipt = receipt
				reply.Provenance = facts.Provenance
			}
		}
	}
	raw, err := encodeLinuxPacket(linuxMemberBindResult, packet.id, 1, reply)
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	return r.work.write(raw, minLinuxPacketDeadline(deadline))
}
func (r *linuxHelperRuntime) reduceMemberFinal(ctx context.Context) error {
	if r.init.Planned == nil || r.memberBinding == nil {
		return nil
	}
	// Called only after originaltarget Done and workowner join. No concurrent
	// bind can rewrite this original receipt; finalreport is never Start input.
	binding := *r.memberBinding
	prior := NewTransportHighWater(binding.RuntimeBinding())
	deadline := r.startup.snapshot().deadline
	if deadline.IsZero() {
		r.memberFinal = &linuxMemberFinal{Receipt: binding.Receipt(), Finality: FinalUnavailable, Reason: FinalExitUnclean}
		return nil
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	final, err := r.process.FinalAccounting(ctx, binding, prior)
	peerCount, peerErr := r.memberPeerState(ctx)
	r.memberFinal = &linuxMemberFinal{Receipt: binding.Receipt(), Finality: final.Finality, Traffic: final.Traffic, Reason: final.Reason, PeersKnown: peerErr == nil, PeerCount: peerCount, MembershipSHA256: r.membership.descriptor.SHA256}
	return errors.Join(err, peerErr)
}
func (o *LinuxFixture) plannedMemberFinal(receipt StartReceipt, prior TransportHighWater) (FinalAccountingResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.helperJoined || !o.helperWaitKnown || !o.targetKnown || o.memberFinal == nil {
		return FinalAccountingResult{}, ErrFinalAccountingUnavailable
	}
	final := *o.memberFinal
	switch final.Finality {
	case FinalExact:
		if final.Reason != "" {
			return FinalAccountingResult{}, ErrFinalReportInvalid
		}
	case FinalUnavailable:
		switch final.Reason {
		case FinalExitUnclean, FinalReportRejected, FinalCapabilityCloseFailed, FinalArtifactsReleased:
		default:
			return FinalAccountingResult{}, ErrFinalReportInvalid
		}
	default:
		return FinalAccountingResult{}, ErrFinalReportInvalid
	}
	if final.Receipt.Public != receipt.Public || final.Receipt.Incarnation.Ordinal != receipt.Incarnation.Ordinal || !final.Receipt.Incarnation.StartedAt.Equal(receipt.Incarnation.StartedAt) || prior.invalid || prior.binding.Identity != receipt.Public.Identity || !prior.binding.StartedAt.Equal(receipt.Incarnation.StartedAt) {
		return FinalAccountingResult{}, ErrFinalReportInvalid
	}
	if final.Finality == FinalExact {
		if !final.Traffic.StartedAt.Equal(receipt.Incarnation.StartedAt) || final.Traffic.ReadAt.Before(final.Traffic.StartedAt) || prior.seen && (final.Traffic.BytesSent < prior.sent || final.Traffic.BytesReceived < prior.received) {
			return FinalAccountingResult{}, ErrFinalReportInvalid
		}
		if o.correlation.poisoned || !o.targetExit.Clean() || !o.helperWait.Clean() {
			return FinalAccountingResult{}, errors.Join(ErrFinalAccountingUnavailable, ErrLinuxOwnerPoisoned)
		}
	}
	return FinalAccountingResult{Finality: final.Finality, Traffic: final.Traffic, Reason: final.Reason}, nil
}

// Called with the original namespace mutex held by the existing constructor.
// Only its exact fully released predecessor can authorize one later planned
// incarnation. The generic one-use claimed bit is never reset.
func consumePlannedNamespaceReuseLocked(spec LinuxFixtureSpec, n *OwnedNetNS) bool {
	p := spec.planned
	if p == nil || p.previous == nil || n.plannedOwner != p.previous {
		return false
	}
	previous := p.previous
	previous.mu.Lock()
	defer previous.mu.Unlock()
	old := previous.spec.planned
	if !previous.plannedReleased || previous.plannedConsumed || previous.namespace != n || old == nil || old.network != p.network || old.membership != p.membership || old.member != p.member || old.ordinal+1 != p.ordinal || previous.spec.Config.DataDir != spec.Config.DataDir || previous.spec.UID != spec.UID || previous.spec.GID != spec.GID || previous.spec.Credentials != spec.Credentials || previous.spec.TargetSHA256 != spec.TargetSHA256 {
		return false
	}
	previous.plannedConsumed = true
	return true
}
func (o *LinuxFixture) confirmPlannedNamespaceIdle() error {
	n := o.namespace
	p := o.spec.planned
	n.mu.Lock()
	defer n.mu.Unlock()
	if p == nil || n.plannedOwner != o || n.busy || n.closed || n.poisoned || n.fd < 0 {
		return ErrStopUnconfirmed
	}
	backend, ok := p.network.backend.(*linuxPlannedNetworkBackend)
	if !ok {
		return ErrStopUnconfirmed
	}
	row := backend.resource(plannedNamespace, string(p.member.id))
	actual, err := namespaceIdentity(n.fd)
	if err != nil || row == nil || row.namespace != n || actual != n.identity || actual != row.namespaceIdentity {
		return errors.Join(ErrStopUnconfirmed, err)
	}
	if n.helperReaped != nil {
		select {
		case <-n.helperReaped:
		default:
			return ErrStopUnconfirmed
		}
	}
	return nil
}
