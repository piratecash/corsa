//go:build linux

package loadstand

import (
	"context"
	"errors"
	"fmt"
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
	Diagnostic *linuxBindHelperDiagnostic `json:",omitempty"`
}

// Bounded testimony only. No field participates in Ready/receipt authority.
type linuxBindHelperDiagnostic struct {
	Stage, Error                                                                                                string
	PreconditionsKnown, Planned, MembershipKnown, Membership, UnboundKnown, Unbound, OrdinalKnown, OrdinalEqual bool
	BindingKnown, BindingEqual                                                                                  bool
	ReadinessStage, ReadinessTerminal, ListenerPhase                                                            string
	StatusKnown, StatusOK, BanKnown, BanOK, ListenerKnown, ListenerOK                                           bool
	ReadinessPoll, StatusPoll, BanPoll, ListenerPoll                                                            uint64
	// Source observations can come from different polls. These historical
	// failures are testimony only, never current readiness or authority.
	StatusLastNonContextFailure, BanLastNonContextFailure, ListenerLastNonContextFailure linuxBindReadinessFailure
}

type linuxBindReadinessFailure struct {
	Known        bool
	Poll         uint64
	Phase, Error string
}

func linuxBindReadinessFailureValue(d processReadinessFailure) linuxBindReadinessFailure {
	text := plannedMemberDiagnosticError(d.known, d.err)
	if len(text) > 192 {
		text = text[:189] + "..."
	}
	return linuxBindReadinessFailure{Known: d.known, Poll: d.poll, Phase: d.phase, Error: text}
}

type linuxBindRuntimeDiagnostic struct {
	stage, detail string
	err           error
}

func linuxBindDiagnosticText(text string) string {
	if len(text) > 2048 {
		return text[:2045] + "..."
	}
	return text
}

func linuxBindHelperContext(d linuxBindHelperDiagnostic) string {
	return linuxBindDiagnosticText(fmt.Sprintf("stage=%q preconditions_known=%t planned=%t membership_known=%t membership=%t unbound_known=%t unbound=%t ordinal_known=%t ordinal_equal=%t binding_known=%t binding_equal=%t readiness_stage=%q readiness_terminal=%q observations_noncoherent=true poll=%d status_poll=%d status_known=%t status_ok=%t ban_poll=%d ban_known=%t ban_ok=%t listener_poll=%d listener_known=%t listener_ok=%t listener_phase=%q historical_status_failure_known=%t historical_status_failure_poll=%d historical_ban_failure_known=%t historical_ban_failure_poll=%d historical_listener_failure_known=%t historical_listener_failure_poll=%d historical_listener_phase=%q historical_status_error=%q historical_ban_error=%q historical_listener_error=%q error=%q", d.Stage, d.PreconditionsKnown, d.Planned, d.MembershipKnown, d.Membership, d.UnboundKnown, d.Unbound, d.OrdinalKnown, d.OrdinalEqual, d.BindingKnown, d.BindingEqual, d.ReadinessStage, d.ReadinessTerminal, d.ReadinessPoll, d.StatusPoll, d.StatusKnown, d.StatusOK, d.BanPoll, d.BanKnown, d.BanOK, d.ListenerPoll, d.ListenerKnown, d.ListenerOK, d.ListenerPhase, d.StatusLastNonContextFailure.Known, d.StatusLastNonContextFailure.Poll, d.BanLastNonContextFailure.Known, d.BanLastNonContextFailure.Poll, d.ListenerLastNonContextFailure.Known, d.ListenerLastNonContextFailure.Poll, d.ListenerLastNonContextFailure.Phase, d.StatusLastNonContextFailure.Error, d.BanLastNonContextFailure.Error, d.ListenerLastNonContextFailure.Error, d.Error))
}

const (
	linuxBindMismatchKind uint16 = 1 << iota
	linuxBindMismatchID
	linuxBindMismatchOrdinal
	linuxBindMismatchNonce
	linuxBindMismatchCode
	linuxBindMismatchProvenance
	linuxBindMismatchReceiptOrdinal
	linuxBindMismatchStartedAt
	linuxBindMismatchPublic
)

// All primitive bits come from the already decoded reply. Public verification
// retains its original short-circuit: no new crypto call after an earlier fail.
func linuxBindReplyMismatch(packet linuxPacket, reply linuxMemberBindReply, request linuxMemberBindRequest, id LinuxRequestID, provenance LinuxProcessProvenance) (uint16, bool) {
	var mismatch uint16
	if packet.kind != linuxMemberBindResult {
		mismatch |= linuxBindMismatchKind
	}
	if packet.id != id {
		mismatch |= linuxBindMismatchID
	}
	if packet.ordinal != 1 {
		mismatch |= linuxBindMismatchOrdinal
	}
	if reply.Nonce != request.Nonce {
		mismatch |= linuxBindMismatchNonce
	}
	if reply.Code != linuxResultReady {
		mismatch |= linuxBindMismatchCode
	}
	if reply.Provenance != provenance {
		mismatch |= linuxBindMismatchProvenance
	}
	if reply.Receipt.Incarnation.Ordinal != request.Ordinal {
		mismatch |= linuxBindMismatchReceiptOrdinal
	}
	if reply.Receipt.Incarnation.StartedAt.IsZero() {
		mismatch |= linuxBindMismatchStartedAt
	}
	checkedPublic := mismatch == 0
	if checkedPublic && !validLinuxRuntimePublic(reply.Receipt.Public) {
		mismatch |= linuxBindMismatchPublic
	}
	return mismatch, checkedPublic
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
	diagnostic := linuxMemberBindDiagnostic{known: true, phase: "encode"}
	raw, err := encodeLinuxPacket(linuxMemberBind, exchange.id, 1, request)
	if err == nil {
		diagnostic.phase = "write"
		err = o.work.write(raw, deadline)
	}
	var packet linuxPacket
	var reply linuxMemberBindReply
	if err == nil {
		diagnostic.phase = "read"
		packet, err = o.work.read(deadline)
	}
	if err == nil {
		diagnostic.phase = "decode"
		reply, err = decodeLinuxBody[linuxMemberBindReply](packet)
	}
	if err == nil {
		diagnostic.phase = "reply-validate"
		diagnostic.replyKnown, diagnostic.replyCode = true, reply.Code
		diagnostic.mismatch, diagnostic.publicChecked = linuxBindReplyMismatch(packet, reply, request, exchange.id, o.readyProvenance)
		if reply.Diagnostic != nil && reply.Diagnostic.Stage != "" {
			diagnostic.helperKnown, diagnostic.helper = true, *reply.Diagnostic
			diagnostic.helper.Stage = linuxBindDiagnosticText(diagnostic.helper.Stage)
			diagnostic.helper.Error = linuxBindDiagnosticText(diagnostic.helper.Error)
			diagnostic.helper.ReadinessStage = linuxBindDiagnosticText(diagnostic.helper.ReadinessStage)
			diagnostic.helper.ReadinessTerminal = linuxBindDiagnosticText(diagnostic.helper.ReadinessTerminal)
			diagnostic.helper.ListenerPhase = linuxBindDiagnosticText(diagnostic.helper.ListenerPhase)
		}
		if diagnostic.mismatch != 0 {
			err = ErrLinuxOwnerProtocol
		}
	}
	o.mu.Lock()
	if err == nil && (!o.running || o.stopping || o.targetKnown || o.helperWaitKnown || o.correlation.poisoned) {
		diagnostic.phase = "owner-state"
		err = ErrMemberState
	}
	if err == nil {
		diagnostic.phase = "ready"
	}
	diagnostic.err = err
	if !o.memberBindDiagnostic.known {
		o.memberBindDiagnostic = diagnostic
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
	diagnostic := linuxBindHelperDiagnostic{Stage: "precondition", PreconditionsKnown: true, Planned: r.init.Planned != nil}
	if r.init.Planned != nil {
		diagnostic.MembershipKnown, diagnostic.Membership = true, r.membership != nil
		if r.membership != nil {
			diagnostic.UnboundKnown, diagnostic.Unbound = true, r.memberBinding == nil
			if r.memberBinding == nil {
				diagnostic.OrdinalKnown, diagnostic.OrdinalEqual = true, request.Ordinal == r.init.Planned.Ordinal
			}
		}
	}
	var readyDiagnostic processReadinessDiagnostic
	var runtimeDiagnostic linuxBindRuntimeDiagnostic
	var failure error
	if r.init.Planned != nil && r.membership != nil && r.memberBinding == nil && request.Ordinal == r.init.Planned.Ordinal {
		diagnostic.Stage = "await-ready"
		readyErr := r.process.awaitReady(ctx, r.init.Config.Listener, r.init.Config.Listen, nil, &readyDiagnostic)
		var facts linuxRuntimeFacts
		err := readyErr
		if err == nil {
			diagnostic.Stage = "runtime"
			facts, err = r.observeOriginalRuntime(ctx, &runtimeDiagnostic)
		}
		if err == nil {
			diagnostic.Stage = "completed-start"
			receipt := StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: request.Ordinal, StartedAt: facts.StartedAt}}
			binding, err := r.process.CompletedStart(receipt)
			if err == nil {
				diagnostic.BindingKnown, diagnostic.BindingEqual = true, binding.launch == (launchBinding{nonce: r.init.Planned.FinalNonce, fingerprint: r.init.Planned.FinalFingerprint})
			}
			if err == nil && binding.launch == (launchBinding{nonce: r.init.Planned.FinalNonce, fingerprint: r.init.Planned.FinalFingerprint}) {
				r.memberBinding = &binding
				reply.Code = linuxResultReady
				reply.Receipt = receipt
				reply.Provenance = facts.Provenance
			}
			failure = err
		} else {
			failure = err
		}
	}
	if reply.Code != linuxResultReady {
		if diagnostic.Stage == "runtime" {
			diagnostic.Stage += "/" + runtimeDiagnostic.stage + "/" + runtimeDiagnostic.detail
		}
		diagnostic.ReadinessStage, diagnostic.ReadinessTerminal = readyDiagnostic.stage, readyDiagnostic.terminal
		diagnostic.StatusKnown, diagnostic.StatusOK = readyDiagnostic.statusKnown, readyDiagnostic.statusOK
		diagnostic.BanKnown, diagnostic.BanOK = readyDiagnostic.banKnown, readyDiagnostic.banOK
		diagnostic.ListenerKnown, diagnostic.ListenerOK = readyDiagnostic.listenerKnown, readyDiagnostic.listenerOK
		diagnostic.ListenerPhase = readyDiagnostic.listenerPhase
		diagnostic.ReadinessPoll, diagnostic.StatusPoll, diagnostic.BanPoll, diagnostic.ListenerPoll = readyDiagnostic.poll, readyDiagnostic.statusPoll, readyDiagnostic.banPoll, readyDiagnostic.listenerPoll
		diagnostic.StatusLastNonContextFailure = linuxBindReadinessFailureValue(readyDiagnostic.statusFailure)
		diagnostic.BanLastNonContextFailure = linuxBindReadinessFailureValue(readyDiagnostic.banFailure)
		diagnostic.ListenerLastNonContextFailure = linuxBindReadinessFailureValue(readyDiagnostic.listenerFailure)
		diagnostic.Error = linuxBindDiagnosticText(fmt.Sprintf("failure=%q runtime=%q last_status=%q last_ban=%q last_listener=%q", plannedMemberDiagnosticError(true, failure), plannedMemberDiagnosticError(runtimeDiagnostic.stage != "", runtimeDiagnostic.err), plannedMemberDiagnosticError(readyDiagnostic.statusKnown, readyDiagnostic.statusErr), plannedMemberDiagnosticError(readyDiagnostic.banKnown, readyDiagnostic.banErr), plannedMemberDiagnosticError(readyDiagnostic.listenerKnown, readyDiagnostic.listenerErr)))
		reply.Diagnostic = &diagnostic
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
