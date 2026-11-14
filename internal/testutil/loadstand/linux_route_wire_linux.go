//go:build linux

package loadstand

import (
	"context"
	"errors"
	"time"
)

type linuxRouteWireCensusOutcome struct {
	commitment RouteWireCommitment
	err        error
}

// CaptureRouteWireCensus obtains the bounded FullNode census on the existing
// work channel and retains that exact commitment for the following Stop.
func (g *LinuxMemberGroup) CaptureRouteWireCensus(ctx context.Context, id MemberID) (RouteWireCommitment, error) {
	member := g.routeWireMember(id)
	if member == nil || !member.planned.RouteWire.Bounds.valid() {
		return RouteWireCommitment{}, ErrFinalAccountingUnavailable
	}
	if _, ok := ctx.Deadline(); !ok {
		return RouteWireCommitment{}, ErrLinuxOwnerBound
	}
	if err := member.CheckRunning(ctx); err != nil {
		return RouteWireCommitment{}, err
	}
	clock, err := ReadLinuxRouteWireClockDomain()
	if err != nil || clock != g.routeWireClock {
		return RouteWireCommitment{}, errors.Join(ErrFinalAccountingUnavailable, err)
	}
	o := member.fixture
	o.mu.Lock()
	if !o.running || o.workBusy || o.stopping || o.correlation.poisoned || o.spec.planned == nil ||
		o.spec.planned.ordinal != member.current.Incarnation.Ordinal {
		o.mu.Unlock()
		return RouteWireCommitment{}, ErrLinuxOwnerBusy
	}
	idRequest, err := o.correlation.allocate()
	if err != nil {
		o.mu.Unlock()
		return RouteWireCommitment{}, err
	}
	o.workBusy = true
	o.mu.Unlock()
	x := linuxSampleExchange{ctx: ctx, id: idRequest, wireCensusClock: clock, wireCensusResult: make(chan linuxRouteWireCensusOutcome)}
	select {
	case o.exchanges <- x:
	case <-ctx.Done():
		o.mu.Lock()
		o.workBusy = false
		o.mu.Unlock()
		return RouteWireCommitment{}, ctx.Err()
	case <-o.stoppingDone:
		return RouteWireCommitment{}, ErrLinuxOwnerPoisoned
	}
	select {
	case result := <-x.wireCensusResult:
		if result.err != nil {
			return RouteWireCommitment{}, result.err
		}
		current, err := ReadLinuxRouteWireClockDomain()
		if err != nil || current != clock {
			return RouteWireCommitment{}, errors.Join(ErrFinalAccountingUnavailable, err)
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return RouteWireCommitment{}, err
		}
		if !o.running || o.stopping || o.correlation.poisoned || o.routeWireStop != nil && *o.routeWireStop != result.commitment {
			return RouteWireCommitment{}, ErrFinalAccountingUnavailable
		}
		o.routeWireStop = &result.commitment
		return result.commitment, nil
	case <-ctx.Done():
		o.poison()
		return RouteWireCommitment{}, ctx.Err()
	case <-o.stoppingDone:
		return RouteWireCommitment{}, ErrLinuxOwnerPoisoned
	}
}

func (o *LinuxFixture) routeWireCensusOnWorkOwner(x linuxSampleExchange) {
	deadline, _ := x.ctx.Deadline()
	raw, err := encodeLinuxPacket(linuxRouteWireCensus, x.id, 1, linuxSampleRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(time.Until(deadline))})
	if err == nil {
		err = o.work.write(raw, deadline)
	}
	var commitment RouteWireCommitment
	if err == nil {
		var packet linuxPacket
		packet, err = o.work.read(deadline)
		if err == nil {
			commitment, err = decodeLinuxRouteWireCensusReply(packet, x.id, o.correlation.nonce, o.spec.planned.plan.RouteWire, x.wireCensusClock)
		}
	}
	if err != nil && !errors.Is(err, ErrFinalAccountingUnavailable) {
		o.poison()
	}
	o.mu.Lock()
	o.workBusy = false
	o.mu.Unlock()
	select {
	case x.wireCensusResult <- linuxRouteWireCensusOutcome{commitment: commitment, err: err}:
	case <-x.ctx.Done():
		o.poison()
	case <-o.stoppingDone:
	}
}

// BindRouteWireWindow binds the controller's independent census to the
// current FullNode launch. The Stop packet carries only its commitment.
// Like the other MemberGroup operations, the group executor owns this call.
func (g *LinuxMemberGroup) BindRouteWireWindow(id MemberID, expected RouteWireExpectation) error {
	member := g.routeWireMember(id)
	if member == nil || member.current == nil || member.fixture == nil {
		return ErrFinalAccountingUnavailable
	}
	commitment, err := commitRouteWireWindow(expected, member.current.Public.Identity)
	if err != nil {
		return err
	}
	o := member.fixture
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.running || o.stopping || o.correlation.poisoned || o.spec.planned == nil ||
		o.spec.planned.ordinal != member.current.Incarnation.Ordinal ||
		!acceptsRouteWireCommitment(o.spec.planned.plan.RouteWire, commitment) {
		return ErrFinalAccountingUnavailable
	}
	if o.routeWireStop != nil {
		if *o.routeWireStop == commitment {
			return nil
		}
		return ErrFinalAccountingUnavailable
	}
	if commitment.Bounds.valid() {
		return ErrFinalAccountingUnavailable
	}
	o.routeWireStop = &commitment
	return nil
}

// RouteWireSeal returns the joined helper's seal for the last stopped
// incarnation. The controller still verifies the separately read report.
func (g *LinuxMemberGroup) RouteWireSeal(id MemberID, ordinal IncarnationOrdinal) (RouteWireFinalSeal, error) {
	member := g.routeWireMember(id)
	if member == nil || member.previousFixture == nil || len(member.ended) == 0 || member.routeWireFinalOrdinal != ordinal {
		return RouteWireFinalSeal{}, ErrFinalAccountingUnavailable
	}
	ended := member.ended[len(member.ended)-1]
	if ended.Incarnation.Ordinal != ordinal || ended.Finality != FinalExact {
		return RouteWireFinalSeal{}, ErrFinalAccountingUnavailable
	}
	o := member.previousFixture
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.spec.planned == nil || o.spec.planned.ordinal != ordinal ||
		o.correlation.poisoned || !o.helperJoined || !o.helperWaitKnown || !o.targetKnown ||
		!o.helperWait.Clean() || !o.targetExit.Clean() || o.memberFinal == nil ||
		o.memberFinal.Receipt.Incarnation.Ordinal != ordinal || o.memberFinal.Finality != FinalExact ||
		o.routeWireStop == nil || o.memberFinal.RouteWire == nil || !o.memberFinal.RouteWire.validFor(*o.routeWireStop) {
		return RouteWireFinalSeal{}, ErrFinalAccountingUnavailable
	}
	return *o.memberFinal.RouteWire, nil
}

// ReadRouteWireReport returns the exact class counters for the last stopped
// incarnation through its audited profile parent. The controller supplies the
// same independent census it bound before Stop.
func (g *LinuxMemberGroup) ReadRouteWireReport(ctx context.Context, id MemberID, ordinal IncarnationOrdinal, expected RouteWireExpectation) (HeadlessRouteWireAccounting, error) {
	return g.readRouteWireReport(ctx, id, ordinal, RouteWireCommitment{}, &expected)
}

// ReadCommittedRouteWireReport verifies the report against the commitment
// previously captured from the running FullNode registry.
func (g *LinuxMemberGroup) ReadCommittedRouteWireReport(ctx context.Context, id MemberID, ordinal IncarnationOrdinal, commitment RouteWireCommitment) (HeadlessRouteWireAccounting, error) {
	if !commitment.Bounds.valid() {
		return HeadlessRouteWireAccounting{}, ErrFinalReportInvalid
	}
	return g.readRouteWireReport(ctx, id, ordinal, commitment, nil)
}

func (g *LinuxMemberGroup) readRouteWireReport(ctx context.Context, id MemberID, ordinal IncarnationOrdinal, commitment RouteWireCommitment, expected *RouteWireExpectation) (HeadlessRouteWireAccounting, error) {
	var empty HeadlessRouteWireAccounting
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	seal, err := g.RouteWireSeal(id, ordinal)
	if err != nil {
		return empty, err
	}
	member := g.routeWireMember(id)
	o := member.previousFixture
	o.mu.Lock()
	receipt, traffic := o.memberFinal.Receipt, o.memberFinal.Traffic
	launch := o.spec.planned.binding
	o.mu.Unlock()
	binding, err := completedStartBinding(launch, receipt)
	if err != nil {
		return empty, err
	}
	if expected != nil {
		commitment, err = commitRouteWireWindow(*expected, binding.RuntimeBinding().Identity)
	}
	if err != nil || !seal.validFor(commitment) {
		return empty, ErrFinalReportInvalid
	}
	lease, err := member.parent.acquire()
	if err != nil {
		return empty, err
	}
	profile := routeWireProfileIdentity{device: member.profileDevice, inode: member.profileInode, uid: uint32(member.spec.UID), gid: member.spec.GID}
	raw, err := readRouteWireProfile(ctx, lease.file, profile, launch.nonce, seal)
	if err := errors.Join(err, lease.file.Close()); err != nil {
		return empty, err
	}
	prior := NewTransportHighWater(binding.RuntimeBinding())
	prior.Observe(traffic, nil)
	result, err := decodeCommittedHeadlessRouteWireReport(raw, binding, prior, commitment)
	if err != nil {
		return empty, err
	}
	if !result.Traffic.ReadAt.Equal(traffic.ReadAt) || result.Traffic.BytesSent != traffic.BytesSent ||
		result.Traffic.BytesReceived != traffic.BytesReceived {
		return empty, ErrFinalReportInvalid
	}
	return result, nil
}

func (g *LinuxMemberGroup) routeWireMember(id MemberID) *linuxMember {
	if g != nil {
		for _, member := range g.owners {
			if member.id == id && member.role == MemberFull && member.planned.RouteWire != nil {
				return member
			}
		}
	}
	return nil
}

func (r *linuxHelperRuntime) bindRouteWireStop(ctx context.Context, commitment RouteWireCommitment) error {
	if r.init.Planned == nil || !acceptsRouteWireCommitment(r.init.Planned.Plan.RouteWire, commitment) {
		return ErrFinalReportInvalid
	}
	r.mu.Lock()
	binding := r.memberBinding
	verified := !commitment.Bounds.valid() || r.wireCensus != nil && *r.wireCensus == commitment
	r.mu.Unlock()
	if binding == nil || !verified {
		return ErrFinalAccountingUnavailable
	}
	return r.process.bindRouteWireCommitment(ctx, *binding, commitment)
}

func (r *linuxHelperRuntime) writeRouteWireCensus(ctx context.Context, id LinuxRequestID) error {
	r.mu.Lock()
	binding := r.memberBinding
	r.mu.Unlock()
	reply := linuxRouteWireCensusReply{Nonce: r.init.Nonce, Code: linuxResultUnavailable}
	if r.init.Planned != nil && binding != nil {
		plan := r.init.Planned.Plan.RouteWire
		if plan != nil && plan.Bounds.valid() {
			clock, clockErr := ReadLinuxRouteWireClockDomain()
			if clockErr == nil {
				commitment, err := r.process.ReadRouteWireCensus(ctx, *binding, *plan, clock)
				current, currentErr := ReadLinuxRouteWireClockDomain()
				if err == nil && currentErr == nil && current == clock {
					r.mu.Lock()
					if r.wireCensus == nil || *r.wireCensus == commitment {
						r.wireCensus = &commitment
						reply.Code, reply.Census, reply.Clock = linuxResultReady, &commitment, &clock
					}
					r.mu.Unlock()
				}
			}
		}
	}
	raw, err := encodeLinuxPacket(linuxRouteWireCensusResult, id, 1, reply)
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	return r.work.write(raw, minLinuxPacketDeadline(deadline))
}
