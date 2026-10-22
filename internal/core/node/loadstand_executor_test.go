package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

type loadStandExecutionLimits struct {
	RouteReady      time.Duration
	Readiness       time.Duration
	Action          time.Duration
	Cleanup         time.Duration
	ReceiptDrain    time.Duration
	AllowedLateness time.Duration
	MaxActions      int
	MaxSamples      int
	MaxTraceBytes   int
}

func (l loadStandExecutionLimits) validate() error {
	if l.RouteReady <= 0 || l.Readiness <= 0 || l.Action <= 0 || l.Cleanup <= 0 || l.ReceiptDrain <= 0 || l.AllowedLateness <= 0 || l.MaxActions < 1 || l.MaxSamples < 2 || l.MaxTraceBytes < 1 {
		return loadstand.ErrInvalidConfiguration
	}
	return nil
}

type loadStandExecutionAction struct {
	At        time.Duration
	Kind      loadStandEventKind
	Node      loadStandNodeID
	Recipient loadStandNodeID
	Sample    bool
}
type loadStandActionTrace struct {
	Scheduled   time.Duration
	Begin       time.Duration
	End         time.Duration
	WallAt      time.Time
	Kind        string
	Node        loadStandNodeID
	Incarnation loadstand.Incarnation
	Recipient   domain.PeerIdentity
	BodySHA256  string
	Pending     bool
}
type loadStandMemberObservation struct {
	At      time.Duration
	Node    loadstand.MemberID
	PreStop bool
	// Siblings are sampled for the stopping member's shared heap, not stopped.
	PreStopOf loadstand.MemberID
	Sample    loadstand.MemberSample
}
type loadStandExecutionID string
type loadStandGlobalObservation struct {
	MessageID        domain.MessageID
	Topic            domain.MessageTopic
	ClaimedSender    domain.PeerIdentity
	BodySHA256       string
	Observer         loadstand.MemberID
	ObserverIdentity domain.PeerIdentity
	Incarnation      loadstand.Incarnation
	Provenance       loadstand.GlobalReplicaProvenance
}
type loadStandExpectedGlobal struct {
	Message          domain.GlobalMessage
	BodySHA256       string
	Observer         loadStandNodeID
	ObserverIdentity domain.PeerIdentity
	Incarnation      loadstand.Incarnation
	Observed         bool
}
type loadStandRuntimeCheck struct {
	At                  time.Duration
	PreStopOf           loadstand.MemberID
	Group               uint64
	Members             []loadstand.MemberID
	Provider            loadstand.MemberID
	ProviderIncarnation loadstand.Incarnation
	RuntimeReadAt       time.Time
	FloorBytes          uint64
	HeapObjectsBytes    *uint64
	Verdict             string
}
type loadStandExecutionReport struct {
	RuntimeChecks               []loadStandRuntimeCheck
	GlobalObservations          []loadStandGlobalObservation
	ExecutionID                 loadStandExecutionID
	OwnedAfterCleanup           []loadstand.MemberID
	StartupConvergenceElapsed   time.Duration
	RoutesReady                 bool
	PlanVersion                 string
	PlanDigest                  loadStandPlanDigest
	FunctionalLifecycleComplete bool
	FullSampleAvailable         bool
	AcceptanceEligible          bool
	Actions                     []loadStandActionTrace
	Samples                     []loadStandMemberObservation
	Receipts                    []loadstand.Receipt
	Ended                       map[loadstand.MemberID][]loadstand.EndedIncarnation
}
type loadStandExpectedReceipt struct {
	Sender     domain.PeerIdentity
	Recipient  domain.PeerIdentity
	BodySHA256 string
	Event      int
	Delivered  bool
	MessageID  domain.MessageID
}
type loadStandExecutor struct {
	plan          loadStandPlan
	members       map[loadStandNodeID]loadstand.Member
	wall          runjournal.Clock
	schedule      func() loadstand.ScheduleClock
	limits        loadStandExecutionLimits
	ownedStarts   []loadStandNodeID
	starts        map[loadStandNodeID]loadstand.StartReceipt
	processes     map[loadStandNodeID]loadstand.RuntimeProcess
	runtimeGroups map[loadstand.RuntimeProcess]uint64
	globals       []loadStandExpectedGlobal
	expected      []loadStandExpectedReceipt
	report        loadStandExecutionReport
	clock         loadstand.ScheduleClock
	lastElapsed   time.Duration
}

// compile validates the whole immutable plan before the first side effect.
// User traffic requires ordinary SDK publisher ports; FULL members only
// observe replicas. A raw-frame command is never a publisher capability.
func (e *loadStandExecutor) compile() ([]loadStandExecutionAction, error) {
	if err := e.limits.validate(); err != nil {
		return nil, err
	}
	if e.wall == nil || e.schedule == nil || e.plan.Duration() <= 0 || e.plan.Duration() > loadStandMaxPlanDuration || e.plan.computeDigest() != e.plan.Digest() {
		return nil, loadstand.ErrUnsupportedPlan
	}
	key := runjournal.ConfigKey{Params: e.plan.Params()}
	version, ok := key.Param("plan_version")
	if !ok || version != "3" {
		return nil, loadstand.ErrUnsupportedPlan
	}
	gossip, ok := key.Param("gossip_interval")
	if !ok {
		return nil, loadstand.ErrUnsupportedPlan
	}
	if len(e.members) != len(e.plan.nodes) {
		return nil, loadstand.ErrUnsupportedPlan
	}
	if gossip != "off" {
		publishers, observers := 0, 0
		for _, member := range e.members {
			if member.Role() == loadstand.MemberSDK {
				if _, ok := member.(loadstand.GlobalPublisher); !ok {
					return nil, loadstand.ErrUnsupportedPlan
				}
				publishers++
			}
			if member.Role() == loadstand.MemberFull {
				if _, ok := member.(loadstand.GlobalObserver); !ok {
					return nil, loadstand.ErrUnsupportedPlan
				}
				observers++
			}
		}
		if publishers == 0 || observers == 0 {
			return nil, loadstand.ErrUnsupportedPlan
		}
	}
	byID := map[loadStandNodeID]loadStandPlanNode{}
	for _, n := range e.plan.Nodes() {
		m, ok := e.members[n.id]
		if !ok || m == nil || m.ID() != loadstand.MemberID(n.id) || m.Owned() || len(m.EndedIncarnations()) != 0 {
			return nil, loadstand.ErrUnsupportedPlan
		}
		want := loadstand.MemberFull
		if n.role == loadStandRoleEdge {
			want = loadstand.MemberSDK
			if _, ok := m.(loadstand.UserTraffic); !ok {
				return nil, loadstand.ErrUnsupportedPlan
			}
		} else if n.role != loadStandRoleFull {
			return nil, loadstand.ErrUnsupportedPlan
		}
		if m.Role() != want {
			return nil, loadstand.ErrUnsupportedPlan
		}
		if _, dup := byID[n.id]; dup {
			return nil, loadstand.ErrUnsupportedPlan
		}
		byID[n.id] = n
	}
	for _, n := range e.plan.Nodes() {
		if len(n.dialTargets) == 0 {
			return nil, loadstand.ErrUnsupportedPlan
		}
		seen := map[loadStandNodeID]bool{}
		for _, id := range n.dialTargets {
			target, ok := byID[id]
			if !ok || id == n.id || target.role != loadStandRoleFull || seen[id] {
				return nil, loadstand.ErrUnsupportedPlan
			}
			seen[id] = true
		}
		bootstrap := e.members[n.id].Bootstrap()
		expectedBootstrap := make([]domain.PeerAddress, 0, len(n.dialTargets))
		for _, id := range n.dialTargets {
			endpoint, ok := e.members[id].Endpoint()
			if !ok {
				return nil, loadstand.ErrUnsupportedPlan
			}
			expectedBootstrap = append(expectedBootstrap, endpoint)
		}
		slices.Sort(bootstrap)
		slices.Sort(expectedBootstrap)
		if !slices.Equal(bootstrap, expectedBootstrap) {
			return nil, loadstand.ErrUnsupportedPlan
		}
		for _, id := range n.contacts {
			c, ok := byID[id]
			if !ok || id == n.id || c.role != loadStandRoleEdge {
				return nil, loadstand.ErrUnsupportedPlan
			}
		}
	}
	if len(e.plan.timeline) > e.limits.MaxActions || len(e.plan.samples)+2 > e.limits.MaxSamples {
		return nil, loadstand.ErrUnsupportedPlan
	}
	sorted := e.plan.Timeline()
	sortLoadStandEvents(sorted)
	if !slices.Equal(sorted, e.plan.timeline) {
		return nil, loadstand.ErrUnsupportedPlan
	}
	online := map[loadStandNodeID]bool{}
	for id := range byID {
		online[id] = true
	}
	globalObservations := 0
	actions := make([]loadStandExecutionAction, 0, len(sorted)+len(e.plan.samples)+2)
	for i, event := range sorted {
		n, ok := byID[event.node]
		if !ok || event.at < 0 || event.at >= e.plan.Duration() {
			return nil, loadstand.ErrUnsupportedPlan
		}
		if i > 0 && event == sorted[i-1] {
			return nil, loadstand.ErrUnsupportedPlan
		}
		switch event.kind {
		case loadStandEventStop:
			if !online[n.id] {
				return nil, loadstand.ErrUnsupportedPlan
			}
			online[n.id] = false
		case loadStandEventStart:
			if online[n.id] {
				return nil, loadstand.ErrUnsupportedPlan
			}
			online[n.id] = true
		case loadStandEventGossip:
			if !online[n.id] || n.role != loadStandRoleEdge || event.topic != loadStandGlobalTopic {
				return nil, loadstand.ErrUnsupportedPlan
			}
			liveFull := false
			for id, node := range byID {
				if node.role == loadStandRoleFull && online[id] {
					liveFull = true
					globalObservations++
					if globalObservations > e.limits.MaxActions {
						return nil, loadstand.ErrUnsupportedPlan
					}
				}
			}
			if !liveFull {
				return nil, loadstand.ErrUnsupportedPlan
			}
		case loadStandEventDirectMessage:
			if !online[n.id] || n.role != loadStandRoleEdge || byID[event.recipient].role != loadStandRoleEdge || !slices.Contains(n.contacts, event.recipient) {
				return nil, loadstand.ErrUnsupportedPlan
			}
		default:
			return nil, loadstand.ErrUnsupportedPlan
		}
		actions = append(actions, loadStandExecutionAction{At: event.at, Kind: event.kind, Node: event.node, Recipient: event.recipient})
	}
	previous := time.Duration(-1)
	for _, at := range e.plan.Samples() {
		if at <= previous || at < 0 || at > e.plan.Duration() {
			return nil, loadstand.ErrUnsupportedPlan
		}
		previous = at
		actions = append(actions, loadStandExecutionAction{At: at, Sample: true})
	}
	if !slices.Contains(e.plan.samples, time.Duration(0)) {
		actions = append(actions, loadStandExecutionAction{At: 0, Sample: true})
	}
	if !slices.Contains(e.plan.samples, e.plan.Duration()) {
		actions = append(actions, loadStandExecutionAction{At: e.plan.Duration(), Sample: true})
	}
	slices.SortStableFunc(actions, func(a, b loadStandExecutionAction) int {
		if a.At < b.At {
			return -1
		}
		if a.At > b.At {
			return 1
		}
		if a.Sample && !b.Sample {
			return 1
		}
		if !a.Sample && b.Sample {
			return -1
		}
		return 0
	})
	return actions, nil
}
func (e *loadStandExecutor) run(ctx context.Context) (report loadStandExecutionReport, err error) {
	actions, err := e.compile()
	if err != nil {
		return report, err
	}
	var runID [16]byte
	if _, err := rand.Read(runID[:]); err != nil {
		return report, err
	}
	e.expected = nil
	e.globals = nil
	e.ownedStarts = nil
	e.lastElapsed = 0
	e.starts = map[loadStandNodeID]loadstand.StartReceipt{}
	e.processes = map[loadStandNodeID]loadstand.RuntimeProcess{}
	e.runtimeGroups = map[loadstand.RuntimeProcess]uint64{}
	e.report = loadStandExecutionReport{ExecutionID: loadStandExecutionID(hex.EncodeToString(runID[:])), PlanVersion: "3", PlanDigest: e.plan.Digest(), FullSampleAvailable: true, Ended: map[loadstand.MemberID][]loadstand.EndedIncarnation{}}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), e.limits.Cleanup)
		defer stop()
		cleanupErr := e.cleanup(cleanup)
		cancel()
		err = errors.Join(err, cleanupErr)
		err = errors.Join(err, e.traceBound())
		e.report.FunctionalLifecycleComplete = err == nil
		report = e.report
	}()
	for _, n := range e.plan.Nodes() {
		if err := ctx.Err(); err != nil {
			return e.report, err
		}
		if err := e.start(ctx, lifetime, n.id); err != nil {
			return e.report, err
		}
	}
	if err := e.identityBarrier(); err != nil {
		return e.report, err
	}
	if err := e.peerBarrier(ctx); err != nil {
		return e.report, err
	}
	if err := e.contacts(ctx); err != nil {
		return e.report, err
	}
	if err := e.routeBarrier(ctx); err != nil {
		return e.report, err
	}
	e.clock = e.schedule()
	if e.clock == nil {
		return e.report, loadstand.ErrInvalidConfiguration
	}
	for index, action := range actions {
		if err := e.waitUntil(ctx, action.At); err != nil {
			return e.report, err
		}
		begin, err := e.elapsed(action.At)
		if err != nil {
			return e.report, err
		}
		if err := e.checkRunning(ctx); err != nil {
			return e.report, err
		}
		operation, stop := context.WithTimeout(ctx, e.limits.Action)
		trace := loadStandActionTrace{Scheduled: action.At, Begin: begin, WallAt: e.wall.Now(), Kind: action.Kind.String(), Node: action.Node}
		err = e.dispatch(operation, lifetime, action, index, &trace)
		stop()
		trace.End = e.clock.Elapsed()
		e.report.Actions = append(e.report.Actions, trace)
		if err != nil {
			return e.report, err
		}
		if trace.End < begin || trace.End-action.At > e.limits.AllowedLateness {
			return e.report, loadstand.ErrScheduleMissed
		}
		e.lastElapsed = trace.End
		if err := e.traceBound(); err != nil {
			return e.report, err
		}
	}
	drain, stop := context.WithTimeout(ctx, e.limits.ReceiptDrain)
	defer stop()
	if err := e.drainReceipts(drain); err != nil {
		return e.report, err
	}
	return e.report, nil
}
func (e *loadStandExecutor) start(ctx, lifetime context.Context, id loadStandNodeID) error {
	ready, stop := context.WithTimeout(ctx, e.limits.Readiness)
	defer stop()
	r, err := e.members[id].Start(lifetime, ready)
	if e.members[id].Owned() {
		e.removeOwnedStart(id)
		e.ownedStarts = append(e.ownedStarts, id)
	}
	if err != nil {
		return err
	}
	if r.Public.Identity.IsZero() || r.Incarnation.Ordinal == 0 || r.Incarnation.StartedAt.IsZero() {
		return loadstand.ErrMemberState
	}
	if old, ok := e.starts[id]; ok && (old.Public != r.Public || r.Incarnation.Ordinal != old.Incarnation.Ordinal+1 || !r.Incarnation.StartedAt.After(old.Incarnation.StartedAt)) {
		return loadstand.ErrMemberState
	}
	process := e.members[id].RuntimeProcess()
	if !process.Valid() {
		return loadstand.ErrMemberState
	}
	if e.processes == nil {
		e.processes = map[loadStandNodeID]loadstand.RuntimeProcess{}
	}
	if e.runtimeGroups == nil {
		e.runtimeGroups = map[loadstand.RuntimeProcess]uint64{}
	}
	if _, found := e.runtimeGroups[process]; !found {
		e.runtimeGroups[process] = uint64(len(e.runtimeGroups) + 1)
	}
	e.processes[id] = process
	e.starts[id] = r
	return nil
}
func (e *loadStandExecutor) identityBarrier() error {
	seen := map[domain.PeerIdentity]bool{}
	for _, n := range e.plan.Nodes() {
		p := e.starts[n.id].Public
		if seen[p.Identity] {
			return loadstand.ErrMemberState
		}
		seen[p.Identity] = true
	}
	return nil
}
func (e *loadStandExecutor) peerBarrier(ctx context.Context) error {
	barrier, stop := context.WithTimeout(ctx, e.limits.Readiness)
	defer stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		all := true
		for _, n := range e.plan.Nodes() {
			if !e.members[n.id].Owned() {
				continue
			}
			if err := e.members[n.id].CheckRunning(barrier); err != nil {
				return err
			}
			if err := e.members[n.id].PeerHealth(barrier); err != nil {
				all = false
			}
		}
		if all {
			return nil
		}
		select {
		case <-barrier.Done():
			return barrier.Err()
		case <-tick.C:
		}
	}
}
func (e *loadStandExecutor) contacts(ctx context.Context) error {
	for _, n := range e.plan.Nodes() {
		if !e.members[n.id].Owned() {
			continue
		}
		contacts := make([]loadstand.PublicIdentity, 0, len(n.contacts))
		for _, id := range n.contacts {
			contacts = append(contacts, e.starts[id].Public)
		}
		if len(contacts) == 0 {
			continue
		}
		action, stop := context.WithTimeout(ctx, e.limits.Action)
		err := e.members[n.id].ImportContacts(action, contacts)
		stop()
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *loadStandExecutor) elapsed(at time.Duration) (time.Duration, error) {
	elapsed := e.clock.Elapsed()
	if elapsed < e.lastElapsed || elapsed < at || elapsed-at > e.limits.AllowedLateness {
		return elapsed, loadstand.ErrScheduleMissed
	}
	return elapsed, nil
}
func (e *loadStandExecutor) checkRunning(ctx context.Context) error {
	for _, n := range e.plan.Nodes() {
		m := e.members[n.id]
		if m.Owned() {
			if err := m.CheckRunning(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *loadStandExecutor) dispatch(ctx, lifetime context.Context, a loadStandExecutionAction, index int, trace *loadStandActionTrace) error {
	if a.Sample {
		trace.Kind = "sample"
		return e.sample(ctx, a.At, false)
	}
	trace.Incarnation = e.starts[a.Node].Incarnation
	switch a.Kind {
	case loadStandEventStart:
		if err := e.start(ctx, lifetime, a.Node); err != nil {
			return err
		}
		trace.Incarnation = e.starts[a.Node].Incarnation
		// A restart must regain its planned connections, without moving deadlines.
		if err := e.peerBarrier(ctx); err != nil {
			return err
		}
		return e.contacts(ctx)
	case loadStandEventStop:
		return e.stop(ctx, a.Node, a.At)
	case loadStandEventGossip:
		publisher, ok := e.members[a.Node].(loadstand.GlobalPublisher)
		if !ok {
			return loadstand.ErrUnsupportedPlan
		}
		body := loadstand.MessageBody(fmt.Sprintf("t5d/%s/%s/%d", e.plan.Digest(), e.report.ExecutionID, index))
		result, err := publisher.PublishGlobal(ctx, body)
		if err != nil {
			return err
		}
		if result.Outcome != domain.GlobalStoredLocally || result.Message.Topic != domain.GlobalMessageTopic || result.Message.ClaimedSender != e.starts[a.Node].Public.Identity || string(result.Message.Body) != string(body) || !result.Message.ID.IsValid() {
			return loadstand.ErrInvalidSample
		}
		for _, prior := range e.globals {
			if prior.Message.ID == result.Message.ID {
				return loadstand.ErrInvalidSample
			}
		}
		digest := sha256.Sum256([]byte(result.Message.Body))
		trace.BodySHA256 = hex.EncodeToString(digest[:])
		trace.Pending = true
		for _, node := range e.plan.Nodes() {
			if node.role == loadStandRoleFull && e.members[node.id].Owned() {
				start := e.starts[node.id]
				e.globals = append(e.globals, loadStandExpectedGlobal{Message: result.Message, BodySHA256: trace.BodySHA256, Observer: node.id, ObserverIdentity: start.Public.Identity, Incarnation: start.Incarnation})
			}
		}
		return nil
	case loadStandEventDirectMessage:
		sender, ok := e.members[a.Node].(loadstand.UserTraffic)
		if !ok {
			return loadstand.ErrUnsupportedPlan
		}
		body := loadstand.MessageBody(fmt.Sprintf("t5c/%s/%s/%d", e.plan.Digest(), e.report.ExecutionID, index))
		digest := sha256.Sum256([]byte(body))
		recipient := e.starts[a.Recipient].Public.Identity
		ack, err := sender.SendDM(ctx, recipient, body)
		if err != nil {
			return err
		}
		if !ack.Queued || ack.Recipient != recipient {
			return loadstand.ErrMemberState
		}
		trace.Recipient = recipient
		trace.BodySHA256 = hex.EncodeToString(digest[:])
		trace.Pending = true
		e.expected = append(e.expected, loadStandExpectedReceipt{Sender: e.starts[a.Node].Public.Identity, Recipient: recipient, BodySHA256: trace.BodySHA256, Event: index})
		return nil
	default:
		return loadstand.ErrUnsupportedPlan
	}
}

// collectRuntimeSweep keeps the real executor/lifecycle. A nonzero filter is
// exactly one retained process group; all other owned members are untouched.
func (e *loadStandExecutor) collectRuntimeSweep(ctx context.Context, at time.Duration, filter loadstand.RuntimeProcess, preStopOf loadstand.MemberID) error {
	var groups []loadStandRuntimeCheck
	byOwner := map[loadstand.RuntimeProcess]int{}
	for _, n := range e.plan.Nodes() {
		m := e.members[n.id]
		if !m.Owned() || (filter.Valid() && e.processes[n.id] != filter) {
			continue
		}
		s, readErr := m.Sample(ctx)
		e.report.Samples = append(e.report.Samples, loadStandMemberObservation{At: at, Node: m.ID(), PreStop: preStopOf == m.ID(), PreStopOf: preStopOf, Sample: s})
		if !s.FullSampleAvailable() {
			e.report.FullSampleAvailable = false
		}
		// Identity/incarnation and owner custody are separate original bindings.
		if s.Incarnation != e.starts[n.id].Incarnation || s.Identity != e.starts[n.id].Public.Identity {
			return errors.Join(readErr, loadstand.ErrMemberState)
		}
		expected := e.processes[n.id]
		check := loadStandRuntimeCheck{At: at, PreStopOf: preStopOf, Group: e.runtimeGroups[expected], Members: []loadstand.MemberID{m.ID()}, Provider: m.ID(), ProviderIncarnation: s.Incarnation}
		fail := func(verdict string) error {
			check.Verdict = verdict
			e.report.RuntimeChecks = append(e.report.RuntimeChecks, check)
			return errors.Join(readErr, loadStandRuntimeFailure(check))
		}
		if !expected.Valid() || s.Process != expected || m.RuntimeProcess() != expected {
			return fail("runtime_owner_mismatch")
		}
		if !s.Runtime.Available() {
			return fail("runtime_unavailable")
		}
		if err := loadstand.ValidateMemberRuntime(*s.Runtime.Value, e.starts[n.id].Incarnation.StartedAt); err != nil {
			return fail("runtime_binding_invalid")
		}
		if readErr != nil {
			return readErr
		}
		if !s.Resources.Available() {
			return fail("resource_floor_unavailable")
		}
		floor, err := loadStandCheckedFloor(*s.Resources.Value)
		if err != nil {
			return fail("resource_floor_overflow")
		}
		index, found := byOwner[expected]
		if !found {
			index = len(groups)
			byOwner[expected] = index
			groups = append(groups, loadStandRuntimeCheck{At: at, PreStopOf: preStopOf, Group: e.runtimeGroups[expected]})
		}
		g := &groups[index]
		if floor > ^uint64(0)-g.FloorBytes {
			return fail("resource_floor_overflow")
		}
		g.FloorBytes += floor
		g.Members = append(g.Members, m.ID())
		// Last in executor read order, not maximum remote wall time. Its runtime
		// read follows ALL resource reads of this physical group in this sweep.
		heap := *s.Runtime.Value.HeapObjectsBytes
		g.HeapObjectsBytes = &heap
		g.RuntimeReadAt = s.Runtime.Value.ReadAt
		g.Provider = m.ID()
		g.ProviderIncarnation = s.Incarnation
	}
	var failures []error
	for _, check := range groups {
		check.Verdict = "pass"
		if check.FloorBytes > *check.HeapObjectsBytes {
			check.Verdict = string(loadStandDefectFloorAboveHeap)
		}
		e.report.RuntimeChecks = append(e.report.RuntimeChecks, check)
		if check.Verdict != "pass" {
			failures = append(failures, loadStandRuntimeFailure(check))
		}
	}
	return errors.Join(failures...)
}
func loadStandRuntimeFailure(check loadStandRuntimeCheck) error {
	return errors.Join(loadstand.ErrMemberState, &loadStandDefect{Kind: loadStandDefectKind(check.Verdict), Detail: fmt.Sprintf("group=%d provider=%s ordinal=%d preStopOf=%s floor=%d", check.Group, check.Provider, check.ProviderIncarnation.Ordinal, check.PreStopOf, check.FloorBytes)})
}
func loadStandCheckedFloor(b domain.ResourceBreakdown) (uint64, error) {
	var floor uint64
	for _, subsystem := range b.Subsystems() {
		for _, gauge := range subsystem.Gauges() {
			if gauge.Kind() == domain.ResourceGaugeSaturation {
				continue
			}
			count, entry := gauge.Count(), gauge.EntryBytes()
			if entry != 0 && count > ^uint64(0)/entry {
				return 0, loadstand.ErrInvalidSample
			}
			value := count * entry
			if value > ^uint64(0)-floor {
				return 0, loadstand.ErrInvalidSample
			}
			floor += value
		}
	}
	return floor, nil
}
func (e *loadStandExecutor) sample(ctx context.Context, at time.Duration, preStop bool) error {
	// preStop is retained for existing call sites; stopping has an exact target.
	if preStop {
		return loadstand.ErrMemberState
	}
	if err := e.collectRuntimeSweep(ctx, at, loadstand.RuntimeProcess{}, ""); err != nil {
		return err
	}
	return errors.Join(e.observeReceipts(ctx), e.observeGlobals(ctx))
}
func (e *loadStandExecutor) stop(ctx context.Context, id loadStandNodeID, at time.Duration) error {
	m := e.members[id]
	if !m.Owned() {
		return loadstand.ErrMemberState
	}
	// Never reuse a periodic sample. Fresh siblings share the same Go heap.
	process := e.processes[id]
	var sampleErr error
	if process.Valid() {
		sampleErr = e.collectRuntimeSweep(ctx, at, process, m.ID())
	} else {
		// Partial Start still owns cleanup; an absent successful binding cannot
		// authorize any floor result, and must not suppress original Stop.
		s, err := m.Sample(ctx)
		e.report.Samples = append(e.report.Samples, loadStandMemberObservation{At: at, Node: m.ID(), PreStop: true, PreStopOf: m.ID(), Sample: s})
		e.report.FullSampleAvailable = false
		sampleErr = errors.Join(loadstand.ErrMemberState, err)
	}
	receipt, stopErr := m.Stop(ctx)
	if !receipt.Terminated {
		stopErr = errors.Join(stopErr, loadstand.ErrStopUnconfirmed)
	} else if m.Owned() {
		stopErr = errors.Join(stopErr, loadstand.ErrFinalAccountingPending)
	} else {
		e.removeOwnedStart(id)
	}
	return errors.Join(sampleErr, stopErr)
}

// Only the currently owned incarnation is retained, bounded by plan members.
func (e *loadStandExecutor) removeOwnedStart(id loadStandNodeID) {
	e.ownedStarts = slices.DeleteFunc(e.ownedStarts, func(existing loadStandNodeID) bool { return existing == id })
}
func (e *loadStandExecutor) cleanup(ctx context.Context) error {
	var errs []error
	e.report.OwnedAfterCleanup = nil
	order := slices.Clone(e.ownedStarts)
	slices.Reverse(order)
	for _, id := range order {
		if e.members[id].Owned() {
			errs = append(errs, e.stop(ctx, id, e.plan.Duration()))
		}
	}
	for _, n := range e.plan.Nodes() {
		m := e.members[n.id]
		e.report.Ended[m.ID()] = m.EndedIncarnations()
		if m.Owned() {
			e.report.OwnedAfterCleanup = append(e.report.OwnedAfterCleanup, m.ID())
		}
	}
	return errors.Join(errs...)
}
func (e *loadStandExecutor) observeReceipts(ctx context.Context) error {
	for _, n := range e.plan.Nodes() {
		m, ok := e.members[n.id].(loadstand.UserTraffic)
		if !ok {
			continue
		}
		receipts, err := m.Receipts(ctx, e.limits.MaxActions)
		if err != nil {
			return err
		}
		seen := map[domain.MessageID]bool{}
		for _, r := range receipts {
			if seen[r.ID] || !r.ID.IsValid() {
				return loadstand.ErrInvalidSample
			}
			seen[r.ID] = true
			matched := false
			for i := range e.expected {
				want := &e.expected[i]
				if want.Sender == r.Sender && want.Recipient == r.Recipient && want.BodySHA256 == r.BodySHA256 {
					if want.Delivered && want.MessageID != r.ID {
						return loadstand.ErrInvalidSample
					}
					if !want.Delivered {
						want.Delivered = true
						want.MessageID = r.ID
						e.report.Receipts = append(e.report.Receipts, r)
					}
					matched = true
					break
				}
			}
			if !matched {
				return loadstand.ErrInvalidSample
			}
		}
	}
	return nil
}
func (e *loadStandExecutor) drainReceipts(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := errors.Join(e.observeReceipts(ctx), e.observeGlobals(ctx)); err != nil {
			return err
		}
		complete := true
		for _, r := range e.expected {
			complete = complete && r.Delivered
		}
		for _, global := range e.globals {
			complete = complete && global.Observed
		}
		if complete {
			return nil
		}
		if err := e.checkRunning(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (e *loadStandExecutor) traceBound() error {
	raw, err := json.Marshal(e.report)
	if err != nil {
		return err
	}
	if len(raw) > e.limits.MaxTraceBytes {
		return loadstand.ErrUnsupportedPlan
	}
	return nil
}
func (e *loadStandExecutor) journalKey() runjournal.ConfigKey {
	params := e.plan.Params()
	params = append(params, loadStandParam("executor_version", "2"), loadStandParam("plan_digest", string(e.plan.Digest())), loadStandParam("profile", "functional-loopback"), loadStandParam("acceptance_eligible", "false"), loadStandParam("action_budget", e.limits.Action.String()), loadStandParam("readiness_budget", e.limits.Readiness.String()), loadStandParam("route_ready_budget", e.limits.RouteReady.String()), loadStandParam("cleanup_budget", e.limits.Cleanup.String()), loadStandParam("receipt_drain", e.limits.ReceiptDrain.String()), loadStandParam("allowed_lateness", e.limits.AllowedLateness.String()), loadStandParam("max_actions", strconv.Itoa(e.limits.MaxActions)), loadStandParam("max_samples", strconv.Itoa(e.limits.MaxSamples)), loadStandParam("max_trace_bytes", strconv.Itoa(e.limits.MaxTraceBytes)))
	return runjournal.ConfigKey{Measurement: "loadstand-functional-lifecycle", Label: "headless-sdk", Params: params}
}
func (e *loadStandExecutor) runJournal(ctx context.Context, j *runjournal.Journal) (loadStandExecutionReport, error) {
	key := e.journalKey()
	var result loadStandExecutionReport
	var executionErr error
	_, err := j.Sweep(ctx, []runjournal.ConfigKey{key}, runjournal.SelectAll(), func(ctx context.Context, _ runjournal.ConfigKey) ([]byte, error) {
		result, executionErr = e.run(ctx)
		if executionErr != nil {
			return nil, loadstand.ErrMemberState
		}
		return json.Marshal(result)
	})
	if err != nil || executionErr != nil {
		return result, errors.Join(err, executionErr)
	}
	recorded, err := j.Result(ctx, key)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(recorded.Body, &result); err != nil {
		return result, err
	}
	return result, nil
}

// One bounded wait observer, using snapshots of owned incarnation channels.
// It never reads mutable adapters concurrently and is joined before dispatch.
func (e *loadStandExecutor) waitUntil(ctx context.Context, at time.Duration) error {
	waitCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	finished := make(chan struct{})
	joined := make(chan struct{})
	cases := []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(finished)}}
	for _, n := range e.plan.Nodes() {
		m := e.members[n.id]
		if m.Owned() {
			done := m.Done()
			if done == nil {
				return loadstand.ErrMemberState
			}
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)})
		}
	}
	go func() {
		defer close(joined)
		chosen, _, _ := reflect.Select(cases)
		if chosen > 0 {
			cancel(loadstand.ErrMemberState)
		}
	}()
	err := e.clock.WaitUntil(waitCtx, at)
	close(finished)
	<-joined
	cause := context.Cause(waitCtx)
	cancel(nil)
	if cause != nil {
		return cause
	}
	return err
}

func (e *loadStandExecutor) routeBarrier(ctx context.Context) error {
	key := runjournal.ConfigKey{Params: e.plan.Params()}
	dm, _ := key.Param("dm_mean_interval")
	if dm == "off" {
		e.report.RoutesReady = true
		return nil
	}
	elapsed := e.schedule()
	if elapsed == nil {
		return loadstand.ErrInvalidConfiguration
	}
	begin := elapsed.Elapsed()
	defer func() { e.report.StartupConvergenceElapsed = elapsed.Elapsed() - begin }()
	barrier, stop := context.WithTimeout(ctx, e.limits.RouteReady)
	defer stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		all := true
		for _, n := range e.plan.Nodes() {
			if n.role != loadStandRoleEdge {
				continue
			}
			sender := e.members[n.id].(loadstand.UserTraffic)
			gateways := make([]domain.PeerIdentity, 0, len(n.dialTargets))
			for _, id := range n.dialTargets {
				gateways = append(gateways, e.starts[id].Public.Identity)
			}
			for _, recipient := range n.contacts {
				err := sender.PlannedRoute(barrier, e.starts[recipient].Public.Identity, gateways)
				if errors.Is(err, loadstand.ErrRouteNotReady) {
					all = false
				} else if err != nil {
					return err
				}
			}
		}
		if all {
			e.report.RoutesReady = true
			return nil
		}
		if err := e.checkRunning(barrier); err != nil {
			return err
		}
		select {
		case <-barrier.Done():
			return barrier.Err()
		case <-ticker.C:
		}
	}
}

func (e *loadStandExecutor) observeGlobals(ctx context.Context) error {
	for i := range e.globals {
		want := &e.globals[i]
		if want.Observed {
			continue
		}
		if e.starts[want.Observer].Incarnation != want.Incarnation || e.starts[want.Observer].Public.Identity != want.ObserverIdentity {
			return loadstand.ErrMemberState
		}
		observer, ok := e.members[want.Observer].(loadstand.GlobalObserver)
		if !ok {
			return loadstand.ErrUnsupportedPlan
		}
		actual, err := observer.GlobalReplica(ctx, want.Message.ID)
		if errors.Is(err, loadstand.ErrGlobalReplicaAbsent) {
			continue
		}
		if err != nil {
			return err
		}
		digest := sha256.Sum256([]byte(actual.Body))
		if actual.ID != want.Message.ID || actual.Topic != domain.GlobalMessageTopic || actual.ClaimedSender != want.Message.ClaimedSender || hex.EncodeToString(digest[:]) != want.BodySHA256 {
			return loadstand.ErrInvalidSample
		}
		want.Observed = true
		e.report.GlobalObservations = append(e.report.GlobalObservations, loadStandGlobalObservation{MessageID: actual.ID, Topic: actual.Topic, ClaimedSender: actual.ClaimedSender, BodySHA256: want.BodySHA256, Observer: loadstand.MemberID(want.Observer), ObserverIdentity: want.ObserverIdentity, Incarnation: want.Incarnation, Provenance: loadstand.UnsignedGlobalRuntimeStore})
	}
	return nil
}
