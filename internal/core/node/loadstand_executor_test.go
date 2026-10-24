package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
	BodyBytes   int              `json:",omitempty"`
	MessageID   domain.MessageID `json:",omitempty"`
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
	// First successful observation ONLY; nil means drain/outside periodic sampling.
	ObservedSweep *int `json:",omitempty"`
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

// Only complete original network sweeps are byte-balance edges. Sample indexes
// refer to the existing bounded report; no duplicate MemberSample vectors.
type loadStandTrafficSweep struct {
	At                       time.Duration
	Begin, End               time.Time
	SampleBegin, SampleCount int
	// Number of already completed sequential action traces at the traffic edge.
	CompletedActions int
}
type loadStandBalanceDTO struct {
	Nodes, Stops                                 int
	Sent, Received, Volume, Imbalance, Allowance loadStandByteCount
	// Nil is an undefined/nonfinite share, never a fabricated finite zero.
	AllowanceShare *loadStandShare
}
type loadStandByteBalanceCheck struct {
	Phase, Status, Reason string
	Earlier, Later        *int
	Node                  loadstand.MemberID
	Ordinal               loadstand.IncarnationOrdinal
	PreStopOf             loadstand.MemberID
	Tolerance             loadStandBalanceTolerance
	Computed              *loadStandBalanceDTO
}

var errLoadStandByteBalanceSource = errors.New("byte balance original source invalid")

type loadStandExecutionReport struct {
	C2Checks          []loadStandC2Check `json:",omitempty"`
	TrafficSweeps     []loadStandTrafficSweep
	ByteBalanceChecks []loadStandByteBalanceCheck
	// This functional profile declares a closed fixture, not kernel isolation.
	ByteBalanceScope            string
	ByteBalanceTerminal         string
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
	// Explicit functional control only. Nil preserves prior execution semantics.
	c2            *loadStandC2Expectation
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
	trafficLast   map[loadStandNodeID]domain.TransportTrafficStats
	// Diagnostic-only execution location. Not serialized into acceptance report.
	diagnosticStage       string
	diagnosticActionIndex *int
}

// compile validates the whole immutable plan before the first side effect.
// User traffic requires ordinary SDK publisher ports; FULL members only
// observe replicas. A raw-frame command is never a publisher capability.
func (e *loadStandExecutor) compile() ([]loadStandExecutionAction, error) {
	if err := e.limits.validate(); err != nil {
		return nil, err
	}
	if err := e.validateGossipPayload(); err != nil {
		return nil, err
	}
	if e.wall == nil || e.schedule == nil || e.plan.Duration() <= 0 || e.plan.Duration() > loadStandMaxPlanDuration || e.plan.computeDigest() != e.plan.Digest() {
		return nil, loadstand.ErrUnsupportedPlan
	}
	key := runjournal.ConfigKey{Params: e.plan.Params()}
	version, ok := key.Param("plan_version")
	if !ok || string(version) != e.plan.Version() {
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
	e.diagnosticStage = "compile"
	e.diagnosticActionIndex = nil
	actions, err := e.compile()
	if err != nil {
		return report, err
	}
	if e.c2 != nil && (e.c2.Executed || e.c2.Attempted || e.c2.Service != nil) {
		return report, errLoadStandC2Epoch
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
	e.trafficLast = map[loadStandNodeID]domain.TransportTrafficStats{}
	e.report = loadStandExecutionReport{ExecutionID: loadStandExecutionID(hex.EncodeToString(runID[:])), PlanVersion: e.plan.Version(), PlanDigest: e.plan.Digest(), FullSampleAvailable: true, ByteBalanceScope: "declared_functional_closed_fixture", Ended: map[loadstand.MemberID][]loadstand.EndedIncarnation{}}
	e.diagnosticStage = "startup"
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer func() {
		primaryFailure := err != nil
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), e.limits.Cleanup)
		defer stop()
		cleanupErr := e.cleanup(cleanup)
		cancel()
		err = errors.Join(err, cleanupErr, e.reconcileByteBalance())
		if err == nil && e.c2 != nil && !e.c2.Checked {
			err = errors.Join(loadstand.ErrMemberState, errLoadStandC2Attribution)
		}
		err = errors.Join(err, e.traceBound())
		if err != nil && !primaryFailure {
			e.diagnosticStage = "cleanup_reconcile_or_report"
			e.diagnosticActionIndex = nil
		}
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
	e.diagnosticStage = "identity_barrier"
	if err := e.identityBarrier(); err != nil {
		return e.report, err
	}
	e.diagnosticStage = "peer_barrier"
	if err := e.peerBarrier(ctx); err != nil {
		return e.report, err
	}
	e.diagnosticStage = "contacts"
	if err := e.contacts(ctx); err != nil {
		return e.report, err
	}
	e.diagnosticStage = "route_barrier"
	if err := e.routeBarrier(ctx); err != nil {
		return e.report, err
	}
	e.diagnosticStage = "clock_factory"
	e.clock = e.schedule()
	if e.clock == nil {
		return e.report, loadstand.ErrInvalidConfiguration
	}
	for index, action := range actions {
		actionIndex := index
		e.diagnosticActionIndex = &actionIndex
		e.diagnosticStage = "scheduled_wait"
		if err := e.waitUntil(ctx, action.At); err != nil {
			return e.report, err
		}
		e.diagnosticStage = "scheduled_elapsed"
		begin, err := e.elapsed(action.At)
		if err != nil {
			return e.report, err
		}
		e.diagnosticStage = "check_running"
		if err := e.checkRunning(ctx); err != nil {
			return e.report, err
		}
		operation, stop := context.WithTimeout(ctx, e.limits.Action)
		trace := loadStandActionTrace{Scheduled: action.At, Begin: begin, WallAt: e.wall.Now(), Kind: action.Kind.String(), Node: action.Node}
		e.diagnosticStage = "dispatch"
		err = e.dispatch(operation, lifetime, action, index, &trace)
		stop()
		trace.End = e.clock.Elapsed()
		e.report.Actions = append(e.report.Actions, trace)
		if err != nil {
			return e.report, err
		}
		e.diagnosticStage = "dispatch_lateness"
		if trace.End < begin || trace.End-action.At > e.limits.AllowedLateness {
			return e.report, fmt.Errorf("%w: site=dispatch_lateness action_index=%d scheduled_ns=%d begin_ns=%d end_ns=%d allowed_lateness_ns=%d", loadstand.ErrScheduleMissed, index, action.At, begin, trace.End, e.limits.AllowedLateness)
		}
		e.lastElapsed = trace.End
		e.diagnosticStage = "trace_bound"
		if err := e.traceBound(); err != nil {
			return e.report, err
		}
	}
	e.diagnosticStage = "receipt_drain"
	e.diagnosticActionIndex = nil
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
	if e.trafficLast == nil {
		e.trafficLast = map[loadStandNodeID]domain.TransportTrafficStats{}
	}
	delete(e.trafficLast, id)
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
	var publisher *loadstand.PublicIdentity
	if e.plan.gossipPayloadProfile == loadStandC1Global32K96 {
		original, err := e.c1OriginalPublisher()
		if err != nil {
			return err
		}
		publisher = &original
	}
	for _, n := range e.plan.Nodes() {
		if !e.members[n.id].Owned() {
			continue
		}
		contacts := make([]loadstand.PublicIdentity, 0, len(n.contacts))
		for _, id := range n.contacts {
			contacts = append(contacts, e.starts[id].Public)
		}
		// DM Contacts0 remains untouched. Only fixedC1 Full observers learn the
		// one genuine successful SDK publisher through their normal import path.
		if publisher != nil && n.role == loadStandRoleFull {
			contacts = append(contacts, *publisher)
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
		index := -1
		if e.diagnosticActionIndex != nil {
			index = *e.diagnosticActionIndex
		}
		return elapsed, fmt.Errorf("%w: site=scheduled_elapsed action_index=%d scheduled_ns=%d elapsed_ns=%d last_elapsed_ns=%d allowed_lateness_ns=%d", loadstand.ErrScheduleMissed, index, at, elapsed, e.lastElapsed, e.limits.AllowedLateness)
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
		body, err := e.globalBody(index)
		if err != nil {
			return err
		}
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
		if e.plan.gossipPayloadProfile != "" {
			trace.BodyBytes = len(body)
			trace.MessageID = result.Message.ID
		}
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
	// The journal/action clock can deliberately jump. TransportTrafficStats
	// stamps actual time.Now.UTC; use its actual kernel wall-clock domain here.
	trafficBegin := time.Now().UTC()
	var groups []loadStandRuntimeCheck
	byOwner := map[loadstand.RuntimeProcess]int{}
	for _, n := range e.plan.Nodes() {
		m := e.members[n.id]
		if !m.Owned() || (filter.Valid() && e.processes[n.id] != filter) {
			continue
		}
		s, readErr := m.Sample(ctx)
		// Freeze the scalar cell before report/anchor retention. A provider may
		// lawfully reuse its own Observation.Value storage on the next read.
		if s.Traffic.Value != nil {
			traffic := *s.Traffic.Value
			s.Traffic.Value = &traffic
		}
		if s.Datagram.Value != nil {
			d := *s.Datagram.Value
			d.LocalSenders = loadstand.CloneLocalSendersSnapshot(d.LocalSenders)
			s.Datagram.Value = &d
		}
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
		if err := e.checkC2Sample(m.ID(), s); err != nil {
			return err
		}
		if err := e.validateByteTraffic(n.id, s, trafficBegin, time.Now().UTC()); err != nil {
			return err
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
	if preStop {
		return loadstand.ErrMemberState
	}
	// Expected live scope comes from original successful lifecycle custody.
	// A disappearing owner is not silently treated as a planned stopped member.
	var live []loadstand.MemberID
	for _, n := range e.plan.Nodes() {
		owned := e.members[n.id].Owned()
		if owned != slices.Contains(e.ownedStarts, n.id) {
			return e.byteSourceFailure(n.id, "live_scope_changed")
		}
		if owned {
			live = append(live, e.members[n.id].ID())
		}
	}
	edge := loadStandTrafficSweep{At: at, Begin: time.Now().UTC(), SampleBegin: len(e.report.Samples), CompletedActions: len(e.report.Actions)}
	if err := e.collectRuntimeSweep(ctx, at, loadstand.RuntimeProcess{}, ""); err != nil {
		return err
	}
	edge.End = time.Now().UTC()
	edge.SampleCount = len(e.report.Samples) - edge.SampleBegin
	if edge.SampleCount != len(live) {
		return e.byteSourceFailure("", "incomplete_network_sweep")
	}
	for i, id := range live {
		observation := e.report.Samples[edge.SampleBegin+i]
		if observation.Node != id || observation.PreStopOf != "" {
			return e.byteSourceFailure(loadStandNodeID(id), "network_sweep_membership")
		}
	}
	e.report.TrafficSweeps = append(e.report.TrafficSweeps, edge)
	if err := e.recordByteBalance(len(e.report.TrafficSweeps)-1, "periodic"); err != nil {
		return err
	}
	return errors.Join(e.observeReceipts(ctx), e.observeGlobals(ctx, len(e.report.TrafficSweeps)-1))
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
	// This process-group slice is NOT a closed-network byte endpoint.
	e.report.ByteBalanceChecks = append(e.report.ByteBalanceChecks, loadStandByteBalanceCheck{Phase: "prestop", Status: "undefined", Reason: "partial_network_sweep", PreStopOf: m.ID(), Tolerance: loadStandDefaultBalanceTolerance()})
	receipt, stopErr := m.Stop(ctx)
	// Freeze the original successful stopped life before a later Start can
	// replace e.starts[id]. Even a history refusal never skips original Stop.
	stopErr = errors.Join(stopErr, e.retainByteEnded(id, receipt.Ended))
	if !receipt.Terminated {
		stopErr = errors.Join(stopErr, loadstand.ErrStopUnconfirmed)
	} else if m.Owned() {
		stopErr = errors.Join(stopErr, loadstand.ErrFinalAccountingPending)
	} else {
		e.removeOwnedStart(id)
	}
	return errors.Join(sampleErr, stopErr)
}

// Invalid source evidence is distinct from the qualified checker's legitimate
// lack of power. Only stable local reason labels enter this functional report.
func (e *loadStandExecutor) byteSourceFailure(id loadStandNodeID, reason string) error {
	ordinal := e.starts[id].Incarnation.Ordinal
	e.report.ByteBalanceChecks = append(e.report.ByteBalanceChecks, loadStandByteBalanceCheck{Phase: "source", Status: "source_invalid", Reason: reason, Node: loadstand.MemberID(id), Ordinal: ordinal})
	return errors.Join(loadstand.ErrMemberState, fmt.Errorf("%w: node=%s ordinal=%d reason=%s", errLoadStandByteBalanceSource, id, ordinal, reason))
}
func (e *loadStandExecutor) validateByteTraffic(id loadStandNodeID, s loadstand.MemberSample, begin, end time.Time) error {
	if !s.Traffic.Available() {
		return e.byteSourceFailure(id, "traffic_unavailable")
	}
	t := *s.Traffic.Value
	start := e.starts[id].Incarnation.StartedAt
	// Successful live incarnations follow the already validated, scalar-frozen
	// Ended prefix exactly. Initial1 also excludes MaxInt before legacy loops.
	if s.Incarnation.Ordinal != loadstand.IncarnationOrdinal(len(e.report.Ended[loadstand.MemberID(id)]))+1 {
		return e.byteSourceFailure(id, "live_ordinal_prefix")
	}
	if start.IsZero() || !t.StartedAt.Equal(start) || t.ReadAt.IsZero() || t.ReadAt.Before(start) {
		return e.byteSourceFailure(id, "traffic_original_lifetime")
	}
	if begin.IsZero() || end.Before(begin) || t.ReadAt.Before(begin) || t.ReadAt.After(end) {
		return e.byteSourceFailure(id, "traffic_clock_domain")
	}
	if prior, ok := e.trafficLast[id]; ok {
		if !t.StartedAt.Equal(prior.StartedAt) || !t.ReadAt.After(prior.ReadAt) || t.BytesSent < prior.BytesSent || t.BytesReceived < prior.BytesReceived {
			return e.byteSourceFailure(id, "traffic_regression")
		}
	}
	if e.trafficLast == nil {
		e.trafficLast = map[loadStandNodeID]domain.TransportTrafficStats{}
	}
	e.trafficLast[id] = t
	return nil
}
func loadStandSameTraffic(a, b domain.TransportTrafficStats) bool {
	return a.StartedAt.Equal(b.StartedAt) && a.ReadAt.Equal(b.ReadAt) && a.BytesSent == b.BytesSent && a.BytesReceived == b.BytesReceived
}
func (e *loadStandExecutor) retainByteEnded(id loadStandNodeID, stopped *loadstand.EndedIncarnation) error {
	m := e.members[id]
	observed := m.EndedIncarnations()
	prior := e.report.Ended[m.ID()]
	if len(observed) < len(prior) {
		return e.byteSourceFailure(id, "ended_history_shrank")
	}
	for i := range prior {
		a, b := prior[i], observed[i]
		if a.Incarnation != b.Incarnation || a.Finality != b.Finality || a.Final.State != b.Final.State || !a.Final.Available() || !b.Final.Available() || !loadStandSameTraffic(*a.Final.Value, *b.Final.Value) {
			return e.byteSourceFailure(id, "ended_prefix_changed")
		}
	}
	original, started := e.starts[id]
	// Only the current original successful Start can contribute ONE new life.
	if len(observed) > len(prior) && (!started || len(observed) != len(prior)+1) {
		return e.byteSourceFailure(id, "ended_unbound_life")
	}
	for i, life := range observed {
		if life.Incarnation.Ordinal != loadstand.IncarnationOrdinal(i+1) || life.Finality != loadstand.FinalExact || !life.Final.Available() {
			return e.byteSourceFailure(id, "ended_not_exact")
		}
		t := *life.Final.Value
		if life.Incarnation.StartedAt.IsZero() || !t.StartedAt.Equal(life.Incarnation.StartedAt) || t.ReadAt.Before(t.StartedAt) || t.ReadAt.After(time.Now().UTC()) {
			return e.byteSourceFailure(id, "ended_original_lifetime")
		}
		if i > 0 && (!life.Incarnation.StartedAt.After(observed[i-1].Incarnation.StartedAt) || life.Incarnation.StartedAt.Before(observed[i-1].Final.Value.ReadAt)) {
			return e.byteSourceFailure(id, "ended_life_order")
		}
	}
	if len(observed) > len(prior) {
		life := observed[len(observed)-1]
		if life.Incarnation != original.Incarnation {
			return e.byteSourceFailure(id, "ended_foreign_start")
		}
		t := *life.Final.Value
		if last, ok := e.trafficLast[id]; ok && (!t.StartedAt.Equal(last.StartedAt) || !t.ReadAt.After(last.ReadAt) || t.BytesSent < last.BytesSent || t.BytesReceived < last.BytesReceived) {
			return e.byteSourceFailure(id, "ended_counter_regression")
		}
	}
	if stopped != nil {
		if len(observed) == 0 {
			return e.byteSourceFailure(id, "stop_receipt_missing_history")
		}
		last := observed[len(observed)-1]
		if stopped.Incarnation != last.Incarnation || stopped.Finality != last.Finality || !stopped.Final.Available() || !loadStandSameTraffic(*stopped.Final.Value, *last.Final.Value) {
			return e.byteSourceFailure(id, "stop_receipt_history_mismatch")
		}
	}
	if started && !m.Owned() && uint64(len(observed)) != uint64(original.Incarnation.Ordinal) {
		return e.byteSourceFailure(id, "successful_life_missing_final")
	}
	if e.report.Ended == nil {
		e.report.Ended = map[loadstand.MemberID][]loadstand.EndedIncarnation{}
	}
	frozen := make([]loadstand.EndedIncarnation, len(observed))
	for i, life := range observed {
		frozen[i] = life.Clone()
	}
	e.report.Ended[m.ID()] = frozen
	return nil
}
func (e *loadStandExecutor) byteSweep(index int) (loadStandSweep, error) {
	if index < 0 || index >= len(e.report.TrafficSweeps) {
		return loadStandSweep{}, e.byteSourceFailure("", "missing_network_edge")
	}
	edge := e.report.TrafficSweeps[index]
	if edge.Begin.IsZero() || edge.End.Before(edge.Begin) || edge.SampleBegin < 0 || edge.SampleCount < 0 || edge.SampleBegin > len(e.report.Samples)-edge.SampleCount {
		return loadStandSweep{}, e.byteSourceFailure("", "invalid_network_edge")
	}
	sweep := loadStandSweep{Begin: edge.Begin, End: edge.End}
	seen := map[loadstand.MemberID]bool{}
	for _, reading := range e.report.Samples[edge.SampleBegin : edge.SampleBegin+edge.SampleCount] {
		s := reading.Sample
		ordinal := s.Incarnation.Ordinal
		life := loadStandLife(ordinal)
		if reading.PreStopOf != "" || seen[reading.Node] || life <= 0 || uint64(life) != uint64(ordinal) || !s.Traffic.Available() || !s.Traffic.Value.StartedAt.Equal(s.Incarnation.StartedAt) || s.Traffic.Value.ReadAt.Before(edge.Begin) || s.Traffic.Value.ReadAt.After(edge.End) {
			return sweep, e.byteSourceFailure(loadStandNodeID(reading.Node), "invalid_projected_edge")
		}
		seen[reading.Node] = true
		sweep.Nodes = append(sweep.Nodes, loadStandNodeSample{Node: loadStandNodeID(reading.Node), Incarnation: loadStandIncarnationID{Life: life, StartedAt: s.Incarnation.StartedAt}, ReadAt: s.ReadAt, Traffic: *s.Traffic.Value})
	}
	return sweep, nil
}
func (e *loadStandExecutor) byteEnded() ([]loadStandEndedIncarnation, error) {
	var result []loadStandEndedIncarnation
	for _, n := range e.plan.Nodes() {
		for _, original := range e.report.Ended[loadstand.MemberID(n.id)] {
			life := loadStandLife(original.Incarnation.Ordinal)
			if life <= 0 || uint64(life) != uint64(original.Incarnation.Ordinal) || original.Finality != loadstand.FinalExact || !original.Final.Available() {
				return nil, e.byteSourceFailure(n.id, "invalid_projected_final")
			}
			result = append(result, loadStandEndedIncarnation{Node: n.id, Life: life, Final: *original.Final.Value, TailAllowance: 0})
		}
	}
	return result, nil
}

// The qualified checker is unchanged. Guard its unchecked accumulation and
// float->uint conversion before calling it. This repeats NO judgement policy.
func loadStandByteBalanceRange(earlier, later loadStandSweep, ended []loadStandEndedIncarnation, t loadStandBalanceTolerance) error {
	if len(earlier.Nodes) == 0 || len(later.Nodes) == 0 {
		return nil
	}
	edges := loadStandBalanceEdges(earlier, later)
	if edges.inner <= 0 {
		return nil
	}
	spans, err := loadStandNodeSpans(earlier, later, ended)
	if err != nil {
		return err
	}
	var sent, received, tails uint64
	var stops int
	add := func(total *uint64, value uint64) bool {
		if value > ^uint64(0)-*total {
			return false
		}
		*total += value
		return true
	}
	for _, span := range spans {
		first, last, present := span.lives(earlier, later)
		if !present {
			continue
		}
		for life := first; life <= last; life++ {
			before, startErr := span.lifeStart(life, earlier)
			after, tail, stopped, endErr := span.lifeEnd(life, later)
			if startErr != nil || endErr != nil {
				return errors.Join(startErr, endErr)
			}
			if !after.StartedAt.Equal(before.StartedAt) || !after.ReadAt.After(before.ReadAt) || after.BytesSent < before.BytesSent || after.BytesReceived < before.BytesReceived {
				return loadstand.ErrInvalidSample
			}
			if !add(&sent, after.BytesSent-before.BytesSent) || !add(&received, after.BytesReceived-before.BytesReceived) || !add(&tails, uint64(tail)) {
				return loadstand.ErrInvalidSample
			}
			if stopped {
				if stops == int(^uint(0)>>1) {
					return loadstand.ErrInvalidSample
				}
				stops++
			}
			if life == last {
				break
			}
		}
	}
	if stops > int(^uint(0)>>1)-len(later.Nodes) {
		return loadstand.ErrInvalidSample
	}
	// Same expression as the existing checker, solely to refuse nonrepresentable
	// inputs before its uint64 cast. No altered tolerance or pass threshold.
	allowance := float64(t.BurstFactor)*float64(max(sent, received))*float64(edges.spread)/float64(edges.inner) + float64(t.PerNodeInFlight)*float64(len(later.Nodes)+stops) + float64(tails)
	if math.IsNaN(allowance) || math.IsInf(allowance, 0) || allowance < 0 || math.Ceil(allowance) >= math.Exp2(64) {
		return loadstand.ErrInvalidSample
	}
	return nil
}
func (e *loadStandExecutor) recordByteBalance(laterIndex int, phase string) error {
	tolerance := loadStandDefaultBalanceTolerance()
	later := laterIndex
	check := loadStandByteBalanceCheck{Phase: phase, Later: &later, Tolerance: tolerance}
	if laterIndex == 0 {
		check.Status = "undefined"
		check.Reason = "awaiting_second_edge"
		e.report.ByteBalanceChecks = append(e.report.ByteBalanceChecks, check)
		return nil
	}
	first := 0
	check.Earlier = &first
	earlier, err := e.byteSweep(first)
	if err != nil {
		return err
	}
	current, err := e.byteSweep(laterIndex)
	if err != nil {
		return err
	}
	ended, err := e.byteEnded()
	if err != nil {
		return err
	}
	if err := loadStandByteBalanceRange(earlier, current, ended, tolerance); err != nil {
		return e.byteSourceFailure("", "byte_span_or_range_invalid")
	}
	balance, judgement := checkLoadStandByteBalance(earlier, current, ended, tolerance)
	edges := loadStandEdges{}
	if len(earlier.Nodes) != 0 && len(current.Nodes) != 0 {
		edges = loadStandBalanceEdges(earlier, current)
	}
	if len(earlier.Nodes) != 0 && len(current.Nodes) != 0 && edges.inner > 0 {
		check.Computed = loadStandByteBalanceDTO(balance)
	}
	switch {
	case judgement == nil:
		check.Status = "judged_no_defect"
	case errors.Is(judgement, errLoadStandDefect):
		check.Status = "defect"
		check.Reason = string(loadStandDefectByteBalance)
	case errors.Is(judgement, errLoadStandBalanceUndefined):
		check.Status = "undefined"
		switch {
		case len(earlier.Nodes) == 0 || len(current.Nodes) == 0:
			check.Reason = "empty_network_edge"
		case edges.inner <= 0:
			check.Reason = "overlapping_network_edges"
		case balance.Volume == 0:
			check.Reason = "zero_volume"
		case balance.AllowanceShare > tolerance.MaxAllowanceShare:
			check.Reason = "allowance_blind"
		default:
			return e.byteSourceFailure("", "unclassified_undefined")
		}
	default:
		return e.byteSourceFailure("", "checker_input_invalid")
	}
	e.report.ByteBalanceChecks = append(e.report.ByteBalanceChecks, check)
	if check.Status == "defect" {
		return errors.Join(loadstand.ErrMemberState, judgement)
	}
	return nil
}
func loadStandByteBalanceDTO(balance loadStandBalance) *loadStandBalanceDTO {
	computed := &loadStandBalanceDTO{Nodes: balance.Nodes, Stops: balance.Stops, Sent: balance.Sent, Received: balance.Received, Volume: balance.Volume, Imbalance: balance.Imbalance, Allowance: balance.Allowance}
	if !math.IsInf(float64(balance.AllowanceShare), 0) && !math.IsNaN(float64(balance.AllowanceShare)) {
		share := balance.AllowanceShare
		computed.AllowanceShare = &share
	}
	return computed
}
func (e *loadStandExecutor) reconcileByteBalance() error {
	// Cleanup's original Ended prefixes have just been revalidated. Reconcile
	// only the same actual pair; later final readings do NOT extend its span.
	e.report.ByteBalanceTerminal = "terminal_tail_unjudged"
	if len(e.report.TrafficSweeps) < 2 {
		return nil
	}
	return e.recordByteBalance(len(e.report.TrafficSweeps)-1, "reconcile")
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
		errs = append(errs, e.retainByteEnded(n.id, nil))
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
	params = append(params, loadStandParam("executor_version", "3"), loadStandParam("plan_digest", string(e.plan.Digest())), loadStandParam("profile", "functional-loopback"), loadStandParam("acceptance_eligible", "false"), loadStandParam("action_budget", e.limits.Action.String()), loadStandParam("readiness_budget", e.limits.Readiness.String()), loadStandParam("route_ready_budget", e.limits.RouteReady.String()), loadStandParam("cleanup_budget", e.limits.Cleanup.String()), loadStandParam("receipt_drain", e.limits.ReceiptDrain.String()), loadStandParam("allowed_lateness", e.limits.AllowedLateness.String()), loadStandParam("max_actions", strconv.Itoa(e.limits.MaxActions)), loadStandParam("max_samples", strconv.Itoa(e.limits.MaxSamples)), loadStandParam("max_trace_bytes", strconv.Itoa(e.limits.MaxTraceBytes)))
	if e.c2 != nil {
		params = append(params, e.c2.params()...)
	}
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

func (e *loadStandExecutor) observeGlobals(ctx context.Context, periodic ...int) error {
	if len(periodic) > 1 || (len(periodic) == 1 && (periodic[0] < 0 || periodic[0] >= len(e.report.TrafficSweeps))) {
		return loadstand.ErrInvalidSample
	}
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
		var observedSweep *int
		if len(periodic) == 1 {
			index := periodic[0]
			observedSweep = &index
		}
		want.Observed = true
		e.report.GlobalObservations = append(e.report.GlobalObservations, loadStandGlobalObservation{ObservedSweep: observedSweep, MessageID: actual.ID, Topic: actual.Topic, ClaimedSender: actual.ClaimedSender, BodySHA256: want.BodySHA256, Observer: loadstand.MemberID(want.Observer), ObserverIdentity: want.ObserverIdentity, Incarnation: want.Incarnation, Provenance: loadstand.UnsignedGlobalRuntimeStore})
	}
	return nil
}

// Only this fixed opt-in payload changes body construction. Old t5d bodies,
// publisher authority, scheduler, receipts and default C3 tolerances stay exact.
func (e *loadStandExecutor) globalBody(index int) (loadstand.MessageBody, error) {
	prefix := fmt.Sprintf("t5d/%s/%s/%d", e.plan.Digest(), e.report.ExecutionID, index)
	switch e.plan.gossipPayloadProfile {
	case "":
		return loadstand.MessageBody(prefix), nil
	case loadStandC1Global32K96:
		if len(prefix) > loadStandC1BodyBytes {
			return "", loadstand.ErrUnsupportedPlan
		}
		return loadstand.MessageBody(prefix + strings.Repeat("x", loadStandC1BodyBytes-len(prefix))), nil
	default:
		return "", loadstand.ErrUnsupportedPlan
	}
}
func (e *loadStandExecutor) validateGossipPayload() error {
	profile := e.plan.gossipPayloadProfile
	if profile != "" && profile != loadStandC1Global32K96 {
		return loadstand.ErrUnsupportedPlan
	}
	wanted := map[string]string{"plan_version": e.plan.Version()}
	if profile != "" {
		wanted["gossip_payload_profile"] = string(profile)
		wanted["gossip_body_bytes"] = strconv.Itoa(loadStandC1BodyBytes)
		wanted["gossip_publications"] = strconv.Itoa(loadStandC1Publications)
		wanted["gossip_sender_setup"] = loadStandC1SenderSetup
	}
	seen := map[string]bool{}
	for _, param := range e.plan.Params() {
		name := string(param.Name)
		if value, ok := wanted[name]; ok {
			if seen[name] || string(param.Value) != value {
				return loadstand.ErrUnsupportedPlan
			}
			seen[name] = true
		} else if name == "gossip_payload_profile" || name == "gossip_body_bytes" || name == "gossip_publications" || name == "gossip_sender_setup" {
			return loadstand.ErrUnsupportedPlan
		}
	}
	if len(seen) != len(wanted) {
		return loadstand.ErrUnsupportedPlan
	}
	if profile != "" {
		count := 0
		for _, event := range e.plan.Timeline() {
			if event.kind != loadStandEventGossip {
				return loadstand.ErrUnsupportedPlan
			}
			count++
		}
		if count != loadStandC1Publications {
			return loadstand.ErrUnsupportedPlan
		}
	}
	return nil
}

// Custody comes from successful Start + its retained original process, not a
// caller-supplied role/identity string. No Full missing-BoxSig row is imported.
func (e *loadStandExecutor) c1OriginalPublisher() (loadstand.PublicIdentity, error) {
	var publisher loadstand.PublicIdentity
	full, sdk := 0, 0
	if len(e.plan.nodes) != 3 || len(e.starts) != 3 {
		return publisher, loadstand.ErrMemberState
	}
	for _, n := range e.plan.Nodes() {
		member, ok := e.members[n.id]
		receipt, started := e.starts[n.id]
		if !ok || !started || !member.Owned() || !slices.Contains(e.ownedStarts, n.id) || receipt.Incarnation.Ordinal != 1 || receipt.Incarnation.StartedAt.IsZero() || !e.processes[n.id].Valid() || member.RuntimeProcess() != e.processes[n.id] {
			return publisher, loadstand.ErrMemberState
		}
		switch n.role {
		case loadStandRoleFull:
			if member.Role() != loadstand.MemberFull {
				return publisher, loadstand.ErrMemberState
			}
			full++
		case loadStandRoleEdge:
			if member.Role() != loadstand.MemberSDK || receipt.Public.Identity.IsZero() || receipt.Public.PublicKey == "" || receipt.Public.BoxPublicKey == "" || receipt.Public.BoxSignature == "" {
				return publisher, loadstand.ErrMemberState
			}
			publisher = receipt.Public
			sdk++
		default:
			return publisher, loadstand.ErrMemberState
		}
	}
	if full != 2 || sdk != 1 {
		return loadstand.PublicIdentity{}, loadstand.ErrMemberState
	}
	return publisher, nil
}
