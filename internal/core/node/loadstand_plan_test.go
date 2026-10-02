package node

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// A load-stand plan is everything a run will do, decided before any node
// starts: which nodes exist and whom they dial, whom each one knows, when
// each node stops and starts, when each one sends, and when the stand
// samples. It is a pure function of loadStandPlanOpts — the seed included —
// so a run can be named by its parameters in the journal and bound to the
// exact schedule it executed by the plan's digest.
//
// The plan is an immutable value: its fields are unexported and every
// accessor returns a copy. Its executor is the stand's one goroutine; the
// plan itself starts nothing.
//
// Every random choice is drawn from ChaCha8, whose output Go guarantees
// across releases, through integer arithmetic only. Neither math/rand's
// bounded helpers (whose algorithms a release may change) nor floating point
// (which an architecture may fuse differently) is used, so the same
// parameters produce the same digest on every machine.

var (
	errLoadStandPlanInvalidOpts = errors.New("invalid load stand plan parameters")
	// errLoadStandPlanTransition is a churn schedule a node could not
	// follow: stopping a stopped node, starting a running one, or a node's
	// transitions out of time order.
	errLoadStandPlanTransition = errors.New("load stand plan schedules a transition the node cannot make")
)

// loadStandPlanSeed is the only source of randomness of a plan.
type loadStandPlanSeed uint64

type loadStandNodeCount int

type loadStandContactCount int

// loadStandPercent is a share in whole percent, 0..100.
type loadStandPercent int

type loadStandRoundCount int

// loadStandChurnProfile names how nodes come and go during a run.
type loadStandChurnProfile string

const (
	// loadStandChurnQuiet — every node stays up for the whole run.
	loadStandChurnQuiet loadStandChurnProfile = "quiet"
	// loadStandChurnFlap — a share of the edges goes down and comes back
	// again and again, each on a fixed cycle with its own phase.
	loadStandChurnFlap loadStandChurnProfile = "flap"
	// loadStandChurnStorm — in each round a share of ALL nodes, a hub among
	// them, restarts within a short window, followed by a quiet recovery.
	loadStandChurnStorm loadStandChurnProfile = "storm"
)

// loadStandTopic is a gossip topic the stand publishes to. The project has
// no domain type for topics; this one exists so a topic cannot be confused
// with any other string of the plan.
type loadStandTopic string

const loadStandGlobalTopic loadStandTopic = "global"

// loadStandPlanDigest is hex sha256 over the plan's canonical form.
type loadStandPlanDigest string

// loadStandEventKind is declared in the order the executor applies the
// events of one instant, and that order IS the sort key: churn first, so
// traffic at the instant a node stops or starts is judged against the state
// after it — the state onlineAt reports. The zero value is no kind, so an
// event built without one never passes for a stop.
type loadStandEventKind uint8

const (
	loadStandEventStop loadStandEventKind = iota + 1
	loadStandEventStart
	loadStandEventDirectMessage
	loadStandEventGossip
)

// loadStandEventKindNames are the names the digest and the logs use.
var loadStandEventKindNames = map[loadStandEventKind]string{
	loadStandEventStop:          "stop",
	loadStandEventStart:         "start",
	loadStandEventDirectMessage: "dm",
	loadStandEventGossip:        "gossip",
}

func (k loadStandEventKind) String() string {
	if name, known := loadStandEventKindNames[k]; known {
		return name
	}
	return fmt.Sprintf("invalid(%d)", uint8(k))
}

// loadStandPlanTimeUnit is the granularity of every scheduled instant.
// Durations finer than it are refused rather than rounded, so a parameter
// always means what it says.
const loadStandPlanTimeUnit = time.Millisecond

// loadStandMaxPlanDuration caps a run. The stand's scenarios last minutes;
// the cap is what lets every later sum of durations stay far from int64.
const loadStandMaxPlanDuration = 24 * time.Hour

// loadStandTrafficResolution is the slot of the direct-message process: in
// each slot every node sends with probability resolution/MeanInterval — a
// Bernoulli process, the integer-only form of a Poisson stream. At the
// stand's intervals (tens of seconds) the two are indistinguishable.
const loadStandTrafficResolution = 100 * time.Millisecond

// loadStandPlanVersion changes whenever the same parameters start producing
// a different plan, so a journal keyed on parameters does not take a run of
// the old generator for one of the new.
const loadStandPlanVersion = "2"

const (
	loadStandPlanDigestDomain = "corsa/node/loadstand/plan/v1\x00"
	loadStandDrawsDomain      = "corsa/node/loadstand/draws/v1\x00"
)

// loadStandPlanOpts are every input of a plan. Hubs are the first Hubs full
// nodes; every other full node differs from a hub only in whom it dials.
type loadStandPlanOpts struct {
	Seed      loadStandPlanSeed
	FullNodes loadStandNodeCount
	Hubs      loadStandNodeCount
	EdgeNodes loadStandNodeCount
	// EdgeUplinks is how many distinct full nodes each edge dials; one of
	// them is always a hub, so every edge can reach the core directly.
	EdgeUplinks loadStandNodeCount
	// Contacts is how many other nodes each node knows (import_contacts):
	// the identities its presence probing and identity lookups run over,
	// and the only recipients of its direct messages — so contacts are drawn
	// only among roles that accept direct messages.
	Contacts loadStandContactCount
	Duration time.Duration
	// Settle is how long the network converges before churn and traffic
	// begin; samples run from the start.
	Settle         time.Duration
	SampleInterval time.Duration
	Churn          loadStandChurnOpts
	Traffic        loadStandTrafficOpts
}

// loadStandChurnOpts carries the settings of exactly the named profile; a
// profile's settings are nil when it is not the profile, so an unused value
// never sits in the opts looking as if it mattered.
type loadStandChurnOpts struct {
	Profile loadStandChurnProfile
	Flap    *loadStandFlapOpts
	Storm   *loadStandStormOpts
}

type loadStandFlapOpts struct {
	// Percent of the edges flap.
	Percent loadStandPercent
	// Up is how long a flapping edge stays up between outages; its first
	// outage begins at a random point within the first Up after Settle.
	Up time.Duration
	// Down is how long each outage lasts.
	Down time.Duration
}

type loadStandStormOpts struct {
	// Percent of all nodes restart in each round, rounded half up.
	Percent loadStandPercent
	// Window is when a round's nodes stop and start again.
	Window time.Duration
	// Recovery follows each window with no churn at all.
	Recovery time.Duration
	Rounds   loadStandRoundCount
}

// loadStandTrafficOpts: nil means that kind of traffic is not generated.
type loadStandTrafficOpts struct {
	DirectMessages *loadStandDMOpts
	Gossip         *loadStandGossipOpts
}

type loadStandDMOpts struct {
	// MeanInterval is each node's mean time between direct messages.
	MeanInterval time.Duration
}

type loadStandGossipOpts struct {
	// Interval is the network-wide period of global-topic messages, each
	// from one node picked among those up at that instant.
	Interval time.Duration
}

type loadStandPlan struct {
	params   []runjournal.Param
	duration time.Duration
	nodes    []loadStandPlanNode
	timeline []loadStandPlanEvent
	samples  []time.Duration
	digest   loadStandPlanDigest
}

type loadStandPlanNode struct {
	id          loadStandNodeID
	role        loadStandRole
	hub         bool
	dialTargets []loadStandNodeID
	contacts    []loadStandNodeID
}

// loadStandPlanEvent is one node event: a stop, a start, or a message sent
// by the node. recipient is meaningful only for a direct message and topic
// only for gossip; the accessors say which.
type loadStandPlanEvent struct {
	at        time.Duration
	kind      loadStandEventKind
	node      loadStandNodeID
	recipient loadStandNodeID
	topic     loadStandTopic
}

// buildLoadStandPlan validates opts and derives the plan from them.
func buildLoadStandPlan(opts loadStandPlanOpts) (loadStandPlan, error) {
	if err := opts.validate(); err != nil {
		return loadStandPlan{}, err
	}
	nodes := layoutLoadStandTopology(opts)
	churn := loadStandChurnProfiles[opts.Churn.Profile].schedule(opts, nodes)
	sortLoadStandEvents(churn)
	availability, err := newLoadStandAvailability(nodes, churn)
	if err != nil {
		return loadStandPlan{}, err
	}
	timeline := slices.Concat(
		churn,
		scheduleLoadStandDirectMessages(opts, nodes, availability),
		scheduleLoadStandGossip(opts, nodes, availability),
	)
	sortLoadStandEvents(timeline)

	plan := loadStandPlan{
		params:   opts.params(),
		duration: opts.Duration,
		nodes:    nodes,
		timeline: timeline,
		samples:  scheduleLoadStandSamples(opts),
	}
	plan.digest = plan.computeDigest()
	return plan, nil
}

// --- validation ---

// loadStandPlanRule is one requirement on the opts and the words that name
// it when broken.
type loadStandPlanRule struct {
	holds     bool
	violation string
}

func firstBrokenLoadStandPlanRule(rules []loadStandPlanRule) error {
	for _, rule := range rules {
		if !rule.holds {
			return fmt.Errorf("%w: %s", errLoadStandPlanInvalidOpts, rule.violation)
		}
	}
	return nil
}

func (o loadStandPlanOpts) validate() error {
	err := firstBrokenLoadStandPlanRule([]loadStandPlanRule{
		{o.FullNodes >= 1, "a plan needs at least one full node"},
		{o.Hubs >= 1 && o.Hubs <= o.FullNodes, "hubs must number between one and the full nodes"},
		{o.EdgeNodes >= 0, "edge nodes cannot be negative"},
		{o.EdgeUplinks >= 1 && o.EdgeUplinks <= o.FullNodes, "edge uplinks must number between one and the full nodes"},
		{o.Contacts >= 0 && (o.Contacts == 0 || int(o.Contacts) < o.directMessageRecipients()), "contacts must be fewer than the nodes that accept direct messages"},
		{o.Duration > 0 && o.Duration <= loadStandMaxPlanDuration, "duration must be positive and within the cap"},
		{o.Settle >= 0 && o.Settle < o.Duration, "settle must fit inside the run"},
		{o.SampleInterval > 0 && o.SampleInterval <= o.Duration, "sample interval must fit inside the run"},
		{loadStandWholeUnits(o.Duration, o.Settle, o.SampleInterval), "durations must be whole milliseconds"},
	})
	if err != nil {
		return err
	}
	profile, known := loadStandChurnProfiles[o.Churn.Profile]
	if !known {
		return fmt.Errorf("%w: unknown churn profile %q", errLoadStandPlanInvalidOpts, o.Churn.Profile)
	}
	if err := profile.validate(o); err != nil {
		return err
	}
	return o.validateTraffic()
}

// directMessageRecipients counts the plan's nodes whose role accepts direct
// messages — the pool contacts are drawn from.
func (o loadStandPlanOpts) directMessageRecipients() int {
	counts := map[loadStandRole]int{
		loadStandRoleFull: int(o.FullNodes),
		loadStandRoleEdge: int(o.EdgeNodes),
	}
	recipients := 0
	for role, count := range counts {
		if loadStandRoleAcceptsDirectMessages[role] {
			recipients += count
		}
	}
	return recipients
}

func (o loadStandPlanOpts) validateTraffic() error {
	rules := []loadStandPlanRule{}
	if dm := o.Traffic.DirectMessages; dm != nil {
		rules = append(rules,
			loadStandPlanRule{o.Contacts >= 1, "direct messages need contacts to go to"},
			loadStandPlanRule{dm.MeanInterval >= loadStandTrafficResolution, "direct-message mean interval is below the traffic resolution"},
		)
	}
	if gossip := o.Traffic.Gossip; gossip != nil {
		rules = append(rules, loadStandPlanRule{
			gossip.Interval > 0 && gossip.Interval <= o.Duration && loadStandWholeUnits(gossip.Interval),
			"gossip interval must be a whole number of milliseconds inside the run",
		})
	}
	return firstBrokenLoadStandPlanRule(rules)
}

func loadStandWholeUnits(durations ...time.Duration) bool {
	for _, duration := range durations {
		if duration%loadStandPlanTimeUnit != 0 {
			return false
		}
	}
	return true
}

// loadStandShareOf is percent of count, rounded half up.
func loadStandShareOf(count int, percent loadStandPercent) int {
	return (count*int(percent) + 50) / 100
}

// --- churn profiles ---

// loadStandChurnProfileSpec is everything that differs between profiles.
type loadStandChurnProfileSpec struct {
	validate func(loadStandPlanOpts) error
	schedule func(loadStandPlanOpts, []loadStandPlanNode) []loadStandPlanEvent
	params   func(loadStandChurnOpts) []runjournal.Param
}

var loadStandChurnProfiles = map[loadStandChurnProfile]loadStandChurnProfileSpec{
	loadStandChurnQuiet: {
		validate: validateLoadStandQuiet,
		schedule: func(loadStandPlanOpts, []loadStandPlanNode) []loadStandPlanEvent { return nil },
		params:   func(loadStandChurnOpts) []runjournal.Param { return nil },
	},
	loadStandChurnFlap: {
		validate: validateLoadStandFlap,
		schedule: scheduleLoadStandFlap,
		params:   loadStandFlapParams,
	},
	loadStandChurnStorm: {
		validate: validateLoadStandStorm,
		schedule: scheduleLoadStandStorm,
		params:   loadStandStormParams,
	},
}

func validateLoadStandQuiet(o loadStandPlanOpts) error {
	return firstBrokenLoadStandPlanRule([]loadStandPlanRule{
		{o.Churn.Flap == nil && o.Churn.Storm == nil, "the quiet profile takes no churn settings"},
	})
}

func validateLoadStandFlap(o loadStandPlanOpts) error {
	if o.Churn.Flap == nil || o.Churn.Storm != nil {
		return fmt.Errorf("%w: the flap profile takes flap settings and only them", errLoadStandPlanInvalidOpts)
	}
	flap := *o.Churn.Flap
	return firstBrokenLoadStandPlanRule([]loadStandPlanRule{
		{flap.Percent >= 1 && flap.Percent <= 100, "flap percent must be between 1 and 100"},
		{loadStandShareOf(int(o.EdgeNodes), flap.Percent) >= 1, "flap percent selects no edge"},
		{flap.Up > 0 && flap.Down > 0, "flap up and down must be positive"},
		{flap.Up <= o.Duration && flap.Down <= o.Duration, "flap up and down must fit inside the run"},
		{loadStandWholeUnits(flap.Up, flap.Down), "flap durations must be whole milliseconds"},
	})
}

func validateLoadStandStorm(o loadStandPlanOpts) error {
	if o.Churn.Storm == nil || o.Churn.Flap != nil {
		return fmt.Errorf("%w: the storm profile takes storm settings and only them", errLoadStandPlanInvalidOpts)
	}
	storm := *o.Churn.Storm
	totalNodes := int(o.FullNodes) + int(o.EdgeNodes)
	err := firstBrokenLoadStandPlanRule([]loadStandPlanRule{
		{storm.Percent >= 1 && storm.Percent <= 100, "storm percent must be between 1 and 100"},
		{loadStandShareOf(totalNodes, storm.Percent) >= 1, "storm percent selects no node"},
		// A stop and a later start of one node need two distinct instants.
		{storm.Window >= 2*loadStandPlanTimeUnit, "storm window must hold a stop and a later start"},
		{storm.Recovery > 0, "storm recovery must be positive"},
		{storm.Window <= o.Duration && storm.Recovery <= o.Duration, "storm window and recovery must fit inside the run"},
		{storm.Rounds >= 1, "a storm needs at least one round"},
		{loadStandWholeUnits(storm.Window, storm.Recovery), "storm durations must be whole milliseconds"},
	})
	if err != nil {
		return err
	}
	// Settle + Rounds*(Window+Recovery) <= Duration, asked by division: the
	// cycle is bounded above, but Rounds is not, and the product can wrap.
	cycle := storm.Window + storm.Recovery
	return firstBrokenLoadStandPlanRule([]loadStandPlanRule{
		{int64(storm.Rounds) <= int64((o.Duration-o.Settle)/cycle), "storm rounds with their recoveries do not fit inside the run"},
	})
}

// scheduleLoadStandFlap: each flapping edge alternates stop (then Down) and
// start (then Up) from a random phase within its first Up after Settle.
func scheduleLoadStandFlap(o loadStandPlanOpts, nodes []loadStandPlanNode) []loadStandPlanEvent {
	flap := *o.Churn.Flap
	edges := loadStandNodeIDsWhere(nodes, func(node loadStandPlanNode) bool { return node.role == loadStandRoleEdge })
	draws := newLoadStandDraws(o.Seed, loadStandDrawFlap)
	flapping := draws.pickDistinct(edges, loadStandShareOf(len(edges), flap.Percent))
	cycle := [...]struct {
		kind loadStandEventKind
		next time.Duration
	}{
		{loadStandEventStop, flap.Down},
		{loadStandEventStart, flap.Up},
	}

	var events []loadStandPlanEvent
	for _, id := range flapping {
		at := o.Settle + draws.instantWithin(flap.Up)
		for step := 0; at < o.Duration; step++ {
			phase := cycle[step%len(cycle)]
			events = append(events, loadStandPlanEvent{at: at, kind: phase.kind, node: id})
			at += phase.next
		}
	}
	return events
}

// scheduleLoadStandStorm: in each round one hub and enough other nodes to
// make the declared share each stop and start again inside the window.
func scheduleLoadStandStorm(o loadStandPlanOpts, nodes []loadStandPlanNode) []loadStandPlanEvent {
	storm := *o.Churn.Storm
	all := loadStandNodeIDsWhere(nodes, func(loadStandPlanNode) bool { return true })
	hubs := loadStandNodeIDsWhere(nodes, func(node loadStandPlanNode) bool { return node.hub })
	affected := loadStandShareOf(len(all), storm.Percent)
	draws := newLoadStandDraws(o.Seed, loadStandDrawStorm)

	var events []loadStandPlanEvent
	for round := range int(storm.Rounds) {
		begin := o.Settle + time.Duration(round)*(storm.Window+storm.Recovery)
		hub := hubs[draws.below(len(hubs))]
		others := loadStandIDsWithout(all, hub)
		for _, id := range append([]loadStandNodeID{hub}, draws.pickDistinct(others, affected-1)...) {
			stopAt, startAt := draws.orderedInstantsWithin(storm.Window)
			events = append(events,
				loadStandPlanEvent{at: begin + stopAt, kind: loadStandEventStop, node: id},
				loadStandPlanEvent{at: begin + startAt, kind: loadStandEventStart, node: id},
			)
		}
	}
	return events
}

// --- topology ---

func loadStandFullNodeID(index int) loadStandNodeID {
	return loadStandNodeID(fmt.Sprintf("full-%03d", index))
}

func loadStandEdgeNodeID(index int) loadStandNodeID {
	return loadStandNodeID(fmt.Sprintf("edge-%03d", index))
}

// layoutLoadStandTopology: full nodes first, then edges, in index order.
// Every full node dials every other hub; every edge dials one random hub and
// EdgeUplinks-1 other random full nodes. Nothing ever dials an edge — it has
// no listener.
func layoutLoadStandTopology(o loadStandPlanOpts) []loadStandPlanNode {
	nodes := make([]loadStandPlanNode, 0, int(o.FullNodes)+int(o.EdgeNodes))
	var fulls, hubs []loadStandNodeID
	for i := range int(o.FullNodes) {
		id := loadStandFullNodeID(i)
		fulls = append(fulls, id)
		if i < int(o.Hubs) {
			hubs = append(hubs, id)
		}
	}
	for i, id := range fulls {
		nodes = append(nodes, loadStandPlanNode{
			id:          id,
			role:        loadStandRoleFull,
			hub:         i < int(o.Hubs),
			dialTargets: loadStandIDsWithout(hubs, id),
		})
	}

	uplinks := newLoadStandDraws(o.Seed, loadStandDrawUplinks)
	for i := range int(o.EdgeNodes) {
		hub := hubs[uplinks.below(len(hubs))]
		others := loadStandIDsWithout(fulls, hub)
		nodes = append(nodes, loadStandPlanNode{
			id:          loadStandEdgeNodeID(i),
			role:        loadStandRoleEdge,
			dialTargets: append([]loadStandNodeID{hub}, uplinks.pickDistinct(others, int(o.EdgeUplinks)-1)...),
		})
	}

	assignLoadStandContacts(o, nodes)
	return nodes
}

func assignLoadStandContacts(o loadStandPlanOpts, nodes []loadStandPlanNode) {
	recipients := loadStandNodeIDsWhere(nodes, func(node loadStandPlanNode) bool {
		return loadStandRoleAcceptsDirectMessages[node.role]
	})
	draws := newLoadStandDraws(o.Seed, loadStandDrawContacts)
	for i := range nodes {
		others := loadStandIDsWithout(recipients, nodes[i].id)
		contacts := draws.pickDistinct(others, int(o.Contacts))
		slices.Sort(contacts)
		nodes[i].contacts = contacts
	}
}

// loadStandIDsWithout is a copy of ids without excluded; ids is untouched.
func loadStandIDsWithout(ids []loadStandNodeID, excluded loadStandNodeID) []loadStandNodeID {
	return slices.DeleteFunc(slices.Clone(ids), func(id loadStandNodeID) bool { return id == excluded })
}

func loadStandNodeIDsWhere(nodes []loadStandPlanNode, keep func(loadStandPlanNode) bool) []loadStandNodeID {
	var ids []loadStandNodeID
	for _, node := range nodes {
		if keep(node) {
			ids = append(ids, node.id)
		}
	}
	return ids
}

// --- availability ---

// loadStandAvailability answers "is this node up at t" for a churn schedule.
// Every node starts the run up; one with no transitions stays up.
type loadStandAvailability struct {
	// transitions holds each node's churn events in time order.
	transitions map[loadStandNodeID][]loadStandPlanEvent
}

// loadStandTransitionRequires is the state each churn kind requires of its
// node: a stop only of a node that is up, a start only of one that is down.
var loadStandTransitionRequires = map[loadStandEventKind]bool{
	loadStandEventStop:  true,
	loadStandEventStart: false,
}

// newLoadStandAvailability replays churn from "every node up" and refuses a
// schedule the nodes could not follow. churn must be in time order.
func newLoadStandAvailability(nodes []loadStandPlanNode, churn []loadStandPlanEvent) (loadStandAvailability, error) {
	online := make(map[loadStandNodeID]bool, len(nodes))
	for _, node := range nodes {
		online[node.id] = true
	}
	transitions := make(map[loadStandNodeID][]loadStandPlanEvent)
	for _, event := range churn {
		if err := checkLoadStandTransition(online, transitions[event.node], event); err != nil {
			return loadStandAvailability{}, err
		}
		online[event.node] = event.kind == loadStandEventStart
		transitions[event.node] = append(transitions[event.node], event)
	}
	return loadStandAvailability{transitions: transitions}, nil
}

func checkLoadStandTransition(online map[loadStandNodeID]bool, earlier []loadStandPlanEvent, event loadStandPlanEvent) error {
	requiresOnline, isChurn := loadStandTransitionRequires[event.kind]
	isOnline, known := online[event.node]
	outOfOrder := len(earlier) > 0 && event.at <= earlier[len(earlier)-1].at
	switch {
	case !isChurn:
		return fmt.Errorf("%w: %s of %s at %s is not a churn event", errLoadStandPlanTransition, event.kind, event.node, event.at)
	case !known:
		return fmt.Errorf("%w: %s of unknown node %s at %s", errLoadStandPlanTransition, event.kind, event.node, event.at)
	case outOfOrder:
		return fmt.Errorf("%w: %s of %s at %s is not after its previous transition", errLoadStandPlanTransition, event.kind, event.node, event.at)
	case isOnline != requiresOnline:
		return fmt.Errorf("%w: %s of %s at %s finds it online=%v", errLoadStandPlanTransition, event.kind, event.node, event.at, isOnline)
	default:
		return nil
	}
}

// onlineAt is the node's state after every transition at or before at.
func (a loadStandAvailability) onlineAt(id loadStandNodeID, at time.Duration) bool {
	transitions := a.transitions[id]
	applied := sort.Search(len(transitions), func(i int) bool { return transitions[i].at > at })
	return applied == 0 || transitions[applied-1].kind == loadStandEventStart
}

// --- traffic and samples ---

// scheduleLoadStandDirectMessages draws every slot of every node whether or
// not the node is up, then drops what an offline node would have sent. The
// draws therefore do not depend on churn: one node's traffic under storm is
// its traffic under quiet minus its outages. Recipients are drawn from the
// contacts whatever their state — messages to offline nodes are the point.
func scheduleLoadStandDirectMessages(o loadStandPlanOpts, nodes []loadStandPlanNode, availability loadStandAvailability) []loadStandPlanEvent {
	dm := o.Traffic.DirectMessages
	if dm == nil {
		return nil
	}
	var events []loadStandPlanEvent
	for _, node := range nodes {
		draws := newLoadStandDraws(o.Seed, loadStandDrawDirectMessages(node.id))
		for at := o.Settle + loadStandTrafficResolution; at < o.Duration; at += loadStandTrafficResolution {
			if !draws.chance(loadStandTrafficResolution, dm.MeanInterval) {
				continue
			}
			recipient := node.contacts[draws.below(len(node.contacts))]
			if availability.onlineAt(node.id, at) {
				events = append(events, loadStandPlanEvent{at: at, kind: loadStandEventDirectMessage, node: node.id, recipient: recipient})
			}
		}
	}
	return events
}

// scheduleLoadStandGossip publishes once per interval from a node picked
// among those up at that instant; an instant with no node up publishes
// nothing.
func scheduleLoadStandGossip(o loadStandPlanOpts, nodes []loadStandPlanNode, availability loadStandAvailability) []loadStandPlanEvent {
	gossip := o.Traffic.Gossip
	if gossip == nil {
		return nil
	}
	draws := newLoadStandDraws(o.Seed, loadStandDrawGossip)
	var events []loadStandPlanEvent
	for at := o.Settle + gossip.Interval; at < o.Duration; at += gossip.Interval {
		online := loadStandNodeIDsWhere(nodes, func(node loadStandPlanNode) bool { return availability.onlineAt(node.id, at) })
		if len(online) == 0 {
			continue
		}
		publisher := online[draws.below(len(online))]
		events = append(events, loadStandPlanEvent{at: at, kind: loadStandEventGossip, node: publisher, topic: loadStandGlobalTopic})
	}
	return events
}

func scheduleLoadStandSamples(o loadStandPlanOpts) []time.Duration {
	var samples []time.Duration
	for at := o.SampleInterval; at <= o.Duration; at += o.SampleInterval {
		samples = append(samples, at)
	}
	return samples
}

// sortLoadStandEvents puts events in the one order the executor runs them:
// by instant, then churn before traffic, then by node and recipient, so even
// simultaneous events have a single order.
func sortLoadStandEvents(events []loadStandPlanEvent) {
	slices.SortFunc(events, func(a, b loadStandPlanEvent) int {
		return cmp.Or(
			cmp.Compare(a.at, b.at),
			cmp.Compare(a.kind, b.kind),
			cmp.Compare(a.node, b.node),
			cmp.Compare(a.recipient, b.recipient),
		)
	})
}

// --- draws ---

// loadStandDrawPurpose names one random stream of a plan.
type loadStandDrawPurpose string

const (
	loadStandDrawUplinks  loadStandDrawPurpose = "topology/uplinks"
	loadStandDrawContacts loadStandDrawPurpose = "topology/contacts"
	loadStandDrawFlap     loadStandDrawPurpose = "churn/flap"
	loadStandDrawStorm    loadStandDrawPurpose = "churn/storm"
	loadStandDrawGossip   loadStandDrawPurpose = "traffic/gossip"
)

// loadStandDrawDirectMessages is one stream per sender, so a node's traffic
// does not depend on how many draws the nodes before it made.
func loadStandDrawDirectMessages(sender loadStandNodeID) loadStandDrawPurpose {
	return loadStandDrawPurpose("traffic/dm/" + string(sender))
}

// loadStandDraws is one deterministic random stream. Each purpose has its
// own stream derived from the seed, so adding draws to one part of the plan
// never shifts the choices of another. The source is an interface only so a
// test can script it; a plan always draws from ChaCha8.
type loadStandDraws struct {
	source rand.Source
}

func newLoadStandDraws(seed loadStandPlanSeed, purpose loadStandDrawPurpose) *loadStandDraws {
	material := []byte(loadStandDrawsDomain)
	material = binary.BigEndian.AppendUint64(material, uint64(seed))
	material = append(material, purpose...)
	return &loadStandDraws{source: rand.NewChaCha8(sha256.Sum256(material))}
}

// uniform is uniform in [0, bound), bound > 0, by rejection: the values at
// the top of the uint64 range that would favour small results are drawn
// again.
func (d *loadStandDraws) uniform(bound uint64) uint64 {
	limit := math.MaxUint64 - math.MaxUint64%bound
	for {
		if value := d.source.Uint64(); value < limit {
			return value % bound
		}
	}
}

// below is uniform in [0, n), n > 0 — an index into n candidates.
func (d *loadStandDraws) below(n int) int {
	return int(d.uniform(uint64(n)))
}

// chance is true with probability numerator/denominator. It stays in uint64
// because a mean interval in nanoseconds overflows a 32-bit int.
func (d *loadStandDraws) chance(numerator, denominator time.Duration) bool {
	return d.uniform(uint64(denominator)) < uint64(numerator)
}

// instantWithin is a uniform whole-unit offset in [0, span).
func (d *loadStandDraws) instantWithin(span time.Duration) time.Duration {
	return time.Duration(d.uniform(uint64(span/loadStandPlanTimeUnit))) * loadStandPlanTimeUnit
}

// orderedInstantsWithin is a uniform pair of distinct whole-unit offsets in
// [0, span), earlier first.
func (d *loadStandDraws) orderedInstantsWithin(span time.Duration) (time.Duration, time.Duration) {
	first := d.instantWithin(span)
	second := d.instantWithin(span - loadStandPlanTimeUnit)
	if second >= first {
		second += loadStandPlanTimeUnit
	}
	return min(first, second), max(first, second)
}

// pickDistinct is k distinct elements of candidates in draw order (a partial
// Fisher–Yates over a copy).
func (d *loadStandDraws) pickDistinct(candidates []loadStandNodeID, k int) []loadStandNodeID {
	pool := slices.Clone(candidates)
	for i := range k {
		j := i + d.below(len(pool)-i)
		pool[i], pool[j] = pool[j], pool[i]
	}
	return pool[:k:k]
}

// --- parameters and digest ---

func loadStandParam(name runjournal.ParamName, value string) runjournal.Param {
	return runjournal.Param{Name: name, Value: runjournal.ParamValue(value)}
}

// loadStandOptionalParam renders an optional setting as "off" when absent,
// so "not generated" is a value of the key rather than a missing parameter.
func loadStandOptionalParam(name runjournal.ParamName, value *time.Duration) runjournal.Param {
	if value == nil {
		return loadStandParam(name, "off")
	}
	return loadStandParam(name, value.String())
}

// params are the plan's inputs as journal parameters — every value that
// changes the plan, including the generator's own constants.
func (o loadStandPlanOpts) params() []runjournal.Param {
	var dmInterval, gossipInterval *time.Duration
	if o.Traffic.DirectMessages != nil {
		dmInterval = &o.Traffic.DirectMessages.MeanInterval
	}
	if o.Traffic.Gossip != nil {
		gossipInterval = &o.Traffic.Gossip.Interval
	}
	params := []runjournal.Param{
		loadStandParam("plan_version", loadStandPlanVersion),
		loadStandParam("seed", strconv.FormatUint(uint64(o.Seed), 10)),
		loadStandParam("full_nodes", strconv.Itoa(int(o.FullNodes))),
		loadStandParam("hubs", strconv.Itoa(int(o.Hubs))),
		loadStandParam("edge_nodes", strconv.Itoa(int(o.EdgeNodes))),
		loadStandParam("edge_uplinks", strconv.Itoa(int(o.EdgeUplinks))),
		loadStandParam("contacts", strconv.Itoa(int(o.Contacts))),
		loadStandParam("duration", o.Duration.String()),
		loadStandParam("settle", o.Settle.String()),
		loadStandParam("sample_interval", o.SampleInterval.String()),
		loadStandParam("churn", string(o.Churn.Profile)),
		loadStandOptionalParam("dm_mean_interval", dmInterval),
		loadStandParam("traffic_resolution", loadStandTrafficResolution.String()),
		loadStandOptionalParam("gossip_interval", gossipInterval),
		loadStandParam("gossip_topic", string(loadStandGlobalTopic)),
	}
	return append(params, loadStandChurnProfiles[o.Churn.Profile].params(o.Churn)...)
}

func loadStandFlapParams(churn loadStandChurnOpts) []runjournal.Param {
	return []runjournal.Param{
		loadStandParam("flap_percent", strconv.Itoa(int(churn.Flap.Percent))),
		loadStandParam("flap_up", churn.Flap.Up.String()),
		loadStandParam("flap_down", churn.Flap.Down.String()),
	}
}

func loadStandStormParams(churn loadStandChurnOpts) []runjournal.Param {
	return []runjournal.Param{
		loadStandParam("storm_percent", strconv.Itoa(int(churn.Storm.Percent))),
		loadStandParam("storm_window", churn.Storm.Window.String()),
		loadStandParam("storm_recovery", churn.Storm.Recovery.String()),
		loadStandParam("storm_rounds", strconv.Itoa(int(churn.Storm.Rounds))),
	}
}

// loadStandCanonicalWriter length-prefixes every field, so no value can
// shift a field boundary and two different plans cannot share a form.
type loadStandCanonicalWriter struct {
	out strings.Builder
}

func (w *loadStandCanonicalWriter) field(tag string, value string) {
	w.out.WriteString(tag)
	w.out.WriteString(strconv.Itoa(len(value)))
	w.out.WriteByte(':')
	w.out.WriteString(value)
	w.out.WriteByte('\n')
}

func (w *loadStandCanonicalWriter) count(tag string, n int) {
	w.field(tag, strconv.Itoa(n))
}

func (w *loadStandCanonicalWriter) ids(tag string, ids []loadStandNodeID) {
	w.count(tag, len(ids))
	for _, id := range ids {
		w.field("i", string(id))
	}
}

// computeDigest hashes everything the plan holds. Scheduled instants are
// written in nanoseconds; the params are hashed as the journal stores them,
// i.e. with durations as Duration.String renders them — so a change in that
// rendering moves the digest and the config key together, never one alone.
func (p loadStandPlan) computeDigest() loadStandPlanDigest {
	var w loadStandCanonicalWriter
	w.count("params", len(p.params))
	for _, param := range p.params {
		w.field("k", string(param.Name))
		w.field("v", string(param.Value))
	}
	w.field("duration", strconv.FormatInt(int64(p.duration), 10))
	w.count("nodes", len(p.nodes))
	for _, node := range p.nodes {
		w.field("id", string(node.id))
		w.field("role", string(node.role))
		w.field("hub", strconv.FormatBool(node.hub))
		w.ids("dials", node.dialTargets)
		w.ids("contacts", node.contacts)
	}
	w.count("events", len(p.timeline))
	for _, event := range p.timeline {
		w.field("at", strconv.FormatInt(int64(event.at), 10))
		w.field("kind", event.kind.String())
		w.field("node", string(event.node))
		w.field("to", string(event.recipient))
		w.field("topic", string(event.topic))
	}
	w.count("samples", len(p.samples))
	for _, sample := range p.samples {
		w.field("s", strconv.FormatInt(int64(sample), 10))
	}
	sum := sha256.Sum256([]byte(loadStandPlanDigestDomain + w.out.String()))
	return loadStandPlanDigest(hex.EncodeToString(sum[:]))
}

// --- accessors ---

// Params are the plan's inputs, for runjournal.ConfigKey.Params.
func (p loadStandPlan) Params() []runjournal.Param { return slices.Clone(p.params) }

func (p loadStandPlan) Duration() time.Duration { return p.duration }

// Nodes are full nodes first, then edges, each in index order — the order a
// stand that builds them sequentially starts them in.
//
// The copy is deep: the executor lives in this package and can reach a
// node's slices directly.
func (p loadStandPlan) Nodes() []loadStandPlanNode {
	nodes := make([]loadStandPlanNode, len(p.nodes))
	for i, node := range p.nodes {
		nodes[i] = node.clone()
	}
	return nodes
}

func (n loadStandPlanNode) clone() loadStandPlanNode {
	n.dialTargets = slices.Clone(n.dialTargets)
	n.contacts = slices.Clone(n.contacts)
	return n
}

// Timeline is every node event in execution order.
func (p loadStandPlan) Timeline() []loadStandPlanEvent { return slices.Clone(p.timeline) }

// Samples are the instants the stand takes a sample at.
func (p loadStandPlan) Samples() []time.Duration { return slices.Clone(p.samples) }

// Digest binds a journal record to this exact plan.
func (p loadStandPlan) Digest() loadStandPlanDigest { return p.digest }

func (n loadStandPlanNode) ID() loadStandNodeID { return n.id }

func (n loadStandPlanNode) Role() loadStandRole { return n.role }

func (n loadStandPlanNode) IsHub() bool { return n.hub }

// DialTargets are the full nodes this node bootstraps to.
func (n loadStandPlanNode) DialTargets() []loadStandNodeID { return slices.Clone(n.dialTargets) }

// Contacts are the nodes whose identities this node imports.
func (n loadStandPlanNode) Contacts() []loadStandNodeID { return slices.Clone(n.contacts) }

func (e loadStandPlanEvent) At() time.Duration { return e.at }

func (e loadStandPlanEvent) Kind() loadStandEventKind { return e.kind }

// Node is the node that stops, starts, or sends.
func (e loadStandPlanEvent) Node() loadStandNodeID { return e.node }

// Recipient is the addressee of a direct message; false for any other kind.
func (e loadStandPlanEvent) Recipient() (loadStandNodeID, bool) {
	return e.recipient, e.kind == loadStandEventDirectMessage
}

// Topic is the topic of a gossip message; false for any other kind.
func (e loadStandPlanEvent) Topic() (loadStandTopic, bool) {
	return e.topic, e.kind == loadStandEventGossip
}
