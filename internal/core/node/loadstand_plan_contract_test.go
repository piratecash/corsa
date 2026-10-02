package node

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// TestLoadStandPlan pins the run plan's contract. Every check that reads a
// timeline replays it with its own state machine (walkLoadStandTimeline)
// rather than the generator's availability: a defect in the generator must
// not be able to vouch for itself.
func TestLoadStandPlan(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T){
		"SameSeedAndParamsGiveTheSamePlan":           testLoadStandPlanSameSeedAndParamsGiveTheSamePlan,
		"AnyChangedParameterChangesDigestAndKey":     testLoadStandPlanAnyChangedParameterChangesDigestAndKey,
		"DigestIsPinned":                             testLoadStandPlanDigestIsPinned,
		"TrafficDependsOnTheSeed":                    testLoadStandPlanTrafficDependsOnTheSeed,
		"TopologyHasDeclaredShapeAndNeverDialsEdges": testLoadStandPlanTopologyHasDeclaredShapeAndNeverDialsEdges,
		"ContactsAreDistinctOtherNodes":              testLoadStandPlanContactsAreDistinctOtherNodes,
		"ChurnStopsOnlyOnlineAndStartsOnlyOffline":   testLoadStandPlanChurnStopsOnlyOnlineAndStartsOnlyOffline,
		"TrafficComesOnlyFromOnlineSenders":          testLoadStandPlanTrafficComesOnlyFromOnlineSenders,
		"DirectMessagesGoToContactsIncludingOffline": testLoadStandPlanDirectMessagesGoToContactsIncludingOffline,
		"GossipGoesToTheGlobalTopic":                 testLoadStandPlanGossipGoesToTheGlobalTopic,
		"TimelineIsOrderedWithoutDuplicates":         testLoadStandPlanTimelineIsOrderedWithoutDuplicates,
		"NothingHappensOutsideTheRun":                testLoadStandPlanNothingHappensOutsideTheRun,
		"SamplesEveryInterval":                       testLoadStandPlanSamplesEveryInterval,
		"QuietHasTrafficAndNoChurn":                  testLoadStandPlanQuietHasTrafficAndNoChurn,
		"FlapCyclesTheDeclaredShareOfEdges":          testLoadStandPlanFlapCyclesTheDeclaredShareOfEdges,
		"StormRestartsTheDeclaredShareWithAHub":      testLoadStandPlanStormRestartsTheDeclaredShareWithAHub,
		"AvailabilityRefusesImpossibleTransitions":   testLoadStandPlanAvailabilityRefusesImpossibleTransitions,
		"RejectsImpossibleParameters":                testLoadStandPlanRejectsImpossibleParameters,
		"ParamsFormAValidConfigKey":                  testLoadStandPlanParamsFormAValidConfigKey,
		"AccessorsDoNotExposeThePlan":                testLoadStandPlanAccessorsDoNotExposeThePlan,
		"GossipPublishesEveryInterval":               testLoadStandPlanGossipPublishesEveryInterval,
		"DirectMessageCountFitsTheRate":              testLoadStandPlanDirectMessageCountFitsTheRate,
		"SimultaneousEventsRunChurnBeforeTraffic":    testLoadStandPlanSimultaneousEventsRunChurnBeforeTraffic,
		"RejectsArithmeticOverflow":                  testLoadStandPlanRejectsArithmeticOverflow,
		"DrawsAreUnbiasedAndExact":                   testLoadStandPlanDrawsAreUnbiasedAndExact,
		"DirectMessagesGoOnlyToRolesThatAcceptThem":  testLoadStandPlanDirectMessagesGoOnlyToRolesThatAcceptThem,
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			test(t)
		})
	}
}

var loadStandPlanFixtureProfiles = []loadStandChurnProfile{
	loadStandChurnQuiet,
	loadStandChurnFlap,
	loadStandChurnStorm,
}

var loadStandPlanFixtureSeeds = []loadStandPlanSeed{1, 13, 977}

// loadStandFixtureChurn returns fresh settings on every call, so a test that
// edits a fixture never edits another test's.
var loadStandFixtureChurn = map[loadStandChurnProfile]func() loadStandChurnOpts{
	loadStandChurnQuiet: func() loadStandChurnOpts {
		return loadStandChurnOpts{Profile: loadStandChurnQuiet}
	},
	loadStandChurnFlap: func() loadStandChurnOpts {
		return loadStandChurnOpts{
			Profile: loadStandChurnFlap,
			Flap:    &loadStandFlapOpts{Percent: 50, Up: 40 * time.Second, Down: 25 * time.Second},
		}
	},
	loadStandChurnStorm: func() loadStandChurnOpts {
		return loadStandChurnOpts{
			Profile: loadStandChurnStorm,
			Storm:   &loadStandStormOpts{Percent: 30, Window: 30 * time.Second, Recovery: 60 * time.Second, Rounds: 2},
		}
	},
}

// loadStandPlanFixture is 6 full nodes (2 hubs) and 14 edges: 30% of the 20
// nodes is exactly 6 and 50% of the 14 edges is exactly 7, so the share tests
// below compare against whole numbers and no rounding rule.
func loadStandPlanFixture(profile loadStandChurnProfile, seed loadStandPlanSeed) loadStandPlanOpts {
	return loadStandPlanOpts{
		Seed:           seed,
		FullNodes:      6,
		Hubs:           2,
		EdgeNodes:      14,
		EdgeUplinks:    2,
		Contacts:       4,
		Duration:       4 * time.Minute,
		Settle:         20 * time.Second,
		SampleInterval: 15 * time.Second,
		Churn:          loadStandFixtureChurn[profile](),
		Traffic: loadStandTrafficOpts{
			DirectMessages: &loadStandDMOpts{MeanInterval: 20 * time.Second},
			Gossip:         &loadStandGossipOpts{Interval: 5 * time.Second},
		},
	}
}

func mustBuildLoadStandPlan(t *testing.T, opts loadStandPlanOpts) loadStandPlan {
	t.Helper()

	plan, err := buildLoadStandPlan(opts)
	if err != nil {
		t.Fatalf("buildLoadStandPlan: %v", err)
	}
	return plan
}

// forEachLoadStandPlanFixture runs check on every profile under several
// seeds: a property of the generator holds for any seed, and one seed can
// hide a branch the others reach.
func forEachLoadStandPlanFixture(t *testing.T, check func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan)) {
	t.Helper()

	for _, profile := range loadStandPlanFixtureProfiles {
		for _, seed := range loadStandPlanFixtureSeeds {
			opts := loadStandPlanFixture(profile, seed)
			plan := mustBuildLoadStandPlan(t, opts)
			t.Run(fmt.Sprintf("%s/seed-%d", profile, seed), func(t *testing.T) {
				check(t, opts, plan)
			})
		}
	}
}

// walkLoadStandTimeline replays the plan from "every node online" and shows
// visit each event together with the state it meets — the state after every
// earlier event, before this one.
func walkLoadStandTimeline(plan loadStandPlan, visit func(event loadStandPlanEvent, online map[loadStandNodeID]bool)) {
	online := make(map[loadStandNodeID]bool, len(plan.Nodes()))
	for _, node := range plan.Nodes() {
		online[node.ID()] = true
	}
	stateAfter := map[loadStandEventKind]bool{
		loadStandEventStop:  false,
		loadStandEventStart: true,
	}
	for _, event := range plan.Timeline() {
		visit(event, online)
		if after, isChurn := stateAfter[event.Kind()]; isChurn {
			online[event.Node()] = after
		}
	}
}

func loadStandPlanEventsOfKind(plan loadStandPlan, kinds ...loadStandEventKind) []loadStandPlanEvent {
	var selected []loadStandPlanEvent
	for _, event := range plan.Timeline() {
		if slices.Contains(kinds, event.Kind()) {
			selected = append(selected, event)
		}
	}
	return selected
}

func loadStandPlanNodesByID(plan loadStandPlan) map[loadStandNodeID]loadStandPlanNode {
	byID := make(map[loadStandNodeID]loadStandPlanNode, len(plan.Nodes()))
	for _, node := range plan.Nodes() {
		byID[node.ID()] = node
	}
	return byID
}

func testLoadStandPlanSameSeedAndParamsGiveTheSamePlan(t *testing.T) {
	for _, profile := range loadStandPlanFixtureProfiles {
		first := mustBuildLoadStandPlan(t, loadStandPlanFixture(profile, 13))
		second := mustBuildLoadStandPlan(t, loadStandPlanFixture(profile, 13))
		if first.Digest() != second.Digest() {
			t.Errorf("%s: digests differ for one seed: %s vs %s", profile, first.Digest(), second.Digest())
		}
		if !reflect.DeepEqual(first, second) {
			t.Errorf("%s: two builds from one seed differ", profile)
		}

		other := mustBuildLoadStandPlan(t, loadStandPlanFixture(profile, 14))
		if other.Digest() == first.Digest() {
			t.Errorf("%s: seeds 13 and 14 give one digest %s", profile, first.Digest())
		}
	}
}

// Every parameter is an input of the plan, so changing any one of them must
// change what the journal stores the run under, what it binds the result to,
// AND what the plan schedules. The last check is the one that matters: Params
// are hashed into the digest, so the digest and the key move even when the
// generator ignores a parameter — only the content shows that it did not.
func testLoadStandPlanAnyChangedParameterChangesDigestAndKey(t *testing.T) {
	changes := map[string]func(*loadStandPlanOpts){
		"seed":           func(o *loadStandPlanOpts) { o.Seed++ },
		"full nodes":     func(o *loadStandPlanOpts) { o.FullNodes++ },
		"hubs":           func(o *loadStandPlanOpts) { o.Hubs++ },
		"edge nodes":     func(o *loadStandPlanOpts) { o.EdgeNodes++ },
		"edge uplinks":   func(o *loadStandPlanOpts) { o.EdgeUplinks++ },
		"contacts":       func(o *loadStandPlanOpts) { o.Contacts++ },
		"duration":       func(o *loadStandPlanOpts) { o.Duration += time.Minute },
		"settle":         func(o *loadStandPlanOpts) { o.Settle += time.Second },
		"sample":         func(o *loadStandPlanOpts) { o.SampleInterval += time.Second },
		"storm percent":  func(o *loadStandPlanOpts) { o.Churn.Storm.Percent = 50 },
		"storm window":   func(o *loadStandPlanOpts) { o.Churn.Storm.Window += time.Second },
		"storm recovery": func(o *loadStandPlanOpts) { o.Churn.Storm.Recovery += time.Second },
		"storm rounds":   func(o *loadStandPlanOpts) { o.Churn.Storm.Rounds = 1 },
		"dm interval":    func(o *loadStandPlanOpts) { o.Traffic.DirectMessages.MeanInterval += time.Second },
		"dm off":         func(o *loadStandPlanOpts) { o.Traffic.DirectMessages = nil },
		"gossip":         func(o *loadStandPlanOpts) { o.Traffic.Gossip.Interval += time.Second },
		"gossip off":     func(o *loadStandPlanOpts) { o.Traffic.Gossip = nil },
		"profile": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnFlap]()
		},
	}
	base := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnStorm, 13))
	baseKey := loadStandPlanConfigKey(base)
	for name, change := range changes {
		opts := loadStandPlanFixture(loadStandChurnStorm, 13)
		change(&opts)
		changed := mustBuildLoadStandPlan(t, opts)
		if changed.Digest() == base.Digest() {
			t.Errorf("%s: digest did not change", name)
		}
		if loadStandPlanConfigKey(changed).ID() == baseKey.ID() {
			t.Errorf("%s: config id did not change", name)
		}
		if reflect.DeepEqual(loadStandPlanContentOf(changed), loadStandPlanContentOf(base)) {
			t.Errorf("%s: nodes, timeline and samples did not change — the generator ignores the parameter", name)
		}
	}

	flapBase := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnFlap, 13))
	flapChanges := map[string]func(*loadStandFlapOpts){
		"flap percent": func(f *loadStandFlapOpts) { f.Percent = 100 },
		"flap up":      func(f *loadStandFlapOpts) { f.Up += time.Second },
		"flap down":    func(f *loadStandFlapOpts) { f.Down += time.Second },
	}
	for name, change := range flapChanges {
		opts := loadStandPlanFixture(loadStandChurnFlap, 13)
		change(opts.Churn.Flap)
		changed := mustBuildLoadStandPlan(t, opts)
		if changed.Digest() == flapBase.Digest() || loadStandPlanConfigKey(changed).ID() == loadStandPlanConfigKey(flapBase).ID() {
			t.Errorf("%s: digest or config id did not change", name)
		}
		if reflect.DeepEqual(loadStandPlanContentOf(changed), loadStandPlanContentOf(flapBase)) {
			t.Errorf("%s: nodes, timeline and samples did not change — the generator ignores the parameter", name)
		}
	}
}

// loadStandPlanContent is everything a plan schedules, without the Params
// that merely describe it.
type loadStandPlanContent struct {
	nodes    []loadStandPlanNode
	timeline []loadStandPlanEvent
	samples  []time.Duration
}

func loadStandPlanContentOf(plan loadStandPlan) loadStandPlanContent {
	return loadStandPlanContent{nodes: plan.Nodes(), timeline: plan.Timeline(), samples: plan.Samples()}
}

func loadStandPlanConfigKey(plan loadStandPlan) runjournal.ConfigKey {
	return runjournal.ConfigKey{Measurement: "loadstand-plan-test", Label: "fixture", Params: plan.Params()}
}

// loadStandPinnedPlanDigests are the fixture digests of every plan version.
// The table is append-only: an entry is never edited. A generator change that
// moves a digest bumps loadStandPlanVersion and ADDS that version's entry —
// otherwise the journal, which keys runs by parameters including the version,
// would take a run of the old generator for a run of the new one.
var loadStandPinnedPlanDigests = map[string]map[loadStandChurnProfile]loadStandPlanDigest{
	"1": {
		loadStandChurnQuiet: "5e59a923044bf8c7e1f4b184003c56812a1ccdb9c513dc583291a5778d5278f2",
		loadStandChurnFlap:  "3e3424f754dd2fddb3d645b79153ba0a2ca8cf5c58f99ec7726aad8e174995aa",
		loadStandChurnStorm: "bde59a0cab0d4ea9ddfb8b1958cd4fb18159dd4297498410f4b9ccd08e466b6e",
	},
	// 2: contacts — and so DM recipients — only among roles that accept
	// direct messages (edges of the main profile).
	"2": {
		loadStandChurnQuiet: "6580dbc0074fa68b3eca8ed5599472094106a01877b4e53e8cd99f676cdba8e9",
		loadStandChurnFlap:  "84c8df6117c5dbe4d7c6621083926ca0c2d0adbd34ccbbf913be0906b909ebe9",
		loadStandChurnStorm: "ba5c24980ebb505ea3756d96ab9e35f4bb6c8f9631c0c69d94ebc556737c4938",
	},
}

// The digest binds a journal entry to a plan across machines and Go
// releases. It is drawn only from ChaCha8 (whose output Go guarantees) and
// integer arithmetic, so it must not move; if it moves, every recorded run
// was made under a plan this code no longer builds.
func testLoadStandPlanDigestIsPinned(t *testing.T) {
	pinned, known := loadStandPinnedPlanDigests[loadStandPlanVersion]
	if !known {
		t.Fatalf("plan version %s has no pinned digests: add its entry, never edit an older version's", loadStandPlanVersion)
	}
	for profile, want := range pinned {
		got := mustBuildLoadStandPlan(t, loadStandPlanFixture(profile, 13)).Digest()
		if got != want {
			t.Errorf("%s: digest %s, pinned %s", profile, got, want)
		}
	}
}

// The traffic schedule must come from the seed, not merely sit next to a
// topology that does: a stream that ignored the seed would replay the same
// senders at the same instants under every seed while the digest still
// changed through the contacts.
func testLoadStandPlanTrafficDependsOnTheSeed(t *testing.T) {
	type trafficInstant struct {
		at     time.Duration
		kind   loadStandEventKind
		sender loadStandNodeID
	}
	project := func(plan loadStandPlan) []trafficInstant {
		var instants []trafficInstant
		for _, event := range loadStandPlanEventsOfKind(plan, loadStandEventDirectMessage, loadStandEventGossip) {
			instants = append(instants, trafficInstant{at: event.At(), kind: event.Kind(), sender: event.Node()})
		}
		return instants
	}
	first := project(mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnQuiet, 13)))
	second := project(mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnQuiet, 14)))
	if len(first) == 0 {
		t.Fatal("fixture schedules no traffic")
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal("seeds 13 and 14 schedule the same traffic instants and senders")
	}
}

func testLoadStandPlanTopologyHasDeclaredShapeAndNeverDialsEdges(t *testing.T) {
	forEachLoadStandPlanFixture(t, checkLoadStandTopology)
	// The fixture's uplink count alone would not tell a generator that reads
	// it from one that happens to use the same number.
	for _, uplinks := range []loadStandNodeCount{1, 3, 6} {
		opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
		opts.EdgeUplinks = uplinks
		t.Run(fmt.Sprintf("uplinks-%d", uplinks), func(t *testing.T) {
			checkLoadStandTopology(t, opts, mustBuildLoadStandPlan(t, opts))
		})
	}
}

func checkLoadStandTopology(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
	t.Helper()

	byID := loadStandPlanNodesByID(plan)
	if len(byID) != len(plan.Nodes()) {
		t.Fatalf("%d nodes share %d ids", len(plan.Nodes()), len(byID))
	}
	counts := map[loadStandRole]int{}
	var hubs []loadStandNodeID
	for _, node := range plan.Nodes() {
		counts[node.Role()]++
		if node.IsHub() {
			hubs = append(hubs, node.ID())
		}
	}
	if counts[loadStandRoleFull] != int(opts.FullNodes) || counts[loadStandRoleEdge] != int(opts.EdgeNodes) || len(hubs) != int(opts.Hubs) {
		t.Fatalf("roles %v with hubs %v, want %d full, %d edge, %d hubs", counts, hubs, opts.FullNodes, opts.EdgeNodes, opts.Hubs)
	}
	// An edge is a client: the harness gives it no listener, so it can
	// never be a dial target.
	if loadStandRoleNodeTypes[loadStandRoleEdge] != domain.NodeTypeClient {
		t.Fatal("edge role no longer maps to a listener-less client node")
	}

	for _, node := range plan.Nodes() {
		targets := node.DialTargets()
		for _, target := range targets {
			if byID[target].Role() != loadStandRoleFull {
				t.Errorf("%s dials %s, which is not a full node", node.ID(), target)
			}
			if target == node.ID() {
				t.Errorf("%s dials itself", node.ID())
			}
		}
		if len(slices.Compact(slices.Sorted(slices.Values(targets)))) != len(targets) {
			t.Errorf("%s dials one node twice: %v", node.ID(), targets)
		}
		checkLoadStandUplinks(t, opts, node, byID, hubs)
	}
}

// checkLoadStandUplinks: an edge dials EdgeUplinks full nodes of which at
// least one is a hub; a full node dials every hub but itself.
func checkLoadStandUplinks(t *testing.T, opts loadStandPlanOpts, node loadStandPlanNode, byID map[loadStandNodeID]loadStandPlanNode, hubs []loadStandNodeID) {
	t.Helper()

	targets := node.DialTargets()
	switch node.Role() {
	case loadStandRoleEdge:
		dialsHub := slices.ContainsFunc(targets, func(id loadStandNodeID) bool { return byID[id].IsHub() })
		if len(targets) != int(opts.EdgeUplinks) || !dialsHub {
			t.Errorf("edge %s dials %v, want %d full nodes including a hub", node.ID(), targets, opts.EdgeUplinks)
		}
	case loadStandRoleFull:
		want := slices.DeleteFunc(slices.Clone(hubs), func(id loadStandNodeID) bool { return id == node.ID() })
		if !reflect.DeepEqual(slices.Sorted(slices.Values(targets)), slices.Sorted(slices.Values(want))) {
			t.Errorf("full %s dials %v, want every other hub %v", node.ID(), targets, want)
		}
	default:
		t.Errorf("%s has role %q", node.ID(), node.Role())
	}
}

func testLoadStandPlanContactsAreDistinctOtherNodes(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		byID := loadStandPlanNodesByID(plan)
		for _, node := range plan.Nodes() {
			contacts := node.Contacts()
			if len(contacts) != int(opts.Contacts) {
				t.Errorf("%s has %d contacts, want %d", node.ID(), len(contacts), opts.Contacts)
			}
			if len(slices.Compact(slices.Sorted(slices.Values(contacts)))) != len(contacts) {
				t.Errorf("%s lists a contact twice: %v", node.ID(), contacts)
			}
			for _, contact := range contacts {
				if _, known := byID[contact]; !known || contact == node.ID() {
					t.Errorf("%s has contact %s, which is itself or no node of the plan", node.ID(), contact)
				}
			}
		}
	})
}

func testLoadStandPlanChurnStopsOnlyOnlineAndStartsOnlyOffline(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		requiredState := map[loadStandEventKind]bool{
			loadStandEventStop:  true,
			loadStandEventStart: false,
		}
		walkLoadStandTimeline(plan, func(event loadStandPlanEvent, online map[loadStandNodeID]bool) {
			want, isChurn := requiredState[event.Kind()]
			if isChurn && online[event.Node()] != want {
				t.Errorf("%s %s at %s finds it online=%v", event.Kind(), event.Node(), event.At(), online[event.Node()])
			}
		})
	})
}

func testLoadStandPlanTrafficComesOnlyFromOnlineSenders(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		traffic := 0
		walkLoadStandTimeline(plan, func(event loadStandPlanEvent, online map[loadStandNodeID]bool) {
			if event.Kind() != loadStandEventDirectMessage && event.Kind() != loadStandEventGossip {
				return
			}
			traffic++
			if !online[event.Node()] {
				t.Errorf("%s from %s at %s, which is offline then", event.Kind(), event.Node(), event.At())
			}
		})
		if traffic == 0 {
			t.Fatal("fixture schedules no traffic")
		}
	})
}

// Presence and store-and-forward are only exercised if some messages are
// addressed to a recipient that is down when they are sent.
func testLoadStandPlanDirectMessagesGoToContactsIncludingOffline(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		byID := loadStandPlanNodesByID(plan)
		toOffline := 0
		walkLoadStandTimeline(plan, func(event loadStandPlanEvent, online map[loadStandNodeID]bool) {
			if event.Kind() != loadStandEventDirectMessage {
				return
			}
			recipient, ok := event.Recipient()
			if !ok || !slices.Contains(byID[event.Node()].Contacts(), recipient) {
				t.Errorf("dm from %s at %s goes to %q (ok=%v), not to a contact", event.Node(), event.At(), recipient, ok)
			}
			if !online[recipient] {
				toOffline++
			}
		})
		hasChurn := len(loadStandPlanEventsOfKind(plan, loadStandEventStop)) > 0
		if hasChurn && toOffline == 0 {
			t.Error("no direct message is addressed to an offline recipient")
		}
	})
}

func testLoadStandPlanGossipGoesToTheGlobalTopic(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		gossip := loadStandPlanEventsOfKind(plan, loadStandEventGossip)
		if len(gossip) == 0 {
			t.Fatal("fixture schedules no gossip")
		}
		for _, event := range plan.Timeline() {
			topic, isGossip := event.Topic()
			_, isDM := event.Recipient()
			if isGossip != (event.Kind() == loadStandEventGossip) || isDM != (event.Kind() == loadStandEventDirectMessage) {
				t.Errorf("%s at %s: topic present=%v, recipient present=%v", event.Kind(), event.At(), isGossip, isDM)
			}
			if isGossip && topic != loadStandGlobalTopic {
				t.Errorf("gossip at %s goes to %q", event.At(), topic)
			}
		}
	})
}

func testLoadStandPlanTimelineIsOrderedWithoutDuplicates(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		timeline := plan.Timeline()
		seen := make(map[loadStandPlanEvent]struct{}, len(timeline))
		for i, event := range timeline {
			if i > 0 && event.At() < timeline[i-1].At() {
				t.Fatalf("event %d (%s at %s) precedes event %d at %s", i, event.Kind(), event.At(), i-1, timeline[i-1].At())
			}
			if _, duplicate := seen[event]; duplicate {
				t.Fatalf("%s %s at %s is scheduled twice", event.Kind(), event.Node(), event.At())
			}
			seen[event] = struct{}{}
		}
	})
}

func testLoadStandPlanNothingHappensOutsideTheRun(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		if plan.Duration() != opts.Duration {
			t.Fatalf("plan lasts %s, want %s", plan.Duration(), opts.Duration)
		}
		for _, event := range plan.Timeline() {
			// Nothing but samples is scheduled while the network settles,
			// and a node event at the very end would never be observed.
			if event.At() < opts.Settle || event.At() >= plan.Duration() {
				t.Errorf("%s %s at %s is outside [%s, %s)", event.Kind(), event.Node(), event.At(), opts.Settle, plan.Duration())
			}
		}
		for _, sample := range plan.Samples() {
			if sample <= 0 || sample > plan.Duration() {
				t.Errorf("sample at %s is outside (0, %s]", sample, plan.Duration())
			}
		}
	})
}

func testLoadStandPlanSamplesEveryInterval(t *testing.T) {
	opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
	plan := mustBuildLoadStandPlan(t, opts)
	var want []time.Duration
	for at := opts.SampleInterval; at <= opts.Duration; at += opts.SampleInterval {
		want = append(want, at)
	}
	if got := plan.Samples(); !reflect.DeepEqual(got, want) {
		t.Fatalf("samples %v, want %v", got, want)
	}
}

func testLoadStandPlanQuietHasTrafficAndNoChurn(t *testing.T) {
	plan := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnQuiet, 13))
	if churn := loadStandPlanEventsOfKind(plan, loadStandEventStop, loadStandEventStart); len(churn) != 0 {
		t.Fatalf("quiet schedules %d churn events", len(churn))
	}
	if len(loadStandPlanEventsOfKind(plan, loadStandEventDirectMessage)) == 0 || len(loadStandPlanEventsOfKind(plan, loadStandEventGossip)) == 0 {
		t.Fatal("quiet schedules no direct messages or no gossip")
	}
}

// Flap: exactly the declared share of EDGES goes down for Down and comes back
// for Up, again and again, starting after the network settled.
func testLoadStandPlanFlapCyclesTheDeclaredShareOfEdges(t *testing.T) {
	for _, seed := range loadStandPlanFixtureSeeds {
		opts := loadStandPlanFixture(loadStandChurnFlap, seed)
		flap := *opts.Churn.Flap
		plan := mustBuildLoadStandPlan(t, opts)
		byID := loadStandPlanNodesByID(plan)
		perNode := map[loadStandNodeID][]loadStandPlanEvent{}
		for _, event := range loadStandPlanEventsOfKind(plan, loadStandEventStop, loadStandEventStart) {
			perNode[event.Node()] = append(perNode[event.Node()], event)
		}
		if len(perNode) != 7 {
			t.Errorf("seed %d: %d nodes flap, want 7 (50%% of 14 edges)", seed, len(perNode))
		}
		gaps := map[loadStandEventKind]time.Duration{
			loadStandEventStop:  flap.Down,
			loadStandEventStart: flap.Up,
		}
		for id, events := range perNode {
			if byID[id].Role() != loadStandRoleEdge {
				t.Errorf("seed %d: full node %s flaps", seed, id)
			}
			if events[0].Kind() != loadStandEventStop || events[0].At() >= opts.Settle+flap.Up {
				t.Errorf("seed %d: %s first flaps with %s at %s, want a stop within one Up after settling", seed, id, events[0].Kind(), events[0].At())
			}
			for i := 1; i < len(events); i++ {
				if gap := events[i].At() - events[i-1].At(); gap != gaps[events[i-1].Kind()] {
					t.Errorf("seed %d: %s stays %s for %s, want %s", seed, id, events[i-1].Kind(), gap, gaps[events[i-1].Kind()])
				}
			}
		}
	}
}

// Storm: in each round exactly 30% of all nodes, a hub among them, are
// stopped and started again inside the window, and nothing happens to any
// node during the recovery that follows.
func testLoadStandPlanStormRestartsTheDeclaredShareWithAHub(t *testing.T) {
	for _, seed := range loadStandPlanFixtureSeeds {
		opts := loadStandPlanFixture(loadStandChurnStorm, seed)
		settle, window, recovery := opts.Settle, opts.Churn.Storm.Window, opts.Churn.Storm.Recovery
		rounds := int(opts.Churn.Storm.Rounds)
		plan := mustBuildLoadStandPlan(t, opts)
		byID := loadStandPlanNodesByID(plan)
		churn := loadStandPlanEventsOfKind(plan, loadStandEventStop, loadStandEventStart)
		for round := range rounds {
			begin := settle + time.Duration(round)*(window+recovery)
			stopped := map[loadStandNodeID]int{}
			started := map[loadStandNodeID]int{}
			counted := map[loadStandEventKind]map[loadStandNodeID]int{loadStandEventStop: stopped, loadStandEventStart: started}
			for _, event := range churn {
				if event.At() >= begin+window && event.At() < begin+window+recovery {
					t.Errorf("seed %d: %s %s at %s falls into recovery", seed, event.Kind(), event.Node(), event.At())
				}
				if event.At() < begin || event.At() >= begin+window {
					continue
				}
				counted[event.Kind()][event.Node()]++
			}
			if len(stopped) != 6 || !reflect.DeepEqual(stopped, started) {
				t.Errorf("seed %d round %d: stopped %v, started %v, want the same 6 nodes once each", seed, round, stopped, started)
			}
			hubHit := false
			for id := range stopped {
				hubHit = hubHit || byID[id].IsHub()
			}
			if !hubHit {
				t.Errorf("seed %d round %d: no hub among %v", seed, round, stopped)
			}
		}
		if len(churn) != rounds*6*2 {
			t.Errorf("seed %d: %d churn events, want %d (%d rounds × 6 nodes × stop+start)", seed, len(churn), rounds*6*2, rounds)
		}
	}
}

// The availability built from a churn list is what traffic is gated on; it
// refuses a list a node could not follow, so a future profile that produces
// one fails the build instead of scheduling a message from a stopped node.
func testLoadStandPlanAvailabilityRefusesImpossibleTransitions(t *testing.T) {
	plan := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnQuiet, 13))
	nodes := plan.Nodes()
	edge := nodes[len(nodes)-1].ID()
	other := nodes[len(nodes)-2].ID()
	event := func(at time.Duration, kind loadStandEventKind, node loadStandNodeID) loadStandPlanEvent {
		return loadStandPlanEvent{at: at, kind: kind, node: node}
	}
	refused := map[string][]loadStandPlanEvent{
		"stop of a stopped node": {
			event(time.Second, loadStandEventStop, edge),
			event(2*time.Second, loadStandEventStop, edge),
		},
		"start of a running node": {
			event(time.Second, loadStandEventStart, edge),
		},
		"transition of an unknown node": {
			event(time.Second, loadStandEventStop, "nobody"),
		},
		"traffic in a churn list": {
			event(time.Second, loadStandEventGossip, edge),
		},
		"a node's transitions out of order": {
			event(2*time.Second, loadStandEventStop, edge),
			event(time.Second, loadStandEventStart, edge),
		},
	}
	for name, churn := range refused {
		if _, err := newLoadStandAvailability(nodes, churn); !errors.Is(err, errLoadStandPlanTransition) {
			t.Errorf("%s: newLoadStandAvailability = %v, want errLoadStandPlanTransition", name, err)
		}
	}

	accepted := []loadStandPlanEvent{
		event(time.Second, loadStandEventStop, edge),
		event(time.Second, loadStandEventStop, other),
		event(3*time.Second, loadStandEventStart, edge),
	}
	availability, err := newLoadStandAvailability(nodes, accepted)
	if err != nil {
		t.Fatalf("newLoadStandAvailability(valid) = %v", err)
	}
	wantOnline := map[time.Duration]bool{0: true, time.Second: false, 2 * time.Second: false, 3 * time.Second: true, 4 * time.Second: true}
	for at, want := range wantOnline {
		if got := availability.onlineAt(edge, at); got != want {
			t.Errorf("onlineAt(%s, %s) = %v, want %v", edge, at, got, want)
		}
	}
	if availability.onlineAt(other, time.Hour) {
		t.Errorf("%s is online after a stop it never recovered from", other)
	}
}

func testLoadStandPlanRejectsImpossibleParameters(t *testing.T) {
	storm := func(change func(*loadStandStormOpts)) func(*loadStandPlanOpts) {
		return func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnStorm]()
			change(o.Churn.Storm)
		}
	}
	flap := func(change func(*loadStandFlapOpts)) func(*loadStandPlanOpts) {
		return func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnFlap]()
			change(o.Churn.Flap)
		}
	}
	refused := map[string]func(*loadStandPlanOpts){
		"no full node":                 func(o *loadStandPlanOpts) { o.FullNodes, o.Hubs, o.EdgeUplinks = 0, 0, 0 },
		"no hub":                       func(o *loadStandPlanOpts) { o.Hubs = 0 },
		"more hubs than full nodes":    func(o *loadStandPlanOpts) { o.Hubs = 7 },
		"negative edges":               func(o *loadStandPlanOpts) { o.EdgeNodes = -1 },
		"no uplink":                    func(o *loadStandPlanOpts) { o.EdgeUplinks = 0 },
		"more uplinks than full nodes": func(o *loadStandPlanOpts) { o.EdgeUplinks = 7 },
		"negative contacts":            func(o *loadStandPlanOpts) { o.Contacts = -1 },
		"as many contacts as nodes":    func(o *loadStandPlanOpts) { o.Contacts = 20 },
		// 14 edges accept DMs: an edge has only 13 others to pick from.
		"as many contacts as dm recipients": func(o *loadStandPlanOpts) { o.Contacts = 14 },
		"no duration":                       func(o *loadStandPlanOpts) { o.Duration = 0 },
		"negative settle":                   func(o *loadStandPlanOpts) { o.Settle = -time.Second },
		"settle as long as the run":         func(o *loadStandPlanOpts) { o.Settle = o.Duration },
		"no sample interval":                func(o *loadStandPlanOpts) { o.SampleInterval = 0 },
		"sample interval beyond run":        func(o *loadStandPlanOpts) { o.SampleInterval = o.Duration + time.Second },
		"duration below a millisecond":      func(o *loadStandPlanOpts) { o.Duration += time.Microsecond },
		"unknown profile":                   func(o *loadStandPlanOpts) { o.Churn = loadStandChurnOpts{Profile: "hurricane"} },
		"no profile":                        func(o *loadStandPlanOpts) { o.Churn = loadStandChurnOpts{} },
		"quiet with flap settings":          func(o *loadStandPlanOpts) { o.Churn.Flap = loadStandFixtureChurn[loadStandChurnFlap]().Flap },
		"flap without settings":             func(o *loadStandPlanOpts) { o.Churn = loadStandChurnOpts{Profile: loadStandChurnFlap} },
		"flap with storm settings": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnFlap]()
			o.Churn.Storm = loadStandFixtureChurn[loadStandChurnStorm]().Storm
		},
		"flap of no percent":            flap(func(f *loadStandFlapOpts) { f.Percent = 0 }),
		"flap above 100 percent":        flap(func(f *loadStandFlapOpts) { f.Percent = 101 }),
		"flap without up":               flap(func(f *loadStandFlapOpts) { f.Up = 0 }),
		"flap without down":             flap(func(f *loadStandFlapOpts) { f.Down = 0 }),
		"flap below a millisecond":      flap(func(f *loadStandFlapOpts) { f.Down += time.Nanosecond }),
		"storm without settings":        func(o *loadStandPlanOpts) { o.Churn = loadStandChurnOpts{Profile: loadStandChurnStorm} },
		"storm of no node":              storm(func(s *loadStandStormOpts) { s.Percent = 1 }),
		"storm above 100 percent":       storm(func(s *loadStandStormOpts) { s.Percent = 101 }),
		"storm window too short":        storm(func(s *loadStandStormOpts) { s.Window = time.Millisecond }),
		"storm without recovery":        storm(func(s *loadStandStormOpts) { s.Recovery = 0 }),
		"storm of no round":             storm(func(s *loadStandStormOpts) { s.Rounds = 0 }),
		"storm rounds beyond the run":   storm(func(s *loadStandStormOpts) { s.Rounds = 3 }),
		"storm window below a ms":       storm(func(s *loadStandStormOpts) { s.Window += time.Microsecond }),
		"dm without contacts":           func(o *loadStandPlanOpts) { o.Contacts = 0 },
		"dm faster than the resolution": func(o *loadStandPlanOpts) { o.Traffic.DirectMessages.MeanInterval = 50 * time.Millisecond },
		"gossip without interval":       func(o *loadStandPlanOpts) { o.Traffic.Gossip.Interval = 0 },
		"gossip below a millisecond":    func(o *loadStandPlanOpts) { o.Traffic.Gossip.Interval = time.Microsecond },
		"flap selecting no edge":        flap(func(f *loadStandFlapOpts) { f.Percent = 3 }),
		"settle below a millisecond":    func(o *loadStandPlanOpts) { o.Settle += time.Microsecond },
		"sample below a millisecond":    func(o *loadStandPlanOpts) { o.SampleInterval += time.Microsecond },
		"flap up below a millisecond":   flap(func(f *loadStandFlapOpts) { f.Up += time.Nanosecond }),
		"storm recovery below a ms":     storm(func(s *loadStandStormOpts) { s.Recovery += time.Microsecond }),
		"dm just below the resolution": func(o *loadStandPlanOpts) {
			o.Traffic.DirectMessages.MeanInterval = loadStandTrafficResolution - time.Nanosecond
		},
		"quiet with storm settings": func(o *loadStandPlanOpts) { o.Churn.Storm = loadStandFixtureChurn[loadStandChurnStorm]().Storm },
		"storm with flap settings": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnStorm]()
			o.Churn.Flap = loadStandFixtureChurn[loadStandChurnFlap]().Flap
		},
	}
	for name, change := range refused {
		opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
		change(&opts)
		if _, err := buildLoadStandPlan(opts); !errors.Is(err, errLoadStandPlanInvalidOpts) {
			t.Errorf("%s: buildLoadStandPlan = %v, want errLoadStandPlanInvalidOpts", name, err)
		}
	}

	// Each boundary is accepted exactly at the value its rule allows, so a
	// rule that drifts by one unit in either direction turns one side red.
	accepted := map[string]func(*loadStandPlanOpts){
		// No traffic and no contacts measures an idle network.
		"idle": func(o *loadStandPlanOpts) {
			o.Contacts = 0
			o.Traffic = loadStandTrafficOpts{}
		},
		"dm at exactly the resolution":             func(o *loadStandPlanOpts) { o.Traffic.DirectMessages.MeanInterval = loadStandTrafficResolution },
		"storm window of two units":                storm(func(s *loadStandStormOpts) { s.Window = 2 * loadStandPlanTimeUnit }),
		"flap selecting one edge":                  flap(func(f *loadStandFlapOpts) { f.Percent = 4 }),
		"storm rounds filling the run":             storm(func(s *loadStandStormOpts) { s.Rounds, s.Recovery = 2, 80*time.Second }),
		"one contact fewer than the dm recipients": func(o *loadStandPlanOpts) { o.Contacts = 13 },
	}
	for name, change := range accepted {
		opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
		change(&opts)
		if _, err := buildLoadStandPlan(opts); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}

// Every duration is bounded before it is added or multiplied: a sum that
// wraps past int64 is negative, passes "fits inside the run", and leaves the
// schedule loops counting toward a bound they never reach. validate is
// called directly so a regression fails here instead of hanging the build.
func testLoadStandPlanRejectsArithmeticOverflow(t *testing.T) {
	huge := time.Duration(math.MaxInt64).Truncate(loadStandPlanTimeUnit)
	stormCycle := 90 * time.Second
	refused := map[string]func(*loadStandPlanOpts){
		"duration at the int64 limit": func(o *loadStandPlanOpts) { o.Duration = huge },
		"duration beyond the cap":     func(o *loadStandPlanOpts) { o.Duration = loadStandMaxPlanDuration + loadStandPlanTimeUnit },
		"flap up beyond the run": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnFlap]()
			o.Churn.Flap.Up = huge
		},
		"flap down beyond the run": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnFlap]()
			o.Churn.Flap.Down = huge
		},
		"storm window beyond the run": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnStorm]()
			o.Churn.Storm.Window = huge
		},
		"storm recovery beyond the run": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnStorm]()
			o.Churn.Storm.Recovery = huge
		},
		"storm rounds wrapping the product": func(o *loadStandPlanOpts) {
			o.Churn = loadStandFixtureChurn[loadStandChurnStorm]()
			o.Churn.Storm.Rounds = loadStandRoundCount(math.MaxInt64/int64(stormCycle) + 1)
		},
		"gossip interval beyond the run": func(o *loadStandPlanOpts) { o.Traffic.Gossip.Interval = huge },
	}
	for name, change := range refused {
		opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
		change(&opts)
		if err := opts.validate(); !errors.Is(err, errLoadStandPlanInvalidOpts) {
			t.Errorf("%s: validate = %v, want errLoadStandPlanInvalidOpts", name, err)
		}
	}

	atCap := loadStandPlanFixture(loadStandChurnQuiet, 13)
	atCap.Duration = loadStandMaxPlanDuration
	if err := atCap.validate(); err != nil {
		t.Errorf("a run of exactly the cap is refused: %v", err)
	}
}

// Gossip is one publication per interval from Settle on; in quiet every
// tick finds a node up, so every tick publishes.
func testLoadStandPlanGossipPublishesEveryInterval(t *testing.T) {
	for _, interval := range []time.Duration{5 * time.Second, 7 * time.Second, 31 * time.Second} {
		opts := loadStandPlanFixture(loadStandChurnQuiet, 13)
		opts.Traffic.Gossip.Interval = interval
		plan := mustBuildLoadStandPlan(t, opts)

		var want, got []time.Duration
		for at := opts.Settle + interval; at < opts.Duration; at += interval {
			want = append(want, at)
		}
		for _, event := range loadStandPlanEventsOfKind(plan, loadStandEventGossip) {
			got = append(got, event.At())
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("interval %s: gossip at %v, want %v", interval, got, want)
		}
	}
}

// In quiet every node is up, so the number of direct messages is a draw from
// a binomial: one trial per node per traffic slot, succeeding with
// probability resolution/MeanInterval. The plan is deterministic, so the
// count of each (interval, seed) is pinned exactly — that is what catches a
// generator using a slightly different rate. The binomial bound (five
// standard deviations) only vouches that the pinned numbers are what such a
// process produces, so a pin cannot be re-recorded from a generator that
// ignores MeanInterval altogether.
func testLoadStandPlanDirectMessageCountFitsTheRate(t *testing.T) {
	pinned := map[time.Duration]map[loadStandPlanSeed]int{
		5 * time.Second:  {1: 863, 13: 837, 977: 864},
		20 * time.Second: {1: 240, 13: 207, 977: 222},
		80 * time.Second: {1: 54, 13: 55, 977: 55},
	}
	for mean, counts := range pinned {
		for seed, want := range counts {
			opts := loadStandPlanFixture(loadStandChurnQuiet, seed)
			opts.Traffic.DirectMessages.MeanInterval = mean
			plan := mustBuildLoadStandPlan(t, opts)

			slots := 0
			for at := opts.Settle + loadStandTrafficResolution; at < opts.Duration; at += loadStandTrafficResolution {
				slots++
			}
			trials := float64(slots * len(plan.Nodes()))
			p := float64(loadStandTrafficResolution) / float64(mean)
			expected, deviation := trials*p, math.Sqrt(trials*p*(1-p))
			if math.Abs(float64(want)-expected) > 5*deviation {
				t.Errorf("mean %s seed %d: pinned %d is not a binomial draw around %.0f ± %.0f", mean, seed, want, expected, 5*deviation)
			}
			if got := len(loadStandPlanEventsOfKind(plan, loadStandEventDirectMessage)); got != want {
				t.Errorf("mean %s seed %d: %d direct messages, pinned %d", mean, seed, got, want)
			}
		}
	}
}

// Within one instant the executor applies churn before traffic, and the
// generator judges a sender at an instant by the state after that instant's
// churn (onlineAt counts transitions AT the instant). The sort is what makes
// the two agree; with traffic first, a node started at t would send at t
// before it runs.
func testLoadStandPlanSimultaneousEventsRunChurnBeforeTraffic(t *testing.T) {
	const at = 30 * time.Second
	earlier := loadStandPlanEvent{at: at - loadStandPlanTimeUnit, kind: loadStandEventGossip, node: "full-001", topic: loadStandGlobalTopic}
	gossip := loadStandPlanEvent{at: at, kind: loadStandEventGossip, node: "full-000", topic: loadStandGlobalTopic}
	dmToThree := loadStandPlanEvent{at: at, kind: loadStandEventDirectMessage, node: "edge-001", recipient: "edge-003"}
	dmToTwo := loadStandPlanEvent{at: at, kind: loadStandEventDirectMessage, node: "edge-001", recipient: "edge-002"}
	start := loadStandPlanEvent{at: at, kind: loadStandEventStart, node: "edge-004"}
	stop := loadStandPlanEvent{at: at, kind: loadStandEventStop, node: "edge-005"}

	events := []loadStandPlanEvent{gossip, dmToThree, dmToTwo, start, stop, earlier}
	sortLoadStandEvents(events)
	want := []loadStandPlanEvent{earlier, stop, start, dmToTwo, dmToThree, gossip}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("sorted %v, want %v", events, want)
	}
}

func testLoadStandPlanParamsFormAValidConfigKey(t *testing.T) {
	var keys []runjournal.ConfigKey
	for _, profile := range loadStandPlanFixtureProfiles {
		for _, seed := range loadStandPlanFixtureSeeds {
			keys = append(keys, loadStandPlanConfigKey(mustBuildLoadStandPlan(t, loadStandPlanFixture(profile, seed))))
		}
	}
	if err := runjournal.RequireDistinct(keys); err != nil {
		t.Fatalf("plan params do not identify distinct runs: %v", err)
	}

	storm := keys[len(keys)-1]
	want := map[runjournal.ParamName]runjournal.ParamValue{
		"churn":            "storm",
		"seed":             "977",
		"storm_percent":    "30",
		"storm_window":     "30s",
		"storm_recovery":   "1m0s",
		"storm_rounds":     "2",
		"dm_mean_interval": "20s",
		"gossip_interval":  "5s",
		"gossip_topic":     "global",
	}
	for name, value := range want {
		if got, ok := storm.Param(name); !ok || got != value {
			t.Errorf("param %s = %q (present=%v), want %q", name, got, ok, value)
		}
	}
	if _, ok := storm.Param("flap_percent"); ok {
		t.Error("a storm plan declares flap parameters it does not use")
	}
}

// The plan is handed to one executor as a value; nothing it returns may let
// a caller change what the next caller reads, or the digest would describe a
// plan nobody runs.
func testLoadStandPlanAccessorsDoNotExposeThePlan(t *testing.T) {
	plan := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnFlap, 13))
	pristine := mustBuildLoadStandPlan(t, loadStandPlanFixture(loadStandChurnFlap, 13))

	plan.Nodes()[0] = loadStandPlanNode{}
	// The executor lives in this package and can reach the fields, so the
	// copy must be deep, not only the accessors' own clones.
	plan.Nodes()[0].dialTargets[0] = "tampered"
	plan.Nodes()[1].contacts[0] = "tampered"
	plan.Nodes()[0].DialTargets()[0] = "tampered"
	plan.Nodes()[len(plan.Nodes())-1].Contacts()[0] = "tampered"
	plan.Timeline()[0] = loadStandPlanEvent{}
	plan.Samples()[0] = time.Hour
	plan.Params()[0].Value = "tampered"

	if !reflect.DeepEqual(plan, pristine) {
		t.Fatal("writing through an accessor's result changed the plan")
	}
	if plan.computeDigest() != plan.Digest() {
		t.Fatal("the plan no longer hashes to its own digest")
	}
}

// loadStandScriptedSource replays fixed values, so the draws can be checked
// at the exact edges of their rejection rule. Running out of values is a
// test failure (index out of range): a draw consumed more than it should.
type loadStandScriptedSource struct {
	values []uint64
	drawn  int
}

func (s *loadStandScriptedSource) Uint64() uint64 {
	value := s.values[s.drawn]
	s.drawn++
	return value
}

// The draws are the plan's only randomness and are written here rather than
// taken from math/rand, so they carry their own proof: uniform rejects
// exactly the values at and above the largest multiple of the bound, chance
// is exactly numerator in denominator, and pickDistinct is a partial
// Fisher–Yates.
func testLoadStandPlanDrawsAreUnbiasedAndExact(t *testing.T) {
	const bound = 6
	limit := uint64(math.MaxUint64) - uint64(math.MaxUint64)%bound
	uniform := map[string]struct {
		values []uint64
		want   uint64
		drawn  int
	}{
		"accepts zero":                       {values: []uint64{0}, want: 0, drawn: 1},
		"accepts the last value below limit": {values: []uint64{limit - 1}, want: (limit - 1) % bound, drawn: 1},
		"redraws the limit and above":        {values: []uint64{math.MaxUint64, limit, limit - 1}, want: (limit - 1) % bound, drawn: 3},
	}
	for name, c := range uniform {
		source := &loadStandScriptedSource{values: c.values}
		draws := &loadStandDraws{source: source}
		if got := draws.uniform(bound); got != c.want || source.drawn != c.drawn {
			t.Errorf("uniform %s: %d after %d draws, want %d after %d", name, got, source.drawn, c.want, c.drawn)
		}
	}

	chances := map[uint64]bool{0: true, 1: false, 199: false, 200: true, 201: false}
	for value, want := range chances {
		draws := &loadStandDraws{source: &loadStandScriptedSource{values: []uint64{value}}}
		if got := draws.chance(1, 200); got != want {
			t.Errorf("chance(1/200) on %d = %v, want %v", value, got, want)
		}
	}

	// Draw 2 swaps position 0 with 0+2%3=2: [c b a]; draw 1 swaps position
	// 1 with 1+1%2=2: [c a b].
	draws := &loadStandDraws{source: &loadStandScriptedSource{values: []uint64{2, 1}}}
	if got := draws.pickDistinct([]loadStandNodeID{"a", "b", "c"}, 2); !reflect.DeepEqual(got, []loadStandNodeID{"c", "a"}) {
		t.Errorf("pickDistinct = %v, want [c a]", got)
	}
}

// A contact is somebody a node messages, so every contact — and with it every
// DM recipient — has a role that accepts direct messages. A full node of the
// main profile refuses DMs addressed to it: a plan that picked it would turn
// part of the traffic into delivery failures and change what 13b measures.
// Full nodes still SEND: the opt-out gates only inbound DMs.
func testLoadStandPlanDirectMessagesGoOnlyToRolesThatAcceptThem(t *testing.T) {
	forEachLoadStandPlanFixture(t, func(t *testing.T, opts loadStandPlanOpts, plan loadStandPlan) {
		byID := loadStandPlanNodesByID(plan)
		for _, node := range plan.Nodes() {
			for _, contact := range node.Contacts() {
				if !loadStandRoleAcceptsDirectMessages[byID[contact].Role()] {
					t.Errorf("%s has contact %s of role %s, which refuses direct messages", node.ID(), contact, byID[contact].Role())
				}
			}
		}
		fullSenders := 0
		for _, event := range loadStandPlanEventsOfKind(plan, loadStandEventDirectMessage) {
			recipient, _ := event.Recipient()
			if !loadStandRoleAcceptsDirectMessages[byID[recipient].Role()] {
				t.Errorf("dm from %s at %s goes to %s of role %s, which refuses direct messages", event.Node(), event.At(), recipient, byID[recipient].Role())
			}
			if byID[event.Node()].Role() == loadStandRoleFull {
				fullSenders++
			}
		}
		if fullSenders == 0 {
			t.Error("no full node sends a direct message; refusing to receive must not stop a node from sending")
		}
	})
}
