package node

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestPresenceFrameCarriesStateSourceAndReason checks the shape that crosses
// local RPC. The source is what lets the interface tell a proven presence from
// an inferred one, so a frame that drops it would silently collapse the two
// back together — the exact regression this work exists to prevent.
func TestPresenceFrameCarriesStateSourceAndReason(t *testing.T) {
	proven := presenceTestContact(1)
	inferred := presenceTestContact(2)
	suppressed := presenceTestContact(3)

	svc := &Service{presenceProjector: newPresenceProjector()}
	svc.presenceSnap.Store(&presenceSnapshot{set: domain.PresenceSet{
		proven:     domain.OnlinePresence(domain.PresenceSourceProof),
		inferred:   domain.OnlinePresence(domain.PresenceSourceRouteFallback),
		suppressed: domain.UnknownPresence(domain.PresenceUnknownRouteSuppressedLocally),
	}})

	frame := svc.presenceFrame()
	if frame.Type != "presence" {
		t.Fatalf("frame type: got %q", frame.Type)
	}
	if len(frame.Presence) != 3 {
		t.Fatalf("frame rows: got %d, want 3", len(frame.Presence))
	}

	rows := make(map[string]protocol.PresenceFrame, len(frame.Presence))
	for _, row := range frame.Presence {
		rows[row.Identity] = row
	}

	if got := rows[proven.String()]; got.State != "online" || got.Source != "proof" {
		t.Fatalf("proven row: got state=%q source=%q", got.State, got.Source)
	}
	if got := rows[inferred.String()]; got.State != "online" || got.Source != "route_fallback" {
		t.Fatalf("inferred row: got state=%q source=%q", got.State, got.Source)
	}
	row := rows[suppressed.String()]
	if row.State != "unknown" || row.Reason != "route_suppressed_locally" {
		t.Fatalf("suppressed row: got state=%q reason=%q", row.State, row.Reason)
	}
}

// TestPresenceFrameSurvivesTheRoundTrip: every name the node emits must parse
// back to the value it came from. A typo in either direction would decode to
// unknown/none and be invisible until somebody wondered why nothing is ever
// green.
func TestPresenceFrameSurvivesTheRoundTrip(t *testing.T) {
	cases := []domain.Presence{
		domain.OnlinePresence(domain.PresenceSourceProof),
		domain.OnlinePresence(domain.PresenceSourcePassive),
		domain.OnlinePresence(domain.PresenceSourceRouteFallback),
		domain.OfflinePresence(domain.PresenceSourceSessionClosed),
		domain.OfflinePresence(domain.PresenceSourceRouteObservation),
		domain.OfflinePresence(domain.PresenceSourceProbeTimeout),
		domain.ProbingPresence(),
		domain.UnknownPresence(domain.PresenceUnknownNoLocalConnectivity),
		domain.UnknownPresence(domain.PresenceUnknownRouteSuppressedLocally),
		domain.UnknownPresence(domain.PresenceUnknownStale),
		domain.UnknownPresence(domain.PresenceUnknownNotProbeable),
	}

	for _, want := range cases {
		t.Run(want.String(), func(t *testing.T) {
			contact := presenceTestContact(7)
			svc := &Service{presenceProjector: newPresenceProjector()}
			svc.presenceSnap.Store(&presenceSnapshot{set: domain.PresenceSet{contact: want}})

			frame := svc.presenceFrame()
			if len(frame.Presence) != 1 {
				t.Fatalf("rows: got %d", len(frame.Presence))
			}
			row := frame.Presence[0]

			got := domain.Presence{
				State:  domain.ParsePresenceState(row.State),
				Source: domain.ParsePresenceSource(row.Source),
				Reason: domain.ParsePresenceUnknownReason(row.Reason),
			}
			if got.State != want.State || got.Source != want.Source {
				t.Fatalf("round trip: got %s, want %s", got, want)
			}
			if want.State == domain.PresenceUnknown && got.Reason != want.Reason {
				t.Fatalf("round trip reason: got %s, want %s", got.Reason, want.Reason)
			}
		})
	}
}

// TestPresenceSnapshotIsCallerOwned: the interface merges this into a cached
// status and mutates its own copy. Handing out the live map would let a reader
// observe a half-applied generation.
func TestPresenceSnapshotIsCallerOwned(t *testing.T) {
	contact := presenceTestContact(4)
	svc := &Service{presenceProjector: newPresenceProjector()}
	svc.presenceSnap.Store(&presenceSnapshot{set: domain.PresenceSet{
		contact: domain.OnlinePresence(domain.PresenceSourceProof),
	}})

	first := svc.PresenceSnapshot()
	first[contact] = domain.OfflinePresence(domain.PresenceSourceProbeTimeout)

	if got := svc.PresenceSnapshot().Get(contact).State; got != domain.PresenceOnline {
		t.Fatalf("mutating a returned snapshot changed the published one: got %s", got)
	}
}

// TestPresenceSnapshotBeforeFirstPassIsNil pins the difference between "no
// answer yet" and "everyone is offline". A node that has not projected once
// must not be readable as a full set of absences.
func TestPresenceSnapshotBeforeFirstPassIsNil(t *testing.T) {
	svc := &Service{presenceProjector: newPresenceProjector()}
	if got := svc.PresenceSnapshot(); got != nil {
		t.Fatalf("presence before the first projection: got %v, want nil", got)
	}
}

// TestUnknownWireNamesDecodeToUnknown: a state or source added by a newer node
// must not be guessed at by an older reader. Unknown is the only safe landing
// place, and none is the only safe source — both read as "not proven", so a
// name we cannot interpret can never be drawn as evidence.
func TestUnknownWireNamesDecodeToUnknown(t *testing.T) {
	if got := domain.ParsePresenceState("teleporting"); got != domain.PresenceUnknown {
		t.Fatalf("unknown state name: got %s, want unknown", got)
	}
	if got := domain.ParsePresenceSource("astrology"); got != domain.PresenceSourceNone {
		t.Fatalf("unknown source name: got %s, want none", got)
	}
	if domain.OnlinePresence(domain.ParsePresenceSource("astrology")).IsProven() {
		t.Fatal("an uninterpretable source must never read as proven")
	}
}

// TestPresenceProberAnswerIsNotStolenFromTheResolver is the ingest contract.
//
// The prober and the identity resolver share the post_identity dtype and are
// told apart by whose label the answer carries. The prober is asked first, so
// it MUST decline anything that is not its own — otherwise it would swallow a
// resolution's reply and that lookup would hang until it timed out.
func TestPresenceProberAnswerIsNotStolenFromTheResolver(t *testing.T) {
	svc := &Service{presenceProjector: newPresenceProjector()}
	prober := newPresenceProber(svc)

	var foreign domain.PeerIdentity
	copy(foreign[:], []byte("a-label-the-prober-never-issued!"))

	if prober.HandleAnswer(datagram.NewLabel(foreign), []byte(`{"v":1}`)) {
		t.Fatal("the prober consumed an answer it never asked for")
	}
}

// TestPresenceArrivalsAreOnlyTheNewOnes: the delivery bell is rung for
// contacts who BECAME present, never for everyone who happens to be present.
// Ringing it every pass for the whole address book would turn a wakeup into a
// poll — the exact thing the centralized trigger replaces.
func TestPresenceArrivalsAreOnlyTheNewOnes(t *testing.T) {
	stayed := presenceTestContact(1)
	arrived := presenceTestContact(2)
	left := presenceTestContact(3)

	previous := domain.PresenceSet{
		stayed:  domain.OnlinePresence(domain.PresenceSourceProof),
		arrived: domain.OfflinePresence(domain.PresenceSourceRouteObservation),
		left:    domain.OnlinePresence(domain.PresenceSourceProof),
	}
	current := domain.PresenceSet{
		stayed:  domain.OnlinePresence(domain.PresenceSourceProof),
		arrived: domain.OnlinePresence(domain.PresenceSourceProof),
		left:    domain.OfflinePresence(domain.PresenceSourceProbeTimeout),
	}

	got := presenceArrivals(previous, current)
	if len(got) != 1 {
		t.Fatalf("arrivals: got %d, want 1", len(got))
	}
	if _, ok := got[arrived]; !ok {
		t.Fatal("the contact who became present is not in the arrivals")
	}
}

// TestProvingAnAssumedPresenceIsAnArrival is the case the whole centralized
// trigger exists for, and the first version missed it.
//
// A contact whose route never went away is `online` by the route fallback the
// entire time they are gone — a route outlives its owner by up to ten minutes.
// Comparing states alone, their real return is `online → online`: no arrival,
// no wake. Routing sees no transition either, for exactly the same reason, so
// the mechanism that would otherwise catch it is blind to the same case. The
// proof is the only event that marks the moment.
func TestProvingAnAssumedPresenceIsAnArrival(t *testing.T) {
	contact := presenceTestContact(6)

	assumed := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceRouteFallback)}
	proven := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceProof)}

	if _, ok := presenceArrivals(assumed, proven)[contact]; !ok {
		t.Fatal("route_fallback → proof is a real return and must wake delivery: " +
			"the routing table never changed, so nothing else will notice it")
	}

	// The reverse is not an arrival: losing a proof and falling back to the
	// routing table is a downgrade, not a return.
	if _, ok := presenceArrivals(proven, assumed)[contact]; ok {
		t.Fatal("proof → route_fallback must not count as an arrival")
	}
	// And a contact already proven present stays quiet.
	if len(presenceArrivals(proven, proven)) != 0 {
		t.Fatal("an already-proven contact must not be re-announced every pass")
	}
}

// A contact present in the first generation this node ever builds IS an
// arrival: there was nothing before it, and a message waiting for them should
// not sit out a poll interval because the node had just started.
func TestFirstGenerationCountsAsArrival(t *testing.T) {
	contact := presenceTestContact(4)
	current := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceRouteFallback)}

	if _, ok := presenceArrivals(nil, current)[contact]; !ok {
		t.Fatal("a contact present in the first generation must count as an arrival")
	}
}

// TestOnlyARealChangeCountsAsAChange pins the condition that keeps the event
// from becoming a tick: two identical generations say the same thing.
//
// Named for what it asserts. It used to be called "quiet node publishes
// nothing", which is not true of a node with contacts — the heartbeat
// re-announces the unchanged state once a minute on purpose — and a test name
// that overstates its own subject is read as a promise by whoever comes next.
func TestOnlyARealChangeCountsAsAChange(t *testing.T) {
	contact := presenceTestContact(5)
	set := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceProof)}
	same := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceProof)}

	if !presenceSetsEqual(set, same) {
		t.Fatal("identical generations must compare equal, or the node emits an event every tick")
	}
	// The SOURCE alone changing is a real change: it is the difference
	// between a proven presence and an assumed one, and the interface draws
	// them differently.
	inferred := domain.PresenceSet{contact: domain.OnlinePresence(domain.PresenceSourceRouteFallback)}
	if presenceSetsEqual(set, inferred) {
		t.Fatal("a change of source must count as a change")
	}
	if presenceSetsEqual(set, domain.PresenceSet{}) {
		t.Fatal("a generation that lost a contact must count as a change")
	}
}

// TestProbeRequiresOnlyTargetProof is the mixed-version guard.
//
// BuildGetIdentityPayload puts `target_proof` into `required`, which is safe
// because that name shipped WITH the required mechanism — every build that can
// answer get_identity understands it. A SECOND requirement would not be: an
// unrecognised name obliges the target to stay silent, and this prober reads
// silence as missed probes and would call a live contact gone.
func TestProbeRequiresOnlyTargetProof(t *testing.T) {
	payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V:           domain.IdentityLookupSchemaVersion,
		TargetProof: true,
	})
	if err != nil {
		t.Fatalf("building the probe payload: %v", err)
	}
	parsed, err := protocol.ParseGetIdentityPayload(payload)
	if err != nil {
		t.Fatalf("parsing the probe payload: %v", err)
	}
	if !parsed.UnderstoodRequirements() {
		t.Fatal("the probe requires something an existing build would not understand: " +
			"such a target stays SILENT, and the prober reads silence as absence")
	}
	for _, name := range parsed.Required {
		if name != domain.LookupRequirementTargetProof {
			t.Fatalf("probe requires %q on top of target_proof: every target that predates "+
				"that name goes mute and is then called offline for being old", name)
		}
	}
}

// TestProbeabilityRequiresTheClaimMaterial pins a contract that REVERSED, and
// the reversal is the point.
//
// The earlier rule was "a probe needs no key of the contact's" — true while the
// probe was an unsealed public request. Once the probe carries a sealed
// reciprocity claim (PR B), a contact whose box key we lack cannot be probed
// WITH one, and sending the probe anyway was fail-open: the target answers an
// unsealed request on the public path, so the gate was bypassed by simply not
// having a key. Worse, counting such a contact as probeable also kept them out
// of the route fallback that is meant to cover exactly this case.
//
// So probeability now requires the material for a claim, and a contact missing
// it gets an identity resolution started instead of a downgraded probe.
func TestProbeabilityRequiresTheClaimMaterial(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := probeabilityFunctionBody(t, string(source))

	if !strings.Contains(body, "contactBoxKey") {
		t.Fatal("presenceContactIsProbeable no longer requires the contact's box key: " +
			"a probe would go out without its sealed claim and be answered on the " +
			"public path, bypassing the gate PR B exists for")
	}
	if !strings.Contains(body, "startPresenceIdentityResolution") {
		t.Fatal("a contact without a box key is refused but nothing asks for their " +
			"record: they would sit on the route fallback forever instead of " +
			"becoming probeable")
	}
	// The general knowledge cache must NOT be what answers this: the target
	// recomputes the token from the CONTACT's stored key, and a different key
	// here produces a token that verifies against nothing.
	if strings.Contains(body, "knownBoxKey") {
		t.Fatal("probeability consults the general knowledge cache: the token must " +
			"be derived from the same key the target will verify against")
	}
}

// TestResolutionForADeletedContactIsUndone closes the race between a delete and
// a projection pass that started before it.
//
// The pass snapshots the contact list, then asks about each contact in turn. A
// delete landing in that gap runs its own cleanup — which cannot remove an
// intent that does not exist yet — and the stale pass then creates one. The
// intent is DURABLE, so the result is a background lookup for somebody no
// longer in the address book, surviving restarts.
//
// The identity here is simply not a contact, which is exactly the state the
// stale pass finds itself in.
func TestResolutionForADeletedContactIsUndone(t *testing.T) {
	svc := newDatagramLayerService(t, true)
	if svc.identityResolver == nil {
		t.Fatal("fixture node has no identity resolver")
	}
	target := domaintest.ID("deleted-between-passes")

	svc.startPresenceIdentityResolution(target)

	for _, seed := range svc.identityResolver.intents.seeds() {
		if seed.Target == target {
			t.Fatal("a durable resolution intent survives for an identity that is " +
				"not a contact: a delete racing a projection pass leaves a " +
				"background lookup running across restarts")
		}
	}
}

// TestSealedClaimAndProbeabilityAgree: the two must refuse the same contacts.
// If probeability said yes where the claim cannot be built, the probe would go
// out unsealed — the fail-open hole again, one layer down.
func TestSealedClaimAndProbeabilityAgree(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := string(source)
	const sealSignature = "func (s *Service) sealLivenessClaim("
	if !strings.Contains(body, sealSignature) {
		t.Fatal("sealLivenessClaim is gone: the probe has no claim to attach")
	}
	sealBody := body[strings.Index(body, sealSignature):]
	if end := strings.Index(sealBody, "\n}\n"); end >= 0 {
		sealBody = sealBody[:end]
	}
	if !strings.Contains(sealBody, "contactBoxKey") {
		t.Fatal("sealLivenessClaim and presenceContactIsProbeable disagree about what " +
			"a claim needs: a contact could pass the second and fail the first, and " +
			"the probe would go out unsealed")
	}
}

// TestSuppressedRouteIsNotReadAsAbsence covers a filter this code could not
// see through.
//
// routing.Table.Lookup applies THIS NODE's own exclusions before returning —
// a dead uplink, a black-hole cooldown — and hands back the same empty slice
// for "the network has no claim about them" and "we are refusing every claim
// there is". Reading that as `offline` turns a two-minute black-hole arm into
// a two-minute claim that somebody left.
//
// LookupWithSuppressed separates the two, and this test pins that the presence
// layer asks the question that can tell them apart.
func TestSuppressedRouteIsNotReadAsAbsence(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := string(source)
	if !strings.Contains(body, "LookupWithSuppressed") {
		t.Fatal("presenceRouteStateFor no longer asks LookupWithSuppressed: plain " +
			"Lookup cannot distinguish 'no claim' from 'every claim filtered by us', " +
			"so our own suppressions would be reported as the contact being offline")
	}
	// The projector must still have a suppressed verdict to receive.
	projector := newPresenceProjector()
	contact := presenceTestContact(11)
	got := projector.project(presenceTestInputs(presenceInstantAt(time.Now()), map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRouteSuppressed,
	}, nil)).Get(contact)
	if got.State != domain.PresenceUnknown || got.Reason != domain.PresenceUnknownRouteSuppressedLocally {
		t.Fatalf("suppressed verdict: got %s, want unknown(route_suppressed_locally)", got)
	}
}

// TestPresenceProberForgetsDepartedContacts: the schedule must not outlive the
// address book. Two maps that only grow is the leak shape this codebase has
// already paid for twice.
func TestPresenceProberForgetsDepartedContacts(t *testing.T) {
	svc := &Service{presenceProjector: newPresenceProjector()}
	prober := newPresenceProber(svc)
	staying := presenceTestContact(5)
	leaving := presenceTestContact(6)
	now := presenceInstantAt(time.Now())

	prober.deferNext(staying, now)
	prober.deferNext(leaving, now)

	prober.forgetContacts(map[domain.PeerIdentity]struct{}{staying: {}})

	prober.mu.Lock()
	defer prober.mu.Unlock()
	if _, still := prober.nextDue[leaving]; still {
		t.Fatal("a removed contact kept its probe schedule")
	}
	if _, kept := prober.nextDue[staying]; !kept {
		t.Fatal("a current contact lost its probe schedule")
	}
}

// probeabilityFunctionBody extracts presenceContactIsProbeable's body so the
// checks above are about that function rather than about the whole file — the
// file legitimately uses a box key elsewhere, in sealLivenessClaim.
func probeabilityFunctionBody(t *testing.T, source string) string {
	t.Helper()
	return functionBody(t, source, "func (s *Service) presenceContactIsProbeable(")
}

// functionBody returns one function's text, for the guards that assert a
// property of the CODE rather than of a result.
func functionBody(t *testing.T, source, signature string) string {
	t.Helper()
	start := strings.Index(source, signature)
	if start < 0 {
		t.Fatalf("%s is gone: this guard no longer guards anything", signature)
	}
	rest := source[start:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("could not find the end of %s", signature)
	}
	return rest[:end]
}

// TestTheFrameTakesOneLoad closes the staple: reading the set and then
// re-reading for the generation lets a projection land in between, and the OLD
// set goes out carrying the NEW number.
//
// The consequence is not a cosmetic mismatch. The really new set then arrives
// with a generation the monitor has already seen and is refused as a duplicate;
// when the stale half was empty, the contact list sits on the routing fallback
// until the one-minute heartbeat.
//
// Checked at the source rather than by racing goroutines: the property is "the
// frame is built from a single load", and a timing test would pass on a lucky
// interleaving even with two.
func TestTheFrameTakesOneLoad(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := functionBody(t, string(source), "func (s *Service) presenceFrame(")

	if loads := strings.Count(body, "PresenceSnapshotAt(") + strings.Count(body, "PresenceSnapshot("); loads != 1 {
		t.Fatalf("presenceFrame loads the snapshot %d times, want exactly 1: two loads "+
			"staple a new generation onto an old set, and the real new set is then "+
			"refused as a duplicate", loads)
	}
	if !strings.Contains(body, "PresenceGeneration") {
		t.Fatal("the frame carries no generation: a reader over RPC cannot order two " +
			"projections and the merge falls back to guessing")
	}
}

// TestTheGenerationDoesNotComeFromTheClock. A wall clock cannot order
// projections: on Windows it advances in steps of 0.5–15.6 ms — longer than a
// projection takes, so two of them get the same instant — and it can step
// backwards, which makes a newer projection look older until real time catches
// up. Under either, a genuinely new projection is refused as stale, and the
// backwards step is not repaired even by the heartbeat.
//
// Asserted at the source, and that is the point: a behavioural test would have
// to reproduce the counter's arithmetic to check it, which is a test of the
// test. What must hold is a property of the CODE — the number comes from the
// projection being replaced, and no clock reading is anywhere near it.
func TestTheGenerationDoesNotComeFromTheClock(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := functionBody(t, string(source), "func (s *Service) refreshPresenceSnapshot(")

	store := body[strings.Index(body, "s.presenceSnap.Store("):]
	store = store[:strings.Index(store, "\n")]
	if !strings.Contains(store, "generation:") {
		t.Fatal("the stored projection carries no generation: two readers cannot be ordered")
	}
	for _, clockish := range []string{"UnixNano", "Unix()", "now.", "time.Now"} {
		if strings.Contains(store, clockish) {
			t.Fatalf("the generation is derived from %q: a clock ties and steps "+
				"backwards, and either refuses a genuinely newer projection", clockish)
		}
	}
	if !strings.Contains(body, "previous.generation") {
		t.Fatal("the generation is not derived from the projection being replaced: " +
			"only that makes it increase by construction rather than by assumption")
	}
}

// TestTheHeartbeatMeasuresElapsedTimeNotWallClocks.
//
// The reconcile is the only repair for a dropped presence event: there is no
// periodic full probe after startup, so if the announcement is lost and the
// projection then stops changing, the interface is stale until the heartbeat
// fires. It therefore has to measure ELAPSED time, on the same hybrid rule as
// everything else — an earlier revision round-tripped the stored instant
// through UnixNano, which made the subtraction a difference of wall clocks and
// let a clock stepped backwards suppress the reconcile indefinitely.
//
// Asserted at the source because the divergence that makes it matter cannot be
// produced in a test. What the type CANNOT express is which measure is used —
// Since is the only one it offers, so this is now a check that the measurement
// happens here at all rather than being replaced by a bare equality.
func TestTheHeartbeatMeasuresElapsedTimeNotWallClocks(t *testing.T) {
	source, err := os.ReadFile("presence_service.go")
	if err != nil {
		t.Fatalf("reading presence_service.go: %v", err)
	}
	body := functionBody(t, string(source), "func (s *Service) refreshPresenceSnapshot(")

	if !strings.Contains(body, "now.Since(s.lastPresenceHeartbeatAt)") {
		t.Fatal("the heartbeat interval is not measured with presenceInstant.Since " +
			"on the stored instant: only that takes whichever clock saw more time, " +
			"and without it a suspended machine or a stepped clock suppresses the " +
			"reconcile indefinitely")
	}
	for _, stripped := range []string{"lastPresenceHeartbeatAtNanos", "time.Unix(0,", "time.Since("} {
		if strings.Contains(body, stripped) {
			t.Fatalf("the heartbeat still measures with %q, which is not the "+
				"presence measure", stripped)
		}
	}
}

// presenceCloseWasRecorded asks the projection whether an attributable session
// close is on record for this contact. Read with NO route, where the two
// answers differ in their SOURCE: `session_closed` means the close was
// recorded, `route_observation` means only the missing route was.
func presenceCloseWasRecorded(t *testing.T, svc *Service, contact domain.PeerIdentity) bool {
	t.Helper()
	got := svc.presenceProjector.project(presenceTestInputs(
		presenceInstantAt(time.Now()),
		map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent},
		nil,
	)).Get(contact)
	return got.Source == domain.PresenceSourceSessionClosed
}

// TestPresenceFollowsTheLastSessionNotTheLastRelaySession.
//
// The close used to be recorded from the routing withdrawal path, which runs
// only when the last RELAY session goes. Routing is right to gate it that way
// and presence is not: the two ask different questions, and the answers diverge
// in both directions.
//
//   - A contact whose build has no mesh_relay_v1 never reached that branch, so a
//     clean EOF from them was never recorded and they stayed green until their
//     evidence window ran out — up to 450 s of the false green this whole
//     feature exists to remove.
//   - A contact who closed a relay session while another session stayed up was
//     written down as gone while this node was still talking to them.
func TestPresenceFollowsTheLastSessionNotTheLastRelaySession(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		setUp func(*Service, domain.PeerIdentity)
		close []domain.Capability
		want  bool
	}{
		{
			name: "a contact with no relay capability at all",
			setUp: func(svc *Service, peer domain.PeerIdentity) {
				svc.onPeerSessionEstablished(peer, nil)
			},
			close: nil,
			want:  true,
		},
		{
			name: "the last relay session goes while another session stays up",
			setUp: func(svc *Service, peer domain.PeerIdentity) {
				svc.onPeerSessionEstablished(peer, []domain.Capability{domain.CapMeshRelayV1})
				svc.onPeerSessionEstablished(peer, nil)
			},
			close: []domain.Capability{domain.CapMeshRelayV1},
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			contactID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			if _, err := svc.trust.remember(trustedContact{
				Address: contactID.Address,
				PubKey:  identity.PublicKeyBase64(contactID.PublicKey),
			}); err != nil {
				t.Fatalf("remember contact: %v", err)
			}
			peer := domain.PeerIdentityFromWire(contactID.Address)

			tc.setUp(svc, peer)
			svc.onPeerSessionClosedWithCause(peer, tc.close, sessionClosePeerInitiated)
			svc.WaitBackground()

			if got := presenceCloseWasRecorded(t, svc, peer); got != tc.want {
				t.Fatalf("close recorded = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAnyReconnectSpendsTheRecordedClose.
//
// The close is spent by the contact's session coming back — the 0 → 1
// transition of their TOTAL session count, mirroring the close on the last
// total session. It used to be spent only where the relay withdrawal timer was
// cancelled, and a contact whose build has no mesh_relay_v1 never reaches that
// branch: the close outlived the reconnect, and for a contact that cannot be
// probed it then reads `offline`/`session_closed` for the whole life of a
// perfectly good session, because the thing that would otherwise clear it — the
// route disappearing and coming back — never moved either. A non-relay peer has
// no direct route at all, which is exactly why it has nothing else to fall back
// on.
//
// A RELAY contact is deliberately not a second case here: their reconnect
// re-adds the direct route, and the projector spends the close on that
// absent → present transition whatever this path does, so the assertion would
// hold with the fix reverted and prove nothing.
func TestAnyReconnectSpendsTheRecordedClose(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	contactID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	if _, err := svc.trust.remember(trustedContact{
		Address: contactID.Address,
		PubKey:  identity.PublicKeyBase64(contactID.PublicKey),
	}); err != nil {
		t.Fatalf("remember contact: %v", err)
	}
	peer := domain.PeerIdentityFromWire(contactID.Address)

	svc.onPeerSessionEstablished(peer, nil)
	svc.onPeerSessionClosedWithCause(peer, nil, sessionClosePeerInitiated)
	svc.WaitBackground()
	if !presenceCloseWasRecorded(t, svc, peer) {
		t.Fatal("precondition: the close was not recorded, so this proves nothing")
	}

	svc.onPeerSessionEstablished(peer, nil)
	svc.WaitBackground()
	if presenceCloseWasRecorded(t, svc, peer) {
		t.Fatal("the contact reconnected and is still recorded as having closed: " +
			"nothing else will clear it, because their route never moved")
	}
}

// presenceContactFixture is a service with one trusted contact, ready for the
// session-lifecycle tests below.
func presenceContactFixture(t *testing.T) (*Service, domain.PeerIdentity) {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	contactID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	if _, err := svc.trust.remember(trustedContact{
		Address: contactID.Address,
		PubKey:  identity.PublicKeyBase64(contactID.PublicKey),
	}); err != nil {
		t.Fatalf("remember contact: %v", err)
	}
	return svc, domain.PeerIdentityFromWire(contactID.Address)
}

// TestACloseCarriesTheTimeItWasObservedNotTheTimeItWasWritten.
//
// The close is stamped at the top of the teardown (peerOfflineEvidence) and
// reaches the projector several steps later — after session accounting, the
// datagram peer forget and the route bookkeeping. Handing the projector the
// clock reading of THAT moment instead of the observation makes every one of
// those steps count as freshness: a proof that genuinely arrived after the
// close, but before the close finished travelling, is outranked and wiped, and
// the contact who just signed for us reads `probing` until somebody asks again.
//
// Asserted through the SERVICE, not by calling the projector with hand-made
// timestamps: the projector's own ordering has its own test, and what broke
// here was the wiring between them.
func TestACloseCarriesTheTimeItWasObservedNotTheTimeItWasWritten(t *testing.T) {
	t.Parallel()
	svc, peer := presenceContactFixture(t)
	svc.onPeerSessionEstablished(peer, nil)

	observed := svc.presenceNow().Add(-time.Minute)
	// The proof lands AFTER the close was observed, while the close is still
	// on its way here.
	svc.presenceProjector.noteProof(peer, observed.Add(time.Second), presenceAliveValidity)

	svc.onPeerSessionClosedWithAttribution(peer, nil, sessionClosePeerInitiated,
		&peerOfflineEvidence{observedAt: observed})
	svc.WaitBackground()

	got := svc.presenceProjector.project(presenceTestInputs(
		svc.presenceNow(),
		map[domain.PeerIdentity]presenceRouteState{peer: presenceRoutePresent},
		nil,
	)).Get(peer)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourceProof {
		t.Fatalf("a close observed before the proof outranked it: got %v", got)
	}
}

// TestAReconnectThatOvertakesTheCloseStillSpendsIt.
//
// The close decrements the session count and releases peerMu long before it
// reaches the projector. A new session can be established in that window and
// run the whole return path first — and the return then found no close to
// withdraw and, in an earlier version, left nothing behind. The close landed
// afterwards and stuck.
//
// For a contact with no direct route that is terminal until they send something
// signed: the route never moved, so the thing that normally spends a close
// never happens.
//
// The two are ordered by their TRANSITION NUMBERS, minted under peerMu, so the
// clock plays no part: the reconnect is transition 2 and the close it followed
// is transition 1, whatever instant either of them carries.
func TestAReconnectThatOvertakesTheCloseStillSpendsIt(t *testing.T) {
	t.Parallel()
	svc, peer := presenceContactFixture(t)

	sameInstant := svc.presenceNow()
	svc.notePresenceSessionReturned(peer, 2)
	svc.notePresenceSessionClosed(peer, sameInstant, 1)
	svc.WaitBackground()

	if presenceCloseWasRecorded(t, svc, peer) {
		t.Fatal("a close that arrived after the reconnect it precedes was applied anyway: " +
			"with no direct route to watch, nothing will ever spend it")
	}
}

// TestSessionOrderingDoesNotDependOnTheClock.
//
// A wall clock cannot order these events and failed three separate ways:
//
//   - the reading has to be taken before peerMu while the transition is decided
//     under it, so two overlapping sessions can take their readings and then
//     reach the lock in the other order — the reconnect ends up stamped EARLIER
//     than the close it followed;
//   - a coarse clock ties them (Windows advances in steps of up to 15.6 ms,
//     longer than "the session came up and immediately got an EOF" takes);
//   - a clock stepped backwards inverts them outright.
//
// The clock here produces exactly that inversion: the close is observed at a
// LATER instant than the reconnect that follows it. Every assertion below must
// hold anyway, because nothing on this path reads a clock to order the two —
// they are numbered under the lock that decides them.
//
// The reading is keyed on WHAT is happening rather than on how many times the
// clock has been called: a call-counting script is silently wrong here, because
// the projection pass this triggers reads the same clock an unspecified number
// of times, and a fixture that mis-scripts its own premise proves nothing.
func TestSessionOrderingDoesNotDependOnTheClock(t *testing.T) {
	t.Parallel()
	svc, peer := presenceContactFixture(t)

	base := time.Now().UTC()
	var closing bool
	svc.presenceClock = func() time.Time {
		if closing {
			return base.Add(20 * time.Second)
		}
		return base
	}

	svc.onPeerSessionEstablished(peer, nil)

	closing = true
	svc.onPeerSessionClosedWithCause(peer, nil, sessionClosePeerInitiated)
	svc.WaitBackground()
	closing = false
	if !presenceCloseWasRecorded(t, svc, peer) {
		t.Fatal("precondition: the close was not recorded, so this proves nothing")
	}

	// The reconnect FOLLOWS that close in the world and precedes it on the
	// clock. It must still spend it.
	svc.onPeerSessionEstablished(peer, nil)
	svc.WaitBackground()
	if presenceCloseWasRecorded(t, svc, peer) {
		t.Fatal("the reconnect was judged older than the close it followed, because " +
			"its clock reading was taken before the lock that decides the transition")
	}
}

// TestThePresenceClockKeepsItsMonotonicReading.
//
// Everything presence does with an instant is either an interval (proof
// validity, probe timeout, probe cadence) or an ordering of two observations
// (a close against a proof). Both run on the wall clock the moment the
// monotonic reading is stripped, and `Time.UTC()` strips it — which is how one
// `.UTC()` at the source made a clock step backwards hold a contact green past
// the validity window, stall every probe timeout, and drop a genuinely later
// close.
//
// Detected through the one API that exposes it: Time.String documents that a
// value carrying a monotonic reading renders it as a trailing " m=±<value>".
func TestThePresenceClockKeepsItsMonotonicReading(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	got := svc.presenceNow()
	if !strings.Contains(got.Wall().String(), " m=") {
		t.Fatal("the presence clock hands out wall-clock-only instants: every interval " +
			"it measures and every pair it orders is then at the mercy of a clock step")
	}
}

// TestPresenceInstantOffersNoWayToGetItWrong.
//
// This is what the presenceInstant type is FOR, and it replaces three
// source-scanning guards that used to stand here. Each of them was written
// after a leak, and the last one still missed a site — the resolution cooldown
// — because it read a list of files somebody had to remember to extend.
//
// Sub, Before and After are the spellings that were repeatedly written by
// accident: they measure with the monotonic clock alone, which stops while the
// machine sleeps. UTC and the Unix conversions are the ones that strip the
// reading entirely. None of them is a method of this type, so none of them
// compiles any more, and the compiler needs no reminding to check a new file.
//
// Asserted rather than assumed, because "the method does not exist" is a
// property somebody can undo in one line while adding a convenience.
func TestPresenceInstantOffersNoWayToGetItWrong(t *testing.T) {
	t.Parallel()
	instant := reflect.TypeOf(presenceInstant{})
	for _, forbidden := range []string{"Sub", "Before", "After", "UTC", "Unix", "UnixNano", "Round", "Truncate", "Equal"} {
		if _, found := instant.MethodByName(forbidden); found {
			t.Fatalf("presenceInstant grew a %s method: that is the spelling this "+
				"type exists to make impossible, and every guard that used to "+
				"catch it by reading source has been removed in its favour", forbidden)
		}
	}
	for _, required := range []string{"Since", "Reached", "ObservedAfter", "Wall", "Add", "IsZero"} {
		if _, found := instant.MethodByName(required); !found {
			t.Fatalf("presenceInstant lost %s: the guard above now guards nothing", required)
		}
	}
}
