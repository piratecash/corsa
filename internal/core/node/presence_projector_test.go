package node

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// presence_projector_test.go covers the rules of
// docs/protocol/presence.md §3. Each test names the source
// of lies it closes, because a presence rule without its motivating failure
// reads as an arbitrary preference and gets "simplified" away later.

func presenceTestContact(last byte) domain.PeerIdentity {
	raw := make([]byte, 40)
	for i := range raw {
		raw[i] = '1'
	}
	raw[39] = "0123456789abcdef"[last%16]
	return domain.PeerIdentityFromWire(string(raw))
}

// presenceTestInputs builds inputs where every contact is probeable, our own
// connectivity is fine, and routes are whatever the caller says.
func presenceTestInputs(now presenceInstant, routes map[domain.PeerIdentity]presenceRouteState, probeable map[domain.PeerIdentity]bool) presenceInputs {
	contacts := make([]domain.PeerIdentity, 0, len(routes))
	for identity := range routes {
		contacts = append(contacts, identity)
	}
	return presenceInputs{
		Now:               now,
		LocalConnectivity: true,
		Contacts:          contacts,
		RouteState: func(identity domain.PeerIdentity) presenceRouteState {
			return routes[identity]
		},
		Probeable: func(identity domain.PeerIdentity) bool {
			if probeable == nil {
				return true
			}
			return probeable[identity]
		},
	}
}

// A route that WE suppressed says nothing about the contact. Quarantine runs up
// to 30 minutes and flap hold-down up to 10, so reading either as "offline"
// would be a lie longer than the one this whole design removes (docs/protocol/presence.md §3 rule 2).
func TestOurOwnSuppressionIsUnknownNotOffline(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(1)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRouteSuppressed,
	}, nil))

	got := set.Get(contact)
	if got.State != domain.PresenceUnknown {
		t.Fatalf("suppressed route: got %s, want unknown", got)
	}
	if got.Reason != domain.PresenceUnknownRouteSuppressedLocally {
		t.Fatalf("suppressed route reason: got %s, want route_suppressed_locally", got.Reason)
	}
}

// The route disappearing while our own connectivity is healthy IS an
// observation about the peer, and it is the fast negative signal presence keeps
// from routing (docs/protocol/presence.md §3 rule 1).
func TestVanishedRouteIsOfflineWhenWeAreHealthy(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(2)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRouteAbsent,
	}, nil))

	got := set.Get(contact)
	if got.State != domain.PresenceOffline {
		t.Fatalf("absent route: got %s, want offline", got)
	}
	if got.Source != domain.PresenceSourceRouteObservation {
		t.Fatalf("absent route source: got %s, want route_observation", got.Source)
	}
}

// Our own outage must not become a claim about fifty other people (docs/protocol/presence.md §3 rule 1).
func TestOurOwnOutageMakesEverybodyUnknown(t *testing.T) {
	now := presenceInstantAt(time.Now())
	present := presenceTestContact(3)
	absent := presenceTestContact(4)
	projector := newPresenceProjector()

	inputs := presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		present: presenceRoutePresent,
		absent:  presenceRouteAbsent,
	}, nil)
	inputs.LocalConnectivity = false

	set := projector.project(inputs)
	for _, contact := range []domain.PeerIdentity{present, absent} {
		got := set.Get(contact)
		if got.State != domain.PresenceUnknown {
			t.Fatalf("during our outage: got %s, want unknown", got)
		}
		if got.Reason != domain.PresenceUnknownNoLocalConnectivity {
			t.Fatalf("during our outage reason: got %s", got.Reason)
		}
	}
}

// A route appearing is not proof of life: it buys "probing", and only an answer
// buys green (docs/protocol/presence.md §3 rule 4).
func TestRouteAloneIsProbingNotOnline(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(5)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRoutePresent,
	}, nil))

	if got := set.Get(contact); got.State != domain.PresenceProbing {
		t.Fatalf("route present, no proof: got %s, want probing", got)
	}
}

// A valid target_proof is the contact's own signature: it makes them online and
// keeps them there until the proof expires (docs/protocol/presence.md §3 rule 5).
func TestProofMakesOnlineUntilItExpires(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(6)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, 90*time.Second)

	set := projector.project(presenceTestInputs(now.Add(time.Second), routes, nil))
	got := set.Get(contact)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourceProof {
		t.Fatalf("fresh proof: got %s, want online(proof)", got)
	}

	set = projector.project(presenceTestInputs(now.Add(91*time.Second), routes, nil))
	if got := set.Get(contact); got.State != domain.PresenceProbing {
		t.Fatalf("expired proof with a route: got %s, want probing", got)
	}
}

// A signed frame from the contact outranks every inference we could make, and
// lifts them out of offline without any probe (docs/protocol/presence.md §3 rule 5).
func TestSignedFrameLiftsOutOfOffline(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(7)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent}

	if got := projector.project(presenceTestInputs(now, routes, nil)).Get(contact); got.State != domain.PresenceOffline {
		t.Fatalf("precondition: got %s, want offline", got)
	}

	projector.notePassive(contact, now.Add(time.Second), 90*time.Second)

	got := projector.project(presenceTestInputs(now.Add(2*time.Second), routes, nil)).Get(contact)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourcePassive {
		t.Fatalf("after their frame arrived: got %s, want online(passive)", got)
	}
}

// A session close ends the green immediately, even though the route is still
// there — the withdrawal grace period holds it open for another twenty seconds,
// and waiting for it is exactly the false green being removed (docs/protocol/presence.md §3 rule 3).
//
// It does NOT immediately claim absence while a path is still visible: their
// session with us ended, which is not the same as them being gone from the
// network. So `probing`, asked at once, and `offline` only once no path is
// left. That distinction is what keeps a contact reachable through transit from
// being falsely greyed out by a neighbour disconnect.
func TestSessionCloseEndsTheGreenImmediately(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(8)
	projector := newPresenceProjector()
	withRoute := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, 90*time.Second)
	if got := projector.project(presenceTestInputs(now, withRoute, nil)).Get(contact); got.State != domain.PresenceOnline {
		t.Fatalf("precondition: got %s, want online", got)
	}

	projector.noteSessionClosed(contact, now.Add(time.Second), 1)

	// Route still selectable (grace), and the contact is no longer green.
	got := projector.project(presenceTestInputs(now.Add(2*time.Second), withRoute, nil)).Get(contact)
	if got.State == domain.PresenceOnline {
		t.Fatalf("still online one second after their session closed: got %s", got)
	}
	if got.State != domain.PresenceProbing {
		t.Fatalf("close with a path still visible: got %s, want probing", got)
	}

	// Grace expires, the route goes, and now absence is the honest answer.
	gone := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent}
	got = projector.project(presenceTestInputs(now.Add(25*time.Second), gone, nil)).Get(contact)
	if got.State != domain.PresenceOffline || got.Source != domain.PresenceSourceSessionClosed {
		t.Fatalf("close with no path left: got %s, want offline(session_closed)", got)
	}
}

// A contact who comes back after a session close must come back.
//
// This is the bug the review caught: the close was cleared only by positive
// liveness evidence and was checked before the route, so a contact who could
// not be probed stayed grey FOREVER once their session had closed — no route,
// however fresh, could lift them. The rule is that the close is spent once the
// route it was recorded against has been seen to go and a new one has appeared.
func TestContactReturnsAfterASessionClose(t *testing.T) {
	now := presenceInstantAt(time.Now())
	legacy := presenceTestContact(9)
	notProbeable := map[domain.PeerIdentity]bool{legacy: false}
	projector := newPresenceProjector()

	present := map[domain.PeerIdentity]presenceRouteState{legacy: presenceRoutePresent}
	absent := map[domain.PeerIdentity]presenceRouteState{legacy: presenceRouteAbsent}

	projector.noteSessionClosed(legacy, now, 1)

	// Grace window: route still there, close still authoritative.
	if got := projector.project(presenceTestInputs(now.Add(time.Second), present, notProbeable)).Get(legacy); got.State == domain.PresenceOnline {
		t.Fatalf("during grace: got %s, want not-online", got)
	}
	// Grace expires: no path, honest absence.
	if got := projector.project(presenceTestInputs(now.Add(30*time.Second), absent, notProbeable)).Get(legacy); got.State != domain.PresenceOffline {
		t.Fatalf("after grace: got %s, want offline", got)
	}
	// They come back, and a route to them appears again.
	got := projector.project(presenceTestInputs(now.Add(5*time.Minute), present, notProbeable)).Get(legacy)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourceRouteFallback {
		t.Fatalf("after they returned: got %s, want online(route_fallback) — the close must be spent", got)
	}
}

// The same return, for a contact we CAN probe: they must leave the close behind
// and land in probing, which the caller then resolves with an immediate probe.
func TestProbeableContactReturnsToProbingAfterAClose(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(10)
	projector := newPresenceProjector()
	present := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}
	absent := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent}

	projector.noteSessionClosed(contact, now, 1)
	projector.project(presenceTestInputs(now.Add(30*time.Second), absent, nil))

	got := projector.project(presenceTestInputs(now.Add(time.Minute), present, nil)).Get(contact)
	if got.State != domain.PresenceProbing {
		t.Fatalf("probeable contact after returning: got %s, want probing", got)
	}
}

// Evidence that says nothing new must not trigger a republish.
//
// Every incoming message from a contact is passive evidence, and a full
// projection runs a routing lookup per contact. Republishing on each message
// would make an active conversation pay thousands of lock acquisitions on the
// ingest path to restate a fact already published.
func TestRepeatedEvidenceDoesNotAskForARepublish(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(13)
	projector := newPresenceProjector()

	if !projector.notePassive(contact, now, 90*time.Second) {
		t.Fatal("the first evidence of life must ask for a republish")
	}
	if projector.notePassive(contact, now.Add(time.Second), 90*time.Second) {
		t.Fatal("a second message from an already-live contact must not ask for a republish")
	}

	// A close, a strike, or a different kind of evidence IS news.
	projector.noteSessionClosed(contact, now.Add(2*time.Second), 1)
	if !projector.notePassive(contact, now.Add(3*time.Second), 90*time.Second) {
		t.Fatal("evidence after a session close must ask for a republish")
	}
	projector.noteProbeUnanswered(contact, now.Add(4*time.Second))
	if !projector.notePassive(contact, now.Add(5*time.Second), 90*time.Second) {
		t.Fatal("evidence that clears a strike must ask for a republish")
	}
	if !projector.noteProof(contact, now.Add(6*time.Second), 90*time.Second) {
		t.Fatal("a proof after passive evidence is a different source and must republish")
	}

	// And expiry is news: the window lapsed, so the next evidence is a return.
	if !projector.notePassive(contact, now.Add(10*time.Minute), 90*time.Second) {
		t.Fatal("evidence after the window expired must ask for a republish")
	}
}

// A contact sitting in probing is what the prober must be told about, or the
// answer waits for the periodic slot — up to ~187 s with jitter.
func TestProbingContactsAreQueuedForAnImmediateProbe(t *testing.T) {
	now := presenceInstantAt(time.Now())
	probeable := presenceTestContact(11)
	legacy := presenceTestContact(12)
	projector := newPresenceProjector()

	projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		probeable: presenceRoutePresent,
		legacy:    presenceRoutePresent,
	}, map[domain.PeerIdentity]bool{probeable: true, legacy: false}))

	queued := projector.takeProbeNow()
	if len(queued) != 1 || queued[0] != probeable {
		t.Fatalf("probe queue: got %v, want exactly the probeable contact", queued)
	}
	if len(projector.takeProbeNow()) != 0 {
		t.Fatal("the probe queue must be drained by taking it, not replayed")
	}
}

// Hysteresis: one lost probe is a lost packet, three is an absence
// (docs/protocol/presence.md §3 rule 6, Detect Mult from BFD).
func TestThreeMissedProbesAreOfflineTwoAreNot(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(9)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, 90*time.Second)
	for i := 0; i < presenceDetectMult-1; i++ {
		projector.noteProbeUnanswered(contact, now.Add(time.Duration(i+1)*time.Minute))
	}
	if got := projector.project(presenceTestInputs(now.Add(time.Minute), routes, nil)).Get(contact); got.State == domain.PresenceOffline {
		t.Fatalf("after %d missed probes: got offline too early", presenceDetectMult-1)
	}

	projector.noteProbeUnanswered(contact, now.Add(time.Duration(presenceDetectMult)*time.Minute))
	got := projector.project(presenceTestInputs(now.Add(10*time.Minute), routes, nil)).Get(contact)
	if got.State != domain.PresenceOffline {
		t.Fatalf("after %d missed probes: got %s, want offline", presenceDetectMult, got)
	}
	if got.Source != domain.PresenceSourceProbeTimeout {
		t.Fatalf("probe timeout source: got %s", got.Source)
	}
}

// TestReturnedRouteRetiresTheStrikes: a contact who was called offline by three
// silent probes, whose route then GOES and COMES BACK, is being asked about
// again — not left grey until the periodic slot.
//
// The strikes describe a period the current route is newer than: a strike is
// only recorded for a probe that actually reached the network, so it predates
// the absence. Rule 4 outranks every route rule below it, so without spending
// the strikes on the transition the contact stayed `offline`, the state never
// entered `probing`, and the triggered probe — the thing that resolves this in
// one round trip — was never armed.
func TestReturnedRouteRetiresTheStrikes(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(14)
	projector := newPresenceProjector()
	present := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}
	absent := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent}

	for i := 0; i < presenceDetectMult; i++ {
		projector.noteProbeUnanswered(contact, now.Add(time.Duration(i)*time.Minute))
	}
	if got := projector.project(presenceTestInputs(now, present, nil)).Get(contact); got.State != domain.PresenceOffline {
		t.Fatalf("after %d silent probes: got %s, want offline", presenceDetectMult, got)
	}
	_ = projector.takeProbeNow()

	// They leave for real, and come back.
	if got := projector.project(presenceTestInputs(now.Add(time.Minute), absent, nil)).Get(contact); got.State != domain.PresenceOffline {
		t.Fatalf("with no route at all: got %s, want offline", got)
	}
	got := projector.project(presenceTestInputs(now.Add(2*time.Minute), present, nil)).Get(contact)
	if got.State != domain.PresenceProbing {
		t.Fatalf("after the route came back: got %s, want probing — the strikes "+
			"describe a period this route is newer than, and keeping them pins a "+
			"returned contact grey until the next periodic slot", got)
	}
	queued := projector.takeProbeNow()
	if len(queued) != 1 || queued[0] != contact {
		t.Fatalf("triggered probe queue: got %v, want exactly the returned contact", queued)
	}
}

// TestSuppressionDoesNotRetireTheStrikes is the other half of the rule above: a
// route we are refusing OURSELVES is a fact about this node, and the moment it
// lifts the SAME route must not read as the contact's return. Without this a
// black-hole cooldown would launder a settled `offline` back into `probing`
// every time it expired.
func TestSuppressionDoesNotRetireTheStrikes(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(15)
	projector := newPresenceProjector()
	present := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}
	suppressed := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteSuppressed}

	for i := 0; i < presenceDetectMult; i++ {
		projector.noteProbeUnanswered(contact, now.Add(time.Duration(i)*time.Minute))
	}
	projector.project(presenceTestInputs(now, present, nil))
	// We suppress the route ourselves, then stop.
	projector.project(presenceTestInputs(now.Add(time.Minute), suppressed, nil))
	got := projector.project(presenceTestInputs(now.Add(2*time.Minute), present, nil)).Get(contact)
	if got.State != domain.PresenceOffline {
		t.Fatalf("after our own suppression lifted: got %s, want offline — a "+
			"suppression of ours is never the contact's departure and its end is "+
			"never their return", got)
	}
}

// An answer clears the strike count: three missed probes must be three IN A
// ROW, not three ever (docs/protocol/presence.md §3 rule 6).
func TestAnAnswerClearsTheStrikeCount(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(10)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProbeUnanswered(contact, now)
	projector.noteProbeUnanswered(contact, now.Add(time.Minute))
	projector.noteProof(contact, now.Add(2*time.Minute), 90*time.Second)
	projector.noteProbeUnanswered(contact, now.Add(3*time.Minute))

	got := projector.project(presenceTestInputs(now.Add(4*time.Minute), routes, nil)).Get(contact)
	if got.State == domain.PresenceOffline {
		t.Fatal("a missed probe after an answer must not carry the old strikes into offline")
	}
}

// The case neither projection nor routing can see: a contact whose proof is
// valid for 450 s goes quiet for one probe cadence and answers the next probe.
// Both projections read `online(proof)`, and the route through a transit hop
// never went anywhere, so the two mechanisms that would otherwise notice a
// return are blind to the SAME event — and a message sent during the gap sits
// out its backoff, up to eleven minutes.
func TestAProofThatEndsASilenceIsAReturn(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(13)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, presenceAliveValidity)
	projector.project(presenceTestInputs(now, routes, nil))
	if returned := projector.takeReturned(); len(returned) != 0 {
		t.Fatalf("a first proof is not a return, it is an arrival: %v", returned)
	}

	// One probe goes unanswered. Not enough to change anything a reader sees:
	// the window from the earlier proof is still wide open.
	silent := now.Add(presenceProbeInterval)
	projector.noteProbeUnanswered(contact, silent)
	before := projector.project(presenceTestInputs(silent, routes, nil)).Get(contact)

	// And they answer the next one.
	back := now.Add(2 * presenceProbeInterval)
	projector.noteProof(contact, back, presenceAliveValidity)
	after := projector.project(presenceTestInputs(back, routes, nil)).Get(contact)

	if before != after {
		t.Fatalf("this test is about an invisible return; the projection changed: %v -> %v", before, after)
	}
	returned := projector.takeReturned()
	if len(returned) != 1 || returned[0] != contact {
		t.Fatalf("the proof that ended the silence must be reported as a return, got %v", returned)
	}
	if again := projector.takeReturned(); len(again) != 0 {
		t.Fatalf("a return is reported once, got it twice: %v", again)
	}
}

// The other half of the same rule: evidence that merely EXTENDS a window is not
// a return. Without this the wake-up would fire on every incoming message of an
// active conversation, which is a poll wearing a wake-up's name.
func TestEvidenceThatDoubtedNothingIsNotAReturn(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(14)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, presenceAliveValidity)
	projector.project(presenceTestInputs(now, routes, nil))
	projector.takeReturned()

	for i := 1; i <= 5; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		projector.notePassive(contact, at, presenceAliveValidity)
		projector.project(presenceTestInputs(at, routes, nil))
		if returned := projector.takeReturned(); len(returned) != 0 {
			t.Fatalf("message %d of a conversation was read as a return: %v", i, returned)
		}
	}
}

// A proof that lands while OUR network is down is held, not dropped: the wake-up
// it would trigger can do nothing until we can reach anybody, and reporting the
// return then is what makes it useful rather than lost.
func TestAReturnDuringOurOwnOutageIsReportedWhenItEnds(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(15)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProbeUnanswered(contact, now)
	projector.noteProof(contact, now.Add(time.Second), presenceAliveValidity)

	outage := presenceTestInputs(now.Add(2*time.Second), routes, nil)
	outage.LocalConnectivity = false
	if got := projector.project(outage).Get(contact); got.State != domain.PresenceUnknown {
		t.Fatalf("our own outage must read as unknown, got %v", got)
	}
	if returned := projector.takeReturned(); len(returned) != 0 {
		t.Fatalf("nothing to wake while our network is down, got %v", returned)
	}

	projector.project(presenceTestInputs(now.Add(3*time.Second), routes, nil))
	if returned := projector.takeReturned(); len(returned) != 1 || returned[0] != contact {
		t.Fatalf("the held return must be reported once we are back, got %v", returned)
	}
}

// The route fallback: a contact that cannot answer a probe at all is shown from
// the routing table, and the source says so, so the interface can draw it as an
// assumption rather than as knowledge. Removed with the full routing table —
// see presence_route_fallback.go.
func TestNotProbeableContactFallsBackToRoute(t *testing.T) {
	now := presenceInstantAt(time.Now())
	reachable := presenceTestContact(11)
	gone := presenceTestContact(12)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		reachable: presenceRoutePresent,
		gone:      presenceRouteAbsent,
	}, map[domain.PeerIdentity]bool{reachable: false, gone: false}))

	got := set.Get(reachable)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourceRouteFallback {
		t.Fatalf("not probeable + route: got %s, want online(route_fallback)", got)
	}
	if got.IsProven() {
		t.Fatal("a route-derived presence must never read as proven")
	}
	if !got.IsInferred() {
		t.Fatal("a route-derived presence must read as inferred")
	}
	if got := set.Get(gone); got.State != domain.PresenceOffline {
		t.Fatalf("not probeable + no route: got %s, want offline", got)
	}
}

// The fallback is for contacts that CANNOT be probed. A probeable contact that
// simply has not answered yet must not borrow it — otherwise honest offline
// never happens while a stale route sits in the table, which is the very lie
// being removed.
func TestProbeableContactNeverUsesTheFallback(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(13)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	for i := 0; i <= presenceDetectMult; i++ {
		projector.noteProbeUnanswered(contact, now.Add(time.Duration(i)*time.Minute))
	}

	got := projector.project(presenceTestInputs(now.Add(10*time.Minute), routes, nil)).Get(contact)
	if got.Source == domain.PresenceSourceRouteFallback {
		t.Fatal("a probeable contact fell back to the routing table")
	}
	if got.State != domain.PresenceOffline {
		t.Fatalf("silent probeable contact with a route: got %s, want offline", got)
	}
}

// Probe silence outranks our own route bookkeeping.
//
// This caught a real ordering bug: suppression was checked BEFORE the strike
// count, so a contact who had genuinely stopped answering read as `unknown` for
// as long as one unusable route sat in the table. The reason it is safe to
// order it this way round is that a strike is only recorded for a probe that
// actually reached the network — a send the layer refuses drops its attempt
// instead of arming a timeout — so strikes can never be an artefact of the same
// local condition that suppressed the route.
func TestProbeSilenceOutranksOurOwnSuppression(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(3)
	projector := newPresenceProjector()

	for i := 0; i < presenceDetectMult; i++ {
		projector.noteProbeUnanswered(contact, now.Add(time.Duration(i)*time.Minute))
	}

	got := projector.project(presenceTestInputs(now.Add(10*time.Minute), map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRouteSuppressed,
	}, nil)).Get(contact)

	if got.State != domain.PresenceOffline {
		t.Fatalf("three silent probes + a suppressed route: got %s, want offline", got)
	}
	if got.Source != domain.PresenceSourceProbeTimeout {
		t.Fatalf("source: got %s, want probe_timeout", got.Source)
	}
}

// A contact that is neither probeable nor route-visible, when a suppression of
// ours is what removed the route, is still unknown: the fallback inherits the
// suppression rule rather than overriding it.
func TestFallbackStillRespectsOurOwnSuppression(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(14)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRouteSuppressed,
	}, map[domain.PeerIdentity]bool{contact: false}))

	got := set.Get(contact)
	if got.State != domain.PresenceUnknown || got.Reason != domain.PresenceUnknownRouteSuppressedLocally {
		t.Fatalf("suppressed route for a non-probeable contact: got %s", got)
	}
}

// Presence is per-contact by construction: an identity that is merely routable
// gets no record at all. This is what keeps the state bounded and is why the
// design survives the routing table shrinking under it.
func TestOnlyContactsGetPresenceRecords(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(15)
	stranger := presenceTestContact(2)
	projector := newPresenceProjector()

	set := projector.project(presenceTestInputs(now, map[domain.PeerIdentity]presenceRouteState{
		contact: presenceRoutePresent,
	}, nil))

	if _, recorded := set[stranger]; recorded {
		t.Fatal("a non-contact identity must not get a presence record")
	}
	if got := set.Get(stranger); got.State != domain.PresenceUnknown {
		t.Fatalf("non-contact lookup: got %s, want unknown", got)
	}
}

// Dropping a contact must drop their record, or the projector becomes the kind
// of map that only ever grows — the leak class this codebase has already paid
// for twice (ban domain, seenReceipts).
func TestForgottenContactLeavesNoRecord(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(4)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, 90*time.Second)
	projector.project(presenceTestInputs(now, routes, nil))

	empty := presenceTestInputs(now.Add(time.Second), map[domain.PeerIdentity]presenceRouteState{}, nil)
	projector.project(empty)

	if got := projector.recordCount(); got != 0 {
		t.Fatalf("records after the contact went away: got %d, want 0", got)
	}
}

// The delivery occasion is part of the SAME write as the doubt that opens it.
//
// A counter bumped from outside — before or after the projector call — leaves a
// window in which a proof publishes a return that spends an occasion nobody has
// opened yet, and for a contact reachable through transit nothing else ever
// opens one. No ordering of two statements closes that; one lock does.
//
// Returns must not move it. That is what stops one physical return, seen by the
// delivery pass and then by a proof, from earning two accelerated attempts.
func TestTheDepartureCountMovesWithTheDoubtAndNotWithTheReturn(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(16)
	projector := newPresenceProjector()

	if got := projector.departuresFor(contact); got != 0 {
		t.Fatalf("a contact nothing is known about has departed %d times", got)
	}

	projector.noteProbeUnanswered(contact, now)
	if got := projector.departuresFor(contact); got != 1 {
		t.Fatalf("a silent probe is a departure: got %d, want 1", got)
	}
	if !projector.noteSessionClosed(contact, now.Add(time.Second), 1) {
		t.Fatal("a close newer than the evidence must be recorded")
	}
	if got := projector.departuresFor(contact); got != 2 {
		t.Fatalf("a session close is a departure: got %d, want 2", got)
	}

	projector.noteProof(contact, now.Add(2*time.Second), presenceAliveValidity)
	projector.notePassive(contact, now.Add(3*time.Second), presenceAliveValidity)
	if got := projector.departuresFor(contact); got != 2 {
		t.Fatalf("a return moved the departure count to %d: one return would then "+
			"earn one accelerated attempt per observer of it", got)
	}
}

// A close cannot overwrite evidence NEWER than itself.
//
// The two events reach the projector from different goroutines — a session
// teardown and an answered probe — and nothing orders them but their own
// observation times. Applied blind, a close recorded a moment before a proof
// wipes that proof: a contact who has just signed for us reads `probing` or
// `offline` until somebody asks again, which for a contact that cannot be
// probed is until their route moves.
func TestACloseOlderThanTheEvidenceIsDropped(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(17)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	projector.noteProof(contact, now, presenceAliveValidity)

	// Observed BEFORE the proof, applied after it: older news.
	if projector.noteSessionClosed(contact, now.Add(-time.Second), 1) {
		t.Fatal("a close older than the evidence must not be recorded")
	}
	got := projector.project(presenceTestInputs(now.Add(time.Second), routes, nil)).Get(contact)
	if got.State != domain.PresenceOnline || got.Source != domain.PresenceSourceProof {
		t.Fatalf("a stale close wiped a live proof: got %v", got)
	}
	if departures := projector.departuresFor(contact); departures != 0 {
		t.Fatalf("a dropped close still counted as a departure: got %d", departures)
	}

	// And the control: a close observed AFTER the proof is real news.
	if !projector.noteSessionClosed(contact, now.Add(2*time.Second), 2) {
		t.Fatal("a close newer than the evidence must be recorded")
	}
	got = projector.project(presenceTestInputs(now.Add(3*time.Second), routes, nil)).Get(contact)
	if got.State == domain.PresenceOnline {
		t.Fatalf("a close newer than the evidence left the contact green: got %v", got)
	}
}

// TestElapsedTakesWhicheverClockSawMoreTime.
//
// Presence measures every duration with one rule, and the rule has to answer
// two failures that pull in opposite directions:
//
//   - SUSPEND. Go documents that the monotonic clock stops while the machine
//     sleeps on some systems, so a laptop closed for three hours reports zero
//     elapsed. On the monotonic delta alone a proof stays inside its 450 s
//     window, an open probe never times out, and the cadence resumes as if
//     nothing happened — the long false green this feature exists to remove,
//     in the normal case for a laptop.
//   - A WALL CLOCK STEPPED BACK. On the wall delta alone that extends a
//     validity window and stalls every timeout until real time catches up.
//
// Asserted over durations rather than over instants because the two readings
// cannot be made to disagree from inside a test: only the runtime produces a
// divergence. What is testable is which of them wins, and that is this.
func TestElapsedTakesWhicheverClockSawMoreTime(t *testing.T) {
	for _, tc := range []struct {
		name      string
		monotonic time.Duration
		wall      time.Duration
		want      time.Duration
	}{
		{"the machine slept: monotonic froze, the wall moved", 0, 3 * time.Hour, 3 * time.Hour},
		{"the wall clock stepped back an hour", 450 * time.Second, -time.Hour, 450 * time.Second},
		{"both agree, as they do in ordinary running", time.Minute, time.Minute, time.Minute},
		{"the wall crept ahead", time.Minute, time.Minute + time.Second, time.Minute + time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := longerElapsed(tc.monotonic, tc.wall); got != tc.want {
				t.Fatalf("elapsed: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStaleEvidenceCannotEraseANewerAbsence is the mirror of
// TestACloseOlderThanTheEvidenceIsDropped, and it was missing for a round.
//
// Evidence of life and evidence of absence arrive from goroutines that do not
// take turns. The message ingest observes a signed frame, is descheduled before
// the projector's mutex, a probe times out or a session closes in the meantime,
// and the older observation lands on top. Applied blind it clears the close and
// the strikes and opens a fresh 450 s window, so a contact who has genuinely
// left reads `online` for another seven minutes — the exact false green this
// feature exists to remove, produced by the feature itself.
func TestStaleEvidenceCannotEraseANewerAbsence(t *testing.T) {
	now := presenceInstantAt(time.Now())

	for _, tc := range []struct {
		name     string
		absence  func(*presenceProjector, domain.PeerIdentity, presenceInstant)
		evidence func(*presenceProjector, domain.PeerIdentity, presenceInstant) bool
	}{
		{
			name: "a session close, then a proof observed before it",
			absence: func(p *presenceProjector, c domain.PeerIdentity, at presenceInstant) {
				p.noteSessionClosed(c, at, 1)
			},
			evidence: func(p *presenceProjector, c domain.PeerIdentity, at presenceInstant) bool {
				return p.noteProof(c, at, presenceAliveValidity)
			},
		},
		{
			name: "a probe given up on, then a frame observed before it",
			absence: func(p *presenceProjector, c domain.PeerIdentity, at presenceInstant) {
				p.noteProbeUnanswered(c, at)
			},
			evidence: func(p *presenceProjector, c domain.PeerIdentity, at presenceInstant) bool {
				return p.notePassive(c, at, presenceAliveValidity)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contact := presenceTestContact(18)
			projector := newPresenceProjector()
			routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRouteAbsent}

			// Observed FIRST, applied SECOND.
			observedEarlier := now
			// Observed SECOND, applied FIRST.
			tc.absence(projector, contact, now.Add(time.Second))

			if changed := tc.evidence(projector, contact, observedEarlier); changed {
				t.Fatal("evidence older than the absence it follows was applied")
			}
			got := projector.project(presenceTestInputs(now.Add(2*time.Second), routes, nil)).Get(contact)
			if got.State == domain.PresenceOnline {
				t.Fatalf("stale evidence put a departed contact back online for the "+
					"whole validity window: got %v", got)
			}

			// The control: evidence observed AFTER the absence is real news and
			// does clear it.
			if changed := tc.evidence(projector, contact, now.Add(2*time.Second)); !changed {
				t.Fatal("evidence newer than the absence must be applied")
			}
			got = projector.project(presenceTestInputs(now.Add(3*time.Second), routes, nil)).Get(contact)
			if got.State != domain.PresenceOnline {
				t.Fatalf("evidence newer than the absence left the contact absent: got %v", got)
			}
		})
	}
}

// TestAStaleStrikeCannotLandOnNewerEvidence is the third direction of the same
// rule, and it was missing after the first two were written by hand.
//
// The prober stamps its sweep, walks its attempts, and reaches this contact
// some time later; a signed frame or a proof observed in between is applied
// first. The strike then lands on top of evidence NEWER than itself, which
// breaks the consecutive-probe semantics the whole detection rests on — the
// contact can reach `offline` after two further misses instead of three — and
// moves the delivery occasion counter for a departure that has already been
// contradicted.
//
// The gate that stops it is written once (presenceRecord.stale) and applied by
// the single path into the record, which is why there is no fourth direction
// left to forget.
func TestAStaleStrikeCannotLandOnNewerEvidence(t *testing.T) {
	now := presenceInstantAt(time.Now())
	contact := presenceTestContact(19)
	projector := newPresenceProjector()
	routes := map[domain.PeerIdentity]presenceRouteState{contact: presenceRoutePresent}

	sweptAt := now
	// Observed after the sweep started, applied before it reached this contact.
	projector.noteProof(contact, now.Add(time.Second), presenceAliveValidity)

	projector.noteProbeUnanswered(contact, sweptAt)

	got := projector.project(presenceTestInputs(now.Add(2*time.Second), routes, nil)).Get(contact)
	if got.State != domain.PresenceOnline {
		t.Fatalf("a strike older than the proof it followed was recorded: got %v", got)
	}
	if departures := projector.departuresFor(contact); departures != 0 {
		t.Fatalf("a contradicted departure still opened a delivery occasion: got %d", departures)
	}

	// The control: a strike observed AFTER the proof is real news.
	projector.noteProbeUnanswered(contact, now.Add(3*time.Second))
	if departures := projector.departuresFor(contact); departures != 1 {
		t.Fatalf("a strike newer than the proof was dropped: departures %d", departures)
	}
}

// presenceRecordWritersOutside lists the functions in a source file that assign
// into the per-contact record map, other than the ones named as allowed. Split
// out so the guard below can be shown to fail.
func presenceRecordWritersOutside(source string, allowed ...string) []string {
	permitted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		permitted[name] = struct{}{}
	}
	var offenders []string
	enclosing := ""
	for _, line := range strings.Split(source, "\n") {
		if strings.HasPrefix(line, "func ") {
			enclosing = line
		}
		if !strings.Contains(line, "p.records[") || !strings.Contains(line, "] = ") {
			continue
		}
		named := false
		for name := range permitted {
			if strings.Contains(enclosing, name) {
				named = true
				break
			}
		}
		if !named {
			offenders = append(offenders, strings.TrimSpace(enclosing))
		}
	}
	return offenders
}

// TestOnlyOneFunctionWritesAPresenceRecord.
//
// Every rule that decides whether an observation is still news — its transition
// number, its evidence time against the contrary observation — has to be
// applied in the SAME critical section that writes the result. Splitting them
// was a bug for exactly one round: a close stored its transition number,
// released the mutex, a reconnect took the lock in that window and found no
// close to withdraw, and the close then landed on top of a live session with
// nothing left to re-check it against. The contact sat at `probing`/`offline`
// for the whole life of a session that was up.
//
// Asserted structurally, and deliberately so: a concurrent test does NOT
// discriminate here. The window is one unlock/relock wide, 300 racing pairs
// under -race never entered it, and a test that passes against the broken code
// is worse than no test. What can be checked is that there is only one door —
// noteObservation, which holds the lock across the decision and the write —
// plus project, which rebuilds the whole map under the same lock.
func TestOnlyOneFunctionWritesAPresenceRecord(t *testing.T) {
	source, err := os.ReadFile("presence_projector.go")
	if err != nil {
		t.Fatalf("reading presence_projector.go: %v", err)
	}
	if offenders := presenceRecordWritersOutside(string(source), "noteObservation", "project"); len(offenders) != 0 {
		t.Fatalf("these write a presence record outside the single gated path: %v — "+
			"whatever they decide before or after that write is decided in a "+
			"different critical section, and something else can land in between",
			offenders)
	}
}

// TestTheSingleWriterGuardWouldActuallyFire.
func TestTheSingleWriterGuardWouldActuallyFire(t *testing.T) {
	good := "func (p *presenceProjector) noteObservation(\n\tp.records[identity] = record\n"
	bad := "func (p *presenceProjector) noteSessionTransition(\n\tp.records[identity] = record\n"
	if got := presenceRecordWritersOutside(good, "noteObservation", "project"); len(got) != 0 {
		t.Fatalf("the guard rejects the gated writer: %v", got)
	}
	if got := presenceRecordWritersOutside(bad, "noteObservation", "project"); len(got) != 1 {
		t.Fatalf("the guard accepts a second writer: it guards nothing (got %v)", got)
	}
}
