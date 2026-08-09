package datagram

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_routed_test.go covers the ORDER of §4.1 on the signed plane: what
// is charged before what, what may occupy state and what may not, and the
// exhaustive Release branch list of §4.1 line 454.

// transitFixture is a relay with a route to the destination.
//
// There is no store decorator in it any more, and there cannot be one: the
// pipeline names *BaseReplayCache, so what a test observes about the layer's
// anti-replay calls is the cache's own counters (pipelineNode.replayCalls) and
// the records it holds. That is a stricter observation than the decorator's call
// list — a Commit that was called and then refused shows up in neither.
type transitFixture struct {
	relay   *pipelineNode
	sender  *pipelineNode
	dst     domain.PeerIdentity
	nextHop domain.PeerIdentity
	private ed25519.PrivateKey
	signer  domain.PeerIdentity
	net     *fakeNetwork
}

func newTransitFixture(t *testing.T, transit bool) *transitFixture {
	t.Helper()
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: transit})
	nextHop := newPipelineNode(t, net, nodeOpts{name: "next-hop", transit: true})
	dst := domaintest.ID("far-destination")

	link(sender, relay, false, transit)
	link(relay, nextHop, transit, true)
	route(relay, dst, nextHop.id, 3)

	return &transitFixture{
		relay: relay, sender: sender, dst: dst,
		nextHop: nextHop.id, private: private, signer: signer, net: net,
	}
}

// settleKeyOf stages the record a PREVIOUS instance of this frame would have
// left behind: the same arrival, committed, in the relay's own cache. It is how
// a test says "the early probe of step 6 meets a hit" now that nothing can be
// scripted into the memory.
func (f *transitFixture) settleKeyOf(t *testing.T, frame protocol.DatagramFrame) {
	t.Helper()
	settleReplayKey(t, f.relay.replay, replayKeyOf(t, frame),
		ProvenIngress(testChannel(f.sender.id.String()), f.sender.id),
		f.relay.clock().Add(time.Minute))
}

func (f *transitFixture) frame(t *testing.T) protocol.DatagramFrame {
	t.Helper()
	return signedRouted(t, routedOpts{
		private: f.private, src: f.signer, dst: f.dst, now: f.relay.clock(),
	})
}

// ---------------------------------------------------------------------------
// Step order
// ---------------------------------------------------------------------------

// TestUnbillableArrivalIsRefusedBeforeParsingAndBeforeCrypto pins the ONE
// budget precondition the conveyor enforces itself: stage one is charged by the
// owner of the receive path, so a caller that hands over a frame WITHOUT the key
// it charged has either skipped the charge or lost the key, and either way stage
// two below has no bucket to bill.
//
// The line is not a valid frame, and the discriminator is the error and the ban
// verdict rather than the reason: delete the BudgetKey gate and the same line
// reaches the strict parser, which refuses it as a stable-header violation —
// ban-worthy, and with an error of its own. So the assertions below cannot hold
// on a build without the gate.
//
// The mutation this kills: dropping the `in.BudgetKey.IsZero()` branch from
// HandleInbound.
func TestUnbillableArrivalIsRefusedBeforeParsingAndBeforeCrypto(t *testing.T) {
	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "node"})

	result := node.pipeline.HandleInbound(context.Background(), InboundOpts{
		Line: []byte("{not even json\n"),
		Peer: domaintest.ID("noisy"),
	})
	requireDrop(t, result, DropMalformed)
	if !errors.Is(result.Err(), errInboundNoBudgetKey) {
		t.Fatalf("err = %v, want the unbillable-arrival refusal: the line reached the parser", result.Err())
	}
	if result.BanWorthy() {
		t.Fatal("a caller that named no budget key is this node's bug, not the neighbour's")
	}
	if node.crypto.charged() != 0 {
		t.Fatal("no verification may be charged for a frame nobody can be billed for")
	}

	// The positive control: the SAME line with a key does reach the parser, and
	// comes back as the ban-worthy refusal that proves the gate above is what
	// stopped it the first time.
	parsed := node.pipeline.HandleInbound(context.Background(), InboundOpts{
		Line:      []byte("{not even json\n"),
		Peer:      domaintest.ID("noisy"),
		Channel:   testChannel("noisy"),
		BudgetKey: ProvenIdentityKey(domaintest.ID("noisy")),
	})
	requireDrop(t, parsed, DropMalformed)
	if !parsed.BanWorthy() {
		t.Fatal("a billable garbage line must still be the parser's ban-worthy refusal")
	}
}

// TestOversizeLineIsADropAndNeverABan pins the price of the §2.3 size rule
// INSIDE the layer, on the one path that can still reach the parser with a line
// past MaxFrameLine.
//
// A frame too large is not a stable-header violation: the neighbour that handed
// it over did not write it, and nothing in the envelope tells it how big the
// frame was when the previous hop read it. §4.4 reserves punishment for what
// every transit is obliged to CHECK about a frame it forwards, so the size gate
// answers with a drop under its own reason and nothing else.
//
// The mutation this kills: folding ErrFrameTooLarge back into parseRefusal's
// default branch, where it becomes droppedWithBan(DropMalformed).
func TestOversizeLineIsADropAndNeverABan(t *testing.T) {
	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "node"})
	peer := domaintest.ID("wide")

	line := `{"type":"datagram","v":2,"payload":"` +
		string(make([]byte, protocol.MaxFrameLine)) + `"}` + "\n"
	result := node.pipeline.HandleInbound(context.Background(), InboundOpts{
		Line:      []byte(line),
		Peer:      peer,
		Channel:   testChannel("wide"),
		BudgetKey: ProvenIdentityKey(peer),
	})
	requireDrop(t, result, DropFrameTooLarge)
	if result.BanWorthy() {
		t.Fatal("an oversize line charged ban points: the neighbour that relayed a frame is not its author")
	}
	if !errors.Is(result.Err(), protocol.ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", result.Err())
	}
}

// TestTheEnvelopeCarriesNoPathRequirement is the removal of the self-gate
// stated as behaviour, not as an absent branch.
//
// A node used to drop a frame whose `req_caps` named anything it did not
// advertise — for transit AND for a frame addressed to itself. That is the
// mechanism by which an old relay refuses a protocol released after it, so the
// field is gone and there is nothing left in the envelope for a receiver to
// refuse over. A node that advertises the two role names and nothing else
// delivers what is addressed to it.
func TestTheEnvelopeCarriesNoPathRequirement(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	node := newPipelineNode(t, net, nodeOpts{name: "plain", transit: true})
	handler := acceptingHandler()
	registerType(t, node, routedType(dtypePush, handler))

	result := node.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: node.id, now: node.clock(),
	}))
	requireOutcome(t, result, InboundDelivered)
	if handler.callCount() != 1 {
		t.Fatalf("handler calls = %d, want the frame delivered", handler.callCount())
	}
}

// TestUndeliverableFrameNeverEntersTheCache is one of the three §9 cases in
// which the anti-replay key must NOT be cached.
func TestUndeliverableFrameNeverEntersTheCache(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})

	result := relay.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: domaintest.ID("unreachable"), now: relay.clock(),
	}))
	requireDrop(t, result, DropUndeliverable)
	if relay.replay.Len() != 0 {
		t.Fatal("an undeliverable frame must not occupy a replay slot")
	}
	if relay.crypto.charged() != 0 {
		t.Fatal("a frame with nowhere to go must not be paid for with a verification")
	}
}

// TestForgedSignatureNeverEntersTheCacheAndCarriesBan is the second §9 case,
// and at the same time the pre-poison test: the forged frame has the SAME
// transcript — hence the same replay key — as the genuine one, so if the key
// were cached before verification the genuine frame would be silenced.
func TestForgedSignatureNeverEntersTheCacheAndCarriesBan(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	genuine := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
	})
	poisoned := genuine.Clone()
	poisoned.Auth.Sig[0] ^= 0xFF

	result := receiver.deliver(t, sender.id, poisoned)
	requireDrop(t, result, DropSignature)
	if !result.BanWorthy() {
		t.Fatal("a forged signature is exactly what ban points are for (§4.4)")
	}
	if receiver.replay.Len() != 0 {
		t.Fatal("an unauthentic frame must not occupy a replay slot")
	}

	// The genuine frame, same transcript and same key, still gets through.
	requireOutcome(t, receiver.deliver(t, sender.id, genuine), InboundDelivered)
	if handler.callCount() != 1 {
		t.Fatalf("the genuine frame reached the handler %d times, want 1", handler.callCount())
	}
}

// TestFingerprintMismatchIsBanWorthy pins the other ban-worthy auth violation
// of §4.4: the frame carries a key that does not fingerprint to src, and the
// refusal happens before any replay slot is taken.
func TestFingerprintMismatchIsBanWorthy(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	registerType(t, receiver, routedType(dtypePush, acceptingHandler()))

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
	})
	stranger, _ := newSigner(t)
	frame.Auth.PubKey = []byte(stranger.Public().(ed25519.PublicKey))

	result := receiver.deliver(t, sender.id, frame)
	requireDrop(t, result, DropFingerprint)
	if !result.BanWorthy() {
		t.Fatal("a key that does not fingerprint to src is a stable-header violation")
	}
	if receiver.replay.Len() != 0 {
		t.Fatal("the refusal precedes the reservation")
	}
	if receiver.crypto.charged() != 0 {
		t.Fatal("the fingerprint check is a hash and runs before the verification token")
	}
}

// TestAuthorizationRejectDoesNotCommitTheKey is the third §9 case: an
// authentic but untrusted sender must not evict other people's records.
func TestAuthorizationRejectDoesNotCommitTheKey(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registration := routedType(dtypePush, handler)
	registration.Authorizer = AuthorizerFunc(
		func(_ context.Context, _ DeliveryContext, _ []byte) AuthorizationDecision {
			return Reject(errors.New("not trusted"))
		})
	registerType(t, receiver, registration)

	result := receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
	}))
	requireDrop(t, result, DropUnauthorized)
	if result.BanWorthy() {
		t.Fatal("a refused authorization is a silent drop, not a punishable violation")
	}
	if handler.callCount() != 0 {
		t.Fatal("a rejected frame must not reach the handler")
	}
	if receiver.replay.Len() != 0 {
		t.Fatal("`reject` must not commit the replay key (§7)")
	}
}

// TestAuthorizationHookSeesTheAuthenticatedSession is the push_identity rule of
// §7: the hook can refuse a frame whose payload names an address other than the
// identity of the session it arrived on — and it does so BEFORE the commit.
func TestAuthorizationHookSeesTheAuthenticatedSession(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()

	registration := routedType(dtypePush, handler)
	registration.Authorizer = AuthorizerFunc(
		func(_ context.Context, delivery DeliveryContext, payload []byte) AuthorizationDecision {
			// "record.address" of the payload, modelled as the raw bytes.
			claimed, err := domain.ParsePeerIdentity(string(payload))
			if err != nil {
				return Reject(err)
			}
			peer, remote := delivery.IncomingPeer().Identity()
			if !remote || peer != claimed {
				return Reject(errors.New("record.address does not match the session identity"))
			}
			return Accept()
		})
	registerType(t, receiver, registration)

	stranger := domaintest.ID("stranger")
	refused := receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
		payload: []byte(stranger.String()),
	}))
	requireDrop(t, refused, DropUnauthorized)
	if receiver.replay.Len() != 0 {
		t.Fatal("the refusal happened before the commit")
	}

	accepted := receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
		payload: []byte(sender.id.String()),
	}))
	requireOutcome(t, accepted, InboundDelivered)
}

// TestUnknownDTypeNeverReachesTheHookAndTakesNoSlot is §7's last clause.
func TestUnknownDTypeNeverReachesTheHookAndTakesNoSlot(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})

	authorized := 0
	registration := routedType(dtypePush, acceptingHandler())
	registration.Authorizer = AuthorizerFunc(
		func(context.Context, DeliveryContext, []byte) AuthorizationDecision {
			authorized++
			return Accept()
		})
	registerType(t, receiver, registration)

	result := receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
		dtype: dtypeUnrelated,
	}))
	requireDrop(t, result, DropUnknownDType)
	if result.BanWorthy() {
		t.Fatal("an unknown dtype keeps the connection alive and charges no ban")
	}
	if authorized != 0 {
		t.Fatal("an unknown dtype must not reach the authorization hook")
	}
	if receiver.replay.Len() != 0 {
		t.Fatal("an unknown dtype must not occupy the replay cache")
	}
	if types := receiver.metrics.unknownTypes(); len(types) != 1 || types[0] != dtypeUnrelated {
		t.Fatalf("the unknown type metric recorded %v", types)
	}
	// Everything the layer refuses is refused silently on the wire, so the
	// metric is the whole observable surface of the decision.
	reasons := receiver.metrics.dropReasons()
	outcomes := receiver.metrics.observedOutcomes()
	if len(reasons) != 1 || reasons[0] != DropUnknownDType {
		t.Fatalf("drop reasons %v", reasons)
	}
	if len(outcomes) != 1 || outcomes[0] != InboundDropped {
		t.Fatalf("outcomes %v", outcomes)
	}
}

// TestModeDemotionIsRefusedByTheRegistry is §3.6 and §9: stripping auth from a
// signed routed datagram and passing it off as a request does not get it
// accepted by a type that declared `routed` only.
func TestModeDemotionIsRefusedByTheRegistry(t *testing.T) {
	net := newFakeNetwork()
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	demoted := requestFrame(t, requestOpts{
		label: newLabel(t, "demoted"), dst: receiver.id, dtype: dtypePush,
	})
	requireDrop(t, receiver.deliver(t, domaintest.ID("relay"), demoted), DropModeNotAllowedForType)
	if handler.callCount() != 0 {
		t.Fatal("a demoted frame must not reach a routed-only type")
	}
	// And the way back is closed by the matrix: a routed frame without auth is
	// refused by the parser itself, so it can never re-enter the signed plane.
	unsigned := protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRouted,
		Class: domain.DatagramClassControl, Src: domaintest.ID("src"), Dst: receiver.id,
		TTL: 5, RoutePolicy: domain.RoutePolicyBest, DType: dtypePush,
	}
	if err := unsigned.Validate(); err == nil {
		t.Fatal("a routed frame without auth must not validate")
	}
}

// TestTransitGateDropsBeforeReservation is §9: a node without
// mesh_datagram_transit_v1 drops somebody else's frame before the reservation
// and takes no replay slot, EVEN THOUGH it has a route to the destination.
func TestTransitGateDropsBeforeReservation(t *testing.T) {
	fixture := newTransitFixture(t, false)

	result := fixture.relay.deliver(t, fixture.sender.id, fixture.frame(t))
	requireDrop(t, result, DropTransitGate)

	assertNoReservationTouched(t, fixture.relay)
	if fixture.relay.replay.Len() != 0 {
		t.Fatal("no replay slot may be taken")
	}

	// And the early Has still stands BEFORE the gate, which is what keeps it a
	// cheap sieve rather than state: a key the cache already holds ends the same
	// frame as a duplicate instead of at the gate.
	repeat := fixture.frame(t)
	fixture.settleKeyOf(t, repeat)
	requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, repeat), DropReplayDuplicate)
}

// TestCryptoTokenIsChargedOnlyImmediatelyBeforeVerify is §9: a frame sieved by
// the early Has, by `ttl <= max_ttl` or by the validity window spends no token.
func TestCryptoTokenIsChargedOnlyImmediatelyBeforeVerify(t *testing.T) {
	t.Run("early Has", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		frame := fixture.frame(t)
		fixture.settleKeyOf(t, frame)
		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropReplayDuplicate)
		if fixture.relay.crypto.charged() != 0 {
			t.Fatal("a duplicate must not pay for Ed25519")
		}
	})

	t.Run("ttl above the signed budget", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		frame := signedRouted(t, routedOpts{
			private: fixture.private, src: fixture.signer, dst: fixture.dst,
			now: fixture.relay.clock(), ttl: 200, maxTTL: 10,
		})
		result := fixture.relay.deliver(t, fixture.sender.id, frame)
		requireDrop(t, result, DropTTLBudget)
		if !result.BanWorthy() {
			t.Fatal("ttl above the SIGNED budget is a stable-header violation")
		}
		if fixture.relay.crypto.charged() != 0 {
			t.Fatal("the cheap raw-value check runs before the token is charged")
		}
	})

	t.Run("stale by the validity window", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		frame := signedRouted(t, routedOpts{
			private: fixture.private, src: fixture.signer, dst: fixture.dst,
			now: fixture.relay.clock().Add(-time.Hour),
		})
		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropStale)
		if fixture.relay.crypto.charged() != 0 {
			t.Fatal("the timing rule is nanoseconds and runs before the token is charged")
		}
	})

	t.Run("a good frame pays exactly one token", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, fixture.frame(t)), InboundForwarded)
		if got := fixture.relay.crypto.charged(); got != 1 {
			t.Fatalf("charged %d tokens, want exactly 1", got)
		}
	})

	t.Run("an exhausted budget refuses without verifying", func(t *testing.T) {
		net := newFakeNetwork()
		private, signer := newSigner(t)
		sender := newPipelineNode(t, net, nodeOpts{id: signer})
		receiver := newPipelineNode(t, net, nodeOpts{name: "receiver", cryptoBudget: 1})
		registerType(t, receiver, routedType(dtypePush, acceptingHandler()))

		first := signedRouted(t, routedOpts{private: private, src: signer, dst: receiver.id, now: receiver.clock()})
		second := signedRouted(t, routedOpts{private: private, src: signer, dst: receiver.id, now: receiver.clock()})
		requireOutcome(t, receiver.deliver(t, sender.id, first), InboundDelivered)
		requireDrop(t, receiver.deliver(t, sender.id, second), DropCryptoBudget)
		if receiver.replay.Len() != 1 {
			t.Fatal("the refused frame must not occupy a slot")
		}
	})
}

// TestSpentSendWindowRefusesOnlyTheTransitPath is §2.2 on the outcome the
// conveyor used to ignore: a frame still inside its validity interval whose
// clamped send_until is already behind now.
//
// §2.2 says such a frame is not enqueued AT ALL, so the refusal has to happen
// where every other timing refusal happens — before the signature and before
// the anti-replay reservation. Left unhandled it was the most expensive drop
// the layer has: an Ed25519 verification, a crypto token and a replay slot are
// all spent, every candidate is walked, and the frame dies at the writer queue
// as `forward_failed`, which reads as backpressure at the next hop rather than
// as a timing refusal.
//
// The second half is the reason the check cannot simply be added to the switch:
// the same outcome on a frame addressed HERE is not a refusal at all. There is
// no socket write ahead of it, so nothing has run out.
func TestSpentSendWindowRefusesOnlyTheTransitPath(t *testing.T) {
	// Past send_until — the validity window less the one-minute send grace —
	// and still a second short of valid_until, so the frame is alive.
	const pastTheSendWindow = 5*time.Minute - time.Second

	t.Run("transit", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		frame := fixture.frame(t)
		fixture.relay.advance(pastTheSendWindow)

		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropSendWindowExpired)
		if charged := fixture.relay.crypto.charged(); charged != 0 {
			t.Fatalf("%d verification tokens paid for a frame §2.2 forbids enqueueing at all", charged)
		}
		assertNoReservationTouched(t, fixture.relay)
		if fixture.relay.replay.Len() != 0 {
			t.Fatal("the frame occupied an anti-replay slot it may never use")
		}
		if handed := len(fixture.net.journal()); handed != 0 {
			t.Fatalf("the frame was handed to %d neighbours after its send window had gone", handed)
		}
	})

	t.Run("addressed to this node", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		registerType(t, fixture.relay, routedType(dtypePush, acceptingHandler()))
		frame := signedRouted(t, routedOpts{
			private: fixture.private, src: fixture.signer,
			dst: fixture.relay.id, now: fixture.relay.clock(),
		})
		fixture.relay.advance(pastTheSendWindow)

		requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, frame), InboundDelivered)
	})
}

// TestReplayHitDropsBeforeCryptography pins the position of the early probe:
// a key the cache already holds ends the frame at step 6, before the layer pays
// for a signature verification and before it takes a reservation.
//
// The probe used to have a third answer — a read failure of a durable store —
// and this test used to pin that one. The cache is in memory and hit-or-miss is
// all it can say, so what is left to pin is that a HIT stops the conveyor at the
// same place the failure used to.
func TestReplayHitDropsBeforeCryptography(t *testing.T) {
	fixture := newTransitFixture(t, true)
	frame := fixture.frame(t)
	// The hit is a REAL record of the same key, settled the way the layer settles
	// one. Nothing can be scripted into the memory any more, and nothing needs to
	// be: what step 6 reads is the state, and this is that state.
	fixture.settleKeyOf(t, frame)
	staged := fixture.relay.replayCalls()

	requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropReplayDuplicate)
	if fixture.relay.crypto.charged() != 0 {
		t.Fatal("the frame is dropped before cryptography")
	}
	assertNoReservationSince(t, fixture.relay, staged)
}

// ---------------------------------------------------------------------------
// Reserve and Release
// ---------------------------------------------------------------------------

// TestReserveStandsAfterEveryReadOnlyDecision is §4.1 line 452 and §9: every
// decision that can end a frame BEFORE the reservation happens before it, so
// none of them occupies a slot — and none of them calls Release.
//
// The list used to be longer, and every entry that left was a hook a TRANSIT
// ran over somebody else's frame: an interceptor's `drop`, HandleTransit's
// `drop` and `failed`, an empty ResolveNextHops. What is left is the layer's own
// arithmetic.
func TestReserveStandsAfterEveryReadOnlyDecision(t *testing.T) {
	t.Run("no candidates", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		// The only route points back at the neighbour the frame came from, so
		// split-horizon empties the candidate list.
		fixture.relay.routes.set(fixture.dst, RouteHint{NextHop: fixture.sender.id, Hops: 2})
		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, fixture.frame(t)), DropUndeliverable)
		assertNoReservationTouched(t, fixture.relay)
	})

	t.Run("stale", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		frame := fixture.frame(t)
		// Past the base replay window the relay can no longer recognise a
		// repeat, so it refuses to carry the frame at all — before any state.
		fixture.relay.advance(domain.DatagramBaseReplayWindow + time.Minute)
		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropStale)
		assertNoReservationTouched(t, fixture.relay)
	})
}

func assertNoReservationTouched(t *testing.T, node *pipelineNode) {
	t.Helper()
	assertNoReservationSince(t, node, replayCalls{})
}

// assertNoReservationSince is the same statement against a cache a test has
// already staged records in: what it pins is that the LAYER added nothing.
func assertNoReservationSince(t *testing.T, node *pipelineNode, before replayCalls) {
	t.Helper()
	calls := node.replayCalls()
	if calls.reserves != before.reserves {
		t.Fatalf("a read-only decision took %d reservations", calls.reserves-before.reserves)
	}
	if calls.releases != before.releases {
		t.Fatalf("a branch before the reservation called Release %d times — it would strip a "+
			"reservation a parallel instance of the same frame is holding", calls.releases-before.releases)
	}
}

// TestBranchBeforeReserveDoesNotStripAParallelReservation is the explicit §9
// test: one instance of a frame is refused before the reservation while a
// parallel instance holds the reservation of the very same key — and that
// reservation survives, because branches before Reserve never call Release.
//
// The parallel holder is built the way the CONVEYOR builds one, out of the same
// arrival, and that is the whole difference between this test and a decorative
// one: HandleInbound always names an admission key, so every ingress the
// pipeline builds lands in the BILLED ingressOwner bucket. A reservation staged
// in a bucket the drop below never walks cannot be stripped by it, whatever the
// code does, so the assertion would hold on a build that called Release on every
// branch.
//
// The holder is taken from INSIDE the route lookup, which is the window §4.1
// line 456 is about: the instance under test has already passed its early Has as
// a miss, and the reservation appears before the branch that ends it. Staging it
// earlier would make the early probe a hit and the frame would never reach the
// branch at all.
func TestBranchBeforeReserveDoesNotStripAParallelReservation(t *testing.T) {
	fixture := newTransitFixture(t, true)
	frame := fixture.frame(t)
	key := replayKeyOf(t, frame)

	// The SAME arrival the drop below is driven by, put through the conveyor's
	// own derivation rather than through a constructor chosen by hand.
	parallel := inboundFrame{
		frame:     frame,
		peer:      fixture.sender.id,
		channel:   testChannel(fixture.sender.id.String()),
		budgetKey: ProvenIdentityKey(fixture.sender.id),
	}.ingress()
	if kind := parallel.owner().kind(); kind != ingressOwnerBilled {
		t.Fatalf("the parallel instance was staged in the %s bucket: the receive path bills every "+
			"arrival, so a holder outside the billed bucket is one no drop could reach", kind)
	}

	var rsv ReservationToken
	var held bool
	fixture.relay.routes.onEveryLookup(func() {
		if held {
			return
		}
		rsv, held = fixture.relay.replay.Reserve(context.Background(), key,
			parallel, fixture.relay.clock().Add(time.Minute)).Reservation()
	})

	// The route is gone, so this instance dies at the deliverability sieve —
	// which stands before the reservation.
	fixture.relay.routes.set(fixture.dst, RouteHint{NextHop: fixture.sender.id, Hops: 2})
	requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, frame), DropUndeliverable)
	if !held {
		t.Fatal("the parallel reservation was never taken: the premise of this test never armed")
	}

	// The direct observation: the branch called no Release at all, and took no
	// reservation of its own — the one the counters show is the parallel holder's.
	assertNoReservationSince(t, fixture.relay, replayCalls{reserves: 1})

	// And the reservation is still the one that was taken: committing it works.
	if !fixture.relay.replay.Commit(context.Background(), rsv).IsApplied() {
		t.Fatal("the drop before Reserve stripped a reservation held by a parallel handler")
	}
}

// TestReleaseOnEveryFailureAfterReservation walks the §4.1 line 454 list that
// the routed conveyor owns. Only one branch is left on the transit path — every
// candidate refusing — because the frame is never written anywhere: the
// StoreIfAbsent rows this test used to carry disappeared with the `store`
// verdict, not because they stopped being checked.
func TestReleaseOnEveryFailureAfterReservation(t *testing.T) {
	t.Run("every candidate refuses", func(t *testing.T) {
		fixture := newTransitFixture(t, true)
		fixture.net.refuseQueue(fixture.nextHop)

		// §4.3 keeps this apart from "no route": admitted candidates existed
		// and the enqueue failed, which is a temporary LOCAL failure — the
		// drop reason is forward_failed, and the reservation is still given
		// back, which is what §4.1 line 454 demands.
		requireDrop(t, fixture.relay.deliver(t, fixture.sender.id, fixture.frame(t)), DropForwardFailed)
		calls := fixture.relay.replayCalls()
		if calls.reserves != 1 || calls.releases != 1 {
			t.Fatalf("want one reservation and one release, got %d/%d", calls.reserves, calls.releases)
		}
		if fixture.relay.replay.Len() != 0 {
			t.Fatal("the released key must be free again")
		}
	})
}

// TestAFrameWithNowhereToGoNeverPaysForAVerification is §4.1 step 7, and the
// sieve has no exceptions left.
//
// The two it used to have were static flags of a behaviour profile:
// can_store_without_route bought a verification for a frame the relay would
// then refuse to keep, and can_resolve_without_route bought one for a next hop
// out of durable path memory. A stateless forwarder has neither, so a frame the
// routing table cannot place is refused before a signature is checked at all.
func TestAFrameWithNowhereToGoNeverPaysForAVerification(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	link(sender, relay, false, true)

	requireDrop(t, relay.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: domaintest.ID("unreachable"), now: relay.clock(),
	})), DropUndeliverable)

	if charged := relay.crypto.charged(); charged != 0 {
		t.Fatalf("a frame with nowhere to go paid %d verifications", charged)
	}
	if relay.replay.Len() != 0 {
		t.Fatal("a frame sieved out before cryptography must occupy no anti-replay slot")
	}
}

// TestTransitForwardCommitsForwarded is the happy path of step 11.
func TestTransitForwardCommitsForwarded(t *testing.T) {
	fixture := newTransitFixture(t, true)
	frame := fixture.frame(t)

	requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, frame), InboundForwarded)
	calls := fixture.relay.replayCalls()
	if calls.reserves != 1 || calls.releases != 0 {
		t.Fatalf("want one reservation and no release, got %d/%d", calls.reserves, calls.releases)
	}
	// The commit is read off the cache's OWN counter, which is what the outcome
	// table is actually about: a Commit that was called and then refused leaves
	// the counter where it was.
	if calls.commits != 1 {
		t.Fatalf("the transited key was committed %d times, want 1", calls.commits)
	}
	journal := fixture.net.journal()
	if len(journal) != 1 || journal[0].to != fixture.nextHop {
		t.Fatalf("the frame went to %v", journal)
	}
	if got := journal[0].frame.TTL; got != OriginTTL()-1 {
		t.Fatalf("forwarded ttl %d, want one decrement below %d", got, OriginTTL())
	}
}

// TestLocallyCreatedFrameRunsTheSameCycle pins that an outgoing frame gets the
// same Reserve / Commit cycle and the same candidate walk as a transited one,
// with incoming_peer = local.
func TestLocallyCreatedFrameRunsTheSameCycle(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	origin := newPipelineNode(t, net, nodeOpts{id: signer})
	dst := domaintest.ID("far")
	hop := newPipelineNode(t, net, nodeOpts{name: "hop", transit: true})
	link(origin, hop, false, true)
	route(origin, dst, hop.id, 2)

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: dst, now: origin.clock(),
	})
	outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{Frame: frame})
	if outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal: %s (%v)", outcome, outcome.Err())
	}
	if hopChosen, ok := outcome.NextHop(); !ok || hopChosen != hop.id {
		t.Fatalf("queued next hop %v/%v, want %v", hopChosen, ok, hop.id)
	}
	calls := origin.replayCalls()
	if calls.reserves != 1 || calls.releases != 0 {
		t.Fatalf("a local frame runs the same cycle: %d reserves, %d releases", calls.reserves, calls.releases)
	}
	if calls.commits != 1 {
		t.Fatalf("the locally created key was committed %d times, want 1", calls.commits)
	}
	// The origin does not decrement: the first hop receives the full budget.
	journal := net.journal()
	if len(journal) != 1 || journal[0].frame.TTL != OriginTTL() {
		t.Fatalf("the first hop saw ttl %d, want %d", journal[0].frame.TTL, OriginTTL())
	}
}

// TestLocalSendNoRouteDoesNotReserve pins the same ordering on the outgoing
// path: no candidates means no reservation to release.
func TestLocalSendNoRouteDoesNotReserve(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	origin := newPipelineNode(t, net, nodeOpts{id: signer})

	outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: signedRouted(t, routedOpts{
			private: private, src: signer, dst: domaintest.ID("unreachable"), now: origin.clock(),
		}),
	})
	if outcome.Kind() != SendNoRoute {
		t.Fatalf("outcome %s, want no_route", outcome)
	}
	assertNoReservationTouched(t, origin)
}

// ---------------------------------------------------------------------------
// The duplicate branch
// ---------------------------------------------------------------------------

// TestBaseCacheDuplicateStaysASilentDrop is the other half of §9: the
// five-minute plane keeps its cheap silent drop before cryptography.
func TestBaseCacheDuplicateStaysASilentDrop(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
	})
	requireOutcome(t, receiver.deliver(t, sender.id, frame), InboundDelivered)
	charged := receiver.crypto.charged()

	requireDrop(t, receiver.deliver(t, sender.id, frame), DropReplayDuplicate)
	if receiver.crypto.charged() != charged {
		t.Fatal("a base-cache duplicate dies before cryptography")
	}
	if handler.callCount() != 1 {
		t.Fatal("a duplicate must not reach the handler twice")
	}
}

// TestRoutedPlaneNeverTouchesReverseState is the other direction of the §4.1
// plane separation.
func TestRoutedPlaneNeverTouchesReverseState(t *testing.T) {
	fixture := newTransitFixture(t, true)
	requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, fixture.frame(t)), InboundForwarded)
	if fixture.relay.reverse.Len() != 0 {
		t.Fatal("the routed plane must not create reverse state")
	}
	if events := fixture.relay.metrics.reverseEvents(); len(events) != 0 {
		t.Fatalf("the routed plane touched the reverse table: %v", events)
	}
}

// TestCommitForwardedFailureReleasesAndKeepsTheFrameQueued is the transit
// branch of "Commit.fail in all three branches" (§9): the frame is already
// queued and the outcome is FINAL, so the layer releases the key, logs, and
// does not rewrite the outcome — a repeat of the frame will pass and at worst
// yields one duplicate the neighbours' anti-replay puts out.
//
// The failure is not injected into the memory — there is nowhere to inject it —
// but staged as the STATE that makes the layer's Commit answer `fail`: the
// record is gone. The cache reaches that state on its own when the
// abandoned-reservation watchdog reclaims a branch that outlived replay_until
// plus the whole hop budget (baseHeldReservationGrace), and the fixture reaches
// it at the one moment inside forwardRouted where the reservation is held and
// the frame is already at the wire.
func TestCommitForwardedFailureReleasesAndKeepsTheFrameQueued(t *testing.T) {
	fixture := newTransitFixture(t, true)
	frame := fixture.frame(t)
	key := replayKeyOf(t, frame)

	reclaimed := false
	fixture.relay.onBeforeEmit(func() {
		if reclaimed {
			return
		}
		reclaimed = forgetReplayRecord(fixture.relay.replay, key)
	})

	requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, frame), InboundForwarded)
	if !reclaimed {
		t.Fatal("the fixture never reclaimed the reservation: the Commit below cannot have failed")
	}
	calls := fixture.relay.replayCalls()
	if calls.commits != 0 {
		t.Fatalf("the Commit landed after all (%d): the premise of this test never armed", calls.commits)
	}
	if calls.releases != 1 {
		t.Fatalf("Commit(forwarded).fail must Release, got %d releases", calls.releases)
	}
	if len(fixture.net.journal()) != 1 {
		t.Fatal("the frame was queued and stays queued: the outcome is final at enqueue")
	}

	// The key is free again, so the repeat goes through — duplicate over loss.
	fixture.relay.onBeforeEmit(nil)
	requireOutcome(t, fixture.relay.deliver(t, fixture.sender.id, frame), InboundForwarded)
}

// THE `Release.fail` BRANCH IS NOT TESTED HERE ANY MORE, AND CANNOT BE.
//
// TestReleaseFailureLeavesTheKeyOccupied used to stand here. It drove a store
// decorator whose Release answered `fail`, and pinned that the key then stayed
// occupied until replay_until. No memory the layer can be handed does that:
// BaseReplayCache.Release drops the record or reports a stale token as ok, which
// is the ABA guard rather than a failure — and with the store interface gone
// there is no second implementation to answer otherwise. The test was pinning a
// state the type system now forbids, so it went with the branch in
// Pipeline.release that used to log it.
