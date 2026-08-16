package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestIdentityLookupThroughTransitChain is the full §4.2 path: A asks for
// B's keys through transit C, only B answers — with a target_proof bound to
// A's attempt — and A ends with B's verified record on disk and B's keys in
// its knowledge maps.
//
// C is B's DIRECT peer and holds B's record from the initial push of their
// session, which makes it exactly the node the "transit must forward, never
// answer" clause is about: a reply manufactured by C could not carry the
// proof A demands, so the success below proves the answer came from B.
func TestIdentityLookupThroughTransitChain(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)
	addrC := freeAddress(t)

	nodeC, stopC := startEmptyRegistryDatagramNode(t, addrC)
	defer stopC()
	nodeA, _, stopA := startDatagramNode(t, addrA, normalizeAddress(addrC))
	defer stopA()
	nodeB, _, stopB := startDatagramNode(t, addrB, normalizeAddress(addrC))
	defer stopB()

	dstB := domain.PeerIdentityFromWire(nodeB.Address())
	waitForCondition(t, 25*time.Second, func() bool {
		return datagramReachableNow(t, nodeA, dstB, domain.DTypeGetIdentity)
	})
	// The premise of the forward-only clause: the transit REALLY holds B's
	// record (initial push of the B—C session) and still must not answer.
	waitForConditionMsg(t, 25*time.Second, "C never learned B's record via the initial push", func() bool {
		_, _, ok := nodeC.trust.recordFor(testRecordStoreNetwork, dstB)
		return ok
	})

	if _, err := nodeA.identityResolver.StartResolution(dstB, identityIntentReason{Type: identityIntentReasonUIChat}); err != nil {
		t.Fatalf("start resolution: %v", err)
	}

	waitForConditionMsg(t, 25*time.Second, "A never obtained B's authoritative record", func() bool {
		_, _, ok := nodeA.trust.recordFor(testRecordStoreNetwork, dstB)
		return ok
	})

	_, body, _ := nodeA.trust.recordFor(testRecordStoreNetwork, dstB)
	if body.Address != dstB || body.Seq == 0 {
		t.Fatalf("stored record body = %+v, want B's record with a positive seq", body)
	}
	if key, ok := nodeA.knownPubKey(dstB.String()); !ok || key == "" {
		t.Error("B's signing key did not reach A's knowledge maps")
	}
	waitForConditionMsg(t, 10*time.Second, "the resolution never reached its terminal", func() bool {
		nodeA.identityResolver.mu.Lock()
		defer nodeA.identityResolver.mu.Unlock()
		_, running := nodeA.identityResolver.resolutions[dstB]
		return !running
	})
}

// TestInitialIdentityPushExchange: right after auth a datagram-capable pair
// exchanges initial push_identity, so each side can DM the other without a
// network lookup.
func TestInitialIdentityPushExchange(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)

	nodeB, stopB := startEmptyRegistryDatagramNode(t, addrB)
	defer stopB()
	nodeA, stopA := startEmptyRegistryDatagramNode(t, addrA, normalizeAddress(addrB))
	defer stopA()

	idA := domain.PeerIdentityFromWire(nodeA.Address())
	idB := domain.PeerIdentityFromWire(nodeB.Address())

	waitForConditionMsg(t, 25*time.Second, "the pair did not exchange initial push_identity", func() bool {
		_, _, aHasB := nodeA.trust.recordFor(testRecordStoreNetwork, idB)
		_, _, bHasA := nodeB.trust.recordFor(testRecordStoreNetwork, idA)
		return aHasB && bHasA
	})

	if key, ok := nodeA.knownBoxKeyForTest(idB.String()); !ok || key == "" {
		t.Error("B's box key did not reach A via the initial push")
	}
}

// knownBoxKeyForTest reads the knowledge box-key map under its mutex.
func (s *Service) knownBoxKeyForTest(address string) (string, bool) {
	s.knowledgeMu.RLock()
	defer s.knowledgeMu.RUnlock()
	key, ok := s.boxKeys[address]
	return key, ok && key != ""
}

// newGetIdentityDelivery builds the DeliveryContext of a get_identity frame
// addressed to svc, arriving from a proven neighbour.
func newGetIdentityDelivery(t *testing.T, svc *Service, label domain.PeerIdentity, neighbour domain.PeerIdentity) datagram.DeliveryContext {
	t.Helper()
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRequest,
		Class:       domain.DatagramClassControl,
		Src:         label,
		Dst:         domain.PeerIdentityFromWire(svc.identity.Address),
		TTL:         5,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DTypeGetIdentity,
		Payload:     []byte("{}"),
	}
	header, err := datagram.NewDeliveryHeader(frame)
	if err != nil {
		t.Fatalf("delivery header: %v", err)
	}
	delivery, err := datagram.NewDeliveryContext(datagram.DeliveryContextOpts{
		Header:        header,
		IncomingPeer:  datagram.ProvenIngress(datagram.NetworkChannel(domain.ConnID(7)), neighbour),
		LocalIdentity: domain.PeerIdentityFromWire(svc.identity.Address),
	})
	if err != nil {
		t.Fatalf("delivery context: %v", err)
	}
	return delivery
}

func randomAttemptLabel(t *testing.T) domain.PeerIdentity {
	t.Helper()
	var label domain.PeerIdentity
	if _, err := rand.Read(label[:]); err != nil {
		t.Fatalf("entropy: %v", err)
	}
	return label
}

// TestGetIdentityHandlerAnswersWithProof: the owner answers its own record
// and a target_proof bound to the attempt and the request bytes.
func TestGetIdentityHandlerAnswersWithProof(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	handler := &getIdentityHandler{
		svc: svc, network: testRecordStoreNetwork,
		clock:                 func() time.Time { return time.Now().UTC() },
		seenRequesterAttempts: map[domain.PeerIdentity]time.Time{},
	}
	label := randomAttemptLabel(t)
	neighbour := domaintest.ID("n1")
	delivery := newGetIdentityDelivery(t, svc, label, neighbour)

	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V:           domain.IdentityLookupSchemaVersion,
		TargetProof: true,
	})
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	result := handler.Handle(context.Background(), delivery, request)
	answer, ok := result.Response()
	if result.Outcome() != datagram.HandlerAccepted || !ok {
		t.Fatalf("outcome = %s (answer %v), want accepted with an answer", result.Outcome(), ok)
	}
	if answer.DType() != domain.DTypePostIdentity {
		t.Fatalf("answer dtype = %s", answer.DType())
	}

	parsed, err := protocol.ParsePostIdentityPayload(answer.Payload())
	if err != nil {
		t.Fatalf("parse answer: %v", err)
	}
	selfID := domain.PeerIdentityFromWire(svc.identity.Address)
	body, err := protocol.VerifyIdentityRecord(parsed.Record, testRecordStoreNetwork, selfID)
	if err != nil {
		t.Fatalf("verify record: %v", err)
	}
	if err := protocol.VerifyTargetProof(parsed.TargetProof, body, testRecordStoreNetwork, label, sha256Of(request), parsed.Record); err != nil {
		t.Fatalf("verify proof: %v", err)
	}
}

// TestGetIdentityHandlerSilentDrops: the §4.2 silence cases — a requirement
// this build does not understand, a future schema version, malformed bytes.
// The initiator reads silence as silence; there is no refusal frame.
func TestGetIdentityHandlerSilentDrops(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	handler := &getIdentityHandler{
		svc: svc, network: testRecordStoreNetwork,
		clock:                 func() time.Time { return time.Now().UTC() },
		seenRequesterAttempts: map[domain.PeerIdentity]time.Time{},
	}
	delivery := newGetIdentityDelivery(t, svc, randomAttemptLabel(t), domaintest.ID("n1"))

	cases := map[string][]byte{
		"unknown requirement": []byte(`{"v":1,"required":["locators"]}`),
		"future version":      []byte(`{"v":9}`),
		"malformed":           []byte(`{"v":1,`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			result := handler.Handle(context.Background(), delivery, payload)
			if result.Outcome() != datagram.HandlerRejected {
				t.Fatalf("outcome = %s, want rejected (silent drop)", result.Outcome())
			}
			if _, answered := result.Response(); answered {
				t.Fatal("a dropped request must not be answered")
			}
		})
	}
}

// newPushDelivery builds an authorized-path DeliveryContext for a signed
// push_identity frame from `signer`, presented on a session whose identity
// is `presented`.
func newPushDelivery(t *testing.T, svc *Service, signer *identity.Identity, presented domain.PeerIdentity, record protocol.SignedIdentityRecord) (datagram.DeliveryContext, []byte) {
	t.Helper()
	payload, err := protocol.BuildPushIdentityPayload(protocol.PushIdentityPayload{
		V:      domain.IdentityLookupSchemaVersion,
		Record: record,
	})
	if err != nil {
		t.Fatalf("build push payload: %v", err)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("entropy: %v", err)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         domain.PeerIdentityFromWire(signer.Address),
		Dst:         domain.PeerIdentityFromWire(svc.identity.Address),
		TTL:         1,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DTypePushIdentity,
		Payload:     payload,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			PubKey:      append([]byte(nil), signer.PublicKey...),
			Salt:        salt,
			MaxTTL:      1,
			Time:        time.Now().UTC().Unix(),
		},
	}
	signed, err := protocol.SignDatagram(frame, testRecordStoreNetwork, signer.PrivateKey)
	if err != nil {
		t.Fatalf("sign push frame: %v", err)
	}
	header, err := datagram.NewDeliveryHeader(signed)
	if err != nil {
		t.Fatalf("delivery header: %v", err)
	}
	delivery, err := datagram.NewDeliveryContext(datagram.DeliveryContextOpts{
		Header:        header,
		IncomingPeer:  datagram.ProvenIngress(datagram.NetworkChannel(domain.ConnID(9)), presented),
		LocalIdentity: domain.PeerIdentityFromWire(svc.identity.Address),
	})
	if err != nil {
		t.Fatalf("delivery context: %v", err)
	}
	return delivery, payload
}

// TestPushIdentityAuthorizerSessionRule: the record must be the frame
// signer's own AND match the session identity; any other pairing is refused
// before a replay slot is taken.
func TestPushIdentityAuthorizerSessionRule(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	stranger, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)
	strangerID := domain.PeerIdentityFromWire(stranger.Address)
	record, _ := issueTestRecord(t, peer, 1, true)
	authorizer := &pushIdentityAuthorizer{}

	t.Run("own record on own session accepted", func(t *testing.T) {
		delivery, payload := newPushDelivery(t, svc, peer, peerID, record)
		if decision := authorizer.Authorize(context.Background(), delivery, payload); !decision.Accepted() {
			t.Fatalf("refused: %v", decision.Err())
		}
	})

	t.Run("record of a third identity refused", func(t *testing.T) {
		strangerRecord, _ := issueTestRecord(t, stranger, 1, true)
		delivery, payload := newPushDelivery(t, svc, peer, peerID, strangerRecord)
		if decision := authorizer.Authorize(context.Background(), delivery, payload); decision.Accepted() {
			t.Fatal("a peer pushed a record it cannot sign for and was admitted")
		}
	})

	t.Run("session identity mismatch refused", func(t *testing.T) {
		delivery, payload := newPushDelivery(t, svc, peer, strangerID, record)
		if decision := authorizer.Authorize(context.Background(), delivery, payload); decision.Accepted() {
			t.Fatal("a push through a foreign session was admitted")
		}
	})
}

// TestPushIdentityHandlerMergeOutcomes: duplicate and stale are silent
// no-ops, higher seq replaces; the seq counter of the stored record proves
// which happened.
func TestPushIdentityHandlerMergeOutcomes(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)
	handler := &pushIdentityHandler{
		svc: svc, network: testRecordStoreNetwork,
		clock:          func() time.Time { return time.Now().UTC() },
		lastAcceptedAt: map[pushSessionKey]time.Time{},
		violationAt:    map[pushSessionKey]time.Time{},
	}
	// The rate limiter is exercised separately; give each push its own slot.
	admitAll := func() { handler.lastAcceptedAt = map[pushSessionKey]time.Time{} }

	recordSeq2, _ := issueTestRecord(t, peer, 2, true)
	delivery, payload := newPushDelivery(t, svc, peer, peerID, recordSeq2)
	if result := handler.Handle(context.Background(), delivery, payload); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("insert outcome = %s", result.Outcome())
	}

	admitAll()
	if result := handler.Handle(context.Background(), delivery, payload); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("duplicate outcome = %s, want accepted no-op", result.Outcome())
	}

	admitAll()
	recordSeq1, _ := issueTestRecord(t, peer, 1, true)
	staleDelivery, stalePayload := newPushDelivery(t, svc, peer, peerID, recordSeq1)
	if result := handler.Handle(context.Background(), staleDelivery, stalePayload); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("stale outcome = %s, want accepted silent drop", result.Outcome())
	}
	if _, body, _ := svc.trust.recordFor(testRecordStoreNetwork, peerID); body.Seq != 2 {
		t.Fatalf("stored seq = %d, want 2 — stale must not regress", body.Seq)
	}

	admitAll()
	recordSeq3, _ := issueTestRecord(t, peer, 3, true)
	freshDelivery, freshPayload := newPushDelivery(t, svc, peer, peerID, recordSeq3)
	if result := handler.Handle(context.Background(), freshDelivery, freshPayload); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("replace outcome = %s", result.Outcome())
	}
	if _, body, _ := svc.trust.recordFor(testRecordStoreNetwork, peerID); body.Seq != 3 {
		t.Fatalf("stored seq = %d, want 3", body.Seq)
	}
}

// TestPushIdentityRateLimiter: admit → drop → close inside one window;
// a new window admits again.
func TestPushIdentityRateLimiter(t *testing.T) {
	t.Parallel()

	now := time.Unix(1780000000, 0)
	handler := &pushIdentityHandler{
		clock:          func() time.Time { return now },
		lastAcceptedAt: map[pushSessionKey]time.Time{},
		violationAt:    map[pushSessionKey]time.Time{},
	}
	session1 := pushSessionKey{peer: domaintest.ID("p"), conn: domain.ConnID(1)}

	if got := handler.admitPushRate(session1); got != pushRateAdmit {
		t.Fatalf("first push = %v, want admit", got)
	}
	now = now.Add(10 * time.Second)
	if got := handler.admitPushRate(session1); got != pushRateDrop {
		t.Fatalf("second push in window = %v, want drop", got)
	}

	// The limit is PER SESSION: a reconnect (new ConnID) gets a fresh
	// budget — its mandatory initial push must not inherit the previous
	// session's violations.
	session2 := pushSessionKey{peer: session1.peer, conn: domain.ConnID(2)}
	if got := handler.admitPushRate(session2); got != pushRateAdmit {
		t.Fatalf("fresh session inherited the old budget: %v", got)
	}

	now = now.Add(10 * time.Second)
	if got := handler.admitPushRate(session1); got != pushRateCloseSession {
		t.Fatalf("third push in window = %v, want close", got)
	}
	now = now.Add(pushIdentityMinInterval)
	if got := handler.admitPushRate(session1); got != pushRateAdmit {
		t.Fatalf("push after window = %v, want admit", got)
	}
}

// sha256Of is a tiny local helper for proof assertions.
func sha256Of(payload []byte) [32]byte {
	return sha256.Sum256(payload)
}
