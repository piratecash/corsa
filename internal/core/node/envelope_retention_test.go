package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// envelope_retention_test.go pins the unified message-lifetime contract:
//
//   - classifyEnvelope: topic/sender/recipient → EnvelopeClass.
//   - retentionPolicyFor: class → MaxAge / Repropagate.
//   - envelopeAgeExceeded: absolute ceiling anchored on the immutable
//     CreatedAt — re-injection cannot reset it; a future-dated CreatedAt
//     beyond the clock-skew tolerance is treated as AGED (not fresh), so it
//     cannot buy immortality.
//   - transitAgedOnAdmission: only TRANSIT is refused on arrival.

func TestClassifyEnvelope(t *testing.T) {
	t.Parallel()
	const self = "self"
	cases := []struct {
		name                     string
		topic, sender, recipient string
		want                     EnvelopeClass
	}{
		{"transit dm", "dm", "alice", "bob", ClassTransitDM},
		{"local inbox", "dm", "alice", self, ClassLocalInbox},
		{"local outbox", "dm", self, "bob", ClassLocalOutbox},
		{"broadcast star", "dm", "alice", "*", ClassBroadcast},
		{"broadcast empty recipient", "dm", "alice", "", ClassBroadcast},
		{"broadcast non-dm topic", "gazeta", "alice", "bob", ClassBroadcast},
		{"control dm", protocol.TopicControlDM, "alice", "bob", ClassControlDM},
		{"control dm to self still control", protocol.TopicControlDM, "alice", self, ClassControlDM},
	}
	for _, c := range cases {
		if got := classifyEnvelope(c.topic, c.sender, c.recipient, self); got != c.want {
			t.Errorf("%s: classifyEnvelope = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestRetentionPolicyFor(t *testing.T) {
	t.Parallel()
	broadcast := 48 * time.Hour
	cases := []struct {
		class           EnvelopeClass
		wantMaxAge      time.Duration
		wantRepropagate bool
	}{
		// Broadcast is the only class with an age ceiling left.
		{ClassTransitDM, 0, true},
		{ClassControlDM, 0, true},
		{ClassBroadcast, broadcast, true},
		{ClassLocalOutbox, 0, true},
		{ClassLocalInbox, 0, false},
	}
	for _, c := range cases {
		got := retentionPolicyFor(c.class, broadcast)
		if got.MaxAge != c.wantMaxAge || got.Repropagate != c.wantRepropagate {
			t.Errorf("class %d: got %+v, want MaxAge=%s Repropagate=%v", c.class, got, c.wantMaxAge, c.wantRepropagate)
		}
	}
}

func TestEnvelopeAgeExceeded(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	maxAge := 24 * time.Hour
	cases := []struct {
		name      string
		createdAt time.Time
		maxAge    time.Duration
		want      bool
	}{
		{"fresh transit", now.Add(-1 * time.Hour), maxAge, false},
		{"exactly at ceiling", now.Add(-24 * time.Hour), maxAge, false},
		{"just past ceiling", now.Add(-24*time.Hour - time.Second), maxAge, true},
		{"ten days old (zombie)", now.Add(-10 * 24 * time.Hour), maxAge, true},
		{"far-future is bogus → aged (no immortality)", now.Add(48 * time.Hour), maxAge, true},
		{"within clock-skew tolerance → fresh", now.Add(protocol.DefaultMessageTimeDrift / 2), maxAge, false},
		{"ceiling disabled (maxAge=0)", now.Add(-10 * 24 * time.Hour), 0, false},
		{"zero CreatedAt has no anchor", time.Time{}, maxAge, false},
	}
	for _, c := range cases {
		if got := envelopeAgeExceeded(c.createdAt, now, c.maxAge, protocol.DefaultMessageTimeDrift); got != c.want {
			t.Errorf("%s: envelopeAgeExceeded = %v, want %v", c.name, got, c.want)
		}
	}
}

func retentionTestService(enabled bool) *Service {
	return &Service{
		identity: &identity.Identity{Address: "self"},
		cfg:      config.Node{EnvelopeRetentionEnabled: enabled},
	}
}

// TestTransitCarriesNoAgeCeiling pins the decision this layer was corrected
// on: a relay does not refuse a DM for being old, and there is no setting
// that brings the refusal back.
//
// The ceiling could not tell an envelope circulating on its own from a live
// sender re-sending an old message, and refused both — so a message older
// than a day could never cross a relay again, while the silent refusal cost
// working routes a reputation hit on every attempt. Leaving an operator knob
// would be worse than useless: such a node advertises v30 ("I carry old
// messages") and behaves like a pre-v30 one, which turns into a silent black
// hole the moment the network floor rises and the sender-side re-stamp is
// deleted. What bounds circulation is the hop budget, forward-once and the
// transit rings, none of which look at the date a human typed.
func TestTransitCarriesNoAgeCeiling(t *testing.T) {
	t.Parallel()
	for _, retention := range []bool{true, false} {
		svc := retentionTestService(retention)
		for _, class := range []EnvelopeClass{ClassTransitDM, ClassControlDM} {
			pol := retentionPolicyFor(class, svc.broadcastMaxAge())
			if pol.MaxAge != 0 {
				t.Errorf("EnvelopeRetentionEnabled=%v, class %d: MaxAge = %s, want 0",
					retention, class, pol.MaxAge)
			}
		}
	}
	// Broadcast keeps its ceiling: nobody owns a global topic's delivery, so
	// nothing else would ever retire its history.
	if got := retentionTestService(true).broadcastMaxAge(); got != defaultBroadcastMaxAge {
		t.Errorf("broadcastMaxAge = %s, want %s", got, defaultBroadcastMaxAge)
	}
}

func TestTransitAgedOnAdmission(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	svc := retentionTestService(true)

	// The reported bug, at its choke point: a months-old DM from a sender who
	// is still trying must cross this relay.
	aged := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob", CreatedAt: now.Add(-90 * 24 * time.Hour)}
	if svc.transitAgedOnAdmission(aged, now) {
		t.Error("an old transit DM must be forwarded: age says nothing about whether it is circulating")
	}

	fresh := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob", CreatedAt: now.Add(-1 * time.Hour)}
	if svc.transitAgedOnAdmission(fresh, now) {
		t.Error("fresh transit DM must be admitted")
	}

	// A DM addressed to us is local, not transit — never refused by this gate
	// even when old (the sender-owned / chatlog layers own its lifetime).
	localOld := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "self", CreatedAt: now.Add(-48 * time.Hour)}
	if svc.transitAgedOnAdmission(localOld, now) {
		t.Error("local inbox DM must never be refused by the transit admission gate")
	}

	// Future-dating is still refused, and by a rule of its own now: it used to
	// live inside the age check, which no longer runs for this class. A
	// relay_message skips timestamp validation and the outer created_at is
	// not signed, so without this a forged date would sit at the top of the
	// recipient's conversation forever.
	forged := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob", CreatedAt: now.Add(48 * time.Hour)}
	if !svc.transitAgedOnAdmission(forged, now) {
		t.Error("a transit DM dated far into the future must be refused")
	}
	withinSkew := incomingMessage{
		Topic: "dm", Sender: "alice", Recipient: "bob",
		CreatedAt: now.Add(protocol.DefaultMessageTimeDrift / 2),
	}
	if svc.transitAgedOnAdmission(withinSkew, now) {
		t.Error("clock skew within tolerance must not be treated as forgery")
	}

	// A transit DM with NO timestamp is refused whatever the ceiling says:
	// that is a separate rule about the frame being anomalous, not about age.
	// Legitimate senders always stamp one, and the relay path skips timestamp
	// validation entirely.
	noTS := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob"}
	if !svc.transitAgedOnAdmission(noTS, now) {
		t.Error("transit DM with zero CreatedAt must be refused (anomalous frame)")
	}
	if !retentionTestService(false).transitAgedOnAdmission(noTS, now) {
		t.Error("the zero-CreatedAt refusal does not depend on the ceiling being on")
	}
}

func TestEnvelopePropagationAged(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	svc := retentionTestService(true)

	// Broadcast past the ceiling → aged (must not be re-propagated).
	if !svc.envelopePropagationAged("gazeta", "alice", "*", now.Add(-48*time.Hour), now) {
		t.Error("aged broadcast must be flagged for the re-propagation gate")
	}
	// Control DM: no age ceiling any more, same as the transit DM it rides
	// with. Its retry is owned at the app layer.
	if svc.envelopePropagationAged(protocol.TopicControlDM, "alice", "bob", now.Add(-48*time.Hour), now) {
		t.Error("an old control DM must still propagate: the DM classes carry no age ceiling")
	}
	// And the transit DM itself — the case the user reported.
	if svc.envelopePropagationAged("dm", "alice", "bob", now.Add(-90*24*time.Hour), now) {
		t.Error("an old transit DM must still propagate")
	}
	// Fresh broadcast → not aged.
	if svc.envelopePropagationAged("gazeta", "alice", "*", now.Add(-1*time.Hour), now) {
		t.Error("fresh broadcast must not be gated")
	}
	// Local outbox (MaxAge=0) → never aged regardless of age.
	if svc.envelopePropagationAged("dm", "self", "bob", now.Add(-100*24*time.Hour), now) {
		t.Error("local-outbox lifetime is owned by the sender engine, never aged here")
	}

	// Zero CreatedAt: DM-class (transit/control) is refused (legit ones always
	// carry a timestamp), broadcast stays lenient (its history is local).
	if !svc.envelopePropagationAged("dm", "alice", "bob", time.Time{}, now) {
		t.Error("zero-CreatedAt transit DM must be refused (no immutable anchor)")
	}
	if !svc.envelopePropagationAged(protocol.TopicControlDM, "alice", "bob", time.Time{}, now) {
		t.Error("zero-CreatedAt control DM must be refused (backstop, legit ones carry CreatedAt)")
	}
	if svc.envelopePropagationAged("gazeta", "alice", "*", time.Time{}, now) {
		t.Error("zero-CreatedAt broadcast must stay lenient (local history)")
	}
}

func TestHasRelayRetryEntryGatesAckSuppression(t *testing.T) {
	t.Parallel()
	svc := &Service{relayRetry: map[string]relayAttempt{}}

	if svc.hasRelayRetryEntry("ghost") {
		t.Error("phantom id must not report a relay-retry entry")
	}
	svc.relayRetry[relayMessageKey("real")] = relayAttempt{}
	if !svc.hasRelayRetryEntry("real") {
		t.Error("an active relay-retry id must report present")
	}
}

// TestStoreIncomingMessageCarriesAgedTransit is the regression for the
// reported failure, at the admission choke point.
//
// A relay used to refuse a transit DM older than a day and, by contract,
// answer nothing at all — so the sender saw silence, counted its uplink as
// a black hole and moved on. The message could never arrive, no matter how
// many times its author re-sent it, and working routes were charged a
// failure for every attempt. The age of a message is not evidence that it
// is circulating; the hop budget and forward-once are what bound that.
func TestStoreIncomingMessageCarriesAgedTransit(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, sender)

	aged := incomingMessage{
		ID:        "aged-transit",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC().Add(-90 * 24 * time.Hour),
		Body:      sealDMBody(t, sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey)),
	}
	ok, _, errCode := svc.storeIncomingMessage(aged, false)
	if !ok {
		t.Fatalf("an old transit DM must be admitted for forwarding (errCode %q)", errCode)
	}
}

// TestStoreIncomingMessageRefusesFutureDatedTransit keeps the one refusal
// that survived: a message cannot legitimately originate in the future, and
// the outer date is neither signed nor validated on the relay path.
func TestStoreIncomingMessageRefusesFutureDatedTransit(t *testing.T) {
	t.Parallel()
	svc := retentionTestService(true)
	svc.topics = map[string][]protocol.Envelope{}

	forged := incomingMessage{
		ID:        "future-transit",
		Topic:     "dm",
		Sender:    "alice",
		Recipient: "bob",
		CreatedAt: time.Now().UTC().Add(72 * time.Hour),
		Body:      "x",
	}
	ok, _, _ := svc.storeIncomingMessage(forged, false)
	if ok {
		t.Fatal("a future-dated transit DM must not be admitted")
	}
	if len(svc.topics["dm"]) != 0 {
		t.Fatalf("refused transit DM must not enter s.topics, got %d", len(svc.topics["dm"]))
	}
}

// TestCleanupDropsAgedBroadcastDespiteFreshStoredAt is what remains of the
// CreatedAt-anchored sweep: broadcast, the one class that still has a
// ceiling. It was the regression for the original storm — the StoredAt
// window alone let a re-injected but months-old envelope survive forever,
// because admission re-stamps StoredAt every time.
func TestCleanupDropsAgedBroadcastDespiteFreshStoredAt(t *testing.T) {
	t.Parallel()
	svc := retentionTestService(true)
	now := time.Now().UTC()

	mk := func(id string, createdAgo time.Duration) protocol.Envelope {
		return protocol.Envelope{
			ID:        protocol.MessageID(id),
			Topic:     "gazeta",
			Sender:    "alice",
			Recipient: "*",
			CreatedAt: now.Add(-createdAgo),
			StoredAt:  now, // fresh — defeats the legacy transitExpired window
		}
	}
	svc.topics = map[string][]protocol.Envelope{
		"gazeta": {mk("aged", 48*time.Hour), mk("fresh", 1*time.Hour)},
	}

	svc.cleanupExpiredMessagesForce()

	got := map[protocol.MessageID]bool{}
	for _, e := range svc.topics["gazeta"] {
		got[e.ID] = true
	}
	if got["aged"] {
		t.Error("aged broadcast (fresh StoredAt) must be dropped by the CreatedAt ceiling")
	}
	if !got["fresh"] {
		t.Error("fresh broadcast must be kept")
	}
}

// TestCleanupKeepsAgedTransitWithoutCeiling is the default-configuration
// half: with no ceiling, an old transit DM whose forwarding window is still
// open stays available to forward. Dropping it here was the other half of
// the reported failure — the sender's re-send would be admitted and then
// swept out from under the forwarder by its own date.
func TestCleanupKeepsAgedTransitWithoutCeiling(t *testing.T) {
	t.Parallel()
	svc := retentionTestService(true)
	now := time.Now().UTC()

	svc.topics = map[string][]protocol.Envelope{
		"dm": {{
			ID:        "aged-but-in-flight",
			Topic:     "dm",
			Sender:    "alice",
			Recipient: "bob",
			CreatedAt: now.Add(-90 * 24 * time.Hour),
			StoredAt:  now,
		}},
	}

	svc.cleanupExpiredMessagesForce()

	if len(svc.topics["dm"]) != 1 {
		t.Fatal("an old transit DM inside its forwarding window must not be swept by date")
	}
}
