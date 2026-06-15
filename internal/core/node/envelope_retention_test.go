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
	transit := 24 * time.Hour
	broadcast := 48 * time.Hour
	cases := []struct {
		class           EnvelopeClass
		wantMaxAge      time.Duration
		wantRepropagate bool
	}{
		{ClassTransitDM, transit, true},
		{ClassControlDM, transit, true},
		{ClassBroadcast, broadcast, true},
		{ClassLocalOutbox, 0, true},
		{ClassLocalInbox, 0, false},
	}
	for _, c := range cases {
		got := retentionPolicyFor(c.class, transit, broadcast)
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

func TestTransitMaxAgeKillSwitch(t *testing.T) {
	t.Parallel()
	if got := retentionTestService(false).transitMaxAge(); got != 0 {
		t.Errorf("disabled: transitMaxAge = %s, want 0 (legacy no-ceiling)", got)
	}
	if got := retentionTestService(true).transitMaxAge(); got != defaultTransitMaxAge {
		t.Errorf("enabled default: transitMaxAge = %s, want %s", got, defaultTransitMaxAge)
	}
	custom := &Service{
		identity: &identity.Identity{Address: "self"},
		cfg:      config.Node{EnvelopeRetentionEnabled: true, TransitMaxAge: 1 * time.Hour},
	}
	if got := custom.transitMaxAge(); got != time.Hour {
		t.Errorf("configured: transitMaxAge = %s, want 1h", got)
	}
}

func TestTransitAgedOnAdmission(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	svc := retentionTestService(true)

	aged := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob", CreatedAt: now.Add(-48 * time.Hour)}
	if !svc.transitAgedOnAdmission(aged, now) {
		t.Error("aged transit DM should be refused on admission")
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

	// Kill-switch off → no admission refusal regardless of age.
	if retentionTestService(false).transitAgedOnAdmission(aged, now) {
		t.Error("with retention disabled, transit admission gate must be inert")
	}

	// Zero CreatedAt transit cannot be age-bounded → refused when enabled,
	// admitted when the ceiling is disabled (legacy).
	noTS := incomingMessage{Topic: "dm", Sender: "alice", Recipient: "bob"}
	if !svc.transitAgedOnAdmission(noTS, now) {
		t.Error("transit DM with zero CreatedAt must be refused (no immutable anchor)")
	}
	if retentionTestService(false).transitAgedOnAdmission(noTS, now) {
		t.Error("zero-CreatedAt transit must be admitted when the ceiling is disabled")
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
	// Control DM past the ceiling → aged (its only bound: never enters topics).
	if !svc.envelopePropagationAged(protocol.TopicControlDM, "alice", "bob", now.Add(-48*time.Hour), now) {
		t.Error("aged control DM must be flagged for the re-propagation gate")
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

// TestStoreIncomingMessageRefusesAgedTransit checks the admission choke point:
// a transit DM past the ceiling is neither stored nor propagated.
func TestStoreIncomingMessageRefusesAgedTransit(t *testing.T) {
	t.Parallel()
	svc := retentionTestService(true)
	svc.topics = map[string][]protocol.Envelope{}

	aged := incomingMessage{
		ID:        "aged-transit",
		Topic:     "dm",
		Sender:    "alice",
		Recipient: "bob",
		CreatedAt: time.Now().UTC().Add(-48 * time.Hour),
		Body:      "x",
	}
	ok, _, _ := svc.storeIncomingMessage(aged, false)
	if ok {
		t.Fatal("aged transit DM must not be admitted")
	}
	if len(svc.topics["dm"]) != 0 {
		t.Fatalf("aged transit DM must not enter s.topics, got %d", len(svc.topics["dm"]))
	}
}

// TestCleanupDropsAgedDespiteFreshStoredAt is the regression for the root bug:
// the old StoredAt-anchored window let a re-injected (fresh StoredAt) but
// months-old transit DM survive forever. The CreatedAt ceiling now drops it
// even when StoredAt is fresh, while a genuinely fresh envelope is kept.
func TestCleanupDropsAgedDespiteFreshStoredAt(t *testing.T) {
	t.Parallel()
	svc := retentionTestService(true)
	now := time.Now().UTC()

	mk := func(id string, createdAgo time.Duration) protocol.Envelope {
		return protocol.Envelope{
			ID:        protocol.MessageID(id),
			Topic:     "dm",
			Sender:    "alice",
			Recipient: "bob",
			CreatedAt: now.Add(-createdAgo),
			StoredAt:  now, // fresh — defeats the legacy transitExpired window
		}
	}
	svc.topics = map[string][]protocol.Envelope{
		"dm": {mk("aged", 48*time.Hour), mk("fresh", 1*time.Hour)},
	}

	svc.cleanupExpiredMessagesForce()

	got := map[protocol.MessageID]bool{}
	for _, e := range svc.topics["dm"] {
		got[e.ID] = true
	}
	if got["aged"] {
		t.Error("aged transit DM (fresh StoredAt) must be dropped by the CreatedAt ceiling")
	}
	if !got["fresh"] {
		t.Error("fresh transit DM must be kept")
	}
}
