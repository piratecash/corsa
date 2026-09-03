package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestLegacyTransitRestampRewritesOldEnvelope pins what the temporary
// compatibility measure does: an old message handed to transit carries a
// fresh date, because a node below v30 anywhere along the route drops it
// otherwise — silently, with no hop-ack, so the sender learns nothing.
func TestLegacyTransitRestampRewritesOldEnvelope(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 22, 0, 0, 0, time.UTC)
	original := protocol.Envelope{
		ID: "old-1", Topic: "dm", Sender: "alice", Recipient: "bob",
		Payload: []byte("sealed"), CreatedAt: now.Add(-90 * 24 * time.Hour),
	}

	stamped := legacyTransitRestamp(original, now)

	if !stamped.CreatedAt.Equal(now) {
		t.Errorf("transit copy CreatedAt = %v, want %v", stamped.CreatedAt, now)
	}
	if stamped.ID != original.ID {
		t.Errorf("the id must not change: %s → %s", original.ID, stamped.ID)
	}
	if string(stamped.Payload) != string(original.Payload) {
		t.Error("the sealed payload must not change — the DM signature covers it")
	}
	// The caller's envelope is what awaitingDelivered and the chatlog hold.
	// Rewriting it here would move the date the USER sees on their own copy.
	if !original.CreatedAt.Equal(now.Add(-90 * 24 * time.Hour)) {
		t.Error("the caller's envelope must be left alone")
	}
}

// TestLegacyTransitRestampLeavesRecentEnvelopeAlone keeps the measure off
// ordinary traffic. Anything younger than the old ceiling crosses every
// version of the network with its real date, so rewriting it would trade a
// true timestamp for a slightly different one and gain nothing.
func TestLegacyTransitRestampLeavesRecentEnvelopeAlone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 22, 0, 0, 0, time.UTC)
	createdAt := now.Add(-legacyTransitRestampAfter / 2)
	envelope := protocol.Envelope{
		ID: "recent-1", Topic: "dm", Sender: "alice", Recipient: "bob",
		CreatedAt: createdAt,
	}

	if got := legacyTransitRestamp(envelope, now); !got.CreatedAt.Equal(createdAt) {
		t.Errorf("a recent message must keep its real date: %v → %v", createdAt, got.CreatedAt)
	}

	// A zero timestamp is not ours to invent: a transit DM without one is
	// refused as anomalous, and stamping one here would launder it.
	zero := protocol.Envelope{ID: "no-date", Topic: "dm", Sender: "alice", Recipient: "bob"}
	if got := legacyTransitRestamp(zero, now); !got.CreatedAt.IsZero() {
		t.Errorf("a zero CreatedAt must stay zero, got %v", got.CreatedAt)
	}
}

// TestLegacyTransitRestampThresholdLeavesMarginForTheWorstCeiling states the
// arithmetic the measure depends on, against the SMALLEST ceiling a pre-v30
// node can be running rather than the default one. The frame is built before
// it is sent — queuePeerFrame freezes it and flushPendingPeerFrames writes it
// later — and then still has hops to cross, so a threshold near that ceiling
// would let a message keep its date and arrive already expired.
func TestLegacyTransitRestampThresholdLeavesMarginForTheWorstCeiling(t *testing.T) {
	t.Parallel()
	if legacyTransitSmallestCeiling != time.Hour {
		t.Fatalf("legacyTransitSmallestCeiling = %s: CORSA_TRANSIT_MAX_AGE_HOURS parsed whole hours, so 1h is the floor",
			legacyTransitSmallestCeiling)
	}
	if margin := legacyTransitSmallestCeiling - legacyTransitRestampAfter; margin < legacyTransitSmallestCeiling/2 {
		t.Fatalf("threshold %s leaves only %s before the smallest legacy ceiling (%s); a queued frame can spend that in a pending ring",
			legacyTransitRestampAfter, margin, legacyTransitSmallestCeiling)
	}
}

// TestLegacyTransitRestampSkipsBroadcast is the class boundary. Broadcast
// keeps a real age ceiling of its own, and re-dating one on every hop would
// hand it a fresh lifetime each time — the re-circulation bypass the
// retention layer exists to prevent.
func TestLegacyTransitRestampSkipsBroadcast(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 22, 0, 0, 0, time.UTC)
	writtenAt := now.Add(-90 * 24 * time.Hour)

	for _, c := range []struct {
		name      string
		topic     string
		recipient string
		wantStamp bool
	}{
		{"addressed dm", "dm", "bob", true},
		{"addressed control dm", protocol.TopicControlDM, "bob", true},
		{"broadcast by wildcard recipient", "dm", "*", false},
		{"broadcast by empty recipient", "dm", "", false},
		{"global topic", "gazeta", "*", false},
	} {
		got := legacyTransitCreatedAt(c.topic, c.recipient, writtenAt, now)
		stamped := got.Equal(now)
		if stamped != c.wantStamp {
			t.Errorf("%s: re-stamped = %v, want %v", c.name, stamped, c.wantStamp)
		}
	}

	// The wire-string form has to agree, or the fast-path forward would
	// re-date broadcast frames the Envelope form leaves alone.
	raw := writtenAt.Format(time.RFC3339)
	if got, _ := legacyTransitWireStamp("gazeta", "*", raw, 0, now); got != raw {
		t.Errorf("broadcast wire created_at was re-stamped: %s → %s", raw, got)
	}
	if got, _ := legacyTransitWireStamp("dm", "bob", raw, 0, now); got == raw {
		t.Error("an addressed DM must be re-stamped on the wire-string path too")
	}
}

// TestTransitAgeRestampIsStillNeeded is the reminder to delete all of this.
// The measure exists only while the network floor is below the version that
// stopped refusing old DMs; once MinimumProtocolVersion reaches it, this
// test fails and points at what to remove.
func TestTransitAgeRestampIsStillNeeded(t *testing.T) {
	t.Parallel()
	if config.MinimumProtocolVersion >= config.ProtocolVersionNoTransitAgeCeiling {
		t.Fatalf("MinimumProtocolVersion is now %d: every node understands old dates, so delete "+
			"legacyTransitRestamp, its threshold, its call site in dispatchEnvelopeRetry, "+
			"config.ProtocolVersionNoTransitAgeCeiling and this file "+
			"(TODO(transit-age-restamp-removal))", config.MinimumProtocolVersion)
	}
}

// TestLegacyTransitStampPreservesTheDeadline is the invariant that keeps the
// measure from becoming a lifetime extension.
//
// TTL is stored as a duration from created_at, so moving the date forward
// without shortening the TTL moves the DEADLINE — a message with an hour to
// live would get a fresh hour at every hop, and an expired one would come
// back. What the author set is the deadline; the re-dated copy carries
// whatever is left of it, and an already-expired message is not re-dated at
// all so the gates that drop it still see the truth.
func TestLegacyTransitStampPreservesTheDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 22, 0, 0, 0, time.UTC)

	// Written 40 minutes ago with a 2-hour TTL: 80 minutes left.
	writtenAt := now.Add(-40 * time.Minute)
	const ttl = int(2 * 60 * 60)
	stamped, gotTTL := legacyTransitStamp("dm", "bob", writtenAt, ttl, now)
	if !stamped.Equal(now) {
		t.Fatalf("an old message must be re-dated, got %v", stamped)
	}
	deadlineBefore := writtenAt.Add(time.Duration(ttl) * time.Second)
	deadlineAfter := stamped.Add(time.Duration(gotTTL) * time.Second)
	if deadlineAfter.After(deadlineBefore) {
		t.Errorf("the deadline moved forward: %v → %v (ttl %d → %d)",
			deadlineBefore, deadlineAfter, ttl, gotTTL)
	}
	if deadlineBefore.Sub(deadlineAfter) > time.Second {
		t.Errorf("the deadline lost more than a rounding second: %v → %v", deadlineBefore, deadlineAfter)
	}

	// Already expired — 40 minutes old with a 30-minute TTL. Re-dating it
	// would revive it for another half hour.
	expiredAt := now.Add(-40 * time.Minute)
	gotAt, keptTTL := legacyTransitStamp("dm", "bob", expiredAt, int(30*60), now)
	if !gotAt.Equal(expiredAt) || keptTTL != int(30*60) {
		t.Errorf("an expired message must not be re-dated: %v/%d", gotAt, keptTTL)
	}

	// No TTL: nothing to preserve, the date moves as before.
	if at, ttl := legacyTransitStamp("dm", "bob", writtenAt, 0, now); !at.Equal(now) || ttl != 0 {
		t.Errorf("a message with no deadline must still be re-dated: %v/%d", at, ttl)
	}
}
