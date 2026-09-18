package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// A hop ack proves the frame reached the next hop; the only consumer of
// FrameLine is the failover resend that a hop ack makes impossible. The
// wire bytes must be released on the ack, not held for the remaining
// TTL — at 10 000 states × 64 KiB that is the difference between a
// bounded store and a 625 MiB peak of bodies nothing will read.
func TestMarkHopAckObservedReleasesFrameLine(t *testing.T) {
	t.Parallel()
	rs := newRelayStateStore()
	line := `{"type":"relay_message","payload":"` + string(make([]byte, 4096)) + `"}`
	rs.store(&relayForwardState{
		MessageID:            "msg-acked",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-recipient"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
		FrameLine:            line,
	})
	rs.store(&relayForwardState{
		MessageID:            "msg-pending",
		ForwardedTo:          domain.PeerAddress("peer-c"),
		Recipient:            domaintest.ID("id-recipient"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
		FrameLine:            line,
	})
	if got := rs.frameLineBytes(); got != 2*len(line) {
		t.Fatalf("frameLineBytes before ack = %d, want %d", got, 2*len(line))
	}

	if !rs.markHopAckObserved("msg-acked") {
		t.Fatal("markHopAckObserved returned false for a live state")
	}

	rs.mu.Lock()
	acked := rs.states["msg-acked"].FrameLine
	pending := rs.states["msg-pending"].FrameLine
	rs.mu.Unlock()
	if acked != "" {
		t.Fatalf("FrameLine still held after the hop ack (%d bytes)", len(acked))
	}
	if pending != line {
		t.Fatal("FrameLine of a state still waiting for its ack must be kept")
	}
	if got := rs.frameLineBytes(); got != len(line) {
		t.Fatalf("frameLineBytes after ack = %d, want %d", got, len(line))
	}
	// The ack-observed state must still be a dedupe marker: releasing
	// the payload is not releasing the entry.
	if !rs.hasSeen("msg-acked") {
		t.Fatal("state must survive the payload release for the rest of its TTL")
	}
}

// A failover that re-arms the hop-ack budget after a previous ack is a
// fresh forward; it comes with a fresh FrameLine through store(), so the
// release on ack must not starve it. Covered by the idempotent-upsert
// path: the reroute overwrites the field verbatim.
func TestRerouteAfterAckRestoresFrameLine(t *testing.T) {
	t.Parallel()
	rs := newRelayStateStore()
	rs.store(&relayForwardState{
		MessageID: "msg-reroute", ForwardedTo: domain.PeerAddress("peer-b"),
		Recipient: domaintest.ID("id-recipient"), RemainingTTL: 60, HopAckRemainingTicks: 3, FrameLine: "first",
	})
	rs.markHopAckObserved("msg-reroute")
	rs.store(&relayForwardState{
		MessageID: "msg-reroute", ForwardedTo: domain.PeerAddress("peer-c"),
		Recipient: domaintest.ID("id-recipient"), RemainingTTL: 60, HopAckRemainingTicks: 3, FrameLine: "second",
	})
	rs.mu.Lock()
	got := rs.states["msg-reroute"].FrameLine
	rs.mu.Unlock()
	if got != "second" {
		t.Fatalf("FrameLine after reroute = %q, want the reroute's own frame", got)
	}
}

// The retry loop snapshots the backlog under deliveryMu, releases it,
// and only then records the attempt. Anything that removed the message
// and its relayRetry entry in between (receipt ack, cancel, transit
// eviction) must win: recording the attempt for a message that is gone
// re-created an entry no sweep could ever reach, because the only reaper
// walks the live backlog.
func TestNoteRelayAttemptDoesNotResurrectRemovedEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender, _ := identity.Generate()
	recipient, _ := identity.Generate()
	now := time.Now().UTC()

	transit := protocol.Envelope{
		ID: "transit-dm-race", Topic: "dm", Sender: sender.Address, Recipient: recipient.Address,
		Flag: protocol.MessageFlagSenderDelete, CreatedAt: now,
	}
	svc.gossipMu.Lock()
	svc.topics["dm"] = []protocol.Envelope{transit}
	svc.gossipMu.Unlock()
	svc.deliveryMu.Lock()
	svc.relayRetry[relayMessageKey(transit.ID)] = relayAttempt{FirstSeen: now}
	svc.deliveryMu.Unlock()

	snapshot := svc.retryableRelayMessages(now)
	if len(snapshot) != 1 || snapshot[0].ID != transit.ID {
		t.Fatalf("snapshot = %+v, want the armed transit message", snapshot)
	}

	// Concurrent removal lands between the snapshot and the attempt.
	svc.gossipMu.Lock()
	svc.topics["dm"] = nil
	svc.gossipMu.Unlock()
	svc.dropRelayRetryEntries([]protocol.MessageID{transit.ID})

	attempts, live := svc.noteRelayAttempt(relayMessageKey(snapshot[0].ID), now)
	if live {
		t.Fatalf("noteRelayAttempt reported a live entry (attempts=%d) for a message that was removed", attempts)
	}

	svc.deliveryMu.RLock()
	_, resurrected := svc.relayRetry[relayMessageKey(transit.ID)]
	svc.deliveryMu.RUnlock()
	if resurrected {
		t.Fatal("relayRetry entry resurrected after its message left the backlog")
	}
}

// The counterpart: an entry that is still tracked records the attempt as
// before.
func TestNoteRelayAttemptCountsLiveEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	now := time.Now().UTC()
	key := relayMessageKey("live-dm")
	svc.deliveryMu.Lock()
	svc.relayRetry[key] = relayAttempt{FirstSeen: now}
	svc.deliveryMu.Unlock()

	for want := 1; want <= 3; want++ {
		attempts, live := svc.noteRelayAttempt(key, now)
		if !live || attempts != want {
			t.Fatalf("attempt %d: live=%v attempts=%d", want, live, attempts)
		}
	}
}

// The hop-ack timer fires, the callback takes its snapshot and starts a
// failover — and a late ACK for the abandoned uplink lands in the middle.
// Releasing the payload on that ACK left the re-armed state with nothing
// to retry from: the next timeout found an empty FrameLine and skipped
// both the second uplink and the gossip fallback, so the message stopped
// silently. The release must follow the failover, not race it.
func TestLateHopAckKeepsPayloadOfInFlightFailover(t *testing.T) {
	t.Parallel()
	rs := newRelayStateStore()
	const line = `{"type":"relay_message","id":"msg-race"}`
	rs.store(&relayForwardState{
		MessageID:            "msg-race",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-recipient"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 1,
		FrameLine:            line,
	})

	// 1. The budget elapses: the ticker hands the callback a snapshot.
	fired := rs.tickHopAckBudgets()
	if len(fired) != 1 || fired[0].FrameLine != line {
		t.Fatalf("fired = %+v, want one state carrying the frame", fired)
	}

	// 2. A late ACK for the uplink that just timed out arrives while the
	//    failover is still running.
	rs.markHopAckObserved("msg-race")

	// 3. The failover sends its snapshot through another uplink and
	//    re-arms the budget for it.
	if !rs.recordFailoverRetry("msg-race", domain.PeerAddress("peer-c"), fired[0].FrameLine) {
		t.Fatal("recordFailoverRetry returned false for a live state")
	}

	rs.mu.Lock()
	stored := rs.states["msg-race"].FrameLine
	rs.mu.Unlock()
	if stored != line {
		t.Fatalf("FrameLine after the failover re-arm = %q, want the frame it just sent", stored)
	}

	// 4. The re-armed budget must fire with a payload the retry and the
	//    gossip fallback can still use.
	var second []relayForwardState
	for range defaultHopAckBudgetSeconds {
		if second = rs.tickHopAckBudgets(); len(second) > 0 {
			break
		}
	}
	if len(second) != 1 {
		t.Fatalf("the re-armed budget never fired: %+v", second)
	}
	if second[0].FrameLine != line {
		t.Fatal("the second timeout has no payload: failover retry and gossip fallback would both be skipped")
	}
}

// The genuine cancel — an ACK that arrives while the timer is still
// running — releases the payload as before: no failover can follow it.
func TestHopAckBeforeTimeoutStillReleasesPayload(t *testing.T) {
	t.Parallel()
	rs := newRelayStateStore()
	rs.store(&relayForwardState{
		MessageID: "msg-clean", ForwardedTo: domain.PeerAddress("peer-b"),
		Recipient: domaintest.ID("id-recipient"), RemainingTTL: 60,
		HopAckRemainingTicks: 5, FrameLine: "payload",
	})
	if !rs.markHopAckObserved("msg-clean") {
		t.Fatal("markHopAckObserved returned false for a live state")
	}
	rs.mu.Lock()
	stored := rs.states["msg-clean"].FrameLine
	rs.mu.Unlock()
	if stored != "" {
		t.Fatalf("FrameLine = %q after a cancelling ack, want released", stored)
	}
}
