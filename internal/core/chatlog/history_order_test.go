package chatlog

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// history_order_test.go pins the order of the thread itself.
//
// Messages carry second-resolution timestamps on the wire, so several rows of
// one conversation sharing a created_at is ordinary rather than exotic — a
// question and its answer land inside the same second all the time. Ordering
// by created_at alone leaves those rows to the sorter, which is free to return
// them in any order it likes and does change its mind depending on the plan it
// picks: the reply can be drawn above the message it answers, and the "last
// message in the cache" — the sidebar's fallback preview — can be any of them.
//
// The tie-break is rowid: within one second, the row inserted first is shown
// first.

// alternatingWithinOneSecond is a conversation whose four rows all carry the
// same stamp, alternating direction so that any ordering which groups by
// sender or recipient (an index scan over one leg, then the other) comes out
// visibly wrong rather than accidentally right.
func alternatingWithinOneSecond() []Entry {
	const stamp = "2026-01-01T10:00:00Z"
	return []Entry{
		{ID: "m1", Sender: lastEntrySelf, Recipient: lastEntryPeer, Body: "one", CreatedAt: stamp},
		{ID: "m2", Sender: lastEntryPeer, Recipient: lastEntrySelf, Body: "two", CreatedAt: stamp},
		{ID: "m3", Sender: lastEntrySelf, Recipient: lastEntryPeer, Body: "three", CreatedAt: stamp},
		{ID: "m4", Sender: lastEntryPeer, Recipient: lastEntrySelf, Body: "four", CreatedAt: stamp},
	}
}

func idsOf(entries []Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.ID)
	}
	return ids
}

func equalIDs(got []Entry, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].ID != want[i] {
			return false
		}
	}
	return true
}

func TestReadKeepsInsertionOrderWithinOneSecond(t *testing.T) {
	ctx := context.Background()
	s := storeFor(t, lastEntrySelf)
	rows := alternatingWithinOneSecond()
	appendInOrder(t, s, rows...)

	got, err := s.Read(ctx, "dm", domain.PeerIdentityFromWire(lastEntryPeer))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := idsOf(rows)
	if !equalIDs(got, want) {
		t.Fatalf("thread order = %v, want %v: rows of the same second must keep the order they were written in",
			idsOf(got), want)
	}
}

func TestReadLastKeepsInsertionOrderWithinOneSecond(t *testing.T) {
	ctx := context.Background()
	peer := domain.PeerIdentityFromWire(lastEntryPeer)
	rows := alternatingWithinOneSecond()

	t.Run("whole thread", func(t *testing.T) {
		s := storeFor(t, lastEntrySelf)
		appendInOrder(t, s, rows...)

		got, err := s.ReadLast(ctx, "dm", peer, len(rows))
		if err != nil {
			t.Fatalf("read last: %v", err)
		}
		if want := idsOf(rows); !equalIDs(got, want) {
			t.Fatalf("thread order = %v, want %v", idsOf(got), want)
		}
	})

	t.Run("tail only", func(t *testing.T) {
		s := storeFor(t, lastEntrySelf)
		appendInOrder(t, s, rows...)

		got, err := s.ReadLast(ctx, "dm", peer, 2)
		if err != nil {
			t.Fatalf("read last: %v", err)
		}
		if want := []string{"m3", "m4"}; !equalIDs(got, want) {
			t.Fatalf("tail = %v, want %v: the newest two are the two written last",
				idsOf(got), want)
		}
	})
}
