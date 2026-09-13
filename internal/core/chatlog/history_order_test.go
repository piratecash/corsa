package chatlog

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// history_order_test.go pins the order of the thread itself: the order in
// which THIS node learned of the messages, which is what Read and ReadLast
// order by and what the tests below state in three forms.
//
// Two of them are about rows that share a stamp. Messages carry
// second-resolution timestamps on the wire, so several rows of one
// conversation sharing a created_at is ordinary rather than exotic — a
// question and its answer land inside the same second all the time — and an
// order taken from the stamp alone leaves them to the sorter, which is free to
// return them in any order it likes and does change its mind depending on the
// plan it picks.
//
// The third is about stamps that disagree with arrival, which is the case the
// stamp cannot survive at all: created_at is printed by the SENDER, and a peer
// whose clock lags dates its reply before the message it answers.

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

// laggingPeerAnswersUs is the reported symptom, written down as rows.
//
// We send at 13:47 by our own clock. The peer answers a second later, but
// their clock runs a minute slow, so their reply is dated 13:46 — before the
// message it is an answer to, and inside the drift the node accepts by design.
// Arrival is the only thing that says which came first, and it says we did.
func laggingPeerAnswersUs() []Entry {
	return []Entry{
		{ID: "ours", Sender: lastEntrySelf, Recipient: lastEntryPeer, Body: "so a buyer just holds it?", CreatedAt: "2026-01-01T13:47:02Z"},
		{ID: "theirs", Sender: lastEntryPeer, Recipient: lastEntrySelf, Body: "yes", CreatedAt: "2026-01-01T13:46:11Z"},
	}
}

func TestAReplyIsNeverDrawnAboveTheMessageItAnswers(t *testing.T) {
	ctx := context.Background()
	peer := domain.PeerIdentityFromWire(lastEntryPeer)
	rows := laggingPeerAnswersUs()
	want := idsOf(rows)

	t.Run("Read", func(t *testing.T) {
		s := storeFor(t, lastEntrySelf)
		appendInOrder(t, s, rows...)

		got, err := s.Read(ctx, "dm", peer)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !equalIDs(got, want) {
			t.Fatalf("thread order = %v, want %v: the answer arrived second and belongs second, "+
				"however its author dated it", idsOf(got), want)
		}
	})

	t.Run("ReadLast", func(t *testing.T) {
		s := storeFor(t, lastEntrySelf)
		appendInOrder(t, s, rows...)

		got, err := s.ReadLast(ctx, "dm", peer, len(rows))
		if err != nil {
			t.Fatalf("read last: %v", err)
		}
		if !equalIDs(got, want) {
			t.Fatalf("thread order = %v, want %v", idsOf(got), want)
		}
	})

	// A window of one taken by stamp would answer "ours", which is the same
	// defect the sidebar was moved off created_at for: the newest thing in
	// the conversation is the one that arrived last.
	t.Run("ReadLast window follows arrival too", func(t *testing.T) {
		s := storeFor(t, lastEntrySelf)
		appendInOrder(t, s, rows...)

		got, err := s.ReadLast(ctx, "dm", peer, 1)
		if err != nil {
			t.Fatalf("read last: %v", err)
		}
		if !equalIDs(got, []string{"theirs"}) {
			t.Fatalf("newest = %v, want [theirs]", idsOf(got))
		}
	})
}

// TestReadLastCarriesTheArrivalSequence: an entry that reaches the caller
// without its sequence cannot be placed against a live message, and the
// outer SELECT used to drop the column the inner one had already computed.
func TestReadLastCarriesTheArrivalSequence(t *testing.T) {
	ctx := context.Background()
	peer := domain.PeerIdentityFromWire(lastEntryPeer)
	s := storeFor(t, lastEntrySelf)
	rows := alternatingWithinOneSecond()
	appendInOrder(t, s, rows...)

	got, err := s.ReadLast(ctx, "dm", peer, len(rows))
	if err != nil {
		t.Fatalf("read last: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("got %d entries, want %d", len(got), len(rows))
	}
	for i := range got {
		if got[i].RowID == 0 {
			t.Fatalf("entry %s came back with no arrival sequence", got[i].ID)
		}
		if i > 0 && got[i].RowID <= got[i-1].RowID {
			t.Fatalf("sequences %d then %d do not ascend with the order returned",
				got[i-1].RowID, got[i].RowID)
		}
	}
}
