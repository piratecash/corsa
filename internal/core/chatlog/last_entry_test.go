package chatlog

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// last_entry_test.go covers the one question the sidebar asks this package:
// which row of a conversation is the LAST one.
//
// The answer used to be "the one with the largest created_at", and created_at
// is a string the SENDER printed by the SENDER's clock. Two failure modes
// followed from that, and both are here:
//
//   - a peer whose clock lags writes a message that reads as older than the
//     reply we sent before it, so our own reply stayed on the sidebar while
//     the unread badge counted a message the user could not see;
//   - the column is TEXT, and text order is not time order: "…00Z" sorts
//     ABOVE "…00.5Z" because 'Z' > '.'.
//
// The last row is now the last one to ARRIVE HERE — insertion order, which no
// remote clock can reach.

const (
	lastEntrySelf = "aabbccdd11223344aabbccdd11223344aabbccdd"
	lastEntryPeer = "1111111111111111111111111111111111111111"
)

// appendInOrder writes the rows in the order given, which is the order they
// arrived. Their created_at values are deliberately NOT in that order.
func appendInOrder(t *testing.T, s *Store, rows ...Entry) {
	t.Helper()
	self := domain.PeerIdentityFromWire(lastEntrySelf)
	for _, row := range rows {
		if err := s.Append(context.Background(), "dm", self, row); err != nil {
			t.Fatalf("append %s: %v", row.ID, err)
		}
	}
}

// laggingPeerRows is our reply, and then the peer's answer to it stamped
// earlier than our reply because their clock is behind. The node accepts the
// message — clock drift is tolerated by design — so the row is really there.
func laggingPeerRows() []Entry {
	return []Entry{
		{
			ID: "ours", Sender: lastEntrySelf, Recipient: lastEntryPeer,
			Body: "our reply", CreatedAt: "2026-01-01T10:00:05Z",
		},
		{
			ID: "theirs", Sender: lastEntryPeer, Recipient: lastEntrySelf,
			Body: "their answer", CreatedAt: "2026-01-01T10:00:00Z",
		},
	}
}

// subsecondRows are two rows of the same second where the later one carries a
// fraction — the pair that text ordering gets backwards.
func subsecondRows() []Entry {
	return []Entry{
		{
			ID: "whole", Sender: lastEntrySelf, Recipient: lastEntryPeer,
			Body: "whole second", CreatedAt: "2026-01-01T10:00:00Z",
		},
		{
			ID: "fraction", Sender: lastEntryPeer, Recipient: lastEntrySelf,
			Body: "half a second later", CreatedAt: "2026-01-01T10:00:00.5Z",
		},
	}
}

// TestLastEntryCarriesItsArrivalSequence pins the number the sidebar needs to
// order two answers about the same conversation against each other. The last
// row is chosen by `rowid`, and the reader that chose it hands that value up:
// without it the caller knows WHICH message is last but has no way to compare
// it with a message reaching it by another road.
func TestLastEntryCarriesItsArrivalSequence(t *testing.T) {
	ctx := context.Background()
	s := storeFor(t, lastEntrySelf)
	rows := laggingPeerRows()
	appendInOrder(t, s, rows...)

	entry, err := s.ReadLastEntry(ctx, "dm", domain.PeerIdentityFromWire(lastEntryPeer))
	if err != nil {
		t.Fatalf("read last entry: %v", err)
	}
	if entry == nil {
		t.Fatal("read last entry: no row")
	}
	if entry.RowID <= 0 {
		t.Fatalf("last entry %q came back with sequence %d; the caller cannot order it against anything", entry.ID, entry.RowID)
	}

	seq, ok, err := s.MessageSeq(ctx, domain.MessageID(entry.ID))
	if err != nil {
		t.Fatalf("message seq: %v", err)
	}
	if !ok {
		t.Fatalf("the store does not know the sequence of %q, a row it just returned", entry.ID)
	}
	if seq != entry.RowID {
		t.Fatalf("sequence by id = %d, by last-row read = %d: the two readers disagree about the same row", seq, entry.RowID)
	}

	// The row written before it must sort below it — that is the whole point
	// of the number.
	earlier, ok, err := s.MessageSeq(ctx, domain.MessageID(rows[0].ID))
	if err != nil || !ok {
		t.Fatalf("sequence of the earlier row: ok=%v err=%v", ok, err)
	}
	if earlier >= seq {
		t.Fatalf("the row written first has sequence %d, the one written after it %d", earlier, seq)
	}

	if _, ok, err := s.MessageSeq(ctx, domain.MessageID("never-written")); err != nil || ok {
		t.Fatalf("an unknown id reported ok=%v err=%v; it must report absence without an error", ok, err)
	}
}

func TestReadLastEntryPerPeerCarriesItsArrivalSequence(t *testing.T) {
	ctx := context.Background()
	s := storeFor(t, lastEntrySelf)
	appendInOrder(t, s, laggingPeerRows()...)

	perPeer, err := s.ReadLastEntryPerPeer(ctx)
	if err != nil {
		t.Fatalf("read last entry per peer: %v", err)
	}
	entry, ok := perPeer[lastEntryPeer]
	if !ok {
		t.Fatalf("no entry for %s", lastEntryPeer)
	}
	if entry.RowID <= 0 {
		t.Fatalf("last entry %q came back with sequence %d", entry.ID, entry.RowID)
	}
}

func TestReadLastEntryFollowsArrivalNotTheSendersClock(t *testing.T) {
	ctx := context.Background()
	peer := domain.PeerIdentityFromWire(lastEntryPeer)

	for _, tc := range []struct {
		name string
		rows []Entry
	}{
		{"peer clock behind ours", laggingPeerRows()},
		{"same second, text order inverted", subsecondRows()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storeFor(t, lastEntrySelf)
			appendInOrder(t, s, tc.rows...)
			want := tc.rows[len(tc.rows)-1]

			entry, err := s.ReadLastEntry(ctx, "dm", peer)
			if err != nil {
				t.Fatalf("read last entry: %v", err)
			}
			if entry == nil {
				t.Fatal("read last entry: no row for a conversation that has two")
			}
			if entry.ID != want.ID {
				t.Fatalf("last entry = %q (%s), want %q (%s): the row that arrived last is the last row",
					entry.ID, entry.CreatedAt, want.ID, want.CreatedAt)
			}
		})
	}
}

func TestReadLastEntryPerPeerFollowsArrival(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		rows []Entry
	}{
		{"peer clock behind ours", laggingPeerRows()},
		{"same second, text order inverted", subsecondRows()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storeFor(t, lastEntrySelf)
			appendInOrder(t, s, tc.rows...)
			want := tc.rows[len(tc.rows)-1]

			perPeer, err := s.ReadLastEntryPerPeer(ctx)
			if err != nil {
				t.Fatalf("read last entry per peer: %v", err)
			}
			entry, ok := perPeer[lastEntryPeer]
			if !ok {
				t.Fatalf("no entry for %s; got %d conversations", lastEntryPeer, len(perPeer))
			}
			if entry.ID != want.ID {
				t.Fatalf("last entry = %q (%s), want %q (%s): the row that arrived last is the last row",
					entry.ID, entry.CreatedAt, want.ID, want.CreatedAt)
			}
		})
	}
}

// TestListConversationsReportsTheLastArrivedRow keeps the summary honest about
// the same question. It is a diagnostic surface rather than the sidebar's
// source, but two definitions of "the last message" inside one file is how the
// sidebar came to disagree with itself in the first place.
func TestListConversationsReportsTheLastArrivedRow(t *testing.T) {
	ctx := context.Background()
	s := storeFor(t, lastEntrySelf)
	rows := laggingPeerRows()
	appendInOrder(t, s, rows...)

	summaries, err := s.ListConversations(ctx)
	if err != nil {
		t.Fatalf("list conversations: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("got %d conversations, want 1", len(summaries))
	}

	want, ok := parseStoredTimestamp(rows[len(rows)-1].CreatedAt)
	if !ok {
		t.Fatalf("test fixture: %q does not parse", rows[len(rows)-1].CreatedAt)
	}
	if !summaries[0].LastMessage.Equal(want) {
		t.Fatalf("last message = %s, want %s (the row that arrived last, not the largest stamp)",
			summaries[0].LastMessage, want)
	}
	if summaries[0].Count != len(rows) {
		t.Fatalf("count = %d, want %d", summaries[0].Count, len(rows))
	}
}
