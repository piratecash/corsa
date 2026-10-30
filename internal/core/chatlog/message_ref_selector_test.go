package chatlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestDeleteInConversationInvalidSelectorsCannotDeleteGlobalOrDM(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	refs := []domain.MessageRef{
		{Topic: "global", ID: "shared-id", Sender: self.String(), Recipient: peer.String()},
		{Topic: "dm", ID: "shared-id", Sender: self.String(), Recipient: peer.String()},
	}
	before := map[domain.MessageRef]Entry{}
	for _, ref := range refs {
		entry := Entry{ID: ref.ID, Sender: ref.Sender, Recipient: ref.Recipient, Body: ref.Key(), Metadata: NeverEmittedMetadata, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if inserted, err := s.AppendReportNew(ctx, ref.Topic, self, entry); err != nil || !inserted {
			t.Fatalf("seed %v: inserted=%v err=%v", ref, inserted, err)
		}
		if err := s.MarkSeenConfirmedRef(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkDeliveryFailedRef(ctx, ref); err != nil {
			t.Fatal(err)
		}
		stored, found, err := s.EntryByRef(ctx, ref)
		if err != nil || !found {
			t.Fatalf("seed row %v: found=%v err=%v", ref, found, err)
		}
		before[ref] = stored
	}
	assertState := func(t *testing.T, dmPresent bool) {
		t.Helper()
		for _, ref := range refs {
			wantPresent := ref.Topic == "global" || dmPresent
			entry, found, err := s.EntryByRef(ctx, ref)
			if err != nil || found != wantPresent || (found && entry != before[ref]) {
				t.Fatalf("selector changed row %v: entry=%+v found=%v want=%v err=%v", ref, entry, found, wantPresent, err)
			}
			for _, table := range []string{"seen_ack", "delivery_failed"} {
				var count int
				if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE topic=? AND id=? AND sender=? AND recipient=?`, ref.Topic, ref.ID, ref.Sender, ref.Recipient).Scan(&count); err != nil {
					t.Fatal(err)
				}
				wantCount := 0
				if wantPresent {
					wantCount = 1
				}
				if count != wantCount {
					t.Fatalf("selector changed %s journal for %v: count=%d want=%d", table, ref, count, wantCount)
				}
			}
		}
	}
	invalid := []struct {
		name string
		peer domain.PeerIdentity
		id   domain.MessageID
	}{
		{"zero-peer", domain.PeerIdentity{}, "shared-id"},
		{"empty-id", peer, ""},
		{"both-empty", domain.PeerIdentity{}, ""},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if removed, err := s.DeleteInConversation(ctx, tc.peer, tc.id); err != nil || removed {
				t.Fatalf("invalid selector: removed=%v err=%v", removed, err)
			}
			assertState(t, true)
		})
	}
	if removed, err := s.DeleteInConversation(ctx, peer, "shared-id"); err != nil || !removed {
		t.Fatalf("valid DM selector: removed=%v err=%v", removed, err)
	}
	assertState(t, false)
	// Invalid selectors are rejected before starting even a canceled transaction.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, tc := range invalid {
		if removed, err := s.DeleteInConversation(canceled, tc.peer, tc.id); err != nil || removed {
			t.Fatalf("invalid selector %s reached transaction: removed=%v err=%v", tc.name, removed, err)
		}
	}
}
