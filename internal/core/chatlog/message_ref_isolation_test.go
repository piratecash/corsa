package chatlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestMessageRefJournalsAndLegacyAPIsDoNotMergeCollisions(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	bob := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	carol := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	refs := []domain.MessageRef{{Topic: "dm", ID: "same", Sender: self.String(), Recipient: bob.String()}, {Topic: "dm", ID: "same", Sender: self.String(), Recipient: carol.String()}, {Topic: "dm", ID: "same", Sender: bob.String(), Recipient: self.String()}, {Topic: "global", ID: "same", Sender: bob.String(), Recipient: self.String()}}
	seqs := map[domain.MessageRef]int64{}
	for _, ref := range refs {
		entry := Entry{ID: ref.ID, Sender: ref.Sender, Recipient: ref.Recipient, Body: ref.Key(), Flag: "any-delete", Metadata: NeverEmittedMetadata, CreatedAt: now.Format(time.RFC3339Nano)}
		if inserted, err := s.AppendReportNew(ctx, ref.Topic, self, entry); err != nil || !inserted {
			t.Fatalf("insert %v =%v %v", ref, inserted, err)
		}
		seq, found, err := s.MessageSeqByRef(ctx, ref)
		if err != nil || !found {
			t.Fatalf("sequence %v %v", found, err)
		}
		seqs[ref] = seq
	}
	if err := s.MarkOnWireRefs(ctx, refs[:1]); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearNeverEmittedRefs(ctx, refs[:1]); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkSeenConfirmedRef(ctx, refs[2]); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDeliveryFailedRef(ctx, refs[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateStatusRef(ctx, refs[0], StatusDelivered); err != nil {
		t.Fatal(err)
	}
	statuses, err := s.StoredMessageRefStatuses(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range refs {
		entry, found, err := s.EntryByRef(ctx, ref)
		if err != nil || !found || entry.Body != ref.Key() || entry.RowID != seqs[ref] {
			t.Fatalf("exact row %v changed: %+v %v %v", ref, entry, found, err)
		}
		onWire, known := OnWire(entry.Metadata)
		if !known || onWire != (i == 0) || NeverEmitted(entry.Metadata) != (i != 0) {
			t.Fatalf("emission marks crossed identity: %v %s", ref, entry.Metadata)
		}
		want := StatusSent
		if i == 0 {
			want = StatusDelivered
		}
		if statuses[ref] != want {
			t.Fatalf("status crossed identity %v =%s", ref, statuses[ref])
		}
	}
	for _, tc := range []struct {
		table string
		want  domain.MessageRef
	}{{"seen_ack", refs[2]}, {"delivery_failed", refs[1]}} {
		var got domain.MessageRef
		if err := s.db.QueryRowContext(ctx, `SELECT topic,id,sender,recipient FROM `+tc.table).Scan(&got.Topic, &got.ID, &got.Sender, &got.Recipient); err != nil || got != tc.want {
			t.Fatalf("%s=%+v err=%v", tc.table, got, err)
		}
	}
	for _, attempt := range []struct {
		name string
		run  func() error
	}{
		{"entry", func() error { _, _, err := s.EntryByID(ctx, "same"); return err }},
		{"sequence", func() error { _, _, err := s.MessageSeq(ctx, "same"); return err }},
		{"status", func() error { _, err := s.UpdateStatus(ctx, "dm", bob, "same", StatusSeen); return err }},
		{"emission", func() error { return s.MarkOnWire(ctx, []domain.MessageID{"same"}) }},
		{"failed", func() error { return s.MarkDeliveryFailed(ctx, "same") }},
		{"delete", func() error { _, err := s.DeleteByID(ctx, "same"); return err }},
		{"peer_delete", func() error { _, err := s.DeleteInConversation(ctx, bob, "same"); return err }},
	} {
		t.Run(attempt.name, func(t *testing.T) {
			if err := attempt.run(); !errors.Is(err, domain.ErrAmbiguousMessage) {
				t.Fatalf("ambiguous legacy request=%v", err)
			}
		})
	}
	if removed, err := s.DeleteByRef(ctx, refs[0]); err != nil || !removed {
		t.Fatalf("exact delete=%v %v", removed, err)
	}
	for _, ref := range refs[1:] {
		if _, found, err := s.EntryByRef(ctx, ref); err != nil || !found {
			t.Fatalf("exact delete removed neighbour %v: %v %v", ref, found, err)
		}
	}
}

func TestDeleteIntentsAndRecoveryRootsStayPeerScoped(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	bob := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	carol := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	var intents []DeleteIntent
	for _, peer := range []domain.PeerIdentity{bob, carol} {
		ref := domain.MessageRef{Topic: "dm", ID: "12345678-1234-4234-8234-123456789abc", Sender: self.String(), Recipient: peer.String()}
		if err := s.Append(ctx, "dm", self, Entry{ID: ref.ID, Sender: ref.Sender, Recipient: ref.Recipient, Body: "body", Flag: "any-delete", CreatedAt: now.Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		intent := DeleteIntent{Kind: DeleteIntentMessage, MessageID: domain.MessageID(ref.ID), Peer: peer, Ref: ref, CreatedAt: now, NextAttemptAt: now}
		if removed, err := s.DeleteWithIntent(ctx, intent); err != nil || !removed {
			t.Fatalf("delete-intent=%v %v", removed, err)
		}
		intents = append(intents, intent)
		if _, admitted, _, err := s.AdmitResendIntent(ctx, ResendIntent{Root: "same-root", OriginalID: "original", Peer: peer.String(), ReplacementID: "replacement", CreatedAt: now}, 3, 200, RecoveryProtectedWork{}); err != nil || !admitted {
			t.Fatalf("resend admission=%v %v", admitted, err)
		}
	}
	if _, _, err := s.DeleteIntentByID(ctx, "12345678-1234-4234-8234-123456789abc"); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy intent lookup=%v", err)
	}
	if _, _, err := s.ResendIntentByRoot(ctx, "same-root"); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy root lookup=%v", err)
	}
	if err := s.RecordDeleteIntentAttemptFor(ctx, intents[0], now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	a, found, err := s.DeleteIntentForPeer(ctx, bob, "12345678-1234-4234-8234-123456789abc")
	if err != nil || !found || a.Attempts != 1 {
		t.Fatalf("bob attempt=%+v %v %v", a, found, err)
	}
	b, found, err := s.DeleteIntentForPeer(ctx, carol, "12345678-1234-4234-8234-123456789abc")
	if err != nil || !found || b.Attempts != 0 {
		t.Fatalf("carol was charged=%+v %v %v", b, found, err)
	}
	if dropped, err := s.DropDeleteIntentFor(ctx, intents[0]); err != nil || !dropped {
		t.Fatalf("drop=%v %v", dropped, err)
	}
	if _, found, err := s.DeleteIntentForPeer(ctx, carol, "12345678-1234-4234-8234-123456789abc"); err != nil || !found {
		t.Fatal("another peer's intent was retired")
	}
	if err := s.DeleteResendIntentByPeer(ctx, bob.String(), "same-root"); err != nil {
		t.Fatal(err)
	}
	if got, found, err := s.ResendIntentByPeer(ctx, carol.String(), "same-root"); err != nil || !found || got.Peer != carol.String() {
		t.Fatalf("another peer's root was retired: %+v %v %v", got, found, err)
	}
}

func TestReactionProjectionRefusesDirectionlessCollisions(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	bob := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	carol := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, peer := range []domain.PeerIdentity{bob, carol} {
		if err := s.Append(ctx, "dm", self, Entry{ID: "same", Sender: self.String(), Recipient: peer.String(), Body: "sealed", Flag: "any-delete", CreatedAt: now.Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		fact := domain.ReactionFact{Scope: domain.ReactionScopeForPeer(peer), Key: domain.ReactionKey{MessageID: "same", Actor: self, Emoji: "x"}, Op: domain.ReactionSet, Clock: 1}
		if changed, err := s.ApplyReactionFact(ctx, fact, now); err != nil || !changed {
			t.Fatalf("reaction scope %s changed=%v err=%v", peer, changed, err)
		}
	}
	for _, peer := range []domain.PeerIdentity{bob, carol} {
		facts, err := s.ReactionFactsInConversation(ctx, peer, "same")
		if err != nil || len(facts) != 1 || facts[0].Scope != domain.ReactionScopeForPeer(peer) {
			t.Fatalf("reaction scope merged %s: %+v %v", peer, facts, err)
		}
	}
	if err := s.Append(ctx, "dm", self, Entry{ID: "same", Sender: bob.String(), Recipient: self.String(), Body: "opposite", Flag: "any-delete", CreatedAt: now.Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	ambiguous := domain.ReactionFact{Scope: domain.ReactionScopeForPeer(bob), Key: domain.ReactionKey{MessageID: "same", Actor: self, Emoji: "x"}, Op: domain.ReactionCleared, Clock: 2}
	if _, err := s.ApplyReactionFact(ctx, ambiguous, now); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("directionless reaction did not refuse ambiguity=%v", err)
	}
	if _, err := s.ReactionsForScope(ctx, domain.ReactionScopeForPeer(bob), self); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy reaction map merged directions=%v", err)
	}
	if facts, err := s.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(bob), 64, 0); err != nil || len(facts) != 0 {
		t.Fatalf("ambiguous target was re-offered on unchanged wire: %+v %v", facts, err)
	}
	facts, err := s.ReactionFactsForRef(ctx, domain.MessageRef{Topic: "dm", ID: "same", Sender: self.String(), Recipient: bob.String()})
	if err != nil || len(facts) != 1 || facts[0].Op != domain.ReactionSet || facts[0].Clock != 1 {
		t.Fatalf("refused reaction mutated exact fact=%+v %v", facts, err)
	}
}

// A receiver may record the recovery link before the replacement arrives.
// A different conversation's row carrying that replacement ID is unrelated.
func TestFutureRecoveryReplacementInheritsOnlyTheOriginalConversation(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	stranger := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	original := domain.MessageRef{Topic: "dm", ID: "original", Sender: peer.String(), Recipient: self.String()}
	unrelated := domain.MessageRef{Topic: "dm", ID: "future", Sender: stranger.String(), Recipient: self.String()}
	for _, ref := range []domain.MessageRef{original, unrelated} {
		if err := s.Append(ctx, ref.Topic, self, Entry{ID: ref.ID, Sender: ref.Sender, Recipient: ref.Recipient, Body: ref.Key(), CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if changed, err := s.MarkDecryptFailedRef(ctx, original); err != nil || !changed {
		t.Fatalf("flag original: changed=%v err=%v", changed, err)
	}
	if changed, err := s.MarkSupersededCollapsing(ctx, original.ID, unrelated.ID, original.ID); err != nil || !changed {
		t.Fatalf("reserve future replacement: changed=%v err=%v", changed, err)
	}
	marks, found, err := s.EntryRecoveryMarksRef(ctx, original)
	if err != nil || !found || marks.SupersededBy != "future" || marks.DecryptFailed {
		t.Fatalf("original marks=%+v found=%v err=%v", marks, found, err)
	}
	entry, found, err := s.EntryByRef(ctx, unrelated)
	if err != nil || !found || entry.Metadata != "" || entry.Body != unrelated.Key() {
		t.Fatalf("unrelated replacement-ID row changed: %+v found=%v err=%v", entry, found, err)
	}
}

func TestAppendReportNewDoesNotCallInvalidEntriesDuplicates(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	s := newTestStore(t, self)
	ctx := context.Background()
	for _, bad := range []string{"flag", "topic", "status"} {
		t.Run(bad, func(t *testing.T) {
			topic := "dm"
			entry := Entry{ID: "invalid-" + bad, Sender: peer.String(), Recipient: self.String(), Body: "invalid", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			switch bad {
			case "flag":
				entry.Flag = "invalid-flag"
			case "topic":
				topic = "invalid-topic"
			case "status":
				entry.DeliveryStatus = "invalid-status"
			}
			if inserted, err := s.AppendReportNew(ctx, topic, self, entry); !errors.Is(err, ErrInvalidEntry) || inserted {
				t.Fatalf("invalid entry was classified as duplicate/new: inserted=%v err=%v", inserted, err)
			}
			ref := domain.MessageRef{Topic: topic, ID: entry.ID, Sender: entry.Sender, Recipient: entry.Recipient}
			if _, found, err := s.EntryByRef(ctx, ref); err != nil || found {
				t.Fatalf("invalid entry stored: found=%v err=%v", found, err)
			}
		})
	}
}
