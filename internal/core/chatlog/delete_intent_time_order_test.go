package chatlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestDeleteIntentDueUsesExactNanosecondsAcrossLegacyFractions(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		due, now time.Duration
		want     bool
		fixed    bool
	}{
		{"past-prefix", 100 * time.Millisecond, 110 * time.Millisecond, true, false},
		{"future-prefix", 110 * time.Millisecond, 100 * time.Millisecond, false, false},
		{"whole-second", 0, 500 * time.Millisecond, true, false},
		{"future-fraction", 500 * time.Millisecond, 0, false, false},
		{"one-nanosecond-future", 101 * time.Nanosecond, 100 * time.Nanosecond, false, false},
		{"one-nanosecond-past", 100 * time.Nanosecond, 101 * time.Nanosecond, true, false},
		{"exact-boundary", 123456789 * time.Nanosecond, 123456789 * time.Nanosecond, true, false},
		{"fixed-trailing-zero-legacy", 100 * time.Millisecond, 110 * time.Millisecond, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newIntentStore(t)
			peer := domain.PeerIdentityFromWire(intentPeer)
			dueAt, now := base.Add(tc.due), base.Add(tc.now)
			if err := store.NoteDeleteIntent(ctx, DeleteIntent{MessageID: intentMsgID, Peer: peer, CreatedAt: base, NextAttemptAt: dueAt}); err != nil {
				t.Fatal(err)
			}
			if err := store.NoteConversationDeleteIntent(ctx, DeleteIntent{Kind: DeleteIntentConversation, Peer: domain.PeerIdentityFromWire("3333333333333333333333333333333333333333"), RequestID: domain.ConversationDeleteRequestID("81111111-2222-4333-8444-555555555555"), CreatedAt: base, NextAttemptAt: dueAt}); err != nil {
				t.Fatal(err)
			}
			// Distinct peers keep these original task kinds independent: a
			// conversation wipe must not carry the message request under test.
			if tc.fixed {
				if _, err := store.db.ExecContext(ctx, `UPDATE message_delete_intents SET next_attempt_at = ?`, dueAt.Format("2006-01-02T15:04:05.000000000Z")); err != nil {
					t.Fatal(err)
				}
			}
			got, err := store.DueDeleteIntents(ctx, now, 10)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.want {
				want = 2
			}
			if len(got) != want {
				t.Fatalf("due count=%d, want%d for due=%s now=%s", len(got), want, dueAt, now)
			}
			if tc.want {
				seen := map[DeleteIntentKind]bool{}
				for _, intent := range got {
					if !intent.NextAttemptAt.Equal(dueAt) {
						t.Fatal("stored nanoseconds changed")
					}
					seen[intent.Kind] = true
				}
				if !seen[DeleteIntentMessage] || !seen[DeleteIntentConversation] {
					t.Fatal("one task kind disappeared")
				}
			}
		})
	}
}

func TestDeleteIntentChronologicalLimitAndActualQueryPlans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newIntentStore(t)
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	ids := []domain.MessageID{intentMsgID, intentMsgB, domain.MessageID("8e3f2c8f-8f3f-4c9f-9d7e-3e4f5a6b7c8d")}
	// Reverse chronological insertion; TEXT would put .11Z before .1Z.
	for i, ns := range []time.Duration{110 * time.Millisecond, 100*time.Millisecond + time.Nanosecond, 100 * time.Millisecond} {
		if err := store.NoteDeleteIntent(ctx, DeleteIntent{MessageID: ids[i], Peer: domain.PeerIdentityFromWire(intentPeer), CreatedAt: base, NextAttemptAt: base.Add(ns)}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.DueDeleteIntents(ctx, base.Add(time.Second), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].MessageID != ids[2] || got[1].MessageID != ids[1] {
		t.Fatalf("wrong chronological limited result: %+v", got)
	}
	if got, err := store.DueDeleteIntents(ctx, base.Add(time.Second), 0); err != nil || len(got) != 0 {
		t.Fatalf("non-positive limit: %+v %v", got, err)
	}
	plans := func(query string, args ...any) string {
		t.Helper()
		rows, err := store.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				t.Error(err)
			}
		}()
		var lines []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(lines, "\n")
	}
	newPlan := plans(dueDeleteIntentsSQL, base.Add(time.Second).Format(deleteIntentComparisonLayout), 2)
	if !strings.Contains(newPlan, "idx_message_delete_intents_due_ns") || strings.Contains(newPlan, "USE TEMP B-TREE") {
		t.Fatalf("exact production due query lost its ordered index:\n%s", newPlan)
	}
	revivePlan := plans(reviveDeleteIntentsSQL, base.Format(time.RFC3339Nano), intentPeer, int(HoldPeerAbsent), base.Format(deleteIntentComparisonLayout))
	if !strings.Contains(revivePlan, "idx_message_delete_intents_peer_held_ns") {
		t.Fatalf("exact production revival query lost its range index:\n%s", revivePlan)
	}
	// Keep the old query as an explicit comparison, not a current execution
	// path or a claim that its NOT-covered filtering was O(limit).
	oldSQL := `SELECT ` + deleteIntentColumns + ` FROM message_delete_intents AS i WHERE i.owed=1 AND i.next_attempt_at<=? AND NOT (` + coveredByAStandingWipe + `) ORDER BY i.next_attempt_at ASC LIMIT ?`
	oldPlan := plans(oldSQL, base.Add(time.Second).Format(time.RFC3339Nano), 2)
	t.Logf("old due plan:\n%s\nnew due plan:\n%s\nnew revive plan:\n%s", oldPlan, newPlan, revivePlan)
}

func TestDeleteIntentRevivalKeepsExactTimeAndChargingBoundaries(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		due, now  time.Duration
		hold      DeleteIntentHold
		otherPeer bool
		want      bool
	}{
		{"future-prefix-revived", 110 * time.Millisecond, 100 * time.Millisecond, HoldPeerAbsent, false, true},
		{"already-due-prefix-unchanged", 100 * time.Millisecond, 110 * time.Millisecond, HoldPeerAbsent, false, false},
		{"one-nanosecond-future", 101 * time.Nanosecond, 100 * time.Nanosecond, HoldPeerAbsent, false, true},
		{"equal-boundary-unchanged", 100 * time.Millisecond, 100 * time.Millisecond, HoldPeerAbsent, false, false},
		{"charged-backoff-not-revived", 110 * time.Millisecond, 100 * time.Millisecond, HoldNone, false, false},
		{"other-peer-not-revived", 110 * time.Millisecond, 100 * time.Millisecond, HoldPeerAbsent, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newIntentStore(t)
			peer := domain.PeerIdentityFromWire(intentPeer)
			if err := store.NoteDeleteIntent(ctx, DeleteIntent{MessageID: intentMsgID, Peer: peer, CreatedAt: base, NextAttemptAt: base.Add(tc.due), Attempts: 3, Hold: tc.hold}); err != nil {
				t.Fatal(err)
			}
			target := peer
			if tc.otherPeer {
				target = domain.PeerIdentityFromWire("3333333333333333333333333333333333333333")
			}
			count, err := store.ReviveDeleteIntentsForPeer(ctx, target, base.Add(tc.now))
			if err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if tc.want {
				want = 1
			}
			if count != want {
				t.Fatalf("revived%d want%d", count, want)
			}
			got, found, err := store.DeleteIntentByID(ctx, intentMsgID)
			if err != nil || !found {
				t.Fatalf("intent missing: %v", err)
			}
			wantAt, wantHold := base.Add(tc.due), tc.hold
			if tc.want {
				wantAt, wantHold = base.Add(tc.now), HoldNone
			}
			if got.Attempts != 3 || got.Hold != wantHold || !got.NextAttemptAt.Equal(wantAt) || got.Peer != peer {
				t.Fatalf("revival changed wrong state: %+v", got)
			}
		})
	}
}
