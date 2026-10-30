package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// This is a positive security invariant, not the supplied attack PoC. Both
// envelopes have genuine signatures from distinct authors. Their shared outer
// ID must not erase either author, even when the bloom/backlog has forgotten
// the first arrival. The real chatlog fixture does not include the production
// MessageStoreAdapter's deletion-refusal gate; that requires a separate test.
func TestAdmissionKeepsDistinctAuthorsWithTheSameOuterID(t *testing.T) {
	for _, stage := range []string{"warm-bloom", "backlog-with-empty-bloom", "persistent-store-only"} {
		t.Run(stage, func(t *testing.T) {
			f := newCrossPathFixture(t)
			second := registerSenderKey(t, f.svc)
			if second.Address == f.sender.Address {
				t.Fatal("fixture requires distinct verified authors")
			}
			const shared = protocol.MessageID("440a75c1-ddf9-4bcc-8bbf-ae2ec866d271")
			box := identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey)
			first := incomingMessage{
				ID: shared, Topic: "dm", Sender: f.sender.Address, Recipient: f.svc.Address(),
				Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(),
				Body: sealDMBody(t, f.sender, f.svc.Address(), box),
			}
			next := first
			next.Sender = second.Address
			next.Body = sealDMBody(t, second, f.svc.Address(), box)
			if stored, _, code := f.svc.storeIncomingMessage(first, true); !stored || code != "" {
				t.Fatalf("first verified envelope: stored=%v code=%q", stored, code)
			}
			f.svc.WaitBackground()
			if stage != "warm-bloom" {
				f.svc.gossipMu.Lock()
				f.svc.seen = newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
				if stage == "persistent-store-only" {
					f.svc.topics["dm"] = nil
				}
				f.svc.gossipMu.Unlock()
			}
			if stored, _, code := f.svc.storeIncomingMessage(next, true); !stored || code != "" {
				t.Fatalf("another verified author sharing outer ID must be new: stored=%v code=%q", stored, code)
			}
			if stored, _, code := f.svc.storeIncomingMessage(next, true); stored || code != "" {
				t.Fatalf("exact identity retry must remain duplicate: stored=%v code=%q", stored, code)
			}
			for _, want := range []incomingMessage{first, next} {
				rows := f.store.rows("dm", domain.PeerIdentityFromWire(want.Sender))
				if len(rows) != 1 || rows[0].ID != string(shared) || rows[0].Sender != want.Sender || rows[0].Recipient != want.Recipient || rows[0].Body != want.Body {
					t.Fatalf("author %s lost or replaced its own sealed envelope: %+v", want.Sender, rows)
				}
			}
			if events := f.announcements(t); len(events) != 2 {
				t.Fatalf("two distinct messages and one exact retry must emit two notifications, got %d", len(events))
			}
		})
	}
}
