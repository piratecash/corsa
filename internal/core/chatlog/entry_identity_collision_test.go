package chatlog

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Storage must retain the original wire ID and both endpoint identities.
// A second author, recipient, or direction must not be silently INSERT OR
// IGNORE'd as a retry of an unrelated message. Bodies here are opaque storage
// fixtures; the node companion independently exercises signed envelopes.
func TestAppendReportNewUsesBothMessageEndpoints(t *testing.T) {
	const self = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const peer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const other = "cccccccccccccccccccccccccccccccccccccccc"
	for _, tc := range []struct {
		name             string
		from, to         string
		nextFrom, nextTo string
	}{
		{"different-author", peer, self, other, self},
		{"different-recipient", self, peer, self, other},
		{"opposite-direction", peer, self, self, peer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storeFor(t, self)
			ctx := context.Background()
			first := Entry{ID: "440a75c1-ddf9-4bcc-8bbf-ae2ec866d271", Sender: tc.from, Recipient: tc.to, Body: "first opaque body", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			next := first
			next.Sender, next.Recipient, next.Body = tc.nextFrom, tc.nextTo, "second opaque body"
			for _, want := range []Entry{first, next} {
				inserted, err := s.AppendReportNew(ctx, "dm", domain.PeerIdentityFromWire(self), want)
				if err != nil || !inserted {
					t.Fatalf("distinct endpoint identity must be inserted: inserted=%v err=%v", inserted, err)
				}
			}
			if inserted, err := s.AppendReportNew(ctx, "dm", domain.PeerIdentityFromWire(self), next); err != nil || inserted {
				t.Fatalf("exact endpoint identity retry: inserted=%v err=%v", inserted, err)
			}
			for _, want := range []Entry{first, next} {
				conversation := want.Sender
				if conversation == self {
					conversation = want.Recipient
				}
				rows, err := s.Read(ctx, "dm", domain.PeerIdentityFromWire(conversation))
				if err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, row := range rows {
					if row.ID == want.ID && row.Sender == want.Sender && row.Recipient == want.Recipient && row.Body == want.Body {
						found++
					}
				}
				if found != 1 {
					t.Fatalf("exact sealed-row identity count=%d, want 1", found)
				}
			}
		})
	}
}
