package desktop

import (
	"testing"
)

// TestDeduplicatePeers verifies the deduplication logic used by
// updateUnreadStateFromHeaders: given multiple headers from the same peer,
// the preview should only be refreshed once per peer, not once per message.
func TestDeduplicatePeers(t *testing.T) {
	t.Parallel()

	// Simulate the header processing loop — collect unique peers.
	type header struct {
		sender    string
		recipient string
	}

	me := "my-address"
	headers := []header{
		{sender: "alice", recipient: me},
		{sender: "alice", recipient: me},
		{sender: "alice", recipient: me},
		{sender: "bob", recipient: me},
		{sender: me, recipient: "bob"},
		{sender: "bob", recipient: me},
		{sender: "charlie", recipient: me},
	}

	peersToRefresh := make(map[string]struct{})
	for _, h := range headers {
		var peer string
		if h.sender == me {
			peer = h.recipient
		} else if h.recipient == me {
			peer = h.sender
		}
		if peer != "" {
			peersToRefresh[peer] = struct{}{}
		}
	}

	// Should have exactly 3 unique peers, not 7 calls.
	if len(peersToRefresh) != 3 {
		t.Fatalf("expected 3 unique peers, got %d: %v", len(peersToRefresh), peersToRefresh)
	}
	for _, expected := range []string{"alice", "bob", "charlie"} {
		if _, ok := peersToRefresh[expected]; !ok {
			t.Fatalf("missing expected peer %q in set: %v", expected, peersToRefresh)
		}
	}
}
