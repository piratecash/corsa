package desktop

import (
	"testing"

	"corsa/internal/core/service"
)

func TestKnownRecipientsIncludesDiscovered(t *testing.T) {
	contacts := map[string]service.Contact{
		"trusted-peer": {BoxKey: "box1", PubKey: "pub1"},
	}
	discovered := map[string]struct{}{
		"chatlog-peer": {},
	}

	recipients := knownRecipients(contacts, discovered, "self-addr")

	found := make(map[string]bool)
	for _, r := range recipients {
		found[r] = true
	}

	if !found["trusted-peer"] {
		t.Fatal("expected trusted-peer in recipients")
	}
	if !found["chatlog-peer"] {
		t.Fatal("expected chatlog-peer (from discovered/chatlog) in recipients")
	}
	if found["self-addr"] {
		t.Fatal("self-addr should be excluded from recipients")
	}
}

func TestKnownRecipientsDeduplicates(t *testing.T) {
	contacts := map[string]service.Contact{
		"peer-1": {BoxKey: "box1", PubKey: "pub1"},
	}
	// Same peer also in discovered — should not duplicate.
	discovered := map[string]struct{}{
		"peer-1": {},
		"peer-2": {},
	}

	recipients := knownRecipients(contacts, discovered, "self")

	if len(recipients) != 2 {
		t.Fatalf("expected 2 unique recipients, got %d: %v", len(recipients), recipients)
	}
}

func TestMergeRecipientOrder(t *testing.T) {
	recipients := []string{"a", "b", "c", "d"}
	order := []string{"c", "a"}

	merged := mergeRecipientOrder(recipients, order)

	// "c" and "a" should come first (in order), then "b" and "d" (sorted).
	if len(merged) != 4 {
		t.Fatalf("expected 4, got %d: %v", len(merged), merged)
	}
	if merged[0] != "c" || merged[1] != "a" {
		t.Fatalf("expected [c, a, ...], got %v", merged)
	}
}

func TestMergeRecipientOrderEmpty(t *testing.T) {
	merged := mergeRecipientOrder(nil, []string{"a"})
	if merged != nil {
		t.Fatalf("expected nil for empty recipients, got %v", merged)
	}
}

func TestSearchKnownIdentities(t *testing.T) {
	knownIDs := []string{"abc-def-ghi", "xyz-abc-123", "zzz-yyy-xxx"}
	recipients := []string{"abc-def-ghi"} // already listed
	self := "self-addr"

	results := searchKnownIdentities(knownIDs, recipients, self, "abc")

	// "abc-def-ghi" is already listed → excluded.
	// "xyz-abc-123" matches query → included.
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}
	if results[0] != "xyz-abc-123" {
		t.Fatalf("expected xyz-abc-123, got %s", results[0])
	}
}

func TestSearchKnownIdentitiesEmptyQuery(t *testing.T) {
	results := searchKnownIdentities([]string{"a", "b"}, nil, "self", "")
	if results != nil {
		t.Fatalf("expected nil for empty query, got %v", results)
	}
}

func TestShortFingerprint(t *testing.T) {
	short := "abc"
	if got := shortFingerprint(short); got != short {
		t.Fatalf("expected %q, got %q", short, got)
	}

	long := "abcdefghijklmnopqrstuvwxyz"
	got := shortFingerprint(long)
	if got != "abcdefgh...uvwxyz" {
		t.Fatalf("expected 'abcdefgh...uvwxyz', got %q", got)
	}
}

func TestEllipsize(t *testing.T) {
	if got := ellipsize("hello", 10); got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
	if got := ellipsize("hello world", 5); got != "hell…" {
		t.Fatalf("expected 'hell…', got %q", got)
	}
	if got := ellipsize("", 5); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// TestNetworkStatusSummary verifies that the aggregate network status is based
// on the number of usable peers (healthy + degraded). Stalled peers are
// connected at TCP level but excluded from routing, so they do not count
// as usable for the aggregate status label.
func TestNetworkStatusSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		peers         []service.PeerHealth
		wantState     string
		wantConnected int
		wantTotal     int
	}{
		{
			name:      "no peers is offline",
			peers:     nil,
			wantState: "offline",
		},
		{
			name: "all reconnecting",
			peers: []service.PeerHealth{
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "reconnecting",
			wantConnected: 0,
			wantTotal:     2,
		},
		{
			name: "single healthy peer is limited",
			peers: []service.PeerHealth{
				{State: "healthy"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "single stalled peer is limited (connected but not usable)",
			peers: []service.PeerHealth{
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "two usable peers are healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     2,
		},
		{
			name: "all stalled is limited not healthy (P2 regression)",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "stalled"},
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 3,
			wantTotal:     3,
		},
		{
			name: "less than half usable is warning",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "warning",
			wantConnected: 2,
			wantTotal:     7,
		},
		{
			name: "half usable is healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     4,
		},
		{
			name: "mix of stalled and degraded uses only usable for status",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 4,
			wantTotal:     4,
		},
		{
			name: "stalled peers do not help reach healthy threshold",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "stalled"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "warning",
			wantConnected: 4,
			wantTotal:     9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := service.NodeStatus{PeerHealth: tt.peers}
			gotState, gotConnected, gotTotal, _ := networkStatusSummary(status)
			if gotState != tt.wantState {
				t.Errorf("state: got %q, want %q", gotState, tt.wantState)
			}
			if gotConnected != tt.wantConnected {
				t.Errorf("connected: got %d, want %d", gotConnected, tt.wantConnected)
			}
			if gotTotal != tt.wantTotal {
				t.Errorf("total: got %d, want %d", gotTotal, tt.wantTotal)
			}
		})
	}
}
