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
