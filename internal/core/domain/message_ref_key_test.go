package domain

import "testing"

func TestMessageRefKeyKeepsEveryFieldAndItsBoundaries(t *testing.T) {
	values := []string{"", ":", "a", "A", "\x00", "\u00e9"}
	seen := make(map[string]MessageRef)
	for _, topic := range values {
		for _, id := range values {
			for _, sender := range values {
				for _, recipient := range values {
					ref := MessageRef{Topic: topic, ID: id, Sender: sender, Recipient: recipient}
					key := ref.Key()
					if prior, exists := seen[key]; exists && prior != ref {
						t.Fatalf("distinct local references share a key: %#v and %#v", prior, ref)
					}
					if key != ref.Key() {
						t.Fatal("a local reference changed its key")
					}
					seen[key] = ref
				}
			}
		}
	}
	if want := len(values) * len(values) * len(values) * len(values); len(seen) != want {
		t.Fatalf("distinct reference count = %d, want %d", len(seen), want)
	}
}
