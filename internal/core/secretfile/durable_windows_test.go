//go:build windows

package secretfile

import (
	"errors"
	"testing"
)

func TestUnsupportedDurableBackendRefusesBeforeMutation(t *testing.T) {
	dir, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Write("main", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if outcome, err := dir.DurableWrite("main", []byte("new")); outcome != DurableNotCommitted || !errors.Is(err, ErrDurabilityUnsupported) {
		t.Fatalf("write %v %v", outcome, err)
	}
	if outcome, err := dir.DurableRemove("main"); outcome != DurableNotCommitted || !errors.Is(err, ErrDurabilityUnsupported) {
		t.Fatalf("remove %v %v", outcome, err)
	}
	raw, err := dir.ReadFile("main")
	if err != nil || string(raw) != "old" {
		t.Fatalf("unsupported backend mutated main %q %v", raw, err)
	}
}
