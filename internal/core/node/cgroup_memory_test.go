package node

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadUintFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	good := filepath.Join(dir, "num")
	if err := os.WriteFile(good, []byte("123456\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if v, ok := readUintFile(good); !ok || v != 123456 {
		t.Fatalf("readUintFile(good) = (%d, %v), want (123456, true)", v, ok)
	}

	bad := filepath.Join(dir, "bad")
	if err := os.WriteFile(bad, []byte("not-a-number"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readUintFile(bad); ok {
		t.Fatal("readUintFile(bad) must return ok=false")
	}

	if _, ok := readUintFile(filepath.Join(dir, "missing")); ok {
		t.Fatal("readUintFile(missing) must return ok=false")
	}
}

func TestReadCgroupV2Max(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// "max" → present but unlimited (0, true).
	maxPath := filepath.Join(dir, "memory.max")
	if err := os.WriteFile(maxPath, []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if v, ok := readCgroupV2Max(maxPath); !ok || v != 0 {
		t.Fatalf("readCgroupV2Max(max) = (%d, %v), want (0, true)", v, ok)
	}

	// Numeric limit.
	numPath := filepath.Join(dir, "memory.max.num")
	if err := os.WriteFile(numPath, []byte("536870912\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if v, ok := readCgroupV2Max(numPath); !ok || v != 536870912 {
		t.Fatalf("readCgroupV2Max(num) = (%d, %v), want (536870912, true)", v, ok)
	}

	// Missing → (0, false) so the caller falls through to v1.
	if _, ok := readCgroupV2Max(filepath.Join(dir, "missing")); ok {
		t.Fatal("readCgroupV2Max(missing) must return ok=false")
	}
}

// TestReadCgroupMemory_IsTotal pins that readCgroupMemory is a total
// function: it never panics and returns on any host. The returned values
// are HOST-DEPENDENT — zero on a machine without the cgroup memory files
// (CI / macOS dev / no controller), but legitimately non-zero on a Linux
// host or container with the controller mounted. We therefore make no
// assertion on the magnitudes here; the parsing/unlimited-sentinel logic
// is pinned deterministically by TestReadCgroupV2Max and TestReadUintFile
// against temp files.
func TestReadCgroupMemory_IsTotal(t *testing.T) {
	t.Parallel()
	limit, usage := readCgroupMemory()
	_ = limit
	_ = usage
}
