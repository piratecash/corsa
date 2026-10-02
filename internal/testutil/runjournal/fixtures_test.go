package runjournal

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// steppingClock advances by one second on every reading, so the order in which
// records were written is visible in their timestamps without sleeping.
type steppingClock struct {
	mu   sync.Mutex
	next time.Time
}

func newSteppingClock() *steppingClock {
	return &steppingClock{next: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.next
	c.next = c.next.Add(time.Second)
	return now
}

// writeTree creates files (slash-separated relative path → content) under dir.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("mkdir for %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
}

// snapshotOf takes a one-root, non-test-Go fingerprint of a fresh tree holding
// one file with the given content.
func snapshotOf(t *testing.T, name FingerprintName, content string) Snapshot {
	t.Helper()
	base := t.TempDir()
	writeTree(t, base, map[string]string{"pkg/code.go": content})
	snapshot, err := TakeSnapshot(context.Background(), SourceSpec{
		Name:  name,
		Base:  base,
		Roots: []SourceRoot{{Dir: "pkg", Recursive: true, Filter: NonTestGo}},
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", name, err)
	}
	return snapshot
}

// standSources is the two-fingerprint shape the load stand uses.
func standSources(t *testing.T, measured, stand string) []Snapshot {
	t.Helper()
	return []Snapshot{snapshotOf(t, "measured", measured), snapshotOf(t, "stand", stand)}
}

func newJournal(t *testing.T, dir string, sources []Snapshot) *Journal {
	t.Helper()
	journal, err := New(Config{Dir: dir, Sources: sources, Clock: newSteppingClock()})
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	return journal
}

func gridKey(label string, nodes string) ConfigKey {
	return ConfigKey{
		Measurement: "loadstand",
		Label:       Label(label),
		Params: []Param{
			{Name: "nodes", Value: ParamValue(nodes)},
			{Name: "churn", Value: "quiet"},
			{Name: "seed", Value: "7"},
		},
	}
}

func gridKeys(count int) []ConfigKey {
	keys := make([]ConfigKey, 0, count)
	for index := range count {
		keys = append(keys, gridKey("grid", string(rune('a'+index))))
	}
	return keys
}

func idsOf(keys ...ConfigKey) []ConfigID {
	ids := make([]ConfigID, 0, len(keys))
	for _, key := range keys {
		ids = append(ids, key.ID())
	}
	return ids
}

func newJournalWithClock(t *testing.T, dir string, sources []Snapshot, clock Clock) *Journal {
	t.Helper()
	journal, err := New(Config{Dir: dir, Sources: sources, Clock: clock})
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	return journal
}

// sealRecord builds a record from a raw header line, with a checksum that
// matches it, so a test can produce a file that verifies against itself and
// still says something the writer would never write.
func sealRecord(headerLine, body []byte) []byte {
	raw := []byte(recordFormat + "\n")
	raw = append(raw, headerLine...)
	raw = append(raw, '\n')
	raw = append(raw, checksumPrefix+recordChecksum(headerLine, body)+"\n"...)
	return append(raw, body...)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // a file this test wrote
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw
}

func overwriteFile(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// attemptFiles lists the attempt files of a key in dir, in byte order.
func attemptFiles(t *testing.T, dir string, key ConfigKey) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return key.attemptNames(names)
}
