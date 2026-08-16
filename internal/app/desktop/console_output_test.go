package desktop

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newOverflowTestConsole(t *testing.T) *ConsoleWindow {
	t.Helper()
	c := &ConsoleWindow{
		parent:   &Window{},
		overflow: newConsoleOverflowStore(),
	}
	t.Cleanup(c.overflow.removeAll)
	return c
}

// multiMegabyteJSON builds the shape that actually crashed the desktop: one
// enormous pretty-printed JSON dump (a live-network fetchContacts reply).
func multiMegabyteJSON(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("{\n  \"contacts\": [\n")
	for i := 0; b.Len() < 5*1024*1024; i++ {
		fmt.Fprintf(&b, "    {\"address\": \"%040d\", \"pub_key\": \"%s\"},\n", i, strings.Repeat("A", 60))
	}
	b.WriteString("  ]\n}\n")
	return b.String()
}

// TestConsoleOutputCapsMultiMegabyteJSON is the crash regression: a
// multi-megabyte command output must never reach the Selectable — the
// display text is capped BEFORE layout, the complete output goes to a 0600
// overflow file, and Copy reads it back whole.
func TestConsoleOutputCapsMultiMegabyteJSON(t *testing.T) {
	c := newOverflowTestConsole(t)
	full := multiMegabyteJSON(t)

	entry := c.composeConsoleEntry(consoleEntry{Command: "fetchContacts", CreatedAt: time.Now()}, full)

	// The marker line rides on top of the cap; a kilobyte of slack keeps
	// the assertion honest without pinning the marker's exact wording.
	if len(entry.Output) > maxConsoleEntryDisplayBytes+1024 {
		t.Fatalf("display text is %d bytes — the cap did not hold", len(entry.Output))
	}
	if entry.FullBytes != len(full) {
		t.Errorf("FullBytes = %d, want %d", entry.FullBytes, len(full))
	}
	if entry.OverflowPath == "" {
		t.Fatal("truncated output produced no overflow file")
	}

	info, err := os.Stat(entry.OverflowPath)
	if err != nil {
		t.Fatalf("stat overflow: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("overflow file mode = %o, want 0600", perm)
	}
	if got := c.fullConsoleOutput(&entry); got != full {
		t.Fatal("Copy path did not return the complete output")
	}

	// Closing the window removes the whole overflow directory.
	c.overflow.removeAll()
	if _, err := os.Stat(filepath.Dir(entry.OverflowPath)); !os.IsNotExist(err) {
		t.Errorf("overflow directory survived removeAll: %v", err)
	}
}

// TestConsoleOutputLineCap: many short lines are as dangerous for the
// glyph layouter as raw bytes; the line cap cuts them independently.
func TestConsoleOutputLineCap(t *testing.T) {
	c := newOverflowTestConsole(t)
	full := strings.Repeat("x\n", 20_000)

	entry := c.composeConsoleEntry(consoleEntry{Command: "spam"}, full)
	if lines := strings.Count(entry.Output, "\n"); lines > maxConsoleEntryDisplayLines+2 {
		t.Fatalf("display text has %d lines — the line cap did not hold", lines)
	}
	if entry.OverflowPath == "" {
		t.Fatal("line-capped output produced no overflow file")
	}
}

// TestConsoleOutputSmallPassesVerbatim: an ordinary output stays untouched —
// no marker, no file.
func TestConsoleOutputSmallPassesVerbatim(t *testing.T) {
	c := newOverflowTestConsole(t)
	entry := c.composeConsoleEntry(consoleEntry{Command: "ping"}, "pong")
	if entry.Output != "pong" || entry.OverflowPath != "" {
		t.Fatalf("small output mangled: %+v", entry)
	}
	if got := c.fullConsoleOutput(&entry); got != "pong" {
		t.Fatalf("full output = %q", got)
	}
}

// TestConsoleEntriesCountEviction: the entry list is bounded and eviction
// deletes the evicted entries' overflow files.
func TestConsoleEntriesCountEviction(t *testing.T) {
	c := newOverflowTestConsole(t)

	oldest := c.composeConsoleEntry(consoleEntry{Command: "big-0"}, strings.Repeat("z", maxConsoleEntryDisplayBytes+10))
	overflowPath := oldest.OverflowPath
	if overflowPath == "" {
		t.Fatal("test setup: first entry must overflow")
	}
	c.appendConsoleEntry(oldest)
	for i := 1; i <= maxConsoleEntries; i++ {
		c.appendConsoleEntry(c.composeConsoleEntry(consoleEntry{Command: fmt.Sprintf("cmd-%d", i)}, "ok"))
	}

	c.mu.Lock()
	count := len(c.consoleEntries)
	last := c.consoleEntries[count-1].Command
	c.mu.Unlock()
	if count != maxConsoleEntries {
		t.Fatalf("entries = %d, want the cap %d", count, maxConsoleEntries)
	}
	if last == "big-0" {
		t.Fatal("the oldest entry survived past the cap")
	}
	if _, err := os.Stat(overflowPath); !os.IsNotExist(err) {
		t.Fatalf("evicted entry's overflow file survived: %v", err)
	}
}

// TestConsoleEntriesByteEviction: the TOTAL display bytes are bounded too —
// a burst of maximal entries must not accumulate without limit.
func TestConsoleEntriesByteEviction(t *testing.T) {
	c := newOverflowTestConsole(t)
	entryText := strings.Repeat("y", maxConsoleEntryDisplayBytes)
	for i := 0; i < 60; i++ {
		c.appendConsoleEntry(c.composeConsoleEntry(consoleEntry{Command: fmt.Sprintf("cmd-%d", i)}, entryText))
	}

	c.mu.Lock()
	total := 0
	for i := range c.consoleEntries {
		total += len(c.consoleEntries[i].Output)
	}
	c.mu.Unlock()
	if total > maxConsoleEntriesDisplayBytes+maxConsoleEntryDisplayBytes {
		t.Fatalf("total display bytes = %d, cap %d ignored", total, maxConsoleEntriesDisplayBytes)
	}
}

// TestConsoleOverflowOrphanSweep: a stale per-process directory from a
// crashed run is removed at startup; a fresh one (possibly another live
// instance) is left alone.
func TestConsoleOverflowOrphanSweep(t *testing.T) {
	stale := filepath.Join(os.TempDir(), consoleOverflowDirPrefix+"test-stale")
	fresh := filepath.Join(os.TempDir(), consoleOverflowDirPrefix+"test-fresh")
	for _, dir := range []string{stale, fresh} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(stale); _ = os.RemoveAll(fresh) })
	old := time.Now().Add(-consoleOverflowOrphanAge - time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	cleanupOrphanedConsoleOverflow(time.Now())

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("stale overflow directory survived the sweep")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Error("fresh overflow directory was swept — a live instance would lose its files")
	}
}
