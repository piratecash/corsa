package desktop

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// console_output.go bounds what the console keeps and renders. A live-network
// fetchContacts dump once reached multiple megabytes; rendered as ONE console
// entry it inflated the frame's vertex data to ~1.2 GB and crashed the Metal
// backend. Two independent caps close that class of failure:
//
//   - a full command output NEVER reaches a widget.Selectable: the display
//     text is capped in bytes and lines BEFORE any layout, with an explicit
//     truncation marker;
//   - the entry list itself is bounded in count and in total display bytes —
//     until now it grew without limit (the existing cap covered only the
//     typed-command history ring).
//
// The full output is not lost: it goes to a temp file with an explicit
// lifecycle — 0600 in a per-process directory, deleted when its entry is
// evicted and when the console window closes, with a startup sweep for
// directories orphaned by crashed processes. Copy reads the file, so the
// clipboard still receives the complete output.

const (
	// maxConsoleEntryDisplayBytes / maxConsoleEntryDisplayLines cap what a
	// single entry hands to its Selectable. Both are generous for reading
	// and far below anything that endangers the renderer.
	maxConsoleEntryDisplayBytes = 64 * 1024
	maxConsoleEntryDisplayLines = 400

	// maxConsoleEntries / maxConsoleEntriesDisplayBytes bound the entry
	// list: count for the layout loop, total bytes for memory.
	maxConsoleEntries             = 100
	maxConsoleEntriesDisplayBytes = 2 * 1024 * 1024

	// consoleOverflowDirPrefix names the per-process overflow directories
	// under os.TempDir(); the pid suffix keeps concurrent instances apart.
	consoleOverflowDirPrefix = "corsa-console-"

	// consoleOverflowOrphanAge is how old an overflow directory must be
	// before the startup sweep treats it as orphaned. Generous on purpose:
	// a false positive deletes another LIVE instance's files, a false
	// negative merely leaves a stale directory one more start.
	consoleOverflowOrphanAge = 48 * time.Hour
)

// consoleOverflowStore owns the temp files carrying full console outputs.
// Its own lock: writes come from command goroutines, removals from the UI
// goroutine and the eviction path.
type consoleOverflowStore struct {
	mu  sync.Mutex
	dir string
	seq int
	// closed marks the window's teardown: a command goroutine finishing
	// AFTER removeAll must not recreate the directory — nothing would ever
	// clean that resurrected file up again (until the orphan sweep).
	closed bool
}

func newConsoleOverflowStore() *consoleOverflowStore {
	return &consoleOverflowStore{
		dir: filepath.Join(os.TempDir(), fmt.Sprintf("%s%d", consoleOverflowDirPrefix, os.Getpid())),
	}
}

// save writes one full output and returns its path. The disk work runs
// UNDER the store mutex on purpose: the closed check and the write must be
// one critical section, or a removeAll between them resurrects the
// directory with a file nothing will ever clean up. The store is a cold
// path (oversized console outputs only), so a serialized write is cheap
// and no domain mutex is anywhere near this lock.
func (s *consoleOverflowStore) save(output string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", fmt.Errorf("console overflow store closed")
	}
	s.seq++
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return "", fmt.Errorf("create console overflow directory: %w", err)
	}
	path := filepath.Join(s.dir, fmt.Sprintf("output-%d.txt", s.seq))
	if err := os.WriteFile(path, []byte(output), 0o600); err != nil {
		return "", fmt.Errorf("write console overflow: %w", err)
	}
	return path, nil
}

// remove deletes one overflow file (entry evicted).
func (s *consoleOverflowStore) remove(path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("path", path).Msg("console_overflow_remove_failed")
	}
}

// removeAll deletes the whole per-process directory (window closed) and
// refuses every later save: a late command result composes its entry with
// the truncation-lost marker instead of resurrecting the directory. The
// delete shares the save critical section — an in-flight save either
// completes before the sweep (its file is removed here) or observes
// closed and writes nothing.
func (s *consoleOverflowStore) removeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if err := os.RemoveAll(s.dir); err != nil {
		log.Warn().Err(err).Str("dir", s.dir).Msg("console_overflow_cleanup_failed")
	}
}

// cleanupOrphanedConsoleOverflow sweeps overflow directories left behind by
// crashed processes: any corsa-console-* directory untouched for longer
// than the orphan age. Called once at app start, best effort.
func cleanupOrphanedConsoleOverflow(now time.Time) {
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), consoleOverflowDirPrefix+"*"))
	if err != nil {
		return
	}
	for _, dir := range matches {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		if now.Sub(info.ModTime()) < consoleOverflowOrphanAge {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Warn().Err(err).Str("dir", dir).Msg("console_overflow_orphan_cleanup_failed")
		}
	}
}

// capConsoleOutput truncates a full output to the display caps. truncated
// reports whether anything was cut.
func capConsoleOutput(output string) (display string, truncated bool) {
	display = output
	if len(display) > maxConsoleEntryDisplayBytes {
		display = display[:maxConsoleEntryDisplayBytes]
		truncated = true
	}
	if lines := strings.Count(display, "\n"); lines >= maxConsoleEntryDisplayLines {
		cut := 0
		for i := 0; i < maxConsoleEntryDisplayLines; i++ {
			next := strings.IndexByte(display[cut:], '\n')
			if next < 0 {
				break
			}
			cut += next + 1
		}
		display = display[:cut]
		truncated = true
	}
	return display, truncated
}

// composeConsoleEntry builds a bounded entry from a FULL command output:
// caps the display text before any widget sees it, spills the complete
// output to the overflow store and appends the truncation marker.
func (c *consoleModal) composeConsoleEntry(entry consoleEntry, fullOutput string) consoleEntry {
	display, truncated := capConsoleOutput(fullOutput)
	entry.FullBytes = len(fullOutput)
	if !truncated {
		entry.Output = fullOutput
		entry.OutputText.SetText(entry.Output)
		return entry
	}

	overflowPath, err := c.overflow.save(fullOutput)
	if err != nil {
		log.Warn().Err(err).Msg("console_overflow_save_failed")
		entry.Output = display + "\n" + c.parent.t("console.output_truncated_lost", formatByteSize(len(fullOutput)))
		entry.OutputText.SetText(entry.Output)
		return entry
	}
	entry.OverflowPath = overflowPath
	entry.Output = display + "\n" + c.parent.t("console.output_truncated", formatByteSize(len(fullOutput)), overflowPath)
	entry.OutputText.SetText(entry.Output)
	return entry
}

// appendConsoleEntry prepends the entry and enforces the list caps,
// deleting the overflow files of everything evicted. Must be called
// WITHOUT c.mu held.
func (c *consoleModal) appendConsoleEntry(entry consoleEntry) {
	var evictedOverflow []string

	c.mu.Lock()
	c.consoleEntries = append([]consoleEntry{entry}, c.consoleEntries...)

	totalBytes := 0
	keep := len(c.consoleEntries)
	for i := range c.consoleEntries {
		totalBytes += len(c.consoleEntries[i].Output)
		if i >= maxConsoleEntries || totalBytes > maxConsoleEntriesDisplayBytes {
			keep = i
			break
		}
	}
	// The newest entry always survives, even if alone it crosses the byte
	// cap: the display text is already bounded per entry, so this cannot
	// reintroduce the unbounded case.
	if keep == 0 {
		keep = 1
	}
	for _, evicted := range c.consoleEntries[keep:] {
		if evicted.OverflowPath != "" {
			evictedOverflow = append(evictedOverflow, evicted.OverflowPath)
		}
	}
	c.consoleEntries = append(c.consoleEntries[:0:0], c.consoleEntries[:keep]...)
	c.mu.Unlock()

	// Disk work strictly after the lock is released.
	for _, path := range evictedOverflow {
		c.overflow.remove(path)
	}
}

// fullConsoleOutput returns the COMPLETE output of an entry for the
// clipboard: the overflow file when one exists, the display text otherwise.
func (c *consoleModal) fullConsoleOutput(entry *consoleEntry) string {
	if entry.OverflowPath == "" {
		return entry.Output
	}
	full, err := os.ReadFile(entry.OverflowPath)
	if err != nil {
		log.Warn().Err(err).Str("path", entry.OverflowPath).Msg("console_overflow_read_failed")
		return entry.Output
	}
	return string(full)
}

// formatByteSize renders a byte count for the truncation marker.
func formatByteSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
