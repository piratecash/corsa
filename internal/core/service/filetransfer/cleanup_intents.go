package filetransfer

import (
	"errors"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// cleanup_intents.go makes the erasure of a deleted message's attachment
// survive a failure.
//
// Dropping the mapping and unlinking the files are two different things,
// and only the first one was durable: the mapping went away with the row,
// and if the unlink then failed — a locked file, a full or read-only
// volume, a device that vanished — nothing was left that knew a file was
// supposed to be gone. The bytes of a message the user destroyed stayed on
// disk, and no restart or retry would ever look for them again.
//
// The intent is written in the SAME persisted state that drops the
// mapping, so the two commit together. It is cleared only when the files
// it names are actually gone, and retried by the maintenance tick until
// then — including after a restart, which is the case a purely in-memory
// retry cannot cover.

// pendingCleanup is one attachment still owed an erasure.
type pendingCleanup struct {
	FileID domain.FileID
	// TransmitHash is the sender-side content-addressed blob, erased only
	// when nothing else references it.
	TransmitHash string
	// CompletedPath and PartialPath are the receiver-side downloads.
	CompletedPath string
	PartialPath   string
	NotedAt       time.Time
	// Attempts and NextAttemptAt space the retries out. Some obstacles do
	// not clear on their own — a read-only volume, a device that is gone —
	// and retrying those every tick forever costs a warn log and a full
	// rewrite of the mappings file each time, for an outcome that cannot
	// change. The intent is never abandoned, only asked less often.
	Attempts      int
	NextAttemptAt time.Time
}

// cleanupRetryDelay spaces attempts out from one tick to an hour. The cap
// is what a transient obstacle (a locked file, a full disk that gets
// cleaned up) still recovers from promptly, while a permanent one costs
// 24 attempts a day instead of 8640.
func cleanupRetryDelay(attempts int) time.Duration {
	delay := cleanupRetryBase
	for range attempts {
		delay *= 2
		if delay >= cleanupRetryCap {
			return cleanupRetryCap
		}
	}
	return delay
}

const (
	cleanupRetryBase = 10 * time.Second
	cleanupRetryCap  = time.Hour
)

// noteCleanupLocked records what still has to be erased for a file id.
// Caller MUST hold m.mu and MUST persist afterwards, in the same write
// that drops the mapping: an intent that lands without the mapping being
// dropped is harmless (the retry finds nothing), while a mapping dropped
// without the intent is the leak this exists to prevent.
func (m *Manager) noteCleanupLocked(intent pendingCleanup) {
	if intent.TransmitHash == "" && intent.CompletedPath == "" && intent.PartialPath == "" {
		return
	}
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup, 1)
	}
	existing, ok := m.pendingCleanups[intent.FileID]
	if !ok {
		copied := intent
		m.pendingCleanups[intent.FileID] = &copied
		return
	}
	// A second deletion of the same file id adds to what is owed rather
	// than replacing it: the earlier intent may name a path this one does
	// not know about any more.
	if intent.TransmitHash != "" {
		existing.TransmitHash = intent.TransmitHash
	}
	if intent.CompletedPath != "" {
		existing.CompletedPath = intent.CompletedPath
	}
	if intent.PartialPath != "" {
		existing.PartialPath = intent.PartialPath
	}
}

// runCleanupIntent erases what the intent names and returns what is LEFT,
// or nothing when the attachment is finally gone. It performs file I/O, so
// it MUST run with no mutex held.
//
// Each half is cleared independently: a receiver download that unlinks
// while the transmit blob refuses must not be attempted again, and the
// blob must not be forgotten because the download succeeded.
func (m *Manager) runCleanupIntent(intent pendingCleanup) (pendingCleanup, bool) {
	remaining := pendingCleanup{
		FileID:        intent.FileID,
		NotedAt:       intent.NotedAt,
		Attempts:      intent.Attempts + 1,
		NextAttemptAt: time.Now().Add(cleanupRetryDelay(intent.Attempts)),
	}

	if intent.TransmitHash != "" && m.store != nil {
		if err := m.store.PurgeUnreferenced(intent.TransmitHash); err != nil {
			remaining.TransmitHash = intent.TransmitHash
			log.Warn().Err(err).Str("file_id", string(intent.FileID)).
				Msg("file_transfer: transmit blob of a deleted message is still on disk")
		}
	}
	if intent.CompletedPath != "" {
		if err := m.eraseInDownloadDir(intent.CompletedPath); err != nil {
			remaining.CompletedPath = intent.CompletedPath
			log.Warn().Err(err).Str("file_id", string(intent.FileID)).
				Msg("file_transfer: download of a deleted message is still on disk")
		}
	}
	if intent.PartialPath != "" {
		if err := m.eraseInDownloadDir(intent.PartialPath); err != nil {
			remaining.PartialPath = intent.PartialPath
			log.Warn().Err(err).Str("file_id", string(intent.FileID)).
				Msg("file_transfer: partial download of a deleted message is still on disk")
		}
	}

	done := remaining.TransmitHash == "" && remaining.CompletedPath == "" && remaining.PartialPath == ""
	return remaining, done
}

// eraseInDownloadDir removes a file under the download directory and says
// whether anything is left. A path outside the directory is refused and
// reported as done — it is corrupt persisted state, not a file this node
// may delete, and retrying it forever would keep an intent alive that no
// attempt can ever satisfy.
func (m *Manager) eraseInDownloadDir(path string) error {
	if path == "" {
		return nil
	}
	if err := ensureWithinDir(m.downloadDir, path); err != nil {
		log.Warn().Err(err).Str("path", path).
			Msg("file_transfer: cleanup skipped — path escapes download dir")
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// settleCleanupLocked applies the outcome of one attempt. It does NOT
// write: the outcome only ever removes an intent or moves its next
// attempt, and the mappings file is written whole, so one save per pass
// covers any number of intents. Caller MUST hold m.mu and MUST persist
// before returning to the caller's own idle state.
func (m *Manager) settleCleanupLocked(remaining pendingCleanup, done bool) {
	if done {
		delete(m.pendingCleanups, remaining.FileID)
	} else {
		copied := remaining
		m.pendingCleanups[remaining.FileID] = &copied
	}
	m.cleanupsDirty = true
}

// tickPendingCleanups retries every erasure that has come due. Runs on the
// maintenance tick, which is also the first thing that happens after a
// restart re-reads the intents — the case an in-memory retry cannot cover.
//
// The persist comes FIRST when the intents on disk are stale, and the pass
// gives up for this tick if it fails: unlinking a file the disk does not
// yet know is owed an erasure is exactly the ordering this subsystem
// exists to forbid.
func (m *Manager) tickPendingCleanups() {
	now := time.Now()

	m.mu.Lock()
	if m.cleanupsDirty {
		// The flag is cleared ONLY by a write that succeeded. Clearing it
		// on a logged failure was a way to lose the last record of an
		// erasure: nothing would ever try that write again, and a restart
		// would restore the mappings — and the files — with no intent
		// left to remove them.
		if err := m.saveMappingsLockedErr(); err != nil {
			m.mu.Unlock()
			log.Warn().Err(err).
				Msg("file_transfer: unfinished erasures are not on disk yet; not touching any file this tick")
			return
		}
		m.cleanupsDirty = false
	}
	if len(m.pendingCleanups) == 0 {
		m.mu.Unlock()
		return
	}
	due := make([]pendingCleanup, 0, len(m.pendingCleanups))
	for _, intent := range m.pendingCleanups {
		if intent.NextAttemptAt.After(now) {
			continue
		}
		due = append(due, *intent)
	}
	m.mu.Unlock()

	if len(due) == 0 {
		return
	}

	settled := make([]pendingCleanup, 0, len(due))
	outcomes := make([]bool, 0, len(due))
	for _, intent := range due {
		remaining, done := m.runCleanupIntent(intent)
		settled = append(settled, remaining)
		outcomes = append(outcomes, done)
	}

	// ONE write for the whole pass. The file holds every mapping and every
	// intent, so a save per intent rewrites the same file N times — and a
	// wedged volume would make that N rewrites every tick, forever.
	m.mu.Lock()
	for i, remaining := range settled {
		m.settleCleanupLocked(remaining, outcomes[i])
	}
	if err := m.saveMappingsLockedErr(); err != nil {
		// Still dirty, so the next tick writes before it touches a file.
		log.Warn().Err(err).
			Msg("file_transfer: could not record the outcome of the erasures; retrying on the next tick")
	} else {
		m.cleanupsDirty = false
	}
	m.mu.Unlock()
}
