package filetransfer

import (
	"errors"
	"os"
	"path/filepath"
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
//
// It IS a durable row naming a deleted message, and that is deliberate under
// the contract this subsystem shares with the chatlog: the one thing either of
// them may write down is WORK NOT YET DONE. A request the peer has not answered
// and a file that would not unlink are both descriptions of a future action,
// and both disappear the moment it happens. What neither may keep is a record
// of an action already finished — that was the deletion refusal, and it is
// gone. The corollary is flushCleanupsLocked: the moment the files are erased,
// this row has to leave the disk with them.

// pendingDownload is one receiver-side file an erasure still owes.
//
// Path may be EMPTY, and that is the case this type exists for: a message
// deleted while its download was still being verified has no completed path
// yet, because the verifier picks one only when the hash checks out. Name and
// Hash are enough to find that file afterwards — including after a restart, if
// the verifier renamed it and the process died before it could clean up.
type pendingDownload struct {
	Path string
	Name string
	Hash string
}

// pendingCleanup is one attachment still owed an erasure.
//
// Every field is a SET, because a file id can be given more to erase than it
// was first told: the same id can be registered again with a different
// destination, and an erasure that replaced its list instead of adding to it
// would forget a file it had not managed to unlink yet.
type pendingCleanup struct {
	FileID domain.FileID
	// TransmitHashes are the sender-side content-addressed blobs, each erased
	// only when nothing else references it.
	TransmitHashes []string
	// Downloads are the receiver-side completed files.
	Downloads []pendingDownload
	// Partials are the .part files of downloads that never finished.
	Partials []string
	NotedAt  time.Time
	// Attempts and NextAttemptAt space the retries out. Some obstacles do
	// not clear on their own — a read-only volume, a device that is gone —
	// and retrying those every tick forever costs a warn log and a full
	// rewrite of the mappings file each time, for an outcome that cannot
	// change. The intent is never abandoned, only asked less often.
	Attempts      int
	NextAttemptAt time.Time
	// gen counts the times this intent has been added to. It is in memory only
	// — a restart re-reads the work, not its revisions — and it exists so an
	// attempt that finishes late cannot retire an intent that has been given
	// MORE to do since it started.
	gen uint64
}

// clone copies the intent so an attempt can read it with no mutex held.
//
// The slices matter: a shallow copy shares their backing arrays with the entry
// in the map, and noteCleanupLocked appends to those arrays and rewrites their
// elements while the attempt is running. That is not a rare interleaving — it
// is exactly the case the generation counter exists for — so the snapshot has
// to own its memory.
func (c pendingCleanup) clone() pendingCleanup {
	copied := c
	copied.TransmitHashes = append([]string(nil), c.TransmitHashes...)
	copied.Downloads = append([]pendingDownload(nil), c.Downloads...)
	copied.Partials = append([]string(nil), c.Partials...)
	return copied
}

// empty reports whether there is nothing left to erase.
func (c pendingCleanup) empty() bool {
	return len(c.TransmitHashes) == 0 && len(c.Downloads) == 0 && len(c.Partials) == 0
}

// addString appends a value once, so a repeat of the same deletion does not
// grow the work.
func addString(list []string, value string) ([]string, bool) {
	if value == "" {
		return list, false
	}
	for _, existing := range list {
		if existing == value {
			return list, false
		}
	}
	return append(list, value), true
}

// addDownload records one file to erase, merging it with an entry that names
// the same file and appending it otherwise. The bool says whether the WORK
// changed — noteCleanupLocked turns that into a revision.
//
// "The same file" is not "the same name and content": a path names one file,
// while a pathless entry names whatever that name and content resolve to. Two
// different paths of the same name and content — "report.pdf" and
// "report (a1b2c3).pdf" — are two files, and stay two pieces of work.
func addDownload(list []pendingDownload, add pendingDownload) ([]pendingDownload, bool) {
	if add.Path == "" && add.Name == "" {
		return list, false
	}
	for i, existing := range list {
		switch {
		case add.Path != "" && existing.Path == add.Path:
			// The very same file, named again. Whatever this note knows that
			// the entry does not is worth keeping; the work itself is not new.
			changed := false
			if existing.Name == "" && add.Name != "" {
				list[i].Name = add.Name
				changed = true
			}
			if existing.Hash == "" && add.Hash != "" {
				list[i].Hash = add.Hash
				changed = true
			}
			return list, changed

		case add.Path != "" && existing.Path == "" && existing.Name == add.Name && existing.Hash == add.Hash:
			// The entry of a message deleted mid-verification, now named by
			// path. Merged rather than appended — they are one file — and
			// counted as a REVISION.
			//
			// Counting it is the point. An attempt working from the pathless
			// entry may have looked, found nothing, and be about to report the
			// erasure finished; its outcome must not retire an intent that has
			// since learned where the file is. The cost is that the intent
			// lives one more retry — a note about a deleted message on disk for
			// another cycle — and that is the cheaper of the two: the other way
			// leaves a file nothing will ever look for again.
			list[i].Path = add.Path
			return list, true

		case add.Path == "" && existing.Name == add.Name && existing.Hash == add.Hash:
			// The same file named by content, again. Nothing new either way.
			return list, false
		}
	}
	return append(list, add), true
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
	if intent.empty() {
		return
	}
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup, 1)
	}
	existing, ok := m.pendingCleanups[intent.FileID]
	if !ok {
		copied := intent.clone()
		copied.gen++
		m.pendingCleanups[intent.FileID] = &copied
		return
	}
	// A second deletion of the same file id ADDS to what is owed. It may name a
	// path the earlier one never knew, and the earlier one may name a file that
	// has not been unlinked yet — a failed unlink, a deferred one — so anything
	// dropped here is a file nothing will ever look for again.
	changed := false
	for _, hash := range intent.TransmitHashes {
		var added bool
		existing.TransmitHashes, added = addString(existing.TransmitHashes, hash)
		changed = changed || added
	}
	for _, download := range intent.Downloads {
		var added bool
		existing.Downloads, added = addDownload(existing.Downloads, download)
		changed = changed || added
	}
	for _, partial := range intent.Partials {
		var added bool
		existing.Partials, added = addString(existing.Partials, partial)
		changed = changed || added
	}
	if !changed {
		// Nothing new was named, so nothing about this intent is different
		// from what the attempt in flight is working on. Counting it as a
		// revision would make that attempt's "done" look like an answer to a
		// question nobody asked, and the record of a finished erasure would
		// stay on the disk until some later retry happened to clear it.
		return
	}
	existing.gen++
}

// attemptCleanup runs one attempt at an intent, if no other attempt is running
// for it. The bool says whether it ran; when it is false the caller has nothing
// to settle, because whoever holds the attempt will settle it.
func (m *Manager) attemptCleanup(intent pendingCleanup) (pendingCleanup, bool, bool) {
	m.mu.Lock()
	claimed := m.claimCleanupLocked(intent.FileID)
	if claimed {
		if current, ok := m.pendingCleanups[intent.FileID]; ok {
			// The generation of what is owed NOW, which may already include
			// paths this caller's own intent did not name. Cloned, because from
			// here it is read with the mutex released.
			intent = current.clone()
		}
	}
	m.mu.Unlock()
	if !claimed {
		return pendingCleanup{}, false, false
	}
	remaining, done := m.runCleanupIntent(intent)
	return remaining, done, true
}

// claimCleanupLocked reserves the only attempt an intent may have in flight and
// says whether this caller got it. releaseCleanupLocked gives it back. Caller
// MUST hold m.mu for both.
func (m *Manager) claimCleanupLocked(fileID domain.FileID) bool {
	if _, running := m.runningCleanups[fileID]; running {
		return false
	}
	if m.runningCleanups == nil {
		m.runningCleanups = make(map[domain.FileID]struct{}, 1)
	}
	m.runningCleanups[fileID] = struct{}{}
	return true
}

func (m *Manager) releaseCleanupLocked(fileID domain.FileID) {
	delete(m.runningCleanups, fileID)
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
		gen:           intent.gen,
	}

	for _, hash := range intent.TransmitHashes {
		if m.store == nil {
			continue
		}
		if err := m.store.PurgeUnreferenced(hash); err != nil {
			remaining.TransmitHashes, _ = addString(remaining.TransmitHashes, hash)
			log.Warn().Err(err).Str("file_id", logID(string(intent.FileID))).
				Msg("file_transfer: transmit blob of a deleted message is still on disk")
		}
	}
	for _, download := range intent.Downloads {
		if left, keep := m.eraseDownload(intent.FileID, download); keep {
			remaining.Downloads, _ = addDownload(remaining.Downloads, left)
		}
	}
	for _, partial := range intent.Partials {
		if left := m.erasePartial(intent.FileID, partial); left != "" {
			remaining.Partials, _ = addString(remaining.Partials, left)
		}
	}

	return remaining, remaining.empty()
}

// erasePartial removes a .part file and returns what is LEFT to erase — the
// path if the work is still owed, empty if it is finished.
//
// The .part path is derived from the file id, so a download registered for that
// id after the deletion writes to the very same place. While such a mapping
// exists the file is not erased and the work is KEPT: erasing would take the
// bytes of a transfer in progress, and forgetting would leave the old partial
// on disk with nothing that would ever look for it again. The question is asked
// again on the next attempt, and it settles itself — the new download either
// renames the partial away when it completes, or goes and leaves the path to
// this erasure.
func (m *Manager) erasePartial(fileID domain.FileID, partial string) string {
	// This file id's .part stripe FIRST — the lock every writer, verifier and
	// canceller of that file takes — and then the namespace, so the two
	// orderings of the download directory are the one ordering.
	releasePartial := m.lockPartial(fileID)
	defer releasePartial()

	m.downloadNamespaceMu.Lock()
	defer m.downloadNamespaceMu.Unlock()

	// Gone already? Then the work is finished, whoever removed it — the new
	// download renamed it away when it completed, or an earlier attempt of this
	// erasure got it. Asked FIRST, so a mapping that owns nothing cannot keep
	// this record alive: an erasure that never settles is a note about a
	// deleted message that stays on the disk for good.
	if _, err := os.Lstat(partial); err != nil {
		if !os.IsNotExist(err) {
			log.Warn().Err(err).Str("file_id", logID(string(fileID))).
				Msg("file_transfer: cannot tell whether the partial download is still there")
			return partial
		}
		// The unlink still has to be durable, whoever made it.
		if err := syncDirectory(filepath.Dir(partial)); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Warn().Err(err).Str("file_id", logID(string(fileID))).
				Msg("file_transfer: the removal of the partial download is not on disk yet")
			return partial
		}
		return ""
	}

	m.mu.Lock()
	_, live := m.receiverMaps[fileID]
	m.mu.Unlock()
	if live {
		deletionLog().Debug().Str("file_id", logID(string(fileID))).
			Msg("file_transfer: a transfer is registered for this id again; its partial is not this erasure's to take")
		return partial
	}
	if err := m.eraseInDownloadDir(partial); err != nil {
		log.Warn().Err(err).Str("file_id", logID(string(fileID))).
			Msg("file_transfer: partial download of a deleted message is still on disk")
		return partial
	}
	return ""
}

// eraseDownload erases one completed download and says what is LEFT of it.
//
// A download with no path yet is resolved first: the message was deleted while
// its file was still being verified, so the name and the content are all there
// was to record. Resolving it here — at the attempt, after a restart if need be
// — is what finds the file a verifier renamed into place after the mapping that
// knew about it was already gone.
func (m *Manager) eraseDownload(fileID domain.FileID, download pendingDownload) (pendingDownload, bool) {
	path := download.Path
	if path == "" {
		path = resolveExistingDownload(m.downloadDir, download.Name, download.Hash)
		if path == "" {
			// Nothing was ever written under that name: the verifier gave up
			// before it renamed, and there is no file to erase.
			return pendingDownload{}, false
		}
	}

	// Held across BOTH the question and the unlink. A download finishing in
	// between would otherwise rename its file onto the path this erasure has
	// just decided is unowned, and lose it to the os.Remove below.
	m.downloadNamespaceMu.Lock()
	defer m.downloadNamespaceMu.Unlock()

	m.mu.Lock()
	verdict := m.pathOwnershipLocked(path, download.Hash)
	m.mu.Unlock()

	switch verdict {
	case erasureOwnedByAnother:
		// Another message still shows this file, and takes it along when it
		// goes. Nothing left for this erasure to do.
		deletionLog().Debug().Str("file_id", logID(string(fileID))).
			Msg("file_transfer: the downloaded file belongs to another message as well; only the mapping went")
		return pendingDownload{}, false
	case erasureMayBeClaimed:
		// A download in flight is going to land on this path. Erasing now takes
		// the file out from under it; forgetting the erasure leaves the file
		// with nothing that could ever remove it, because a download that is
		// cancelled never claims the path either. So the work stays owed and
		// the question is asked again.
		download.Path = path
		return download, true
	default:
		if err := m.eraseInDownloadDir(path); err != nil {
			download.Path = path
			log.Warn().Err(err).Str("file_id", logID(string(fileID))).
				Msg("file_transfer: download of a deleted message is still on disk")
			return download, true
		}
		return pendingDownload{}, false
	}
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
	// The unlink is not on disk until the DIRECTORY is. Without this the
	// erasure can be reported as finished, the intent that owed it dropped and
	// written down durably, and a power cut then bring the file back with
	// nothing left that knows it should be gone — an attachment with no mapping
	// and no second chance at being erased.
	//
	// Flushed on ENOENT TOO, which is the whole reason for a retry. "Already
	// gone" is exactly what the second attempt sees after a first one that
	// unlinked the file and then failed to flush — answering success there
	// would retire the intent over the same unflushed directory the retry
	// exists to finish.
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// The directory itself is not there, so neither is the entry this
			// erasure is about. Nothing to flush and nothing left to erase.
			return nil
		}
		return err
	}
	return nil
}

// settleCleanupLocked applies the outcome of one attempt. It does NOT
// write: the outcome only ever removes an intent or moves its next
// attempt, and the mappings file is written whole, so one save per pass
// covers any number of intents. Caller MUST hold m.mu and MUST persist
// through flushCleanupsLocked before returning to its own idle state.
func (m *Manager) settleCleanupLocked(remaining pendingCleanup, done bool) {
	// The claim goes back here, at the one place every attempt ends.
	m.releaseCleanupLocked(remaining.FileID)
	if current, ok := m.pendingCleanups[remaining.FileID]; ok && current.gen != remaining.gen {
		// The intent was added to while this attempt was running — another
		// deletion of the same file id named paths this attempt never saw.
		// Its outcome describes work that is no longer the whole of what is
		// owed, so it neither retires the intent nor overwrites it; the sweep
		// takes the current one from the top.
		current.NextAttemptAt = remaining.NextAttemptAt
		m.cleanupsDirty = true
		return
	}
	if done {
		delete(m.pendingCleanups, remaining.FileID)
	} else {
		copied := remaining
		m.pendingCleanups[remaining.FileID] = &copied
	}
	m.cleanupsDirty = true
}

// flushCleanupsLocked writes the intents out when memory is ahead of the file.
// Caller MUST hold m.mu.
//
// Every path that finishes an erasure has to end here, and that is the whole
// point of the function existing: a FINISHED intent is a note naming the
// message id, the paths of the files that hung off it and the moment they were
// destroyed. Leaving it in the file until the maintenance tick — or, if the
// process is closed first, for ever — means the deletion of an attachment
// leaves behind a precise description of what was deleted and when. The mapping
// is already gone from memory at that point, so the note is the only remaining
// record, and it describes exactly what the user asked to be rid of.
func (m *Manager) flushCleanupsLocked() {
	if !m.cleanupsDirty {
		return
	}
	if err := m.saveMappingsLockedErr(); err != nil {
		// Still dirty, so the tick retries. Worth a line: what stayed behind
		// is a record of a deletion the user was told had finished.
		log.Warn().Err(err).
			Msg("file_transfer: the record of a finished erasure is still on disk; retrying on the maintenance tick")
		return
	}
	m.cleanupsDirty = false
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
		// Skipped rather than queued: an attempt is already running for this
		// intent, and a second one would only race it to write the outcome.
		if !m.claimCleanupLocked(intent.FileID) {
			continue
		}
		due = append(due, intent.clone())
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
	m.flushCleanupsLocked()
	m.mu.Unlock()
}
