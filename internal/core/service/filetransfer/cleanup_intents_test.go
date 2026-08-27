package filetransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// newCleanupIntentManager builds a manager over real directories and a real
// mappings file, because the point of the intent is that it survives a
// process — which cannot be tested against an in-memory stub.
func newCleanupIntentManager(t *testing.T) (*Manager, string, string) {
	t.Helper()
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(transmitDir, 0o700); err != nil {
		t.Fatalf("mkdir transmit: %v", err)
	}
	if err := os.MkdirAll(downloadDir, 0o700); err != nil {
		t.Fatalf("mkdir downloads: %v", err)
	}
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  downloadDir,
		mappingsPath: filepath.Join(dir, "transfers.json"),
		stopCh:       make(chan struct{}),
	}
	return m, transmitDir, downloadDir
}

// TestAnErasureIsNotFinishedUntilTheDirectoryIsOnDisk pins the ordering between
// the unlink and the record that owes it.
//
// os.Remove returning nil means the entry is gone from the page cache, not from
// the disk. The intent is what remembers that a deleted message's bytes are
// still there, and it is dropped and written down durably the moment the
// erasure reports success — so reporting success before the directory is
// flushed is the one ordering that can leave a file resurrected by a power cut
// with nothing left that knows it should be gone.
func TestAnErasureIsNotFinishedUntilTheDirectoryIsOnDisk(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("44444444-4444-4444-8444-444444444444")
	completed := filepath.Join(downloadDir, "receipt.pdf")
	if err := os.WriteFile(completed, []byte("bytes of a deleted message"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "receipt.pdf",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: completed,
		CreatedAt:     time.Now(),
	}

	var flushed []string
	restore := syncDirectory
	syncDirectory = func(dir string) error {
		flushed = append(flushed, dir)
		return errors.New("the disk did not take it")
	}
	t.Cleanup(func() { syncDirectory = restore })

	m.CleanupTransferByMessageID(fileID)

	if len(flushed) == 0 {
		t.Fatal("the directory was never flushed after the unlink")
	}
	m.mu.Lock()
	intent, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed {
		t.Fatal("the erasure was reported as finished although the directory entry may still be on disk")
	}
	if len(intent.Downloads) != 1 || intent.Downloads[0].Path != completed {
		t.Errorf("the kept intent names %+v, want %q", intent.Downloads, completed)
	}
}

// TestRemovingAContactKeepsTheFileAnotherChatShows is the same rule on the
// other entry point.
//
// Deleting a CONTACT erases every attachment of that conversation, and it walks
// its own list of mappings — so a file shared with a third party has to be
// checked there too, not only on the per-message path.
func TestRemovingAContactKeepsTheFileAnotherChatShows(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		fromAlice = domain.FileID("88888888-8888-4888-8888-888888888888")
		fromCarol = domain.FileID("99999999-9999-4999-8999-999999999999")
	)
	shared := filepath.Join(downloadDir, "agenda.pdf")
	if err := os.WriteFile(shared, []byte("the same bytes in both chats"), 0o600); err != nil {
		t.Fatalf("prepare the shared download: %v", err)
	}
	alice, carol := domaintest.ID("alice"), domaintest.ID("carol")
	for id, sender := range map[domain.FileID]domain.PeerIdentity{fromAlice: alice, fromCarol: carol} {
		m.receiverMaps[id] = &receiverFileMapping{
			FileID:        id,
			FileName:      "agenda.pdf",
			Sender:        sender,
			State:         receiverCompleted,
			CompletedPath: shared,
			CreatedAt:     time.Now(),
		}
	}

	m.CleanupPeerTransfers(alice)

	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("removing a contact erased the file another chat shows: %v", err)
	}
	m.mu.Lock()
	_, aliceGone := m.receiverMaps[fromAlice]
	_, carolKept := m.receiverMaps[fromCarol]
	m.mu.Unlock()
	if aliceGone {
		t.Error("the removed contact kept its mapping")
	}
	if !carolKept {
		t.Error("the other conversation lost its mapping")
	}

	// And the last holder still takes the file with it.
	m.CleanupPeerTransfers(carol)
	if _, err := os.Stat(shared); !os.IsNotExist(err) {
		t.Errorf("the last conversation holding the file left it on disk: err=%v", err)
	}
}

// TestARetryAfterAFailedFlushStillOwesTheErasure pins the second attempt, which
// is where the ordering is easy to lose.
//
// The first attempt unlinks the file and fails to flush, so the erasure is
// still owed. The retry then finds the file ALREADY GONE — the unlink happened,
// it just is not durable — and "already gone" must not be read as success while
// the directory entry has still never been flushed.
func TestARetryAfterAFailedFlushStillOwesTheErasure(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("55555555-5555-4555-8555-555555555555")
	completed := filepath.Join(downloadDir, "invoice.pdf")
	if err := os.WriteFile(completed, []byte("bytes of a deleted message"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "invoice.pdf",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: completed,
		CreatedAt:     time.Now(),
	}

	var flushes int
	restore := syncDirectory
	failing := true
	syncDirectory = func(dir string) error {
		flushes++
		if failing {
			return errors.New("the disk did not take it")
		}
		return restore(dir)
	}
	t.Cleanup(func() { syncDirectory = restore })

	m.CleanupTransferByMessageID(fileID)
	if _, err := os.Stat(completed); !os.IsNotExist(err) {
		t.Fatalf("the fixture did not unlink the file: err=%v", err)
	}
	m.mu.Lock()
	intent, owed := m.pendingCleanups[fileID]
	if owed {
		// Due now, so the tick below picks it up.
		intent.NextAttemptAt = time.Now().Add(-time.Minute)
		m.pendingCleanups[fileID] = intent
	}
	m.mu.Unlock()
	if !owed {
		t.Fatal("the first attempt reported success although the flush failed")
	}

	before := flushes
	m.tickPendingCleanups()
	if flushes == before {
		t.Error("the retry did not flush the directory: it took ENOENT for a finished erasure")
	}
	m.mu.Lock()
	_, stillOwed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !stillOwed {
		t.Fatal("the retry reported success over a directory entry that was never flushed")
	}

	// And once the flush goes through, the erasure is finished.
	failing = false
	m.mu.Lock()
	intent = m.pendingCleanups[fileID]
	intent.NextAttemptAt = time.Now().Add(-time.Minute)
	m.pendingCleanups[fileID] = intent
	m.mu.Unlock()
	m.tickPendingCleanups()

	m.mu.Lock()
	_, owedAfterFlush := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if owedAfterFlush {
		t.Error("a flushed erasure is still owed")
	}
}

// TestADownloadInFlightKeepsTheFileItIsAboutToClaim pins the race the static
// check cannot see.
//
// A download that has all its chunks has no completed path yet, and it finishes
// by ASKING for one: for a name and content already on disk, the answer is that
// same file, and it renames onto it. The mutex is not held between the check
// and the unlink, so a mapping that is going to own the path has to count as
// owning it now — otherwise clearing one chat takes the file another chat is in
// the middle of receiving.
func TestADownloadInFlightKeepsTheFileItIsAboutToClaim(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		fromAlice = domain.FileID("66666666-6666-4666-8666-666666666666")
		fromCarol = domain.FileID("77777777-7777-4777-8777-777777777777")
	)
	const hash = "8f9e6c1a2b3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7"
	shared := filepath.Join(downloadDir, "contract.pdf")
	if err := os.WriteFile(shared, []byte("the same bytes in both chats"), 0o600); err != nil {
		t.Fatalf("prepare the shared download: %v", err)
	}
	m.receiverMaps[fromAlice] = &receiverFileMapping{
		FileID:        fromAlice,
		FileName:      "contract.pdf",
		FileHash:      hash,
		Sender:        domaintest.ID("alice"),
		State:         receiverCompleted,
		CompletedPath: shared,
		CreatedAt:     time.Now(),
	}
	// Carol's copy: every chunk in, hash being verified, no path yet.
	m.receiverMaps[fromCarol] = &receiverFileMapping{
		FileID:    fromCarol,
		FileName:  "contract.pdf",
		FileHash:  hash,
		Sender:    domaintest.ID("carol"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.CleanupTransferByMessageID(fromAlice)

	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("the file Carol's download is about to claim was erased: %v", err)
	}
	m.mu.Lock()
	_, carolKept := m.receiverMaps[fromCarol]
	intent, owed := m.pendingCleanups[fromAlice]
	m.mu.Unlock()
	if !carolKept {
		t.Error("the other conversation lost its mapping")
	}
	// The erasure is OWED, not forgotten. A download that is about to claim the
	// path may also never claim it, and dropping the work here is what would
	// leave the file with nothing able to remove it.
	if !owed || len(intent.Downloads) != 1 || intent.Downloads[0].Path != shared {
		t.Fatalf("the erasure was written off while the file was still there: owed=%v intent=%+v", owed, intent)
	}

	// Carol's download gives up. Nothing is going to claim the path any more,
	// so the retry finishes what the first attempt could not.
	m.mu.Lock()
	delete(m.receiverMaps, fromCarol)
	pending := m.pendingCleanups[fromAlice]
	pending.NextAttemptAt = time.Now().Add(-time.Minute)
	m.pendingCleanups[fromAlice] = pending
	m.mu.Unlock()
	m.tickPendingCleanups()

	if _, err := os.Stat(shared); !os.IsNotExist(err) {
		t.Errorf("the file outlived every message that held it: err=%v", err)
	}
	m.mu.Lock()
	_, stillOwed := m.pendingCleanups[fromAlice]
	m.mu.Unlock()
	if stillOwed {
		t.Error("a finished erasure is still owed")
	}
}

// TestTheSuffixedCopyIsRecognisedAsTheSameFile pins the second name a download
// can be stored under.
//
// When the plain name is already taken by different content, the store writes
// the file as "report (a1b2c3).pdf" — but the MAPPING still carries the name the
// sender sent, "report.pdf". Comparing only that one misses the case where two
// messages are most likely to share a file: the second copy of a file whose
// plain name is occupied.
func TestTheSuffixedCopyIsRecognisedAsTheSameFile(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		deleted  = domain.FileID("ccccccc3-3333-4333-8333-333333333333")
		inFlight = domain.FileID("ddddddd4-4444-4444-8444-444444444444")
		hash     = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	)
	// What completedDownloadPath produces when "report.pdf" is taken.
	suffixed := filepath.Join(downloadDir, "report ("+hash[:6]+").pdf")
	if err := os.WriteFile(suffixed, []byte("the shared bytes"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[deleted] = &pendingCleanup{
		FileID:        deleted,
		Downloads:     []pendingDownload{{Path: suffixed, Hash: hash}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// The other chat is receiving the same file. Its mapping carries the plain
	// name, and it will land on the suffixed path.
	m.receiverMaps[inFlight] = &receiverFileMapping{
		FileID:    inFlight,
		FileName:  "report.pdf",
		FileHash:  hash,
		Sender:    domaintest.ID("carol"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(suffixed); err != nil {
		t.Fatalf("the file the other download is about to claim was erased: %v", err)
	}
	m.mu.Lock()
	_, owed := m.pendingCleanups[deleted]
	m.mu.Unlock()
	if !owed {
		t.Error("the erasure was written off although the file is still there")
	}
}

// TestAnErasureAndAFinishingDownloadDoNotOverlap pins the exclusion the two
// operations on a download path need.
//
// Deciding that nobody owns a file and unlinking it cannot be one step under
// m.mu — file I/O does not run under that mutex here — so both the erasure and
// the rename that finishes a download take the namespace lock instead. If the
// erasure did not hold it across the whole check-and-unlink, a download landing
// in between would be erased a moment after it arrived.
func TestAnErasureAndAFinishingDownloadDoNotOverlap(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("eeeeeee5-5555-4555-8555-555555555555")
	erased := filepath.Join(downloadDir, "notes.pdf")
	if err := os.WriteFile(erased, []byte("bytes of a deleted message"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Downloads:     []pendingDownload{{Path: erased}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	m.mu.Unlock()

	// A download holding the namespace lock — which is what the finalize path
	// does from choosing its destination until the file is renamed onto it.
	m.downloadNamespaceMu.Lock()

	swept := make(chan struct{})
	go func() {
		m.tickPendingCleanups()
		close(swept)
	}()

	select {
	case <-swept:
		t.Fatal("the erasure ran while a download held the path namespace")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(erased); err != nil {
		t.Fatalf("the file was unlinked while the namespace was held: %v", err)
	}

	m.downloadNamespaceMu.Unlock()
	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatal("the erasure never ran after the namespace was released")
	}
	if _, err := os.Stat(erased); !os.IsNotExist(err) {
		t.Errorf("the erasure did not remove the file after it got the namespace: err=%v", err)
	}
}

// TestADownloadOfDifferentContentDoesNotHoldTheErasure pins that the file is
// identified by CONTENT, not by name alone.
//
// A download of a different file that happens to have the same name is stored
// beside this one, under a name of its own — it never takes this path. Treating
// it as a claimant held the erasure for as long as that unrelated download
// lasted, which for a stalled transfer is indefinitely.
func TestADownloadOfDifferentContentDoesNotHoldTheErasure(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		deleted   = domain.FileID("ffffffff-6666-4666-8666-666666666666")
		unrelated = domain.FileID("00000000-7777-4777-8777-777777777777")
		hashA     = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
		hashC     = "0f9e8d7c6b5a493827160f9e8d7c6b5a493827160f9e8d7c6b5a49382716aabb"
	)
	erased := filepath.Join(downloadDir, "report.pdf")
	if err := os.WriteFile(erased, []byte("the deleted message's attachment"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[deleted] = &pendingCleanup{
		FileID:        deleted,
		Downloads:     []pendingDownload{{Path: erased, Hash: hashA}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// Another peer is sending a DIFFERENT file that happens to be called the
	// same thing.
	m.receiverMaps[unrelated] = &receiverFileMapping{
		FileID:    unrelated,
		FileName:  "report.pdf",
		FileHash:  hashC,
		Sender:    domaintest.ID("carol"),
		State:     receiverDownloading,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(erased); !os.IsNotExist(err) {
		t.Errorf("an unrelated download of the same name kept the deleted attachment on disk: err=%v", err)
	}
	m.mu.Lock()
	_, owed := m.pendingCleanups[deleted]
	_, transferKept := m.receiverMaps[unrelated]
	m.mu.Unlock()
	if owed {
		t.Error("the erasure is still owed although nothing claims the file")
	}
	if !transferKept {
		t.Error("the unrelated transfer was disturbed by the erasure")
	}
}

// TestTheStaleVerifierCleanupTakesTheNamespaceToo pins the other unlink in this
// package.
//
// A verifier whose transfer was cancelled while it was hashing has already
// renamed its file onto the download path, and removes it again — but only if
// the inode there is still its own. That check and the unlink are two syscalls,
// so without the namespace lock a newer attempt renaming in between has its
// file deleted by an identity test that passed a moment earlier.
func TestTheStaleVerifierCleanupTakesTheNamespaceToo(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	path := filepath.Join(downloadDir, "cancelled.pdf")
	if err := os.WriteFile(path, []byte("what the cancelled verifier renamed"), 0o600); err != nil {
		t.Fatalf("prepare the file: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	m.downloadNamespaceMu.Lock()
	removed := make(chan struct{})
	go func() {
		m.removeOwnedFileInDownloadDir(path, info, "test cleanup")
		close(removed)
	}()

	select {
	case <-removed:
		t.Fatal("the stale-verifier cleanup ran while the download namespace was held")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the file was unlinked while the namespace was held: %v", err)
	}

	m.downloadNamespaceMu.Unlock()
	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("the cleanup never ran after the namespace was released")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the cleanup did not remove its own file: err=%v", err)
	}
}

// TestOnlyOneAttemptRunsPerErasure pins that two attempts at the same erasure
// cannot write their outcomes back in whatever order they finish.
//
// A deletion starts an attempt immediately; the maintenance tick comes round
// while it is still going. Left alone, a slow FAILED attempt could restore an
// intent that a fast successful one had just retired — putting the record of a
// finished deletion back on disk.
func TestOnlyOneAttemptRunsPerErasure(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("11111112-1111-4111-8111-111111111112")
	path := filepath.Join(downloadDir, "once.pdf")
	if err := os.WriteFile(path, []byte("bytes of a deleted message"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Downloads:     []pendingDownload{{Path: path}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// An attempt is already in flight for it.
	claimed := m.claimCleanupLocked(fileID)
	m.mu.Unlock()
	if !claimed {
		t.Fatal("the fixture could not claim the attempt")
	}

	m.tickPendingCleanups()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the tick ran a second attempt at an erasure already in flight: %v", err)
	}
	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.releaseCleanupLocked(fileID)
	m.mu.Unlock()
	if !owed {
		t.Fatal("the tick settled an intent whose attempt it never ran")
	}

	// Once the attempt in flight is done, the tick works normally again.
	m.tickPendingCleanups()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the erasure never ran after the claim was released: err=%v", err)
	}
}

// TestALateOutcomeDoesNotRetireWorkAddedSinceItStarted is the other half of the
// same protection.
//
// While an attempt runs, the same file id can be given MORE to erase — another
// deletion naming a path the running attempt never saw. Its "done" describes
// only what it took, so it must not retire the intent that now owes more.
func TestALateOutcomeDoesNotRetireWorkAddedSinceItStarted(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("22222223-2222-4222-8222-222222222223")
	first := filepath.Join(downloadDir, "first.pdf")
	second := filepath.Join(downloadDir, "second.pdf")
	for _, p := range []string{first, second} {
		if err := os.WriteFile(p, []byte("bytes"), 0o600); err != nil {
			t.Fatalf("prepare %s: %v", p, err)
		}
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.noteCleanupLocked(pendingCleanup{FileID: fileID, Downloads: []pendingDownload{{Path: first}}, NotedAt: time.Now()})
	running := *m.pendingCleanups[fileID]
	// The attempt starts; meanwhile a second deletion adds another path.
	m.noteCleanupLocked(pendingCleanup{FileID: fileID, Downloads: []pendingDownload{{Path: second}}, NotedAt: time.Now()})
	// The first attempt reports that everything it knew about is gone.
	m.settleCleanupLocked(pendingCleanup{FileID: fileID, gen: running.gen, NextAttemptAt: time.Now()}, true)
	intent, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()

	if !owed {
		t.Fatal("a late outcome retired an intent that had been given more to erase")
	}
	if len(intent.Downloads) != 2 {
		t.Errorf("the surviving intent names %+v, want both the path it was erasing and the one added since", intent.Downloads)
	}
}

// TestAReNotedErasureCarriesItsOwnHash pins that the path and the content it is
// expected to hold move together.
//
// The hash is how the erasure tells a download that will take this very file
// from one that merely shares its name, so a new path carried over with the old
// hash makes the ownership check answer about a file that is not there.
func TestAReNotedErasureCarriesItsOwnHash(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		fileID = domain.FileID("33333334-3333-4333-8333-333333333334")
		hashA  = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
		hashB  = "0f9e8d7c6b5a493827160f9e8d7c6b5a493827160f9e8d7c6b5a49382716aabb"
	)
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.noteCleanupLocked(pendingCleanup{
		FileID:    fileID,
		Downloads: []pendingDownload{{Path: filepath.Join(downloadDir, "old.pdf"), Hash: hashA}},
		NotedAt:   time.Now(),
	})
	m.noteCleanupLocked(pendingCleanup{
		FileID:    fileID,
		Downloads: []pendingDownload{{Path: filepath.Join(downloadDir, "new.pdf"), Hash: hashB}},
		NotedAt:   time.Now(),
	})
	intent := *m.pendingCleanups[fileID]
	m.mu.Unlock()

	if len(intent.Downloads) != 2 {
		t.Fatalf("the intent names %+v, want both paths kept", intent.Downloads)
	}
	for _, download := range intent.Downloads {
		want := hashA
		if download.Path == filepath.Join(downloadDir, "new.pdf") {
			want = hashB
		}
		if download.Hash != want {
			t.Errorf("%s carries the hash %q, want %q", filepath.Base(download.Path), download.Hash, want)
		}
	}
}

// TestAFileTheVerifierWroteIsErasedWithoutAPathToNameIt pins the file nobody
// had a path for yet.
//
// Deleting a message whose download is still being VERIFIED leaves no completed
// path to record: the verifier picks one only when the hash checks out. But the
// file may already be there — the rename happens before the verifier looks at
// whether its transfer still exists — and after a crash nothing would name it
// at all. The erasure therefore records the NAME and the CONTENT, and resolves
// the path when it runs, however much later that is.
func TestAFileTheVerifierWroteIsErasedWithoutAPathToNameIt(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("44444445-4444-4444-8444-444444444445")
	content := []byte("what the verifier was checking")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])

	// The verifier has renamed its file into place; the deletion arrives before
	// it gets to check whether its transfer is still there.
	written := filepath.Join(downloadDir, "invoice.pdf")
	if err := os.WriteFile(written, content, 0o600); err != nil {
		t.Fatalf("simulate the verifier's rename: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileName:  "invoice.pdf",
		FileHash:  hash,
		Sender:    domaintest.ID("bob"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)

	if _, err := os.Stat(written); !os.IsNotExist(err) {
		t.Errorf("the file the verifier wrote is still on disk: err=%v", err)
	}
	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if owed {
		t.Error("a finished erasure is still owed")
	}
}

// TestARestartFindsTheFileByNameAndContent is the same case across a restart,
// which is where the path really cannot be known.
//
// The intent on disk carries a name and a hash and no path — that is what is
// written when a message is deleted mid-verification. A new process resolves it
// against the download directory and finishes the erasure.
func TestARestartFindsTheFileByNameAndContent(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("77777778-7777-4777-8777-777777777778")
	content := []byte("written by a verifier of a process that died")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	written := filepath.Join(downloadDir, "statement.pdf")
	if err := os.WriteFile(written, content, 0o600); err != nil {
		t.Fatalf("prepare the orphaned file: %v", err)
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Downloads:     []pendingDownload{{Name: "statement.pdf", Hash: hash}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(written); !os.IsNotExist(err) {
		t.Errorf("the orphaned file was not found by name and content: err=%v", err)
	}
	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if owed {
		t.Error("the erasure is still owed after the file was removed")
	}
}

// TestACancelledVerifierLeavesAnErasureBehindWhenItCannotUnlink pins the last
// exit of the verifier.
//
// A verifier whose transfer was deleted while it hashed has already renamed its
// file into place, and the mapping that named it is gone. If the unlink then
// fails, that file is unreachable and nothing knows it should be erased —
// unless the failure is written down as work owed, which is what a retry and a
// restart both need.
func TestACancelledVerifierLeavesAnErasureBehindWhenItCannotUnlink(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("88888889-8888-4888-8888-888888888889")
	path := filepath.Join(downloadDir, "cancelled-late.pdf")
	if err := os.WriteFile(path, []byte("what the cancelled verifier renamed"), 0o600); err != nil {
		t.Fatalf("prepare the file: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// The unlink cannot reach the disk.
	restore := syncDirectory
	syncDirectory = func(string) error { return errors.New("the disk did not take it") }
	t.Cleanup(func() { syncDirectory = restore })

	if m.removeOwnedFileInDownloadDir(path, info, "test cleanup") {
		t.Fatal("the cleanup reported success although the flush failed")
	}

	// Which is what the caller acts on: the file becomes work owed.
	m.mu.Lock()
	m.noteCleanupLocked(pendingCleanup{
		FileID:    fileID,
		Downloads: []pendingDownload{{Path: path, Name: "cancelled-late.pdf"}},
		NotedAt:   time.Now(),
	})
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed {
		t.Fatal("nothing was left to finish the erasure")
	}

	// And the retry finishes it once the disk cooperates.
	syncDirectory = restore
	m.mu.Lock()
	m.pendingCleanups[fileID].NextAttemptAt = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	m.tickPendingCleanups()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the retry did not remove the file: err=%v", err)
	}
}

// TestTheIntentFileIsReadableBothWays pins the shape on disk across a version
// step in either direction.
//
// A file written by an older build names one file of each kind in a field of
// its own; this build keeps lists. Reading has to fold the old form in, or a
// deleted message's files come back unowned after an upgrade. Writing has to
// keep filling the old fields in while there is one of each, or a DOWNGRADE
// loads the file, finds no work, and leaves those files on disk forever.
func TestTheIntentFileIsReadableBothWays(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	// What a previous build left behind.
	legacy := `{
	  "version": 1,
	  "updated_at": "2026-01-02T03:04:05Z",
	  "transfers": [],
	  "cleanups": [{
	    "file_id": "99999990-9999-4999-8999-999999999990",
	    "transmit_hash": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
	    "completed_path": "` + filepath.Join(downloadDir, "legacy.pdf") + `",
	    "partial_path": "` + filepath.Join(downloadDir, "partial", "legacy.part") + `",
	    "noted_at": "2026-01-02T03:04:05Z"
	  }]
	}`
	if err := os.WriteFile(m.mappingsPath, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write the legacy file: %v", err)
	}
	m.loadMappings()

	const fileID = domain.FileID("99999990-9999-4999-8999-999999999990")
	m.mu.Lock()
	intent, ok := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !ok {
		t.Fatal("an erasure written by an older build was not read back")
	}
	if len(intent.TransmitHashes) != 1 || len(intent.Downloads) != 1 || len(intent.Partials) != 1 {
		t.Fatalf("the old form folded into %+v, want one of each", intent)
	}

	// And writing it again keeps the fields that build can read.
	m.mu.Lock()
	m.cleanupsDirty = true
	if err := m.saveMappingsLockedErr(); err != nil {
		t.Fatalf("save: %v", err)
	}
	m.mu.Unlock()

	written, err := os.ReadFile(m.mappingsPath)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var parsed persistedTransferFile
	if err := json.Unmarshal(written, &parsed); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(parsed.Cleanups) != 1 {
		t.Fatalf("wrote %d erasures, want 1", len(parsed.Cleanups))
	}
	saved := parsed.Cleanups[0]
	if saved.CompletedPath == "" || saved.PartialPath == "" || saved.TransmitHash == "" {
		t.Errorf("a previous build would read no work from this file: %+v", saved)
	}
	if len(saved.Downloads) != 1 || len(saved.Partials) != 1 || len(saved.TransmitHashes) != 1 {
		t.Errorf("this build would read %+v, want one of each", saved)
	}

	// Reading our own file back does not duplicate the work it names twice.
	m.mu.Lock()
	m.pendingCleanups = nil
	m.mu.Unlock()
	m.loadMappings()
	m.mu.Lock()
	reloaded, ok := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !ok {
		t.Fatal("the erasure did not survive its own round trip")
	}
	if len(reloaded.TransmitHashes) != 1 || len(reloaded.Downloads) != 1 || len(reloaded.Partials) != 1 {
		t.Errorf("the round trip doubled the work: %+v", reloaded)
	}
}

// TestAnAttemptReadsItsOwnCopyOfTheWork pins the snapshot an attempt runs from.
//
// The intent holds slices. A copy of the struct shares their backing arrays, so
// an attempt reading them with the mutex released would be reading memory that
// noteCleanupLocked appends to AND rewrites in place — addDownload fills in a
// hash on an entry that is already there. The attempt has to own what it reads.
func TestAnAttemptReadsItsOwnCopyOfTheWork(t *testing.T) {
	t.Parallel()

	original := pendingCleanup{
		FileID:         "cccccccd-cccc-4ccc-8ccc-cccccccccccd",
		TransmitHashes: []string{"hash-a"},
		Downloads:      []pendingDownload{{Path: "/downloads/one.pdf"}},
		Partials:       []string{"/downloads/partial/one.part"},
	}
	snapshot := original.clone()

	// What noteCleanupLocked does to the entry while an attempt is running.
	original.TransmitHashes, _ = addString(original.TransmitHashes, "hash-b")
	original.Downloads, _ = addDownload(original.Downloads, pendingDownload{Path: "/downloads/one.pdf", Hash: "hash-c"})
	original.Downloads, _ = addDownload(original.Downloads, pendingDownload{Path: "/downloads/two.pdf"})
	original.Partials, _ = addString(original.Partials, "/downloads/partial/two.part")

	if len(snapshot.TransmitHashes) != 1 || len(snapshot.Downloads) != 1 || len(snapshot.Partials) != 1 {
		t.Fatalf("the snapshot grew with the entry: %+v", snapshot)
	}
	if snapshot.Downloads[0].Hash != "" {
		t.Errorf("the snapshot's entry was rewritten in place: %+v", snapshot.Downloads[0])
	}
}

// TestWorkAddedDuringAnAttemptSurvivesIt pins the other half: what is added
// while an attempt runs is neither retired by its outcome nor lost.
func TestWorkAddedDuringAnAttemptSurvivesIt(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("aaaaaaab-aaaa-4aaa-8aaa-aaaaaaaaaaab")
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	for i := range 8 {
		m.noteCleanupLocked(pendingCleanup{
			FileID:    fileID,
			Downloads: []pendingDownload{{Path: filepath.Join(downloadDir, fmt.Sprintf("early-%d.pdf", i))}},
			NotedAt:   time.Now(),
		})
	}
	m.pendingCleanups[fileID].NextAttemptAt = time.Now().Add(-time.Minute)
	m.mu.Unlock()

	// The attempt is SLOWED from inside, so its walk over the work it was given
	// overlaps with the work being added — which is the window the generation
	// counter is about, and the window in which the two would share memory.
	started := make(chan struct{})
	var once sync.Once
	restore := syncDirectory
	syncDirectory = func(dir string) error {
		once.Do(func() { close(started) })
		time.Sleep(time.Millisecond)
		return restore(dir)
	}
	t.Cleanup(func() { syncDirectory = restore })

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.tickPendingCleanups()
	}()
	<-started

	for i := range 64 {
		m.mu.Lock()
		// New work, and work that REVISES an entry the attempt is holding: a
		// later deletion of the same file can learn the hash the earlier one
		// did not know, and adding it rewrites that element in place.
		m.noteCleanupLocked(pendingCleanup{
			FileID:    fileID,
			Downloads: []pendingDownload{{Path: filepath.Join(downloadDir, fmt.Sprintf("added-%d.pdf", i))}},
			NotedAt:   time.Now(),
		})
		m.noteCleanupLocked(pendingCleanup{
			FileID: fileID,
			Downloads: []pendingDownload{{
				Path: filepath.Join(downloadDir, fmt.Sprintf("early-%d.pdf", i%8)),
				Hash: fmt.Sprintf("%064d", i),
			}},
			NotedAt: time.Now(),
		})
		m.mu.Unlock()
	}
	wg.Wait()

	m.mu.Lock()
	intent, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed {
		t.Fatal("the work added while the attempt ran was retired with it")
	}
	if len(intent.Downloads) < 64 {
		t.Errorf("the intent holds %d downloads, want at least the 64 added during the attempt", len(intent.Downloads))
	}
}

// TestAnAlreadyGonePartialFinishesTheErasure pins that a registration cannot
// keep a finished erasure alive.
//
// The new download renames its .part away when it completes and the mapping
// stays — in waiting_ack, then completed. An erasure that only asked "is there
// a mapping" would answer "not mine" forever and keep a note about a deleted
// message on disk for good, with no file left anywhere to justify it.
func TestAnAlreadyGonePartialFinishesTheErasure(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ddddddde-dddd-4ddd-8ddd-ddddddddddde")
	partial := partialDownloadPath(downloadDir, fileID)

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Partials:      []string{partial},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// The download that took the id since has finished: its .part was renamed
	// away, and the mapping is still here.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "again.pdf",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: filepath.Join(downloadDir, "again.pdf"),
		CreatedAt:     time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if owed {
		t.Error("a note about a deleted message is kept although the file it names is gone")
	}
}

// TestTheVerifierDoesNotTakeANewAttemptsPartial pins the last unlink of a
// verifier whose transfer went away.
//
// The .part path is derived from the file id, so a transfer cancelled and
// started again writes to the same place. The decision is by OWNERSHIP of the
// id, not by inode identity: a filesystem may hand the replacement file the
// same inode, and then an identity check would happily delete a download in
// progress.
func TestTheVerifierDoesNotTakeANewAttemptsPartial(t *testing.T) {
	m, _, _ := newCleanupIntentManager(t)

	const fileID = domain.FileID("eeeeeeef-eeee-4eee-8eee-eeeeeeeeeeef")
	mine := &receiverFileMapping{
		FileID:     fileID,
		FileName:   "again.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverVerifying,
		Generation: 7,
		CreatedAt:  time.Now(),
	}

	// Nobody owns the id: the partial was this verifier's and goes with it.
	if stillOurs, takenOver := m.verifierStandingLocked(fileID, mine, 7); stillOurs || takenOver {
		t.Errorf("with no mapping: stillOurs=%v takenOver=%v, want false/false", stillOurs, takenOver)
	}

	// A new attempt registered for the same id: its file, not ours.
	m.mu.Lock()
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileName:   "again.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverDownloading,
		Generation: 8,
		CreatedAt:  time.Now(),
	}
	m.mu.Unlock()
	if stillOurs, takenOver := m.verifierStandingLocked(fileID, mine, 7); stillOurs || !takenOver {
		t.Errorf("after a new attempt: stillOurs=%v takenOver=%v, want false/true", stillOurs, takenOver)
	}

	// And the verifier that still owns its transfer carries on.
	m.mu.Lock()
	m.receiverMaps[fileID] = mine
	m.mu.Unlock()
	if stillOurs, takenOver := m.verifierStandingLocked(fileID, mine, 7); !stillOurs || takenOver {
		t.Errorf("while it still owns the transfer: stillOurs=%v takenOver=%v, want true/false", stillOurs, takenOver)
	}
}

// TestAStaleResumeLeavesTheCurrentAttemptsPartialAlone pins the two deferred
// paths that also remove a .part: the truncation before a resume and the
// cleanup of a download that ran out of retries.
//
// Both are dispatched from a tick, so the action they carry can be several
// moments old, and the file they name belongs to whatever transfer holds the id
// NOW. "Idempotent local I/O" was true of the syscall and false of its meaning.
func TestAStaleResumeLeavesTheCurrentAttemptsPartialAlone(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ffffff03-ffff-4fff-8fff-ffffffffff03")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	write := func() {
		if err := os.WriteFile(partial, []byte("the current attempt"), 0o600); err != nil {
			t.Fatalf("prepare the partial: %v", err)
		}
	}
	write()

	// The transfer that owns the id now, at generation 9.
	m.mu.Lock()
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileName:   "current.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverDownloading,
		Generation: 9,
		CreatedAt:  time.Now(),
	}
	m.mu.Unlock()

	// A resume built before that, at generation 8.
	m.truncatePartialFile(resumeSnapshot{fileID: fileID, truncatePartial: true}, 8)
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("a stale resume deleted the current attempt's partial: %v", err)
	}

	// And a failure cleanup built before it, likewise.
	m.removeFailedPartial(receiverTickAction{fileID: fileID, generation: 8})
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("a stale failure cleanup deleted the current attempt's partial: %v", err)
	}

	// Both still do their job for the generation they belong to.
	m.truncatePartialFile(resumeSnapshot{fileID: fileID, truncatePartial: true}, 9)
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the resume of the current attempt did not clear its partial: err=%v", err)
	}
	write()
	m.removeFailedPartial(receiverTickAction{fileID: fileID, generation: 9})
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the failure cleanup of the current attempt did not clear its partial: err=%v", err)
	}
}

// TestARepeatOfTheSameWorkIsNotARevision pins what counts as "the intent was
// given more to do".
//
// A transient failure can hand the same partial to an intent that already names
// it. That is not new work, and counting it as a revision makes the attempt in
// flight look stale: its "done" would then be refused, and the record of a
// finished erasure would stay on disk — the one thing this subsystem may not
// leave behind.
func TestARepeatOfTheSameWorkIsNotARevision(t *testing.T) {
	t.Parallel()

	m, _, downloadDir := newCleanupIntentManager(t)
	const fileID = domain.FileID("ffffff04-ffff-4fff-8fff-ffffffffff04")
	partial := partialDownloadPath(downloadDir, fileID)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.noteCleanupLocked(pendingCleanup{FileID: fileID, Partials: []string{partial}, NotedAt: time.Now()})
	first := m.pendingCleanups[fileID].gen

	// The same path again — nothing new is owed.
	m.noteCleanupLocked(pendingCleanup{FileID: fileID, Partials: []string{partial}, NotedAt: time.Now()})
	if got := m.pendingCleanups[fileID].gen; got != first {
		t.Errorf("a repeat of the same work counted as a revision: %d → %d", first, got)
	}

	// Something genuinely new does count.
	m.noteCleanupLocked(pendingCleanup{
		FileID:    fileID,
		Downloads: []pendingDownload{{Path: filepath.Join(downloadDir, "other.pdf")}},
		NotedAt:   time.Now(),
	})
	if got := m.pendingCleanups[fileID].gen; got == first {
		t.Error("new work did not count as a revision")
	}
}

// TestACancelHoldsTheIdWhileItResets pins that a cancel does not publish
// `available` — the state that lets a new download start — before it has taken
// the file id it is about to clear.
func TestACancelHoldsTheIdWhileItResets(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ffffff05-ffff-4fff-8fff-ffffffffff05")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("a download in progress"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}
	m.mu.Lock()
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileName:   "in-flight.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverDownloading,
		Generation: 3,
		CreatedAt:  time.Now(),
	}
	m.mu.Unlock()

	release := m.lockPartial(fileID)
	cancelled := make(chan error, 1)
	go func() { cancelled <- m.CancelDownload(fileID) }()
	time.Sleep(20 * time.Millisecond)

	// While the cancel waits for the id, the transfer must NOT look restartable
	// yet: a StartDownload here would write into the very file the cancel is
	// about to remove.
	m.mu.Lock()
	state := m.receiverMaps[fileID].State
	m.mu.Unlock()
	if state == receiverAvailable {
		t.Error("the cancel published `available` before it held the file id")
	}

	release()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("CancelDownload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the cancel never finished")
	}
	m.mu.Lock()
	state = m.receiverMaps[fileID].State
	m.mu.Unlock()
	if state != receiverAvailable {
		t.Errorf("state after the cancel = %s, want available", state)
	}
}

// TestAResumeLeavesAChunkThatLandedFirstAlone pins the second half of what a
// deferred resume has to check.
//
// The generation says the transfer was not replaced. It does not say that
// nothing was accepted since: a chunk_response that was already in flight when
// the resume was decided lands, writes real bytes and moves NextOffset. Erasing
// the file then throws away a chunk this very transfer has counted.
func TestAResumeLeavesAChunkThatLandedFirstAlone(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ffffff06-ffff-4fff-8fff-ffffffffff06")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("bytes of a chunk that landed"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}
	m.mu.Lock()
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileName:   "resuming.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverDownloading,
		Generation: 4,
		// The chunk that arrived after the resume was decided.
		NextOffset: 65536,
		CreatedAt:  time.Now(),
	}
	m.mu.Unlock()

	// The resume was decided when the file was to be restarted from zero.
	m.truncatePartialFile(resumeSnapshot{fileID: fileID, truncatePartial: true, startOffset: 0}, 4)

	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("the resume erased a chunk the transfer had already accepted: %v", err)
	}

	// And the resume itself is called off: its request would ask for bytes the
	// transfer already has, and a failed send would roll the offset back over
	// them.
	if m.truncatePartialFile(resumeSnapshot{fileID: fileID, truncatePartial: true, startOffset: 0}, 4) {
		t.Error("a resume built before that chunk was allowed to go ahead")
	}

	// With nothing accepted since, the same resume does clear the stale file.
	m.mu.Lock()
	m.receiverMaps[fileID].NextOffset = 0
	m.mu.Unlock()
	if !m.truncatePartialFile(resumeSnapshot{fileID: fileID, truncatePartial: true, startOffset: 0}, 4) {
		t.Error("a current resume was called off")
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the resume did not clear a stale partial: err=%v", err)
	}
}

// TestLearningThePathOfAPathlessErasureMergesAndCounts pins how the two ways of
// naming one file meet.
//
// An erasure written while the download was still being verified names the file
// by content; a later note of the same file knows its path. They are ONE file,
// so the note is merged rather than appended — and it counts as a revision,
// because an attempt working from the pathless entry may have looked, found
// nothing, and be about to report the erasure finished. Its outcome must not
// retire an intent that has since learned where the file is.
func TestLearningThePathOfAPathlessErasureMergesAndCounts(t *testing.T) {
	t.Parallel()

	m, _, downloadDir := newCleanupIntentManager(t)
	const (
		fileID = domain.FileID("ffffff07-ffff-4fff-8fff-ffffffffff07")
		hash   = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.noteCleanupLocked(pendingCleanup{
		FileID:    fileID,
		Downloads: []pendingDownload{{Name: "report.pdf", Hash: hash}},
		NotedAt:   time.Now(),
	})
	before := m.pendingCleanups[fileID].gen

	m.noteCleanupLocked(pendingCleanup{
		FileID: fileID,
		Downloads: []pendingDownload{{
			Path: filepath.Join(downloadDir, "report.pdf"),
			Name: "report.pdf",
			Hash: hash,
		}},
		NotedAt: time.Now(),
	})

	intent := m.pendingCleanups[fileID]
	if len(intent.Downloads) != 1 {
		t.Fatalf("the same file is named twice: %+v", intent.Downloads)
	}
	if intent.Downloads[0].Path == "" {
		t.Error("the path the later note knew was not kept")
	}
	if intent.gen == before {
		t.Error("learning where the file is did not make an attempt that looked for it stale")
	}

	// A DIFFERENT path of the same name and content is a different file — the
	// store writes the second copy beside the first — and stays its own work.
	m.noteCleanupLocked(pendingCleanup{
		FileID: fileID,
		Downloads: []pendingDownload{{
			Path: filepath.Join(downloadDir, "report (a1b2c3).pdf"),
			Name: "report.pdf",
			Hash: hash,
		}},
		NotedAt: time.Now(),
	})
	if got := len(m.pendingCleanups[fileID].Downloads); got != 2 {
		t.Errorf("the intent names %d file(s), want both paths", got)
	}
}

// TestEveryoneWhoTouchesAPartialTakesTheSameLock pins the coordination the
// .part path needs, which no single check could give it.
//
// That path is derived from the file id, so it is shared by the transfer
// writing it, the transfer that replaces it after a cancel, the verifier that
// renames it away, and the erasure of a deleted message. Each of those used to
// decide on its own whether the file was theirs, and every fix produced another
// window. Now all of them take one lock per file id.
func TestEveryoneWhoTouchesAPartialTakesTheSameLock(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ffffff01-ffff-4fff-8fff-ffffffffff01")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("chunks of a deleted message"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Partials:      []string{partial},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	m.mu.Unlock()

	// Somebody is working on that .part — a chunk handler, a verifier, a
	// cancel: they all hold this.
	release := m.lockPartial(fileID)

	swept := make(chan struct{})
	go func() {
		m.tickPendingCleanups()
		close(swept)
	}()
	select {
	case <-swept:
		t.Fatal("the erasure ran while another writer held the file id")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("the partial was unlinked while the file id was held: %v", err)
	}

	release()
	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatal("the erasure never ran after the file id was released")
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the erasure did not remove the partial: err=%v", err)
	}
}

// TestACancelWaitsForTheFileIdItIsResetting is the other side of the same lock:
// a cancel removes the .part, and it cannot do that while a verifier is between
// its ownership check and its rename.
func TestACancelWaitsForTheFileIdItIsResetting(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("ffffff02-ffff-4fff-8fff-ffffffffff02")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("a download in progress"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}
	m.mu.Lock()
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileName:   "in-flight.pdf",
		Sender:     domaintest.ID("bob"),
		State:      receiverDownloading,
		Generation: 3,
		CreatedAt:  time.Now(),
	}
	m.mu.Unlock()

	release := m.lockPartial(fileID)
	cancelled := make(chan error, 1)
	go func() { cancelled <- m.CancelDownload(fileID) }()

	select {
	case <-cancelled:
		t.Fatal("the cancel removed the partial while the file id was held elsewhere")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("the partial was removed while the file id was held: %v", err)
	}

	release()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("CancelDownload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the cancel never finished after the file id was released")
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the cancel left the partial behind: err=%v", err)
	}
}

// TestAPartialOfATransferThatWroteNothingIsStillOwed pins the .part of a
// deleted message when the same file id has been registered again.
//
// The path is derived from the id, so the two share it. Erasing takes the bytes
// of a transfer in progress; FORGETTING leaves the old partial on disk with
// nothing that would ever look for it — which is what happens if the new
// mapping is only `available` and never writes anything. So the work stays
// owed, and the next attempt settles it once the id is free again.
func TestAPartialOfATransferThatWroteNothingIsStillOwed(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("bbbbbbbc-bbbb-4bbb-8bbb-bbbbbbbbbbbc")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("the deleted message's chunks"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Partials:      []string{partial},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// Announced, nothing downloaded: this mapping has never touched the file.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileName:  "again.pdf",
		Sender:    domaintest.ID("bob"),
		State:     receiverAvailable,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("the partial was erased under a registered transfer: %v", err)
	}
	m.mu.Lock()
	intent, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed || len(intent.Partials) != 1 {
		t.Fatalf("the erasure of the old partial was forgotten: owed=%v intent=%+v", owed, intent)
	}

	// The new transfer goes away, and the next attempt finishes the job.
	m.mu.Lock()
	delete(m.receiverMaps, fileID)
	m.pendingCleanups[fileID].NextAttemptAt = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	m.tickPendingCleanups()

	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("the partial of the deleted message is still on disk: err=%v", err)
	}
}

// TestASecondDeletionDoesNotForgetTheFirstFile pins that adding work never
// replaces work.
//
// The first file could not be unlinked and is still owed; a second deletion of
// the same file id then names a different path. Keeping only the newer one
// leaves the first file on disk with nothing that will ever look for it again.
func TestASecondDeletionDoesNotForgetTheFirstFile(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("55555556-5555-4555-8555-555555555556")
	stubborn := filepath.Join(downloadDir, "stuck.pdf")
	if err := os.MkdirAll(filepath.Join(stubborn, "blocker"), 0o700); err != nil {
		t.Fatalf("prepare the undeletable path: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "stuck.pdf",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: stubborn,
		CreatedAt:     time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)
	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed {
		t.Fatal("the failed unlink left nothing behind")
	}

	// The same file id is registered and deleted again, this time with a file
	// that is somewhere else.
	second := filepath.Join(downloadDir, "second.pdf")
	if err := os.WriteFile(second, []byte("the second file"), 0o600); err != nil {
		t.Fatalf("prepare the second download: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "second.pdf",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: second,
		CreatedAt:     time.Now(),
	}
	m.CleanupTransferByMessageID(fileID)

	m.mu.Lock()
	intent := *m.pendingCleanups[fileID]
	m.mu.Unlock()
	paths := make(map[string]bool, len(intent.Downloads))
	for _, download := range intent.Downloads {
		paths[download.Path] = true
	}
	if !paths[stubborn] {
		t.Errorf("the second deletion forgot the file the first one could not remove: %+v", intent.Downloads)
	}
	if _, err := os.Stat(second); !os.IsNotExist(err) {
		t.Errorf("the second file was not erased: err=%v", err)
	}
}

// TestARetriedErasureLeavesALiveTransfersPartialAlone pins the .part path,
// which is derived from the file id and therefore SHARED with any download
// registered for that id afterwards.
//
// An erasure that survived a failure or a restart would otherwise delete the
// partial file of a transfer that is running right now.
func TestARetriedErasureLeavesALiveTransfersPartialAlone(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("66666667-6666-4666-8666-666666666667")
	partial := partialDownloadPath(downloadDir, fileID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o700); err != nil {
		t.Fatalf("prepare the partial dir: %v", err)
	}
	if err := os.WriteFile(partial, []byte("chunks of a download in progress"), 0o600); err != nil {
		t.Fatalf("prepare the partial: %v", err)
	}

	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	// The erasure left over from the deleted message.
	m.pendingCleanups[fileID] = &pendingCleanup{
		FileID:        fileID,
		Partials:      []string{partial},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// And the download registered for the same id since.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileName:  "again.pdf",
		Sender:    domaintest.ID("bob"),
		State:     receiverDownloading,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(partial); err != nil {
		t.Errorf("the retry deleted the partial file of a live transfer: %v", err)
	}
}

// TestARetryAsksAgainWhoOwnsTheFile pins that the ownership question is asked
// at every attempt rather than once, when the work was written down.
//
// An intent can wait minutes between attempts, and in that time the file can
// acquire an owner it did not have — a message that arrived after the deletion
// was scheduled and mapped onto the same name and content. An attempt working
// from the answer it got when the intent was created would erase a file the new
// message is showing.
func TestARetryAsksAgainWhoOwnsTheFile(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		deleted = domain.FileID("aaaaaaa1-1111-4111-8111-111111111111")
		arrived = domain.FileID("bbbbbbb2-2222-4222-8222-222222222222")
	)
	shared := filepath.Join(downloadDir, "slides.pdf")
	if err := os.WriteFile(shared, []byte("the same bytes in both chats"), 0o600); err != nil {
		t.Fatalf("prepare the download: %v", err)
	}

	// The erasure was scheduled when nothing else held the file. Make it fail
	// once so it is still owed when the newcomer appears.
	m.mu.Lock()
	if m.pendingCleanups == nil {
		m.pendingCleanups = make(map[domain.FileID]*pendingCleanup)
	}
	m.pendingCleanups[deleted] = &pendingCleanup{
		FileID:        deleted,
		Downloads:     []pendingDownload{{Path: shared}},
		NotedAt:       time.Now().Add(-time.Hour),
		NextAttemptAt: time.Now().Add(-time.Minute),
	}
	// A message that arrived since, mapped onto the very same file.
	m.receiverMaps[arrived] = &receiverFileMapping{
		FileID:        arrived,
		FileName:      "slides.pdf",
		Sender:        domaintest.ID("carol"),
		State:         receiverCompleted,
		CompletedPath: shared,
		CreatedAt:     time.Now(),
	}
	m.mu.Unlock()

	m.tickPendingCleanups()

	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("the retry erased a file that had acquired an owner since: %v", err)
	}
	m.mu.Lock()
	_, stillOwed := m.pendingCleanups[deleted]
	m.mu.Unlock()
	if stillOwed {
		t.Error("the erasure is still owed although the file now belongs to another message")
	}
}

// TestTheSharedFileOfAnotherChatIsNotErased pins that a download belonging to
// two messages outlives the first of them.
//
// The same attachment arriving twice — same name, same content — is stored
// ONCE: completedDownloadPath hands the second message the file that is already
// there. Clearing one chat therefore reaches a file another chat is showing,
// and unlinking it leaves that conversation with a mapping pointing at nothing.
func TestTheSharedFileOfAnotherChatIsNotErased(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const (
		fromAlice = domain.FileID("22222222-2222-4222-8222-222222222222")
		fromCarol = domain.FileID("33333333-3333-4333-8333-333333333333")
	)
	shared := filepath.Join(downloadDir, "report.pdf")
	if err := os.WriteFile(shared, []byte("the same bytes in both chats"), 0o600); err != nil {
		t.Fatalf("prepare the shared download: %v", err)
	}
	for id, peer := range map[domain.FileID]string{fromAlice: "alice", fromCarol: "carol"} {
		m.receiverMaps[id] = &receiverFileMapping{
			FileID:        id,
			FileName:      "report.pdf",
			Sender:        domaintest.ID(peer),
			State:         receiverCompleted,
			CompletedPath: shared,
			CreatedAt:     time.Now(),
		}
	}

	// Alice's chat is cleared.
	m.CleanupTransferByMessageID(fromAlice)

	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("the file Carol's message still points at was erased: %v", err)
	}
	m.mu.Lock()
	_, aliceGone := m.receiverMaps[fromAlice]
	_, carolKept := m.receiverMaps[fromCarol]
	_, owed := m.pendingCleanups[fromAlice]
	m.mu.Unlock()
	if aliceGone {
		t.Error("the mapping of the deleted message survived")
	}
	if !carolKept {
		t.Error("the other conversation lost its mapping")
	}
	if owed {
		t.Error("an erasure is still owed for a file that must not be erased")
	}

	// And when the LAST message holding it goes, the file goes.
	m.CleanupTransferByMessageID(fromCarol)
	if _, err := os.Stat(shared); !os.IsNotExist(err) {
		t.Errorf("the last message holding the file left it on disk: err=%v", err)
	}
}

// TestUnfinishedErasureSurvivesRestart is the whole reason the intent
// exists. Dropping the mapping is what makes the file unreachable, so if
// the unlink then fails there is nothing left that knows the bytes of a
// deleted message are still on disk — not after a retry, not after a
// restart.
func TestUnfinishedErasureSurvivesRestart(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	// A download the unlink cannot remove: it is a non-empty DIRECTORY at
	// the path the mapping names, which os.Remove refuses.
	const fileID = domain.FileID("11111111-1111-4111-8111-111111111111")
	completed := filepath.Join(downloadDir, "photo.png")
	if err := os.MkdirAll(filepath.Join(completed, "blocker"), 0o700); err != nil {
		t.Fatalf("prepare undeletable path: %v", err)
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileName:      "photo.png",
		Sender:        domaintest.ID("bob"),
		State:         receiverCompleted,
		CompletedPath: completed,
		CreatedAt:     time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)

	m.mu.Lock()
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed {
		t.Fatal("a failed unlink left nothing behind; the file is now unreachable and unerasable")
	}

	// A fresh manager over the same file is what the next process sees.
	restarted := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        m.store,
		downloadDir:  m.downloadDir,
		mappingsPath: m.mappingsPath,
		stopCh:       make(chan struct{}),
	}
	restarted.loadMappings()
	restarted.mu.Lock()
	intent, restored := restarted.pendingCleanups[fileID]
	restarted.mu.Unlock()
	if !restored {
		t.Fatal("the unfinished erasure did not survive the restart")
	}
	if len(intent.Downloads) != 1 || intent.Downloads[0].Path != completed {
		t.Errorf("restored downloads = %+v, want the one path %q", intent.Downloads, completed)
	}

	// The obstacle goes away and the maintenance tick finishes the job.
	if err := os.RemoveAll(filepath.Join(completed, "blocker")); err != nil {
		t.Fatalf("clear the obstacle: %v", err)
	}
	// The backoff of the failed attempt survived the restart too — the intent
	// is written out after each pass, schedule included — so the tick would
	// otherwise skip it as not yet due. Wound back rather than waited out.
	restarted.mu.Lock()
	restarted.pendingCleanups[fileID].NextAttemptAt = time.Now().Add(-time.Second)
	restarted.mu.Unlock()
	restarted.tickPendingCleanups()

	if _, err := os.Stat(completed); !os.IsNotExist(err) {
		t.Errorf("the file of a deleted message is still on disk (err=%v)", err)
	}
	restarted.mu.Lock()
	_, stillOwed := restarted.pendingCleanups[fileID]
	restarted.mu.Unlock()
	if stillOwed {
		t.Error("the intent outlived the erasure it asked for")
	}
}

// TestSuccessfulErasureLeavesNoIntent: the retry machinery must not turn
// the ordinary path into a permanent bookkeeping cost.
func TestSuccessfulErasureLeavesNoIntent(t *testing.T) {
	m, transmitDir, downloadDir := newCleanupIntentManager(t)

	const (
		fileID = domain.FileID("22222222-2222-4222-8222-222222222222")
		hash   = "aabbccdd11223344556677889900aabb11223344556677889900aabbccddeeff"
	)
	transmitPath := filepath.Join(transmitDir, hash+".png")
	if err := os.WriteFile(transmitPath, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write transmit file: %v", err)
	}
	completed := filepath.Join(downloadDir, "photo.png")
	if err := os.WriteFile(completed, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write download: %v", err)
	}

	m.store.mu.Lock()
	m.store.refs[hash] = 1
	m.store.mu.Unlock()
	m.senderMaps[fileID] = &senderFileMapping{
		FileID: fileID, FileHash: hash, FileName: "photo.png",
		Recipient: domaintest.ID("bob"), State: senderAnnounced, CreatedAt: time.Now(),
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID: fileID, FileName: "photo.png", Sender: domaintest.ID("bob"),
		State: receiverCompleted, CompletedPath: completed, CreatedAt: time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)

	for _, path := range []string{transmitPath, completed} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s survived the cleanup (err=%v)", path, err)
		}
	}
	m.mu.Lock()
	owed := len(m.pendingCleanups)
	m.mu.Unlock()
	if owed != 0 {
		t.Errorf("%d intents left after a clean erasure", owed)
	}

	// And not in the FILE either. Memory is not where this matters: the intent
	// names the deleted message and the paths of what hung off it, so an intent
	// that is gone from the map but still written down is a description of the
	// deletion, sitting on disk until some later tick happens to rewrite the
	// file — or for ever, if the user closes the application first.
	restarted := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        m.store,
		downloadDir:  m.downloadDir,
		mappingsPath: m.mappingsPath,
		stopCh:       make(chan struct{}),
	}
	restarted.loadMappings()
	restarted.mu.Lock()
	survived, stillThere := restarted.pendingCleanups[fileID]
	restarted.mu.Unlock()
	if stillThere {
		t.Errorf("a finished erasure is still recorded on disk after a restart: %+v", survived)
	}

	raw, err := os.ReadFile(m.mappingsPath)
	if err != nil {
		t.Fatalf("read the mappings file: %v", err)
	}
	if bytes.Contains(raw, []byte(fileID)) {
		t.Errorf("the mappings file still names the deleted message:\n%s", raw)
	}
}

// TestPurgeUnreferencedLeavesASharedBlobAlone: the same content can back
// two messages, and erasing one must not take the other's file. The retry
// path uses PurgeUnreferenced precisely because it can be repeated without
// touching the ref count that answers this.
func TestPurgeUnreferencedLeavesASharedBlobAlone(t *testing.T) {
	m, transmitDir, _ := newCleanupIntentManager(t)

	const hash = "aabbccdd11223344556677889900aabb11223344556677889900aabbccddeeff"
	transmitPath := filepath.Join(transmitDir, hash+".png")
	if err := os.WriteFile(transmitPath, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write transmit file: %v", err)
	}
	m.store.mu.Lock()
	m.store.refs[hash] = 2
	m.store.mu.Unlock()

	if err := m.store.PurgeUnreferenced(hash); err != nil {
		t.Fatalf("PurgeUnreferenced: %v", err)
	}
	if _, err := os.Stat(transmitPath); err != nil {
		t.Fatalf("a blob another message still references was erased: %v", err)
	}

	// Repeating it after the last reference goes is what a retry does.
	m.store.mu.Lock()
	delete(m.store.refs, hash)
	m.store.mu.Unlock()
	if err := m.store.PurgeUnreferenced(hash); err != nil {
		t.Fatalf("PurgeUnreferenced (unreferenced): %v", err)
	}
	if _, err := os.Stat(transmitPath); !os.IsNotExist(err) {
		t.Errorf("the unreferenced blob is still on disk (err=%v)", err)
	}
}

// countingWriteManager is a manager whose mappings file lives in a
// directory a test can make unwritable, so the ordering rule — record
// first, erase second — can be exercised rather than argued about.
func TestNothingIsErasedUntilTheRecordIsOnDisk(t *testing.T) {
	m, transmitDir, downloadDir := newCleanupIntentManager(t)

	const (
		fileID = domain.FileID("33333333-3333-4333-8333-333333333333")
		hash   = "aabbccdd11223344556677889900aabb11223344556677889900aabbccddeeff"
	)
	transmitPath := filepath.Join(transmitDir, hash+".png")
	if err := os.WriteFile(transmitPath, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write transmit file: %v", err)
	}
	completed := filepath.Join(downloadDir, "photo.png")
	if err := os.WriteFile(completed, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write download: %v", err)
	}
	m.store.mu.Lock()
	m.store.refs[hash] = 1
	m.store.mu.Unlock()
	m.senderMaps[fileID] = &senderFileMapping{
		FileID: fileID, FileHash: hash, FileName: "photo.png",
		Recipient: domaintest.ID("bob"), State: senderAnnounced, CreatedAt: time.Now(),
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID: fileID, FileName: "photo.png", Sender: domaintest.ID("bob"),
		State: receiverCompleted, CompletedPath: completed, CreatedAt: time.Now(),
	}

	// The mappings file cannot be written: its path is a directory.
	if err := os.MkdirAll(m.mappingsPath, 0o700); err != nil {
		t.Fatalf("block the mappings file: %v", err)
	}

	m.CleanupTransferByMessageID(fileID)

	for _, path := range []string{transmitPath, completed} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s was erased before the record of the erasure reached the disk: %v", path, err)
		}
	}
	m.mu.Lock()
	dirty := m.cleanupsDirty
	_, owed := m.pendingCleanups[fileID]
	m.mu.Unlock()
	if !owed || !dirty {
		t.Fatalf("intent owed=%v dirty=%v; the erasure was forgotten instead of deferred", owed, dirty)
	}

	// A tick with the record still unwritable must not touch a file either.
	m.tickPendingCleanups()
	if _, err := os.Stat(completed); err != nil {
		t.Errorf("the tick erased a file while the record was still unwritable: %v", err)
	}

	// The file system recovers; the next tick records, then erases.
	if err := os.RemoveAll(m.mappingsPath); err != nil {
		t.Fatalf("unblock the mappings file: %v", err)
	}
	m.tickPendingCleanups()

	for _, path := range []string{transmitPath, completed} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s survived the recovered tick (err=%v)", path, err)
		}
	}
}

// TestStuckIntentBacksOff: an obstacle that never clears — a read-only
// volume, a device that is gone — must not cost a warn log and a full
// rewrite of the mappings file every tick forever.
func TestStuckIntentBacksOff(t *testing.T) {
	m, _, downloadDir := newCleanupIntentManager(t)

	const fileID = domain.FileID("44444444-4444-4444-8444-444444444444")
	completed := filepath.Join(downloadDir, "photo.png")
	if err := os.MkdirAll(filepath.Join(completed, "blocker"), 0o700); err != nil {
		t.Fatalf("prepare undeletable path: %v", err)
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID: fileID, FileName: "photo.png", Sender: domaintest.ID("bob"),
		State: receiverCompleted, CompletedPath: completed, CreatedAt: time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)

	m.mu.Lock()
	intent, owed := m.pendingCleanups[fileID]
	first := *intent
	m.mu.Unlock()
	if !owed {
		t.Fatal("no intent after a failed erasure")
	}
	if first.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", first.Attempts)
	}
	if !first.NextAttemptAt.After(time.Now()) {
		t.Fatal("the next attempt is already due; a stuck intent would retry every tick")
	}

	// A tick before the intent comes due does nothing at all.
	m.tickPendingCleanups()
	m.mu.Lock()
	unchanged := *m.pendingCleanups[fileID]
	m.mu.Unlock()
	if unchanged.Attempts != first.Attempts {
		t.Errorf("attempts = %d after an early tick, want %d", unchanged.Attempts, first.Attempts)
	}

	// Forced due, the retry runs and pushes the schedule further out.
	m.mu.Lock()
	m.pendingCleanups[fileID].NextAttemptAt = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.tickPendingCleanups()

	m.mu.Lock()
	second := *m.pendingCleanups[fileID]
	m.mu.Unlock()
	if second.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", second.Attempts)
	}
	if !second.NextAttemptAt.After(first.NextAttemptAt) {
		t.Error("the retry interval did not grow")
	}
}

// TestErasingAnAttachmentWritesNothingToTheLog is the same contract as the
// chatlog's, in the package that owns the files.
//
// A deleted message's attachment is erased here, and the line that reported it
// said so at Info: which transfer, whether the bytes went, and — by the time it
// was written — when. The id had been a digest for a while, which made the line
// anonymous rather than absent. What it still said is that this user destroyed
// something a moment ago, in a file no checkpoint or migration ever touches.
func TestErasingAnAttachmentWritesNothingToTheLog(t *testing.T) {
	m, transmitDir, downloadDir := newCleanupIntentManager(t)

	var captured bytes.Buffer
	restore := log.Logger
	log.Logger = zerolog.New(&captured).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = restore })

	const (
		fileID = domain.FileID("33333333-3333-4333-8333-333333333333")
		hash   = "ccddeeff11223344556677889900aabb11223344556677889900aabbccddeeff"
	)
	transmitPath := filepath.Join(transmitDir, hash+".png")
	if err := os.WriteFile(transmitPath, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write transmit file: %v", err)
	}
	completed := filepath.Join(downloadDir, "holiday.png")
	if err := os.WriteFile(completed, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write download: %v", err)
	}
	m.store.mu.Lock()
	m.store.refs[hash] = 1
	m.store.mu.Unlock()
	m.senderMaps[fileID] = &senderFileMapping{
		FileID: fileID, FileHash: hash, FileName: "holiday.png",
		Recipient: domaintest.ID("bob"), State: senderAnnounced, CreatedAt: time.Now(),
	}
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID: fileID, FileName: "holiday.png", Sender: domaintest.ID("bob"),
		State: receiverCompleted, CompletedPath: completed, CreatedAt: time.Now(),
	}

	m.CleanupTransferByMessageID(fileID)

	written := captured.String()
	if strings.Contains(written, "cleaned up by message id") {
		t.Errorf("the log states that an attachment was erased:\n%s", written)
	}
	for _, secret := range []string{string(fileID), hash, "holiday.png"} {
		if strings.Contains(written, secret) {
			t.Errorf("%q appears in the log of a deletion:\n%s", secret, written)
		}
	}
}
