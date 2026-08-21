package filetransfer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	if intent.CompletedPath != completed {
		t.Errorf("restored path = %q, want %q", intent.CompletedPath, completed)
	}

	// The obstacle goes away and the maintenance tick finishes the job.
	if err := os.RemoveAll(filepath.Join(completed, "blocker")); err != nil {
		t.Fatalf("clear the obstacle: %v", err)
	}
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
