package filetransfer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestPrepareFileAnnounceRejectsWhenFull verifies that PrepareFileAnnounce
// returns an error when the sender mapping count reaches maxFileMappings.
func TestPrepareFileAnnounceRejectsWhenFull(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	// Create a transmit file so HasFile passes.
	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")

	// Fill up to the limit.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("file-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("hash-%d", i),
			FileName:  fmt.Sprintf("file-%d.bin", i),
			FileSize:  1000,
			Recipient: receiver,
			State:     senderAnnounced,
			CreatedAt: time.Now(),
		}
	}

	// Attempt to prepare one more — should fail.
	_, err = m.PrepareFileAnnounce(hash, "overflow.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Error("PrepareFileAnnounce should fail when at maxFileMappings limit")
	}
}

// TestPrepareFileAnnounceAndCommitSucceeds verifies the full
// PrepareFileAnnounce → Commit flow when below the limit.
func TestPrepareFileAnnounceAndCommitSucceeds(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	// Create a transmit file for the hash.
	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "test.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce should succeed below limit: %v", err)
	}

	// Pending slot should be reserved.
	m.mu.Lock()
	pending := m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 1 {
		t.Errorf("pendingSenderSlots = %d, want 1", pending)
	}

	// Commit should create the sender mapping.
	fileID := domain.FileID("new-file")
	recipient := domain.PeerIdentity("receiver")
	if err := token.Commit(fileID, recipient); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	sm, ok := m.senderMaps[fileID]
	if !ok {
		t.Fatal("sender mapping should exist after Commit")
	}
	if sm.State != senderAnnounced {
		t.Errorf("state = %s, want announced", sm.State)
	}

	// Pending slot should be released after Commit.
	m.mu.Lock()
	pending = m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingSenderSlots after Commit = %d, want 0", pending)
	}
}

// ---------------------------------------------------------------------------
// PrepareFileAnnounce / Commit / Rollback tests
// ---------------------------------------------------------------------------

// TestPrepareFileAnnounceRejectsNilStore verifies that PrepareFileAnnounce
// returns an error when the transmit store is nil.
func TestPrepareFileAnnounceRejectsNilStore(t *testing.T) {
	t.Parallel()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
		// store intentionally nil
	}

	_, err := m.PrepareFileAnnounce("somehash", "file.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Fatal("PrepareFileAnnounce should fail when store is nil")
	}
}

// TestPrepareFileAnnounceRejectsAtQuota verifies that PrepareFileAnnounce
// returns an error when committed mappings fill the quota.
func TestPrepareFileAnnounceRejectsAtQuota(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("fill-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID: fid,
			State:  senderAnnounced,
		}
	}

	_, err = m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Fatal("PrepareFileAnnounce should fail when at maxFileMappings limit")
	}
}

// TestPrepareFileAnnounceSucceedsBelowQuota verifies that PrepareFileAnnounce
// succeeds and increments the pending counter, and Rollback releases it.
func TestPrepareFileAnnounceSucceedsBelowQuota(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce should succeed below limit: %v", err)
	}

	m.mu.Lock()
	pending := m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 1 {
		t.Errorf("pendingSenderSlots = %d, want 1", pending)
	}

	token.Rollback()

	m.mu.Lock()
	pending = m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingSenderSlots after Rollback = %d, want 0", pending)
	}
}

// TestPrepareFileAnnounceCountsPendingAgainstQuota verifies that pending
// reservations are counted against the quota: if committed + pending
// reaches maxFileMappings, the next reservation is rejected.
func TestPrepareFileAnnounceCountsPendingAgainstQuota(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	// Fill committed mappings to one below the limit.
	for i := 0; i < maxFileMappings-1; i++ {
		fid := domain.FileID(fmt.Sprintf("fill-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID: fid,
			State:  senderAnnounced,
		}
	}

	// Reserve the last slot.
	token1, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("first PrepareFileAnnounce should succeed: %v", err)
	}

	// The next reservation should fail: committed (255) + pending (1) = 256 = maxFileMappings.
	_, err = m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Fatal("second PrepareFileAnnounce should fail when committed + pending = maxFileMappings")
	}

	// Rollback the first reservation. Now quota has room again.
	// Rollback calls RemoveUnreferenced which deletes the transmit file
	// from disk, so we must recreate it for the next PrepareFileAnnounce.
	token1.Rollback()

	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	token2, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce after Rollback should succeed: %v", err)
	}
	token2.Rollback()
}

// TestConcurrentPrepareFileAnnounceRespectsQuota verifies that concurrent
// reservations cannot over-commit beyond maxFileMappings. This is the
// exact race condition that the old CanRegisterSender pre-check could not
// prevent.
func TestConcurrentPrepareFileAnnounceRespectsQuota(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	// Leave exactly 10 slots open.
	for i := 0; i < maxFileMappings-10; i++ {
		fid := domain.FileID(fmt.Sprintf("fill-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID: fid,
			State:  senderAnnounced,
		}
	}

	// Launch 20 goroutines that each try to reserve a slot.
	const goroutines = 20
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		reserved int
		rejected int
		tokens   []*SenderAnnounceToken
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
			if err != nil {
				mu.Lock()
				rejected++
				mu.Unlock()
			} else {
				mu.Lock()
				reserved++
				tokens = append(tokens, token)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if reserved != 10 {
		t.Errorf("reserved = %d, want exactly 10 (only 10 slots were available)", reserved)
	}
	if rejected != 10 {
		t.Errorf("rejected = %d, want exactly 10", rejected)
	}

	// Rollback all reserved tokens.
	for _, token := range tokens {
		token.Rollback()
	}

	m.mu.Lock()
	pending := m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingSenderSlots after full rollback = %d, want 0", pending)
	}
}

// TestRollbackIdempotent verifies that Rollback does not underflow the
// pending counter below zero, and that double-rollback is harmless.
func TestRollbackIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce: %v", err)
	}

	// First rollback releases the slot.
	token.Rollback()
	// Second rollback is a no-op.
	token.Rollback()

	m.mu.Lock()
	pending := m.pendingSenderSlots
	m.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingSenderSlots = %d, want 0 (should not go negative)", pending)
	}
}

// TestRollbackIsNoOpAfterCommit verifies that calling Rollback after
// Commit does not undo the committed mapping.
func TestRollbackIsNoOpAfterCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce: %v", err)
	}

	fileID := domain.FileID("commit-then-rollback")
	recipient := domain.PeerIdentity("receiver")
	if err := token.Commit(fileID, recipient); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Rollback after Commit should be a no-op.
	token.Rollback()

	if _, ok := m.senderMaps[fileID]; !ok {
		t.Error("sender mapping should still exist after Rollback-after-Commit")
	}
}

// TestCommitReleasesPending verifies that a successful Commit releases the
// FileStore pending reservation placed by PrepareFileAnnounce. Without this,
// pending[hash] leaks and RemoveUnreferenced can never clean up the blob
// after the transfer completes and Release drops the committed ref.
func TestCommitReleasesPending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	filePath := dir + "/" + hash + ".bin"
	if err := writeTestFile(filePath, []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce: %v", err)
	}

	// Before Commit: pending must be 1.
	store.mu.RLock()
	pendingBefore := store.pending[hash]
	store.mu.RUnlock()
	if pendingBefore != 1 {
		t.Fatalf("pending before Commit = %d, want 1", pendingBefore)
	}

	fileID := domain.FileID("commit-releases-pending")
	recipient := domain.PeerIdentity("receiver-identity-1234567890ab")
	if err := token.Commit(fileID, recipient); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After Commit: pending must be 0 — Commit released it.
	store.mu.RLock()
	pendingAfter := store.pending[hash]
	store.mu.RUnlock()
	if pendingAfter != 0 {
		t.Fatalf("pending after Commit = %d, want 0 (leaked pending reservation)", pendingAfter)
	}

	// refs must be 1 — Acquire incremented it.
	store.mu.RLock()
	refs := store.refs[hash]
	store.mu.RUnlock()
	if refs != 1 {
		t.Fatalf("refs after Commit = %d, want 1", refs)
	}

	// After Release (simulating transfer completion), RemoveUnreferenced
	// must be able to delete the blob — no leaked pending blocks it.
	store.Release(hash)
	store.RemoveUnreferenced(hash)

	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatal("transmit file should be deleted after Release+RemoveUnreferenced — leaked pending blocked cleanup")
	}
}

// TestCommitRollsBackOnPersistFailure verifies that Commit rolls back the
// in-memory sender mapping and store ref when saveMappingsLockedErr fails
// (e.g. disk full). After the error, Rollback must still work correctly to
// release the pending reservation and sender slot.
// TestCommitKeepsMappingOnPersistFailure verifies that when Commit cannot
// persist to disk, the in-memory mapping and Acquire'd ref are preserved.
// This is critical for the post-DM-sent path: the recipient already holds
// a file card and will request chunks. If Commit rolled back the mapping,
// the sender could not serve those requests, and the subsequent defer
// Rollback (committed=false) would also remove the transmit blob —
// leaving the transfer completely dead.
func TestCommitKeepsMappingOnPersistFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	filePath := dir + "/" + hash + ".bin"
	if err := writeTestFile(filePath, []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	// Use an unwritable path so atomicWriteJSON fails.
	unwritablePath := filepath.Join(dir, "readonly", "transfers.json")
	if err := os.MkdirAll(filepath.Join(dir, "readonly"), 0o500); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: unwritablePath,
		stopCh:       make(chan struct{}),
	}

	token, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce: %v", err)
	}

	// Commit returns error (persist fails) but keeps mapping in memory.
	fileID := domain.FileID("persist-fail")
	recipient := domain.PeerIdentity("receiver-identity-1234567890ab")
	commitErr := token.Commit(fileID, recipient)
	if commitErr == nil {
		t.Fatal("Commit should return error when persist fails")
	}

	// Sender mapping MUST exist in memory — the transfer is live.
	m.mu.Lock()
	mapping, ok := m.senderMaps[fileID]
	m.mu.Unlock()
	if !ok {
		t.Fatal("sender mapping must be kept in memory on persist failure")
	}
	if mapping.State != senderAnnounced {
		t.Errorf("state = %s, want senderAnnounced", mapping.State)
	}

	// Store refs must be 1 — Acquire was not rolled back.
	store.mu.RLock()
	refs := store.refs[hash]
	store.mu.RUnlock()
	if refs != 1 {
		t.Errorf("store.refs[hash] = %d, want 1 (Acquire kept)", refs)
	}

	// committed=true, so Rollback is a no-op — mapping and blob survive.
	token.Rollback()

	m.mu.Lock()
	_, stillExists := m.senderMaps[fileID]
	m.mu.Unlock()
	if !stillExists {
		t.Fatal("Rollback must be no-op after Commit (even with persist failure)")
	}

	// Pending reservation should have been released by Commit.
	store.mu.RLock()
	pending := store.pending[hash]
	store.mu.RUnlock()
	if pending != 0 {
		t.Errorf("store.pending[hash] = %d, want 0 (released by Commit)", pending)
	}

	// Pending sender slot should have been released by Commit.
	m.mu.Lock()
	slots := m.pendingSenderSlots
	m.mu.Unlock()
	if slots != 0 {
		t.Errorf("pendingSenderSlots = %d, want 0 (released by Commit)", slots)
	}

	// Transmit blob must still exist on disk.
	if !store.HasFile(hash) {
		t.Fatal("transmit blob must survive — mapping holds a ref")
	}
}

// TestPrepareFileAnnounceAtomicReservation verifies that PrepareFileAnnounce
// atomically checks file existence and places the pending reservation, so a
// concurrent Rollback on the same hash cannot delete the transmit blob in the
// gap between the two steps. This is the regression test for the P2 TOCTOU bug.
func TestPrepareFileAnnounceAtomicReservation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	filePath := dir + "/" + hash + ".bin"
	if err := writeTestFile(filePath, []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	// First prepare succeeds.
	token1, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("first PrepareFileAnnounce: %v", err)
	}

	// Second prepare on the same hash succeeds (file still exists, pending=1
	// protects it).
	token2, err := m.PrepareFileAnnounce(hash, "file.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("second PrepareFileAnnounce: %v", err)
	}

	// Rollback the first token — this releases its pending and calls
	// RemoveUnreferenced. But token2's pending reservation must protect the file.
	token1.Rollback()

	if _, err := os.Stat(filePath); err != nil {
		t.Fatal("transmit file should still exist — token2's pending reservation protects it")
	}

	// token2 can still commit successfully.
	fileID := domain.FileID("atomic-test")
	recipient := domain.PeerIdentity("receiver-identity-1234567890ab")
	if err := token2.Commit(fileID, recipient); err != nil {
		t.Fatalf("Commit after concurrent Rollback: %v", err)
	}

	if _, ok := m.senderMaps[fileID]; !ok {
		t.Error("sender mapping should exist after successful Commit")
	}
}

// TestPrepareFileAnnounceRejectsDeletedFile verifies that PrepareFileAnnounce
// fails when the transmit blob has been deleted (e.g. by a completed Rollback)
// before the call — the atomic check prevents returning a useless token.
func TestPrepareFileAnnounceRejectsDeletedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	// Do NOT create the file on disk — simulating post-Rollback deletion.

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	_, err = m.PrepareFileAnnounce(hash, "gone.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Fatal("PrepareFileAnnounce should fail when transmit file does not exist")
	}

	m.mu.Lock()
	slots := m.pendingSenderSlots
	m.mu.Unlock()
	if slots != 0 {
		t.Errorf("pendingSenderSlots = %d, want 0 (no reservation placed)", slots)
	}
}

// TestTombstoneCleanupAfterTTL verifies that tickSenderMappings and
// tickReceiverMappings remove expired tombstone and completed entries after
// tombstoneTTL has passed.
func TestTombstoneCleanupAfterTTL(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	expiredTime := time.Now().Add(-(tombstoneTTL + time.Hour))
	recentTime := time.Now().Add(-time.Hour)

	// Expired tombstone — should be cleaned.
	m.senderMaps["expired-tombstone"] = &senderFileMapping{
		FileID:      "expired-tombstone",
		State:       senderTombstone,
		CompletedAt: expiredTime,
	}

	// Expired completed — should be cleaned.
	m.senderMaps["expired-completed"] = &senderFileMapping{
		FileID:      "expired-completed",
		State:       senderCompleted,
		CompletedAt: expiredTime,
	}

	// Recent tombstone — should NOT be cleaned.
	m.senderMaps["recent-tombstone"] = &senderFileMapping{
		FileID:      "recent-tombstone",
		State:       senderTombstone,
		CompletedAt: recentTime,
	}

	// Active mapping — should NOT be cleaned.
	m.senderMaps["active-serving"] = &senderFileMapping{
		FileID:       "active-serving",
		State:        senderServing,
		LastServedAt: time.Now(),
	}

	// Expired completed receiver — should be cleaned.
	m.receiverMaps["expired-recv"] = &receiverFileMapping{
		FileID:      "expired-recv",
		State:       receiverCompleted,
		CompletedAt: expiredTime,
	}

	// Recent completed receiver — should NOT be cleaned.
	m.receiverMaps["recent-recv"] = &receiverFileMapping{
		FileID:      "recent-recv",
		State:       receiverCompleted,
		CompletedAt: recentTime,
	}

	// Expired failed receiver — should be cleaned (same TTL as completed).
	m.receiverMaps["expired-failed"] = &receiverFileMapping{
		FileID:      "expired-failed",
		State:       receiverFailed,
		CompletedAt: expiredTime,
	}

	// Recent failed receiver — should NOT be cleaned.
	m.receiverMaps["recent-failed"] = &receiverFileMapping{
		FileID:      "recent-failed",
		State:       receiverFailed,
		CompletedAt: recentTime,
	}

	m.tickSenderMappings()
	m.tickReceiverMappings()

	// Verify expired entries are gone.
	if _, ok := m.senderMaps["expired-tombstone"]; ok {
		t.Error("expired tombstone should be removed")
	}
	if _, ok := m.senderMaps["expired-completed"]; ok {
		t.Error("expired completed sender should be removed")
	}
	if _, ok := m.receiverMaps["expired-recv"]; ok {
		t.Error("expired completed receiver should be removed")
	}
	if _, ok := m.receiverMaps["expired-failed"]; ok {
		t.Error("expired failed receiver should be removed")
	}

	// Verify recent/active entries remain.
	if _, ok := m.senderMaps["recent-tombstone"]; !ok {
		t.Error("recent tombstone should NOT be removed")
	}
	if _, ok := m.senderMaps["active-serving"]; !ok {
		t.Error("active serving should NOT be removed")
	}
	if _, ok := m.receiverMaps["recent-recv"]; !ok {
		t.Error("recent completed receiver should NOT be removed")
	}
	if _, ok := m.receiverMaps["recent-failed"]; !ok {
		t.Error("recent failed receiver should NOT be removed")
	}
}

// TestTickSenderMappingsTombstoneTTLDoesNotReleaseRef verifies that when
// tickSenderMappings expires a tombstone mapping, it does NOT call Release on
// the store. Tombstones are ref-less by construction: RemoveSenderMapping
// explicitly skips Release for them, and load-time tombstones created for
// missing blobs are inserted without contributing to activeHashes. If the TTL
// path releases a tombstone, it decrements another live mapping's ref for the
// same hash (e.g. a re-send that reintroduced the content), leading to
// premature blob deletion.
func TestTickSenderMappingsTombstoneTTLDoesNotReleaseRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	// Single live ref — owned by the active sender mapping below.
	store.mu.Lock()
	store.refs[hash] = 1
	store.mu.Unlock()

	m := NewFileTransferManager(Config{
		Store:       store,
		DownloadDir: dir,
	})

	expiredTime := time.Now().Add(-(tombstoneTTL + time.Hour))

	// Expired tombstone for the same hash — must NOT release when removed.
	tombstoneID := domain.FileID("expired-tombstone")
	// Active mapping that owns the single live ref — must keep its ref.
	activeID := domain.FileID("active-serving")

	m.mu.Lock()
	m.senderMaps[tombstoneID] = &senderFileMapping{
		FileID:      tombstoneID,
		FileHash:    hash,
		FileName:    "resurrected.txt",
		FileSize:    4,
		Recipient:   "bob",
		State:       senderTombstone,
		CompletedAt: expiredTime,
	}
	m.senderMaps[activeID] = &senderFileMapping{
		FileID:       activeID,
		FileHash:     hash,
		FileName:     "resurrected.txt",
		FileSize:     4,
		Recipient:    "carol",
		State:        senderServing,
		LastServedAt: time.Now(),
	}
	m.mu.Unlock()

	m.tickSenderMappings()

	// Expired tombstone must be removed from the map.
	m.mu.Lock()
	_, tombstoneStillPresent := m.senderMaps[tombstoneID]
	_, activeStillPresent := m.senderMaps[activeID]
	m.mu.Unlock()

	if tombstoneStillPresent {
		t.Error("expired tombstone should have been removed by tickSenderMappings")
	}
	if !activeStillPresent {
		t.Error("active serving mapping must NOT be removed")
	}

	// Ref count must remain 1 — the tombstone never owned a ref, so its
	// expiry must not decrement the live mapping's ref.
	store.mu.Lock()
	refCount := store.refs[hash]
	store.mu.Unlock()
	if refCount != 1 {
		t.Fatalf("ref count should be 1 (tombstone TTL must not release), got %d", refCount)
	}

	// Blob must still exist on disk — the live mapping still needs it.
	if _, err := os.Stat(filepath.Join(dir, hash+".bin")); os.IsNotExist(err) {
		t.Fatal("blob should survive — active mapping still holds a ref")
	}
}

// TestTickSenderMappingsCompletedTTLReleasesRef verifies that tickSenderMappings
// DOES release the ref for an expired senderCompleted mapping. Completed
// mappings own a transmit ref (they held the blob until recipient ack), and
// the TTL expiry path must return that ref to the store so the blob can be
// deleted when no other mapping holds it.
func TestTickSenderMappingsCompletedTTLReleasesRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc3333"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 1
	store.mu.Unlock()

	m := NewFileTransferManager(Config{
		Store:       store,
		DownloadDir: dir,
	})

	expiredTime := time.Now().Add(-(tombstoneTTL + time.Hour))
	completedID := domain.FileID("expired-completed")

	m.mu.Lock()
	m.senderMaps[completedID] = &senderFileMapping{
		FileID:      completedID,
		FileHash:    hash,
		FileName:    "done.txt",
		FileSize:    4,
		Recipient:   "bob",
		State:       senderCompleted,
		CompletedAt: expiredTime,
	}
	m.mu.Unlock()

	m.tickSenderMappings()

	// Ref must be released → 0 → blob deleted.
	store.mu.Lock()
	refCount := store.refs[hash]
	store.mu.Unlock()
	if refCount != 0 {
		t.Fatalf("ref count should be 0 (completed TTL must release), got %d", refCount)
	}

	if _, err := os.Stat(filepath.Join(dir, hash+".bin")); !os.IsNotExist(err) {
		t.Fatal("blob should be deleted after completed mapping TTL expiry released the last ref")
	}
}

// TestTickSenderMappingsStallReclaimRestoresPreServeStateForReDownload
// verifies that when a stalled senderServing slot is reclaimed, the mapping
// is restored to its pre-serve state — senderCompleted for a re-download
// whose serving run originated from senderCompleted — not unconditionally
// downgraded to senderAnnounced. Downgrading would visibly change
// semantics: the mapping would start counting against the live sender
// quota, reappear in active snapshots/UI, and lose the information that
// the original transfer had already completed.
func TestTickSenderMappingsStallReclaimRestoresPreServeStateForReDownload(t *testing.T) {
	t.Parallel()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("re-download-stalled")
	// A re-download in progress: the mapping is senderServing now, and
	// PreServeState records that it was senderCompleted before the current
	// chunk_request promoted it. LastServedAt is well beyond the stall
	// timeout so tickSenderMappings will reclaim the slot.
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:        fileID,
		FileHash:      "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222",
		FileName:      "photo.jpg",
		FileSize:      1024,
		Recipient:     domain.PeerIdentity("bob"),
		State:         senderServing,
		PreServeState: senderCompleted,
		CompletedAt:   time.Now().Add(-time.Hour), // original completion
		LastServedAt:  time.Now().Add(-2 * senderServingStallTimeout),
	}

	m.tickSenderMappings()

	sm := m.senderMaps[fileID]
	if sm == nil {
		t.Fatal("mapping must not be removed on reclaim")
	}
	if sm.State != senderCompleted {
		t.Errorf("State = %s, want senderCompleted (re-download must return to completed, not announced)", sm.State)
	}
	if sm.PreServeState != "" {
		t.Errorf("PreServeState = %s, want empty after reclaim (next chunk_request re-captures origin)", sm.PreServeState)
	}
}

// TestTickSenderMappingsStallReclaimFirstDownloadFallsBackToAnnounced
// verifies the preserved behaviour for first-time downloads: when a
// stalled serving slot has PreServeState == senderAnnounced (set by
// validateChunkRequest on promotion from announced), reclaim restores
// senderAnnounced. This is the historical behaviour and remains the
// correct one for first-time downloads.
func TestTickSenderMappingsStallReclaimFirstDownloadFallsBackToAnnounced(t *testing.T) {
	t.Parallel()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("first-download-stalled")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:        fileID,
		FileHash:      "bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc3333",
		FileName:      "first.bin",
		FileSize:      1024,
		Recipient:     domain.PeerIdentity("bob"),
		State:         senderServing,
		PreServeState: senderAnnounced,
		LastServedAt:  time.Now().Add(-2 * senderServingStallTimeout),
	}

	m.tickSenderMappings()

	sm := m.senderMaps[fileID]
	if sm == nil {
		t.Fatal("mapping must not be removed on reclaim")
	}
	if sm.State != senderAnnounced {
		t.Errorf("State = %s, want senderAnnounced", sm.State)
	}
	if sm.PreServeState != "" {
		t.Errorf("PreServeState = %s, want empty", sm.PreServeState)
	}
}

// TestTickSenderMappingsStallReclaimEmptyPreServeStateDefaultsToAnnounced
// verifies backwards compatibility: a senderServing mapping loaded from
// older persisted JSON will have an empty PreServeState. Reclaim must
// fall back to senderAnnounced rather than leaving the mapping in an
// invalid empty-state value.
func TestTickSenderMappingsStallReclaimEmptyPreServeStateDefaultsToAnnounced(t *testing.T) {
	t.Parallel()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("legacy-stalled")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:       fileID,
		FileHash:     "cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc3333dddd4444",
		FileName:     "legacy.bin",
		FileSize:     1024,
		Recipient:    domain.PeerIdentity("bob"),
		State:        senderServing,
		// PreServeState intentionally empty (simulates legacy JSON)
		LastServedAt: time.Now().Add(-2 * senderServingStallTimeout),
	}

	m.tickSenderMappings()

	sm := m.senderMaps[fileID]
	if sm.State != senderAnnounced {
		t.Errorf("State = %s, want senderAnnounced (legacy fallback)", sm.State)
	}
}

// TestCommitDuplicateFileIDRejected verifies that committing a token with
// a FileID that already exists in senderMaps returns an error.
func TestCommitDuplicateFileIDRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(dir+"/"+hash+".bin", []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("dup-file-id")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID: fileID,
		State:  senderAnnounced,
	}

	token, err := m.PrepareFileAnnounce(hash, "name.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce: %v", err)
	}
	defer token.Rollback()

	if err := token.Commit(fileID, domain.PeerIdentity("receiver")); err == nil {
		t.Error("Commit with duplicate FileID should fail")
	}
}

// TestHandleChunkRequestRejectsNilStore verifies that HandleChunkRequest
// does not panic when the transmit store is nil but sender mappings exist
// (e.g. loaded from persisted state).
func TestHandleChunkRequestRejectsNilStore(t *testing.T) {
	t.Parallel()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
		// store intentionally nil
	}

	fileID := domain.FileID("persisted-sender")
	recipient := domain.PeerIdentity("recipient-1234567890abcdef")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
		FileName:  "test.bin",
		FileSize:  1000,
		Recipient: recipient,
		State:     senderAnnounced,
	}

	// Must not panic.
	m.HandleChunkRequest(recipient, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   512,
	})
}

// TestRegisterFileReceiveIdempotent verifies that calling RegisterFileReceive
// twice with the same FileID is a no-op (does not overwrite existing mapping).
func TestRegisterFileReceiveIdempotent(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("idempotent-recv")
	sender := domain.PeerIdentity("sender-1234567890abcdef12345678")
	hash1 := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"
	hash2 := "f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8"

	if err := m.RegisterFileReceive(fileID, hash1, "file.bin", "text/plain", 5000, sender); err != nil {
		t.Fatalf("first RegisterFileReceive: %v", err)
	}

	// Set state to downloading to track if it gets overwritten.
	m.receiverMaps[fileID].State = receiverDownloading

	// Second call should be a no-op.
	if err := m.RegisterFileReceive(fileID, hash2, "file2.bin", "text/html", 9000, sender); err != nil {
		t.Fatalf("second RegisterFileReceive (idempotent): %v", err)
	}

	if m.receiverMaps[fileID].FileHash != hash1 {
		t.Error("second RegisterFileReceive should not overwrite existing mapping")
	}
	if m.receiverMaps[fileID].State != receiverDownloading {
		t.Error("state should remain downloading")
	}
}

// TestMaxReceiverMappingsRejectsWhenFull verifies that RegisterFileReceive
// returns an error when the receiver mapping count reaches maxFileMappings.
func TestMaxReceiverMappingsRejectsWhenFull(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	// Fill receiver maps to the limit.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("recv-%d", i))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:   fid,
			FileHash: fmt.Sprintf("%064x", i),
			FileName: fmt.Sprintf("file-%d.bin", i),
			FileSize: 1000,
			Sender:   sender,
			State:    receiverAvailable,
		}
	}

	// Attempt to register one more — should fail.
	err := m.RegisterFileReceive(
		domain.FileID("overflow-recv"),
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"overflow.bin", "application/octet-stream",
		2000, sender,
	)
	if err == nil {
		t.Error("RegisterFileReceive should fail when at maxFileMappings limit")
	}

	// Verify the overflow entry was NOT added.
	if _, exists := m.receiverMaps[domain.FileID("overflow-recv")]; exists {
		t.Error("overflow mapping should not be inserted")
	}
}

// TestMaxReceiverMappingsAllowsBelowLimit verifies that RegisterFileReceive
// succeeds when below the limit.
func TestMaxReceiverMappingsAllowsBelowLimit(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	hash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	err := m.RegisterFileReceive(
		domain.FileID("new-recv"),
		hash, "test.bin", "application/octet-stream",
		5000, sender,
	)
	if err != nil {
		t.Fatalf("RegisterFileReceive should succeed below limit: %v", err)
	}

	rm, exists := m.receiverMaps[domain.FileID("new-recv")]
	if !exists {
		t.Fatal("receiver mapping should be created")
	}
	if rm.State != receiverAvailable {
		t.Errorf("state = %s, want available", rm.State)
	}
}

// TestForceRetryChunkResetsRetryCounter verifies that ForceRetryChunk sends
// an immediate chunk_request and resets the retry counter.
func TestForceRetryChunkResetsRetryCounter(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("force-retry-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:       fileID,
		Sender:       sender,
		State:        receiverDownloading,
		NextOffset:   50000,
		ChunkSize:    domain.DefaultChunkSize,
		ChunkRetries: 5,
	}

	err := m.ForceRetryChunk(fileID)
	if err != nil {
		t.Fatalf("ForceRetryChunk: %v", err)
	}

	if !chunkRequested {
		t.Error("force retry should send chunk_request immediately")
	}

	if m.receiverMaps[fileID].ChunkRetries != 0 {
		t.Errorf("ChunkRetries = %d, want 0 (reset)", m.receiverMaps[fileID].ChunkRetries)
	}
}

// TestForceRetryChunkRejectsUnreachableSender verifies that ForceRetryChunk
// returns an error and transitions to waiting_route when the sender is offline,
// consistent with the automatic retry guard in tickReceiverMappings.
func TestForceRetryChunkRejectsUnreachableSender(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false // sender is offline
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("force-retry-unreachable")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:       fileID,
		Sender:       sender,
		State:        receiverDownloading,
		NextOffset:   50000,
		ChunkSize:    domain.DefaultChunkSize,
		ChunkRetries: 2,
	}

	err := m.ForceRetryChunk(fileID)
	if err == nil {
		t.Fatal("ForceRetryChunk should fail when sender is unreachable")
	}

	if chunkRequested {
		t.Error("no chunk_request should be sent to an unreachable sender")
	}

	if m.receiverMaps[fileID].State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route", m.receiverMaps[fileID].State)
	}
}

// TestForceRetryChunkSucceedsWhenSenderReachable verifies that ForceRetryChunk
// proceeds normally when peerReachable returns true.
func TestForceRetryChunkSucceedsWhenSenderReachable(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true // sender is online
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("force-retry-reachable")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:       fileID,
		Sender:       sender,
		State:        receiverDownloading,
		NextOffset:   50000,
		ChunkSize:    domain.DefaultChunkSize,
		ChunkRetries: 3,
	}

	err := m.ForceRetryChunk(fileID)
	if err != nil {
		t.Fatalf("ForceRetryChunk: %v", err)
	}

	if !chunkRequested {
		t.Error("force retry should send chunk_request when sender is reachable")
	}

	if m.receiverMaps[fileID].ChunkRetries != 0 {
		t.Errorf("ChunkRetries = %d, want 0 (reset)", m.receiverMaps[fileID].ChunkRetries)
	}
}

// TestForceRetryChunkResumesFromWaitingRoute verifies that ForceRetryChunk
// can resume a transfer paused in waiting_route when the sender is reachable
// again, mirroring the auto-resume logic in tickReceiverMappings.
func TestForceRetryChunkResumesFromWaitingRoute(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("waiting-route-resume")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:       fileID,
		Sender:       sender,
		State:        receiverWaitingRoute,
		NextOffset:   80000,
		ChunkSize:    domain.DefaultChunkSize,
		ChunkRetries: 7,
	}

	err := m.ForceRetryChunk(fileID)
	if err != nil {
		t.Fatalf("ForceRetryChunk from waiting_route: %v", err)
	}

	if !chunkRequested {
		t.Error("force retry should send chunk_request after resuming from waiting_route")
	}

	if m.receiverMaps[fileID].State != receiverDownloading {
		t.Errorf("state = %s, want downloading", m.receiverMaps[fileID].State)
	}

	if m.receiverMaps[fileID].ChunkRetries != 0 {
		t.Errorf("ChunkRetries = %d, want 0 (reset on resume)", m.receiverMaps[fileID].ChunkRetries)
	}
}

// TestForceRetryChunkWaitingRouteSenderStillOffline verifies that
// ForceRetryChunk on a waiting_route transfer returns an error and keeps
// the state as waiting_route when the sender is still unreachable.
func TestForceRetryChunkWaitingRouteSenderStillOffline(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("waiting-route-still-offline")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		Sender:     sender,
		State:      receiverWaitingRoute,
		NextOffset: 80000,
		ChunkSize:  domain.DefaultChunkSize,
	}

	err := m.ForceRetryChunk(fileID)
	if err == nil {
		t.Fatal("ForceRetryChunk should fail when sender is still unreachable")
	}

	if chunkRequested {
		t.Error("no chunk_request should be sent to an unreachable sender")
	}

	if m.receiverMaps[fileID].State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route (unchanged)", m.receiverMaps[fileID].State)
	}
}

// TestStartDownloadIgnoresOtherActiveDownloads verifies that StartDownload
// proceeds even when several other receiver mappings are already in the
// downloading state. The receiver no longer enforces a concurrent download
// cap — parallelism is the user's choice.
func TestStartDownloadIgnoresOtherActiveDownloads(t *testing.T) {
	dir := t.TempDir()

	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	// Several already-active downloads must NOT block a fresh start.
	for i := 0; i < 4; i++ {
		fid := domain.FileID(fmt.Sprintf("active-download-%d", i))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:     fid,
			Sender:     domain.PeerIdentity(fmt.Sprintf("sender-%d", i)),
			State:      receiverDownloading,
			NextOffset: 0,
			ChunkSize:  domain.DefaultChunkSize,
		}
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("fresh-start-with-others-active")
	validHash := "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  validHash,
		FileName:  "fresh.bin",
		FileSize:  100000,
		Sender:    sender,
		State:     receiverAvailable,
		ChunkSize: domain.DefaultChunkSize,
		CreatedAt: time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	if !chunkRequested {
		t.Error("StartDownload should send chunk_request even when other downloads are active")
	}

	if m.receiverMaps[fileID].State != receiverDownloading {
		t.Errorf("state = %s, want downloading", m.receiverMaps[fileID].State)
	}
}

// TestForceRetryChunkWaitingRouteIgnoresOtherActiveDownloads verifies that
// ForceRetryChunk resumes a waiting_route transfer regardless of how many
// other downloads are already active. The receiver no longer enforces a
// concurrent download cap — parallelism is the user's choice.
func TestForceRetryChunkWaitingRouteIgnoresOtherActiveDownloads(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkRequested = true
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	// Several already-active downloads must NOT block resume.
	for i := 0; i < 4; i++ {
		fid := domain.FileID(fmt.Sprintf("active-download-%d", i))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:     fid,
			Sender:     domain.PeerIdentity(fmt.Sprintf("sender-%d", i)),
			State:      receiverDownloading,
			NextOffset: 0,
			ChunkSize:  domain.DefaultChunkSize,
		}
	}

	fileID := domain.FileID("waiting-route-resume")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		Sender:     sender,
		State:      receiverWaitingRoute,
		NextOffset: 50000,
		ChunkSize:  domain.DefaultChunkSize,
	}

	if err := m.ForceRetryChunk(fileID); err != nil {
		t.Fatalf("ForceRetryChunk: %v", err)
	}

	if !chunkRequested {
		t.Error("force retry should send chunk_request even when other downloads are active")
	}

	if m.receiverMaps[fileID].State != receiverDownloading {
		t.Errorf("state = %s, want downloading", m.receiverMaps[fileID].State)
	}
}

// TestForceRetryChunkRejectsIncompatibleState verifies that ForceRetryChunk
// rejects transfers in states other than downloading or waiting_route.
func TestForceRetryChunkRejectsIncompatibleState(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	incompatible := []receiverState{
		receiverAvailable,
		receiverVerifying,
		receiverWaitingAck,
		receiverCompleted,
		receiverFailed,
	}

	for _, state := range incompatible {
		fid := domain.FileID("state-" + string(state))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID: fid,
			Sender: domain.PeerIdentity("sender"),
			State:  state,
		}

		err := m.ForceRetryChunk(fid)
		if err == nil {
			t.Errorf("ForceRetryChunk should reject state %s", state)
		}
	}
}

// TestForceRetryChunkRollsBackOnSendFailureFromWaitingRoute verifies that
// when ForceRetryChunk resumes a waiting_route transfer but the send fails,
// the state is rolled back to waiting_route instead of being stranded in
// downloading with no request in flight.
func TestForceRetryChunkRollsBackOnSendFailureFromWaitingRoute(t *testing.T) {
	downloadDir := createPartialDir(t)
	fileID := domain.FileID("rollback-waiting-route")

	// Create a valid partial file so offset is preserved through the
	// partial file sanity check in prepareResumeLocked.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 50000), 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		Sender:        sender,
		State:         receiverWaitingRoute,
		FileSize:      100000, // must be >= partial file size to avoid oversized .part reset
		NextOffset:    50000,
		BytesReceived: 50000,
		ChunkSize:     domain.DefaultChunkSize,
	}

	err := m.ForceRetryChunk(fileID)
	if err == nil {
		t.Fatal("ForceRetryChunk should fail when send fails")
	}

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route (rolled back)", rm.State)
	}
	if rm.NextOffset != 50000 {
		t.Errorf("NextOffset = %d, want 50000 (preserved)", rm.NextOffset)
	}
}

// TestForceRetryChunkRollsBackOnSendFailureFromDownloading verifies that
// when ForceRetryChunk fails the send for an already-downloading transfer,
// the state stays downloading (it was already downloading before).
func TestForceRetryChunkRollsBackOnSendFailureFromDownloading(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("rollback-downloading")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		Sender:        sender,
		State:         receiverDownloading,
		NextOffset:    50000,
		BytesReceived: 50000,
		ChunkSize:     domain.DefaultChunkSize,
	}

	err := m.ForceRetryChunk(fileID)
	if err == nil {
		t.Fatal("ForceRetryChunk should fail when send fails")
	}

	rm := m.receiverMaps[fileID]
	if rm.State != receiverDownloading {
		t.Errorf("state = %s, want downloading (was already downloading)", rm.State)
	}
}

// ---------------------------------------------------------------------------
// P2: rejected receiver mapping must not appear as downloadable
// ---------------------------------------------------------------------------

// TestRejectedReceiverMappingNotDownloadable verifies that when
// RegisterFileReceive fails (e.g. receiver quota full), ReceiverProgress
// returns found=false and StartDownload returns an error for that fileID.
// This is the regression test for the P2 bug where a ghost file-card
// appeared in the UI for a rejected announce.
func TestRejectedReceiverMappingNotDownloadable(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	// Fill receiver maps to the limit so the next registration is rejected.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("fill-%d", i))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:   fid,
			FileHash: fmt.Sprintf("%064x", i),
			FileName: fmt.Sprintf("file-%d.bin", i),
			FileSize: 1000,
			Sender:   sender,
			State:    receiverAvailable,
		}
	}

	// Attempt to register a new receiver mapping — must fail.
	rejectedFileID := domain.FileID("rejected-file-announce")
	err := m.RegisterFileReceive(
		rejectedFileID,
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"secret.bin", "application/octet-stream",
		50000, sender,
	)
	if err == nil {
		t.Fatal("RegisterFileReceive should fail when receiver mapping limit is reached")
	}

	// ReceiverProgress must return found=false for the rejected fileID.
	_, _, _, found := m.ReceiverProgress(rejectedFileID)
	if found {
		t.Error("ReceiverProgress should return found=false for a rejected receiver mapping")
	}

	// StartDownload must return an error for the rejected fileID.
	dlErr := m.StartDownload(rejectedFileID)
	if dlErr == nil {
		t.Error("StartDownload should fail for a fileID with no receiver mapping")
	}
}

// TestRejectedReceiverMappingInvalidMetadata verifies that when
// RegisterFileReceive rejects the announce due to invalid metadata
// (e.g. zero file size), the fileID is not downloadable.
// TestCompletedSenderMappingsDoNotBlockQuota verifies that sender mappings
// in terminal states (completed, tombstone) do not count toward the
// maxFileMappings quota, allowing new file announces even when many
// finished transfers are retained for dedup.
func TestCompletedSenderMappingsDoNotBlockQuota(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	now := time.Now()

	// Fill ALL slots with terminal-state mappings.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("done-%d", i))
		state := senderCompleted
		if i%2 == 0 {
			state = senderTombstone
		}
		m.senderMaps[fid] = &senderFileMapping{
			FileID:      fid,
			FileHash:    fmt.Sprintf("hash-%d", i),
			FileName:    fmt.Sprintf("file-%d.bin", i),
			FileSize:    1000,
			Recipient:   receiver,
			State:       state,
			CreatedAt:   now,
			CompletedAt: now,
		}
	}

	// Despite len(senderMaps) == maxFileMappings, a new announce must succeed
	// because none of the existing mappings are active.
	token, err := m.PrepareFileAnnounce(hash, "new.bin", "application/octet-stream", 1000)
	if err != nil {
		t.Fatalf("PrepareFileAnnounce should succeed when all existing mappings are terminal: %v", err)
	}
	token.Rollback()

	// Verify that active mappings DO block when at capacity.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("active-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("ahash-%d", i),
			FileName:  fmt.Sprintf("afile-%d.bin", i),
			FileSize:  1000,
			Recipient: receiver,
			State:     senderAnnounced,
			CreatedAt: now,
		}
	}

	_, err = m.PrepareFileAnnounce(hash, "overflow.bin", "application/octet-stream", 1000)
	if err == nil {
		t.Fatal("PrepareFileAnnounce should fail when active mappings are at capacity")
	}
}

// TestActiveSenderCountLocked verifies that the helper counts only
// announced and serving states, ignoring completed and tombstone.
func TestActiveSenderCountLocked(t *testing.T) {
	m := &Manager{
		senderMaps: map[domain.FileID]*senderFileMapping{
			"a": {State: senderAnnounced},
			"s": {State: senderServing},
			"c": {State: senderCompleted},
			"t": {State: senderTombstone},
		},
		stopCh: make(chan struct{}),
	}

	m.mu.Lock()
	got := m.activeSenderCountLocked()
	m.mu.Unlock()

	if got != 2 {
		t.Fatalf("activeSenderCountLocked: expected 2 (announced+serving), got %d", got)
	}
}

// TestFailedReceiverMappingsDoNotBlockQuota verifies that receiver mappings
// in terminal states (completed, failed) do not count toward the
// maxFileMappings quota for incoming file announces.
func TestFailedReceiverMappingsDoNotBlockQuota(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	now := time.Now()

	// Fill ALL slots with terminal-state receiver mappings.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("term-%d", i))
		state := receiverCompleted
		if i%2 == 0 {
			state = receiverFailed
		}
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:      fid,
			FileHash:    fmt.Sprintf("%064x", i),
			FileName:    fmt.Sprintf("file-%d.bin", i),
			FileSize:    1000,
			Sender:      sender,
			State:       state,
			CompletedAt: now,
		}
	}

	// Despite len(receiverMaps) == maxFileMappings, a new announce must succeed
	// because none of the existing mappings are active.
	err := m.RegisterFileReceive(
		domain.FileID("fresh-recv"),
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"fresh.bin", "application/octet-stream",
		2000, sender,
	)
	if err != nil {
		t.Fatalf("RegisterFileReceive should succeed when all existing mappings are terminal: %v", err)
	}

	// Verify that active mappings DO block when at capacity.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("active-%d", i))
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:   fid,
			FileHash: fmt.Sprintf("%064x", i+maxFileMappings),
			FileName: fmt.Sprintf("active-%d.bin", i),
			FileSize: 1000,
			Sender:   sender,
			State:    receiverAvailable,
		}
	}

	err = m.RegisterFileReceive(
		domain.FileID("overflow-recv"),
		"c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
		"overflow.bin", "application/octet-stream",
		2000, sender,
	)
	if err == nil {
		t.Fatal("RegisterFileReceive should fail when active mappings are at capacity")
	}
}

// TestActiveReceiverCountLocked verifies that the helper counts only
// non-terminal states, ignoring completed and failed.
func TestActiveReceiverCountLocked(t *testing.T) {
	m := &Manager{
		receiverMaps: map[domain.FileID]*receiverFileMapping{
			"avail":  {State: receiverAvailable},
			"dl":     {State: receiverDownloading},
			"verify": {State: receiverVerifying},
			"wack":   {State: receiverWaitingAck},
			"wroute": {State: receiverWaitingRoute},
			"done":   {State: receiverCompleted},
			"failed": {State: receiverFailed},
		},
		stopCh: make(chan struct{}),
	}

	m.mu.Lock()
	got := m.activeReceiverCountLocked()
	m.mu.Unlock()

	// 5 active (available, downloading, verifying, waitingAck, waitingRoute)
	// 2 terminal (completed, failed) — excluded
	if got != 5 {
		t.Fatalf("activeReceiverCountLocked: expected 5, got %d", got)
	}
}

func TestRejectedReceiverMappingInvalidMetadata(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	rejectedFileID := domain.FileID("invalid-metadata-file")

	// Zero file size → rejected.
	err := m.RegisterFileReceive(
		rejectedFileID,
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"secret.bin", "application/octet-stream",
		0, sender,
	)
	if err == nil {
		t.Fatal("RegisterFileReceive should fail for zero file size")
	}

	_, _, _, found := m.ReceiverProgress(rejectedFileID)
	if found {
		t.Error("ReceiverProgress should return found=false for rejected metadata")
	}

	dlErr := m.StartDownload(rejectedFileID)
	if dlErr == nil {
		t.Error("StartDownload should fail for a fileID with no receiver mapping")
	}
}

// ---------------------------------------------------------------------------
// TransfersSnapshot / MappingsSnapshot — terminal filtering
// ---------------------------------------------------------------------------

// TestTransfersSnapshotExcludesTerminalSenders verifies that TransfersSnapshot
// does not return sender mappings in completed or tombstone state.
func TestTransfersSnapshotExcludesTerminalSenders(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	now := time.Now()
	peer := domain.PeerIdentity("peer-snap-sender-1234567890abcd")

	// One active (announced), one active (serving), one terminal (completed),
	// one terminal (tombstone).
	m.senderMaps[domain.FileID("s-announced")] = &senderFileMapping{
		FileID: "s-announced", State: senderAnnounced, Recipient: peer, CreatedAt: now,
	}
	m.senderMaps[domain.FileID("s-serving")] = &senderFileMapping{
		FileID: "s-serving", State: senderServing, Recipient: peer, CreatedAt: now,
	}
	m.senderMaps[domain.FileID("s-completed")] = &senderFileMapping{
		FileID: "s-completed", State: senderCompleted, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}
	m.senderMaps[domain.FileID("s-tombstone")] = &senderFileMapping{
		FileID: "s-tombstone", State: senderTombstone, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}

	entries := m.TransfersSnapshot()
	if len(entries) != 2 {
		t.Fatalf("TransfersSnapshot: got %d entries, want 2 (only active senders)", len(entries))
	}

	seen := map[string]bool{}
	for _, e := range entries {
		seen[string(e.FileID)] = true
		if e.Direction != "send" {
			t.Errorf("unexpected direction %q for sender entry %s", e.Direction, e.FileID)
		}
	}
	if !seen["s-announced"] {
		t.Error("TransfersSnapshot missing announced sender")
	}
	if !seen["s-serving"] {
		t.Error("TransfersSnapshot missing serving sender")
	}
}

// TestTransfersSnapshotExcludesTerminalReceivers verifies that TransfersSnapshot
// does not return receiver mappings in completed or failed state.
func TestTransfersSnapshotExcludesTerminalReceivers(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	now := time.Now()
	sender := domain.PeerIdentity("peer-snap-recv-1234567890abcd")

	// Active states.
	m.receiverMaps[domain.FileID("r-available")] = &receiverFileMapping{
		FileID: "r-available", State: receiverAvailable, Sender: sender, CreatedAt: now,
	}
	m.receiverMaps[domain.FileID("r-downloading")] = &receiverFileMapping{
		FileID: "r-downloading", State: receiverDownloading, Sender: sender, CreatedAt: now,
	}
	m.receiverMaps[domain.FileID("r-verifying")] = &receiverFileMapping{
		FileID: "r-verifying", State: receiverVerifying, Sender: sender, CreatedAt: now,
	}
	m.receiverMaps[domain.FileID("r-waiting-ack")] = &receiverFileMapping{
		FileID: "r-waiting-ack", State: receiverWaitingAck, Sender: sender, CreatedAt: now,
	}
	m.receiverMaps[domain.FileID("r-waiting-route")] = &receiverFileMapping{
		FileID: "r-waiting-route", State: receiverWaitingRoute, Sender: sender, CreatedAt: now,
	}

	// Terminal states.
	m.receiverMaps[domain.FileID("r-completed")] = &receiverFileMapping{
		FileID: "r-completed", State: receiverCompleted, Sender: sender, CreatedAt: now, CompletedAt: now,
	}
	m.receiverMaps[domain.FileID("r-failed")] = &receiverFileMapping{
		FileID: "r-failed", State: receiverFailed, Sender: sender, CreatedAt: now, CompletedAt: now,
	}

	entries := m.TransfersSnapshot()
	if len(entries) != 5 {
		t.Fatalf("TransfersSnapshot: got %d entries, want 5 (only active receivers)", len(entries))
	}

	for _, e := range entries {
		if e.State == string(receiverCompleted) || e.State == string(receiverFailed) {
			t.Errorf("TransfersSnapshot returned terminal receiver state %q for %s", e.State, e.FileID)
		}
	}
}

// TestTransfersSnapshotMixedSenderReceiver verifies filtering when both
// sender and receiver mappings are present in active and terminal states.
func TestTransfersSnapshotMixedSenderReceiver(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	now := time.Now()
	peer := domain.PeerIdentity("peer-snap-mixed-123456789abcde")

	m.senderMaps[domain.FileID("s-active")] = &senderFileMapping{
		FileID: "s-active", State: senderAnnounced, Recipient: peer, CreatedAt: now,
	}
	m.senderMaps[domain.FileID("s-done")] = &senderFileMapping{
		FileID: "s-done", State: senderCompleted, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}
	m.receiverMaps[domain.FileID("r-active")] = &receiverFileMapping{
		FileID: "r-active", State: receiverDownloading, Sender: peer, CreatedAt: now,
	}
	m.receiverMaps[domain.FileID("r-done")] = &receiverFileMapping{
		FileID: "r-done", State: receiverFailed, Sender: peer, CreatedAt: now, CompletedAt: now,
	}

	entries := m.TransfersSnapshot()
	if len(entries) != 2 {
		t.Fatalf("TransfersSnapshot mixed: got %d entries, want 2", len(entries))
	}

	seen := map[string]bool{}
	for _, e := range entries {
		seen[string(e.FileID)] = true
	}
	if !seen["s-active"] || !seen["r-active"] {
		t.Errorf("TransfersSnapshot missing active entries, seen: %v", seen)
	}
}

// TestMappingsSnapshotExcludesTerminalSenders verifies that MappingsSnapshot
// does not return sender mappings in completed or tombstone state.
func TestMappingsSnapshotExcludesTerminalSenders(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	now := time.Now()
	peer := domain.PeerIdentity("peer-mapping-snap-12345678abcdef")

	m.senderMaps[domain.FileID("m-announced")] = &senderFileMapping{
		FileID: "m-announced", State: senderAnnounced, Recipient: peer, CreatedAt: now,
	}
	m.senderMaps[domain.FileID("m-serving")] = &senderFileMapping{
		FileID: "m-serving", State: senderServing, Recipient: peer, CreatedAt: now,
	}
	m.senderMaps[domain.FileID("m-completed")] = &senderFileMapping{
		FileID: "m-completed", State: senderCompleted, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}
	m.senderMaps[domain.FileID("m-tombstone")] = &senderFileMapping{
		FileID: "m-tombstone", State: senderTombstone, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}

	entries := m.MappingsSnapshot()
	if len(entries) != 2 {
		t.Fatalf("MappingsSnapshot: got %d entries, want 2 (only active senders)", len(entries))
	}

	seen := map[string]bool{}
	for _, e := range entries {
		seen[string(e.FileID)] = true
	}
	if !seen["m-announced"] || !seen["m-serving"] {
		t.Errorf("MappingsSnapshot missing active entries, seen: %v", seen)
	}
}

// TestTransfersSnapshotEmptyWhenAllTerminal verifies that TransfersSnapshot
// returns an empty slice when all mappings are in terminal states.
func TestTransfersSnapshotEmptyWhenAllTerminal(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
		stopCh:       make(chan struct{}),
	}

	now := time.Now()
	peer := domain.PeerIdentity("peer-all-terminal-12345678abcdef")

	m.senderMaps[domain.FileID("t-completed")] = &senderFileMapping{
		FileID: "t-completed", State: senderCompleted, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}
	m.senderMaps[domain.FileID("t-tombstone")] = &senderFileMapping{
		FileID: "t-tombstone", State: senderTombstone, Recipient: peer, CreatedAt: now, CompletedAt: now,
	}
	m.receiverMaps[domain.FileID("t-recv-done")] = &receiverFileMapping{
		FileID: "t-recv-done", State: receiverCompleted, Sender: peer, CreatedAt: now, CompletedAt: now,
	}
	m.receiverMaps[domain.FileID("t-recv-fail")] = &receiverFileMapping{
		FileID: "t-recv-fail", State: receiverFailed, Sender: peer, CreatedAt: now, CompletedAt: now,
	}

	entries := m.TransfersSnapshot()
	if len(entries) != 0 {
		t.Fatalf("TransfersSnapshot: got %d entries, want 0 (all terminal)", len(entries))
	}
}

// TestIsSenderTerminal verifies the terminal classification helper.
func TestIsSenderTerminal(t *testing.T) {
	cases := []struct {
		state    senderState
		terminal bool
	}{
		{senderAnnounced, false},
		{senderServing, false},
		{senderCompleted, true},
		{senderTombstone, true},
	}
	for _, tc := range cases {
		if got := isSenderTerminal(tc.state); got != tc.terminal {
			t.Errorf("isSenderTerminal(%q) = %v, want %v", tc.state, got, tc.terminal)
		}
	}
}

// TestIsReceiverTerminal verifies the terminal classification helper.
func TestIsReceiverTerminal(t *testing.T) {
	cases := []struct {
		state    receiverState
		terminal bool
	}{
		{receiverAvailable, false},
		{receiverDownloading, false},
		{receiverVerifying, false},
		{receiverWaitingAck, false},
		{receiverWaitingRoute, false},
		{receiverCompleted, true},
		{receiverFailed, true},
	}
	for _, tc := range cases {
		if got := isReceiverTerminal(tc.state); got != tc.terminal {
			t.Errorf("isReceiverTerminal(%q) = %v, want %v", tc.state, got, tc.terminal)
		}
	}
}

// TestRemoveSenderMappingTargeted verifies that RemoveSenderMapping removes
// only the specified mapping, leaving other sender and receiver mappings
// intact. This is critical for the post-Commit peerGen race fix — the stale
// goroutine must clean up only its own orphaned mapping, not destroy
// legitimate transfers for the same peer in a newer generation.
func TestRemoveSenderMappingTargeted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 0
	store.mu.Unlock()

	m := NewFileTransferManager(Config{
		Store:       store,
		DownloadDir: dir,
	})

	peer := domain.PeerIdentity("alice")
	orphanedID := domain.FileID("orphaned-msg-1")
	legitimateID := domain.FileID("legit-msg-2")

	// Manually insert two sender mappings for the same peer.
	m.mu.Lock()
	m.senderMaps[orphanedID] = &senderFileMapping{
		FileID:    orphanedID,
		FileHash:  hash,
		FileName:  "orphaned.txt",
		FileSize:  100,
		Recipient: peer,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}
	m.senderMaps[legitimateID] = &senderFileMapping{
		FileID:    legitimateID,
		FileHash:  hash,
		FileName:  "legit.txt",
		FileSize:  200,
		Recipient: peer,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}
	// Acquire refs for both mappings (Commit would do this).
	_ = store.Acquire(hash)
	_ = store.Acquire(hash)
	m.mu.Unlock()

	// Remove only the orphaned mapping.
	removed := m.RemoveSenderMapping(orphanedID)
	if !removed {
		t.Fatal("RemoveSenderMapping must return true for existing mapping")
	}

	// Orphaned mapping must be gone.
	m.mu.Lock()
	_, orphanExists := m.senderMaps[orphanedID]
	_, legitExists := m.senderMaps[legitimateID]
	m.mu.Unlock()

	if orphanExists {
		t.Fatal("orphaned mapping must be deleted")
	}
	if !legitExists {
		t.Fatal("legitimate mapping must survive targeted removal")
	}

	// Removing a non-existent mapping returns false.
	if m.RemoveSenderMapping(domain.FileID("no-such-id")) {
		t.Fatal("RemoveSenderMapping must return false for non-existent fileID")
	}
}

// TestRemoveSenderMappingCompletedReleasesRef verifies that removing a
// completed sender mapping releases its transmit file ref. Completed mappings
// hold a ref so the blob stays available for re-downloads. RemoveSenderMapping
// is a legitimate deletion path that must release the ref.
//
// Tombstone mappings have no ref (blob already lost), so removal is a no-op
// for refs.
func TestRemoveSenderMappingCompletedReleasesRef(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "cccc1111dddd2222eeee3333ffff4444aaaa5555bbbb6666cccc1111dddd2222"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 2 // one ref for completed mapping, one for another
	store.mu.Unlock()

	m := NewFileTransferManager(Config{
		Store:       store,
		DownloadDir: dir,
	})

	completedID := domain.FileID("completed-msg-1")

	m.mu.Lock()
	m.senderMaps[completedID] = &senderFileMapping{
		FileID:    completedID,
		FileHash:  hash,
		FileName:  "done.txt",
		FileSize:  50,
		Recipient: "bob",
		State:     senderCompleted,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	removed := m.RemoveSenderMapping(completedID)
	if !removed {
		t.Fatal("RemoveSenderMapping must return true for existing mapping")
	}

	// Ref count must drop by 1 — completed mapping releases its ref.
	store.mu.Lock()
	refCount := store.refs[hash]
	store.mu.Unlock()

	if refCount != 1 {
		t.Fatalf("ref count should be 1 (completed mapping released its ref), got %d", refCount)
	}

	// Blob must still exist — another mapping holds the remaining ref.
	if _, err := os.Stat(filepath.Join(dir, hash+".bin")); os.IsNotExist(err) {
		t.Fatal("blob should survive — another mapping holds a ref")
	}
}

// TestRemoveSenderMappingTombstoneSkipsRefRelease verifies that removing a
// tombstone sender mapping does NOT release a ref — tombstoned mappings lost
// their blob and have no ref to release.
func TestRemoveSenderMappingTombstoneSkipsRefRelease(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "cccc1111dddd2222eeee3333ffff4444aaaa5555bbbb6666cccc1111dddd2222"
	if err := writeTestFile(filepath.Join(dir, hash+".bin"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[hash] = 1 // ref from some other mapping
	store.mu.Unlock()

	m := NewFileTransferManager(Config{
		Store:       store,
		DownloadDir: dir,
	})

	tombstoneID := domain.FileID("tombstone-msg-1")

	m.mu.Lock()
	m.senderMaps[tombstoneID] = &senderFileMapping{
		FileID:    tombstoneID,
		FileHash:  hash,
		FileName:  "lost.txt",
		FileSize:  50,
		Recipient: "bob",
		State:     senderTombstone,
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	removed := m.RemoveSenderMapping(tombstoneID)
	if !removed {
		t.Fatal("RemoveSenderMapping must return true for existing mapping")
	}

	// Ref count must remain 1 — tombstone mapping has no ref to release.
	store.mu.Lock()
	refCount := store.refs[hash]
	store.mu.Unlock()

	if refCount != 1 {
		t.Fatalf("ref count should be 1 (tombstone must not release), got %d", refCount)
	}
}

// TestEnsureStoredCleanupOnPrepareFailure verifies that when EnsureStored
// places a blob on disk but PrepareFileAnnounce fails (e.g. maxFileMappings),
// RemoveUnreferencedTransmitFile deletes the orphaned blob. This is the P2
// fix for the gap between EnsureStored (no ref, no pending) and
// PrepareFileAnnounce (creates pending reservation on success only).
func TestEnsureStoredCleanupOnPrepareFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	// Create a source file and store it via EnsureStored (no ref increment).
	srcPath := filepath.Join(t.TempDir(), "photo.jpg")
	if err := writeTestFile(srcPath, []byte("orphan-content")); err != nil {
		t.Fatal(err)
	}
	hash, err := store.EnsureStored(srcPath)
	if err != nil {
		t.Fatalf("EnsureStored: %v", err)
	}

	// Blob must exist on disk.
	if !store.HasFile(hash) {
		t.Fatal("transmit blob must exist after EnsureStored")
	}

	// Build a manager at maxFileMappings so PrepareFileAnnounce fails.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("fill-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("aa%062x", i),
			FileName:  fmt.Sprintf("fill-%d.bin", i),
			FileSize:  1,
			Recipient: domain.PeerIdentity("peer"),
			State:     senderAnnounced,
			CreatedAt: time.Now(),
		}
	}

	// PrepareFileAnnounce must fail — no token, no pending reservation.
	_, err = m.PrepareFileAnnounce(hash, "photo.jpg", "image/jpeg", 14)
	if err == nil {
		t.Fatal("PrepareFileAnnounce should fail at maxFileMappings")
	}

	// Simulate what PrepareAndSend does: clean up the orphaned blob.
	m.RemoveUnreferencedTransmitFile(hash)

	// Blob must be gone — refs=0, pending=0.
	if store.HasFile(hash) {
		t.Fatal("orphaned transmit blob should be deleted by RemoveUnreferencedTransmitFile")
	}
}

// TestEnsureStoredCleanupSafeWithConcurrentSender verifies that
// RemoveUnreferencedTransmitFile does NOT delete a blob when another
// sender has already acquired a ref for the same hash.
func TestEnsureStoredCleanupSafeWithConcurrentSender(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	// Store the file and acquire a ref (simulating a concurrent sender
	// that already committed).
	srcPath := filepath.Join(t.TempDir(), "shared.dat")
	if err := writeTestFile(srcPath, []byte("shared-content")); err != nil {
		t.Fatal(err)
	}
	hash, err := store.EnsureStored(srcPath)
	if err != nil {
		t.Fatalf("EnsureStored: %v", err)
	}
	if err := store.Acquire(hash); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// RemoveUnreferencedTransmitFile must NOT delete — refs=1.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}
	m.RemoveUnreferencedTransmitFile(hash)

	if !store.HasFile(hash) {
		t.Fatal("blob must survive RemoveUnreferencedTransmitFile when refs > 0")
	}
}

// TestEnsureStoredCleanupSafeWithPendingReservation verifies that
// RemoveUnreferencedTransmitFile does NOT delete a blob when another
// announce has a pending reservation for the same hash.
func TestEnsureStoredCleanupSafeWithPendingReservation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	srcPath := filepath.Join(t.TempDir(), "pending.dat")
	if err := writeTestFile(srcPath, []byte("pending-content")); err != nil {
		t.Fatal(err)
	}
	hash, err := store.EnsureStored(srcPath)
	if err != nil {
		t.Fatalf("EnsureStored: %v", err)
	}

	// Simulate another announce that placed a pending reservation.
	store.ReservePending(hash)

	// RemoveUnreferencedTransmitFile must NOT delete — pending=1.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		stopCh:       make(chan struct{}),
	}
	m.RemoveUnreferencedTransmitFile(hash)

	if !store.HasFile(hash) {
		t.Fatal("blob must survive RemoveUnreferencedTransmitFile when pending > 0")
	}
}
