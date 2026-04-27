package filetransfer

import (
	"bytes"
	"encoding/base64"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestCommitChunkProgressLockedOwnershipMatrix is the table-driven
// pin for the partial-file ownership decision baked into
// commitChunkProgressLocked + receiverOwnsPartial. The two functions
// together decide whether a stale chunk write should unlink the
// partial it just produced; the matrix below covers every meaningful
// combination of (mapping presence, generation match, current state,
// progress, offset). Each row asserts the resulting chunkCommitStatus
// — and, by extension, whether a future predicate change would
// cause the caller in HandleChunkResponse to wrongly remove a
// resumable / verifying / actively-downloading partial.
//
// The waiting_route + progress row is the regression case from the
// review: a chunk that passes validation in receiverDownloading and
// whose mapping pauses to receiverWaitingRoute with NextOffset > 0
// MUST NOT be treated as orphan, because the partial holds resumable
// bytes a future StartDownload picks up.
func TestCommitChunkProgressLockedOwnershipMatrix(t *testing.T) {
	t.Parallel()

	const (
		fileID  = domain.FileID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5")
		ownGen  = uint64(7)
		othrGen = uint64(8)
	)

	type setup struct {
		// presentInMap controls whether receiverMaps has the entry at all.
		presentInMap bool
		// mappingGen is what we put on the mapping.Generation field.
		// Only meaningful when presentInMap.
		mappingGen uint64
		state      receiverState
		nextOffset uint64
	}

	cases := []struct {
		name        string
		setup       setup
		expectedGen uint64
		writeOffset uint64
		want        chunkCommitStatus
	}{
		{
			name: "ok_same_gen_downloading_offset_matches",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverDownloading,
				nextOffset:   1024,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitOK,
		},
		{
			name: "stale_offset_same_gen_downloading_dup_chunk",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverDownloading,
				nextOffset:   2048, // duplicate winner already advanced
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitStaleOffset,
		},
		{
			name: "non_downloading_same_gen_verifying_owns_partial",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverVerifying,
				nextOffset:   4096,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitNonDownloading,
		},
		{
			// THE REGRESSION CASE: validate happened in
			// receiverDownloading; mapping paused to waiting_route with
			// progress; commit must NOT unlink — the partial is the
			// resumable file. receiverOwnsPartial returns true here
			// because waiting_route + NextOffset > 0 owns the partial.
			name: "non_downloading_same_gen_waiting_route_with_progress_owns",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverWaitingRoute,
				nextOffset:   2048,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitNonDownloading,
		},
		{
			// waiting_route WITHOUT progress: nothing on disk to protect,
			// stale write into a fresh empty path is true orphan.
			name: "removed_same_gen_waiting_route_no_progress_orphan",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverWaitingRoute,
				nextOffset:   0,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "removed_same_gen_available",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverAvailable,
				nextOffset:   0,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "removed_same_gen_failed",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverFailed,
				nextOffset:   4096,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "removed_same_gen_completed",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverCompleted,
				nextOffset:   4096,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "removed_same_gen_waiting_ack",
			setup: setup{
				presentInMap: true,
				mappingGen:   ownGen,
				state:        receiverWaitingAck,
				nextOffset:   4096,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "gen_mismatch_new_downloading_active",
			setup: setup{
				presentInMap: true,
				mappingGen:   othrGen,
				state:        receiverDownloading,
				nextOffset:   512,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitGenerationMismatch,
		},
		{
			name: "gen_mismatch_new_verifying_active",
			setup: setup{
				presentInMap: true,
				mappingGen:   othrGen,
				state:        receiverVerifying,
				nextOffset:   4096,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitGenerationMismatch,
		},
		{
			name: "gen_mismatch_new_waiting_route_with_progress_resumable",
			setup: setup{
				presentInMap: true,
				mappingGen:   othrGen,
				state:        receiverWaitingRoute,
				nextOffset:   2048,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitGenerationMismatch,
		},
		{
			name: "gen_mismatch_new_waiting_route_no_progress_orphan",
			setup: setup{
				presentInMap: true,
				mappingGen:   othrGen,
				state:        receiverWaitingRoute,
				nextOffset:   0,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "gen_mismatch_new_available_after_cancel_reset",
			setup: setup{
				presentInMap: true,
				mappingGen:   othrGen,
				state:        receiverAvailable,
				nextOffset:   0,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
		{
			name: "mapping_removed_no_entry",
			setup: setup{
				presentInMap: false,
			},
			expectedGen: ownGen,
			writeOffset: 1024,
			want:        chunkCommitMappingRemoved,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := &Manager{
				receiverMaps: make(map[domain.FileID]*receiverFileMapping),
			}

			if tc.setup.presentInMap {
				m.receiverMaps[fileID] = &receiverFileMapping{
					FileID:     fileID,
					Generation: tc.setup.mappingGen,
					State:      tc.setup.state,
					NextOffset: tc.setup.nextOffset,
				}
			}

			// commitChunkProgressLocked requires caller-held m.mu.
			m.mu.Lock()
			_, status := m.commitChunkProgressLocked(fileID, tc.writeOffset, 512, tc.expectedGen)
			m.mu.Unlock()

			if status != tc.want {
				t.Fatalf("commitChunkProgressLocked status = %v, want %v", status, tc.want)
			}
		})
	}
}

// TestReceiverOwnsPartialPredicate is the lower-level pin on the
// ownership predicate itself. Future refactors may inline the
// predicate or move the call site; this test makes the contract
// explicit and independent of the commit-status caller.
func TestReceiverOwnsPartialPredicate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		state      receiverState
		nextOffset uint64
		want       bool
	}{
		{"downloading owns", receiverDownloading, 0, true},
		{"downloading owns even with zero offset", receiverDownloading, 0, true},
		{"verifying owns", receiverVerifying, 4096, true},
		{"waiting_route with progress owns (regression)", receiverWaitingRoute, 1024, true},
		{"waiting_route without progress does not own", receiverWaitingRoute, 0, false},
		{"available does not own", receiverAvailable, 0, false},
		{"waiting_ack does not own (verifier already renamed)", receiverWaitingAck, 4096, false},
		{"completed does not own", receiverCompleted, 4096, false},
		{"failed does not own", receiverFailed, 4096, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rm := &receiverFileMapping{
				State:      tc.state,
				NextOffset: tc.nextOffset,
			}
			if got := receiverOwnsPartial(rm); got != tc.want {
				t.Fatalf("receiverOwnsPartial(state=%s, nextOffset=%d) = %v, want %v",
					tc.state, tc.nextOffset, got, tc.want)
			}
		})
	}

	// Nil mapping is a non-owner — exercise this separately so the
	// happy-path table stays focused on real states.
	t.Run("nil_mapping_is_not_owner", func(t *testing.T) {
		t.Parallel()
		if receiverOwnsPartial(nil) {
			t.Fatal("receiverOwnsPartial(nil) = true, want false")
		}
	})
}

// TestPostValidationDuplicateRaceDropsLoserWithoutWriting is the
// concurrency-level pin for the pre-write revalidation guard in
// HandleChunkResponse (see receiver.go: prep.writeMu.Lock + classify
// before writeChunkToFile). The classifier matrix above proves the
// decision logic is correct in isolation; this test proves the
// decision is actually made on the live race that motivated the
// guard.
//
// Race scenario: two duplicate chunk_response handlers (a legitimate
// retry from the sender, or a poisoned dup) both pass
// validateChunkResponseLocked at the same offset before either
// commits. validate snapshots NextOffset under m.mu and releases
// the lock; both handlers then queue on the per-mapping
// writePartialMu. With the guard, the loser's pre-write classify
// returns chunkCommitStaleOffset (offset != mapping.NextOffset
// because the winner already committed) and drops without ever
// calling writeChunkToFile. Without the guard, the loser would
// WriteAt(offset) — overwriting the winner's canonical bytes —
// before post-write commit detected the stale offset, leaving a
// poisoned partial that fails hash verification on an otherwise
// healthy download.
//
// The test does not control which goroutine wins the writeMu race
// (sync.Mutex is not strictly FIFO under contention). It snapshots
// disk bytes WHILE the winner is blocked in sendCommand (proving
// only the winner has touched the file so far) and asserts that the
// disk bytes are byte-identical AFTER the loser has run. With the
// guard, the loser drops; without the guard, the loser overwrites
// and the snapshot diverges.
func TestPostValidationDuplicateRaceDropsLoserWithoutWriting(t *testing.T) {
	// Intentionally NOT t.Parallel(): postValidatePreWriteMuHook is a
	// package-global variable, and other tests in this package call
	// HandleChunkResponse — running in parallel would let those
	// handlers invoke this test's hook, racing the counter and
	// double-closing the bothValidated channel. Serial execution
	// keeps the hook private to this test for its lifetime.

	downloadDir := testDownloadDir(t)

	sender := domain.PeerIdentity("dup-race-sender-identity-1234")
	fileID := domain.FileID("dup-race-file")
	chunkSize := uint32(64)
	// Two-chunk file so the first chunk is non-final and
	// onDownloadComplete does not fire — we want to isolate the
	// validate → writeMu → write window for the FIRST chunk and avoid
	// pulling the verifier into the test.
	fileSize := uint64(2 * chunkSize)

	// Two distinct payloads at offset 0. payloadA is the canonical
	// bytes (any byte that differs in the final assertion is the
	// regression); payloadB models a poisoned duplicate — corrupted
	// retry, malicious retransmit, etc. The runtime is free to pick
	// either as the writeMu winner; the assertion below works in
	// both directions.
	payloadA := bytes.Repeat([]byte{0xAA}, int(chunkSize))
	payloadB := bytes.Repeat([]byte{0xBB}, int(chunkSize))

	// FileHash is intentionally bogus — the test never reaches
	// verification because it only delivers the first chunk. Using
	// any non-empty string keeps testReceiverMapping happy.
	fileHash := "00000000000000000000000000000000000000000000000000000000deadbeef"

	m := newTestTransferManager(t, downloadDir, nil)
	// Gated send: blocks the winner inside requestNextChunk (called
	// from the OK branch of HandleChunkResponse) so the test can
	// snapshot the canonical disk bytes WHILE the winner still
	// holds writeMu and the loser is still queued. Overrides the
	// noop sendCommand installed by newTestTransferManager.
	gate := make(chan struct{})
	called := make(chan struct{}, 2)
	m.sendCommand = func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
		called <- struct{}{}
		<-gate
		return nil
	}

	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, fileHash, "race.bin",
		fileSize, chunkSize, receiverDownloading,
	)
	rm := m.receiverMaps[fileID]

	// Pre-lock writePartialMu so neither handler can win the write
	// race until the test releases it. validateChunkResponseLocked
	// runs sequentially under m.mu and passes for both (NextOffset =
	// 0 = offset for both); the production code then fires the
	// post-validate hook (installed below) before each handler
	// attempts writeMu.Lock(). Releasing the lock at the right
	// moment kicks off the actual race.
	rm.writePartialMu.Lock()

	// Deterministic barrier. The hook fires AFTER each handler has
	// captured its validate snapshot (NextOffset = 0, generation
	// matching) and released m.mu, but BEFORE either tries to
	// acquire writeMu. We wait until the hook has fired exactly
	// twice, which proves both handlers entered the race with the
	// same stale snapshot — without this barrier a late-scheduled
	// second goroutine could validate AFTER the winner committed,
	// reducing the test to the already-covered sequential
	// stale-offset path and silently masking a regression of the
	// pre-write revalidation guard.
	var validatedCount atomic.Int32
	bothValidated := make(chan struct{})
	postValidatePreWriteMuHook = func() {
		if validatedCount.Add(1) == 2 {
			close(bothValidated)
		}
	}
	t.Cleanup(func() { postValidatePreWriteMuHook = nil })

	encA := base64.RawURLEncoding.EncodeToString(payloadA)
	encB := base64.RawURLEncoding.EncodeToString(payloadB)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
			FileID: fileID,
			Offset: 0,
			Data:   encA,
		})
	}()
	go func() {
		defer wg.Done()
		m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
			FileID: fileID,
			Offset: 0,
			Data:   encB,
		})
	}()

	// Block until both handlers have passed validate and the hook
	// has fired twice. 5s is a generous safety net for an event that
	// normally takes microseconds; if this ever times out, validate
	// is rejecting one of the chunks (e.g. the production code
	// changed and the snapshot conditions no longer match).
	select {
	case <-bothValidated:
	case <-time.After(5 * time.Second):
		t.Fatal("post-validate hook did not fire twice — handlers did not both reach the race")
	}

	// Open the gate. Both handlers have validated with the same
	// stale snapshot and are now serialised on writeMu. Exactly one
	// wins, classifies OK, writes its payload, commits (NextOffset
	// = chunkSize), then blocks on `gate` inside requestNextChunk →
	// sendCommand. The other stays queued on writeMu (winner holds
	// it via defer).
	rm.writePartialMu.Unlock()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("winner never reached sendCommand — write/commit stuck")
	}

	// Snapshot canonical bytes while the winner is blocked in
	// sendCommand and the loser is still queued on writeMu. These
	// MUST match the post-race bytes if the guard is doing its job.
	partialPath := partialDownloadPath(downloadDir, fileID)
	canonical, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial during race: %v", err)
	}
	if uint64(len(canonical)) < uint64(chunkSize) {
		t.Fatalf("partial too short during race: %d bytes, want >= %d",
			len(canonical), chunkSize)
	}
	canonicalChunk := append([]byte{}, canonical[:chunkSize]...)

	// Sanity: the snapshot matches exactly one of the two payloads.
	// The runtime picked one writer; this test does not control which.
	matchesA := bytes.Equal(canonicalChunk, payloadA)
	matchesB := bytes.Equal(canonicalChunk, payloadB)
	if !matchesA && !matchesB {
		t.Fatalf("canonical bytes match neither payload — separate corruption bug:\n  got: %x\n    A: %x\n    B: %x",
			canonicalChunk, payloadA, payloadB)
	}

	// Release the gate. Winner returns from sendCommand → returns
	// from HandleChunkResponse → defer releases writeMu → loser
	// acquires writeMu. With the guard, the loser's
	// classifyChunkOwnershipLocked returns chunkCommitStaleOffset
	// (NextOffset is now chunkSize, not 0) and the handler drops
	// without ever calling writeChunkToFile.
	close(gate)
	wg.Wait()

	// THE ASSERTION: post-race partial bytes must equal the canonical
	// snapshot. If they differ, the loser called writeChunkToFile
	// after the winner committed, overwriting the canonical bytes —
	// exactly the regression the pre-write revalidation prevents.
	after, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial after race: %v", err)
	}
	if !bytes.Equal(after[:chunkSize], canonicalChunk) {
		t.Fatalf("loser overwrote canonical bytes — pre-write revalidation guard regressed:\n canonical: %x\n     after: %x",
			canonicalChunk, after[:chunkSize])
	}

	// Mapping invariants: exactly one chunk committed. Without the
	// guard the loser still fails commit with chunkCommitStaleOffset
	// (it does not advance NextOffset twice), so this check alone
	// would not catch the bug — the disk-bytes assertion above is
	// the load-bearing one. These checks pin the post-race state
	// against unrelated regressions.
	m.mu.Lock()
	nextOffset := rm.NextOffset
	bytesReceived := rm.BytesReceived
	state := rm.State
	m.mu.Unlock()

	if nextOffset != uint64(chunkSize) {
		t.Fatalf("NextOffset = %d, want %d (exactly one chunk committed)",
			nextOffset, chunkSize)
	}
	if bytesReceived != uint64(chunkSize) {
		t.Fatalf("BytesReceived = %d, want %d (exactly one chunk committed)",
			bytesReceived, chunkSize)
	}
	if state != receiverDownloading {
		t.Fatalf("state = %s, want %s (still mid-download for two-chunk file)",
			state, receiverDownloading)
	}

	// The loser must NOT have entered the OK branch and called
	// requestNextChunk for chunk #2. With the guard, the loser
	// returns from the StaleOffset branch before sendCommand. If a
	// second sendCommand fired, the loser somehow committed OK —
	// that is a different regression from the one this test pins,
	// but worth catching here too.
	select {
	case <-called:
		t.Fatal("loser also called sendCommand — should have dropped on classify")
	default:
	}
}
