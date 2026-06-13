package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
)

// newDebounceTestService builds a Service via NewService WITHOUT starting
// Run/bootstrapLoop, so no background goroutine races the manual
// lastPeerSave / peerStateDirty edits these tests perform. The peer state
// file lives in a per-test temp dir.
func newDebounceTestService(t *testing.T) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	cfg := config.Node{
		ListenAddress:  ":0",
		BootstrapPeers: []string{},
		PeersStatePath: filepath.Join(t.TempDir(), "peers.json"),
		Type:           config.NodeTypeClient,
	}
	return NewService(cfg, id, nil)
}

func (s *Service) peerSaveStateForTest() (time.Time, bool) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.lastPeerSave, s.peerStateDirty
}

// TestMaybeSavePeerState_DebounceWindowSuppressesFlush verifies that a burst
// of dirty marks inside the debounce window is coalesced — no flush happens,
// and the dirty flag is retained for the next eligible tick. This is the
// behavior that turns startup bootstrap priming from O(peers^2) full
// snapshot+marshal+disk writes into a single deferred flush.
func TestMaybeSavePeerState_DebounceWindowSuppressesFlush(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	now := time.Now()
	svc.peerMu.Lock()
	svc.lastPeerSave = now
	svc.peerStateDirty = false
	svc.peerMu.Unlock()

	// Simulate a back-to-back add_peer / ban-clear burst.
	svc.markPeerStateDirty()
	svc.markPeerStateDirty()
	svc.markPeerStateDirty()

	svc.maybeSavePeerState()

	last, dirty := svc.peerSaveStateForTest()
	if !last.Equal(now) {
		t.Fatalf("flush must be suppressed inside debounce window; lastPeerSave changed from %v to %v", now, last)
	}
	if !dirty {
		t.Fatal("dirty flag must be retained when the flush is suppressed")
	}
}

// TestMaybeSavePeerState_DebounceElapsedFlushesAndClearsDirty verifies that
// once the debounce gap has elapsed, a dirty node flushes exactly once and
// clears the dirty flag.
func TestMaybeSavePeerState_DebounceElapsedFlushesAndClearsDirty(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	svc.peerMu.Lock()
	svc.lastPeerSave = time.Now().Add(-time.Duration(peerStateDebounceSeconds+1) * time.Second)
	svc.peerStateDirty = false
	svc.peerMu.Unlock()

	svc.markPeerStateDirty()
	svc.maybeSavePeerState()

	last, dirty := svc.peerSaveStateForTest()
	if dirty {
		t.Fatal("dirty flag must be cleared after a successful flush")
	}
	if time.Since(last) > 5*time.Second {
		t.Fatalf("lastPeerSave must advance to ~now after flush, got %v", last)
	}
	if _, err := os.Stat(svc.peersStatePath); err != nil {
		t.Fatalf("peers state file must be written by flush: %v", err)
	}
}

// TestMaybeSavePeerState_CleanWithinPeriodicNoFlush verifies that a clean
// node (no dirty mark) within the periodic window performs no snapshot,
// marshal, or disk write at all.
func TestMaybeSavePeerState_CleanWithinPeriodicNoFlush(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	now := time.Now()
	svc.peerMu.Lock()
	svc.lastPeerSave = now
	svc.peerStateDirty = false
	svc.peerMu.Unlock()

	svc.maybeSavePeerState()

	last, _ := svc.peerSaveStateForTest()
	if !last.Equal(now) {
		t.Fatalf("clean node within periodic window must not flush; lastPeerSave changed to %v", last)
	}
}

// TestMaybeSavePeerState_PeriodicCatchAllFlushesWhenClean verifies the
// catch-all path still persists slowly-evolving fields (health, score,
// version policy) that mutate without an explicit dirty mark.
func TestMaybeSavePeerState_PeriodicCatchAllFlushesWhenClean(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	svc.peerMu.Lock()
	svc.lastPeerSave = time.Now().Add(-(peerStateSaveMinutes + 1) * time.Minute)
	svc.peerStateDirty = false
	svc.peerMu.Unlock()

	svc.maybeSavePeerState()

	last, _ := svc.peerSaveStateForTest()
	if time.Since(last) > 5*time.Second {
		t.Fatalf("periodic catch-all must flush a clean node; lastPeerSave=%v", last)
	}
}

// TestFlushPeerState_ClearsDirty verifies the flush primitive clears the
// dirty flag and records lastPeerSave on success.
func TestFlushPeerState_ClearsDirty(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	svc.markPeerStateDirty()
	svc.flushPeerState()

	last, dirty := svc.peerSaveStateForTest()
	if dirty {
		t.Fatal("flushPeerState must clear the dirty flag")
	}
	if last.IsZero() {
		t.Fatal("flushPeerState must set lastPeerSave on a successful write")
	}
}

// TestFlushPeerState_FailedWriteRemarksDirty verifies that a failed disk
// write re-marks the state dirty (so the next tick retries on the debounce
// path) and does NOT advance lastPeerSave.
func TestFlushPeerState_FailedWriteRemarksDirty(t *testing.T) {
	t.Parallel()
	svc := newDebounceTestService(t)

	// A regular file used as a directory component makes savePeerState fail.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	svc.peerMu.Lock()
	svc.peersStatePath = filepath.Join(blocker, "peers.json")
	svc.peerMu.Unlock()

	svc.markPeerStateDirty()
	svc.flushPeerState()

	last, dirty := svc.peerSaveStateForTest()
	if !dirty {
		t.Fatal("a failed write must re-mark dirty so the next tick retries")
	}
	if !last.IsZero() {
		t.Fatalf("a failed write must not advance lastPeerSave, got %v", last)
	}
}
