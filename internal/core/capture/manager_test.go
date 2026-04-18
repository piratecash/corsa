package capture_test

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/capture"
	capturemocks "github.com/piratecash/corsa/internal/core/capture/mocks"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// writerBuffer captures bytes written through a MockWriter for assertions.
type writerBuffer struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (wb *writerBuffer) String() string {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.buf.String()
}

func (wb *writerBuffer) IsClosed() bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.closed
}

// newMockWriterWithBuffer creates a standalone MockWriter backed by a
// writerBuffer. Used by session tests that need a single Writer instance.
func newMockWriterWithBuffer(t *testing.T) (*capturemocks.MockWriter, *writerBuffer) {
	t.Helper()
	wb := &writerBuffer{}
	m := capturemocks.NewMockWriter(t)
	m.EXPECT().Write(mock.Anything).RunAndReturn(func(p []byte) (int, error) {
		wb.mu.Lock()
		defer wb.mu.Unlock()
		return wb.buf.Write(p)
	}).Maybe()
	m.EXPECT().Close().RunAndReturn(func() error {
		wb.mu.Lock()
		defer wb.mu.Unlock()
		wb.closed = true
		return nil
	}).Maybe()
	m.EXPECT().Sync().Return(nil).Maybe()
	return m, wb
}

// mockWriterTracker creates MockWriter instances per file path and collects
// their output buffers for inspection.
type mockWriterTracker struct {
	t       *testing.T
	mu      sync.Mutex
	buffers map[string]*writerBuffer
}

func newMockWriterTracker(t *testing.T) *mockWriterTracker {
	return &mockWriterTracker{t: t, buffers: make(map[string]*writerBuffer)}
}

func (tr *mockWriterTracker) factory(path string) (capture.Writer, error) {
	m, wb := newMockWriterWithBuffer(tr.t)
	tr.mu.Lock()
	tr.buffers[path] = wb
	tr.mu.Unlock()
	return m, nil
}

// testResolver creates a MockConnResolver backed by a mutable map.
// Returns both the mock and the map so tests can add/remove entries
// after construction.
func testResolver(t *testing.T, conns ...capture.ConnInfo) (*capturemocks.MockConnResolver, map[domain.ConnID]capture.ConnInfo) {
	t.Helper()
	connMap := make(map[domain.ConnID]capture.ConnInfo)
	for _, c := range conns {
		connMap[c.ConnID] = c
	}
	m := capturemocks.NewMockConnResolver(t)
	m.EXPECT().ConnInfoByID(mock.Anything).RunAndReturn(func(id domain.ConnID) (capture.ConnInfo, bool) {
		c, ok := connMap[id]
		return c, ok
	}).Maybe()
	m.EXPECT().ConnInfoByIP(mock.Anything).RunAndReturn(func(ip netip.Addr) []capture.ConnInfo {
		var result []capture.ConnInfo
		for _, c := range connMap {
			if c.RemoteIP == ip {
				result = append(result, c)
			}
		}
		return result
	}).Maybe()
	m.EXPECT().AllConnInfo().RunAndReturn(func() []capture.ConnInfo {
		result := make([]capture.ConnInfo, 0, len(connMap))
		for _, c := range connMap {
			result = append(result, c)
		}
		return result
	}).Maybe()
	return m, connMap
}

func testManager(t *testing.T, resolver capture.ConnResolver) (*capture.Manager, *mockWriterTracker) {
	t.Helper()
	tracker := newMockWriterTracker(t)
	m := capture.NewManager(context.Background(), capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        func() time.Time { return time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC) },
		ConnResolver: resolver,
		Factory:      tracker.factory,
	})
	t.Cleanup(func() { m.Close() })
	return m, tracker
}

var testIP = netip.MustParseAddr("203.0.113.10")

// ---------------------------------------------------------------------------
// StartByConnIDs
// ---------------------------------------------------------------------------

func TestManager_StartByConnIDs_Success(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	result := m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	if len(result.Started) != 1 {
		t.Fatalf("expected 1 started, got %d", len(result.Started))
	}
	if result.Started[0].ConnID != 42 {
		t.Errorf("expected conn_id=42, got %d", result.Started[0].ConnID)
	}
	if result.Started[0].Format != domain.CaptureFormatCompact {
		t.Errorf("expected format=compact, got %s", result.Started[0].Format)
	}
}

func TestManager_StartByConnIDs_NotFound(t *testing.T) {
	resolver, _ := testResolver(t)
	m, _ := testManager(t, resolver)

	result := m.StartByConnIDs([]domain.ConnID{99}, domain.CaptureFormatCompact)

	if len(result.NotFound) != 1 {
		t.Fatalf("expected 1 not_found, got %d", len(result.NotFound))
	}
}

func TestManager_StartByConnIDs_Idempotent(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)
	result := m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	if len(result.AlreadyActive) != 1 {
		t.Fatalf("expected 1 already_active, got %d", len(result.AlreadyActive))
	}
	if len(result.Started) != 0 {
		t.Fatalf("expected 0 started on idempotent call, got %d", len(result.Started))
	}
}

func TestManager_StartByConnIDs_FormatConflict(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)
	result := m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatPretty)

	if len(result.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(result.Conflicts))
	}
}

// ---------------------------------------------------------------------------
// StartByIPs
// ---------------------------------------------------------------------------

func TestManager_StartByIPs_Success(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	result := m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)

	if len(result.InstalledRules) != 1 {
		t.Fatalf("expected 1 installed rule, got %d", len(result.InstalledRules))
	}
	if len(result.Started) != 1 {
		t.Fatalf("expected 1 started, got %d", len(result.Started))
	}
	if result.InstalledRules[0].Scope != domain.CaptureScopeIP {
		t.Errorf("expected scope=ip, got %s", result.InstalledRules[0].Scope)
	}
}

func TestManager_StartByIPs_NoCurrentConns(t *testing.T) {
	resolver, _ := testResolver(t)
	m, _ := testManager(t, resolver)

	result := m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)

	if len(result.InstalledRules) != 1 {
		t.Fatalf("rule should still be installed even without current matches")
	}
	if len(result.NotFound) != 1 {
		t.Fatalf("expected 1 not_found for no current connections, got %d", len(result.NotFound))
	}
}

// ---------------------------------------------------------------------------
// StartAll
// ---------------------------------------------------------------------------

func TestManager_StartAll(t *testing.T) {
	info1 := capture.ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := capture.ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	resolver, _ := testResolver(t, info1, info2)
	m, _ := testManager(t, resolver)

	result := m.StartAll(domain.CaptureFormatPretty)

	if len(result.Started) != 2 {
		t.Fatalf("expected 2 started, got %d", len(result.Started))
	}
	if len(result.InstalledRules) != 1 {
		t.Fatalf("expected 1 installed rule, got %d", len(result.InstalledRules))
	}
	if result.InstalledRules[0].Scope != domain.CaptureScopeAll {
		t.Errorf("expected scope=all, got %s", result.InstalledRules[0].Scope)
	}
}

// ---------------------------------------------------------------------------
// Stop commands
// ---------------------------------------------------------------------------

func TestManager_StopByConnIDs(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)
	result := m.StopByConnIDs([]domain.ConnID{42})

	if len(result.Stopped) != 1 {
		t.Fatalf("expected 1 stopped, got %d", len(result.Stopped))
	}
	if m.HasActiveCaptures() {
		t.Fatal("should have no active captures after stop")
	}
}

func TestManager_StopByConnIDs_NotFound(t *testing.T) {
	resolver, _ := testResolver(t)
	m, _ := testManager(t, resolver)
	result := m.StopByConnIDs([]domain.ConnID{99})

	if len(result.NotFound) != 1 {
		t.Fatalf("expected 1 not_found, got %d", len(result.NotFound))
	}
}

func TestManager_StopByIPs(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)
	result := m.StopByIPs([]netip.Addr{testIP})

	if len(result.Stopped) != 1 {
		t.Fatalf("expected 1 stopped, got %d", len(result.Stopped))
	}
	if len(result.RemovedRules) != 1 {
		t.Fatalf("expected 1 removed rule, got %d", len(result.RemovedRules))
	}
}

func TestManager_StopAll(t *testing.T) {
	info1 := capture.ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := capture.ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	resolver, _ := testResolver(t, info1, info2)
	m, _ := testManager(t, resolver)

	m.StartAll(domain.CaptureFormatCompact)
	result := m.StopAll()

	if len(result.Stopped) != 2 {
		t.Fatalf("expected 2 stopped, got %d", len(result.Stopped))
	}
	if len(result.RemovedRules) != 1 {
		t.Fatalf("expected 1 removed rule, got %d", len(result.RemovedRules))
	}
	if m.HasActiveCaptures() {
		t.Fatal("should have no active captures after StopAll")
	}
}

// ---------------------------------------------------------------------------
// Enqueue and file content
// ---------------------------------------------------------------------------

func TestManager_Enqueue_WritesToFile(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, tracker := testManager(t, resolver)

	result := m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)
	if len(result.Started) != 1 {
		t.Fatalf("expected 1 started, got %d", len(result.Started))
	}

	m.EnqueueRecv(42, `{"type":"pong"}`, domain.PayloadKindJSON)
	m.EnqueueSend(42, `{"type":"ping"}`, domain.PayloadKindJSON, domain.SendOutcomeSent)

	// Stop and wait for drain.
	stopResult := m.StopByConnIDs([]domain.ConnID{42})
	if len(stopResult.Stopped) != 1 {
		t.Fatalf("expected 1 stopped, got %d", len(stopResult.Stopped))
	}

	filePath := result.Started[0].FilePath
	tracker.mu.Lock()
	w, ok := tracker.buffers[filePath]
	tracker.mu.Unlock()
	if !ok {
		t.Fatalf("writer not found for path %s", filePath)
	}

	content := w.String()
	if content == "" {
		t.Fatal("expected non-empty capture file")
	}
	if len(content) < 10 {
		t.Fatalf("content too short: %q", content)
	}
}

// ---------------------------------------------------------------------------
// OnNewConnection — standing rule auto-capture
// ---------------------------------------------------------------------------

func TestManager_OnNewConnection_MatchesByIPRule(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, connMap := testResolver(t, info)
	m, tracker := testManager(t, resolver)

	// Install by_ip rule with no current connections initially.
	m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)

	// Simulate a new connection arriving.
	newInfo := capture.ConnInfo{ConnID: 100, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	connMap[100] = newInfo
	m.OnNewConnection(newInfo)

	// The session should be registered synchronously (pending state).
	// Events enqueued immediately should NOT be lost — this verifies
	// the fix for first-frame loss.
	snap, ok := m.SessionSnapshotByID(100)
	if !ok {
		t.Fatal("expected session for conn_id=100 immediately after OnNewConnection")
	}
	if !snap.Recording {
		t.Error("expected recording=true")
	}

	// Enqueue an event immediately — before the writer goroutine starts.
	m.EnqueueRecv(100, `{"type":"welcome"}`, domain.PayloadKindJSON)

	// Give the background goroutine time to open the file and attach writer.
	time.Sleep(100 * time.Millisecond)

	// Stop and verify the event was captured.
	stopResult := m.StopByConnIDs([]domain.ConnID{100})
	if len(stopResult.Stopped) != 1 {
		t.Fatalf("expected 1 stopped, got %d", len(stopResult.Stopped))
	}

	filePath := stopResult.Stopped[0].FilePath
	tracker.mu.Lock()
	w, ok := tracker.buffers[filePath]
	tracker.mu.Unlock()
	if !ok {
		t.Fatalf("writer not found for path %s", filePath)
	}
	content := w.String()
	if content == "" {
		t.Fatal("expected first-frame event to be captured (was it lost?)")
	}
}

// ---------------------------------------------------------------------------
// OnConnectionClosed
// ---------------------------------------------------------------------------

func TestManager_OnConnectionClosed(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	if !m.HasActiveCaptures() {
		t.Fatal("expected active capture")
	}

	m.OnConnectionClosed(42)

	// Give the writer goroutine time to drain.
	time.Sleep(50 * time.Millisecond)

	if m.HasActiveCaptures() {
		t.Fatal("expected no active captures after OnConnectionClosed")
	}
}

// ---------------------------------------------------------------------------
// Failed writer self-eviction
// ---------------------------------------------------------------------------

func TestManager_EvictFailedSession(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	// Directly test evictFailedSession by injecting a failed session.
	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	if !m.HasActiveCaptures() {
		t.Fatal("expected active capture")
	}

	// Force state to failed and evict via exported test helpers.
	m.ForceSessionFailed(42)
	m.EvictFailedSessionForTest(42)

	if m.HasActiveCaptures() {
		t.Fatal("expected failed session to be evicted")
	}

	// Verify a new start for the same conn_id is now possible.
	result := m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)
	if len(result.Started) != 1 {
		t.Fatalf("expected restart after eviction, got started=%d conflicts=%v", len(result.Started), result.Conflicts)
	}
}

// ---------------------------------------------------------------------------
// Auto-start cleanup must not wipe a replacement session
// ---------------------------------------------------------------------------

func TestManager_AutoStartCleanup_DoesNotWipeReplacement(t *testing.T) {
	// Scenario: auto-start goroutine is slow (factory blocks). While it is
	// blocked, the operator stops the pending session and starts a fresh one
	// for the same conn_id. The original goroutine's cleanup must not delete
	// the replacement session from m.sessions.

	// ready is sent by the goroutine's factory call right before it blocks,
	// so the test knows the goroutine has reached the blocking point and it
	// is safe to proceed with stop + start without a call-count race.
	ready := make(chan struct{}, 1)
	gate := make(chan struct{}) // released by the test to unblock the goroutine

	// Start with an empty resolver — no existing connections for testIP.
	// This ensures StartByIPs installs the rule without calling the factory
	// synchronously (no current matches). The first factory call will come
	// from the OnNewConnection goroutine, which is what we want to gate.
	resolver, connMap := testResolver(t)

	var once sync.Once
	tracker := newMockWriterTracker(t)

	gatedFactory := func(path string) (capture.Writer, error) {
		blocked := false
		once.Do(func() { blocked = true })

		if blocked {
			// This is the OnNewConnection goroutine's call.
			// Signal the test we are about to block, then wait.
			ready <- struct{}{}
			<-gate
			return nil, fmt.Errorf("simulated disk error")
		}
		// All other calls (manual StartByConnIDs) succeed immediately.
		return tracker.factory(path)
	}

	m := capture.NewManager(context.Background(), capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        func() time.Time { return time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC) },
		ConnResolver: resolver,
		Factory:      gatedFactory,
	})
	t.Cleanup(func() { m.Close() })

	// Install a by_ip rule with no current connections. The rule is
	// installed but no sessions are started (no factory calls yet).
	m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)

	// Now add conn_id=100 to the resolver and trigger auto-start.
	// OnNewConnection registers a pending session synchronously, then
	// spawns a goroutine that does MkdirAll + gatedFactory.
	newInfo := capture.ConnInfo{ConnID: 100, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	connMap[100] = newInfo
	m.OnNewConnection(newInfo)

	// Wait until the goroutine is blocked inside the factory call.
	// This eliminates any race between the goroutine and the test.
	<-ready

	// Pending session was registered synchronously by OnNewConnection.
	_, ok := m.SessionSnapshotByID(100)
	if !ok {
		t.Fatal("expected pending session for conn_id=100")
	}

	// Operator stops the pending session.
	m.StopByConnIDs([]domain.ConnID{100})

	// Operator starts a fresh session for the same conn_id.
	// This factory call is NOT the first (once.Do already fired), so it
	// succeeds immediately via tracker.factory.
	result := m.StartByConnIDs([]domain.ConnID{100}, domain.CaptureFormatCompact)
	if len(result.Started) != 1 {
		t.Fatalf("expected 1 started, got %d (already_active=%d)", len(result.Started), len(result.AlreadyActive))
	}

	// Release the original auto-start goroutine. Factory returns error,
	// cleanup calls removeSessionIfOwner — must NOT delete replacement.
	close(gate)

	// Give the cleanup goroutine time to execute.
	time.Sleep(100 * time.Millisecond)

	// The replacement session must still be in the map.
	snap, ok := m.SessionSnapshotByID(100)
	if !ok {
		t.Fatal("replacement session was wiped by auto-start cleanup (P1 bug)")
	}
	if !snap.Recording {
		t.Error("replacement session should still be recording")
	}
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

func TestManager_SessionSnapshot(t *testing.T) {
	info := capture.ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver, _ := testResolver(t, info)
	m, _ := testManager(t, resolver)

	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	snap, ok := m.SessionSnapshotByID(42)
	if !ok {
		t.Fatal("expected snapshot for conn_id=42")
	}
	if snap.ConnID != 42 {
		t.Errorf("expected ConnID=42, got %d", snap.ConnID)
	}
	if snap.RemoteIP != testIP {
		t.Errorf("expected RemoteIP=%s, got %s", testIP, snap.RemoteIP)
	}
	if !snap.Recording {
		t.Error("expected Recording=true")
	}
	if snap.Format != domain.CaptureFormatCompact {
		t.Errorf("expected format=compact, got %s", snap.Format)
	}

	_, ok = m.SessionSnapshotByID(999)
	if ok {
		t.Error("expected no snapshot for non-existent conn_id")
	}
}

func TestManager_AllSessionSnapshots(t *testing.T) {
	info1 := capture.ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := capture.ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	resolver, _ := testResolver(t, info1, info2)
	m, _ := testManager(t, resolver)

	m.StartAll(domain.CaptureFormatCompact)

	snaps := m.AllSessionSnapshots()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
}
