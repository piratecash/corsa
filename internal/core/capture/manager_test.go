package capture

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// memWriter is an in-memory Writer for tests.
type memWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *memWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *memWriter) Close() error { return nil }
func (w *memWriter) Sync() error  { return nil }

func (w *memWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// writerTracker collects all created writers for inspection.
type writerTracker struct {
	mu      sync.Mutex
	writers map[string]*memWriter
}

func newWriterTracker() *writerTracker {
	return &writerTracker{writers: make(map[string]*memWriter)}
}

func (t *writerTracker) factory(path string) (Writer, error) {
	w := &memWriter{}
	t.mu.Lock()
	t.writers[path] = w
	t.mu.Unlock()
	return w, nil
}

// stubResolver implements ConnResolver for tests.
type stubResolver struct {
	conns map[domain.ConnID]ConnInfo
}

func (r *stubResolver) ConnInfoByID(id domain.ConnID) (ConnInfo, bool) {
	c, ok := r.conns[id]
	return c, ok
}

func (r *stubResolver) ConnInfoByIP(ip netip.Addr) []ConnInfo {
	var result []ConnInfo
	for _, c := range r.conns {
		if c.RemoteIP == ip {
			result = append(result, c)
		}
	}
	return result
}

func (r *stubResolver) AllConnInfo() []ConnInfo {
	result := make([]ConnInfo, 0, len(r.conns))
	for _, c := range r.conns {
		result = append(result, c)
	}
	return result
}

func testManager(t *testing.T, resolver ConnResolver) (*Manager, *writerTracker) {
	t.Helper()
	tracker := newWriterTracker()
	m := NewManager(context.Background(), ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        func() time.Time { return time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC) },
		ConnResolver: resolver,
		Factory:      tracker.factory,
	})
	t.Cleanup(func() { m.Close() })
	return m, tracker
}

var testIP = netip.MustParseAddr("203.0.113.10")

func testResolver(conns ...ConnInfo) *stubResolver {
	r := &stubResolver{conns: make(map[domain.ConnID]ConnInfo)}
	for _, c := range conns {
		r.conns[c.ConnID] = c
	}
	return r
}

// ---------------------------------------------------------------------------
// StartByConnIDs
// ---------------------------------------------------------------------------

func TestManager_StartByConnIDs_Success(t *testing.T) {
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	m, _ := testManager(t, testResolver())

	result := m.StartByConnIDs([]domain.ConnID{99}, domain.CaptureFormatCompact)

	if len(result.NotFound) != 1 {
		t.Fatalf("expected 1 not_found, got %d", len(result.NotFound))
	}
}

func TestManager_StartByConnIDs_Idempotent(t *testing.T) {
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	m, _ := testManager(t, testResolver(info))

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
	m, _ := testManager(t, testResolver())

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
	info1 := ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	m, _ := testManager(t, testResolver(info1, info2))

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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	m, _ := testManager(t, testResolver())
	result := m.StopByConnIDs([]domain.ConnID{99})

	if len(result.NotFound) != 1 {
		t.Fatalf("expected 1 not_found, got %d", len(result.NotFound))
	}
}

func TestManager_StopByIPs(t *testing.T) {
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	info1 := ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	m, _ := testManager(t, testResolver(info1, info2))

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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, tracker := testManager(t, testResolver(info))

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
	w, ok := tracker.writers[filePath]
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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	resolver := testResolver(info)
	m, tracker := testManager(t, resolver)

	// Install by_ip rule with no current connections initially.
	m.StartByIPs([]netip.Addr{testIP}, domain.CaptureFormatCompact)

	// Simulate a new connection arriving.
	newInfo := ConnInfo{ConnID: 100, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	resolver.conns[100] = newInfo
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
	w, ok := tracker.writers[filePath]
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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

	// Directly test evictFailedSession by injecting a failed session.
	m.StartByConnIDs([]domain.ConnID{42}, domain.CaptureFormatCompact)

	if !m.HasActiveCaptures() {
		t.Fatal("expected active capture")
	}

	// Simulate the writer goroutine calling evictFailedSession.
	m.mu.Lock()
	s := m.sessions[42]
	m.mu.Unlock()

	// Force state to failed.
	s.mu.Lock()
	s.state = sessionFailed
	s.mu.Unlock()

	m.evictFailedSession(42)

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
	resolver := testResolver()

	var once sync.Once
	tracker := newWriterTracker()

	gatedFactory := func(path string) (Writer, error) {
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

	m := NewManager(context.Background(), ManagerOpts{
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
	newInfo := ConnInfo{ConnID: 100, RemoteIP: testIP, PeerDir: domain.PeerDirectionInbound}
	resolver.conns[100] = newInfo
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
	info := ConnInfo{ConnID: 42, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	m, _ := testManager(t, testResolver(info))

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
	info1 := ConnInfo{ConnID: 1, RemoteIP: testIP, PeerDir: domain.PeerDirectionOutbound}
	info2 := ConnInfo{ConnID: 2, RemoteIP: netip.MustParseAddr("198.51.100.7"), PeerDir: domain.PeerDirectionInbound}
	m, _ := testManager(t, testResolver(info1, info2))

	m.StartAll(domain.CaptureFormatCompact)

	snaps := m.AllSessionSnapshots()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
}
