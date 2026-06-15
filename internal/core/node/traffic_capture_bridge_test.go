package node

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// ---------------------------------------------------------------------------
// Minimal in-package test doubles for capture.ConnResolver and capture.Writer
// ---------------------------------------------------------------------------

// stubCaptureResolver answers Manager lookups without a live net.Conn registry.
// Only the methods exercised by the publisher path are implemented.
type stubCaptureResolver struct {
	byID map[domain.ConnID]capture.ConnInfo
}

func (r *stubCaptureResolver) ConnInfoByID(id domain.ConnID) (capture.ConnInfo, bool) {
	info, ok := r.byID[id]
	return info, ok
}

func (r *stubCaptureResolver) ConnInfoByIP(ip netip.Addr) []capture.ConnInfo {
	var out []capture.ConnInfo
	for _, info := range r.byID {
		if info.RemoteIP == ip {
			out = append(out, info)
		}
	}
	return out
}

func (r *stubCaptureResolver) AllConnInfo() []capture.ConnInfo {
	out := make([]capture.ConnInfo, 0, len(r.byID))
	for _, info := range r.byID {
		out = append(out, info)
	}
	return out
}

// memCaptureWriter is an in-memory capture.Writer backed by a bytes.Buffer
// so the test never touches the filesystem.
type memCaptureWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *memCaptureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *memCaptureWriter) Close() error { return nil }
func (w *memCaptureWriter) Sync() error  { return nil }

// memFactory is a capture.WriterFactory that emits fresh memCaptureWriters
// regardless of path.
func memFactory(_ string) (capture.Writer, error) {
	return &memCaptureWriter{}, nil
}

// ---------------------------------------------------------------------------
// Bridge publisher integration test
// ---------------------------------------------------------------------------

// TestTrafficCaptureBridgePublishesStartedStoppedEvents guards the contract
// between Service and NodeStatusMonitor: every successful StartCapture* /
// StopCapture* call must emit the paired ebus event so the monitor can keep
// Recording* live without polling fetchPeerHealth. A regression here would
// freeze the UI "recording" dot between startup probes — the original bug.
func TestTrafficCaptureBridgePublishesStartedStoppedEvents(t *testing.T) {
	bus := ebus.New()
	defer bus.Shutdown()

	// Collect events synchronously so assertions do not race with async
	// subscriber goroutines. TopicCapture*Started/Stopped are fanned out
	// through the same bus, so WithSync() is enough.
	var mu sync.Mutex
	var started []ebus.CaptureSessionStarted
	var stopped []ebus.CaptureSessionStopped

	bus.Subscribe(ebus.TopicCaptureSessionStarted, func(ev ebus.CaptureSessionStarted) {
		mu.Lock()
		defer mu.Unlock()
		started = append(started, ev)
	}, ebus.WithSync())
	bus.Subscribe(ebus.TopicCaptureSessionStopped, func(ev ebus.CaptureSessionStopped) {
		mu.Lock()
		defer mu.Unlock()
		stopped = append(stopped, ev)
	}, ebus.WithSync())

	connID := domain.ConnID(4242)
	remoteIP := netip.MustParseAddr("10.0.0.9")
	resolver := &stubCaptureResolver{
		byID: map[domain.ConnID]capture.ConnInfo{
			connID: {
				ConnID:   connID,
				RemoteIP: remoteIP,
				PeerDir:  domain.PeerDirectionOutbound,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseDir := t.TempDir()
	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      baseDir,
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      memFactory,
	})
	defer mgr.Close()

	// Service is constructed by hand so the test does not need a full
	// identity/config/transport stack — the publisher only reads eventBus
	// and captureManager.
	svc := &Service{
		eventBus:       bus,
		captureManager: mgr,
	}

	// ── Start ──
	startedAtBefore := time.Now()
	raw, err := svc.StartCaptureByConnIDs([]uint64{uint64(connID)}, string(domain.CaptureFormatCompact))
	if err != nil {
		t.Fatalf("StartCaptureByConnIDs: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("StartCaptureByConnIDs returned empty json")
	}

	// Poll — SessionSnapshotByID reports the pending session synchronously,
	// so the event is already published when StartCaptureByConnIDs returns,
	// but the subscriber is sync so the slice is safe to read.
	mu.Lock()
	if len(started) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 started event, got %d", len(started))
	}
	ev := started[0]
	mu.Unlock()

	if ev.ConnID != connID {
		t.Fatalf("started ConnID = %d, want %d", ev.ConnID, connID)
	}
	if ev.FilePath == "" {
		t.Fatal("started FilePath should not be empty")
	}
	if ev.Format != domain.CaptureFormatCompact {
		t.Fatalf("started Format = %q, want %q", ev.Format, domain.CaptureFormatCompact)
	}
	if ev.Scope != domain.CaptureScopeConnID {
		t.Fatalf("started Scope = %q, want %q", ev.Scope, domain.CaptureScopeConnID)
	}
	if ev.StartedAt == nil {
		t.Fatal("started StartedAt should not be nil")
	}
	if ev.StartedAt.Before(startedAtBefore.Add(-time.Second)) {
		t.Fatalf("StartedAt = %v is implausibly old", ev.StartedAt)
	}

	// ── Stop ──
	if _, err := svc.StopCaptureByConnIDs([]uint64{uint64(connID)}); err != nil {
		t.Fatalf("StopCaptureByConnIDs: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(stopped) != 1 {
		t.Fatalf("expected 1 stopped event, got %d", len(stopped))
	}
	if stopped[0].ConnID != connID {
		t.Fatalf("stopped ConnID = %d, want %d", stopped[0].ConnID, connID)
	}
}

// TestTrafficCaptureBridgeNilBusIsSilent guards against panics or stray writes
// when a Service is built without an event bus (legacy corsa-node relay mode).
// The publisher helpers must short-circuit so capture continues to work even
// when no monitor is wired up.
func TestTrafficCaptureBridgeNilBusIsSilent(t *testing.T) {
	connID := domain.ConnID(7)
	resolver := &stubCaptureResolver{
		byID: map[domain.ConnID]capture.ConnInfo{
			connID: {
				ConnID:   connID,
				RemoteIP: netip.MustParseAddr("10.0.0.1"),
				PeerDir:  domain.PeerDirectionInbound,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      memFactory,
	})
	defer mgr.Close()

	svc := &Service{
		captureManager: mgr,
		// eventBus intentionally nil
	}

	// Neither call should panic; publish helpers must short-circuit on
	// eventBus==nil without touching SessionSnapshotByID.
	if _, err := svc.StartCaptureByConnIDs([]uint64{uint64(connID)}, string(domain.CaptureFormatCompact)); err != nil {
		t.Fatalf("StartCaptureByConnIDs: %v", err)
	}
	if _, err := svc.StopCaptureByConnIDs([]uint64{uint64(connID)}); err != nil {
		t.Fatalf("StopCaptureByConnIDs: %v", err)
	}
}

// Compile-time sanity check: the stub satisfies the capture.ConnResolver
// interface. Kept as a cheap guard so a future resolver method addition
// surfaces here instead of at test runtime.
var _ capture.ConnResolver = (*stubCaptureResolver)(nil)

// Compile-time sanity check: the in-memory writer satisfies capture.Writer.
var _ capture.Writer = (*memCaptureWriter)(nil)

// TestTrafficCaptureBridgeStartedPayloadCarriesOverlayIdentity guards the
// Bug 3 race-recovery contract: when a StartCapture emits faster than the
// first TopicPeerHealthChanged for the connection, NodeStatusMonitor still
// has to draw a recording dot. The monitor can do that only if the start
// event carries Address + PeerID + Direction — otherwise there is no
// composite key to materialize a PeerHealth row against. This test
// populates s.conns with a real NetCore and asserts those three fields
// actually travel end-to-end through publishCaptureStarted.
func TestTrafficCaptureBridgeStartedPayloadCarriesOverlayIdentity(t *testing.T) {
	bus := ebus.New()
	defer bus.Shutdown()

	var mu sync.Mutex
	var started []ebus.CaptureSessionStarted
	bus.Subscribe(ebus.TopicCaptureSessionStarted, func(ev ebus.CaptureSessionStarted) {
		mu.Lock()
		defer mu.Unlock()
		started = append(started, ev)
	}, ebus.WithSync())

	const (
		connID         = domain.ConnID(5150)
		overlayAddress = domain.PeerAddress("peer-alpha.overlay:8123")
	)
	peerIdentity := domaintest.ID("fingerprint-alpha-ed25519")
	remoteIP := netip.MustParseAddr("10.0.1.42")

	// Real NetCore so entry.core.Address()/Identity()/Dir() return the
	// values the resolver reads. net.Pipe is enough: writerLoop only needs
	// a conn it can write to (tests never send frames through it). The
	// "left" half is owned by the NetCore — core.Close() closes it; we
	// only close the "right" half directly so the pipe's other end
	// unblocks when the test tears down.
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()

	core := netcore.New(netcore.ConnID(connID), left, netcore.Outbound, netcore.Options{
		Address:  overlayAddress,
		Identity: peerIdentity,
	})
	defer core.Close()

	// Resolver for the capture.Manager — independent of s.conns (the
	// Manager does not know anything about node.Service).
	resolver := &stubCaptureResolver{
		byID: map[domain.ConnID]capture.ConnInfo{
			connID: {
				ConnID:   connID,
				RemoteIP: remoteIP,
				PeerDir:  domain.PeerDirectionOutbound,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      memFactory,
	})
	defer mgr.Close()

	svc := &Service{
		eventBus:        bus,
		captureManager:  mgr,
		conns:           map[netcore.ConnID]*connEntry{netcore.ConnID(connID): {core: core}},
		connIDByNetConn: map[net.Conn]netcore.ConnID{},
	}

	if _, err := svc.StartCaptureByConnIDs([]uint64{uint64(connID)}, string(domain.CaptureFormatCompact)); err != nil {
		t.Fatalf("StartCaptureByConnIDs: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(started) != 1 {
		t.Fatalf("expected 1 started event, got %d", len(started))
	}
	ev := started[0]
	if ev.Address != overlayAddress {
		t.Fatalf("Address = %q, want %q", ev.Address, overlayAddress)
	}
	if ev.PeerID != peerIdentity {
		t.Fatalf("PeerID = %q, want %q", ev.PeerID, peerIdentity)
	}
	if ev.Direction != domain.PeerDirectionOutbound {
		t.Fatalf("Direction = %q, want %q", ev.Direction, domain.PeerDirectionOutbound)
	}
}

// TestTrafficCaptureBridgeStartedPayloadEmptyWhenConnUnregistered
// documents the publisher's graceful-fallback behaviour: when the
// connection disappears between StartCapture and the publish (tear-down
// race), resolveOverlayIdentityByConnID returns zero values and the event
// still fires with empty identity fields. The subscriber treats empty
// Address as "no recovery possible" and leaves the slice untouched, which
// matches the TestApplyCaptureStartedEmptyAddressIsNoOp contract.
func TestTrafficCaptureBridgeStartedPayloadEmptyWhenConnUnregistered(t *testing.T) {
	bus := ebus.New()
	defer bus.Shutdown()

	var mu sync.Mutex
	var started []ebus.CaptureSessionStarted
	bus.Subscribe(ebus.TopicCaptureSessionStarted, func(ev ebus.CaptureSessionStarted) {
		mu.Lock()
		defer mu.Unlock()
		started = append(started, ev)
	}, ebus.WithSync())

	const connID = domain.ConnID(6161)
	remoteIP := netip.MustParseAddr("10.0.2.7")

	resolver := &stubCaptureResolver{
		byID: map[domain.ConnID]capture.ConnInfo{
			connID: {
				ConnID:   connID,
				RemoteIP: remoteIP,
				PeerDir:  domain.PeerDirectionInbound,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      memFactory,
	})
	defer mgr.Close()

	svc := &Service{
		eventBus:        bus,
		captureManager:  mgr,
		conns:           map[netcore.ConnID]*connEntry{}, // intentionally empty
		connIDByNetConn: map[net.Conn]netcore.ConnID{},
	}

	if _, err := svc.StartCaptureByConnIDs([]uint64{uint64(connID)}, string(domain.CaptureFormatCompact)); err != nil {
		t.Fatalf("StartCaptureByConnIDs: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(started) != 1 {
		t.Fatalf("expected 1 started event, got %d", len(started))
	}
	ev := started[0]
	if ev.Address != "" {
		t.Fatalf("Address should be empty when connection is unregistered, got %q", ev.Address)
	}
	if !ev.PeerID.IsZero() {
		t.Fatalf("PeerID should be empty when connection is unregistered, got %q", ev.PeerID)
	}
	if ev.Direction != "" {
		t.Fatalf("Direction should be empty when connection is unregistered, got %q", ev.Direction)
	}
	// ConnID must still be set so the subscriber can at least correlate
	// the stop event to this session.
	if ev.ConnID != connID {
		t.Fatalf("ConnID = %d, want %d", ev.ConnID, connID)
	}
}

// ---------------------------------------------------------------------------
// Startup recording (CORSA_RECORD_ALL_TRAFFIC) — startConfiguredCapture
// ---------------------------------------------------------------------------

// newStartupCaptureService builds the minimal Service + Manager pair that
// Service.Run wires up before opening the listener: an EMPTY resolver stands
// in for the "no peers yet" startup state.
func newStartupCaptureService(t *testing.T, cfg config.Node) (*Service, *capture.Manager, *stubCaptureResolver) {
	t.Helper()
	resolver := &stubCaptureResolver{byID: map[domain.ConnID]capture.ConnInfo{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      memFactory,
	})
	t.Cleanup(mgr.Close)
	return &Service{cfg: cfg, captureManager: mgr}, mgr, resolver
}

// waitForSessionDir blocks until the auto-start goroutine inside
// Manager.OnNewConnection has finished its filesystem work (MkdirAll of the
// session dir happens before the writer attach). Without this barrier the
// test can exit while MkdirAll races t.TempDir's RemoveAll cleanup —
// Manager.Close does not wait for a still-pending session's setup goroutine.
func waitForSessionDir(t *testing.T, filePath string) {
	t.Helper()
	dir := filepath.Dir(filePath)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(dir); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session dir %s not created within deadline", dir)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStartConfiguredCaptureInstallsStandingAllRule guards the startup
// recording contract: startConfiguredCapture must install the standing
// scope=all rule while there are still ZERO connections, so the very first
// handshake of the very first peer is captured — and the auto-start must be
// published on the bus (topics.go contract; NodeStatusMonitor's recording
// UI depends on it). A reorder in Service.Run that starts accepting
// connections first would surface here as the auto-start not firing.
func TestStartConfiguredCaptureInstallsStandingAllRule(t *testing.T) {
	svc, mgr, resolver := newStartupCaptureService(t, config.Node{
		RecordAllTraffic: true,
		// RecordTrafficFormat left empty — must default to compact.
	})

	// Wire a sync bus so the auto-start publish is observable.
	bus := ebus.New()
	defer bus.Shutdown()
	svc.eventBus = bus

	var mu sync.Mutex
	var started []ebus.CaptureSessionStarted
	var stopped []ebus.CaptureSessionStopped
	bus.Subscribe(ebus.TopicCaptureSessionStarted, func(ev ebus.CaptureSessionStarted) {
		mu.Lock()
		defer mu.Unlock()
		started = append(started, ev)
	}, ebus.WithSync())
	bus.Subscribe(ebus.TopicCaptureSessionStopped, func(ev ebus.CaptureSessionStopped) {
		mu.Lock()
		defer mu.Unlock()
		stopped = append(stopped, ev)
	}, ebus.WithSync())

	svc.startConfiguredCapture()

	// Startup state: rule installed, but nothing recording yet.
	if mgr.HasActiveCaptures() {
		t.Fatal("no sessions expected before the first connection")
	}

	// First connection arrives — the standing rule must auto-start capture.
	// OnNewConnection + publishAutoStartedCapture mirror the wiring in
	// notifyCaptureNewConn (the registry/sink half needs a live conn).
	connID := domain.ConnID(1)
	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: netip.MustParseAddr("10.0.0.1"),
		PeerDir:  domain.PeerDirectionInbound,
	}
	resolver.byID[connID] = info
	mgr.OnNewConnection(info)
	svc.publishAutoStartedCapture(connID)

	snap, ok := mgr.SessionSnapshotByID(connID)
	if !ok {
		t.Fatal("standing all-rule did not auto-start capture for the first connection")
	}
	if snap.Scope != domain.CaptureScopeAll {
		t.Fatalf("Scope = %q, want %q", snap.Scope, domain.CaptureScopeAll)
	}
	if snap.Format != domain.CaptureFormatCompact {
		t.Fatalf("Format = %q, want compact (empty RecordTrafficFormat default)", snap.Format)
	}

	// The auto-start must have been published for the status monitor.
	mu.Lock()
	if len(started) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 started event for the auto-started session, got %d", len(started))
	}
	ev := started[0]
	mu.Unlock()
	if ev.ConnID != connID {
		t.Fatalf("started ConnID = %d, want %d", ev.ConnID, connID)
	}
	if ev.Scope != domain.CaptureScopeAll {
		t.Fatalf("started Scope = %q, want %q", ev.Scope, domain.CaptureScopeAll)
	}

	// Barrier against the TempDir cleanup race (see waitForSessionDir).
	waitForSessionDir(t, snap.FilePath)

	// Peer disconnects — the auto-started session has no Stop* RPC call to
	// publish its teardown, so notifyCaptureConnClosed must emit Stopped or
	// the status monitor keeps showing an active recording forever.
	svc.notifyCaptureConnClosed(connID)

	mu.Lock()
	defer mu.Unlock()
	if len(stopped) != 1 {
		t.Fatalf("expected 1 stopped event after connection close, got %d", len(stopped))
	}
	if stopped[0].ConnID != connID {
		t.Fatalf("stopped ConnID = %d, want %d", stopped[0].ConnID, connID)
	}
}

// TestManagerOnSessionEvictedFiresOnAutoStartFailure pins the failure leg of
// the started/stopped pairing: when a standing rule auto-starts a session but
// the deferred writer setup fails (factory error), the Manager must invoke
// OnSessionEvicted so the bridge can publish Stopped — otherwise the status
// monitor shows "recording" for a session that never opened a file.
func TestManagerOnSessionEvictedFiresOnAutoStartFailure(t *testing.T) {
	resolver := &stubCaptureResolver{byID: map[domain.ConnID]capture.ConnInfo{}}

	type eviction struct {
		snap  capture.SessionSnapshot
		cause error
	}
	var mu sync.Mutex
	var evicted []eviction

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory: func(string) (capture.Writer, error) {
			return nil, errors.New("simulated open failure")
		},
		OnSessionEvicted: func(snap capture.SessionSnapshot, cause error) {
			mu.Lock()
			defer mu.Unlock()
			evicted = append(evicted, eviction{snap: snap, cause: cause})
		},
	})
	defer mgr.Close()

	// Standing all-rule installed with zero connections (startup state).
	if res := mgr.StartAll(domain.CaptureFormatCompact); len(res.Errors) != 0 {
		t.Fatalf("StartAll errors: %v", res.Errors)
	}

	connID := domain.ConnID(9)
	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: netip.MustParseAddr("10.0.0.9"),
		PeerDir:  domain.PeerDirectionInbound,
	}
	resolver.byID[connID] = info
	mgr.OnNewConnection(info)

	// The failing factory runs in the deferred setup goroutine — poll.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(evicted)
		var got eviction
		if n > 0 {
			got = evicted[0]
		}
		mu.Unlock()
		if n > 0 {
			if got.snap.ConnID != connID {
				t.Fatalf("evicted ConnID = %d, want %d", got.snap.ConnID, connID)
			}
			// Setup failures must carry the error as cause — the session
			// never recorded one itself, and after eviction the diagnostics
			// are unrecoverable (CaptureSessionStopped contract).
			if got.cause == nil || got.cause.Error() != "simulated open failure" {
				t.Fatalf("evicted cause = %v, want simulated open failure", got.cause)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("OnSessionEvicted not called after auto-start writer failure")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The dead session must be gone so a later start can re-create it.
	if _, ok := mgr.SessionSnapshotByID(connID); ok {
		t.Fatal("failed session still present after eviction")
	}
}

// closeSignalWriter is a capture.Writer whose Close signals a channel —
// used to observe the auto-start goroutine's discard path (attachWriter
// returning false closes the writer it was handed).
type closeSignalWriter struct {
	memCaptureWriter
	closed chan struct{}
	once   sync.Once
}

func (w *closeSignalWriter) Close() error {
	w.once.Do(func() { close(w.closed) })
	return nil
}

// TestManagerOnSessionEvictedSkipsReplacedSession pins the stale-goroutine
// guard: an auto-start stuck in the writer factory must NOT publish an
// eviction after the operator did stop→start for the same conn_id — status
// consumers key sessions by ConnID, so a stale Stopped would mark the live
// replacement session inactive.
func TestManagerOnSessionEvictedSkipsReplacedSession(t *testing.T) {
	connID := domain.ConnID(11)
	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: netip.MustParseAddr("10.0.0.11"),
		PeerDir:  domain.PeerDirectionInbound,
	}
	resolver := &stubCaptureResolver{byID: map[domain.ConnID]capture.ConnInfo{}}

	release := make(chan struct{})
	ready := make(chan struct{})
	discarded := make(chan struct{})
	var factoryMu sync.Mutex
	factoryCalls := 0
	factory := func(string) (capture.Writer, error) {
		factoryMu.Lock()
		factoryCalls++
		first := factoryCalls == 1
		factoryMu.Unlock()
		if first {
			// The auto-start goroutine stalls here while the operator
			// replaces the session underneath it. The test waits on ready
			// before issuing stop→start, so the first call is guaranteed
			// to be the auto-start goroutine — without that barrier the
			// replacement StartByConnIDs could reach the factory first and
			// deadlock on release, which is only closed later.
			close(ready)
			<-release
			return &closeSignalWriter{closed: discarded}, nil
		}
		return &memCaptureWriter{}, nil
	}

	var evictMu sync.Mutex
	evictions := 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := capture.NewManager(ctx, capture.ManagerOpts{
		BaseDir:      t.TempDir(),
		Clock:        time.Now,
		ConnResolver: resolver,
		Factory:      factory,
		OnSessionEvicted: func(capture.SessionSnapshot, error) {
			evictMu.Lock()
			evictions++
			evictMu.Unlock()
		},
	})
	defer mgr.Close()

	// Standing rule, then the connection arrives — auto-start parks in factory.
	mgr.StartAll(domain.CaptureFormatCompact)
	resolver.byID[connID] = info
	mgr.OnNewConnection(info)

	// Barrier: the stop→start below must not run until the auto-start
	// goroutine is provably parked inside the factory (see factory comment).
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("auto-start goroutine never reached the writer factory")
	}

	// Operator: stop (reports teardown via its own result), then start a
	// replacement for the SAME conn_id. The sync start path uses the second
	// factory call, which returns immediately.
	mgr.StopByConnIDs([]domain.ConnID{connID})
	if res := mgr.StartByConnIDs([]domain.ConnID{connID}, domain.CaptureFormatCompact); len(res.Started) != 1 {
		t.Fatalf("replacement start: started=%d, errors=%v", len(res.Started), res.Errors)
	}

	// Wake the parked goroutine. Its session is stopped, so attachWriter
	// fails and the handed writer is closed — that signal tells us the
	// discard path (and its ownership check) has fully run.
	close(release)
	select {
	case <-discarded:
	case <-time.After(5 * time.Second):
		t.Fatal("stale auto-start goroutine did not discard its writer")
	}
	// The writer is closed inside attachWriter, just before the ownership
	// check — give a regressed notify-after-close a moment to surface.
	time.Sleep(50 * time.Millisecond)

	evictMu.Lock()
	got := evictions
	evictMu.Unlock()
	if got != 0 {
		t.Fatalf("stale goroutine published %d eviction(s); replacement session would be marked stopped", got)
	}

	// The replacement session must still be alive and recording.
	snap, ok := mgr.SessionSnapshotByID(connID)
	if !ok || !snap.Recording {
		t.Fatalf("replacement session lost: ok=%v snap=%+v", ok, snap)
	}
}

// TestStartConfiguredCaptureDefaultOff pins the opt-in default: with
// RecordAllTraffic false nothing is installed, and new connections are
// not captured.
func TestStartConfiguredCaptureDefaultOff(t *testing.T) {
	svc, mgr, resolver := newStartupCaptureService(t, config.Node{})

	svc.startConfiguredCapture()

	connID := domain.ConnID(2)
	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: netip.MustParseAddr("10.0.0.2"),
		PeerDir:  domain.PeerDirectionInbound,
	}
	resolver.byID[connID] = info
	mgr.OnNewConnection(info)

	if _, ok := mgr.SessionSnapshotByID(connID); ok {
		t.Fatal("capture session started although RecordAllTraffic is off")
	}
}

// TestStartConfiguredCaptureBadFormatDoesNotInstall pins the failure mode:
// an invalid RecordTrafficFormat is rejected (logged by the caller) and no
// standing rule is installed — the node keeps running without recording.
func TestStartConfiguredCaptureBadFormatDoesNotInstall(t *testing.T) {
	svc, mgr, resolver := newStartupCaptureService(t, config.Node{
		RecordAllTraffic:    true,
		RecordTrafficFormat: "verbose", // not compact|pretty
	})

	svc.startConfiguredCapture()

	connID := domain.ConnID(3)
	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: netip.MustParseAddr("10.0.0.3"),
		PeerDir:  domain.PeerDirectionInbound,
	}
	resolver.byID[connID] = info
	mgr.OnNewConnection(info)

	if _, ok := mgr.SessionSnapshotByID(connID); ok {
		t.Fatal("capture session started although the configured format is invalid")
	}
}
