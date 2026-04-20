package node

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/domain"
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
		peerIdentity   = domain.PeerIdentity("fingerprint-alpha-ed25519")
	)
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
	if ev.PeerID != "" {
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
