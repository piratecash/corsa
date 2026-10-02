package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
)

// ---------------------------------------------------------------------------
// Fields every caller may read concurrently with Run's start-up.
//
// The application starts the RPC server and the metrics collector BEFORE it
// calls Run (internal/app/node/app.go), and the desktop and SDK runtimes start
// Run on a goroutine of its own and hand the Service to the UI at once. So any
// field a local-frame handler or an RPC provider method reads may be read while
// Run is still starting. A field Run ASSIGNS during start-up is therefore
// published to those readers with no happens-before edge at all.
//
// The race tests below are deterministic under -race rather than probabilistic:
// each reader's FIRST action is the read of the field, so it acquires nothing
// that Run could have released after its write, and the race detector reports
// the pair whichever of the two runs first. The detector reports a given pair
// once per process, so with -count>1 only the first iteration can fail.
// Without -race they still execute both paths and prove nothing more than
// that; the behavioural tests next to them fail without the detector.
// ---------------------------------------------------------------------------

func newStartupPublicationTestService(t *testing.T) *Service {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	// A client node has no listener, so Run reaches its steady state without
	// binding a port; everything this file pins happens before that point.
	svc := NewService(config.Node{
		ListenAddress:  ":0",
		TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
		ChatLogDir:     dir,
		Type:           config.NodeTypeClient,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	return svc
}

// startRunWithoutWaiting starts Run on its own goroutine and returns a stop
// function that cancels it and waits for it to return. It deliberately waits
// for NOTHING in between: a test that waited for readiness would put a
// happens-before edge between Run's start-up and its reader — exactly the edge
// production callers do not have.
func startRunWithoutWaiting(t *testing.T, svc *Service) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(ctx) }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run returned %v for a client node that was only cancelled", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("Run did not return within 15s of its context being cancelled")
		}
	}
	t.Cleanup(stop)
	return stop
}

// awaitRunStartupFinished waits — AFTER the reader under test has done its
// read — until Run has passed every start-up assignment this file pins, so the
// race detector sees both halves of each pair before the test ends. The gossip
// pool flag is the latest of them in Run's order, and only Run sets it.
func awaitRunStartupFinished(t *testing.T, svc *Service) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for !svc.gossipPoolUp.Load() {
		if time.Now().After(deadline) {
			t.Fatal("Run did not start its gossip pool within 15s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func runConcurrentlyWithRunStartup(t *testing.T, svc *Service, reader func()) {
	t.Helper()

	readerDone := make(chan struct{})
	startRunWithoutWaiting(t, svc)
	go func() {
		defer close(readerDone)
		reader()
	}()

	select {
	case <-readerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the reader did not return within 15s")
	}
	awaitRunStartupFinished(t, svc)
}

// TestCaptureRPCDuringRunStartupIsNotADataRace pins the reported race: Run
// used to assign s.captureManager in initCaptureManager while every capture
// RPC (and fetch_peer_health, through peerHealthFrames) read it with no
// synchronisation.
func TestCaptureRPCDuringRunStartupIsNotADataRace(t *testing.T) {
	t.Parallel()

	svc := newStartupPublicationTestService(t)
	runConcurrentlyWithRunStartup(t, svc, func() {
		// The error is irrelevant: before the fix it is "not available", after
		// it there is simply nothing to stop. The read is what is under test.
		_, _ = svc.StopCaptureAll()
	})
}

// TestTheCaptureManagerExistsBeforeRun is the behavioural half: the manager is
// built where no concurrent reader can exist, so an RPC that arrives before
// Run reaches the same manager Run later drives instead of being told that
// capture is unavailable.
func TestTheCaptureManagerExistsBeforeRun(t *testing.T) {
	t.Parallel()

	svc := newStartupPublicationTestService(t)
	before := svc.CaptureManager()
	if before == nil {
		t.Fatal("the capture manager is nil before Run: a capture RPC served before Run is refused, and Run has to publish the field to readers it cannot synchronise with")
	}
	if _, err := svc.StopCaptureAll(); err != nil {
		t.Fatalf("StopCaptureAll before Run: %v", err)
	}

	stop := startRunWithoutWaiting(t, svc)
	awaitRunStartupFinished(t, svc)
	if after := svc.CaptureManager(); after != before {
		t.Fatal("Run replaced the capture manager: the field is written after construction, so readers that started before Run race that write")
	}
	stop()
}

// TestReadingTheLifecycleContextDuringRunStartupIsNotADataRace pins the same
// shape for s.runCtx: Run used to assign it while handlers reachable before
// Run (send_message → storeIncomingMessage, the sender-key recovery path, the
// eager peer-health rebuild) read it.
func TestReadingTheLifecycleContextDuringRunStartupIsNotADataRace(t *testing.T) {
	t.Parallel()

	svc := newStartupPublicationTestService(t)
	// Its first action is the read of s.runCtx.
	runConcurrentlyWithRunStartup(t, svc, svc.refreshHotReadSnapshotsAfterPeerStateChange)
}

// TestTheLifecycleContextTakenBeforeRunEndsWithRun is the behavioural half for
// s.runCtx. Work started before Run — triggerSenderKeySyncAsync parents its
// goroutine on s.runCtx — must still be bounded by the Service lifecycle. When
// Run replaced the field, such work held a context nothing would ever cancel.
func TestTheLifecycleContextTakenBeforeRunEndsWithRun(t *testing.T) {
	t.Parallel()

	svc := newStartupPublicationTestService(t)
	takenBeforeRun := svc.runCtx

	stop := startRunWithoutWaiting(t, svc)
	awaitRunStartupFinished(t, svc)
	if err := takenBeforeRun.Err(); err != nil {
		t.Fatalf("the lifecycle context ended while Run was running: %v", err)
	}
	stop()

	if takenBeforeRun.Err() == nil {
		t.Fatal("a lifecycle context taken before Run is still live after Run returned: work bound to it outlives the Service")
	}
}

// TestGossipDispatchDuringRunStartupIsNotADataRace pins the gossip lanes.
// startGossipDispatch assigns s.gossipJobs and publishes them with the
// gossipPoolUp store; the dispatcher read the channel BEFORE loading that flag,
// so its read was unordered with the assignment — and startGossipDispatch runs
// only after the connection manager is Ready, so even a caller that waits for
// Ready is exposed.
func TestGossipDispatchDuringRunStartupIsNotADataRace(t *testing.T) {
	t.Parallel()

	svc := newStartupPublicationTestService(t)
	runConcurrentlyWithRunStartup(t, svc, func() {
		svc.dispatchGossipSend(func() {})
		svc.dispatchGossipNoticeSend(func() {})
	})
}
