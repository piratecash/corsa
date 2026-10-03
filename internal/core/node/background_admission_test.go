package node

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// background_admission_test.go pins the two halves of the shutdown contract
// for work that outlives a single call:
//
//   - once Run has returned, nothing may START: a fire-and-forget job handed
//     to goBackground after that point is refused, so WaitBackground — which
//     callers run after Run, right before they close the stores and delete the
//     data directory — never races a job registering itself from zero;
//   - Run does not return while a goroutine it started is still serving a peer
//     session: the outbound session goroutine onCMSessionEstablished launches
//     is the producer that used to hand goBackground new work after Run had
//     already returned.

// TestBackgroundJobIsRefusedOnceRunHasReturned is the admission half.
//
// Before the gate existed, goBackground called backgroundWg.Add(1)
// unconditionally: a job handed to it after Run returned ran against a
// stopped node — a receipt send, a key-sync dial, a trust-store write after
// the caller had closed the stores — and its Add raced the WaitBackground the
// composition root runs at exactly that moment.
//
// The mutation this kills: dropping the admission check from goBackground, or
// never closing the gate on Run's exit.
func TestBackgroundJobIsRefusedOnceRunHasReturned(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	running := runServiceForTest(t.Context(), svc)
	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("AwaitReady = %v, want nil", err)
	}
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	var ran atomic.Bool
	admitted := svc.goBackground(func() { ran.Store(true) })
	if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("DrainBackground = %v, want nil", err)
	}

	if admitted {
		t.Error("goBackground reported a job handed to it after Run returned as admitted")
	}
	if ran.Load() {
		t.Fatal("a background job handed to goBackground after Run returned was started: " +
			"work must not begin on a stopped node, and its registration races the WaitBackground " +
			"callers run right after Run")
	}
}

// TestBackgroundJobIsAdmittedWhenRunWasNeverCalled pins the other side of the
// gate: it is closed by Run's exit and by nothing else. A Service that was
// never run — the fixture most unit tests use, calling handlers directly and
// joining their jobs with WaitBackground as a barrier — keeps admitting work,
// and WaitBackground itself does not close the gate.
//
// The mutation this kills: closing the gate in WaitBackground, or starting a
// Service with the gate closed.
func TestBackgroundJobIsAdmittedWhenRunWasNeverCalled(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)

	var runs atomic.Int32
	svc.goBackground(func() { runs.Add(1) })
	svc.WaitBackground()
	svc.goBackground(func() { runs.Add(1) })
	svc.WaitBackground()

	if got := runs.Load(); got != 2 {
		t.Fatalf("background jobs run on a Service that was never started = %d, want 2", got)
	}
}

// TestRunWaitsForAnOutboundSessionGoroutineInsideAFrame is the join half.
//
// A client node dials a full node that holds a DM for it. The DM arrives on
// the client's OUTBOUND session and is handed to the message store on the
// goroutine onCMSessionEstablished started; the store parks there. Cancelling
// the client must not let Run return while that goroutine is still inside the
// frame: once released, it goes on to store the message and to schedule the
// delivery receipt on goBackground, and a Run that had already returned would
// see both happen against a node its caller is entitled to tear down.
//
// The store parks only when it is called from that session goroutine (checked
// on the goroutine's own stack), so the premise is the defect's own path and
// not some other caller of storeIncomingMessage.
//
// The mutation this kills: starting the session goroutine with a bare `go`
// instead of on the lifecycle group stopRunLifecycle joins.
func TestRunWaitsForAnOutboundSessionGoroutineInsideAFrame(t *testing.T) {
	t.Parallel()

	addressFull := freeAddress(t)
	addressClient := freeAddress(t)

	idClient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate client identity: %v", err)
	}

	fullNode, stopFull := startTestNode(t, config.Node{
		ListenAddress: addressFull,
		Type:          domain.NodeTypeFull,
	})
	defer stopFull()

	ciphertext, err := directmsg.EncryptForParticipants(
		fullNode.identity,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(idClient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(idClient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "parked-in-session"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	stored := fullNode.HandleLocalFrame(sendMessageFrame("dm", "session-join-dm-1", fullNode.Address(), idClient.Address, "sender-delete", ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("full node did not store the DM for the client: %#v", stored)
	}

	store := newSessionParkingStore()

	dir := t.TempDir()
	client := NewService(deriveTestAdvertisePort(config.Node{
		ListenAddress:     addressClient,
		BootstrapPeers:    []string{normalizeAddress(addressFull)},
		Type:              domain.NodeTypeClient,
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		ChatLogDir:        dir,
		AllowPrivatePeers: true,
	}), idClient, nil)
	client.disableRateLimiting = true
	client.markPeerStateIntervalTest = -1
	client.RegisterMessageStore(store)

	running := runServiceForTest(t.Context(), client)
	// Registered AFTER t.TempDir above, so LIFO stops the node before its
	// directory is removed. The store is released first: a test that failed
	// with the session goroutine still parked would otherwise hold Run's join
	// open for the whole stop budget and delete the directory under a live
	// node.
	t.Cleanup(func() {
		store.releaseAll()
		_ = running.Stop(withBudget(t, harnessGenerousBudget))
		_ = running.DrainBackground(withBudget(t, harnessGenerousBudget))
	})
	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("client AwaitReady = %v, want nil", err)
	}

	select {
	case <-store.parked:
	case <-time.After(30 * time.Second):
		t.Fatal("the DM never reached the message store on the outbound session goroutine: the premise of this test never armed")
	}

	err = running.Stop(withBudget(t, 300*time.Millisecond))
	if err == nil {
		t.Fatal("Run returned while the outbound session goroutine it started was still inside a frame: " +
			"that goroutine goes on to store the message and schedule a delivery receipt against a node " +
			"whose caller is already entitled to tear it down")
	}
	requireStopStage(t, err, stopStageRunExit)

	store.releaseAll()
	if err := running.Stop(withBudget(t, 15*time.Second)); err != nil {
		t.Fatalf("Stop after release = %v, want nil once the session goroutine has left the frame", err)
	}
	if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("DrainBackground = %v, want nil", err)
	}
}

// sessionParkingStore is a MessageStore that holds every call made from an
// outbound CM session goroutine until released, and answers every other
// caller at once.
type sessionParkingStore struct {
	parked      chan struct{}
	parkOnce    sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func newSessionParkingStore() *sessionParkingStore {
	return &sessionParkingStore{
		parked:  make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *sessionParkingStore) StoreMessage(protocol.Envelope, bool) StoreResult {
	if calledFromOutboundSessionGoroutine() {
		s.parkOnce.Do(func() { close(s.parked) })
		<-s.release
	}
	return StoreInserted
}

func (s *sessionParkingStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

func (s *sessionParkingStore) releaseAll() {
	s.releaseOnce.Do(func() { close(s.release) })
}

// calledFromOutboundSessionGoroutine reports whether the calling goroutine is
// the per-session goroutine onCMSessionEstablished starts. The goroutine's
// body is a closure of that method, so its frame names it whatever wrapper
// the goroutine is started through.
func calledFromOutboundSessionGoroutine() bool {
	buf := make([]byte, 64<<10)
	n := runtime.Stack(buf, false)
	return strings.Contains(string(buf[:n]), ".onCMSessionEstablished.func")
}

// failRunBeforeStartup makes Run return at its first check, before any loop or
// pool is started, and waits for it. What is left is a Service whose Run has
// returned and whose gossip pool never came up — the state in which the
// goBackground fallbacks are the only path a job can take.
func failRunBeforeStartup(t *testing.T, svc *Service) {
	t.Helper()

	svc.connBudgetErr = errInjectedRunFailure
	running := runServiceForTest(t.Context(), svc)
	err := running.Stop(withBudget(t, harnessGenerousBudget))
	if !errors.Is(err, errInjectedRunFailure) {
		t.Fatalf("Stop = %v, want Run's injected failure", err)
	}
}

var errInjectedRunFailure = errors.New("injected run failure")

// TestARefusedSenderKeySyncPassReturnsItsSlotExactlyOnce: the pass releases
// the caller's in-flight slot from a defer INSIDE the background job. A job
// that is refused never runs that defer, so the refusal branch has to release
// it — once, not twice: a lost release pins the sender and the previous hop
// in senderKeySyncInFlight for good, a doubled one frees a slot another pass
// may already hold.
//
// The mutation this kills: dropping, or duplicating, the release on the
// refusal branch of runSenderKeySyncPass.
func TestARefusedSenderKeySyncPassReturnsItsSlotExactlyOnce(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	failRunBeforeStartup(t, svc)

	var releases atomic.Int32
	svc.runSenderKeySyncPass("", "sender-after-run", nil, func() { releases.Add(1) })
	svc.WaitBackground()

	if got := releases.Load(); got != 1 {
		t.Fatalf("release called %d times for a pass refused after Run returned, want exactly 1", got)
	}
}

// TestTheGossipFallbackDropsAJobOnceRunHasReturned: with the pool never up,
// tryEnqueueGossipJob hands the job to goBackground. Once Run has returned
// that hand-off is refused, and the enqueue must say so — reporting
// gossipEnqueued for a job that will never run is how a send gets counted as
// gone when it never left.
//
// The mutation this kills: ignoring goBackground's refusal in
// tryEnqueueGossipJob.
func TestTheGossipFallbackDropsAJobOnceRunHasReturned(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	failRunBeforeStartup(t, svc)

	var ran atomic.Bool
	got := svc.tryEnqueueGossipJob(&svc.gossipJobs, func() { ran.Store(true) })
	svc.WaitBackground()

	if got != gossipPoolShutdownDrop {
		t.Errorf("tryEnqueueGossipJob after Run returned = %d, want gossipPoolShutdownDrop (%d)", got, gossipPoolShutdownDrop)
	}
	if ran.Load() {
		t.Error("a gossip job handed to the fallback after Run returned was run")
	}
}

// TestARefusedRouteQueryReleasesItsInFlightSlot: triggerRouteQueryAsync
// reserves the target's in-flight slot before it starts the query, and the
// query releases it from a defer inside the background job. A job refused
// because Run has returned never runs that defer, so the refusal branch has to
// release the slot itself — otherwise the target could never be queried again
// on this Service. The query must not run either: it fans out to sessions of
// a node that has stopped.
//
// The gate is closed directly, as Run's exit closes it: the fixture is a
// struct-literal Service with a routing table and one capable peer, which
// is what the trigger's pre-checks need and what Run cannot be given.
//
// The mutation this kills: starting the query with a bare `go`, or dropping
// the release on the refusal branch.
func TestARefusedRouteQueryReleasesItsInFlightSlot(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})
	svc.closeBackgroundAdmission()

	svc.triggerRouteQueryAsync(idTargetX)
	svc.WaitBackground()

	if svc.queryRateLimit.IsInFlight(idTargetX) {
		t.Error("the in-flight slot of a route query refused after Run returned was never released")
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got != 0 {
		t.Errorf("route query emitted %d stamp(s) after Run returned, want 0", got)
	}
}
