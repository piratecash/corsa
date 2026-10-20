//go:build linux

package loadstand

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"

	"golang.org/x/sys/unix"
)

func TestLinuxNamespaceWrongKindAndZeroClose(t *testing.T) {
	file, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, file)
	if _, err := namespaceIdentity(int(file.Fd())); err == nil {
		t.Fatal("regular FD minted namespace")
	}
	var namespace OwnedNetNS
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := namespace.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open("/proc/self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, kindErr := namespaceIdentity(fd)
	closeErr := unix.Close(fd)
	if kindErr == nil || closeErr != nil {
		t.Fatal("wrong namespace kind accepted", kindErr, closeErr)
	}
}

// A no-launch decision resolves ownership, but never fabricates a successful
// child Wait. The public projection must retain that distinction.
func TestLinuxNeverStartedDoesNotInventHelperWait(t *testing.T) {
	owner := &LinuxFixture{startResolved: make(chan struct{}), helperWaitDone: make(chan struct{})}
	owner.resolveNeverStarted()
	state := owner.Snapshot()
	if state.HelperState != LinuxHelperNotStarted || state.HelperWaitKnown || state.HelperWait.Kind != WaitUnknown || state.Finality != FinalUnavailable {
		t.Fatal("no-launch resolution fabricated actual helper Wait", state)
	}
	select {
	case <-owner.helperWaitDone:
	default:
		t.Fatal("no-launch decision left cleanup unresolved")
	}
}

// This fixture tests channel ownership only. It never launches a helper or
// fabricates Cmd.Wait; HelperWaitKnown must remain false for every wire input.
func linuxTestRootWire(t *testing.T) (*LinuxFixture, *linuxPacketConn, *linuxPacketConn) {
	t.Helper()
	return linuxTestRootWirePhase(t, false)
}

// Test-only wire phase: no namespace/process/proc observation is minted here.
func linuxTestRootWirePhase(t *testing.T, initializing bool) (*LinuxFixture, *linuxPacketConn, *linuxPacketConn) {
	t.Helper()
	life, peerLife := linuxTestPacketPair(t)
	work, peerWork := linuxTestPacketPair(t)
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := NewRPCClient(RPCOptions{Address: "127.0.0.1:1", Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	owner := &LinuxFixture{life: life, work: work, rpc: rpc, startCalled: true, running: true,
		correlation: linuxCorrelation{nonce: LaunchNonce{1}, ordinal: 1, lastID: 1},
		initialized: make(chan struct{}), startResolved: make(chan struct{}), helperWaitDone: make(chan struct{}),
		lifeDone: make(chan struct{}), workDone: make(chan struct{}), stoppingDone: make(chan struct{}), exchanges: make(chan linuxSampleExchange)}
	if initializing {
		owner.running = false
		owner.spec = LinuxFixtureSpec{UID: 65532, GID: 65532,
			TargetSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			Config:       ChildConfig{DataDir: t.TempDir(), Listen: "127.0.0.1:42050", RPC: "127.0.0.1:42051", Bootstrap: []domain.PeerAddress{"127.0.0.1:1"}, Listener: false, Procs: 1}}
		owner.parent = &linuxProfileLease{device: 1, inode: 2}
		owner.namespace = &OwnedNetNS{identity: linuxNamespaceIdentity{device: 3, inode: 4}}
	} else {
		owner.initializationOnce.Do(func() { close(owner.initialized) })
	}
	owner.resolvedOnce.Do(func() { close(owner.startResolved) })
	go owner.receiveLifecycle()
	go owner.runWork()
	t.Cleanup(func() {
		rpc.Close()
		if err := life.close(); err != nil {
			t.Error("managed lifecycle close failed", err)
		}
		if err := work.close(); err != nil {
			t.Error("managed work close failed", err)
		}
		// Teardown can wake an initializer which never received any reply.
		// Both read owners see an already-managed-closed endpoint here.
		owner.initializationOnce.Do(func() { close(owner.initialized) })
		owner.poison()
		for _, done := range []<-chan struct{}{owner.lifeDone, owner.workDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("fixed wire owner did not join after managed close")
			}
		}
	})
	return owner, peerLife, peerWork
}

func TestLinuxLifecycleDefectStopsButNeverInventsWait(t *testing.T) {
	owner, peer, _ := linuxTestRootWire(t)
	packet, err := encodeLinuxPacket(linuxDefect, 0, 1, linuxTerminal{Nonce: owner.correlation.nonce, Code: linuxResultUnavailable})
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.write(packet, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	stop, err := peer.read(time.Now().Add(time.Second))
	if err != nil || stop.kind != linuxStop || stop.id <= 1 {
		t.Fatal("authenticated defect did not submit independent Stop", err)
	}
	state := owner.Snapshot()
	if !state.Poisoned || state.HelperWaitKnown || state.TargetKnown || state.Finality != FinalUnavailable {
		t.Fatal("wire defect fabricated terminal ownership", state)
	}
}

func TestLinuxSampleTimeoutPoisonsAndNeverReusesReply(t *testing.T) {
	owner, _, peer := linuxTestRootWire(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	outcome := make(chan error, 1)
	go func() { _, err := owner.Sample(ctx); outcome <- err }()
	request, err := peer.read(time.Now().Add(time.Second))
	if err != nil || request.kind != linuxSample || request.id <= 1 {
		t.Fatal("sample exchange not admitted", err)
	}
	select {
	case err := <-outcome:
		if err == nil {
			t.Fatal("missing reply became a sample")
		}
	case <-time.After(time.Second):
		t.Fatal("caller waited for a missing reply beyond its context")
	}
	if _, err := owner.Sample(t.Context()); !errors.Is(err, ErrLinuxOwnerBound) {
		t.Fatal("deadline-free work admitted", err)
	}
	next, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if _, err := owner.Sample(next); !errors.Is(err, ErrLinuxOwnerPoisoned) {
		t.Fatal("timed-out owner reused its exchange for sample B", err)
	}
	if owner.Snapshot().HelperWaitKnown {
		t.Fatal("sample cancellation fabricated helper Wait")
	}
}

func TestLinuxNamespaceTransferredTCPRetainsSingleCallerOwner(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, listener)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	connection, err := (&net.Dialer{}).DialTCP(ctx, "tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0), listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, connection)
	peer, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, peer)
	owner := &LinuxFixture{stoppingDone: make(chan struct{})}
	offer := &linuxDialOffer{conn: connection, notice: make(chan struct{}), ack: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- owner.offerDial(ctx, offer) }()
	select {
	case <-offer.notice:
	case <-ctx.Done():
		t.Fatal("socket offer not delivered")
	}
	owner.mu.Lock()
	offer.phase = linuxDialTransferred
	close(offer.ack)
	owner.mu.Unlock()
	cancel() // a cancellation after the transfer cannot revoke the caller socket
	if err := <-result; err != nil {
		t.Fatal("worker closed an already transferred socket", err)
	}
	if err := connection.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		t.Fatal("transferred socket was closed by worker", err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var value [1]byte
	if _, err := peer.Read(value[:]); err != nil || value[0] != 1 {
		t.Fatal("transferred socket lost its single live owner", err)
	}
}

// Actual private packet reader plus the same launch gate, paused at the
// preflight/claim boundary. This is Linux channel behavior, not a full helper
// namespace/credentials or SDK-process claim; native execution is separate.
func TestLinuxStartPreflightReaderObservesStopBeforeClaim(t *testing.T) {
	for name, stopPacket := range map[string]bool{"stop": true, "eof": false} {
		t.Run(name, func(t *testing.T) {
			helper, root := linuxTestPacketPair(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			workCtx, cancelWork := context.WithCancel(ctx)
			defer cancelWork()
			r := &linuxHelperRuntime{life: helper, init: linuxInit{Nonce: LaunchNonce{1}}, startup: newLinuxStartClaim(), cancelWork: cancelWork, lifeDone: make(chan struct{})}
			go r.readLifecycle()
			t.Cleanup(func() {
				if err := r.joinLifecycleReader(); err != nil {
					t.Error("early lifecycle owner did not join", err)
				}
			})
			if stopPacket {
				packet, err := encodeLinuxPacket(linuxStop, 2, 1, linuxStopRequest{Nonce: r.init.Nonce, BudgetNanos: int64(time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				if err := root.write(packet, time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			if err := root.conn.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-workCtx.Done(): // the real reader committed its stop latch
			case <-ctx.Done():
				t.Fatal("early lifecycle reader waited for Process publication")
			}
			if r.startup.claim() {
				process := linuxTestClaimProcess(t)
				if err := r.startup.publish(process); err != nil {
					t.Fatal(err)
				}
				t.Fatal("actual packet Stop/EOF before claim started a real command")
			}
			if r.process != nil || r.startup.snapshot().phase != linuxStartDenied {
				t.Fatal("preflight denial published a Process")
			}
		})
	}
}

func TestLinuxNamespaceRepeatedManagedCloseRetainsResult(t *testing.T) {
	owner, _ := linuxTestPacketPair(t)
	first := owner.close()
	if first != nil || owner.close() != first {
		t.Fatal("managed close did not retain its one actual result", first)
	}
	broken, original, err := newLinuxPacketPair()
	if err != nil {
		t.Fatal(err)
	}
	defer linuxTestClose(t, original)
	if err := broken.conn.Close(); err != nil {
		t.Fatal(err)
	}
	unexpected := broken.close()
	if unexpected == nil || broken.close() != unexpected {
		t.Fatal("unexpected raw-owner close was hidden or retried", unexpected)
	}
}

// Actual managed packet exchange plus the SAME work owner and Start waiter.
// These fixtures prove wire/result ordering, never actual helper Wait or PID.
func linuxTestBeginInitialization(t *testing.T, owner *LinuxFixture, peer *linuxPacketConn, ctx context.Context) linuxInit {
	t.Helper()
	select {
	case owner.exchanges <- linuxSampleExchange{initialize: true, ctx: ctx}:
	case <-ctx.Done():
		t.Fatal("initializer was not admitted", ctx.Err())
	}
	deadline, _ := ctx.Deadline()
	packet, err := peer.read(deadline)
	if err != nil {
		t.Fatal("actual Init request missing", err)
	}
	init, err := decodeLinuxBody[linuxInit](packet)
	if err != nil || packet.kind != linuxInitPublic || packet.id != 1 || packet.ordinal != 1 || init.Nonce != owner.correlation.nonce || init.Role != "loadstand-linux-helper-v4" {
		t.Fatal("unexpected Init request binding", err)
	}
	return init
}
func linuxTestSendReady(t *testing.T, peer *linuxPacketConn, init linuxInit, code linuxResultCode) {
	t.Helper()
	ready := linuxReady{Nonce: init.Nonce, Code: code}
	if code == linuxResultReady {
		ready.Provenance = LinuxProcessProvenance{UID: init.UID, RunningExecutableSHA256: init.TargetHash,
			RunningExecutableVerified: true, KernelStartTimeAvailable: true, Incarnation: ProcessIncarnation{PID: 1, StartTicks: 1}}
	}
	packet, err := encodeLinuxPacket(linuxInitReady, 1, 1, ready)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.write(packet, time.Now().Add(time.Second)); err != nil {
		t.Fatal("actual InitReady write failed", err)
	}
}
func linuxTestWaitSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owned phase did not complete")
	}
}
func linuxTestNoWireWaitAuthority(t *testing.T, owner *LinuxFixture) {
	t.Helper()
	result := owner.Snapshot()
	if result.TargetKnown || result.HelperJoined || result.HelperWaitKnown || result.Finality != FinalUnavailable {
		t.Fatal("initialization packet fabricated actual process/join/Wait authority", result)
	}
}

func TestLinuxInitializationFailureSurvivesStoppingFirst(t *testing.T) {
	for _, name := range []string{"unsupported", "wrong-nonce", "wrong-kind", "malformed", "eof"} {
		t.Run(name, func(t *testing.T) {
			owner, peer, _ := linuxTestRootWirePhase(t, true)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			init := linuxTestBeginInitialization(t, owner, peer, ctx)
			want := ErrLinuxOwnerProtocol
			if name == "eof" {
				if err := peer.close(); err != nil {
					t.Fatal(err)
				}
				want = io.EOF
			} else {
				ready := linuxReady{Nonce: init.Nonce, Code: linuxResultUnsupported}
				kind := linuxInitReady
				var body any = ready
				switch name {
				case "unsupported":
					want = ErrLinuxOwnerUnsupported
				case "wrong-nonce":
					ready.Nonce = LaunchNonce{2}
					body = ready
				case "wrong-kind":
					kind = linuxSampleResult
				case "malformed":
					body = map[string]any{"Nonce": init.Nonce, "Code": linuxResultUnsupported} // required Provenance absent
				}
				packet, err := encodeLinuxPacket(kind, 1, 1, body)
				if err != nil {
					t.Fatal(err)
				}
				if err := peer.write(packet, time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			// No select lottery: the old payload sender is gone before this waiter.
			linuxTestWaitSignal(t, owner.initialized)
			linuxTestWaitSignal(t, owner.stoppingDone)
			linuxTestWaitSignal(t, owner.workDone)
			owner.mu.Lock()
			first := owner.initResult
			owner.mu.Unlock()
			if err := owner.awaitInitialization(ctx); !errors.Is(err, want) || (name != "unsupported" && errors.Is(err, ErrLinuxOwnerUnsupported)) {
				t.Fatal("observed initialization failure lost behind Stop", name, err)
			}
			if err := owner.requestStop(time.Second); err != nil {
				t.Fatal("owned Stop submission failed", err)
			}
			owner.mu.Lock()
			retained := owner.initResult
			owner.mu.Unlock()
			if !first.known || !retained.known || retained.err != first.err {
				t.Fatal("Stop overwrote immutable init result")
			}
			linuxTestNoWireWaitAuthority(t, owner)
		})
	}
}

func TestLinuxInitializationReadyDoesNotOverrideStopOrCancel(t *testing.T) {
	for _, name := range []string{"ready-stop", "ready-cancel", "pending-stop", "pending-cancel", "never-started"} {
		t.Run(name, func(t *testing.T) {
			owner, peer, _ := linuxTestRootWirePhase(t, true)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if name == "never-started" {
				owner.resolveNeverStarted()
				owner.initializationOnce.Do(func() { close(owner.initialized) })
				if err := owner.awaitInitialization(ctx); err == nil || errors.Is(err, ErrLinuxOwnerUnsupported) {
					t.Fatal("NeverStarted notification fabricated initialization", err)
				}
				linuxTestNoWireWaitAuthority(t, owner)
				return
			}
			init := linuxTestBeginInitialization(t, owner, peer, ctx)
			if name == "ready-stop" || name == "ready-cancel" {
				linuxTestSendReady(t, peer, init, linuxResultReady)
				linuxTestWaitSignal(t, owner.initialized)
			}
			if name == "ready-cancel" || name == "pending-cancel" {
				cancel()
			} else {
				if err := owner.requestStop(time.Second); err != nil {
					t.Fatal("owned Stop submission failed", err)
				}
			}
			if err := owner.awaitInitialization(ctx); err == nil || errors.Is(err, ErrLinuxOwnerUnsupported) {
				t.Fatal("Stop/cancel revived Ready or fabricated Unsupported", name, err)
			}
			if name == "pending-stop" || name == "pending-cancel" {
				// A late real packet may be unread or parsed, but can never reopen.
				linuxTestSendReady(t, peer, init, linuxResultReady)
				linuxTestWaitSignal(t, owner.initialized)
			}
			live, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			if err := owner.awaitInitialization(live); err == nil {
				t.Fatal("late Ready reopened a stopped owner")
			}
			if _, err := owner.Sample(live); !errors.Is(err, ErrLinuxOwnerPoisoned) {
				t.Fatal("stopped initializer admitted new Sample", err)
			}
			linuxTestNoWireWaitAuthority(t, owner)
		})
	}
}

func TestLinuxInitializationErrorSurvivesLaterCancellation(t *testing.T) {
	owner, peer, _ := linuxTestRootWirePhase(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	init := linuxTestBeginInitialization(t, owner, peer, ctx)
	linuxTestSendReady(t, peer, init, linuxResultUnsupported)
	linuxTestWaitSignal(t, owner.initialized)
	linuxTestWaitSignal(t, owner.workDone)
	cancel()
	if err := owner.awaitInitialization(ctx); !errors.Is(err, ErrLinuxOwnerUnsupported) || !errors.Is(err, context.Canceled) {
		t.Fatal("later cancellation erased observed initialization error", err)
	}
	linuxTestNoWireWaitAuthority(t, owner)
}

func TestLinuxInitializationReadyReleasesWorkOwner(t *testing.T) {
	owner, peer, workPeer := linuxTestRootWirePhase(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	init := linuxTestBeginInitialization(t, owner, peer, ctx)
	linuxTestSendReady(t, peer, init, linuxResultReady)
	if err := owner.awaitInitialization(ctx); err != nil {
		t.Fatal("actual Ready was not available", err)
	}
	outcome, joined := make(chan error, 1), make(chan struct{})
	go func() { _, err := owner.Sample(ctx); outcome <- err; close(joined) }()
	t.Cleanup(func() {
		cancel()
		if err := owner.work.close(); err != nil {
			t.Error("managed work close failed", err)
		}
		linuxTestWaitSignal(t, joined)
	})
	deadline, _ := ctx.Deadline()
	packet, err := workPeer.read(deadline)
	if err != nil || packet.kind != linuxSample || packet.id <= 1 || packet.ordinal != 1 {
		t.Fatal("work owner stranded on obsolete init-result send", err)
	}
	request, err := decodeLinuxBody[linuxSampleRequest](packet)
	if err != nil || request.Nonce != init.Nonce {
		t.Fatal("Sample request binding mismatch", err)
	}
	if err := workPeer.close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-outcome:
		if err == nil {
			t.Fatal("missing actual Sample response became measurement")
		}
	case <-ctx.Done():
		t.Fatal("owned Sample did not complete", ctx.Err())
	}
	linuxTestNoWireWaitAuthority(t, owner)
}

func TestLinuxNamespaceTaskRetirementConjunction(t *testing.T) {
	expected := linuxThreadIdentity{tid: 42, start: 99}
	absent := linuxTaskObservation{presence: linuxTaskAbsent}
	present := linuxTaskObservation{presence: linuxTaskPresent, identity: ProcessIncarnation{PID: 42, StartTicks: 99}, state: 'S'}
	reused := present
	reused.identity.StartTicks++
	wrongPID := present
	wrongPID.identity.PID++
	unknown := linuxTaskObservation{err: unix.EACCES}
	cases := []struct {
		name          string
		held, current linuxTaskObservation
		gone, invalid bool
	}{
		{"both-absent", absent, absent, true, false},
		{"held-absent-current-reused", absent, reused, true, false},
		{"held-absent-current-same", absent, present, false, false},
		{"readable-old-current-absent", present, absent, false, false},
		{"readable-old-current-reused", present, reused, false, false},
		{"readable-old-current-same", present, present, false, false},
		{"held-unknown", unknown, absent, false, true},
		{"current-unknown", absent, unknown, false, true},
		{"wrong-current-pid", absent, wrongPID, false, true},
		{"wrong-held-incarnation", reused, absent, false, true},
		{"zero-presence-not-absence", linuxTaskObservation{}, absent, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gone, err := linuxTaskRetired(expected, tc.held, tc.current)
			if gone != tc.gone || (err != nil) != tc.invalid {
				t.Fatalf("task proof changed: gone=%t err=%v", gone, err)
			}
		})
	}
	for _, state := range []byte{'Z', 'X'} {
		observation := present
		observation.state = state
		if gone, err := linuxTaskRetired(expected, observation, reused); gone || err != nil {
			t.Fatal("observable dead-state incarnation became physical absence", state, gone, err)
		}
	}
	for _, err := range []error{unix.EACCES, unix.EIO, context.Canceled, context.DeadlineExceeded, ErrProcessObservation} {
		if linuxTaskAbsentError(err) {
			t.Fatal("unknown error became absence", err)
		}
	}
	if !linuxTaskAbsentError(unix.ESRCH) || !linuxTaskAbsentError(unix.ENOENT) {
		t.Fatal("kernel absence errors lost")
	}
}
func TestLinuxNamespaceTaskStateParsing(t *testing.T) {
	for _, state := range []string{"S", "R", "Z", "X"} {
		raw := strings.Replace(procStatFixture(42, "bounded ) comm", 99), " S ", " "+state+" ", 1)
		result := linuxParseTaskContent([]byte(raw))
		if result.err != nil || result.presence != linuxTaskPresent || result.identity != (ProcessIncarnation{PID: 42, StartTicks: 99}) || result.state != state[0] {
			t.Fatal("diagnostic content lost identity/state", state, result.err)
		}
	}
	for _, raw := range []string{"42 (x) S 0", procStatFixture(0, "x", 99), procStatFixture(42, "x", 0), strings.Replace(procStatFixture(42, "x", 99), " S ", " ? ", 1), strings.Repeat("x", 4097)} {
		if result := linuxParseTaskContent([]byte(raw)); result.presence != linuxTaskUnknown || result.err == nil {
			t.Fatal("unknown task content became proof")
		}
	}
}
