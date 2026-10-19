package loadstand

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

func privateAddress(t *testing.T) domain.ListenAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return domain.ListenAddress(address)
}
func stopChild(t *testing.T, child *Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer cancel()
	if err := child.Stop(ctx); err != nil {
		t.Error(err)
	}
	if err := child.Wait(ctx); err != nil {
		t.Error(err)
	}
	if err := child.ReleaseArtifacts(ctx); err != nil {
		t.Error(err)
	}
}
func startChild(t *testing.T, ctx context.Context, options ProcessOptions) *Process {
	t.Helper()
	child, err := StartLoopbackProcess(ctx, options)
	if err != nil {
		if child != nil {
			stopChild(t, child)
		}
		t.Fatalf("start SDK child (public log %s): %v", filepath.Join(options.Config.DataDir, "child.log"), err)
	}
	t.Cleanup(func() { stopChild(t, child) })
	return child
}
func bindActualChild(t *testing.T, ctx context.Context, p *Process, ordinal IncarnationOrdinal) (CompletedStartBinding, TransportHighWater) {
	t.Helper()
	status, err := NewProcessSource(p.RPC()).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt := StartReceipt{Public: PublicIdentity{Identity: domain.PeerIdentityFromWire(status.Address), PublicKey: status.PublicKey, BoxPublicKey: status.BoxPublicKey}, Incarnation: Incarnation{Ordinal: ordinal, StartedAt: status.StartedAt}}
	var frame protocol.Frame
	if err := p.RPC().Call(ctx, "fetchContacts", nil, &frame); err != nil {
		t.Fatal(err)
	}
	for _, c := range frame.Contacts {
		if c.Address == status.Address && c.PubKey == status.PublicKey && c.BoxKey == status.BoxPublicKey {
			receipt.Public.BoxSignature = c.BoxSig
		}
	}
	b, err := p.CompletedStart(receipt)
	if err != nil {
		t.Fatal(err)
	}
	prior := NewTransportHighWater(b.RuntimeBinding())
	stats, err := NewProcessSource(p.RPC()).TransportTraffic(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prior.Observe(stats, nil)
	return b, prior
}
func stopActualChildFinal(t *testing.T, ctx context.Context, p *Process, b CompletedStartBinding, prior TransportHighWater) FinalAccountingResult {
	t.Helper()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	exit, err := p.ExitResult()
	if err != nil || !exit.Clean() || exit.Usage.Scope != OwnedCommandUsage {
		t.Fatal("unclean or falsely scoped owned-command exit", exit, err)
	}
	final, err := p.FinalAccounting(ctx, b, prior)
	if err != nil || final.Finality != FinalExact {
		t.Fatal("actual SDK final report unavailable", final, err)
	}
	again, err := p.FinalAccounting(ctx, b, prior)
	if err != nil || again != final {
		t.Fatal("actual final report not immutable", again, err)
	}
	return final
}
func awaitChat(t *testing.T, ctx context.Context, child *Process, peer, body string, id domain.MessageID) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var entries []chatlog.Entry
		err := child.RPC().Call(ctx, "fetchChatlog", map[string]string{"topic": "dm", "peer_address": peer}, &entries)
		if err == nil {
			for _, entry := range entries {
				if entry.ID == string(id) && entry.Body != body && entry.Body != "" && entry.Sender == peer {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SDK SQLite chatlog missing normal DM: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
func awaitConnected(t *testing.T, ctx context.Context, child *Process) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		peers, err := NewProcessSource(child.RPC()).ConnectedPeers(ctx)
		if err == nil && len(peers) > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("peer readiness: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
func TestSDKChildNormalDMGracefulRestartAndPrivateArtifacts(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS loopback functional sandbox; Linux needs real namespace runner")
	}
	if testing.Short() {
		t.Skip("real SDK child build/process smoke")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	root := t.TempDir()
	binary := filepath.Join(root, "sdkedge")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	build := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "./sdkedge")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("SDK child build: %v: %s", err, output)
	}
	leftListen, rightListen := privateAddress(t), privateAddress(t)
	makeOptions := func(name string, listen, remote domain.ListenAddress) ProcessOptions {
		credentials, err := NewCredentials()
		if err != nil {
			t.Fatal(err)
		}
		return ProcessOptions{Binary: binary, Config: ChildConfig{DataDir: filepath.Join(root, name), Listen: listen, RPC: privateAddress(t), Bootstrap: []domain.PeerAddress{domain.PeerAddress(remote)}, Listener: true, Procs: 1}, Credentials: credentials}
	}
	leftOptions := makeOptions("left", leftListen, rightListen)
	rightOptions := makeOptions("right", rightListen, leftListen)
	left := startChild(t, ctx, leftOptions)
	right := startChild(t, ctx, rightOptions)
	leftStatus, err := NewProcessSource(left.RPC()).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	boundSource, bindErr := NewBoundProcessSource(left.RPC(), runjournal.SystemClock{}, RuntimeBinding{Identity: domain.PeerIdentityFromWire(leftStatus.Identity), StartedAt: leftStatus.StartedAt})
	if bindErr != nil {
		t.Fatal(bindErr)
	}
	reading, offErr := boundSource.DatagramSummary(ctx)
	if offErr != nil {
		t.Fatalf("actual configured-off SDK process is not authoritatively observable: %v", offErr)
	}
	if _, offErr = reading.Get(); !errors.Is(offErr, ErrDatagramDisabled) {
		t.Fatalf("actual configured-off process was not explicit disabled: %v", offErr)
	}
	rightStatus, err := NewProcessSource(right.RPC()).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range []*Process{left, right} {
		awaitConnected(t, ctx, child)
		metrics, err := NewProcessSource(child.RPC()).Runtime(ctx)
		if err != nil {
			t.Fatal(err)
		}
		source := NewProcessSource(child.RPC())
		traffic, err := source.TransportTraffic(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sessions, err := source.SessionOutcomes(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bans, err := source.BanObservation(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if traffic.StartedAt != metrics.StartedAt || sessions.StartedAt != metrics.StartedAt || bans.StartedAt != metrics.StartedAt {
			t.Fatal("transport adapters mixed process incarnations")
		}
		if _, err := source.RouteEntries(ctx); err != nil {
			t.Fatal(err)
		}
		if *metrics.GOMAXPROCS != 1 {
			t.Fatal("sealed per-process GOMAXPROCS lost")
		}
		provenance := child.Provenance()
		if provenance.PID <= 0 || len(provenance.LaunchFileSHA256) != 64 || provenance.RunningExecutableVerified || provenance.KernelStartTimeAvailable {
			t.Fatal("invalid or overstated process provenance")
		}
	}
	leftBinding, leftPrior := bindActualChild(t, ctx, left, 1)
	rightBinding, rightPrior := bindActualChild(t, ctx, right, 1)
	body := "sdk-process-sender-owned-normal-dm"
	if err := left.RPC().Call(ctx, "sendDm", map[string]string{"to": rightStatus.Address, "body": body}, new(any)); err != nil {
		t.Fatal(err)
	}
	receivedID := awaitReceipt(t, ctx, rightOptions.Config.DataDir, leftStatus.Address, body)
	awaitChat(t, ctx, right, leftStatus.Address, body, receivedID)
	// Graceful SIGTERM closes the real DesktopClient/router/bus and SQLite before
	// a new process opens the same profile. Parent never reads identity secrets.
	rightFinal := stopActualChildFinal(t, ctx, right, rightBinding, rightPrior)
	if rightFinal.Traffic.BytesReceived <= rightPrior.received {
		t.Fatal("actual post-sample DM tail was lost")
	}
	sqlite, err := os.ReadFile(filepath.Join(rightOptions.Config.DataDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(sqlite, []byte("SQLite format 3\x00")) {
		t.Fatal("SDK child did not use SQLite")
	}
	identity, err := domain.ParsePeerIdentity(rightStatus.Address)
	if err != nil {
		t.Fatal(err)
	}
	rightOptions.ExpectedIdentity = &identity
	rightAgain := startChild(t, ctx, rightOptions)
	if err := right.RPC().Call(ctx, "getNodeStatus", nil, new(any)); !errors.Is(err, ErrRPCClosed) {
		t.Fatal("old process handle crossed restart")
	}
	staleSource, err := NewBoundProcessSource(rightAgain.RPC(), runjournal.SystemClock{}, RuntimeBinding{Identity: identity, StartedAt: rightStatus.StartedAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staleSource.DatagramSummary(ctx); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("old receipt authorized off on restarted process: %v", err)
	}
	againStatus, err := NewProcessSource(rightAgain.RPC()).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	againSource, err := NewBoundProcessSource(rightAgain.RPC(), runjournal.SystemClock{}, RuntimeBinding{Identity: identity, StartedAt: againStatus.StartedAt})
	if err != nil {
		t.Fatal(err)
	}
	againReading, err := againSource.DatagramSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := againReading.Get(); !errors.Is(err, ErrDatagramDisabled) {
		t.Fatal("fresh restart receipt failed", err)
	}
	if rightAgain.Provenance().PID == right.Provenance().PID {
		t.Fatal("restart reused process lifetime")
	}
	againBinding, againPrior := bindActualChild(t, ctx, rightAgain, 2)
	if againBinding.launch.nonce == rightBinding.launch.nonce {
		t.Fatal("restart reused launch nonce")
	}
	awaitChat(t, ctx, rightAgain, leftStatus.Address, body, receivedID)
	awaitConnected(t, ctx, rightAgain)
	reply := "sdk-process-restarted-sender-owned-reply"
	if err := rightAgain.RPC().Call(ctx, "sendDm", map[string]string{"to": leftStatus.Address, "body": reply}, new(any)); err != nil {
		t.Fatal(err)
	}
	replyID := awaitReceipt(t, ctx, leftOptions.Config.DataDir, rightStatus.Address, reply)
	awaitChat(t, ctx, left, rightStatus.Address, reply, replyID)
	stopActualChildFinal(t, ctx, rightAgain, againBinding, againPrior)
	leftFinal := stopActualChildFinal(t, ctx, left, leftBinding, leftPrior)
	if leftFinal.Traffic.BytesReceived <= leftPrior.received || leftFinal.Traffic.BytesSent <= leftPrior.sent {
		t.Fatal("actual DM/restart tail failed to dominate pre-stop samples")
	}
	for _, pair := range []struct {
		options ProcessOptions
		child   *Process
	}{{leftOptions, left}, {rightOptions, rightAgain}} {
		options := pair.options
		path := filepath.Join(options.Config.DataDir, "public-provenance.json")
		manifest, err := json.Marshal(pair.child.Provenance())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, manifest, 0600); err != nil {
			t.Fatal(err)
		}
		if err := ScanPublicArtifacts(ctx, [][]byte{options.Credentials.password.raw, []byte(options.Credentials.passwordText())}, []string{filepath.Join(options.Config.DataDir, "child.log"), filepath.Join(options.Config.DataDir, "receipts.jsonl"), path}); err != nil {
			t.Fatal(err)
		}
		for _, public := range []any{options, pair.child, pair.child.RPC()} {
			if strings.Contains(fmt.Sprintf("%#v", public), options.Credentials.passwordText()) {
				t.Fatal("credential formatting leak")
			}
		}
	}
}

func awaitReceipt(t *testing.T, ctx context.Context, directory, peer, body string) domain.MessageID {
	t.Helper()
	digest := sha256.Sum256([]byte(body))
	wanted := hex.EncodeToString(digest[:])
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		raw, err := os.ReadFile(filepath.Join(directory, "receipts.jsonl"))
		if err == nil {
			scanner := bufio.NewScanner(bytes.NewReader(raw))
			for scanner.Scan() {
				var receipt Receipt
				if json.Unmarshal(scanner.Bytes(), &receipt) == nil && receipt.Sender.String() == peer && receipt.BodySHA256 == wanted && receipt.ID.IsValidOrEmpty() && receipt.ID != "" {
					return receipt.ID
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("normal SDK decryption receipt missing: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestNoncooperativeChildFixture(t *testing.T) {
	if os.Getenv("LOADSTAND_NONCOOPERATIVE_FIXTURE") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		os.Exit(2)
	}
	<-time.After(time.Hour)
}
func TestForcedStopReapsChildWithoutClaimingGracefulExit(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("POSIX signal fixture")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestNoncooperativeChildFixture$", "-test.timeout=0")
	command.Env = append(os.Environ(), "LOADSTAND_NONCOOPERATIVE_FIXTURE=1")
	pipe, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill() }() // cleanup guard if the fixture's readiness fails
	process := startOwnedProcess(t.Context(), command, protocolClient(t, 200, `{}`), nil, Provenance{}, nil, false)
	ready, err := bufio.NewReader(pipe).ReadString('\n')
	if err != nil || ready != "ready\n" {
		t.Fatalf("fixture readiness: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := process.Stop(ctx); !errors.Is(err, ErrForcedStop) {
		t.Fatalf("forced stop claimed graceful: %v", err)
	}
	select {
	case <-process.Done():
	default:
		t.Fatal("Stop returned before forced child was reaped")
	}
	exit, err := process.ExitResult()
	if err != nil || exit.Kind != WaitSignaled || exit.Signal == 0 || !exit.Signals.ForceAttempted || exit.Clean() {
		t.Fatal("lost owned forced-exit facts", exit, err)
	}
	if exit.Usage.State != ProcessMetricMeasured || exit.Usage.Scope != OwnedCommandUsage || exit.Usage.Value.HighWaterRSS == 0 {
		t.Fatal("signaled own wait lost real kernel usage", exit.Usage)
	}

}
