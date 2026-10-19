//go:build linux

package startupfence

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type helperConfig struct {
	Endpoint, Cert, Key, CA, Ready, Mode string
	Node                                 domain.PeerIdentity
	Store                                StoreID
}
type childReady struct {
	PID, PPID, UID int
	Fenced         bool
}

func TestContainedHelper(t *testing.T) {
	split := -1
	for i, arg := range os.Args {
		if arg == "--" {
			split = i
			break
		}
	}
	if split < 0 || len(os.Args) <= split+1 {
		return
	}
	args := os.Args[split+1:]
	switch args[0] {
	case "supervisor", "supervisor-stop":
		raw, err := os.ReadFile(args[1])
		if err != nil {
			t.Fatal(err)
		}
		var cfg helperConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			t.Fatal(err)
		}
		cert, roots, err := LoadTLSMaterial(cfg.Cert, cfg.Key, cfg.CA)
		if err != nil {
			t.Fatal(err)
		}
		client, err := NewBrokerClient(cfg.Endpoint, roots, cert)
		if err != nil {
			t.Fatal(err)
		}
		controller, err := NewController(client, cfg.Node, cfg.Store)
		if err != nil {
			t.Fatal(err)
		}
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := ChildCommand{Path: exe, Args: []string{"-test.run=TestContainedHelper", "--", "child", cfg.Ready, cfg.Mode, cfg.Node.String(), string(cfg.Store)}, UID: 65534, GID: 65534}
		ctx := context.Background()
		if args[0] == "supervisor-stop" {
			var stop context.CancelFunc
			ctx, stop = signal.NotifyContext(ctx, syscall.SIGTERM)
			defer stop()
		}
		if err := controller.Start(ctx, command); err != nil {
			t.Fatal(err)
		}
		if err := controller.Wait(); err != nil {
			t.Fatal(err)
		}
	case "child":
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		session, err := ConnectSupervisor(os.NewFile(3, "supervised"), exe, domain.PeerIdentityFromWire(args[3]), StoreID(args[4]))
		if err != nil {
			t.Fatal(err)
		}
		stopContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stopSignals()
		ready := childReady{PID: os.Getpid(), PPID: os.Getppid(), UID: os.Getuid()}
		if args[2] != "idle" && args[2] != "clean-idle" {
			binding, err := session.Binding(domain.PeerIdentityFromWire(args[3]), 1, time.Unix(1, 0).UTC(), domain.IdentityPinMetadata{})
			if err != nil {
				t.Fatal(err)
			}
			permit, err := session.BeginRemoval(context.Background(), binding)
			if err != nil {
				t.Fatal(err)
			}
			ready.Fenced = true
			if args[2] != "fence" {
				receipt := Receipt{Permit: permit, Kind: ReceiptCleanCommitted, SnapshotDigest: strings.Repeat("a", 64)}
				if args[2] == "lost-ack" || args[2] == "forged-close" {
					if err := session.encoder.Encode(sessionRequest{Receipt: &receipt}); err != nil {
						t.Fatal(err)
					}
					if args[2] == "forged-close" {
						// A receipt is known before its ACK; it cannot prove receipt
						// of the fresh supervisor acknowledgement challenge.
						_ = session.encoder.Encode(sessionRequest{Close: &sessionClose{Receipt: &receipt}})
						time.Sleep(100 * time.Millisecond)
					}
					_ = session.file.Close()
					os.Exit(0)
				}
				if err := session.RecordReceipt(context.Background(), receipt); err != nil {
					t.Fatal(err)
				}
			}

		}
		raw, err := json.Marshal(ready)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(args[1], raw, 0o600); err != nil {
			t.Fatal(err)
		}
		switch args[2] {
		case "clean-stop", "fatal-stop", "raw-stop":
			<-stopContext.Done()
			if args[2] == "raw-stop" {
				return
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			if args[2] == "fatal-stop" {
				t.Fatal("fatal after graceful close")
			}
			return
		case "clean", "clean-idle", "fatal", "hang":
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			// Hold the close-before-exit window open: plain EOF must not kill a
			// child that has completed the explicit bounded shutdown handshake.
			time.Sleep(100 * time.Millisecond)
			if args[2] == "clean" || args[2] == "clean-idle" {
				return
			}
			if args[2] == "fatal" {
				t.Fatal("fatal storage latch after acknowledged receipt")
			}
		case "raw-eof":
			_ = session.file.Close()
		case "oversize":
			_, _ = session.file.Write([]byte(strings.Repeat("x", 32<<10) + "\n"))
		}
		for {
			time.Sleep(time.Hour)
		}
	default:
		t.Fatal("unknown helper mode")
	}
}

type pauseConsume struct {
	transport http.RoundTripper
	consumed  chan struct{}
	proceed   chan struct{}
}

func (p pauseConsume) RoundTrip(req *http.Request) (*http.Response, error) {
	var decoded request
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		_ = json.NewDecoder(body).Decode(&decoded)
		_ = body.Close()
	}
	resp, err := p.transport.RoundTrip(req)
	if err != nil || decoded.Action != "consume" {
		return resp, err
	}
	close(p.consumed)
	select {
	case <-p.proceed:
		return resp, nil
	case <-req.Context().Done():
		_ = resp.Body.Close()
		return nil, req.Context().Err()
	}
}

func TestLifetimeLockPreventsSecondControllerBeforeFirstSpawn(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux launcher")
	}
	ledger, server, pki, e := testBroker(t)
	client := testClient(t, server, pki, pki.launcher())
	pause := pauseConsume{transport: client.http.Transport, consumed: make(chan struct{}), proceed: make(chan struct{})}
	client.http.Transport = pause
	first, err := NewController(client, e.Node, e.Store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewController(testClient(t, server, pki, pki.launcher()), e.Node, e.Store)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	shared, err := os.MkdirTemp("", "fence-lock-shared-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(shared) }()
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	command := ChildCommand{Path: exe, Args: []string{"-test.run=TestContainedHelper", "--", "child", filepath.Join(shared, "ready"), "idle", e.Node.String(), string(e.Store)}, UID: 65534, GID: 65534}
	started := make(chan error, 1)
	go func() { started <- first.Start(context.Background(), command) }()
	select {
	case <-pause.consumed:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach consumed/pre-spawn window")
	}
	before, err := ledger.Status(context.Background(), e.Node)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(context.Background(), command); err == nil {
		t.Fatal("second controller bypassed lifetime lock")
	}
	after, err := ledger.Status(context.Background(), e.Node)
	if err != nil {
		t.Fatal(err)
	}
	if before.Generation != after.Generation || before.State != after.State {
		t.Fatal("contender reached broker before owning lifetime lock")
	}
	close(pause.proceed)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	_ = awaitReady(t, filepath.Join(shared, "ready"))
	_ = first.Stop()
	_ = first.Wait()
	probe, err := lockBinding(e.Node, e.Store)
	if err != nil {
		entries, _ := os.ReadDir("/proc/self/fd")
		for _, entry := range entries {
			target, _ := os.Readlink("/proc/self/fd/" + entry.Name())
			if strings.Contains(target, "corsa-supervisor") {
				t.Logf("leaked lock fd %s -> %s", entry.Name(), target)
			}
		}
		t.Fatalf("lifetime lock remained after Wait: %v", err)
	}
	_ = probe.Close()
}

func TestOperatorCannotFinalizeWhileRecoveryWriterIsActive(t *testing.T) {
	ctx := context.Background()
	ledger, _, _, e := testBroker(t)
	grant, err := ledger.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ledger.BeginRemoval(ctx, lease, pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	clean := Receipt{Permit: p, Kind: ReceiptCleanCommitted, SnapshotDigest: strings.Repeat("a", 64)}
	if err := ledger.RecordReceipt(ctx, clean); err != nil {
		t.Fatal(err)
	}
	recovery, err := ledger.PrepareRecovery(ctx, e.Node, "recover")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeRecovery(ctx, recovery); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finalize(ctx, clean); err == nil {
		t.Fatal("finalized while recovery writer active")
	}
	restored := clean
	restored.Kind = ReceiptProtectionRestored
	restored.SnapshotDigest = strings.Repeat("b", 64)
	if err := ledger.RecordReceipt(ctx, restored); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finalize(ctx, restored); err == nil {
		t.Fatal("receipt alone released active recovery")
	}
	if err := ledger.CompleteRecovery(ctx, recovery, restored); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finalize(ctx, clean); err == nil {
		t.Fatal("old clean selection finalized restored state")
	}
	if _, err := ledger.Finalize(ctx, restored); err != nil {
		t.Fatal(err)
	}
}

func persistPKI(t *testing.T, pki testPKI) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	cert := filepath.Join(dir, "launcher.pem")
	key := filepath.Join(dir, "launcher.key")
	ca := filepath.Join(dir, "ca.pem")
	keyDER, err := x509.MarshalPKCS8PrivateKey(pki.launcher().PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	for path, raw := range map[string][]byte{cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.launcher().Certificate[0]}), key: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pki.launcher().Certificate[1]})} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return cert, key, ca
}

func TestSupervisorCrashKillsChildAndEveryRestartChecksExternalFence(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux containment; exercised in the bounded Docker test")
	}
	for _, mode := range []string{"idle", "fence"} {
		t.Run(mode, func(t *testing.T) {
			_, server, pki, e := testBroker(t)
			cert, key, ca := persistPKI(t, pki)
			shared, err := os.MkdirTemp("", "fence-child-shared-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(shared) })
			if err := os.Chmod(shared, 0o777); err != nil {
				t.Fatal(err)
			}
			readyPath := filepath.Join(shared, "child.json")
			configDir := t.TempDir()
			if err := os.Chmod(configDir, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(configDir, "helper.json")
			cfg := helperConfig{Endpoint: server.URL, Cert: cert, Key: key, CA: ca, Ready: readyPath, Mode: mode, Node: e.Node, Store: e.Store}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(configPath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			logs := make(map[*exec.Cmd]*bytes.Buffer)
			launch := func() *exec.Cmd {
				cmd := exec.Command(exe, "-test.run=TestContainedHelper", "--", "supervisor", configPath)
				log := new(bytes.Buffer)
				logs[cmd] = log
				cmd.Stdout, cmd.Stderr = log, log
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cmd.Process.Kill() })
				return cmd
			}
			supervisor := launch()
			ready := awaitReady(t, readyPath)
			if ready.UID != 65534 || ready.PPID != supervisor.Process.Pid {
				t.Fatalf("uncontained child: %+v", ready)
			}
			if err := supervisor.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = supervisor.Wait()
			deadline := time.Now().Add(5 * time.Second)
			for processAlive(ready.PID) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if processAlive(ready.PID) {
				_ = syscall.Kill(ready.PID, syscall.SIGKILL)
				t.Fatal("child survived SIGKILL of supervisor")
			}
			awaitProfileUnlocked(t, e)
			if err := os.Remove(readyPath); err != nil {
				t.Fatal(err)
			}
			restarted := launch()
			if mode == "fence" {
				done := make(chan error, 1)
				go func() { done <- restarted.Wait() }()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("fenced restart succeeded")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("fenced restart did not refuse")
				}
				if !strings.Contains(logs[restarted].String(), ErrFenced.Error()) {
					t.Fatalf("restart did not reach broker fence: %s", logs[restarted].String())
				}
				if _, err := os.Stat(readyPath); !os.IsNotExist(err) {
					t.Fatal("fenced restart spawned a child")
				}
			} else {
				next := awaitReady(t, readyPath)
				if next.PID == ready.PID {
					t.Fatal("reused old child")
				}
				_ = restarted.Process.Kill()
				_ = restarted.Wait()
				deadline := time.Now().Add(5 * time.Second)
				for processAlive(next.PID) && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if processAlive(next.PID) {
					t.Fatal("second child survived supervisor")
				}
			}
		})
	}
}

func awaitProfileUnlocked(t *testing.T, e Enrollment) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		file, err := lockBinding(e.Node, e.Store)
		if err == nil {
			_ = file.Close()
			return
		}
		if !errors.Is(err, ErrChanged) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("kernel did not release old process lifetime lock")
}

func awaitReady(t *testing.T, path string) childReady {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var ready childReady
			if json.Unmarshal(raw, &ready) == nil {
				return ready
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child did not complete the private supervisor handshake")
	return childReady{}
}
func processAlive(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return true
	}
	end := strings.LastIndex(string(raw), ") ")
	if end < 0 {
		return true
	}
	state := string(raw[end+2 : end+3])
	return state != "Z" && state != "X"
}

func TestRecoveryGrantIsNotAStartupGrant(t *testing.T) {
	ctx := context.Background()
	ledger, _, _, e := testBroker(t)
	grant, err := ledger.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	b := pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()})
	if _, err := ledger.BeginRemoval(ctx, lease, b); err != nil {
		t.Fatal(err)
	}
	recovery, err := ledger.PrepareRecovery(ctx, e.Node, "recover")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeStartup(ctx, recovery); err == nil {
		t.Fatal("recovery grant started network")
	}
	if _, err := ledger.ConsumeRecovery(ctx, recovery); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeRecovery(ctx, recovery); err == nil {
		t.Fatal("replayed recovery")
	}
	status, err := ledger.Status(ctx, e.Node)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateFenced {
		t.Fatal(fmt.Sprint("recovery unfenced: ", status.State))
	}
}

func TestReceiptAcknowledgementRequiresBoundedGracefulChildExit(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux launcher")
	}
	for _, mode := range []string{"clean", "clean-idle", "fatal", "hang", "raw-eof", "oversize", "lost-ack", "forged-close"} {
		t.Run(mode, func(t *testing.T) {
			ledger, server, pki, e := testBroker(t)
			controller, err := NewController(testClient(t, server, pki, pki.launcher()), e.Node, e.Store)
			if err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			shared, err := os.MkdirTemp("", "fence-exit-shared-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(shared) })
			if err := os.Chmod(shared, 0o777); err != nil {
				t.Fatal(err)
			}
			command := ChildCommand{Path: exe, Args: []string{"-test.run=TestContainedHelper", "--", "child", filepath.Join(shared, "ready"), mode, e.Node.String(), string(e.Store)}, UID: 65534, GID: 65534}
			if err := controller.Start(context.Background(), command); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = controller.Stop() })
			done := make(chan error, 1)
			go func() { done <- controller.Wait() }()
			select {
			case err := <-done:
				if (mode == "clean" || mode == "clean-idle") && err != nil {
					t.Fatalf("clean acknowledged shutdown was killed: %v", err)
				}
				if mode != "clean" && mode != "clean-idle" && err == nil {
					t.Fatal("unsafe shutdown reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown was not bounded")
			}
			status, err := ledger.Status(context.Background(), e.Node)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "clean-idle" {
				if status.State != StateRunning || status.WriterActive || status.Receipt != nil {
					t.Fatal("ordinary shutdown changed fence state")
				}
				return
			}
			if status.State != StateFenced {
				t.Fatal("shutdown unfenced startup")
			}
			if mode == "clean" {
				if status.WriterActive || status.Receipt == nil {
					t.Fatal("clean exit did not seal acknowledged receipt")
				}
				if _, err := ledger.Finalize(context.Background(), *status.Receipt); err != nil {
					t.Fatal(err)
				}
			} else {
				if !status.WriterActive {
					t.Fatal("unsafe exit sealed writer")
				}
				if status.Receipt != nil {
					if _, err := ledger.Finalize(context.Background(), *status.Receipt); err == nil {
						t.Fatal("unsafe writer was finalizable")
					}
				}
			}
		})
	}
}

func TestSupervisorSIGTERMPreservesExactGracefulReceiptAndFatalExit(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux supervisor")
	}
	for _, mode := range []string{"clean-stop", "fatal-stop", "raw-stop", "hang-stop"} {
		t.Run(mode, func(t *testing.T) {
			ledger, server, pki, enrollment := testBroker(t)
			cert, key, ca := persistPKI(t, pki)
			shared, err := os.MkdirTemp("", "fence-stop-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(shared) }()
			if err := os.Chmod(shared, 0o777); err != nil {
				t.Fatal(err)
			}
			cfg := helperConfig{Endpoint: server.URL, Cert: cert, Key: key, CA: ca, Ready: filepath.Join(shared, "ready"), Mode: mode, Node: enrollment.Node, Store: enrollment.Store}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "supervisor.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(exe, "-test.run=^TestContainedHelper$", "--", "supervisor-stop", path)
			output := new(bytes.Buffer)
			command.Stdout, command.Stderr = output, output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = command.Process.Kill() }()
			_ = awaitReady(t, cfg.Ready)
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			select {
			case err := <-done:
				if (err == nil) != (mode == "clean-stop") {
					t.Fatalf("supervisor stop outcome %s: %v\n%s", mode, err, output.String())
				}
			case <-time.After(8 * time.Second):
				t.Fatal("supervisor TERM teardown exceeded service stop budget")
			}
			status, err := ledger.Status(context.Background(), enrollment.Node)
			if err != nil {
				t.Fatal(err)
			}
			if status.State != StateFenced || status.Receipt == nil {
				t.Fatal("stop lost fence/receipt")
			}
			if mode == "clean-stop" {
				if status.WriterActive {
					t.Fatal("clean TERM shutdown did not seal exact ACKed receipt")
				}
				if _, err := ledger.Finalize(context.Background(), *status.Receipt); err != nil {
					t.Fatal(err)
				}
			} else if !status.WriterActive {
				t.Fatal("unsafe TERM shutdown sealed a receipt")
			}
		})
	}
}
