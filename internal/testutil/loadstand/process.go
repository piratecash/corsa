package loadstand

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

var ErrNamespaceRunnerRequired = errors.New("real Linux namespace runner required; functional loopback runner unavailable")
var ErrForcedStop = errors.New("functional process did not stop gracefully")
var ErrStopUnconfirmed = errors.New("functional process termination was not confirmed")

type ProcessID int

// Provenance distinguishes the launch file from the running executable. Linux
// PID/starttime/exe verification is a separate, not-yet-implemented admission.
type Provenance struct {
	PID                       ProcessID `json:"pid"`
	LaunchedAt                time.Time `json:"launched_at"`
	LaunchFileSHA256          string    `json:"launch_file_sha256"`
	Platform                  string    `json:"platform"`
	RunningExecutableVerified bool      `json:"running_executable_verified"`
	KernelStartTimeAvailable  bool      `json:"kernel_start_time_available"`
}
type ProcessOptions struct {
	Binary           string
	Config           ChildConfig
	Credentials      *Credentials
	ExpectedIdentity *domain.PeerIdentity
}

// Process has one wait owner. The result is published by closing done; Stop
// never calls Cmd.Wait a second time. Cmd.Env is never returned or formatted.
type processReadiness struct {
	listener bool
	address  domain.ListenAddress
	expected *domain.PeerIdentity
}

type Process struct {
	finalRole  MemberRole
	command    *exec.Cmd
	done       chan struct{}
	exit       ProcessExitResult
	terminal   *processController
	artifacts  *launchArtifacts
	reduction  finalReduction
	rpc        *RPCClient
	provenance Provenance
	readiness  processReadiness
}

func (p *Process) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprintf(state, "Process(pid=%d)", p.provenance.PID)
}
func (p *Process) Provenance() Provenance { return p.provenance }
func (p *Process) RPC() *RPCClient        { return p.rpc }
func (p *Process) Done() <-chan struct{}  { return p.done }
func (p *Process) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.waitError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StartLoopbackProcess is deliberately limited to an enforced macOS sandbox.
// It cannot be promoted to resource acceptance by changing an environment flag.
func StartLoopbackProcess(ctx context.Context, options ProcessOptions) (*Process, error) {
	process, err := LaunchLoopbackProcess(ctx, options)
	if err != nil {
		return process, err
	}
	if err := process.AwaitReady(ctx); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		// Retain ownership even if stopping could not confirm termination.
		return process, errors.Join(err, process.Stop(cleanup))
	}
	return process, nil
}

// LaunchLoopbackProcess transfers ownership immediately after spawn. Readiness
// has a separate context: cancelling a readiness deadline must not cancel Run.
// AwaitReady errors leave the handle owned and require Stop, including timeout.
func LaunchLoopbackProcess(ctx context.Context, options ProcessOptions) (*Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateAmbientEnvironment(os.Environ()); err != nil {
		return nil, err
	}
	if err := options.Config.ValidateLoopback(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(options.Binary) || !options.Credentials.valid() {
		return nil, ErrInvalidConfiguration
	}
	if runtime.GOOS != "darwin" {
		return nil, ErrNamespaceRunnerRequired
	}
	if _, err := os.Stat("/usr/bin/sandbox-exec"); err != nil {
		return nil, ErrNamespaceRunnerRequired
	}
	launchHash, err := hashLaunchFile(options.Binary)
	if err != nil {
		return nil, err
	}
	if err := preparePrivateProfile(options.Config.DataDir); err != nil {
		return nil, err
	}
	environment, err := options.Config.environment(options.Credentials)
	if err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(options.Config.DataDir, "child.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open child log: %w", err)
	}
	profile, err := sandboxProfile(options.Config)
	if err != nil {
		return nil, errors.Join(err, logFile.Close())
	}
	binding, err := newLaunchBinding(options.Config, launchHash)
	if err != nil {
		return nil, errors.Join(err, logFile.Close())
	}
	artifacts, err := newLaunchArtifacts(ctx, options.Config.DataDir, binding)
	if err != nil {
		return nil, errors.Join(err, logFile.Close())
	}
	client, err := NewRPCClient(RPCOptions{Address: options.Config.RPC, Credentials: options.Credentials})
	if err != nil {
		return nil, errors.Join(err, artifacts.close(), logFile.Close())
	}
	environment = append(environment, "LOADSTAND_FINAL_FD=3", "LOADSTAND_FINAL_NONCE="+binding.nonce.String(), "LOADSTAND_FINAL_PLAN="+binding.fingerprint.String())
	command := exec.Command("/usr/bin/sandbox-exec", "-p", profile, options.Binary)
	command.Env = environment
	command.Dir = options.Config.DataDir
	command.Stdout = logFile
	command.Stderr = logFile
	command.ExtraFiles = []*os.File{artifacts.writer}
	if err := ctx.Err(); err != nil {
		client.Close()
		return nil, errors.Join(err, artifacts.close(), logFile.Close())
	}
	if err := command.Start(); err != nil {
		client.Close()
		return nil, errors.Join(fmt.Errorf("start SDK child: %w", err), artifacts.close(), logFile.Close())
	}
	extraCloseErr := artifacts.writer.Close()
	artifacts.writer = nil
	process := startOwnedProcess(ctx, command, client, logFile, Provenance{PID: ProcessID(command.Process.Pid), LaunchedAt: time.Now().UTC(), LaunchFileSHA256: launchHash, Platform: runtime.GOOS + "/" + runtime.GOARCH}, artifacts, extraCloseErr != nil)
	process.readiness = processReadiness{listener: options.Config.Listener, address: options.Config.Listen}
	if options.ExpectedIdentity != nil {
		expected := *options.ExpectedIdentity
		process.readiness.expected = &expected
	}
	if extraCloseErr != nil {
		process.terminal.submit(StopExplicit, false, time.Time{}, false)
		return process, ErrParentProcessClose
	}

	return process, nil
}

func (p *Process) AwaitReady(ctx context.Context) error {
	return p.awaitReady(ctx, p.readiness.listener, p.readiness.address, p.readiness.expected)
}
func hashLaunchFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", ErrInvalidConfiguration
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, readErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
func (p *Process) awaitReady(ctx context.Context, listener bool, address domain.ListenAddress, expected *domain.PeerIdentity) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := NewProcessSource(p.rpc).Status(ctx)
		if err == nil {
			actual, parseErr := domain.ParsePeerIdentity(status.Address)
			if parseErr != nil || actual.IsZero() {
				return ErrInvalidSample
			}
			if expected != nil && actual != *expected {
				return fmt.Errorf("%w: restarted identity changed", ErrInvalidSample)
			}
			_, banErr := NewProcessSource(p.rpc).BanObservation(ctx)
			if banErr == nil && (!listener || loopbackListenerReady(ctx, address)) {
				return nil
			}
		}
		select {
		case <-p.done:
			return errors.Join(errors.New("child exited before readiness"), p.waitError())
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func loopbackListenerReady(ctx context.Context, address domain.ListenAddress) bool {
	dialer := net.Dialer{Timeout: 100 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", string(address))
	if err != nil {
		return false
	}
	return conn.Close() == nil
}
func (p *Process) Stop(ctx context.Context) error {
	p.rpc.Close()
	select {
	case <-p.done:
		return p.waitError()
	default:
	}
	deadline, _ := ctx.Deadline()
	p.terminal.submit(StopExplicit, false, deadline, false)
	select {
	case <-p.done:
		return p.waitError()
	case <-ctx.Done():
		p.terminal.submit(StopDeadline, true, time.Time{}, false)
		cleanup := time.NewTimer(2 * time.Second)
		defer cleanup.Stop()
		select {
		case <-p.done:
			return errors.Join(ctx.Err(), p.waitError())
		case <-cleanup.C:
			if !p.terminal.markUnconfirmed() {
				return errors.Join(ctx.Err(), p.waitError())
			}
			return errors.Join(ErrStopUnconfirmed, ctx.Err())
		}
	}
}

// SBPL localhost includes every local interface. Restricting it to explicitly
// planned TCP ports plus numeric loopback config avoids granting other local
// services. SBPL still forbids every remote host, including production seeds.
func sandboxProfile(config ChildConfig) (string, error) {
	if err := config.ValidateLoopback(); err != nil {
		return "", err
	}
	remotePorts := make(map[string]struct{})
	for _, address := range append([]string{string(config.Listen)}, peerAddressStrings(config.Bootstrap)...) {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", err
		}
		remotePorts[port] = struct{}{}
	}
	profile := strings.Builder{}
	profile.WriteString(`(version 1)(allow default)(deny network-outbound)(deny network-bind)`)
	ports := make([]string, 0, len(remotePorts))
	for port := range remotePorts {
		ports = append(ports, port)
	}
	sort.Strings(ports)
	for _, port := range ports {
		fmt.Fprintf(&profile, `(allow network-outbound (remote tcp "localhost:%s"))`, port)
	}
	for _, address := range []string{string(config.Listen), string(config.RPC)} {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&profile, `(allow network-bind (local tcp "localhost:%s"))`, port)
	}
	return profile.String(), nil
}

func preparePrivateProfile(directory string) error {
	for _, dir := range []string{directory, filepath.Join(directory, "tmp")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("create private profile: %w", err)
		}
		info, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode().Perm() != 0700 {
			return fmt.Errorf("%w: profile must be a private directory", ErrInvalidConfiguration)
		}
	}
	for _, name := range []string{"identity.json", "identity-intents.json", "child.log", "receipts.jsonl"} {
		if err := secretfile.AuditLocation(filepath.Join(directory, name)); err != nil {
			return fmt.Errorf("private profile audit: %w", err)
		}
	}
	return nil
}
