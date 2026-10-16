//go:build interop

package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// release_interop_test.go runs a node of this build (in process) against the
// REAL released corsa-node binaries that speak only v1 — every release named
// for each legacy ProtocolVersion (28, 29, 30) in CORSA_INTEROP_BIN_V28 /
// _V29 / _V30, built from their tags. It is the cross-binary counterpart of
// the emulation in legacy_peer_emulation_test.go: here the old side is the
// released code itself, not this build in v1-only mode.
//
// A released binary is run only in isolation, and every rule below is a
// condition of running one, enforced by the test rather than trusted to the
// operator:
//   - an old binary never starts without the network fuse: sandbox-exec with a
//     profile that denies every outbound connection and bind except loopback,
//     verified before the first start (an external connect must fail with
//     "Operation not permitted", a loopback one must succeed). Where there is
//     no sandbox-exec the test skips; CORSA_CONNECT_ONLY and lsof alone are
//     not isolation. `--version` runs under the same fuse, environment,
//     lifeline and watchdog as a node: a binary that ignored the argument
//     would start as one;
//   - a fail-closed watchdog reads the old process group's sockets from the
//     moment it starts and every 0.5 s after, and kills the group on any
//     non-loopback endpoint (listener included: the sandbox does not stop a
//     bind on 0.0.0.0), on any lsof error or stderr output, and when the
//     node's listener has vanished; after the run the old node's logs are
//     scanned for any non-loopback address;
//   - the old node runs with an empty environment, its own temporary HOME and
//     data directory and a test identity this test generated; the user's
//     data directory is write-denied by the profile. A released node has no
//     flag for its data directory (it ignores arguments) and an empty
//     bootstrap list means the production seeds, so both always name a
//     loopback peer of the test;
//   - releases run one after another, each old process in its own process
//     group behind a lifeline shell that kills the whole group when the old
//     binary exits, when the test closes the lifeline, or when this test
//     process dies (the kernel closes the pipe); t.Cleanup kills the group
//     while its leader is still unreaped, so the group id is never reused;
//   - every legacy ProtocolVersion must be given a binary; one is left out
//     only by name in CORSA_INTEROP_SKIP, and the skip shows in the output;
//   - the run must hold the machine-wide heavy-job lock (scripts/heavy-run.sh),
//     so it never overlaps a heavy measurement.
//
// How to build the binaries and run it: docs/protocol/session_v2.md, "Running
// the compatibility check against released builds".

// interopBinaryEnv names the environment variable that lists the released
// binaries of one legacy ProtocolVersion, separated like PATH.
func interopBinaryEnv(version int) string {
	return "CORSA_INTEROP_BIN_" + interopVersionKey(version)
}

// interopVersionKey is how CORSA_INTEROP_SKIP names a ProtocolVersion: "V28".
func interopVersionKey(version int) string {
	return "V" + strconv.Itoa(version)
}

const (
	sandboxExecPath = "/usr/bin/sandbox-exec"
	lsofPath        = "/usr/sbin/lsof"
	pgrepPath       = "/usr/bin/pgrep"
	// interopFuseProbeTarget is an external address the fuse must refuse.
	interopFuseProbeTarget = "1.1.1.1"
	// interopWatchEvery is how often the watchdog reads the old node's sockets.
	interopWatchEvery = 500 * time.Millisecond
	// interopNodeLifetime bounds one old process however the test goes: the
	// watchdog kills its group after it.
	interopNodeLifetime = 5 * time.Minute
	// interopVersionDeadline bounds `corsa-node --version`.
	interopVersionDeadline = 10 * time.Second
	// interopLatencyRuns is how many first dials scenario (f) measures.
	interopLatencyRuns = 20
	// releaseConnRatePerWindow and releaseConnRateWindow are the per-IP
	// inbound connection limiter of every release v28–v30
	// (defaultConnRateLimit, defaultConnRateWindow in their conn_limiter.go).
	releaseConnRatePerWindow = 10
	releaseConnRateWindow    = 10 * time.Second
	// interopLatencyPacing spaces the measured dials so that the two
	// connections of each stay within the limiter: at most four dials, eight
	// connections, fall into one window.
	interopLatencyPacing = 2500 * time.Millisecond
	// heavyRunLockDefault is the lock directory of scripts/heavy-run.sh unless
	// HEAVY_LOCK names another.
	heavyRunLockDefault = "/tmp/corsa-heavy-run.lock"
)

// releaseVersionFormat is what `corsa-node --version` of a release prints.
var releaseVersionFormat = regexp.MustCompile(`^\d+\.\d+\.\d+\n$`)

// lifelineScript stands between sandbox-exec and the old binary, which it
// runs with the arguments given after the script name. It kills its own
// process group — signal 0 names the group, never a pid another process may
// have inherited — when the binary exits, or when its stdin, a pipe whose
// write end only this test process holds, is closed by the test or by the
// kernel because the test died. The stdin is kept on fd 3 first: a
// background list of a non-interactive shell reads /dev/null otherwise.
const lifelineScript = `exec 3<&0; "$@" </dev/null & child=$!; ( read -r _ <&3; kill -KILL 0 ) & wait "$child"; kill -KILL 0`

func TestReleasedBuildsInterop(t *testing.T) {
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	skipped := interopSkips(t)
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			if skipped[interopVersionKey(version)] {
				t.Skipf("ProtocolVersion %d left out by CORSA_INTEROP_SKIP", version)
			}
			for _, binary := range interopBinaries(t, version) {
				release := describeRelease(t, fuse, binary, version)
				t.Run(release.release, func(t *testing.T) {
					runReleaseScenarios(t, fuse, release)
				})
			}
		})
	}
}

// interopSkips reads CORSA_INTEROP_SKIP ("V29,V30"). A name that is not a
// legacy ProtocolVersion fails the test: a typo must not turn into a skip of
// nothing while the version it meant runs unnoticed — or into a version that
// silently runs nowhere.
func interopSkips(t *testing.T) map[string]bool {
	t.Helper()
	known := map[string]bool{}
	for _, version := range releasedLegacyVersions() {
		known[interopVersionKey(version)] = true
	}
	skipped := map[string]bool{}
	for _, name := range strings.Split(os.Getenv("CORSA_INTEROP_SKIP"), ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !known[name] {
			t.Fatalf("CORSA_INTEROP_SKIP names %q, not one of the legacy versions %v", name, slices.Sorted(maps.Keys(known)))
		}
		skipped[name] = true
	}
	return skipped
}

// interopBinaries is the list of released binaries of version; an empty list
// fails the test — leaving a version out takes CORSA_INTEROP_SKIP.
func interopBinaries(t *testing.T, version int) []string {
	t.Helper()
	var binaries []string
	for _, path := range filepath.SplitList(os.Getenv(interopBinaryEnv(version))) {
		if strings.TrimSpace(path) != "" {
			binaries = append(binaries, path)
		}
	}
	if len(binaries) == 0 {
		t.Fatalf("%s names no binary: build the releases of ProtocolVersion %d, or leave it out with CORSA_INTEROP_SKIP=%s",
			interopBinaryEnv(version), version, interopVersionKey(version))
	}
	return binaries
}

func runReleaseScenarios(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	scenarios := []struct {
		name string
		run  func(*testing.T, networkFuse, releaseUnderTest)
	}{
		{"a_new_dials_old_tls_then_probe_then_legacy", scenarioNewDialsOld},
		{"b_dm_new_to_old", scenarioDMNewToOld},
		{"c_transit_through_old_relay", scenarioTransitThroughOld},
		{"d_old_dials_new", scenarioOldDialsNew},
		{"e_legacy_refused_pinned_identity_outbound", scenarioPinnedIdentityOutbound},
		{"e_legacy_refused_pinned_identity_inbound", scenarioPinnedIdentityInbound},
		{"e_legacy_refused_v2_bound_endpoint", scenarioBoundEndpoint},
		{"f_first_dial_latency", scenarioFirstDialLatency},
		{"f_first_dials_beyond_the_rate_limit", scenarioFirstDialsBeyondTheRateLimit},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) { scenario.run(t, fuse, release) })
	}
}

// --- the network fuse --------------------------------------------------------

// networkFuse is the sandbox profile an old binary runs under, verified to
// refuse an external connection and allow a loopback one.
type networkFuse struct {
	profile string
}

// requireNetworkFuse skips where there is no sandbox to run an old binary in,
// and fails when the sandbox exists but does not cut the network: an old
// binary is never started on the word of a profile nobody tried.
func requireNetworkFuse(t *testing.T) networkFuse {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("no network fuse: sandbox-exec exists only on darwin")
	}
	if _, err := os.Stat(sandboxExecPath); err != nil {
		t.Skip("no network fuse: sandbox-exec is missing")
	}
	fuse := networkFuse{profile: fuseProfile(t, realUserDataDirs(t))}
	requireExternalRefused(t, fuse)
	requireLoopbackAllowed(t, fuse)
	return fuse
}

// fuseProfile denies every outbound connection and every bind except
// loopback, and every write under each of the user's real data directories.
func fuseProfile(t *testing.T, writeDenied []string) string {
	t.Helper()
	profile := `(version 1)(allow default)` +
		`(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))` +
		`(deny network-bind)(allow network-bind (local ip "localhost:*"))`
	for _, dir := range writeDenied {
		if strings.ContainsAny(dir, "\"\\") {
			t.Fatalf("the user's data directory cannot be quoted in a sandbox profile")
		}
		profile += `(deny file-write* (subpath "` + dir + `"))`
	}
	return profile
}

// realUserDataDirs is the data directory a released node would use for the
// user running the test, read from the directory service rather than $HOME,
// which the caller may have redirected — and, when it exists and resolves
// elsewhere through a symlink, its resolved path too: the sandbox matches the
// path a write resolves to.
func realUserDataDirs(t *testing.T) []string {
	t.Helper()
	current, err := user.Current()
	if err != nil {
		t.Fatalf("current user: %v", err)
	}
	out, err := exec.Command("/usr/bin/dscl", ".", "-read", "/Users/"+current.Username, "NFSHomeDirectory").Output()
	if err != nil {
		t.Fatalf("dscl NFSHomeDirectory: %v", err)
	}
	home := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "NFSHomeDirectory:"))
	if !filepath.IsAbs(home) {
		t.Fatalf("dscl gave no absolute home directory")
	}
	dir := filepath.Join(home, "Library", "Application Support", "CorsaCore")
	dirs := []string{dir}
	resolved, err := filepath.EvalSymlinks(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		t.Fatalf("resolve the user's data directory: %v", err)
	case resolved != dir:
		dirs = append(dirs, resolved)
	}
	return dirs
}

func requireExternalRefused(t *testing.T, fuse networkFuse) {
	t.Helper()
	out, err := fuse.command("/usr/bin/nc", "-vz", "-w3", interopFuseProbeTarget, "443").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "Operation not permitted") {
		t.Fatalf("the network fuse did not refuse %s:443 (err=%v): %s", interopFuseProbeTarget, err, out)
	}
}

func requireLoopbackAllowed(t *testing.T, fuse networkFuse) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer closeProbeListener(t, listener)
	go acceptAndClose(listener)
	if out, err := fuse.command("/usr/bin/nc", "-vz", "-w3", "127.0.0.1", portOf(t, listener.Addr().String())).CombinedOutput(); err != nil {
		t.Fatalf("the network fuse refused loopback (err=%v): %s", err, out)
	}
}

func closeProbeListener(t *testing.T, listener net.Listener) {
	t.Helper()
	if err := listener.Close(); err != nil {
		t.Errorf("close the loopback probe listener: %v", err)
	}
}

// acceptAndClose answers the fuse's loopback probe. Only the connect matters
// to nc -z, so a connection that fails to close afterwards proves nothing
// either way and its error is dropped on purpose.
func acceptAndClose(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = conn.Close() // see above: the probe already succeeded or failed on connect
	}
}

// command runs name under the fuse with an empty environment.
func (f networkFuse) command(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(sandboxExecPath, append([]string{"-p", f.profile, name}, args...)...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	return cmd
}

func portOf(t *testing.T, address string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %q: %v", address, err)
	}
	return port
}

// requireHeavyRunLock fails unless this test runs as the job holding the
// machine-wide heavy-job lock: the run must not overlap a heavy measurement.
// The lock directory and its owner file are trusted only as heavy-run.sh
// trusts them: a plain directory and a plain file owned by this user, never a
// symlink someone else planted in /tmp.
func requireHeavyRunLock(t *testing.T) {
	t.Helper()
	lock := heavyRunLockDefault
	if custom := os.Getenv("HEAVY_LOCK"); custom != "" {
		lock = custom
	}
	requireOwnedByMe(t, lock, fs.ModeDir)
	ownerFile := filepath.Join(lock, "owner")
	requireOwnedByMe(t, ownerFile, 0)
	owner, err := os.ReadFile(ownerFile)
	if err != nil {
		t.Fatalf("run this test under scripts/heavy-run.sh: read the heavy-job lock owner: %v", err)
	}
	want := "pgid=" + strconv.Itoa(syscall.Getpgrp())
	if !slices.Contains(strings.Split(string(owner), "\n"), want) {
		t.Fatalf("run this test under scripts/heavy-run.sh: the heavy-job lock is held by another job")
	}
}

// requireOwnedByMe fails unless path is, without following a symlink, of the
// given type (fs.ModeDir, or 0 for a regular file) and owned by this user.
func requireOwnedByMe(t *testing.T, path string, kind fs.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("run this test under scripts/heavy-run.sh: %v", err)
	}
	if info.Mode().Type() != kind {
		t.Fatalf("the heavy-job lock path %s is not a plain %s (mode %s)", filepath.Base(path), kindName(kind), info.Mode())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		t.Fatalf("the heavy-job lock path %s is not owned by this user", filepath.Base(path))
	}
}

func kindName(kind fs.FileMode) string {
	if kind == fs.ModeDir {
		return "directory"
	}
	return "file"
}

// --- the release under test ------------------------------------------------

// releaseUnderTest is one released binary and what it is.
type releaseUnderTest struct {
	binary string
	// protocol is the ProtocolVersion the binary must announce; release is
	// its release version (corsa-node --version). They are different numbers.
	protocol int
	release  string
	sha256   string
}

// describeRelease hashes the binary and asks it for its release version —
// under the fuse, with a node's whole environment, lifeline and watchdog.
func describeRelease(t *testing.T, fuse networkFuse, binary string, protocolVersion int) releaseUnderTest {
	t.Helper()
	content, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read a binary of %s: %v", interopBinaryEnv(protocolVersion), err)
	}
	sum := sha256.Sum256(content)
	release := releaseUnderTest{
		binary:   binary,
		protocol: protocolVersion,
		release:  releaseVersionOf(t, fuse, binary),
		sha256:   hex.EncodeToString(sum[:]),
	}
	t.Logf("interop release: ProtocolVersion %d = corsa-node %s, sha256 %s", release.protocol, release.release, release.sha256)
	return release
}

// releaseVersionOf runs `binary --version` exactly as a node would be run —
// the same environment pinned to loopback, fuse, lifeline and watchdog — so a
// binary that ignores the argument starts as an isolated node and fails the
// format check, instead of running with the production seeds.
func releaseVersionOf(t *testing.T, fuse networkFuse, binary string) string {
	t.Helper()
	node := prepareReleasedNode(t)
	process := startSandboxed(t, fuse, node, binary, freeAddress(t), nil, "--version")
	deadline := time.Now().Add(interopVersionDeadline)
	for !bytes.Contains(node.stdout(t), []byte("\n")) {
		if violation := process.watch.violation(); violation != "" {
			t.Fatalf("the watchdog stopped `--version`: %s", violation)
		}
		if time.Now().After(deadline) {
			t.Fatalf("`--version` printed no line within %s", interopVersionDeadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
	process.stop(t)
	out := node.stdout(t)
	if !releaseVersionFormat.Match(out) {
		t.Fatalf("`--version` printed %q, want one MAJOR.MINOR.BUILD line", out)
	}
	return strings.TrimSpace(string(out))
}

// --- one sandboxed old process -------------------------------------------

// sandboxedProcess is one old binary running under the fuse, in its own
// process group, behind the lifeline and the watchdog.
type sandboxedProcess struct {
	cmd      *exec.Cmd
	group    int
	lifeline *os.File
	output   *os.File
	watch    *loopbackWatch
	stopOnce sync.Once
}

// startSandboxed starts binary with args and node's environment, its egress
// pinned to peer. t.Cleanup stops it and then runs afterStop.
func startSandboxed(t *testing.T, fuse networkFuse, node *releasedNode, binary, peer string, afterStop func(*testing.T), args ...string) *sandboxedProcess {
	t.Helper()
	requireLoopbackPeer(t, peer)
	output, err := os.Create(node.stdoutPath)
	if err != nil {
		t.Fatalf("create stdout log: %v", err)
	}
	lifelineRead, lifelineWrite, err := os.Pipe()
	if err != nil {
		t.Fatalf("lifeline pipe: %v", err)
	}
	argv := append([]string{"-p", fuse.profile, "/bin/sh", "-c", lifelineScript, "corsa-release", binary}, args...)
	cmd := exec.Command(sandboxExecPath, argv...)
	cmd.Env = node.environment(peer)
	cmd.Dir = node.home
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = lifelineRead
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", filepath.Base(binary), err)
	}
	process := &sandboxedProcess{cmd: cmd, group: cmd.Process.Pid, lifeline: lifelineWrite, output: output}
	process.watch = startLoopbackWatch(process.group, node.listen)
	t.Cleanup(func() {
		process.stop(t)
		if afterStop != nil {
			afterStop(t)
		}
	})
	// The child holds its own copy of the read end; ours would keep the pipe
	// open after the test died and so disarm the lifeline.
	if err := lifelineRead.Close(); err != nil {
		t.Fatalf("close the parent's lifeline read end: %v", err)
	}
	return process
}

// stop ends the lifeline, kills the group and reaps its leader. The group is
// killed before the leader is reaped — nothing else waits for it — so its id
// still names this group and no other. A violation the watchdog saw fails the
// test here at the latest.
func (p *sandboxedProcess) stop(t *testing.T) {
	t.Helper()
	p.stopOnce.Do(func() {
		p.watch.stop()
		if err := p.lifeline.Close(); err != nil {
			t.Errorf("close the lifeline: %v", err)
		}
		if err := killGroup(p.group); err != nil {
			t.Errorf("kill the old process group %d: %v", p.group, err)
		}
		p.awaitOnlyLeader(t)
		p.reapLeader(t)
		if err := p.output.Close(); err != nil {
			t.Errorf("close the stdout log: %v", err)
		}
		if violation := p.watch.violation(); violation != "" {
			t.Errorf("the watchdog stopped the old node: %s", violation)
		}
	})
}

// awaitOnlyLeader waits until nothing but the (unreaped) leader is left in
// the group.
func (p *sandboxedProcess) awaitOnlyLeader(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		members, err := groupMembers(p.group)
		if err != nil {
			t.Errorf("list the old process group %d: %v", p.group, err)
			return
		}
		if len(members) == 0 || slices.Equal(members, []int{p.group}) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("the old process group %d still holds %v 5s after SIGKILL", p.group, members)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *sandboxedProcess) reapLeader(t *testing.T) {
	t.Helper()
	reaped := make(chan error, 1)
	go func() { reaped <- p.cmd.Wait() }()
	select {
	case err := <-reaped:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Errorf("reap the old process group's leader: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Errorf("the old process group's leader did not exit within 10s of SIGKILL")
	}
}

// killGroup sends SIGKILL to every process of group. Callers reach it only
// while the group's leader is unreaped, so the id still names this group. A
// group with nothing left but its zombie leader — the lifeline already killed
// it — answers EPERM on darwin: that is a group with nothing to kill, and
// only then is the error dropped.
func killGroup(group int) error {
	err := syscall.Kill(-group, syscall.SIGKILL)
	switch {
	case err == nil, errors.Is(err, syscall.ESRCH):
		return nil
	case errors.Is(err, syscall.EPERM):
		members, listErr := groupMembers(group)
		if listErr != nil {
			return fmt.Errorf("%w; listing the group: %w", err, listErr)
		}
		if len(members) == 0 || slices.Equal(members, []int{group}) {
			return nil
		}
		return fmt.Errorf("%w with members %v alive", err, members)
	default:
		return err
	}
}

// groupMembers lists the pids of group; pgrep exits 1 when there is none.
func groupMembers(group int) ([]int, error) {
	out, err := exec.Command(pgrepPath, "-g", strconv.Itoa(group)).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 && len(out) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var members []int
	for _, field := range strings.Fields(string(out)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return nil, fmt.Errorf("pgrep printed %q", field)
		}
		members = append(members, pid)
	}
	slices.Sort(members)
	return members, nil
}

// --- one released node ---------------------------------------------------

// releasedNode is the environment of one old process and, once started, the
// node itself.
type releasedNode struct {
	// keys is the test identity the old node runs with; this test wrote it
	// into the node's temporary data directory.
	keys        *identity.Identity
	listen      domain.PeerAddress
	listenPort  string
	rpcAddress  string
	rpcHost     string
	rpcPort     string
	rpcPassword string
	// home is the old node's HOME; dataDir is the data directory a release
	// derives from it, created in advance.
	home       string
	dataDir    string
	stdoutPath string
	process    *sandboxedProcess
}

func (n *releasedNode) peerIdentity() domain.PeerIdentity {
	return domain.PeerIdentityFromWire(n.keys.Address)
}

// startReleasedNode starts the release as a node with its egress pinned to
// peer, a loopback address of this test. t.Cleanup stops it, kills its group
// and scans and keeps its logs.
func startReleasedNode(t *testing.T, fuse networkFuse, release releaseUnderTest, peer string) *releasedNode {
	t.Helper()
	node := prepareReleasedNode(t)
	node.process = startSandboxed(t, fuse, node, release.binary, peer, func(t *testing.T) {
		node.requireOnlyLoopbackInLogs(t)
		node.keepLogs(t)
	})
	node.awaitReady(t)
	return node
}

func requireLoopbackPeer(t *testing.T, peer string) {
	t.Helper()
	host, _, err := net.SplitHostPort(peer)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("an old node's peer must be a loopback address of the test, got %q", peer)
	}
}

func prepareReleasedNode(t *testing.T) *releasedNode {
	t.Helper()
	home := filepath.Join(t.TempDir(), "h")
	dataDir := filepath.Join(home, "Library", "Application Support", "CorsaCore")
	for _, dir := range []string{dataDir, filepath.Join(home, "tmp")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("create %s: %v", filepath.Base(dir), err)
		}
	}
	keys, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate the old node's test identity: %v", err)
	}
	listen, rpcAddress := freeAddress(t), freeAddress(t)
	rpcHost, rpcPort, err := net.SplitHostPort(rpcAddress)
	if err != nil {
		t.Fatalf("split the RPC address: %v", err)
	}
	node := &releasedNode{
		keys:        keys,
		listen:      domain.PeerAddress(listen),
		listenPort:  portOf(t, listen),
		rpcAddress:  rpcAddress,
		rpcHost:     rpcHost,
		rpcPort:     rpcPort,
		rpcPassword: randomHex(t, 16),
		home:        home,
		dataDir:     dataDir,
		stdoutPath:  filepath.Join(home, "stdout.log"),
	}
	if err := identity.Save(node.identityPath(), keys); err != nil {
		t.Fatalf("write the old node's test identity: %v", err)
	}
	return node
}

func (n *releasedNode) identityPath() string {
	return filepath.Join(n.dataDir, "identity-"+n.listenPort+".json")
}

// environment is the whole environment of the old process: nothing of the
// caller's is inherited.
func (n *releasedNode) environment(peer string) []string {
	return []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + n.home,
		"TMPDIR=" + filepath.Join(n.home, "tmp"),
		"CORSA_DATA_DIR=" + n.dataDir,
		"CORSA_CHATLOG_DIR=" + n.dataDir,
		"CORSA_IDENTITY_PATH=" + n.identityPath(),
		"CORSA_LISTEN_ADDRESS=" + string(n.listen),
		"CORSA_ADVERTISE_PORT=" + n.listenPort,
		"CORSA_BOOTSTRAP_PEERS=" + peer,
		"CORSA_CONNECT_ONLY=" + peer,
		"CORSA_RPC_HOST=" + n.rpcHost,
		"CORSA_RPC_PORT=" + n.rpcPort,
		"CORSA_RPC_USERNAME=interop",
		"CORSA_RPC_PASSWORD=" + n.rpcPassword,
		"CORSA_ACCEPT_DM=true",
		"CORSA_LOG_LEVEL=debug",
	}
}

// stdout is what the old process wrote to its stdout and stderr so far.
func (n *releasedNode) stdout(t *testing.T) []byte {
	t.Helper()
	content, err := os.ReadFile(n.stdoutPath)
	if err != nil {
		t.Fatalf("read the old process's stdout: %v", err)
	}
	return content
}

func randomHex(t *testing.T, size int) string {
	t.Helper()
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(buf)
}

// awaitReady waits until the old node listens on its port and answers RPC.
func (n *releasedNode) awaitReady(t *testing.T) {
	t.Helper()
	waitForConditionMsg(t, 30*time.Second, "the old node never started listening and answering RPC", func() bool {
		if violation := n.process.watch.violation(); violation != "" {
			t.Fatalf("the watchdog stopped the old node: %s", violation)
		}
		if !n.process.watch.listening() {
			return false
		}
		_, err := n.rpc("getPeers", nil)
		return err == nil
	})
}

// rpc runs one named command on the old node and returns its JSON body.
func (n *releasedNode) rpc(command string, args map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"command": command, "args": args})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, "http://"+n.rpcAddress+"/rpc/v1/exec", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth("interop", n.rpcPassword)
	request.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); readErr == nil && closeErr != nil {
		return nil, closeErr
	}
	if readErr != nil {
		return nil, readErr
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc %s: status %d", command, response.StatusCode)
	}
	return raw, nil
}

// hasDM reports whether the old node stored the DM with this id.
func (n *releasedNode) hasDM(id string) bool {
	raw, err := n.rpc("fetchMessages", map[string]any{"topic": "dm"})
	if err != nil {
		return false
	}
	var reply releaseMessages
	if json.Unmarshal(raw, &reply) != nil {
		return false
	}
	return slices.ContainsFunc(reply.Messages, func(m releaseMessage) bool { return m.ID == id })
}

// releaseMessages is the part of a release's fetchMessages answer this test
// reads.
type releaseMessages struct {
	Messages []releaseMessage `json:"messages"`
}

type releaseMessage struct {
	ID string `json:"id"`
}

var (
	ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	// ipv4Candidate and ipv6Candidate find what may be an address; net.ParseIP
	// decides. An IPv6 address is found bracketed or bare, and a clock time
	// such as 20:50:31 is not one.
	ipv4Candidate = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	ipv6Candidate = regexp.MustCompile(`[0-9A-Fa-f]*:[0-9A-Fa-f]*:[0-9A-Fa-f:]*`)
)

// requireOnlyLoopbackInLogs fails when the old node's logs name any address
// outside loopback: a dial it attempted toward the outside world, refused by
// the fuse or not, leaves its target there.
func (n *releasedNode) requireOnlyLoopbackInLogs(t *testing.T) {
	t.Helper()
	for _, path := range n.logPaths(t) {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read the old node's %s: %v", filepath.Base(path), err)
			continue
		}
		if found := nonLoopbackAddresses(ansiEscape.ReplaceAll(content, nil)); len(found) > 0 {
			t.Errorf("the old node's %s names addresses outside loopback: %v", filepath.Base(path), found)
		}
	}
}

// keepLogs copies the old node's logs into CORSA_INTEROP_LOG_DIR, when set,
// under the test's name: the node's own directory is removed with the test.
func (n *releasedNode) keepLogs(t *testing.T) {
	t.Helper()
	root := os.Getenv("CORSA_INTEROP_LOG_DIR")
	if root == "" {
		return
	}
	dir := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "_")+"-"+n.listenPort)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Errorf("create the log copy directory: %v", err)
		return
	}
	for _, path := range n.logPaths(t) {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read the old node's %s: %v", filepath.Base(path), err)
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(path)), content, 0o600); err != nil {
			t.Errorf("copy the old node's %s: %v", filepath.Base(path), err)
		}
	}
}

// logPaths is every log the old node left: its stdout, the crashlog's
// corsa.log and stderr.log, and any crash-*.log report.
func (n *releasedNode) logPaths(t *testing.T) []string {
	t.Helper()
	paths := []string{n.stdoutPath}
	for _, name := range []string{"corsa.log", "stderr.log"} {
		path := filepath.Join(n.dataDir, name)
		if _, err := os.Stat(path); err == nil {
			paths = append(paths, path)
		}
	}
	crashes, err := filepath.Glob(filepath.Join(n.dataDir, "crash-*.log"))
	if err != nil {
		t.Errorf("list the old node's crash reports: %v", err)
	}
	return append(paths, crashes...)
}

func nonLoopbackAddresses(text []byte) []string {
	var found []string
	for _, pattern := range []*regexp.Regexp{ipv4Candidate, ipv6Candidate} {
		for _, candidate := range pattern.FindAll(text, -1) {
			ip := net.ParseIP(string(candidate))
			if ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
				found = append(found, string(candidate))
			}
		}
	}
	return found
}

// --- the watchdog --------------------------------------------------------

// loopbackWatch reads the sockets of one old process group at once and then
// every interopWatchEvery, and kills the group — fail closed — on any
// endpoint outside loopback, on any socket listing it cannot fully trust, when
// the listener it has seen is gone, and after interopNodeLifetime.
type loopbackWatch struct {
	group  int
	listen domain.PeerAddress

	mu         sync.Mutex
	failure    string
	sawListen  bool
	stopOnce   sync.Once
	stopped    chan struct{}
	terminated chan struct{}
}

func startLoopbackWatch(group int, listen domain.PeerAddress) *loopbackWatch {
	watch := &loopbackWatch{group: group, listen: listen, stopped: make(chan struct{}), terminated: make(chan struct{})}
	go watch.run()
	return watch
}

func (w *loopbackWatch) run() {
	defer close(w.terminated)
	ticker := time.NewTicker(interopWatchEvery)
	defer ticker.Stop()
	lifetime := time.NewTimer(interopNodeLifetime)
	defer lifetime.Stop()
	for w.inspect() {
		select {
		case <-w.stopped:
			return
		case <-lifetime.C:
			w.kill(fmt.Sprintf("the old node outlived %s", interopNodeLifetime))
			return
		case <-ticker.C:
		}
	}
}

// inspect reads the group's sockets once; false means it killed the group.
func (w *loopbackWatch) inspect() bool {
	names, err := groupSocketNames(w.group)
	if err != nil {
		w.kill("the socket listing failed: " + err.Error())
		return false
	}
	listening := false
	for _, name := range names {
		if !loopbackSocketName(name) {
			w.kill("a socket outside loopback: " + name)
			return false
		}
		listening = listening || name == string(w.listen)
	}
	w.mu.Lock()
	vanished := w.sawListen && !listening
	w.sawListen = w.sawListen || listening
	w.mu.Unlock()
	if vanished {
		w.kill("the old node's listener is gone: it exited or closed it")
		return false
	}
	return true
}

func (w *loopbackWatch) kill(reason string) {
	if err := killGroup(w.group); err != nil {
		reason += fmt.Sprintf(" (and SIGKILL of the group failed: %v)", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure == "" {
		w.failure = reason
	}
}

func (w *loopbackWatch) stop() {
	w.stopOnce.Do(func() { close(w.stopped) })
	<-w.terminated
}

func (w *loopbackWatch) violation() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failure
}

func (w *loopbackWatch) listening() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sawListen
}

// groupSocketNames lists the Internet socket names (TCP, UDP, v4 and v6;
// "local" or "local->remote") of every process in group. lsof exits 1 with
// no output when nothing matched; any other exit, and any output on stderr,
// is an error: a listing that may be partial is not a clean one.
func groupSocketNames(group int) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, lsofPath, "-nP", "-a", "-g", strconv.Itoa(group), "-i", "-Fn")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if stderr.Len() > 0 {
		return nil, fmt.Errorf("lsof wrote to stderr: %q", strings.TrimSpace(stderr.String()))
	}
	var names []string
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "n") {
			names = append(names, line[1:])
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, scanErr
	}
	if err != nil && !nothingListed(err, stdout.Len()) {
		return nil, err
	}
	return names, nil
}

// nothingListed: lsof exits 1 with no output when nothing matched.
func nothingListed(err error, outputBytes int) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 1 && outputBytes == 0
}

// loopbackSocketName reports whether every endpoint of an lsof socket name is
// a loopback address. A wildcard ("*:port") is a bind on every interface.
func loopbackSocketName(name string) bool {
	for _, endpoint := range strings.Split(name, "->") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			return false
		}
		if host != "127.0.0.1" && host != "::1" {
			return false
		}
	}
	return true
}

// --- scenarios -------------------------------------------------------------

// startRecordingProxy forwards every connection to a RELEASED binary byte for
// byte and only records what the node under test sends. A release announces
// its own version, and a check of that version must read what the release
// put on the wire, not a value the proxy could have written.
func startRecordingProxy(t *testing.T, target string, side oldPeerSide) *legacyVersionProxy {
	t.Helper()
	return launchLegacyProxy(t, &legacyVersionProxy{target: target, side: side, verbatim: true})
}

// (a) A node of this build dials the release: the TLS attempt is answered
// with a v1 line, the identity-less probe follows on a new connection, and a
// legacy session is established on the probe's connection. The address is not
// charged for being old.
func scenarioNewDialsOld(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	old := startReleasedNode(t, fuse, release, freeAddress(t))
	proxy := startRecordingProxy(t, string(old.listen), oldPeerListens)
	dialer, stop := startQuietNode(t, nil)
	defer stop()
	target := proxy.peerAddress()

	session, err := dialEstablishedPastTransport(t, dialer, target)
	if err != nil {
		t.Fatalf("the node did not reach the release: %v", err)
	}
	requireLegacySessionWith(t, session, old, release)
	if got := proxy.connections(); got != 2 {
		t.Fatalf("%d connections, want the TLS attempt and the probe's", got)
	}
	if got := proxy.clientHellos(); got != 1 {
		t.Fatalf("%d connections opened with a ClientHello, want the first one only", got)
	}
	if hellos := proxy.nodeHellos(); len(hellos) < 2 || hellos[0] || !hellos[1] {
		t.Fatalf("hellos (identified?) = %v, want the identity-less probe, then the full hello", hellos)
	}
	requireAddressNotCharged(t, dialer, target)
	if !dialer.secureSessions.marks.legacyObserved(target) {
		t.Fatal("the release was not recorded as legacy_observed")
	}
}

func requireLegacySessionWith(t *testing.T, session *peerSession, old *releasedNode, release releaseUnderTest) {
	t.Helper()
	if session.proven != nil {
		t.Fatal("the session with the release was taken for v2")
	}
	if session.peerIdentity != old.peerIdentity() {
		t.Fatalf("the session names %s, want the release's test identity", session.peerIdentity)
	}
	if session.version != release.protocol {
		t.Fatalf("the release announced ProtocolVersion %d, want %d", session.version, release.protocol)
	}
}

func requireAddressNotCharged(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	svc.peerMu.RLock()
	health := svc.health[address]
	setup := svc.setupFailures[address]
	svc.peerMu.RUnlock()
	if setup != nil && setup.Consecutive > 0 {
		t.Fatalf("reaching the release was counted as a setup failure: %+v", *setup)
	}
	if health != nil && (health.ConsecutiveFailures != 0 || health.Score < 0 || !health.BannedUntil.IsZero()) {
		t.Fatalf("reaching the release charged the address: failures %d, score %d, banned until %v",
			health.ConsecutiveFailures, health.Score, health.BannedUntil)
	}
	if svc.secureSessions.store.endpointRequiresV2(address) {
		t.Fatal("the release left a v2 binding on its address")
	}
}

// (b) A DM from a node of this build reaches the release's inbox, read back
// over the release's own RPC.
func scenarioDMNewToOld(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	newAddress := freeAddress(t)
	old := startReleasedNode(t, fuse, release, newAddress)
	newNode, stop := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  newAddress,
		BootstrapPeers: []string{string(old.listen)},
		ConnectOnly:    string(old.listen),
		Type:           config.NodeTypeFull,
	}, nil)
	defer stop()
	waitForConditionMsg(t, 40*time.Second, "no legacy session with the release", func() bool {
		return newNode.SessionKindStats().OutboundLegacy > 0
	})

	id := newMessageID(t)
	sendDMToRelease(t, newNode, old, id, "to-the-release")
	waitForConditionMsg(t, 40*time.Second, "the DM never reached the release's inbox", func() bool {
		return old.hasDM(id)
	})
}

func newMessageID(t *testing.T) string {
	t.Helper()
	raw := randomHex(t, 16)
	return raw[0:8] + "-" + raw[8:12] + "-4" + raw[13:16] + "-a" + raw[17:20] + "-" + raw[20:32]
}

func sendDMToRelease(t *testing.T, from *Service, to *releasedNode, id, body string) {
	t.Helper()
	ciphertext, err := directmsg.EncryptForParticipants(
		from.identity,
		domain.DMRecipient{
			Address:      to.peerIdentity(),
			BoxKeyBase64: identity.BoxPublicKeyBase64(to.keys.BoxPublicKey),
		},
		domain.OutgoingDM{Body: body},
	)
	if err != nil {
		t.Fatalf("encrypt DM %s: %v", id, err)
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	stored := from.HandleLocalFrame(sendMessageFrame("dm", id, from.Address(), to.keys.Address, "sender-delete", ts, 0, ciphertext))
	if stored.Type != "message_stored" {
		t.Fatalf("send %s: %#v", id, stored)
	}
}

// (c) Transit: nodes A and C of this build reach each other only through the
// release B. A DM crosses each way, and A and C never meet.
func scenarioTransitThroughOld(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	addressA := freeAddress(t)
	addressC := freeAddress(t)
	relay := startReleasedNode(t, fuse, release, addressA)
	nodeA, stopA := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  addressA,
		BootstrapPeers: []string{string(relay.listen)},
		ConnectOnly:    string(relay.listen),
		Type:           config.NodeTypeFull,
	}, nil)
	defer stopA()
	nodeC, stopC := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  addressC,
		BootstrapPeers: []string{string(relay.listen)},
		ConnectOnly:    string(relay.listen),
		Type:           config.NodeTypeFull,
	}, nil)
	defer stopC()
	waitForConditionMsg(t, 40*time.Second, "A and C never both reached the release", func() bool {
		return nodeA.SessionKindStats().OutboundLegacy > 0 && nodeC.SessionKindStats().OutboundLegacy > 0
	})

	idA, idC := domain.PeerIdentityFromWire(nodeA.Address()), domain.PeerIdentityFromWire(nodeC.Address())
	assertNoDirectContact(t, nodeA, idC, "before the DMs")
	deliverDM(t, nodeA, nodeC, newMessageID(t), "a-to-c-through-the-release")
	deliverDM(t, nodeC, nodeA, newMessageID(t), "c-to-a-through-the-release")
	assertNoDirectContact(t, nodeA, idC, "after the DMs")
	assertNoDirectContact(t, nodeC, idA, "after the DMs")
}

// (d) The release dials a node of this build: a legacy session, counted by
// the session-kind metrics as an inbound legacy one below the v2 version.
func scenarioOldDialsNew(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	newAddress := freeAddress(t)
	newNode, stop := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  newAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(nil))
	defer stop()
	old := startReleasedNode(t, fuse, release, newAddress)

	waitForConditionMsg(t, 40*time.Second, "the release never authenticated to the node", func() bool {
		_, v1 := authenticatedInbound(newNode)
		return v1 > 0
	})
	kinds := newNode.SessionKindStats()
	if kinds.InboundLegacy == 0 || kinds.InboundV2 != 0 || kinds.HellosBelowSecureVersion == 0 {
		t.Fatalf("session kinds = %+v, want an inbound legacy session below the v2 version and no v2", kinds)
	}
	if versions := inboundVersionsOf(newNode, old.peerIdentity()); !slices.Equal(versions, []domain.ProtocolVersion{domain.ProtocolVersion(release.protocol)}) {
		t.Fatalf("the release's inbound sessions announced %v, want [%d]", versions, release.protocol)
	}
}

// inboundVersionsOf is the announced ProtocolVersion of every authenticated
// inbound connection of id.
func inboundVersionsOf(svc *Service, id domain.PeerIdentity) []domain.ProtocolVersion {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	var versions []domain.ProtocolVersion
	for _, entry := range svc.conns {
		core := entry.core
		if core == nil || core.Identity() != id {
			continue
		}
		if auth := core.Auth(); auth != nil && auth.Verified {
			versions = append(versions, core.ProtocolVersion())
		}
	}
	return versions
}

// (e) No legacy for an identity that proved v2 — outbound: a node that pinned
// the release's identity refuses the probe's answer and never sends its full
// hello.
func scenarioPinnedIdentityOutbound(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	old := startReleasedNode(t, fuse, release, freeAddress(t))
	proxy := startRecordingProxy(t, string(old.listen), oldPeerListens)
	dialer, stop := startQuietNode(t, func(svc *Service) {
		if err := svc.secureSessions.store.noteProvenInbound(old.peerIdentity()); err != nil {
			t.Fatalf("pin: %v", err)
		}
	})
	defer stop()

	established, err := dialPastTransport(t, dialer, proxy.peerAddress())
	if established || !errors.Is(err, errLegacyRefusedPinned) {
		t.Fatalf("established=%v err=%v, want errLegacyRefusedPinned", established, err)
	}
	proxy.drain(t)
	for i, identified := range proxy.nodeHellos() {
		if identified {
			t.Fatalf("hello %d carried this node's identity to a pinned release", i)
		}
	}
	if !dialer.secureSessions.store.identityPinned(old.peerIdentity()) {
		t.Fatal("the refusal cost the identity its pin")
	}
}

// (e) No legacy for an identity that proved v2 — inbound: the release dials a
// node that pinned its identity and is refused at its hello, before any
// welcome or challenge.
func scenarioPinnedIdentityInbound(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	newAddress := freeAddress(t)
	proxy := startRecordingProxy(t, newAddress, oldPeerDials)
	old := startReleasedNode(t, fuse, release, proxy.address())
	pinned := old.peerIdentity()
	newNode, stop := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  newAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(func(svc *Service) {
		if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
			t.Fatalf("pin: %v", err)
		}
	}))
	defer stop()

	waitForConditionMsg(t, 40*time.Second, "the node never refused the release's hello", func() bool {
		return newNode.SecureSessionStoreStats().V1RefusalsIdentityPinned > 0
	})
	// Every frame the node wrote must have been read by the proxy before the
	// check that it sent no welcome: drain waits until both ends closed.
	proxy.drain(t)
	if _, v1 := authenticatedInbound(newNode); v1 != 0 {
		t.Fatalf("%d legacy sessions with a pinned identity", v1)
	}
	if proxy.nodeSentFrame("welcome") {
		t.Fatal("the node sent a welcome with a challenge to a pinned identity's hello")
	}
	if !newNode.secureSessions.store.identityPinned(pinned) {
		t.Fatal("the refusal cost the identity its pin")
	}
}

// (e) No legacy for an endpoint bound to v2: the release answers the TLS
// attempt with a v1 line at an endpoint where another identity proved v2, and
// the dial ends there — no probe, no hello.
func scenarioBoundEndpoint(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	old := startReleasedNode(t, fuse, release, freeAddress(t))
	proxy := startRecordingProxy(t, string(old.listen), oldPeerListens)
	owner := generateProbeIdentity(t)
	dialer, stop := startQuietNode(t, func(svc *Service) {
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(proxy.peerAddress()), domain.PeerIdentityFromWire(owner.Address), endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}
	})
	defer stop()
	if !dialer.secureSessions.store.endpointRequiresV2(proxy.peerAddress()) {
		t.Fatal("fixture: the endpoint is not bound to v2")
	}

	established, err := dialPastTransport(t, dialer, proxy.peerAddress())
	if established || !errors.Is(err, errLegacyRefusedV2Bound) {
		t.Fatalf("established=%v err=%v, want errLegacyRefusedV2Bound", established, err)
	}
	proxy.drain(t)
	if got := proxy.connections(); got != 1 {
		t.Fatalf("%d connections, want the TLS attempt alone", got)
	}
	if hellos := proxy.nodeHellos(); len(hellos) != 0 {
		t.Fatalf("the node sent %d hellos to a v2-bound endpoint", len(hellos))
	}
	if dialer.SecureSessionStoreStats().V1RefusalsEndpointBound == 0 {
		t.Fatal("the refusal was not counted as an endpoint-bound one")
	}
}

// (f) How long the first dial of a node of this build to the release takes,
// from the start of the dial to the established legacy session: TLS attempt,
// v1 answer, probe, legacy handshake. Each run is a fresh node, so nothing it
// learned on a previous run shortens the next. The same is measured with
// TLS-first switched off (the dial opens with the probe) as the reference for
// what the TLS attempt costs — input for choosing the probe timeouts and
// whether TLS-first stays the default.
//
// The runs are paced: a release accepts at most releaseConnRatePerWindow
// connections from one IP per releaseConnRateWindow and resets the rest, and a
// TLS-first dial to it spends two of them (the TLS attempt and the probe), so
// back-to-back runs from loopback would measure the release's limiter instead
// of the dial. The limiter itself is shown once, unpaced, in
// scenarioFirstDialsBeyondTheRateLimit.
func scenarioFirstDialLatency(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	old := startReleasedNode(t, fuse, release, freeAddress(t))
	for _, mode := range firstDialModes() {
		samples := make([]time.Duration, 0, interopLatencyRuns)
		var failures []string
		for run := range interopLatencyRuns {
			if run > 0 {
				time.Sleep(interopLatencyPacing)
			}
			elapsed, err := timeFirstDial(t, old, mode.setup)
			if err != nil {
				failures = append(failures, fmt.Sprintf("run %d after %s: %v", run+1, elapsed, err))
				continue
			}
			samples = append(samples, elapsed)
		}
		t.Logf("interop latency v%d (%s) %s: %s", release.protocol, release.release, mode.name, latencySummary(samples))
		if len(failures) > 0 {
			t.Errorf("v%d %s: %d of %d paced first dials failed: %v", release.protocol, mode.name, len(failures), interopLatencyRuns, failures)
		}
		time.Sleep(releaseConnRateWindow)
	}
}

// firstDialMode is how a fresh node of this build opens its first dial.
type firstDialMode struct {
	name  string
	setup func(*Service)
}

func firstDialModes() []firstDialMode {
	return []firstDialMode{
		{"tls-first", nil},
		{"probe-first", func(svc *Service) { svc.secureSessions.tlsFirstDisabled = true }},
	}
}

// (f') The release's per-IP connection limiter against unpaced first dials of
// fresh nodes behind one IP: how many dials in a row succeed before the
// release resets them. A TLS-first dial spends two connections of the window,
// a probe-first one spends one. Recorded, not asserted beyond "at least one":
// it is the release's policy, and the number is input for the same choice as
// scenario (f).
func scenarioFirstDialsBeyondTheRateLimit(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	for i, mode := range firstDialModes() {
		if i > 0 {
			time.Sleep(releaseConnRateWindow)
		}
		old := startReleasedNode(t, fuse, release, freeAddress(t))
		succeeded, firstFailure := 0, ""
		for run := range releaseConnRatePerWindow + 1 {
			if _, err := timeFirstDial(t, old, mode.setup); err != nil {
				firstFailure = fmt.Sprintf("run %d: %v", run+1, err)
				break
			}
			succeeded++
		}
		t.Logf("interop rate limit v%d (%s) %s: %d unpaced first dials in a row succeeded; first failure: %s",
			release.protocol, release.release, mode.name, succeeded, firstFailure)
		if succeeded == 0 {
			t.Errorf("v%d %s: not even the first unpaced dial succeeded: %s", release.protocol, mode.name, firstFailure)
		}
	}
}

// timeFirstDial makes one first dial from a fresh node to the release and
// returns how long it took and how it ended.
func timeFirstDial(t *testing.T, old *releasedNode, setup func(*Service)) (time.Duration, error) {
	t.Helper()
	dialer, stop := startQuietNode(t, setup)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	started := time.Now()
	result, err := dialer.dialForCM(ctx, []domain.PeerAddress{old.listen})
	elapsed := time.Since(started)
	if err != nil {
		return elapsed, err
	}
	// The session belongs to no slot and must be closed here; a close error
	// would be about tearing it down, not about the dial being measured, and
	// the node's stop releases whatever is left.
	if closeErr := result.Session.Close(); closeErr != nil {
		t.Logf("close the measured session: %v", closeErr)
	}
	if result.Session.proven != nil {
		return elapsed, errors.New("the first dial to the release ended in a v2 session")
	}
	return elapsed, nil
}

// latencySummary is the nearest-rank p50, p95 and max of samples, and the
// samples themselves in milliseconds.
func latencySummary(samples []time.Duration) string {
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	rank := func(percent int) time.Duration {
		index := (percent*len(sorted)+99)/100 - 1
		return sorted[max(index, 0)]
	}
	ms := make([]string, len(samples))
	for i, sample := range samples {
		ms[i] = strconv.FormatFloat(float64(sample.Microseconds())/1000, 'f', 1, 64)
	}
	if len(sorted) == 0 {
		return "n=0"
	}
	return fmt.Sprintf("n=%d p50=%s p95=%s max=%s samples_ms=[%s]",
		len(sorted), rank(50), rank(95), sorted[len(sorted)-1], strings.Join(ms, " "))
}

// The watchdog and the log scan are the fuse's second line: a socket name or
// a log line naming anything outside loopback must be caught — a wildcard
// bind and a bare IPv6 address included — and a clock time must not be taken
// for an address.
func TestInteropFuseClassifiesSocketsAndLogAddresses(t *testing.T) {
	sockets := map[string]bool{
		"127.0.0.1:5":              true,
		"[::1]:5":                  true,
		"127.0.0.1:5->127.0.0.1:6": true,
		"*:5":                      false,
		"0.0.0.0:5":                false,
		"127.0.0.1:5->1.1.1.1:443": false,
		"192.168.1.8:5":            false,
		"[2001:db8::1]:5":          false,
		"garbage":                  false,
	}
	for name, loopback := range sockets {
		if got := loopbackSocketName(name); got != loopback {
			t.Errorf("loopbackSocketName(%q) = %v, want %v", name, got, loopback)
		}
	}
	log := []byte("20:50:31 dial 2.26.72.130:64646, 127.0.0.1:1, [2001:db8::1]:64646, [::1]:5, 0.0.0.0, " +
		"peer=2001:db8::2 local=::1 any=:: v1.0.64 2026-10-16T17:50:31Z mac=aa:bb:cc:dd:ee:ff")
	if got, want := nonLoopbackAddresses(log), []string{"2.26.72.130", "2001:db8::1", "2001:db8::2"}; !slices.Equal(got, want) {
		t.Errorf("nonLoopbackAddresses = %v, want %v", got, want)
	}
	if !releaseVersionFormat.MatchString("2.4.70\n") || releaseVersionFormat.MatchString("2.4.70\nlog line\n") {
		t.Error("the release version format accepts the wrong output")
	}
}

// The watchdog fails closed on real sockets: a wildcard listener is killed,
// and a listener it has seen that then disappears is a violation, not "no
// sockets, all clear". nc stands in for an old node; it runs under the fuse
// in its own process group.
func TestInteropWatchdogFailsClosed(t *testing.T) {
	fuse := requireNetworkFuse(t)

	t.Run("wildcard listener", func(t *testing.T) {
		port := portOf(t, freeAddress(t))
		group := startWatchedNC(t, fuse, "-l", port)
		watch := startLoopbackWatch(group, domain.PeerAddress("127.0.0.1:"+port))
		defer watch.stop()
		waitForConditionMsg(t, 5*time.Second, "the watchdog let a wildcard listener live", func() bool {
			return strings.Contains(watch.violation(), "outside loopback")
		})
	})

	t.Run("listener gone", func(t *testing.T) {
		address := freeAddress(t)
		group := startWatchedNC(t, fuse, "-l", "127.0.0.1", portOf(t, address))
		watch := startLoopbackWatch(group, domain.PeerAddress(address))
		defer watch.stop()
		waitForConditionMsg(t, 5*time.Second, "the watchdog never saw the listener", watch.listening)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatalf("dial nc: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close the connection to nc: %v", err)
		}
		waitForConditionMsg(t, 5*time.Second, "the watchdog read a vanished listener as all clear", func() bool {
			return strings.Contains(watch.violation(), "listener is gone")
		})
	})
}

// startWatchedNC starts nc under the fuse in its own process group and kills
// the group on cleanup before reaping its leader.
func startWatchedNC(t *testing.T, fuse networkFuse, args ...string) int {
	t.Helper()
	cmd := fuse.command("/usr/bin/nc", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start nc: %v", err)
	}
	group := cmd.Process.Pid
	t.Cleanup(func() {
		if err := killGroup(group); err != nil {
			t.Errorf("kill nc: %v", err)
		}
		var exit *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exit) {
			t.Errorf("reap nc: %v", err)
		}
	})
	return group
}
