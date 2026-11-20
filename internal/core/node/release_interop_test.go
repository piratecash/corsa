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

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// release_interop_test.go runs a node of this build (in process) against the
// released corsa-node binaries on either side of the minimum protocol.
// The older binaries exercise refusal over actual loopback connections.
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

func TestReleasedBuildsBelowMinimumAreRefused(t *testing.T) {
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	skipped := interopSkips(t)
	for _, version := range retiredReleaseVersions {
		t.Run(versionName(version), func(t *testing.T) {
			if skipped[interopVersionKey(version)] {
				t.Skipf("ProtocolVersion %d left out by CORSA_INTEROP_SKIP", version)
			}
			for _, binary := range interopBinaries(t, version) {
				release := describeRelease(t, fuse, binary, version)
				t.Run(release.release, func(t *testing.T) {
					checkBelowFloorRelease(t, fuse, release)
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
	for _, version := range retiredReleaseVersions {
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

// Retired release fixtures cover every previous supported protocol.
var retiredReleaseVersions = [...]int{28, 29, 30}

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
	// ended is what the one stop found: the whole group gone and its leader
	// reaped, with no violation.
	ended bool
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

// stop ends the lifeline, kills the group and reaps its leader, and reports
// whether the group is confirmed gone. The group is killed before the leader
// is reaped — nothing else waits for it — so its id still names this group and
// no other. The watchdog keeps reading the group's sockets until the group is
// confirmed empty: only its listener vanishing is expected from then on. A
// group that does not empty keeps its watchdog — and its unreaped leader, so
// the id is never reused under it. A violation the watchdog saw fails the test
// here at the latest. Only the first call stops; every call reports its result.
func (p *sandboxedProcess) stop(t *testing.T) bool {
	t.Helper()
	p.stopOnce.Do(func() { p.ended = p.terminate(t) })
	return p.ended
}

func (p *sandboxedProcess) terminate(t *testing.T) bool {
	t.Helper()
	p.watch.expectExit()
	if err := p.lifeline.Close(); err != nil {
		t.Errorf("close the lifeline: %v", err)
	}
	if err := killGroup(p.group); err != nil {
		t.Errorf("kill the old process group %d: %v", p.group, err)
	}
	if !p.awaitOnlyLeader(t) {
		return false
	}
	p.watch.stop()
	reaped := p.reapLeader(t)
	if err := p.output.Close(); err != nil {
		t.Errorf("close the stdout log: %v", err)
	}
	if violation := p.watch.violation(); violation != "" {
		t.Errorf("the watchdog stopped the old node: %s", violation)
		return false
	}
	return reaped
}

// awaitOnlyLeader waits until nothing but the (unreaped) leader is left in
// the group, and reports whether that happened.
func (p *sandboxedProcess) awaitOnlyLeader(t *testing.T) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		members, err := groupMembers(p.group)
		if err != nil {
			t.Errorf("list the old process group %d: %v", p.group, err)
			return false
		}
		if len(members) == 0 || slices.Equal(members, []int{p.group}) {
			return true
		}
		if time.Now().After(deadline) {
			t.Errorf("the old process group %d still holds %v 5s after SIGKILL", p.group, members)
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// reapLeader waits for the group's leader and reports whether it was reaped.
func (p *sandboxedProcess) reapLeader(t *testing.T) bool {
	t.Helper()
	reaped := make(chan error, 1)
	go func() { reaped <- p.cmd.Wait() }()
	select {
	case err := <-reaped:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Errorf("reap the old process group's leader: %v", err)
			return false
		}
		return true
	case <-time.After(10 * time.Second):
		t.Errorf("the old process group's leader did not exit within 10s of SIGKILL")
		return false
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
	probeFirst  bool
	// home is the old node's HOME; dataDir is the data directory a release
	// derives from it, created in advance.
	home       string
	dataDir    string
	stdoutPath string
	process    *sandboxedProcess
	// runs counts the starts of this node, so the logs of a run are kept
	// apart from those of the run before a restart.
	runs int
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
	node.start(t, fuse, release, peer)
	return node
}

// start runs the release as this node — its identity, data directory and
// ports — under the fuse.
func (n *releasedNode) start(t *testing.T, fuse networkFuse, release releaseUnderTest, peer string) {
	t.Helper()
	n.runs++
	run := n.runs
	n.process = startSandboxed(t, fuse, n, release.binary, peer, func(t *testing.T) {
		if run != n.runs {
			// A restart already checked and kept this run's logs; the files
			// on disk are a later run's now.
			return
		}
		n.requireOnlyLoopbackInLogs(t)
		n.keepLogs(t, run)
	})
	n.awaitReady(t)
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
	env := []string{
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
	if n.probeFirst {
		env = append(env, "CORSA_V2_TLS_FIRST=off")
	}
	return env
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
func (n *releasedNode) keepLogs(t *testing.T, run int) {
	t.Helper()
	root := os.Getenv("CORSA_INTEROP_LOG_DIR")
	if root == "" {
		return
	}
	dir := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "_")+"-"+n.listenPort+"-run"+strconv.Itoa(run))
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

	mu        sync.Mutex
	failure   string
	sawListen bool
	// exiting is set when the test itself ends the group: its listener
	// vanishing is expected from then on, a socket outside loopback is not.
	exiting    bool
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
	vanished := w.sawListen && !listening && !w.exiting
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

// expectExit tells the watchdog that the test is ending the group.
func (w *loopbackWatch) expectExit() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.exiting = true
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
	return launchLegacyProxy(t, &legacyVersionProxy{target: target, side: side})
}

// clientHellos is how many connections the node under test opened with a
// TLS ClientHello so far.
func (p *legacyVersionProxy) clientHellos() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nodeClientHellos
}

// nodeHellos reports, per hello the node under test sent so far, whether it
// carried an identity.
func (p *legacyVersionProxy) nodeHellos() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.nodeHelloIdentified...)
}

// connections is how many connections the proxy has forwarded.
func (p *legacyVersionProxy) connections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns) / 2
}

// nodeSentFrame reports whether the node under test sent a frame of this type.
func (p *legacyVersionProxy) nodeSentFrame(frameType string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.nodeFrameTypes {
		if t == frameType {
			return true
		}
	}
	return false
}

func checkBelowFloorRelease(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	t.Helper()
	t.Run("outbound", func(t *testing.T) {
		old := startReleasedNode(t, fuse, release, freeAddress(t))
		proxy := startRecordingProxy(t, string(old.listen), oldPeerListens)
		dialer, stop := startQuietNode(t, nil)
		t.Cleanup(stop)
		session, err := dialEstablishedPastTransport(t, dialer, proxy.peerAddress())
		if err == nil || session != nil {
			t.Fatalf("accepted a session with protocol %d below the floor", release.protocol)
		}
		proxy.drain(t)
		if proxy.connections() != 1 || proxy.clientHellos() != 1 || len(proxy.nodeHellos()) != 0 {
			t.Fatalf("wanted one TLS attempt and no legacy hello, connections=%d TLS=%d hello=%v",
				proxy.connections(), proxy.clientHellos(), proxy.nodeHellos())
		}
	})
	t.Run("inbound", func(t *testing.T) {
		listener, address, stop := listenerNode(t, nil)
		t.Cleanup(stop)
		proxy := startRecordingProxy(t, address, oldPeerDials)
		old := startReleasedNode(t, fuse, release, proxy.address())
		waitForConditionMsg(t, 10*time.Second, "released peer never connected to the listener", func() bool {
			return proxy.connections() > 0
		})
		proxy.drain(t)
		if secure, legacy := authenticatedInbound(listener); secure != 0 || legacy != 0 {
			t.Fatalf("accepted a session from protocol %d, TLS=%d legacy=%d", release.protocol, secure, legacy)
		}
		if proxy.nodeSentFrame("welcome") || listener.secureSessions.store.identityPinned(old.peerIdentity()) {
			t.Fatalf("below-floor protocol %d got a welcome or changed the pin store", release.protocol)
		}
	})
}

func newMessageID(t *testing.T) string {
	t.Helper()
	id, err := protocol.NewMessageID()
	if err != nil {
		t.Fatalf("generate message ID: %v", err)
	}
	return string(id)
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
