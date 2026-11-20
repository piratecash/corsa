//go:build interop && linux && d1_owned_guest_interop

package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const published31GuestSHA = "06e26f9b30594d19fb7da9782921f3ef2e7d297da3441d521a8e2c6343d2fade"

// These checks belong only to the separately tagged owned Linux guest. They
// neither replace nor weaken the original Darwin networkFuse/startSandboxed.
type published31GuestInputs struct {
	Schema       int    `json:"schema"`
	Purpose      string `json:"purpose"`
	Nonce        string `json:"nonce"`
	TestSHA256   string `json:"test_sha256"`
	PublishedSHA string `json:"published_sha256"`
}

func published31GuestFileSHA(path string, limit int64) (sum string, err error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		return "", errors.New("guest file metadata")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(file, limit+1))
	if err != nil || n != info.Size() || n > limit {
		return "", errors.New("guest file changed")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func published31GuestNamespace() (string, error) {
	links, err := net.Interfaces()
	if err != nil || len(links) != 1 || links[0].Name != "lo" || links[0].Flags&net.FlagUp == 0 {
		return "", errors.New("guest requires exactly active lo")
	}
	return os.Readlink("/proc/self/ns/net")
}

func published31GuestInputsForTest(t *testing.T) (published31GuestInputs, string) {
	t.Helper()
	if os.Getuid() != 501 || os.Getgid() != 20 {
		t.Fatal("owned guest UID/GID differ")
	}
	ns, err := published31GuestNamespace()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open("/inputs/hmix-guest.json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal("close owned guest descriptor", err)
		}
	}()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 4096 || info.Mode().Perm() != 0o444 {
		t.Fatal("owned guest descriptor metadata")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || owner.Gid != 0 || owner.Nlink != 1 {
		t.Fatal("owned guest descriptor custody")
	}
	var in published31GuestInputs
	decoder := json.NewDecoder(io.LimitReader(file, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		t.Fatal("owned guest descriptor", err)
	}
	var tail any
	if decoder.Decode(&tail) != io.EOF || in.Schema != 1 || in.Purpose != "HMIX_PUBLISHED31_FUNCTIONAL" ||
		len(in.Nonce) != 32 || len(in.TestSHA256) != 64 || in.PublishedSHA != published31GuestSHA {
		t.Fatal("owned guest descriptor binding")
	}
	if _, err := hex.DecodeString(in.Nonce); err != nil {
		t.Fatal("owned guest nonce")
	}
	if got, err := published31GuestFileSHA("/proc/self/exe", 96<<20); err != nil || got != in.TestSHA256 {
		t.Fatal("owned guest test executable binding", err)
	}
	if got, err := published31GuestFileSHA("/inputs/corsa-node", 32<<20); err != nil || got != published31GuestSHA {
		t.Fatal("published v2.5.71 executable binding", err)
	}
	return in, ns
}

// TestOwnedFindNodePublished31GuestDelivery deliberately covers one connection
// and DM direction. The same old executable dials current, current sends to old.
func TestOwnedFindNodePublished31GuestDelivery(t *testing.T) {
	_, namespace := published31GuestInputsForTest(t)
	logBytes := 0
	for _, responder := range []bool{false, true} {
		name := "Off_control"
		if responder {
			name = "private_Responder_with_held_F1"
		}
		if !t.Run(name, func(t *testing.T) {
			var current *Service
			var address string
			if responder {
				current, address = newOwnedBareV2PilotRelay(t, 2, 5)
			} else {
				id, err := identity.Generate()
				if err != nil {
					t.Fatal(err)
				}
				cfg := deriveTestAdvertisePort(configtest.WithTempDataDirs(t, config.Node{
					ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true,
					BootstrapPeers: []string{}, Type: domain.NodeTypeFull, AllowPrivatePeers: true,
				}))
				current = NewService(cfg, id, nil)
				ctx, cancel := context.WithCancel(t.Context())
				_, stop := startTestService(t, ctx, cancel, current)
				t.Cleanup(stop)
				current.peerMu.RLock()
				address = current.listener.Addr().String()
				current.peerMu.RUnlock()
			}
			if current.disableRateLimiting || current.cfg.EnableDatagramV1 ||
				(!responder && current.localFindNodeRole != findNodeRoleOff) ||
				(responder && current.localFindNodeRole != findNodeRoleResponder) {
				t.Fatal("guest comparison escaped original role/datagram/rate construction")
			}
			old, stopOld := published31GuestStart(t, address, namespace, name, &logBytes)
			waitFor(t, 5*time.Second, "original delivery route/key/session not ready before FIND_NODE", func() bool {
				best := current.loadRoutingSnapshot().BestRoute(old.peerIdentity())
				box, known := current.knownBoxKey(old.keys.Address)
				if best == nil || best.NextHop != old.peerIdentity() || best.Hops != 1 || !known || box != identity.BoxPublicKeyBase64(old.keys.BoxPublicKey) {
					return false
				}
				current.peerMu.RLock()
				defer current.peerMu.RUnlock()
				for _, entry := range current.conns {
					if entry.core == nil || entry.core.HasCapability(domain.CapOverlayFindNodeV1) {
						continue
					}
					proof, ok := entry.core.Auth().ProvenIdentity()
					if peer, valid := proof.Identity(); ok && valid && peer == old.peerIdentity() && entry.core.Auth().Hello.Version == 31 {
						return true
					}
				}
				return false
			})
			t.Logf("HMIX_OLD_READY case=%s sha256=%s protocol=31 f1=false cached_route=true keys=true", name, published31GuestSHA)
			if !responder {
				checkDirectReleasedReceiptLifecycle(t, current, old)
			} else {
				target := dialOwnedBareV2PilotClient(t, current, address, true)
				source, wire := dialOwnedBareV2PilotWire(t, current, address, true, net.Dialer{}, nil)
				t.Cleanup(func() { _ = wire.Close() })
				sourceID := domain.PeerIdentityFromWire(source.Address)
				members := make(map[domain.PeerIdentity]findnode.Session)
				for _, member := range bareV2PilotOriginalMembers(t, current, target.id, sourceID) {
					members[member.Identity()] = member
				}
				request := protocol.FindNodeRequest{ID: 72, Target: target.id, Challenge: domain.FindNodeChallenge{72}}
				raw, err := protocol.MarshalFindNodeRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				if err := wire.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := wire.Write(append(raw, '\n')); err != nil {
					t.Fatal(err)
				}
				var forwarded protocol.FindNodeRequest
				select {
				case forwarded = <-target.requests:
					if forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
						t.Fatal("held FIND_NODE changed")
					}
				case err := <-target.errors:
					t.Fatal(err)
				case <-time.After(2 * time.Second):
					t.Fatal("published31 comparison did not retain FIND_NODE")
				}
				pending := func() {
					t.Helper()
					r := current.bridgeCapture.intake.ResourceSnapshot()
					if r.Pending != 1 || r.Intake.Taken != 1 || r.Intake.Answered != 0 {
						t.Fatal("delivery did not coexist with original pending FIND_NODE", r.Intake, r.Pending)
					}
				}
				pending()
				ref := sendRequiredReceiptDMToRelease(t, current, old)
				pending() // The helper has fetched and decrypted the real old inbox.
				if err := wire.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				target.replyTo(t, forwarded)
				line, err := readFrameLine(bufio.NewReader(wire), protocol.MaxFrameLine)
				if err != nil {
					t.Fatal(err)
				}
				answer, err := protocol.ParseFindNodeAnswer([]byte(line))
				if err != nil {
					t.Fatal(err)
				}
				if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil {
					t.Fatal(err)
				}
				after := make(map[domain.PeerIdentity]findnode.Session)
				for _, member := range bareV2PilotOriginalMembers(t, current, target.id, sourceID) {
					after[member.Identity()] = member
				}
				if !maps.Equal(members, after) {
					t.Fatal("old-peer DM changed original F1 sessions/ConnIDs")
				}
				// The same existing receipt assertions finish AFTER the signed return.
				waitForLegacyReceipt(t, current, ref, protocol.ReceiptStatusDelivered)
				markReleasedMessageSeen(t, old, ref)
				waitForLegacyReceipt(t, current, ref, protocol.ReceiptStatusSeen)
				waitForConditionMsg(t, 10*time.Second, "old recipient did not accept seen_ack", func() bool {
					return releasedReceiptLogged(old, ref.ID, ref.Sender, ref.Recipient, protocol.ReceiptStatusSeenAck)
				})
				t.Log("HMIX_HELD_DELIVERY pending=1 decrypted=true signed_return=true original_members=true")
			}
			stopOld()
			if t.Failed() {
				return // Never publish positive closure fields after abnormal cleanup.
			}
			if ns, err := published31GuestNamespace(); err != nil || ns != namespace {
				t.Fatal("owned guest namespace changed", err)
			}
			t.Logf("HMIX_CASE_COMPLETE case=%s delivered=true seen=true seen_ack=true old_normal_exit=0 old_group_absent=true", name)
		}) {
			return // Never start a following case after failure or unknown cleanup.
		}
	}
}

func published31GuestRead(path string, limit int64) (data []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		return nil, errors.New("bounded guest proc read")
	}
	return b, nil
}

func published31GuestStartTicks(pid int) (string, error) {
	b, err := published31GuestRead(fmt.Sprintf("/proc/%d/stat", pid), 4096)
	if err != nil {
		return "", err
	}
	end := bytes.LastIndexByte(b, ')')
	if end < 0 {
		return "", errors.New("guest process stat")
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) < 20 || fields[1] != strconv.Itoa(os.Getpid()) || fields[2] != strconv.Itoa(pid) {
		return "", errors.New("guest process parent/group")
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", errors.New("guest process start ticks")
	}
	return fields[19], nil
}

// Same proc TCP/owned-inode proof as S6 collector.listener_sample, scoped to
// this one live child. No lsof/sandbox-exec emulation or external network probe.
func published31GuestListener(pid int, port string, namespace string) (owned bool) {
	ns, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil || ns != namespace {
		return false
	}
	tcp, err := published31GuestRead(fmt.Sprintf("/proc/%d/net/tcp", pid), 1<<20)
	if err != nil {
		return false
	}
	tcp6, err := published31GuestRead(fmt.Sprintf("/proc/%d/net/tcp6", pid), 1<<20)
	if err != nil {
		return false
	}
	inode, ok := published31GuestListeningInode(tcp, tcp6, port)
	if !ok {
		return false
	}
	dir, err := os.Open(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return false
	}
	defer func() {
		if err := dir.Close(); err != nil {
			owned = false
		}
	}()
	names, err := dir.Readdirnames(4097)
	if (err != nil && err != io.EOF) || len(names) > 4096 {
		return false
	}
	for _, name := range names {
		link, err := os.Readlink(filepath.Join(dir.Name(), name))
		if err == nil && link == "socket:["+inode+"]" {
			after, readErr := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", pid))
			self, selfErr := published31GuestNamespace()
			return readErr == nil && selfErr == nil && after == namespace && self == namespace
		}
	}
	return false
}

// Pure decoding controls use genuine proc row structure; no live process or
// session is manufactured to satisfy the runtime identity/listener proof.
func published31GuestListeningInode(tcp, tcp6 []byte, port string) (string, bool) {
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return "", false
	}
	wantPort := fmt.Sprintf("%04X", p)
	var inode string
	for family, raw := range [][]byte{tcp, tcp6} {
		if len(raw) == 0 || len(raw) > 1<<20 || !bytes.HasSuffix(raw, []byte{'\n'}) {
			return "", false
		}
		lines := strings.Split(string(raw), "\n")
		header := strings.Fields(lines[0])
		for _, required := range []string{"local_address", "st", "inode"} {
			found := false
			for _, name := range header {
				found = found || name == required
			}
			if !found {
				return "", false
			}
		}
		for _, line := range lines[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 || len(fields[3]) != 2 {
				return "", false
			}
			state, err := strconv.ParseUint(fields[3], 16, 8)
			if err != nil {
				return "", false
			}
			if state != 10 {
				continue
			}
			parts := strings.Split(fields[1], ":")
			width := 8
			if family == 1 {
				width = 32
			}
			if len(parts) != 2 || len(parts[0]) != width || len(parts[1]) != 4 {
				return "", false
			}
			if _, err := hex.DecodeString(parts[0]); err != nil {
				return "", false
			}
			actualPort, err := strconv.ParseUint(parts[1], 16, 16)
			if err != nil {
				return "", false
			}
			if fmt.Sprintf("%04X", actualPort) != wantPort {
				continue
			}
			if family != 0 || strings.ToUpper(parts[0]) != "0100007F" || inode != "" {
				return "", false
			}
			if n, err := strconv.ParseUint(fields[9], 10, 64); err != nil || n == 0 {
				return "", false
			}
			inode = fields[9]
		}
	}
	return inode, inode != ""
}

func TestOwnedPublished31GuestProcListenerControls(t *testing.T) {
	header := "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
	row := "0: 0100007F:7E41 00000000:0000 0A 00000000:00000000 00:00000000 00000000 501 0 12345 1\n"
	if inode, ok := published31GuestListeningInode([]byte(header+row), []byte(header), "32321"); !ok || inode != "12345" {
		t.Fatal("original-shaped owned loopback listener not accepted")
	}
	for _, tc := range []struct{ name, tcp, tcp6 string }{
		{"wildcard", header + strings.Replace(row, "0100007F", "00000000", 1), header},
		{"foreign", header + strings.Replace(row, "0100007F", "0100000A", 1), header},
		{"duplicate", header + row + row, header},
		{"wrong-inode", header + strings.Replace(row, "12345", "inode", 1), header},
		{"zero-inode", header + strings.Replace(row, "12345", "0", 1), header},
		{"truncated", header + strings.TrimSuffix(row, "\n"), header},
		{"missing-header", row, header},
		{"short-row", header + "0: bad\n", header},
		{"ipv6", header + row, header + strings.Replace(row, "0100007F:7E41", "00000000000000000000000001000000:7E41", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := published31GuestListeningInode([]byte(tc.tcp), []byte(tc.tcp6), "32321"); ok {
				t.Fatal("ambiguous or foreign listener accepted")
			}
		})
	}
}

func published31GuestStart(t *testing.T, peer, namespace, caseName string, logBytes *int) (*releasedNode, func()) {
	t.Helper()
	requireLoopbackPeer(t, peer)
	old := prepareReleasedNode(t)
	output, err := os.OpenFile(old.stdoutPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/inputs/corsa-node")
	cmd.Env, cmd.Dir, cmd.Stdout, cmd.Stderr = old.environment(peer), old.home, output, output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	var stopOnce sync.Once
	stop := func() {
		t.Helper()
		stopOnce.Do(func() {
			// The leader is deliberately unreaped until after signals: its PID
			// still reserves this exact group, as in the original interop runner.
			if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
				t.Error("owned old group TERM", err)
			}
			waited := false
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
				var info unix.Siginfo
				if err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil); err != nil {
					t.Error("owned old waitid", err)
					break
				}
				if info.Signo != 0 {
					waited = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !waited {
				t.Error("old recipient did not stop normally within original cleanup bound")
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
			// The post-setup BusyBox 120s+10s supervisor and outer host
			// 180s+30s budget handle a hanging wait; no extra guest timer exists.
			if err := cmd.Wait(); err != nil {
				t.Error("old recipient normal exit", err)
			}
			if err := syscall.Kill(-pid, 0); err != syscall.ESRCH {
				t.Error("old process group absence unknown", err)
			}
			if err := output.Close(); err != nil {
				t.Error(err)
			}
			paths := old.logPaths(t)
			if len(paths) > 16 {
				t.Error("old log file count bound")
				return
			}
			boundedTotal := *logBytes
			for _, path := range paths {
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() || info.Size() > 128<<10 || boundedTotal+int(info.Size()) > 256<<10 {
					t.Error("old log metadata/byte bound before original scan")
					return
				}
				boundedTotal += int(info.Size())
			}
			old.requireOnlyLoopbackInLogs(t)
			for _, path := range paths {
				name := filepath.Base(path)
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() || info.Size() > 128<<10 || *logBytes+int(info.Size()) > 256<<10 {
					t.Error("old log file metadata/byte bound", name)
					continue
				}
				raw, err := os.ReadFile(path)
				if err != nil || int64(len(raw)) != info.Size() {
					t.Error("old log changed after reap", name)
					continue
				}
				*logBytes += len(raw)
				t.Logf("HMIX_OLD_RAW case=%s name=%s sha256=%x bytes=%d payload=%s", caseName, name, sha256.Sum256(raw), len(raw), base64.StdEncoding.EncodeToString(raw))
			}
		})
	}
	t.Cleanup(stop)
	ticks, err := published31GuestStartTicks(pid)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := published31GuestFileSHA(fmt.Sprintf("/proc/%d/exe", pid), 32<<20); err != nil || got != published31GuestSHA {
		t.Fatal("actual published old process executable differs", err)
	}
	waitFor(t, 10*time.Second, "owned old listener/RPC not ready", func() bool {
		currentTicks, err := published31GuestStartTicks(pid)
		if err != nil || currentTicks != ticks || !published31GuestListener(pid, old.listenPort, namespace) || !published31GuestListener(pid, old.rpcPort, namespace) {
			return false
		}
		_, err = old.rpc("getNodeStatus", nil)
		return err == nil
	})
	return old, stop
}
