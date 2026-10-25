//go:build linux

package loadstand

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type procFunctionalReady struct {
	Sample   LinuxProcessSample `json:"sample"`
	Controls map[string]bool    `json:"controls"`
	ChildPID ProcessID          `json:"child_pid"`
}
type procFunctionalFinal struct {
	Usage        ProcessMetric[ProcessFinalUsage] `json:"usage"`
	DeadRefused  bool                             `json:"dead_refused"`
	CloseRefused bool                             `json:"close_refused"`
}

func TestLinuxProcFunctional(t *testing.T) {
	if os.Getenv("LOADSTAND_PROC_FUNCTIONAL") != "1" {
		t.Skip("explicit bounded Linux functional container only")
	}
	if os.Geteuid() != 0 {
		t.Fatal("fixture root must launch sameUID helper")
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent, child := os.NewFile(uintptr(pair[0]), "parent-control"), os.NewFile(uintptr(pair[1]), "helper-control")
	connection, err := net.FileConn(parent)
	_ = parent.Close()
	if err != nil {
		_ = child.Close()
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	helper := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLinuxProcHelper$", "-test.timeout=10s")
	helper.Env = append(os.Environ(), "LOADSTAND_PROC_ROLE=helper")
	helper.ExtraFiles = []*os.File{child}
	helper.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 10001, Gid: 10001}}
	helper.Stdout = os.Stdout
	helper.Stderr = os.Stderr
	if err = helper.Start(); err != nil {
		_ = child.Close()
		t.Fatal(err)
	}
	_ = child.Close()
	defer func() {
		_ = connection.Close()
		if helper.ProcessState == nil {
			_ = helper.Wait()
		}
	}()
	if err = connection.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(io.LimitReader(connection, 64<<10))
	decoder.DisallowUnknownFields()
	var ready procFunctionalReady
	if err = decoder.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if !ready.Sample.Provenance.RunningExecutableVerified || !ready.Sample.Provenance.KernelStartTimeAvailable || ready.Sample.Provenance.UID != 10001 {
		t.Fatal("unverified provenance")
	}
	if _, err = ready.Sample.CPU.Frequency.Get(); err != nil {
		t.Fatal("AT_CLKTCK unavailable in fixture")
	}
	if _, err = ready.Sample.CPU.UserNanoseconds.Get(); err != nil {
		t.Fatal(err)
	}
	if rss, e := ready.Sample.Memory.RSS.Get(); e != nil || rss == 0 {
		t.Fatal("missing actual RSS")
	}
	if _, e := ready.Sample.Memory.HighWaterRSS.Get(); e != nil {
		t.Fatal(e)
	}
	if _, e := ready.Sample.Memory.AnonymousRSS.Get(); e != nil {
		t.Fatal(e)
	}
	if count, e := ready.Sample.Descriptors.Get(); e != nil || count == 0 {
		t.Fatal("fd reading unavailable")
	}
	if ready.Sample.Scheduler.Scope != ProcessSchedulerLeaderThread {
		t.Fatal("scheduler scope hidden")
	}
	if _, e := ready.Sample.FinalWait4.Get(); e == nil {
		t.Fatal("external sampler forged wait ownership")
	}
	for _, name := range []string{"zero_value_safe", "same_uid_no_caps", "seccomp_nnp", "wrong_start", "wrong_digest", "wrong_uid", "bounded_exe", "cancelled", "permission_refused", "permission_recovered", "control_private"} {
		if !ready.Controls[name] {
			t.Fatalf("control failed: %s", name)
		}
	}
	// A parent EOF must drain the sameUID-owned child and return its real wait4.
	if err = connection.(*net.UnixConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var final procFunctionalFinal
	if err = decoder.Decode(&final); err != nil {
		t.Fatal(err)
	}
	if !final.DeadRefused || !final.CloseRefused {
		t.Fatal("dead/closed sampler reused a prior observation")
	}
	if _, err = final.Usage.Get(); err != nil {
		t.Fatal("owned wait4 unavailable")
	}
	if err = helper.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Log("sameUID helper: actual exe/start, status/fd/ticks; EOF drains child and owned wait4; no CAP_KILL/SYS_PTRACE")
}
func TestLinuxProcHelper(t *testing.T) {
	if os.Getenv("LOADSTAND_PROC_ROLE") != "helper" {
		t.Skip("private helper")
	}
	unix.CloseOnExec(3)
	controlFile := os.NewFile(3, "helper-control")
	control, err := net.FileConn(controlFile)
	_ = controlFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close() }()
	if err = unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err = unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLinuxProcTarget$", "-test.timeout=8s")
	target.Env = append(os.Environ(), "LOADSTAND_PROC_ROLE=target", "LOADSTAND_PROC_HELPER_PID="+strconv.Itoa(os.Getpid()))
	// ExtraFiles is empty. The parent's control endpoint is CLOEXEC and the
	// helper is nondumpable, so the sameUID target cannot acquire that channel.
	input, err := target.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := target.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	target.Stderr = os.Stderr
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	target.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	target.Cancel = func() error { return target.Process.Signal(syscall.SIGTERM) }
	target.WaitDelay = time.Second
	if err = target.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	var waitResult error
	go func() { waitResult = target.Wait(); close(waited) }()
	defer func() {
		cancel()
		select {
		case <-waited:
		case <-time.After(2 * time.Second):
			os.Exit(3)
		}
	}()
	reader := bufio.NewReader(io.LimitReader(output, 16<<10))
	line, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		cancel()
		<-waited
		t.Fatal("target readiness")
	}
	request := func(command string) string {
		if _, e := fmt.Fprintln(input, command); e != nil {
			t.Fatal(e)
		}
		reply, e := reader.ReadString('\n')
		if e != nil {
			t.Fatal(e)
		}
		return strings.TrimSpace(reply)
	}
	uid := ProcessUID(os.Geteuid())
	incarnation, err := ReadLinuxIncarnation(t.Context(), ProcessID(target.Process.Pid), uid)
	if err != nil {
		t.Fatal(err)
	}
	expected := LinuxProcessExpectation{Incarnation: incarnation, UID: uid, ExecutableSHA256: ExecutableSHA256(os.Getenv("LOADSTAND_PROC_EXPECTED_SHA256")), MaxExecutableBytes: 64 << 20}
	sampler, err := OpenLinuxProcess(t.Context(), expected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sampler.Close(); err != nil {
			t.Error(err)
		}
	}()
	sample, err := sampler.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	controls := map[string]bool{"control_private": request("control-private") == "denied"}
	var zero LinuxProcessSampler
	_, zeroErr := zero.Sample(t.Context())
	controls["zero_value_safe"] = zeroErr != nil && zero.Close() == nil
	if _, err := sampler.Sample(t.Context()); err != nil {
		t.Fatal("zero-value close damaged live descriptors")
	}

	selfStatus, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	controls["same_uid_no_caps"] = uid == 10001 && strings.Contains(string(selfStatus), "CapEff:\t0000000000000000")
	controls["seccomp_nnp"] = strings.Contains(string(selfStatus), "Seccomp:\t2") && strings.Contains(string(selfStatus), "NoNewPrivs:\t1")
	for _, name := range []string{"wrong_start", "wrong_digest", "wrong_uid", "bounded_exe"} {
		bad := expected
		switch name {
		case "wrong_start":
			bad.Incarnation.StartTicks++
		case "wrong_digest":
			bad.ExecutableSHA256 = ExecutableSHA256(strings.Repeat("0", 64))
		case "wrong_uid":
			bad.UID++
		case "bounded_exe":
			bad.MaxExecutableBytes = 1
		}
		handle, e := OpenLinuxProcess(t.Context(), bad)
		controls[name] = e != nil
		if handle != nil {
			_ = handle.Close()
		}
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	_, err = sampler.Sample(cancelled)
	controls["cancelled"] = errors.Is(err, context.Canceled)
	if request("nondumpable") != "ok" {
		t.Fatal("cannot set nondumpable")
	}
	_, err = sampler.Sample(t.Context())
	controls["permission_refused"] = err != nil
	if request("dumpable") != "ok" {
		t.Fatal("cannot restore fixture dumpability")
	}
	_, err = sampler.Sample(t.Context())
	controls["permission_recovered"] = err == nil
	encoder := json.NewEncoder(control)
	if err = encoder.Encode(procFunctionalReady{Sample: sample, Controls: controls, ChildPID: incarnation.PID}); err != nil {
		t.Fatal(err)
	}
	// The controller owns this private endpoint. EOF always ends the target;
	// any nonempty command is a defect rather than an unbounded extension.
	buffer := make([]byte, 1)
	n, readErr := control.Read(buffer)
	if n == 1 && buffer[0] == '!' {
		os.Exit(4)
	}
	if n != 0 || readErr != io.EOF {
		cancel()
		<-waited
		t.Fatal("invalid helper lifecycle command")
	}
	cancel()
	<-waited
	waitErr := waitResult
	if target.ProcessState == nil || !target.ProcessState.Success() {
		t.Fatalf("target did not exit0 after EOF: %v", waitErr)
	}
	_, err = sampler.Sample(t.Context())
	deadRefused := err != nil
	if err = sampler.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = sampler.Sample(t.Context())
	closeRefused := err != nil
	if err = encoder.Encode(procFunctionalFinal{Usage: ownedProcessUsage(target.ProcessState), DeadRefused: deadRefused, CloseRefused: closeRefused}); err != nil {
		t.Fatal(err)
	}
}
func TestLinuxProcTarget(t *testing.T) {
	if os.Getenv("LOADSTAND_PROC_ROLE") != "target" {
		t.Skip("private target")
	}
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		t.Fatal(err)
	}
	if dumpable, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0); err != nil || dumpable != 1 {
		t.Fatal("ordinary target exec must be dumpable for sameUID observation")
	}
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 128), 128)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
		close(lines)
	}()
	fmt.Println("ready")
	for {
		select {
		case <-ctx.Done():
			return
		case command, ok := <-lines:
			if !ok {
				return
			}
			switch command {
			case "control-private":
				file, err := os.Open("/proc/" + os.Getenv("LOADSTAND_PROC_HELPER_PID") + "/fd")
				if err == nil {
					_ = file.Close()
					fmt.Println("accessible")
				} else if errors.Is(err, os.ErrPermission) {
					fmt.Println("denied")
				} else {
					fmt.Println("unexpected")
				}
			case "nondumpable":
				if unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0) != nil {
					t.Fatal("prctl")
				}
				fmt.Println("ok")
			case "dumpable":
				if unix.Prctl(unix.PR_SET_DUMPABLE, 1, 0, 0, 0) != nil {
					t.Fatal("prctl")
				}
				fmt.Println("ok")
			default:
				t.Fatal("unknown fixture command")
			}
		}
	}
}

func TestLinuxProcHelperDeathFunctional(t *testing.T) {
	if os.Getenv("LOADSTAND_PROC_FUNCTIONAL") != "1" {
		t.Skip("explicit bounded Linux functional container only")
	}
	if os.Geteuid() != 0 {
		t.Fatal("fixture root required")
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent, child := os.NewFile(uintptr(pair[0]), "parent-control"), os.NewFile(uintptr(pair[1]), "helper-control")
	connection, err := net.FileConn(parent)
	_ = parent.Close()
	if err != nil {
		_ = child.Close()
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	helper := exec.Command(os.Args[0], "-test.run=^TestLinuxProcHelper$", "-test.timeout=10s")
	helper.Env = append(os.Environ(), "LOADSTAND_PROC_ROLE=helper")
	helper.ExtraFiles = []*os.File{child}
	helper.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 10001, Gid: 10001}}
	helper.Stdout, helper.Stderr = os.Stdout, os.Stderr
	if err = helper.Start(); err != nil {
		_ = child.Close()
		t.Fatal(err)
	}
	_ = child.Close()
	defer func() {
		_ = connection.Close()
		if helper.ProcessState == nil {
			_ = helper.Wait()
		}
	}()
	if err = connection.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(io.LimitReader(connection, 64<<10))
	decoder.DisallowUnknownFields()
	var ready procFunctionalReady
	if err = decoder.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Write([]byte{'!'}); err != nil {
		t.Fatal(err)
	}
	err = helper.Wait()
	if err == nil || helper.ProcessState == nil || helper.ProcessState.ExitCode() != 4 {
		t.Fatalf("helper did not take deliberate crash path: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile("/proc/" + strconv.Itoa(int(ready.ChildPID)) + "/stat")
		if errors.Is(readErr, os.ErrNotExist) {
			return
		}
		if readErr != nil {
			t.Fatal("cannot prove old child stopped")
		}
		close := strings.LastIndexByte(string(raw), ')')
		if close < 0 || close+2 >= len(raw) {
			t.Fatal("malformed child lifecycle evidence")
		}
		if raw[close+2] == 'Z' || raw[close+2] == 'X' {
			return
		}
		stat, parseErr := parseProcessStat(raw)
		if parseErr != nil {
			t.Fatal("unknown child state")
		}
		if stat.ID != ready.Sample.Provenance.Incarnation {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("helper death left its old child running")
}

func TestLinuxProcessReadElapsedProjection(t *testing.T) {
	base := time.Now()
	mono := base != base.Round(0)
	for _, tc := range []struct {
		name       string
		start, end time.Time
		want       int64
		known      bool
	}{
		{"zero", base, base, 0, mono},
		{"positive", base, base.Add(17 * time.Nanosecond), 17, mono},
		{"missing-start", base.Round(0), base.Add(time.Second), 0, false},
		{"missing-end", base, base.Add(time.Second).Round(0), 0, false},
		{"wall-only", time.Unix(1, 0), time.Unix(2, 0), 0, false},
		{"zero-times", time.Time{}, time.Time{}, 0, false},
		{"backwards", base.Add(time.Second), base, 0, false},
		{"saturated-or-stripped", base, base.Add(time.Duration(1<<63 - 1)), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, known := processSampleReadElapsed(tc.start, tc.end)
			want := tc.want
			if !tc.known {
				want = 0
			}
			if got != want || known != tc.known {
				t.Fatal("read-bracket projection", got, known, want, tc.known)
			}
		})
	}
}

// Uses only the current same-UID test process and original /proc binding APIs.
func TestLinuxProcessExecutableReadBrackets(t *testing.T) {
	uid := ProcessUID(os.Geteuid())
	if uid == 0 {
		t.Fatal("same nonroot UID fixture required")
	}
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if info.Size() < 4 || info.Size() > 128<<20 {
		_ = file.Close()
		t.Fatal("self executable outside original cap", info.Size())
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(file, (128<<20)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || n != info.Size() {
		t.Fatal("bound current executable", n, readErr, closeErr)
	}
	incarnation, err := ReadLinuxIncarnation(t.Context(), ProcessID(os.Getpid()), uid)
	if err != nil {
		t.Fatal(err)
	}
	expected := LinuxProcessExpectation{Incarnation: incarnation, UID: uid, ExecutableSHA256: ExecutableSHA256(hex.EncodeToString(h.Sum(nil))), MaxExecutableBytes: 128 << 20}
	sampler, err := OpenLinuxProcess(t.Context(), expected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sampler.Close(); err != nil {
			t.Error(err)
		}
	}()
	t.Run("observed-and-legacy", func(t *testing.T) {
		sample, err := sampler.Sample(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		first, last := sample.ExecutableReadBrackets.First, sample.ExecutableReadBrackets.Last
		if !sample.ReadElapsedKnown || !first.ElapsedKnown || !last.ElapsedKnown {
			t.Fatal("admitted Linux monotonic read/stream diagnostics unavailable", sample)
		}
		if first.VerifiedBytes != info.Size() || last.VerifiedBytes != info.Size() || first.ElapsedNS < 0 || last.ElapsedNS < 0 {
			t.Fatal("completed stream diagnostics", first, last)
		}
		if sample.ReadElapsedKnown && first.ElapsedKnown && last.ElapsedKnown && (first.ElapsedNS > sample.ReadElapsedNS || last.ElapsedNS > sample.ReadElapsedNS-first.ElapsedNS) {
			t.Fatal("stream brackets escape containing read", sample)
		}
		identity, err := sampler.verifyExecutable(t.Context())
		if err != nil || identity.size != info.Size() {
			t.Fatal("legacy verification changed", identity, err)
		}
	})
	t.Run("wrong-expectations", func(t *testing.T) {
		wrong := expected
		wrong.ExecutableSHA256 = ExecutableSHA256(strings.Repeat("0", 64))
		if refused, err := OpenLinuxProcess(t.Context(), wrong); !errors.Is(err, ErrProcessObservation) || refused != nil {
			if refused != nil {
				_ = refused.Close()
			}
			t.Fatal("wrong digest published sampler", err)
		}
		bounded := expected
		bounded.MaxExecutableBytes = info.Size() - 1
		if refused, err := OpenLinuxProcess(t.Context(), bounded); !errors.Is(err, ErrProcessObservation) || refused != nil {
			if refused != nil {
				_ = refused.Close()
			}
			t.Fatal("original size guard bypassed", err)
		}
		wrongIncarnation := expected
		wrongIncarnation.Incarnation.StartTicks++
		if refused, err := OpenLinuxProcess(t.Context(), wrongIncarnation); !errors.Is(err, ErrProcessObservation) || refused != nil {
			if refused != nil {
				_ = refused.Close()
			}
			t.Fatal("wrong incarnation admitted", err)
		}
		wrongUID := expected
		wrongUID.UID++
		if refused, err := OpenLinuxProcess(t.Context(), wrongUID); !errors.Is(err, ErrInvalidConfiguration) || refused != nil {
			if refused != nil {
				_ = refused.Close()
			}
			t.Fatal("foreign UID admitted", err)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		sample, err := sampler.Sample(ctx)
		if !errors.Is(err, context.Canceled) || sample != (LinuxProcessSample{}) {
			t.Fatal("failed sample published metrics", sample, err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		if err := sampler.Close(); err != nil {
			t.Fatal(err)
		}
		sample, err := sampler.Sample(t.Context())
		if !errors.Is(err, ErrProcessObservation) || sample != (LinuxProcessSample{}) {
			t.Fatal("closed sampler reused metrics", sample, err)
		}
	})

}
