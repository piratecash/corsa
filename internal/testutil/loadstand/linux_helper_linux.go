//go:build linux

package loadstand

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	linuxResultReady linuxResultCode = iota + 1
	linuxResultUnsupported
	_ // Reserved wire result 3; no producer emits it.
	linuxResultUnavailable
)

type linuxInit struct {
	Nonce        LaunchNonce
	Role         string
	Config       ChildConfig
	TargetBinary string
	TargetHash   ExecutableSHA256
	HelperHash   ExecutableSHA256
	UID          ProcessUID
	GID          uint32
	ParentDevice uint64
	ParentInode  uint64
	NSDevice     uint64
	NSInode      uint64
}

type linuxReady struct {
	Nonce      LaunchNonce
	Code       linuxResultCode
	Provenance LinuxProcessProvenance
}

type linuxSampleReply struct {
	Nonce  LaunchNonce
	Code   linuxResultCode
	Sample LinuxProcessSample
}

type linuxStopRequest struct {
	Nonce       LaunchNonce
	BudgetNanos int64
}

func (s *linuxStartClaim) markDefect() {
	s.mu.Lock()
	s.state.defect = true
	s.mu.Unlock()
}

type linuxHelperRuntime struct {
	mu                             sync.Mutex
	readerJoinOnce                 sync.Once
	readerJoinErr                  error
	life, work                     *linuxPacketConn
	init                           linuxInit
	process                        *Process
	sampler                        *LinuxProcessSampler
	workCtx                        context.Context
	cancelWork                     context.CancelFunc
	startup                        *linuxStartClaim
	intentionalReadShutdown        bool
	status                         linuxPacketKind
	statusID                       LinuxRequestID
	statusExit                     ProcessExitResult
	wake                           chan struct{}
	lifeDone, workDone, writerDone chan struct{}
}

// RunLinuxHelper is only the dedicated fixture executable's entrypoint. The
// role is authenticated by private inherited channels and actual OS properties;
// no JSON boolean or arbitrary PID/FD grants launch ownership.
type linuxHelperSession struct {
	runtime                    *linuxHelperRuntime
	namespace, parent, profile *os.File
	provenance                 LinuxProcessProvenance
}

func RunLinuxHelper(ctx context.Context) (resultErr error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread() // close retains the creator until actual reap
	session, err := openLinuxHelperSession(ctx)
	if err != nil || session == nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, session.close(ctx)) }()
	prepared, err := session.prepare(ctx)
	if err != nil || !prepared {
		return err
	}
	return session.serve(ctx)
}
func openLinuxHelperSession(ctx context.Context) (session *linuxHelperSession, resultErr error) {
	if os.Geteuid() == 0 || os.Getuid() != os.Geteuid() {
		return nil, ErrLinuxOwnerUnsupported
	}
	files := make([]*os.File, 4)
	for index := range files {
		fd := index + 3
		unix.CloseOnExec(fd)
		files[index] = os.NewFile(uintptr(fd), "private-linux-bootstrap")
	}
	life, err := checkedLinuxPacketFile(files[0])
	if err != nil {
		for _, f := range files[1:] {
			_ = f.Close()
		}
		return nil, err
	}
	work, err := checkedLinuxPacketFile(files[1])
	if err != nil {
		return nil, errors.Join(err, life.close(), files[2].Close(), files[3].Close())
	}
	defer func() {
		if session == nil {
			resultErr = errors.Join(resultErr, life.close(), work.close(), files[2].Close(), files[3].Close())
		}
	}()
	packet, err := life.read(time.Time{})
	if errors.Is(err, io.EOF) {
		return nil, nil
	} // no target was ever started
	if err != nil {
		return nil, ErrLinuxOwnerProtocol
	}
	init, err := decodeLinuxBody[linuxInit](packet)
	if err != nil || packet.kind != linuxInitPublic || packet.id != 1 || packet.ordinal != 1 || init.Nonce == (LaunchNonce{}) || init.Role != "loadstand-linux-helper-v4" {
		return nil, ErrLinuxOwnerProtocol
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	r := &linuxHelperRuntime{life: life, work: work, init: init, startup: newLinuxStartClaim(), workCtx: workCtx, cancelWork: cancelWork, wake: make(chan struct{}, 1), lifeDone: make(chan struct{}), workDone: make(chan struct{}), writerDone: make(chan struct{})}
	// The one lifecycle reader owns all post-Init input, including slow
	// validation and preflight. It only latches Stop until a real Process exists.
	go r.readLifecycle()
	return &linuxHelperSession{runtime: r, namespace: files[2], parent: files[3]}, nil
}
func (s *linuxHelperSession) prepare(ctx context.Context) (bool, error) {
	r := s.runtime
	init, workCtx, life := r.init, r.workCtx, r.life
	if err := validateLinuxHelperInit(workCtx, init, s.namespace); err != nil {
		if r.startup.snapshot().stopped {
			return false, nil
		}
		return false, ErrLinuxOwnerProtocol
	}
	profile, err := createLinuxPrivateProfile(workCtx, s.parent, init)
	if err != nil {
		if r.startup.snapshot().stopped {
			return false, nil
		}
		ready, encodeErr := encodeLinuxPacket(linuxInitReady, 1, 1, linuxReady{Nonce: init.Nonce, Code: linuxResultUnsupported})
		if encodeErr != nil {
			return false, encodeErr
		}
		return false, life.write(ready, time.Now().Add(time.Second))
	}
	s.profile = profile
	if unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0) != nil {
		return false, ErrLinuxOwnerUnsupported
	}
	if !r.startup.claim() {
		return false, nil
	}
	process, err := launchLinuxTarget(workCtx, init)
	if err != nil {
		r.startup.failed()
		return false, err
	}
	r.process = process
	if err := r.startup.publish(process); err != nil {
		process.terminal.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
		<-process.Done()
		return false, errors.Join(err, process.ReleaseArtifacts(ctx))
	}
	// From this point the locked spawning thread must live through actual reap.
	incarnation, observeErr := ReadLinuxIncarnation(ctx, process.provenance.PID, init.UID)
	var sampler *LinuxProcessSampler
	if observeErr == nil {
		sampler, observeErr = OpenLinuxProcess(ctx, LinuxProcessExpectation{Incarnation: incarnation, UID: init.UID, ExecutableSHA256: init.TargetHash, MaxExecutableBytes: 128 << 20})
	}
	if observeErr != nil {
		process.terminal.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
		<-process.Done()
		return false, errors.Join(ErrProcessObservation, process.ReleaseArtifacts(ctx))
	}
	provenance := LinuxProcessProvenance{Incarnation: incarnation, UID: init.UID, RunningExecutableSHA256: init.TargetHash, RunningExecutableVerified: true, KernelStartTimeAvailable: true}
	r.sampler = sampler
	s.provenance = provenance
	if err := s.auditTargetDescriptors(ctx); err != nil {
		return false, err
	}
	return true, nil
}
func (s *linuxHelperSession) serve(ctx context.Context) error {
	r := s.runtime
	init, life, work, process, sampler := r.init, r.life, r.work, r.process, r.sampler
	provenance := s.provenance
	ready, err := encodeLinuxPacket(linuxInitReady, 1, 1, linuxReady{Nonce: init.Nonce, Code: linuxResultReady, Provenance: provenance})
	if err == nil {
		err = life.write(ready, time.Now().Add(time.Second))
	}
	if err != nil {
		process.terminal.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
		<-process.Done()
		return errors.Join(err, sampler.Close(), process.ReleaseArtifacts(ctx))
	}
	go r.sampleWork()
	go r.writeLifecycle()
	<-process.Done()
	exit, err := process.ExitResult()
	if err != nil {
		return err
	} // Done guarantees this cannot manufacture an exit
	r.publishStatus(linuxTargetExitKnown, exit)
	r.cancelWork()
	closeWorkErr := work.close()
	readShutdownErr := r.joinLifecycleReader()
	<-r.workDone // kernel-blocked sampler remains retained, never false join
	if errors.Join(closeWorkErr, readShutdownErr, sampler.Close(), process.ReleaseArtifacts(ctx)) != nil {
		r.startup.markDefect()
	}
	// No independent member StartReceipt exists in this primitive slice. FPA
	// resources are released only post-Done; exact final accounting is unavailable.
	r.publishStatus(linuxHelperJoined, exit)
	<-r.writerDone
	if r.startup.snapshot().defect {
		return ErrLinuxOwnerProtocol
	}
	return nil
}

func (s *linuxHelperSession) close(ctx context.Context) error {
	r := s.runtime
	r.cancelWork()
	var result error
	if r.process != nil {
		select {
		case <-r.process.Done():
		default:
			r.process.terminal.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
			<-r.process.Done() // creator thread cannot be abandoned on timeout
		}
		if r.sampler != nil {
			result = errors.Join(result, r.sampler.Close())
		}
		result = errors.Join(result, r.process.ReleaseArtifacts(ctx))
	}
	result = errors.Join(result, r.joinLifecycleReader(), r.life.close(), r.work.close(), s.namespace.Close(), s.parent.Close())
	if s.profile != nil {
		result = errors.Join(result, s.profile.Close())
	}
	return result
}
func validateLinuxHelperInit(ctx context.Context, init linuxInit, namespace *os.File) error {
	if init.Role != "loadstand-linux-helper-v4" || init.Nonce == (LaunchNonce{}) || init.UID == 0 || uint32(os.Getuid()) != uint32(init.UID) || uint32(os.Geteuid()) != uint32(init.UID) || uint32(os.Getgid()) != init.GID || uint32(os.Getegid()) != init.GID || init.Config.ValidateLoopback() != nil || auditLinuxExecutable(ctx, init.TargetBinary, init.TargetHash) != nil {
		return ErrLinuxOwnerUnsupported
	}
	config, _, err := ReadChildEnvironment(os.Getenv)
	if err != nil || config.DataDir != init.Config.DataDir || config.Listen != init.Config.Listen || config.RPC != init.Config.RPC || config.Listener != init.Config.Listener || config.Procs != init.Config.Procs || !slices.Equal(config.Bootstrap, init.Config.Bootstrap) {
		return ErrLinuxOwnerProtocol
	}
	groups, err := unix.Getgroups()
	if err != nil || len(groups) != 0 {
		return ErrLinuxOwnerUnsupported
	}
	nnp, nnpErr := unix.PrctlRetInt(unix.PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)
	seccomp, seccompErr := unix.PrctlRetInt(unix.PR_GET_SECCOMP, 0, 0, 0, 0)
	var core unix.Rlimit
	if nnpErr != nil || seccompErr != nil || nnp != 1 || seccomp != 2 || unix.Getrlimit(unix.RLIMIT_CORE, &core) != nil || core.Cur != 0 || core.Max != 0 {
		return ErrLinuxOwnerUnsupported
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil || len(status) > processFileLimit {
		return ErrLinuxOwnerUnsupported
	}
	for _, name := range []string{"CapEff:", "CapPrm:", "CapAmb:"} {
		found := false
		for line := range strings.SplitSeq(string(status), "\n") {
			if strings.HasPrefix(line, name) {
				value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, name)), 16, 64)
				if err != nil || value != 0 {
					return ErrLinuxOwnerUnsupported
				}
				found = true
			}
		}
		if !found {
			return ErrLinuxOwnerUnsupported
		}
	}
	inherited, err := namespaceIdentity(int(namespace.Fd()))
	if err != nil || inherited != (linuxNamespaceIdentity{init.NSDevice, init.NSInode}) {
		return ErrLinuxOwnerUnsupported
	}
	fd, current, err := openThreadNetNS()
	if err != nil {
		return err
	}
	if unix.Close(fd) != nil || current != inherited {
		return ErrLinuxOwnerUnsupported
	}
	incarnation, err := ReadLinuxIncarnation(ctx, ProcessID(os.Getpid()), init.UID)
	if err != nil {
		return err
	}
	self, err := OpenLinuxProcess(ctx, LinuxProcessExpectation{Incarnation: incarnation, UID: init.UID, ExecutableSHA256: init.HelperHash, MaxExecutableBytes: 128 << 20})
	if err != nil {
		return err
	}
	return self.Close()
}
func launchLinuxTarget(ctx context.Context, init linuxInit) (*Process, error) {
	_, credentials, err := ReadChildEnvironment(os.Getenv)
	if err != nil {
		return nil, err
	}
	environment, err := init.Config.environment(credentials)
	if err != nil {
		return nil, err
	}
	binding, err := newLaunchBinding(init.Config, string(init.TargetHash))
	if err != nil {
		return nil, err
	}
	artifacts, err := newLaunchArtifacts(ctx, init.Config.DataDir, binding)
	if err != nil {
		return nil, err
	}
	log, err := os.OpenFile(filepath.Join(init.Config.DataDir, "child.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.Join(err, artifacts.close())
	}
	if auditPrivateFinalFD(log, false) != nil {
		return nil, errors.Join(ErrUnsupportedArtifact, log.Close(), artifacts.close())
	}
	client, err := NewRPCClient(RPCOptions{Address: init.Config.RPC, Credentials: credentials})
	if err != nil {
		return nil, errors.Join(err, log.Close(), artifacts.close())
	}
	environment = append(environment, "LOADSTAND_FINAL_FD=3", "LOADSTAND_FINAL_NONCE="+binding.nonce.String(), "LOADSTAND_FINAL_PLAN="+binding.fingerprint.String())
	command := exec.Command(init.TargetBinary)
	command.Env = environment
	command.Dir = init.Config.DataDir
	command.Stdout = log
	command.Stderr = log
	command.ExtraFiles = []*os.File{artifacts.writer}
	// The capless helper MUST NOT set Credential/setgroups again. The direct
	// target inherits already verified UID/groups/caps; Pdeathsig is installed by exec.
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if ctx.Err() != nil {
		client.Close()
		return nil, errors.Join(ctx.Err(), log.Close(), artifacts.close())
	}
	if command.Start() != nil {
		client.Close()
		return nil, errors.Join(ErrLinuxOwnerUnsupported, log.Close(), artifacts.close())
	}
	closeErr := artifacts.writer.Close()
	artifacts.writer = nil
	p := startOwnedProcess(ctx, command, client, log, Provenance{PID: ProcessID(command.Process.Pid), LaunchedAt: time.Now().UTC(), LaunchFileSHA256: string(init.TargetHash), Platform: runtime.GOOS + "/" + runtime.GOARCH}, artifacts, closeErr != nil)
	if closeErr != nil {
		p.terminal.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
	}
	return p, nil
}
func (r *linuxHelperRuntime) requestStop(id LinuxRequestID, budget time.Duration, defect bool) {
	r.startup.observeStop(id, time.Now().Add(budget), defect)
	r.cancelWork()
}
func (r *linuxHelperRuntime) joinLifecycleReader() error {
	r.readerJoinOnce.Do(func() {
		r.mu.Lock()
		r.intentionalReadShutdown = true
		r.mu.Unlock()
		shutdownErr := r.life.conn.CloseRead()
		deadlineErr := r.life.conn.SetReadDeadline(time.Now())
		<-r.lifeDone
		r.readerJoinErr = errors.Join(shutdownErr, deadlineErr)
	})
	return r.readerJoinErr
}
func (r *linuxHelperRuntime) readLifecycle() {
	defer close(r.lifeDone)
	for {
		packet, err := r.life.read(time.Time{})
		if err != nil {
			r.mu.Lock()
			intentional := r.intentionalReadShutdown
			r.mu.Unlock()
			if !intentional {
				r.requestStop(0, 5*time.Second, !errors.Is(err, io.EOF))
			}
			return
		}
		body, err := decodeLinuxBody[linuxStopRequest](packet)
		if err != nil || packet.kind != linuxStop || packet.ordinal != 1 || packet.id <= 1 || body.Nonce != r.init.Nonce || body.BudgetNanos <= 0 {
			r.requestStop(0, 5*time.Second, true)
			return
		}
		r.requestStop(packet.id, time.Duration(body.BudgetNanos), false)
	}
}
func (r *linuxHelperRuntime) sampleWork() {
	defer close(r.workDone)
	var lastID LinuxRequestID = 1
	for {
		packet, err := r.work.read(time.Time{})
		if err != nil {
			if r.workCtx.Err() == nil {
				r.requestStop(0, 5*time.Second, !errors.Is(err, io.EOF))
			}
			return
		}
		body, err := decodeLinuxBody[linuxSampleRequest](packet)
		if err != nil || (packet.kind != linuxSample && packet.kind != linuxObserveRuntime) || packet.ordinal != 1 || packet.id <= lastID || body.Nonce != r.init.Nonce || body.BudgetNanos <= 0 {
			r.requestStop(0, 5*time.Second, true)
			return
		}
		lastID = packet.id
		ctx, cancel := context.WithTimeout(r.workCtx, time.Duration(body.BudgetNanos))
		if packet.kind == linuxObserveRuntime {
			encodeErr := r.writeRuntimeObservation(ctx, packet)
			cancel()
			if encodeErr != nil {
				r.requestStop(0, 5*time.Second, true)
				return
			}
			continue
		}
		sample, err := r.sampler.Sample(ctx)
		code := linuxResultReady
		if err != nil {
			code = linuxResultUnavailable
		}
		encoded, encodeErr := encodeLinuxPacket(linuxSampleResult, packet.id, 1, linuxSampleReply{Nonce: r.init.Nonce, Code: code, Sample: sample})
		deadline, _ := ctx.Deadline()
		deadline = minLinuxPacketDeadline(deadline)
		if encodeErr == nil {
			encodeErr = r.work.write(encoded, deadline)
		}
		cancel()
		if encodeErr != nil {
			r.requestStop(0, 5*time.Second, true)
			return
		}
	}
}
func minLinuxPacketDeadline(deadline time.Time) time.Time {
	ceiling := time.Now().Add(time.Second)
	if deadline.IsZero() || ceiling.Before(deadline) {
		return ceiling
	}
	return deadline
}
func (r *linuxHelperRuntime) publishStatus(kind linuxPacketKind, exit ProcessExitResult) {
	observation := r.startup.snapshot()
	r.mu.Lock()
	if kind > r.status {
		r.status = kind
		r.statusID = observation.stopID
		r.statusExit = exit
	}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *linuxHelperRuntime) writeLifecycle() {
	defer close(r.writerDone)
	for range r.wake {
		r.mu.Lock()
		kind, id, exit := r.status, r.statusID, r.statusExit
		r.mu.Unlock()
		defect := r.startup.snapshot().defect
		code := linuxResultReady
		if defect {
			code = linuxResultUnavailable
		}
		packet, err := encodeLinuxPacket(kind, id, 1, linuxTerminal{Nonce: r.init.Nonce, Code: code, Exit: exit})
		if err == nil {
			err = r.life.write(packet, minLinuxPacketDeadline(time.Time{}))
		}
		if err != nil {
			r.startup.markDefect()
			_ = r.life.close()
			return
		}
		if kind == linuxHelperJoined {
			_ = r.life.close()
			return
		}
	}
}
