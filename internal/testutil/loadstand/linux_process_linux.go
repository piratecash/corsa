//go:build linux

package loadstand

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type LinuxFixtureSpec struct {
	planned      *linuxPlannedLaunch
	HelperBinary string
	HelperSHA256 ExecutableSHA256
	TargetBinary string
	TargetSHA256 ExecutableSHA256
	Config       ChildConfig
	Credentials  *Credentials
	UID          ProcessUID
	GID          uint32
}
type LinuxHelperState uint8

const (
	LinuxHelperPending LinuxHelperState = iota + 1
	LinuxHelperNotStarted
	LinuxHelperRunning
	LinuxHelperReaped
)

type LinuxFixtureResult struct {
	HelperState      LinuxHelperState
	TargetKnown      bool
	TargetExit       ProcessExitResult
	HelperJoined     bool
	HelperWaitKnown  bool
	HelperWait       ProcessExitResult // helper cost, never target wait4
	StopDelivery     LinuxStopDelivery
	StopAcknowledged bool
	Poisoned         bool
	Finality         Finality
}

// Only the fixed work owner publishes this once, before poison/notification.
type linuxInitializationResult struct {
	known bool
	err   error
}

type linuxSampleExchange struct {
	receiptLimit   int
	receiptsResult chan linuxMemberReceiptsOutcome
	bindMember     bool
	memberResult   chan linuxMemberStartOutcome
	initialize     bool
	observe        bool
	runtimeResult  chan linuxRuntimeOutcome
	ctx            context.Context
	id             LinuxRequestID
	result         chan linuxSampleOutcome
}
type linuxSampleOutcome struct {
	sample LinuxProcessSample
	err    error
}

// LinuxFixture retains the original helper/channel ownership. The reviewed
// planned Member uses its private path; legacy one-use callers remain intact.
// Mutable terminal state is guarded by mu; I/O owners never hold it while waiting.
type LinuxFixture struct {
	plannedReleased, plannedConsumed                              bool
	memberFinal                                                   *linuxMemberFinal
	mu                                                            sync.Mutex
	stopSubmitMu                                                  sync.Mutex
	initializationOnce, resolvedOnce, waitOnce                    sync.Once
	releaseGate                                                   chan struct{}
	spec                                                          LinuxFixtureSpec
	parent                                                        *linuxProfileLease
	namespace                                                     *OwnedNetNS
	remote                                                        netip.AddrPort
	correlation                                                   linuxCorrelation
	life, work                                                    *linuxPacketConn
	childLife, childWork, childNS                                 *os.File
	command                                                       *exec.Cmd
	initResult                                                    linuxInitializationResult
	initialized                                                   chan struct{}
	startResolved                                                 chan struct{}
	helperWaitDone                                                chan struct{}
	lifeDone, workDone                                            chan struct{}
	stoppingDone                                                  chan struct{}
	exchanges                                                     chan linuxSampleExchange
	startCalled, running, stopping, workBusy, beforeTargetRefused bool
	helperJoined, helperWaitKnown, targetKnown                    bool
	helperWait, targetExit                                        ProcessExitResult
	readyProvenance                                               LinuxProcessProvenance
	rpc                                                           *RPCClient
	helperState                                                   LinuxHelperState
	runtimeObservation                                            LinuxOwnedRuntimeObservation
}

func (o *LinuxFixture) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprint(state, "LinuxFixture(one-owned-launch)")
}
func (o *LinuxFixture) RPC() *RPCClient { return o.rpc }

func (p *AuditedProfileParent) MemberDirectory() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return filepath.Join(p.path, "member-1")
}
func auditLinuxExecutable(ctx context.Context, path string, expected ExecutableSHA256) error {
	raw, err := hex.DecodeString(string(expected))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != string(expected) || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrInvalidConfiguration
	}
	parent, err := openLinuxProfileAncestor(filepath.Dir(path))
	if err != nil {
		return ErrLinuxOwnerUnsupported
	}
	defer func() { _ = parent.Close() }()
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrLinuxOwnerUnsupported
	}
	file := os.NewFile(uintptr(fd), "immutable-public-executable")
	defer func() { _ = file.Close() }()
	var before, after unix.Stat_t
	if unix.Fstat(fd, &before) != nil || before.Uid != 0 || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&07777 != 0555 || before.Size <= 0 || before.Size > 128<<20 {
		return ErrLinuxOwnerUnsupported
	}
	var magic [4]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil || magic != [4]byte{0x7f, 'E', 'L', 'F'} {
		return ErrLinuxOwnerUnsupported
	}
	digest := sha256.New()
	count, err := io.Copy(digest, io.NewSectionReader(file, 0, before.Size))
	if err != nil || count != before.Size || hex.EncodeToString(digest.Sum(nil)) != string(expected) || unix.Fstat(fd, &after) != nil || before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mtim != after.Mtim || before.Ctim != after.Ctim {
		return ErrLinuxOwnerUnsupported
	}
	info, statErr := file.Stat()
	pathInfo, pathErr := os.Lstat(path)
	if statErr != nil || pathErr != nil || !os.SameFile(info, pathInfo) {
		return ErrLinuxOwnerUnsupported
	}
	return ctx.Err() // source audit only; actual proc/exe verification is separate
}
func CreateLinuxFixture(ctx context.Context, spec LinuxFixtureSpec, parent *AuditedProfileParent, namespace *OwnedNetNS) (*LinuxFixture, error) {
	o, err := newLinuxFixtureResources(ctx, spec, parent, namespace)
	if err != nil {
		return nil, err
	}
	go o.receiveLifecycle()
	go o.runWork()
	return o, nil
}
func newLinuxFixtureResources(ctx context.Context, spec LinuxFixtureSpec, parent *AuditedProfileParent, namespace *OwnedNetNS) (*LinuxFixture, error) {
	if os.Geteuid() != 0 || namespace == nil || parent == nil || spec.UID == 0 || spec.GID == 0 || !spec.Credentials.valid() || !validLinuxFixtureConfiguration(spec, namespace) || spec.Config.DataDir != parent.MemberDirectory() || ValidateAmbientEnvironment(os.Environ()) != nil {
		return nil, ErrInvalidConfiguration
	}
	if err := errors.Join(auditLinuxExecutable(ctx, spec.HelperBinary, spec.HelperSHA256), auditLinuxExecutable(ctx, spec.TargetBinary, spec.TargetSHA256)); err != nil {
		return nil, err
	}
	remote, err := netip.ParseAddrPort(string(spec.Config.RPC))
	if err != nil || remote.Addr() != netip.MustParseAddr("127.0.0.1") {
		return nil, ErrInvalidConfiguration
	}
	lease, err := parent.acquire()
	if err != nil {
		return nil, err
	}
	life, childLife, err := newLinuxPacketPair()
	if err != nil {
		return nil, errors.Join(err, lease.file.Close())
	}
	work, childWork, err := newLinuxPacketPair()
	if err != nil {
		return nil, errors.Join(err, life.conn.Close(), childLife.Close(), lease.file.Close())
	}
	namespace.mu.Lock()
	if namespace.fd < 0 || namespace.closed || namespace.poisoned || namespace.done == nil || namespace.busy || namespace.claimed && !consumePlannedNamespaceReuseLocked(spec, namespace) {
		namespace.mu.Unlock()
		return nil, errors.Join(ErrLinuxOwnerUnsupported, life.conn.Close(), work.conn.Close(), childLife.Close(), childWork.Close(), lease.file.Close())
	}
	namespace.claimed = true
	namespace.mu.Unlock()
	config := spec.Config
	config.Bootstrap = append(config.Bootstrap[:0:0], config.Bootstrap...)
	spec.Config = config
	var binding launchBinding
	if spec.planned == nil {
		binding, err = newLaunchBinding(spec.Config, string(spec.TargetSHA256))
	} else {
		binding = spec.planned.binding
	}
	if err != nil {
		return nil, errors.Join(err, life.conn.Close(), work.conn.Close(), childLife.Close(), childWork.Close(), lease.file.Close())
	}
	o := &LinuxFixture{spec: spec, parent: lease, namespace: namespace, remote: remote, life: life, work: work, childLife: childLife, childWork: childWork, correlation: linuxCorrelation{nonce: binding.nonce, ordinal: 1, lastID: 1}, initialized: make(chan struct{}), startResolved: make(chan struct{}), helperWaitDone: make(chan struct{}), lifeDone: make(chan struct{}), workDone: make(chan struct{}), stoppingDone: make(chan struct{}), exchanges: make(chan linuxSampleExchange), releaseGate: make(chan struct{}, 1)}
	o.releaseGate <- struct{}{}
	o.helperState = LinuxHelperPending
	o.rpc, err = NewRPCClient(RPCOptions{Address: spec.Config.RPC, Credentials: spec.Credentials, DialContext: boundedLinuxRPCDial(o.dialContext)})
	if err != nil {
		return nil, errors.Join(err, life.close(), work.close(), childLife.Close(), childWork.Close(), lease.file.Close())
	}
	if spec.planned != nil {
		namespace.mu.Lock()
		namespace.plannedOwner = o
		namespace.mu.Unlock()
		spec.planned.previous = nil
	}
	return o, nil
}
func (o *LinuxFixture) Start(ctx context.Context) error {
	_, ok := ctx.Deadline()
	if !ok {
		return ErrLinuxOwnerBound
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	o.mu.Lock()
	if o.startCalled || o.stopping {
		o.mu.Unlock()
		return ErrLinuxOwnerPoisoned
	}
	o.startCalled = true
	o.mu.Unlock()
	operation := linuxNamespaceOperation{ctx: ctx, owner: o, result: make(chan error, 1)}
	if err := o.namespace.submit(ctx, operation); err != nil {
		o.resolveNeverStarted()
		o.poison()
		o.initializationOnce.Do(func() { close(o.initialized) })
		return err
	}
	select {
	case err := <-operation.result:
		if err != nil {
			o.poison()
			o.initializationOnce.Do(func() { close(o.initialized) })
			return err
		}
	case <-ctx.Done():
		o.poison()
		o.initializationOnce.Do(func() { close(o.initialized) })
		return errors.Join(ErrStopUnconfirmed, ctx.Err())
	}
	exchange := linuxSampleExchange{initialize: true, ctx: ctx}
	select {
	case o.exchanges <- exchange:
	case <-ctx.Done():
		o.poison()
		o.initializationOnce.Do(func() { close(o.initialized) })
		return ctx.Err()
	case <-o.stoppingDone:
		o.initializationOnce.Do(func() { close(o.initialized) })
		return ErrLinuxOwnerPoisoned
	}
	return o.awaitInitialization(ctx)
}

// The retained row is authority; initialized only wakes the waiter. Stop and
// cancellation cannot hide an already observed failure or revive old Ready.
func (o *LinuxFixture) awaitInitialization(ctx context.Context) error {
	for {
		o.mu.Lock()
		result, stopped := o.initResult, o.stopping || o.correlation.poisoned
		ctxErr := ctx.Err()
		notified := false
		select {
		case <-o.initialized:
			notified = true
		default:
		}
		decided := true
		var err error
		switch {
		case result.known && result.err != nil:
			err = result.err
			if ctxErr != nil {
				err = errors.Join(err, ctxErr)
			}
		case ctxErr != nil:
			err = ctxErr
		case stopped:
			err = ErrLinuxOwnerPoisoned
		case result.known:
			// Ready succeeds only at this live, non-stopping decision.
		case notified:
			err = ErrLinuxOwnerPoisoned // NeverStarted is not parsed Ready.
		default:
			decided = false
		}
		o.mu.Unlock()
		if decided {
			if ctxErr != nil {
				o.poison()
			}
			if ctxErr != nil || (stopped && (!result.known || result.err == nil)) {
				_ = o.life.conn.SetReadDeadline(time.Now())
			}
			return err
		}
		select {
		case <-o.initialized:
		case <-ctx.Done():
		case <-o.stoppingDone:
		}
	}
}
func (o *LinuxFixture) initializeOnWorkOwner(ctx context.Context) (resultErr error) {
	var ready linuxReady
	refused := false
	defer func() {
		o.mu.Lock()
		if o.initResult.known {
			resultErr = ErrLinuxOwnerProtocol // retain the original result.
		} else {
			o.initResult = linuxInitializationResult{known: true, err: resultErr}
			if refused {
				o.beforeTargetRefused = true
			}
			if resultErr == nil && !o.stopping && !o.correlation.poisoned && ctx.Err() == nil {
				o.readyProvenance = ready.Provenance
				o.running = true
			}
		}
		o.mu.Unlock()
		if resultErr != nil {
			o.poison()
		}
		o.initializationOnce.Do(func() { close(o.initialized) })
	}()
	ready, refused, resultErr = o.exchangeLinuxInitialization(ctx)
	return resultErr
}
func (o *LinuxFixture) exchangeLinuxInitialization(ctx context.Context) (ready linuxReady, refused bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return ready, refused, err
	}
	deadline, _ := ctx.Deadline()
	init := linuxInit{Nonce: o.correlation.nonce, Role: "loadstand-linux-helper-v4", Config: o.spec.Config, TargetBinary: o.spec.TargetBinary, TargetHash: o.spec.TargetSHA256, HelperHash: o.spec.HelperSHA256, UID: o.spec.UID, GID: o.spec.GID, ParentDevice: o.parent.device, ParentInode: o.parent.inode, NSDevice: o.namespace.identity.device, NSInode: o.namespace.identity.inode}
	if p := o.spec.planned; p != nil {
		init.Planned = &linuxPlannedInit{Plan: p.plan, Ordinal: p.ordinal, FinalNonce: p.binding.nonce, FinalFingerprint: p.binding.fingerprint, ProfileDevice: p.profileDevice, ProfileInode: p.profileInode}
		if p.membership != nil {
			descriptor := p.membership.descriptor
			init.Planned.Membership = &descriptor
		}
	}
	raw, err := encodeLinuxPacket(linuxInitPublic, 1, 1, init)
	if err == nil {
		err = o.life.write(raw, deadline)
	}
	var packet linuxPacket
	if err == nil {
		packet, err = o.life.read(deadline)
	}
	if err == nil {
		ready, err = decodeLinuxBody[linuxReady](packet)
	}
	if err == nil && (packet.kind != linuxInitReady || packet.id != 1 || packet.ordinal != 1 || ready.Nonce != init.Nonce) {
		err = ErrLinuxOwnerProtocol
	}
	if err == nil && ready.Code != linuxResultReady {
		if ready.Code == linuxResultUnsupported {
			refused = true
			err = ErrLinuxOwnerUnsupported
		} else {
			err = ErrLinuxOwnerProtocol
		}
	}
	if err == nil && (!ready.Provenance.RunningExecutableVerified || !ready.Provenance.KernelStartTimeAvailable || ready.Provenance.UID != o.spec.UID || ready.Provenance.RunningExecutableSHA256 != o.spec.TargetSHA256 || ready.Provenance.Incarnation.PID <= 0 || ready.Provenance.Incarnation.StartTicks == 0) {
		err = ErrLinuxOwnerProtocol
	}
	return ready, refused, err
}

func (o *LinuxFixture) resolveNeverStarted() {
	o.mu.Lock()
	o.helperState = LinuxHelperNotStarted
	o.beforeTargetRefused = true
	o.mu.Unlock()
	o.resolvedOnce.Do(func() { close(o.startResolved) })
	o.waitOnce.Do(func() { close(o.helperWaitDone) })
}
func (o *LinuxFixture) closeBootstrap() error {
	var err error
	for _, file := range []*os.File{o.childLife, o.childWork, o.childNS} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	o.childLife = nil
	o.childWork = nil
	o.childNS = nil
	return err
}
func (o *LinuxFixture) spawnOnNamespaceThread(ctx context.Context, n *OwnedNetNS) (resultErr error) {
	defer o.resolvedOnce.Do(func() { close(o.startResolved) })
	defer func() { resultErr = errors.Join(resultErr, o.closeBootstrap()) }()
	o.mu.Lock()
	stopping := o.stopping
	o.mu.Unlock()
	if stopping || ctx.Err() != nil {
		o.resolveNeverStarted()
		return ErrLinuxOwnerPoisoned
	}
	command, err := o.newLinuxHelperCommand(n)
	if err != nil {
		o.resolveNeverStarted()
		return err
	}
	return o.startLinuxHelper(command, n)
}
func (o *LinuxFixture) newLinuxHelperCommand(n *OwnedNetNS) (*exec.Cmd, error) {
	fd, err := syscall.Dup(n.fd)
	if err != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	syscall.CloseOnExec(fd)
	o.childNS = os.NewFile(uintptr(fd), "owned-netns-verification")
	var environment []string
	if o.spec.planned == nil {
		environment, err = o.spec.Config.environment(o.spec.Credentials)
	} else {
		environment, err = plannedChildEnvironment(o.spec.Config, o.spec.planned.plan, o.spec.Credentials)
	}
	if err != nil {
		return nil, err
	}
	command := exec.Command(o.spec.HelperBinary)
	command.Env = environment
	command.Dir = "/"
	command.ExtraFiles = []*os.File{o.childLife, o.childWork, o.childNS, o.parent.file}
	if p := o.spec.planned; p != nil && p.membership != nil {
		if p.membership.file == nil {
			return nil, ErrMemberState
		}
		command.ExtraFiles = append(command.ExtraFiles, p.membership.file)
	}
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(o.spec.UID), Gid: o.spec.GID, Groups: []uint32{}}, Pdeathsig: syscall.SIGKILL}
	return command, nil
}
func (o *LinuxFixture) startLinuxHelper(command *exec.Cmd, n *OwnedNetNS) error {
	if err := command.Start(); err != nil {
		o.resolveNeverStarted()
		return ErrLinuxOwnerUnsupported
	}
	o.mu.Lock()
	o.command = command
	o.helperState = LinuxHelperRunning
	o.mu.Unlock()
	n.mu.Lock()
	n.helperReaped = o.helperWaitDone
	n.mu.Unlock()
	// Actual direct-child Wait has one owner; no status packet can substitute it.
	go func() {
		waitErr := command.Wait()
		result := captureOwnedExit(command.ProcessState, waitErr != nil)
		o.mu.Lock()
		o.helperWait = result
		o.helperWaitKnown = true
		o.helperState = LinuxHelperReaped
		if waitErr != nil {
			o.correlation.poisoned = true
		}
		o.mu.Unlock()
		o.waitOnce.Do(func() { close(o.helperWaitDone) })
	}()
	return nil
}

func (o *LinuxFixture) Snapshot() LinuxFixtureResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	return LinuxFixtureResult{HelperState: o.helperState, TargetKnown: o.targetKnown, TargetExit: o.targetExit, HelperJoined: o.helperJoined, HelperWaitKnown: o.helperWaitKnown, HelperWait: o.helperWait, StopDelivery: o.correlation.stopDelivery, StopAcknowledged: o.correlation.stopObserved, Poisoned: o.correlation.poisoned, Finality: FinalUnavailable}
}
func (o *LinuxFixture) receiveLifecycle() {
	defer close(o.lifeDone)
	<-o.initialized
	for {
		packet, err := o.life.read(time.Time{})
		if err != nil {
			o.mu.Lock()
			joined := o.helperJoined || o.beforeTargetRefused
			o.mu.Unlock()
			if !joined {
				o.poison()
			}
			return
		}
		terminal, err := decodeLinuxBody[linuxTerminal](packet)
		if err == nil && terminal.MemberFinal != nil && (o.spec.planned == nil || packet.kind != linuxHelperJoined) {
			err = ErrLinuxOwnerProtocol
		}
		o.mu.Lock()
		if err == nil {
			err = o.correlation.acceptTerminal(packet, terminal)
		}
		if err == nil && packet.kind == linuxDefect {
			if terminal.Code != linuxResultUnavailable {
				err = ErrLinuxOwnerProtocol
			}
			o.correlation.poisoned = true
		}
		if err == nil && packet.kind != linuxDefect {
			if (terminal.Code != linuxResultReady && terminal.Code != linuxResultUnavailable) || (terminal.Exit.Kind != WaitNormal && terminal.Exit.Kind != WaitSignaled) {
				err = ErrLinuxOwnerProtocol
			} else if o.targetKnown && o.targetExit != terminal.Exit {
				err = ErrLinuxOwnerProtocol
			} else {
				o.targetKnown = true
				o.targetExit = terminal.Exit
				if terminal.Code == linuxResultUnavailable {
					o.correlation.poisoned = true
				}
				if packet.kind == linuxHelperJoined {
					if terminal.MemberFinal != nil {
						copy := *terminal.MemberFinal
						o.memberFinal = &copy
					}
					o.helperJoined = true
				}
			}
		}
		poisoned := o.correlation.poisoned
		o.mu.Unlock()
		if poisoned {
			o.poison() // submit independent Stop while retaining lifecycle reception
		}
		if err != nil {
			o.poison()
			return
		}
	}
}
func (o *LinuxFixture) runWork() {
	defer close(o.workDone)
	for {
		select {
		case <-o.stoppingDone:
			return
		case exchange := <-o.exchanges:
			if exchange.initialize {
				_ = o.initializeOnWorkOwner(exchange.ctx)
				continue // initialized wakes Start; no INIT payload sender.
			}
			if exchange.receiptLimit > 0 {
				o.receiptsOnWorkOwner(exchange)
				continue
			}
			if exchange.bindMember {
				o.bindMemberOnWorkOwner(exchange)
				continue
			}
			if exchange.observe {
				o.observeOnWorkOwner(exchange)
				o.mu.Lock()
				o.workBusy = false
				o.mu.Unlock()
				continue
			}
			deadline, _ := exchange.ctx.Deadline()
			budget := time.Until(deadline)
			packet, err := encodeLinuxPacket(linuxSample, exchange.id, 1, linuxSampleRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(budget)})
			if err == nil {
				err = o.work.write(packet, deadline)
			}
			var reply linuxPacket
			var body linuxSampleReply
			if err == nil {
				reply, err = o.work.read(deadline)
			}
			if err == nil {
				body, err = decodeLinuxBody[linuxSampleReply](reply)
			}
			if err == nil && (reply.kind != linuxSampleResult || reply.id != exchange.id || reply.ordinal != 1 || body.Nonce != o.correlation.nonce || body.Code != linuxResultReady || body.Sample.Provenance != o.readyProvenance) {
				err = ErrLinuxOwnerProtocol
			}
			if err != nil {
				o.poison()
			}
			o.mu.Lock()
			o.workBusy = false
			o.mu.Unlock()
			select {
			case exchange.result <- linuxSampleOutcome{body.Sample, err}:
			case <-exchange.ctx.Done():
				o.poison()
			case <-o.stoppingDone:
			}
		}
	}
}
func (o *LinuxFixture) Sample(ctx context.Context) (LinuxProcessSample, error) {
	if _, ok := ctx.Deadline(); !ok {
		return LinuxProcessSample{}, ErrLinuxOwnerBound
	}
	if ctx.Err() != nil {
		return LinuxProcessSample{}, ctx.Err()
	}
	o.mu.Lock()
	if !o.running || o.stopping || o.correlation.poisoned {
		o.mu.Unlock()
		return LinuxProcessSample{}, ErrLinuxOwnerPoisoned
	}
	if o.workBusy {
		o.mu.Unlock()
		return LinuxProcessSample{}, ErrLinuxOwnerBusy
	}
	id, err := o.correlation.allocate()
	if err != nil {
		o.mu.Unlock()
		o.poison()
		return LinuxProcessSample{}, err
	}
	o.workBusy = true
	o.mu.Unlock()
	exchange := linuxSampleExchange{ctx: ctx, id: id, result: make(chan linuxSampleOutcome)}
	select {
	case o.exchanges <- exchange:
	case <-ctx.Done():
		o.mu.Lock()
		o.workBusy = false
		o.mu.Unlock()
		return LinuxProcessSample{}, ctx.Err()
	case <-o.stoppingDone:
		return LinuxProcessSample{}, ErrLinuxOwnerPoisoned
	}
	select {
	case result := <-exchange.result:
		return result.sample, result.err
	case <-ctx.Done():
		o.poison()
		return LinuxProcessSample{}, ctx.Err()
	case <-o.stoppingDone:
		return LinuxProcessSample{}, ErrLinuxOwnerPoisoned
	}
}
func (o *LinuxFixture) poison() {
	o.mu.Lock()
	o.correlation.poisoned = true
	o.mu.Unlock()
	_ = o.requestStop(time.Second) // retained in the fixed owner's defect state
}
func (o *LinuxFixture) requestStop(budget time.Duration) error {
	o.stopSubmitMu.Lock()
	defer o.stopSubmitMu.Unlock()
	o.mu.Lock()
	if o.stopping {
		o.mu.Unlock()
		return nil
	}
	wasRunning := o.running
	alreadyJoined := o.helperJoined || o.beforeTargetRefused
	neverStarted := !o.startCalled
	if neverStarted {
		o.startCalled = true
	}
	o.stopping = true
	close(o.stoppingDone)
	var id LinuxRequestID
	var err error
	if !alreadyJoined {
		id, err = o.correlation.allocate()
	}
	if err != nil { // poisoned allocator still needs one uniquely reserved Stop intent
		if o.correlation.lastID != ^LinuxRequestID(0) {
			o.correlation.lastID++
			id = o.correlation.lastID
		} else {
			id = 0
		}
	}
	o.correlation.stopID = id
	o.mu.Unlock()
	if neverStarted {
		o.resolveNeverStarted()
		_ = o.childLife.Close()
		_ = o.childWork.Close()
		o.childLife = nil
		o.childWork = nil
		o.initializationOnce.Do(func() { close(o.initialized) })
	}
	closeErr := o.work.close() // poller-managed Close independently evicts work
	o.rpc.Close()
	packet, encodeErr := encodeLinuxPacket(linuxStop, id, 1, linuxStopRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(budget)})
	delivery := LinuxStopUnknown
	var sendErr error
	if alreadyJoined {
		delivery = LinuxStopNotAttempted
	} else if !wasRunning {
		delivery = LinuxStopNoPacket
		sendErr = o.life.conn.CloseWrite()
	} else if encodeErr == nil && id != 0 {
		delivery, sendErr = o.life.sendStop(packet)
	} else {
		sendErr = ErrLinuxOwnerProtocol
		_ = o.life.conn.CloseWrite()
	}
	o.mu.Lock()
	o.correlation.stopDelivery = delivery
	if sendErr != nil || err != nil || closeErr != nil {
		o.correlation.poisoned = true
	}
	o.mu.Unlock()
	return errors.Join(sendErr, closeErr)
}
func (o *LinuxFixture) Stop(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return ErrLinuxOwnerBound
	}
	if err := o.requestStop(max(time.Until(deadline), time.Nanosecond)); err != nil {
		return err
	}
	for _, done := range []<-chan struct{}{o.startResolved, o.helperWaitDone, o.workDone, o.lifeDone} {
		select {
		case <-done:
		case <-ctx.Done():
			return errors.Join(ErrStopUnconfirmed, ctx.Err())
		}
	}
	state := o.Snapshot()
	o.mu.Lock()
	refused := o.beforeTargetRefused
	o.mu.Unlock()
	if (!state.HelperWaitKnown && state.HelperState != LinuxHelperNotStarted) || (!state.HelperJoined && !refused) {
		return ErrStopUnconfirmed
	}
	if state.Poisoned {
		return ErrLinuxOwnerPoisoned
	}
	return nil
}
func (o *LinuxFixture) Close(ctx context.Context) error {
	stopErr := o.Stop(ctx)
	if errors.Is(stopErr, ErrStopUnconfirmed) || ctx.Err() != nil {
		return errors.Join(stopErr, ctx.Err())
	}
	state := o.Snapshot()
	o.mu.Lock()
	refused := o.beforeTargetRefused
	o.mu.Unlock()
	if (!state.HelperWaitKnown && state.HelperState != LinuxHelperNotStarted) || (!state.HelperJoined && !refused) {
		return ErrStopUnconfirmed
	}
	select {
	case <-o.releaseGate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { o.releaseGate <- struct{}{} }()
	if o.spec.planned == nil {
		if err := o.namespace.Close(ctx); err != nil {
			return errors.Join(stopErr, err)
		}
	} else if err := o.confirmPlannedNamespaceIdle(); err != nil {
		return errors.Join(stopErr, err)
	}
	closeErr := errors.Join(o.life.close(), o.work.close())
	if o.parent.file != nil {
		closeErr = errors.Join(closeErr, o.parent.file.Close())
		o.parent.file = nil
	}
	if o.spec.planned != nil && closeErr == nil {
		o.mu.Lock()
		o.plannedReleased = true
		o.mu.Unlock()
	}
	return errors.Join(stopErr, closeErr)
}
