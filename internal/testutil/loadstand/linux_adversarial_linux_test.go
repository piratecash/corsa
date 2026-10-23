//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"golang.org/x/sys/unix"
)

type linuxAdversarialCase string

const (
	adversarialPdeath  linuxAdversarialCase = "pdeath"
	adversarialFDs     linuxAdversarialCase = "fds"
	adversarialBlocked linuxAdversarialCase = "blocked-sample"
)

type linuxAdversarialCommand string

const (
	adversarialHello     linuxAdversarialCommand = "hello"
	adversarialKill      linuxAdversarialCommand = "self-kill"
	adversarialObserve   linuxAdversarialCommand = "observe-blocked"
	adversarialRelease   linuxAdversarialCommand = "release"
	adversarialCompleted linuxAdversarialCommand = "completed"
	adversarialFinish    linuxAdversarialCommand = "finish"
)

type linuxAdversarialCode string

const (
	adversarialOK        linuxAdversarialCode = "ok"
	adversarialInherited linuxAdversarialCode = "inherited-refused"
	adversarialFailed    linuxAdversarialCode = "failed"
)

type linuxAdversarialFrame struct {
	Version   uint8
	ID        uint64
	Kind      linuxAdversarialCommand
	Mode      linuxAdversarialCase
	Nonce     LaunchNonce
	Challenge LaunchNonce
	Code      linuxAdversarialCode
	WaitKnown bool
}
type linuxAdversarialControl struct {
	conn     *net.UnixConn
	object   linuxObjectIdentity
	once     sync.Once
	closeErr error
}

func (c *linuxAdversarialControl) close() error {
	c.once.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}

// ExtraFiles clears CLOEXEC. Establish it on the inherited object BEFORE conversion.
func linuxAdversarialControlFile(original *os.File, parentPID int) (_ *linuxAdversarialControl, resultErr error) {
	if original == nil {
		return nil, ErrLinuxOwnerProtocol
	}
	flags, err := unix.FcntlInt(original.Fd(), unix.F_GETFD, 0)
	if err == nil {
		_, err = unix.FcntlInt(original.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC)
	}
	verify, verifyErr := unix.FcntlInt(original.Fd(), unix.F_GETFD, 0)
	if err != nil || verifyErr != nil || verify&unix.FD_CLOEXEC == 0 {
		return nil, errors.Join(ErrLinuxOwnerProtocol, original.Close())
	}
	identity, err := linuxObject(int(original.Fd()))
	if err != nil {
		return nil, errors.Join(err, original.Close())
	}
	converted, convertErr := net.FileConn(original)
	aliasErr := original.Close()
	if convertErr != nil || aliasErr != nil {
		if converted != nil {
			aliasErr = errors.Join(aliasErr, converted.Close())
		}
		return nil, errors.Join(ErrLinuxOwnerProtocol, convertErr, aliasErr)
	}
	stream, ok := converted.(*net.UnixConn)
	if !ok || stream.LocalAddr().Network() != "unix" {
		return nil, errors.Join(ErrLinuxOwnerProtocol, converted.Close())
	}
	valid := false
	raw, err := stream.SyscallConn()
	if err == nil {
		err = raw.Control(func(fd uintptr) {
			kind, e1 := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
			status, e2 := unix.FcntlInt(fd, unix.F_GETFL, 0)
			descriptor, e3 := unix.FcntlInt(fd, unix.F_GETFD, 0)
			peer, e4 := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
			_, e5 := unix.Getpeername(int(fd))
			actual, e6 := linuxObject(int(fd))
			valid = errors.Join(e1, e2, e3, e4, e5, e6) == nil && kind == unix.SOCK_STREAM && status&unix.O_NONBLOCK != 0 && descriptor&unix.FD_CLOEXEC != 0 && actual == identity && peer.Uid == 0 && int(peer.Pid) == parentPID
		})
	}
	if err != nil || !valid {
		return nil, errors.Join(ErrLinuxOwnerProtocol, stream.Close())
	}
	return &linuxAdversarialControl{conn: stream, object: identity}, nil
}
func linuxAdversarialPair() (*linuxAdversarialControl, *os.File, error) {
	ends, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	original := os.NewFile(uintptr(ends[0]), "private-test-control-parent")
	child := os.NewFile(uintptr(ends[1]), "private-test-control-child")
	owner, err := linuxAdversarialControlFile(original, os.Getpid())
	if err != nil {
		return nil, nil, errors.Join(err, child.Close())
	}
	return owner, child, nil
}
func linuxAdversarialDeadline(ctx context.Context, ceiling time.Duration) (time.Time, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if bound := time.Now().Add(ceiling); bound.Before(deadline) {
		deadline = bound
	}
	return deadline, nil
}
func (c *linuxAdversarialControl) read(ctx context.Context) (linuxAdversarialFrame, error) {
	deadline, err := linuxAdversarialDeadline(ctx, 5*time.Second)
	if err != nil {
		return linuxAdversarialFrame{}, err
	}
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return linuxAdversarialFrame{}, err
	}
	var buffer [linuxPacketLimit]byte
	readExact := func(destination []byte) error {
		for len(destination) > 0 {
			var ancillary [256]byte
			n, oobn, flags, _, err := c.conn.ReadMsgUnix(destination, ancillary[:])
			disposeErr := disposeLinuxAncillary(ancillary[:oobn])
			if disposeErr != nil || oobn != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
				return ErrLinuxOwnerProtocol
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrUnexpectedEOF
			}
			destination = destination[n:]
		}
		return ctx.Err()
	}
	if err := readExact(buffer[:4]); err != nil {
		return linuxAdversarialFrame{}, err
	}
	size := binary.BigEndian.Uint32(buffer[:4])
	if size == 0 || size > linuxPacketLimit-4 {
		return linuxAdversarialFrame{}, ErrLinuxOwnerBound
	}
	payload := buffer[4 : 4+int(size)]
	if err := readExact(payload); err != nil {
		return linuxAdversarialFrame{}, err
	}
	var frame linuxAdversarialFrame
	if !canonicalLinuxJSON(payload, reflect.TypeFor[linuxAdversarialFrame](), false) || json.Unmarshal(payload, &frame) != nil {
		return frame, ErrLinuxOwnerProtocol
	}
	return frame, nil
}
func (c *linuxAdversarialControl) write(ctx context.Context, frame linuxAdversarialFrame) error {
	deadline, err := linuxAdversarialDeadline(ctx, time.Second)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if len(payload) > linuxPacketLimit-4 {
		return ErrLinuxOwnerBound
	}
	var buffer [linuxPacketLimit]byte
	binary.BigEndian.PutUint32(buffer[:4], uint32(len(payload)))
	copy(buffer[4:], payload)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	n, err := c.conn.Write(buffer[:4+len(payload)])
	if err != nil {
		return err
	}
	if n != 4+len(payload) {
		return io.ErrShortWrite
	}
	return ctx.Err()
}

type linuxAdversarialClient struct {
	owner    *linuxAdversarialControl
	frame    linuxAdversarialFrame
	poisoned bool
}

func (c *linuxAdversarialClient) call(ctx context.Context, command linuxAdversarialCommand) (linuxAdversarialFrame, error) {
	if c.poisoned || c.frame.ID == ^uint64(0) {
		return linuxAdversarialFrame{}, ErrLinuxOwnerProtocol
	}
	c.frame.ID++
	c.frame.Kind = command
	if err := c.owner.write(ctx, c.frame); err != nil {
		c.poisoned = true
		return linuxAdversarialFrame{}, err
	}
	reply, err := c.owner.read(ctx)
	validResult := reply.Code == adversarialOK && !reply.WaitKnown
	if command == adversarialCompleted {
		validResult = reply.Code == adversarialOK || reply.Code == adversarialInherited || reply.Code == adversarialFailed
	}
	if !validResult || err != nil || reply.Version != c.frame.Version || reply.ID != c.frame.ID || reply.Kind != command || reply.Mode != c.frame.Mode || reply.Nonce != c.frame.Nonce || reply.Challenge != c.frame.Challenge {
		c.poisoned = true
		return linuxAdversarialFrame{}, errors.Join(ErrLinuxOwnerProtocol, err)
	}
	return reply, nil
}
func linuxAdversarialRequireNative(t *testing.T) {
	t.Helper()
	if os.Getenv("LOADSTAND_LINUX_NATIVE") != "1" {
		t.Skip("requires separately dispatched owned-container native control")
	}
	if os.Geteuid() != 0 {
		t.Fatal("native parent is not root")
	}
}
func linuxAdversarialFixture(t *testing.T, helperTest bool) (context.Context, *AuditedProfileParent, *OwnedNetNS, LinuxFixtureSpec) {
	t.Helper()
	linuxAdversarialRequireNative(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	var nonce LaunchNonce
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	parent, err := CreateAuditedProfileParent(ctx, "/profiles", nonce)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { linuxTestClose(t, parent) })
	namespace, err := CreateOwnedNetNS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		defer stop()
		if err := namespace.Close(cleanup); err != nil {
			t.Error("namespace join unconfirmed", err)
		}
	})
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	config := ChildConfig{DataDir: parent.MemberDirectory(), Listen: domain.ListenAddress("127.0.0.1:42050"), RPC: domain.ListenAddress("127.0.0.1:42051"), Bootstrap: []domain.PeerAddress{"127.0.0.1:1"}, Listener: false, Procs: 1}
	spec := LinuxFixtureSpec{HelperBinary: os.Getenv("LOADSTAND_NATIVE_HELPER"), HelperSHA256: ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_HELPER_SHA")), TargetBinary: os.Getenv("LOADSTAND_NATIVE_TARGET"), TargetSHA256: ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_TARGET_SHA")), Config: config, Credentials: credentials, UID: 65532, GID: 65532}
	if helperTest {
		raw, err := ReadPublicArtifact(ctx, "/inputs/sha256.expected", 1024)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
		if len(lines) != 3 {
			t.Fatal("input hash manifest shape")
		}
		for i, name := range []string{"loadstand.test", "linuxhelper", "sdkedge"} {
			if len(lines[i]) != 64+2+len(name) || lines[i][64:] != "  "+name {
				t.Fatal("input hash manifest names")
			}
		}
		spec.HelperBinary = "/profiles/fixtures/loadstand.test"
		spec.HelperSHA256 = ExecutableSHA256(lines[0][:64])
	}
	return ctx, parent, namespace, spec
}

// No normal readers are installed. Actual shared command/start owns the sole Wait.
func linuxAdversarialStart(t *testing.T, mode linuxAdversarialCase) (context.Context, *LinuxFixture, *linuxAdversarialClient, <-chan error) {
	t.Helper()
	ctx, parent, namespace, spec := linuxAdversarialFixture(t, true)
	owner, err := newLinuxFixtureResources(ctx, spec, parent, namespace)
	if err != nil {
		t.Fatal(err)
	}
	control, child, err := linuxAdversarialPair()
	if err != nil {
		t.Fatal(err)
	}
	driverDone := make(chan error, 1)
	releaseDriver := make(chan struct{})
	started := make(chan error, 1)
	t.Cleanup(func() {
		// Registered after namespace cleanup: this runs FIRST, only after original assertion.
		if err := control.close(); err != nil {
			t.Error(err)
		}
		if err := errors.Join(owner.life.close(), owner.work.close()); err != nil {
			t.Error(err)
		}
		owner.rpc.Close()
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		defer stop()
		select {
		case <-owner.helperWaitDone:
		case <-cleanup.Done():
			t.Error("actual helper Wait unknown")
			return
		}
		close(releaseDriver)
		select {
		case err := <-driverDone:
			if err != nil {
				t.Error(err)
			}
		case <-cleanup.Done():
			t.Error("creator driver still owned")
			return
		}
		if err := owner.parent.file.Close(); err != nil {
			t.Error(err)
		}
	})
	go func() {
		runtime.LockOSThread()
		restored, original := false, -1
		var driverErr error
		defer func() {
			if original >= 0 {
				driverErr = errors.Join(driverErr, unix.Close(original))
			}
			if restored {
				runtime.UnlockOSThread()
			}
			driverDone <- driverErr // final owner action, after every cleanup
		}()
		var origin linuxNamespaceIdentity
		original, origin, driverErr = openThreadNetNS()
		if driverErr != nil {
			started <- driverErr
			return
		}
		if unix.Setns(namespace.fd, unix.CLONE_NEWNET) != nil {
			driverErr = ErrLinuxOwnerUnsupported
			started <- driverErr
			return
		}
		command, err := owner.newLinuxHelperCommand(namespace)
		if err == nil {
			command.Args = append(command.Args, "-test.run=^TestOwnedLinuxAdversarialChild$", "-test.timeout=25s")
			command.ExtraFiles = append(command.ExtraFiles, child)
			err = owner.startLinuxHelper(command, namespace)
		}
		aliasErr := errors.Join(owner.closeBootstrap(), child.Close())
		restoreErr := unix.Setns(original, unix.CLONE_NEWNET)
		if restoreErr == nil {
			fd, current, e := openThreadNetNS()
			if e == nil {
				e = unix.Close(fd)
			}
			restored = e == nil && current == origin
			if !restored {
				restoreErr = ErrLinuxOwnerPoisoned
			}
		}
		driverErr = errors.Join(err, aliasErr, restoreErr)
		started <- driverErr
		if command != nil && command.Process != nil {
			<-owner.helperWaitDone
		}
		<-releaseDriver
	}()

	select {
	case err := <-started:
		if err != nil {
			t.Fatal("actual shared start failed", err)
		}
	case <-ctx.Done():
		t.Fatal("shared start unknown")
	}
	var challenge LaunchNonce
	if _, err := rand.Read(challenge[:]); err != nil {
		t.Fatal(err)
	}
	client := &linuxAdversarialClient{owner: control, frame: linuxAdversarialFrame{Version: 1, Mode: mode, Nonce: owner.correlation.nonce, Challenge: challenge, Code: adversarialOK}}
	if reply, err := client.call(ctx, adversarialHello); err != nil || reply.Code != adversarialOK {
		t.Fatal("private child admission refused", err)
	}

	return ctx, owner, client, driverDone
}

type linuxAdversarialBarrier struct {
	mu      sync.Mutex
	sampler *LinuxProcessSampler
	held    bool
}

func (b *linuxAdversarialBarrier) hold(sampler *LinuxProcessSampler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sampler.mu.Lock()
	b.sampler = sampler
	b.held = true
}
func (b *linuxAdversarialBarrier) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.held {
		b.held = false
		b.sampler.mu.Unlock()
	}
}
func linuxAdversarialBlockedStack(ctx context.Context) error {
	return linuxAdversarialStack(ctx, []string{".(*linuxHelperRuntime).sampleWork(", ".(*LinuxProcessSampler).Sample(", "sync.Mutex.Lock"})
}
func linuxAdversarialStack(ctx context.Context, required []string) error {
	if runtime.Version() != "go1.27.3" {
		return ErrProcessObservation
	}
	var stack [processFileLimit]byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := runtime.Stack(stack[:], true)
		if n == len(stack) {
			return ErrProcessObservation
		}
		matching := 0
		for _, goroutine := range bytes.Split(stack[:n], []byte("\n\n")) {
			matches := true
			for _, frame := range required {
				if !bytes.Contains(goroutine, []byte(frame)) {
					matches = false
					break
				}
			}
			if matches {
				matching++
			}
		}
		if matching == 1 {
			return nil
		}
		if matching > 1 {
			return ErrProcessObservation
		}
		runtime.Gosched()
	}
}

// This entrypoint is excluded from all normal native selectors. Arguments refuse;
// actual inherited sockets, credentials, nonce and challenge admit the child.
func TestOwnedLinuxAdversarialChild(t *testing.T) {
	selected := false
	for _, arg := range os.Args {
		if arg == "-test.run=^TestOwnedLinuxAdversarialChild$" {
			selected = true
		}
	}
	if !selected || os.Geteuid() == 0 || os.Getuid() != os.Geteuid() || os.Getgid() != os.Getegid() {
		t.Skip("not a sealed capless reexec")
	}
	groups, err := unix.Getgroups()
	if err != nil || len(groups) != 0 {
		t.Fatal("unexpected groups")
	}
	for _, fd := range []int{3, 4} {
		kind, e1 := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
		peer, e2 := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
		if e1 != nil || e2 != nil || kind != unix.SOCK_SEQPACKET || peer.Uid != 0 || int(peer.Pid) != os.Getppid() {
			t.Skip("no private bootstrap")
		}
	}
	initialFlags, flagErr := unix.FcntlInt(7, unix.F_GETFD, 0)
	if flagErr != nil || initialFlags&unix.FD_CLOEXEC != 0 {
		t.Fatal("ExtraFiles CLOEXEC transition was not exercised")
	}
	control, err := linuxAdversarialControlFile(os.NewFile(7, "private-child-test-control"), os.Getppid())
	if err != nil {
		t.Fatal(err)
	}
	if unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0) != nil {
		t.Fatal("nondumpable refused")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	hello, err := control.read(ctx)
	if err != nil || hello.Version != 1 || hello.ID != 1 || hello.Kind != adversarialHello || hello.Code != adversarialOK || hello.WaitKnown || hello.Nonce == (LaunchNonce{}) || hello.Challenge == (LaunchNonce{}) || (hello.Mode != adversarialPdeath && hello.Mode != adversarialFDs && hello.Mode != adversarialBlocked) {
		_ = control.close()
		t.Fatal("private handshake refused", err)
	}
	prepared, completed, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	sessionReady := make(chan struct{})
	var session *linuxHelperSession
	result := linuxAdversarialFrame{Code: adversarialFailed}
	var barrier linuxAdversarialBarrier
	controlDone := make(chan struct{})
	var controlResult error
	go func() {
		var controlErr error
		defer func() {
			if controlErr != nil {
				cancel()
				barrier.release()
				select {
				case <-sessionReady:
					if session != nil {
						session.runtime.requestStop(0, 5*time.Second, true)
					}
				default:
				}
			}
			controlResult = controlErr
			close(controlDone)
		}()
		controlErr = control.write(ctx, hello)
		if controlErr != nil {
			return
		}
		expectedID := uint64(2)
		observed, released, observedCompletion := false, false, false
		for {
			request, err := control.read(ctx)
			if err != nil {
				controlErr = err
				return
			}
			if request.Version != hello.Version || request.ID != expectedID || request.Nonce != hello.Nonce || request.Challenge != hello.Challenge || request.Mode != hello.Mode || request.Code != adversarialOK || request.WaitKnown {
				controlErr = ErrLinuxOwnerProtocol
				return
			}
			expectedID++
			reply := request
			switch request.Kind {
			case adversarialKill:
				if hello.Mode != adversarialPdeath {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				select {
				case <-prepared:
				case <-ctx.Done():
					controlErr = ctx.Err()
					return
				}
				if session == nil || session.runtime.process == nil {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				if err := control.write(ctx, reply); err != nil {
					controlErr = err
					return
				}
				controlErr = unix.Kill(os.Getpid(), unix.SIGKILL)
				return
			case adversarialObserve:
				if hello.Mode != adversarialBlocked || observed {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				select {
				case <-prepared:
				case <-ctx.Done():
					controlErr = ctx.Err()
					return
				}
				controlErr = linuxAdversarialBlockedStack(ctx)
				if controlErr != nil {
					return
				}
				observed = true
			case adversarialRelease:
				if hello.Mode != adversarialBlocked || !observed || released {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				barrier.release()
				released = true
			case adversarialCompleted:
				if hello.Mode == adversarialPdeath || observedCompletion || (hello.Mode == adversarialBlocked && !released) {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				select {
				case <-completed:
				case <-ctx.Done():
					controlErr = ctx.Err()
					return
				}
				reply.Code, reply.WaitKnown = result.Code, result.WaitKnown
				observedCompletion = true
			case adversarialFinish:
				if !observedCompletion {
					controlErr = ErrLinuxOwnerProtocol
					return
				}
				if err := control.write(ctx, reply); err != nil {
					controlErr = err
					return
				}
				close(finished)
				return
			default:
				controlErr = ErrLinuxOwnerProtocol
				return
			}
			if err := control.write(ctx, reply); err != nil {
				controlErr = err
				return
			}
		}
	}()
	// The main driver owns every shared target stage and retains this OS thread.
	session, err = openLinuxHelperSession(ctx)
	close(sessionReady)
	var mainErr error
	var injected linuxObjectIdentity
	if err != nil || session == nil {
		mainErr = errors.Join(err, ErrLinuxOwnerProtocol)
	} else if session.runtime.init.Nonce != hello.Nonce {
		mainErr = ErrLinuxOwnerProtocol
	} else {
		if hello.Mode == adversarialFDs {
			injected, mainErr = linuxPacketObject(session.runtime.life)
			rawErr := session.runtime.life.raw.Control(func(fd uintptr) {
				flags, e := unix.FcntlInt(fd, unix.F_GETFD, 0)
				if e == nil {
					_, e = unix.FcntlInt(fd, unix.F_SETFD, flags&^unix.FD_CLOEXEC)
				}
				actual, e2 := unix.FcntlInt(fd, unix.F_GETFD, 0)
				if e != nil || e2 != nil || actual&unix.FD_CLOEXEC != 0 {
					mainErr = ErrLinuxOwnerProtocol
				}
			})
			mainErr = errors.Join(mainErr, rawErr)
		}
		if mainErr == nil {
			var ready bool
			ready, mainErr = session.prepare(ctx)
			if mainErr == nil && !ready {
				mainErr = ErrLinuxOwnerProtocol
			}
			if mainErr == nil {
				mainErr = session.runtime.sampler.auditDescriptors(ctx, []linuxObjectIdentity{control.object}, session.runtime.process.artifacts)
			}
			if mainErr == nil && hello.Mode == adversarialBlocked {
				barrier.hold(session.runtime.sampler)
			}
		}
	}
	close(prepared)
	if mainErr == nil {
		mainErr = session.serve(ctx)
	}
	barrier.release()
	var closeErr error
	if session != nil {
		closeErr = session.close(ctx)
	}
	if session != nil && session.runtime.process != nil {
		select {
		case <-session.runtime.process.Done():
			_, e := session.runtime.process.ExitResult()
			result.WaitKnown = e == nil
		default:
		}
	}
	var inherited *linuxInheritedCapabilityError
	if errors.As(mainErr, &inherited) && inherited.object == injected && closeErr == nil && result.WaitKnown {
		result.Code = adversarialInherited
	} else if mainErr == nil && closeErr == nil {
		result.Code = adversarialOK
	}
	close(completed)
	select {
	case <-finished:
	case <-ctx.Done():
		mainErr = errors.Join(mainErr, ctx.Err())
	case <-controlDone:
		mainErr = errors.Join(mainErr, controlResult)
	}
	cancel()
	closeErr = errors.Join(closeErr, control.close())
	<-controlDone
	controlErr := controlResult
	if hello.Mode == adversarialFDs && result.Code == adversarialInherited {
		mainErr = nil
	}
	if errors.Join(mainErr, closeErr, controlErr) != nil {
		t.Fatal("actual child join/observation defect", errors.Join(mainErr, closeErr, controlErr))
	}
}

func TestLinuxAdversarialRestoreRetiresThread(t *testing.T) {
	ctx, parent, namespace, spec := linuxAdversarialFixture(t, false)
	owner, err := CreateLinuxFixture(ctx, spec, parent, namespace)
	if err != nil {
		t.Fatal(err)
	}
	namespace.mu.Lock()
	worker := namespace.worker
	if namespace.busy || namespace.closed || namespace.poisoned || namespace.original < 0 {
		namespace.mu.Unlock()
		t.Fatal("namespace not idle")
	}
	namespace.mu.Unlock()
	if worker.tid == unix.Getpid() || worker.start == 0 {
		t.Fatal("namespace worker must be an actual nonleader incarnation")
	}
	procRoot, processDir, err := openProcessDirectory(ProcessID(unix.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	taskParent, parentErr := unix.Openat(processDir, "task", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	aliasErr := errors.Join(unix.Close(processDir), unix.Close(procRoot))
	if parentErr != nil || aliasErr != nil {
		if taskParent >= 0 {
			if err := unix.Close(taskParent); err != nil {
				t.Error(err)
			}
		}
		t.Fatal("task parent unavailable", errors.Join(parentErr, aliasErr))
	}
	defer func() {
		if err := unix.Close(taskParent); err != nil {
			t.Error(err)
		}
	}()
	taskFD, taskErr := unix.Openat(taskParent, strconv.Itoa(worker.tid), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if taskErr != nil {
		t.Fatal(taskErr)
	}
	defer func() {
		if err := unix.Close(taskFD); err != nil {
			t.Error(err)
		}
	}()
	raw, readErr := readProcFile(ctx, taskFD, "stat")
	captured, parseErr := parseProcessStat(raw)
	if readErr != nil || parseErr != nil || captured.ID.PID != ProcessID(worker.tid) || captured.ID.StartTicks != worker.start {
		t.Fatal("worker task binding unavailable")
	}
	namespace.mu.Lock()
	if namespace.busy || namespace.closed || namespace.poisoned || namespace.original < 0 {
		namespace.mu.Unlock()
		t.Fatal("namespace changed before destructive test")
	}
	original := namespace.original
	namespace.original = -1
	err = unix.Close(original)
	namespace.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	start, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err := owner.Start(start); err == nil {
		t.Fatal("failed restoration admitted Start")
	}
	select {
	case <-namespace.done:
	case <-ctx.Done():
		t.Fatal("failed-restoration worker did not retire")
	}
	namespace.mu.Lock()
	poisoned := namespace.poisoned
	namespace.mu.Unlock()
	if !poisoned || !owner.Snapshot().Poisoned {
		t.Fatal("restore failure did not poison actual owners")
	}
	if err := namespace.submit(ctx, linuxNamespaceOperation{ctx: ctx, owner: owner, result: make(chan error, 1)}); !errors.Is(err, ErrLinuxOwnerPoisoned) {
		t.Fatal("poisoned worker admitted future work", err)
	}
	for {
		if ctx.Err() != nil {
			t.Fatal("original locked thread still exists")
		}
		held := linuxReadTaskContent(ctx, taskFD)
		current := linuxReadCurrentTask(ctx, taskParent, worker)
		if ctx.Err() != nil {
			t.Fatal("original locked thread still exists")
		}
		retired, err := linuxTaskRetired(worker, held, current)
		if err != nil {
			t.Fatal("old task disappearance unknown", err)
		}
		if retired {
			break
		}
		runtime.Gosched()
	}

	select {
	case <-owner.helperWaitDone:
	case <-ctx.Done():
		t.Fatal("actual helper Wait unknown")
	}
	// Deliberate failed-init remains unconfirmed; only release exact managed peers.
	if err := errors.Join(owner.life.close(), owner.work.close(), owner.parent.file.Close()); err != nil {
		t.Error(err)
	}
	owner.rpc.Close()
}
func linuxAdversarialReady(t *testing.T, ctx context.Context, owner *LinuxFixture) linuxReady {
	t.Helper()
	start, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ready, _, err := owner.exchangeLinuxInitialization(start)
	if err != nil {
		t.Fatal("actual shared Init/Ready refused", err)
	}
	return ready
}
func linuxAdversarialSample(t *testing.T, ctx context.Context, owner *LinuxFixture, ready linuxReady) {
	t.Helper()
	packet, err := encodeLinuxPacket(linuxSample, 2, 1, linuxSampleRequest{Nonce: owner.correlation.nonce, BudgetNanos: int64(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	if err := owner.work.write(packet, deadline); err != nil {
		t.Fatal(err)
	}
	reply, err := owner.work.read(deadline)
	if err != nil {
		t.Fatal(err)
	}
	body, err := decodeLinuxBody[linuxSampleReply](reply)
	if err != nil || reply.kind != linuxSampleResult || reply.id != 2 || reply.ordinal != 1 || body.Nonce != owner.correlation.nonce || body.Code != linuxResultReady || body.Sample.Provenance != ready.Provenance {
		t.Fatal("actual pre-kill sample/provenance refused", err)
	}
}
func TestLinuxAdversarialPdeathKillsTarget(t *testing.T) {
	ctx, owner, control, _ := linuxAdversarialStart(t, adversarialPdeath)
	ready := linuxAdversarialReady(t, ctx, owner)
	linuxAdversarialSample(t, ctx, owner, ready)
	pid := int(ready.Provenance.Incarnation.PID)
	root, dir, err := openProcessDirectory(ProcessID(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := errors.Join(unix.Close(dir), unix.Close(root)); err != nil {
			t.Error(err)
		}
	}()
	before, err := readProcFile(ctx, dir, "stat")
	if err != nil {
		t.Fatal(err)
	}
	first, err := parseProcessStat(before)
	if err != nil || first.ID != ready.Provenance.Incarnation {
		t.Fatal("pidfd target incarnation mismatch", err)
	}
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Fatal("pidfd unavailable before destructive action", err)
	}
	defer func() {
		if err := unix.Close(pidfd); err != nil {
			t.Error(err)
		}
	}()
	after, err := readProcFile(ctx, dir, "stat")
	if err != nil {
		t.Fatal(err)
	}
	last, err := parseProcessStat(after)
	if err != nil || last.ID != first.ID {
		t.Fatal("pidfd creation raced incarnation")
	}
	poll := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	n, err := unix.Poll(poll, 0)
	if err != nil || n != 0 {
		t.Fatal("target was already exited before self-kill", err)
	}
	deadlineRemaining, _ := ctx.Deadline()
	if time.Until(deadlineRemaining) <= 5*time.Second {
		t.Fatal("insufficient live lifetime for isolated Pdeath observation")
	}
	if _, err := control.call(ctx, adversarialKill); err != nil {
		t.Fatal("self-kill admission failed", err)
	}
	select {
	case <-owner.helperWaitDone:
	case <-ctx.Done():
		t.Fatal("actual helper Wait unknown")
	}
	result := owner.Snapshot()
	if !result.HelperWaitKnown || result.HelperWait.Kind != WaitSignaled || result.HelperWait.Signal != ProcessSignal(syscall.SIGKILL) {
		t.Fatal("helper actual Wait is not owned SIGKILL", result)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			t.Fatal("target remains alive after actual helper SIGKILL")
		}
		n, err = unix.Poll(poll, 10)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			if poll[0].Revents&unix.POLLIN == 0 {
				t.Fatal("target pidfd exit unknown")
			}
			break
		}
	}
	// The assertion above precedes all registered cleanup and creator retirement.
	if result.TargetKnown || result.HelperJoined || result.Finality != FinalUnavailable {
		t.Fatal("pidfd observation invented target Wait/finality")
	}
}
func TestLinuxAdversarialForbiddenFDRefusesReady(t *testing.T) {
	linuxAdversarialRequireNative(t)
	linuxAdversarialStreamChecks(t)
	ctx, owner, control, _ := linuxAdversarialStart(t, adversarialFDs)
	start, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, _, err := owner.exchangeLinuxInitialization(start); err == nil {
		t.Fatal("leaked bootstrap capability admitted Ready")
	}
	reply, err := control.call(ctx, adversarialCompleted)
	if err != nil || reply.Code != adversarialInherited || !reply.WaitKnown {
		t.Fatal("specific inherited-capability refusal/actual target Wait not proved", err, reply.Code)
	}
	if _, err := control.call(ctx, adversarialFinish); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.helperWaitDone:
	case <-ctx.Done():
		t.Fatal("actual helper Wait not joined")
	}
	if !owner.Snapshot().HelperWait.Clean() {
		t.Fatal("negative fixture itself failed", owner.Snapshot())
	}
}
func TestLinuxAdversarialExitPrecedesBlockedSampleJoin(t *testing.T) {
	ctx, owner, control, _ := linuxAdversarialStart(t, adversarialBlocked)
	linuxAdversarialReady(t, ctx, owner)
	packet, err := encodeLinuxPacket(linuxSample, 2, 1, linuxSampleRequest{Nonce: owner.correlation.nonce, BudgetNanos: int64(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.work.write(packet, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := control.call(ctx, adversarialObserve); err != nil {
		t.Fatal("actual mutex-blocked shared sampler not witnessed", err)
	}
	stop, err := encodeLinuxPacket(linuxStop, 3, 1, linuxStopRequest{Nonce: owner.correlation.nonce, BudgetNanos: int64(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if delivery, err := owner.life.sendStop(stop); err != nil || delivery != LinuxStopSent {
		t.Fatal("ordinary Stop not submitted", err)
	}
	terminalPacket, err := owner.life.read(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := decodeLinuxBody[linuxTerminal](terminalPacket)
	if err != nil || terminalPacket.kind != linuxTargetExitKnown || terminalPacket.id != 3 || terminalPacket.ordinal != 1 || terminal.Nonce != owner.correlation.nonce || (terminal.Exit.Kind != WaitNormal && terminal.Exit.Kind != WaitSignaled) {
		t.Fatal("target exit missing or helper prematurely joined", err)
	}
	select {
	case <-owner.helperWaitDone:
		t.Fatal("helper joined while actual sampler held")
	default:
	}
	if _, err := control.call(ctx, adversarialRelease); err != nil {
		t.Fatal(err)
	}
	joinedPacket, err := owner.life.read(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	joined, err := decodeLinuxBody[linuxTerminal](joinedPacket)
	if err != nil || joinedPacket.kind != linuxHelperJoined || joinedPacket.id != 3 || joinedPacket.ordinal != 1 || joined.Nonce != owner.correlation.nonce || joined.Exit != terminal.Exit {
		t.Fatal("actual work join did not precede HelperJoined", err)
	}
	reply, err := control.call(ctx, adversarialCompleted)
	if err != nil || !reply.WaitKnown {
		t.Fatal("actual target waiter unavailable", err)
	}
	if _, err := control.call(ctx, adversarialFinish); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.helperWaitDone:
	case <-ctx.Done():
		t.Fatal("actual helper Wait remains unavailable")
	}
}

func linuxAdversarialStreamChecks(t *testing.T) {
	for _, scenario := range []string{"valid", "oversize", "alias", "partial", "cancel-close", "rights", "forged-reply"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			left, child, err := linuxAdversarialPair()
			if err != nil {
				t.Fatal(err)
			}
			right, err := linuxAdversarialControlFile(child, os.Getpid())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := errors.Join(left.close(), right.close()); err != nil {
					t.Error(err)
				}
			}()
			frame := linuxAdversarialFrame{Version: 1, ID: 1, Kind: adversarialHello, Mode: adversarialFDs, Nonce: LaunchNonce{1}, Challenge: LaunchNonce{2}, Code: adversarialOK}
			if scenario == "forged-reply" {
				done := make(chan error, 1)
				go func() {
					request, err := right.read(ctx)
					if err == nil {
						request.Code = "forged"
						err = right.write(ctx, request)
					}
					done <- err
				}()
				client := &linuxAdversarialClient{owner: left, frame: frame}
				client.frame.ID = 0
				if reply, err := client.call(ctx, adversarialHello); !errors.Is(err, ErrLinuxOwnerProtocol) || !client.poisoned || reply != (linuxAdversarialFrame{}) {
					t.Fatal("forged reply retained control authority", err)
				}
				if _, err := client.call(ctx, adversarialHello); !errors.Is(err, ErrLinuxOwnerProtocol) {
					t.Fatal("poisoned control resumed", err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("peer control writer did not join")
				}
				return
			}
			if scenario == "valid" {
				if err := left.write(ctx, frame); err != nil {
					t.Fatal(err)
				}
				got, err := right.read(ctx)
				if err != nil || got != frame {
					t.Fatal("bounded typed stream changed frame", err)
				}
				return
			}
			if scenario == "cancel-close" {
				done := make(chan error, 1)
				go func() { _, err := right.read(ctx); done <- err }()
				if err := linuxAdversarialStack(ctx, []string{"[IO wait]", ".(*linuxAdversarialControl).read(", "net.(*UnixConn).ReadMsgUnix("}); err != nil {
					t.Fatal("managed read was not actually parked", err)
				}
				cancel()
				if err := right.close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("cancel/Close invented a frame")
					}
				case <-time.After(time.Second):
					t.Fatal("managed control read did not actually join")
				}
				return
			}
			var data []byte
			switch scenario {
			case "oversize":
				data = make([]byte, 4)
				binary.BigEndian.PutUint32(data, linuxPacketLimit)
			case "alias":
				body, err := json.Marshal(frame)
				if err != nil {
					t.Fatal(err)
				}
				body = append(body[:len(body)-1], []byte(`,"id":null}`)...)
				data = make([]byte, 4+len(body))
				binary.BigEndian.PutUint32(data, uint32(len(body)))
				copy(data[4:], body)
			case "partial":
				data = []byte{0, 0}
				short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				ctx = short
			case "rights":
				body, err := json.Marshal(frame)
				if err != nil {
					t.Fatal(err)
				}
				data = make([]byte, 4+len(body))
				binary.BigEndian.PutUint32(data, uint32(len(body)))
				copy(data[4:], body)
			}
			if scenario == "rights" {
				file, err := os.Open("/dev/null")
				if err != nil {
					t.Fatal(err)
				}
				defer linuxTestClose(t, file)
				descriptorsBefore, err := os.ReadDir("/proc/self/fd")
				if err != nil {
					t.Fatal(err)
				}
				raw, err := left.conn.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var sendErr error
				err = raw.Control(func(fd uintptr) {
					_, sendErr = unix.SendmsgN(int(fd), data, unix.UnixRights(int(file.Fd())), nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
				})
				if errors.Join(err, sendErr) != nil {
					t.Fatal(err, sendErr)
				}
				if _, err := right.read(ctx); !errors.Is(err, ErrLinuxOwnerProtocol) {
					t.Fatal("control stream accepted SCM rights", err)
				}
				descriptorsAfter, err := os.ReadDir("/proc/self/fd")
				if err != nil || len(descriptorsAfter) != len(descriptorsBefore) {
					t.Fatal("received rights leaked or FD observation unknown", err)
				}
				return
			}
			deadline, _ := ctx.Deadline()
			if err := left.conn.SetWriteDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			if _, err := left.conn.Write(data); err != nil {
				t.Fatal(err)
			}
			if _, err := right.read(ctx); err == nil {
				t.Fatal("invalid stream frame accepted")
			}
		})
	}
}

// Fresh CONTENT is required: cached proc metadata is not a live-task witness.
type linuxTaskPresence uint8

const (
	linuxTaskUnknown linuxTaskPresence = iota
	linuxTaskAbsent
	linuxTaskPresent
)

type linuxTaskObservation struct {
	presence linuxTaskPresence
	identity ProcessIncarnation
	state    byte
	err      error
}

func linuxTaskAbsentError(err error) bool {
	return errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH)
}
func linuxParseTaskContent(raw []byte) linuxTaskObservation {
	open, close := bytes.IndexByte(raw, '('), bytes.LastIndexByte(raw, ')')
	if len(raw) > 4096 || open < 1 || close <= open || close+1 >= len(raw) || raw[close+1] != ' ' {
		return linuxTaskObservation{err: ErrProcessObservation}
	}
	fields := strings.Fields(string(raw[close+1:]))
	if len(fields) < 20 || len(fields[0]) != 1 || !strings.Contains("RSDZTXtIWPK", fields[0]) {
		return linuxTaskObservation{err: ErrProcessObservation}
	}
	pid, e1 := strconv.ParseUint(strings.TrimSpace(string(raw[:open])), 10, 31)
	start, e2 := strconv.ParseUint(fields[19], 10, 64)
	if e1 != nil || e2 != nil || pid == 0 || start == 0 {
		return linuxTaskObservation{err: ErrProcessObservation}
	}
	return linuxTaskObservation{presence: linuxTaskPresent, identity: ProcessIncarnation{PID: ProcessID(pid), StartTicks: ProcessTicks(start)}, state: fields[0][0]}
}
func linuxReadTaskContent(ctx context.Context, dir int) (result linuxTaskObservation) {
	if err := ctx.Err(); err != nil {
		return linuxTaskObservation{err: err}
	}
	fd, err := unix.Openat(dir, "stat", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if linuxTaskAbsentError(err) {
			return linuxTaskObservation{presence: linuxTaskAbsent}
		}
		return linuxTaskObservation{err: err}
	}
	defer func() {
		if err := unix.Close(fd); err != nil {
			result = linuxTaskObservation{err: errors.Join(result.err, err)}
		}
	}()
	var fs unix.Statfs_t
	var st unix.Stat_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return linuxTaskObservation{err: err}
	}
	if err := unix.Fstat(fd, &st); err != nil {
		return linuxTaskObservation{err: err}
	}
	if fs.Type != unix.PROC_SUPER_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return linuxTaskObservation{err: ErrProcessObservation}
	}
	var storage [4097]byte
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return linuxTaskObservation{err: err}
		}
		n, err := unix.Read(fd, storage[total:])
		if err != nil {
			if linuxTaskAbsentError(err) {
				return linuxTaskObservation{presence: linuxTaskAbsent}
			}
			return linuxTaskObservation{err: err}
		}
		total += n
		if total > 4096 {
			return linuxTaskObservation{err: ErrProcessObservation}
		}
		if n == 0 {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return linuxTaskObservation{err: err}
	}
	return linuxParseTaskContent(storage[:total])
}
func linuxReadCurrentTask(ctx context.Context, parent int, expected linuxThreadIdentity) (result linuxTaskObservation) {
	if err := ctx.Err(); err != nil {
		return linuxTaskObservation{err: err}
	}
	dir, err := unix.Openat(parent, strconv.Itoa(expected.tid), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if linuxTaskAbsentError(err) {
			return linuxTaskObservation{presence: linuxTaskAbsent}
		}
		return linuxTaskObservation{err: err}
	}
	defer func() {
		if err := unix.Close(dir); err != nil {
			result = linuxTaskObservation{err: errors.Join(result.err, err)}
		}
	}()
	return linuxReadTaskContent(ctx, dir)
}
func linuxTaskRetired(expected linuxThreadIdentity, held, current linuxTaskObservation) (bool, error) {
	if held.err != nil || current.err != nil {
		return false, errors.Join(held.err, current.err)
	}
	if expected.tid <= 0 || expected.start == 0 || (held.presence != linuxTaskAbsent && held.presence != linuxTaskPresent) || (current.presence != linuxTaskAbsent && current.presence != linuxTaskPresent) {
		return false, ErrProcessObservation
	}
	original := ProcessIncarnation{PID: ProcessID(expected.tid), StartTicks: expected.start}
	if held.presence == linuxTaskPresent {
		if held.identity != original {
			return false, ErrProcessObservation
		}
		return false, nil // Even S/Z/X is still the original observable task.
	}
	if current.presence == linuxTaskAbsent {
		return true, nil
	}
	if current.identity.PID != original.PID {
		return false, ErrProcessObservation
	}
	return current.identity.StartTicks != original.StartTicks, nil
}

// Only this sealed ROOT reexec enters the shared candidate synchronously in
// init, where Go pins the actual initial thread. Normal test init does no I/O.
var linuxBootstrapInitResult struct {
	selected, known bool
	err             error
}

const linuxBootstrapChildSelector = "-test.run=^TestOwnedLinuxNamespaceBootstrapChild$"
const (
	adversarialBootstrapMain         linuxAdversarialCase = "bootstrap-main"
	adversarialBootstrapDirect       linuxAdversarialCase = "bootstrap-direct"
	adversarialBootstrapCancelBefore linuxAdversarialCase = "bootstrap-cancel-before"
	adversarialBootstrapCancelAfter  linuxAdversarialCase = "bootstrap-cancel-after"
	adversarialBootstrapCloseAfter   linuxAdversarialCase = "bootstrap-close-after"
)

func init() {
	selected := false
	for _, arg := range os.Args {
		if arg == linuxBootstrapChildSelector {
			selected = true
		}
	}
	if !selected {
		return
	}
	linuxBootstrapInitResult.selected = true
	linuxBootstrapInitResult.err = ErrLinuxOwnerProtocol
	defer func() { linuxBootstrapInitResult.known = true }()
	if os.Getuid() != 0 || os.Geteuid() != 0 || unix.Gettid() != unix.Getpid() {
		return
	}
	// Checked CLOEXEC and root creator/parent identity BEFORE any namespace I/O.
	control, err := linuxAdversarialControlFile(os.NewFile(7, "sealed-root-bootstrap-control"), os.Getppid())
	if err != nil {
		linuxBootstrapInitResult.err = err
		return
	}
	defer func() { linuxBootstrapInitResult.err = errors.Join(linuxBootstrapInitResult.err, control.close()) }()
	for _, fd := range []int{3, 4, 5, 6} {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			linuxBootstrapInitResult.err = err
			return
		}
		if st.Mode&unix.S_IFMT != unix.S_IFCHR || uint64(st.Rdev) != unix.Mkdev(1, 3) {
			return
		}
		if err := unix.Close(fd); err != nil {
			linuxBootstrapInitResult.err = err
			return
		}
	}
	// init has no t.Context; this test-only child owns this bounded scope.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	hello, err := control.read(ctx)
	if err != nil {
		linuxBootstrapInitResult.err = err
		return
	}
	validMode := hello.Mode == adversarialBootstrapMain || hello.Mode == adversarialBootstrapDirect || hello.Mode == adversarialBootstrapCancelBefore || hello.Mode == adversarialBootstrapCancelAfter || hello.Mode == adversarialBootstrapCloseAfter
	if !validMode || hello.Version != 1 || hello.ID != 1 || hello.Kind != adversarialHello || hello.Code != adversarialOK || hello.WaitKnown || hello.Nonce == (LaunchNonce{}) || hello.Challenge == (LaunchNonce{}) {
		return
	}
	err = linuxBootstrapInitScenario(ctx, hello.Mode)
	reply := hello
	if err != nil {
		reply.Code = adversarialFailed
	}
	writeErr := control.write(ctx, reply)
	if err != nil || writeErr != nil {
		linuxBootstrapInitResult.err = errors.Join(err, writeErr)
		return
	}
	finish, err := control.read(ctx)
	if err != nil {
		linuxBootstrapInitResult.err = err
		return
	}
	if finish.Version != hello.Version || finish.ID != 2 || finish.Kind != adversarialFinish || finish.Mode != hello.Mode || finish.Nonce != hello.Nonce || finish.Challenge != hello.Challenge || finish.Code != adversarialOK || finish.WaitKnown {
		return
	}
	linuxBootstrapInitResult.err = control.write(ctx, finish)
}
func linuxBootstrapInitScenario(ctx context.Context, mode linuxAdversarialCase) (resultErr error) {
	if unix.Gettid() != unix.Getpid() {
		return ErrProcessObservation
	}
	n := newOwnedNetNSState()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, n.Close(cleanup))
	}()
	ready := make(chan error, 1)
	switch mode {
	case adversarialBootstrapMain:
		n.run(ctx, ready) // actual m0 branch; returns only after clean release
		if unix.Gettid() != unix.Getpid() {
			return ErrProcessObservation
		}
	case adversarialBootstrapDirect:
		var err error
		n, err = CreateOwnedNetNS(ctx) // init holds m0; real constructor uses direct off-main
		if err != nil {
			return err
		}
		if n == nil {
			return ErrProcessObservation
		}
		ready <- nil
	default:
		phaseCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		if mode == adversarialBootstrapCancelBefore {
			cancel()
		}
		// Same real phase methods: the test owns the real initial-thread
		// bootstrap release, with a real locked successor (no fake capture).
		runtime.LockOSThread()
		bootstrap := newLinuxNamespaceBootstrap()
		go n.runSuccessor(phaseCtx, ready, bootstrap)
		captured := <-bootstrap.captured
		if mode != adversarialBootstrapCancelBefore && captured.err != nil {
			bootstrap.releaseLocked()
			return captured.err
		}
		if mode != adversarialBootstrapCancelBefore && (captured.worker.tid == unix.Getpid() || captured.worker.start == 0) {
			bootstrap.releaseLocked()
			return ErrProcessObservation
		}
		if mode == adversarialBootstrapCancelAfter {
			cancel()
		}
		n.requestClose()
		premature := false
		select {
		case <-n.done:
			premature = true
		default:
		}
		select {
		case <-ready:
			premature = true
		default:
		}
		n.mu.Lock()
		touched := n.fd != -1 || n.original != -1
		n.mu.Unlock()
		bootstrap.releaseLocked()
		if premature || touched {
			return ErrProcessObservation
		}
		select {
		case err := <-ready:
			if err == nil {
				return ErrProcessObservation
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-n.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if mode == adversarialBootstrapCancelBefore && !errors.Is(captured.err, context.Canceled) {
			return ErrProcessObservation
		}
		return nil
	}
	select {
	case err := <-ready:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	n.mu.Lock()
	worker, identity := n.worker, n.identity
	n.mu.Unlock()
	if worker.tid == unix.Getpid() || worker.tid == 0 || worker.start == 0 || identity == (linuxNamespaceIdentity{}) {
		return ErrProcessObservation
	}
	return nil
}
func TestOwnedLinuxNamespaceBootstrapChild(t *testing.T) {
	if !linuxBootstrapInitResult.selected {
		t.Skip("requires sealed root init reexec")
	}
	if !linuxBootstrapInitResult.known || linuxBootstrapInitResult.err != nil {
		t.Fatal("actual init bootstrap failed", linuxBootstrapInitResult.err)
	}
}
func TestLinuxNamespaceOffMainBootstrap(t *testing.T) {
	linuxAdversarialRequireNative(t)
	for _, mode := range []linuxAdversarialCase{adversarialBootstrapMain, adversarialBootstrapDirect, adversarialBootstrapCancelBefore, adversarialBootstrapCancelAfter, adversarialBootstrapCloseAfter} {
		t.Run(string(mode), func(t *testing.T) { linuxTestBootstrapReexec(t, mode) })
	}
}
func linuxTestBootstrapReexec(t *testing.T, mode linuxAdversarialCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := ReadPublicArtifact(ctx, "/inputs/sha256.expected", 1024)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.SplitN(string(raw), "\n", 2)[0]
	if len(line) != 64+2+len("loadstand.test") || line[64:] != "  loadstand.test" {
		t.Fatal("root reexec hash shape")
	}
	if err := auditLinuxExecutable(ctx, binary, ExecutableSHA256(line[:64])); err != nil {
		t.Fatal(err)
	}
	control, child, err := linuxAdversarialPair()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := control.close(); err != nil {
			t.Error(err)
		}
	}()
	null, err := os.Open("/dev/null")
	if err != nil {
		if e := child.Close(); e != nil {
			t.Error(e)
		}
		t.Fatal(err)
	}
	command := exec.Command(binary, linuxBootstrapChildSelector, "-test.timeout=25s")
	command.ExtraFiles = []*os.File{null, null, null, null, child}
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	err = command.Start()
	aliasErr := errors.Join(null.Close(), child.Close())
	if err != nil {
		t.Fatal("root reexec Start", errors.Join(err, aliasErr))
	}
	waitDone := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(waitDone) }() // sole actual waiter
	defer func() {
		select {
		case <-waitDone:
			return
		default:
		}
		if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		defer stop()
		select {
		case <-waitDone:
		case <-cleanup.Done():
			t.Error("root child actual Wait unknown")
		}
	}()
	if aliasErr != nil {
		t.Fatal(aliasErr)
	}
	var nonce, challenge LaunchNonce
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(challenge[:]); err != nil {
		t.Fatal(err)
	}
	client := &linuxAdversarialClient{owner: control, frame: linuxAdversarialFrame{Version: 1, Mode: mode, Nonce: nonce, Challenge: challenge, Code: adversarialOK}}
	if _, err := client.call(ctx, adversarialHello); err != nil {
		t.Fatal("root init scenario", err)
	}
	if _, err := client.call(ctx, adversarialFinish); err != nil {
		t.Fatal("root init final ack", err)
	}
	select {
	case <-waitDone:
		if waitErr != nil {
			t.Fatal("root child actual Wait", waitErr)
		}
	case <-ctx.Done():
		t.Fatal("root child actual Wait unknown", ctx.Err())
	}
}
