//go:build linux

package loadstand

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"

	"golang.org/x/sys/unix"
)

type linuxNamespaceIdentity struct{ device, inode uint64 }
type linuxNamespaceOperation struct {
	ctx     context.Context
	owner   *LinuxFixture
	dial    *linuxDialOffer
	planned *linuxPlannedNamespaceOperation
	result  chan error
}

// OwnedNetNS is minted only by this locked worker's Unshare. Its descriptors
// and operation queue are private: exactly one operation, zero waiting jobs.
type OwnedNetNS struct {
	plannedOwner                    *LinuxFixture // exact latest planned incarnation; not a resettable claim
	mu                              sync.Mutex
	fd, original                    int
	identity, origin                linuxNamespaceIdentity
	worker                          linuxThreadIdentity
	operations                      chan linuxNamespaceOperation
	closing                         chan struct{}
	done                            chan struct{}
	helperReaped                    <-chan struct{}
	busy, claimed, closed, poisoned bool
}

func CreateOwnedNetNS(ctx context.Context) (*OwnedNetNS, error) {
	if os.Geteuid() != 0 || ctx.Err() != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	n := newOwnedNetNSState()
	ready := make(chan error, 1)
	go n.run(ctx, ready)
	select {
	case err := <-ready:
		if err != nil {
			<-n.done
			n.releaseDescriptors()
			return nil, err
		}
		return n, nil
	case <-ctx.Done():
		n.requestClose()
		return n, ctx.Err() // ownership is retained until worker join
	}
}
func namespaceIdentity(fd int) (linuxNamespaceIdentity, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	kind, err := unix.IoctlRetInt(fd, unix.NS_GET_NSTYPE)
	if err != nil || kind != unix.CLONE_NEWNET || unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.NSFS_MAGIC {
		return linuxNamespaceIdentity{}, ErrLinuxOwnerUnsupported
	}
	return linuxNamespaceIdentity{uint64(st.Dev), st.Ino}, nil
}
func openThreadNetNS() (int, linuxNamespaceIdentity, error) {
	fd, err := unix.Open("/proc/thread-self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, linuxNamespaceIdentity{}, ErrLinuxOwnerUnsupported
	}
	identity, err := namespaceIdentity(fd)
	if err != nil {
		_ = unix.Close(fd)
		return -1, identity, err
	}
	return fd, identity, nil
}
func enableNamespaceLoopback() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	request, err := unix.NewIfreq("lo")
	if err == nil {
		err = unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, request)
	}
	if err == nil {
		request.SetUint16(request.Uint16() | unix.IFF_UP)
		err = unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, request)
	}
	return errors.Join(err, unix.Close(fd))
}
func (n *OwnedNetNS) restore() bool {
	if unix.Setns(n.original, unix.CLONE_NEWNET) != nil {
		return false
	}
	fd, identity, err := openThreadNetNS()
	if err != nil {
		return false
	}
	return unix.Close(fd) == nil && identity == n.origin
}

// One immutable actual capture and one clean-bootstrap completion. Neither
// channel is a work queue; the successor is the only namespace worker.
type linuxNamespaceCapture struct {
	worker linuxThreadIdentity
	err    error
}
type linuxNamespaceBootstrap struct {
	captured chan linuxNamespaceCapture
	released chan struct{}
}

func newLinuxNamespaceBootstrap() *linuxNamespaceBootstrap {
	return &linuxNamespaceBootstrap{captured: make(chan linuxNamespaceCapture, 1), released: make(chan struct{})}
}

// Called only by the clean, externally locked initial-thread bootstrap.
// Unlock balances OUR external lock; runtime's internal init lock is untouched.
func (b *linuxNamespaceBootstrap) releaseLocked() {
	runtime.UnlockOSThread()
	close(b.released) // final owned bootstrap action; no namespace I/O preceded it
}
func captureLinuxNamespaceWorker(ctx context.Context) linuxNamespaceCapture {
	if err := ctx.Err(); err != nil {
		return linuxNamespaceCapture{err: err}
	}
	if unix.Gettid() == unix.Getpid() {
		return linuxNamespaceCapture{err: ErrLinuxOwnerUnsupported}
	}
	worker, err := captureLinuxThread(ctx)
	if err != nil {
		err = errors.Join(err, ctx.Err())
	}
	if err == nil && (worker.tid != unix.Gettid() || worker.tid == unix.Getpid()) {
		err = ErrLinuxOwnerUnsupported
	}
	return linuxNamespaceCapture{worker: worker, err: err}
}
func newOwnedNetNSState() *OwnedNetNS {
	return &OwnedNetNS{fd: -1, original: -1, operations: make(chan linuxNamespaceOperation), closing: make(chan struct{}), done: make(chan struct{})}
}
func (n *OwnedNetNS) run(ctx context.Context, ready chan<- error) {
	runtime.LockOSThread()
	if unix.Gettid() != unix.Getpid() {
		n.runLockedWorker(ctx, ready, captureLinuxNamespaceWorker(ctx))
		return
	}
	// m0 cannot physically exit: keep it clean and locked until the ONE
	// successor has locked/captured a different actual thread.
	bootstrap := newLinuxNamespaceBootstrap()
	go n.runSuccessor(ctx, ready, bootstrap)
	<-bootstrap.captured
	bootstrap.releaseLocked()
}
func (n *OwnedNetNS) runSuccessor(ctx context.Context, ready chan<- error, bootstrap *linuxNamespaceBootstrap) {
	runtime.LockOSThread()
	captured := captureLinuxNamespaceWorker(ctx)
	bootstrap.captured <- captured
	<-bootstrap.released
	n.runLockedWorker(ctx, ready, captured)
}
func (n *OwnedNetNS) runLockedWorker(ctx context.Context, ready chan<- error, captured linuxNamespaceCapture) {
	// Both callers retain the actual external lock through this whole body.
	// The successor has already joined its clean bootstrap release.
	restored := true // no namespace mutation has happened on this thread yet
	defer func() {
		if restored {
			runtime.UnlockOSThread()
		}
		close(n.done) // Go body completion, never kernel task-disappearance proof
	}()
	err := captured.err
	if err == nil && (captured.worker.tid != unix.Gettid() || unix.Gettid() == unix.Getpid()) {
		err = ErrLinuxOwnerUnsupported
	}
	n.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && n.closed {
		err = ErrLinuxOwnerPoisoned
	}
	if err == nil {
		n.worker = captured.worker
	}
	n.mu.Unlock()
	if err != nil {
		ready <- err
		return
	}
	if err == nil {
		n.original, n.origin, err = openThreadNetNS()
	}
	if err == nil {
		restored = false
		err = unix.Unshare(unix.CLONE_NEWNET)
	}
	if err == nil {
		n.fd, n.identity, err = openThreadNetNS()
	}
	if err == nil {
		err = enableNamespaceLoopback()
	}
	if n.original >= 0 {
		restored = n.restore()
	}
	if err != nil || !restored {
		n.mu.Lock()
		n.poisoned = true
		n.mu.Unlock()
		ready <- ErrLinuxOwnerUnsupported
		return
	}
	n.mu.Lock()
	err = ctx.Err()
	if err == nil && n.closed {
		err = ErrLinuxOwnerPoisoned
	}
	n.mu.Unlock()
	ready <- err
	if err != nil {
		return
	}
	for {
		select {
		case <-n.closing:
			n.mu.Lock()
			reaped := n.helperReaped
			n.mu.Unlock()
			if reaped != nil {
				<-reaped
			}
			return
		case operation := <-n.operations:
			if operation.ctx.Err() != nil {
				if operation.owner != nil && operation.dial == nil {
					operation.owner.resolveNeverStarted()
				}
				n.finishOperation(operation, operation.ctx.Err())
				continue
			}
			if unix.Setns(n.fd, unix.CLONE_NEWNET) != nil {
				n.poison()
				if operation.owner != nil && operation.dial == nil {
					operation.owner.resolveNeverStarted()
				}
				n.finishOperation(operation, ErrLinuxOwnerPoisoned)
				continue
			}
			if operation.planned != nil {
				err = n.plannedInNamespace(operation.ctx, operation.planned)
			} else if operation.dial != nil {
				err = n.dialInNamespace(operation)
			} else {
				err = operation.owner.spawnOnNamespaceThread(operation.ctx, n)
			}
			restored = n.restore()
			if !restored {
				if operation.dial != nil && operation.dial.conn != nil {
					_ = operation.dial.conn.Close()
				}
				n.poison()
				if operation.owner != nil {
					operation.owner.poison()
				}
				n.finishOperation(operation, ErrLinuxOwnerPoisoned)
				return
			}
			if operation.dial != nil && err == nil {
				err = operation.owner.offerDial(operation.ctx, operation.dial)
			}
			n.finishOperation(operation, err)
		}
	}
}
func (n *OwnedNetNS) poison()           { n.mu.Lock(); n.poisoned = true; n.mu.Unlock() }
func (n *OwnedNetNS) releaseOperation() { n.mu.Lock(); n.busy = false; n.mu.Unlock() }

// All effects and namespace restoration finish before the caller sees completion.
func (n *OwnedNetNS) finishOperation(operation linuxNamespaceOperation, err error) {
	n.releaseOperation()
	operation.result <- err
}
func (n *OwnedNetNS) submit(ctx context.Context, operation linuxNamespaceOperation) error {
	n.mu.Lock()
	if n.closed || n.poisoned {
		n.mu.Unlock()
		return ErrLinuxOwnerPoisoned
	}
	if n.busy {
		n.mu.Unlock()
		return ErrLinuxOwnerBusy
	}
	n.busy = true
	n.mu.Unlock()
	select {
	case n.operations <- operation:
		return nil
	case <-ctx.Done():
		n.releaseOperation()
		return ctx.Err()
	case <-n.closing:
		n.releaseOperation()
		return ErrLinuxOwnerPoisoned
	}
}
func (n *OwnedNetNS) requestClose() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.closing)
	}
}
func (n *OwnedNetNS) Close(ctx context.Context) error {
	if n == nil {
		return nil
	}
	if n.done == nil {
		return nil
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrLinuxOwnerBound
	}
	n.requestClose()
	select {
	case <-n.done:
	case <-ctx.Done():
		return errors.Join(ErrStopUnconfirmed, ctx.Err())
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.helperReaped != nil {
		select {
		case <-n.helperReaped:
		default:
			return ErrStopUnconfirmed
		}
	}
	var err error
	if n.fd >= 0 {
		err = unix.Close(n.fd)
		n.fd = -1
	}
	if n.original >= 0 {
		err = errors.Join(err, unix.Close(n.original))
		n.original = -1
	}
	return err
}
func (n *OwnedNetNS) releaseDescriptors() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fd >= 0 {
		_ = unix.Close(n.fd)
		n.fd = -1
	}
	if n.original >= 0 {
		_ = unix.Close(n.original)
		n.original = -1
	}
}
