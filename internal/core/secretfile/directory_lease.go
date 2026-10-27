package secretfile

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"unsafe"
)

var (
	ErrDirectoryLeaseBusy        = errors.New("pinned directory lease busy")
	ErrDirectoryLeaseUnsupported = errors.New("pinned directory lease unsupported")
	ErrDirectoryLeaseInUse       = errors.New("pinned directory lease has an open name scan")
	ErrNameScanLimits            = errors.New("invalid bounded name scan limits")
	ErrNameScanLimit             = errors.New("bounded name scan entry limit exceeded")
)

// DirectoryLease is an advisory cooperative-writer lease on a DIRECTORY
// INODE, not a lock file or permission/ACL/durability proof. It owns separate
// open descriptions for its pinned Root and lock FD. Neither the caller's Dir
// nor a scan/preflight FD aliases the lock open-file description. No raw FD,
// ambient path, LOCK_UN or lock-stealing operation is exposed.
// Existing Dir.Write/DurableWrite are NOT automatically enrolled: the future
// issuer must serialize its own writes, recovery and Close. Do not copy a
// DirectoryLease after first use.
type DirectoryLease struct {
	mu       sync.Mutex
	root     *os.Root
	lock     *os.File
	identity fs.FileInfo
	scanOpen bool
	closed   bool
	closeErr error
}

// TryDirectoryLease never waits for another cooperating writer. All access is
// relative to the already pinned Dir; the independent Root survives d.Close.
// Unsupported kernel/filesystem behavior is an error, never a fallback.
func (d *Dir) TryDirectoryLease() (*DirectoryLease, error) {
	if d == nil || d.root == nil {
		return nil, os.ErrInvalid
	}
	identity, err := d.root.Stat(".")
	if err != nil {
		return nil, stripPath(err)
	}
	root, err := d.root.OpenRoot(".")
	if err != nil {
		return nil, stripPath(err)
	}
	file, err := root.Open(".")
	if err != nil {
		return nil, errors.Join(stripPath(err), stripPath(root.Close()))
	}
	info, err := file.Stat()
	if err != nil || !info.IsDir() || !os.SameFile(identity, info) {
		if err == nil {
			err = os.ErrInvalid
		}
		return nil, errors.Join(stripPath(err), stripPath(root.Close()), stripPath(file.Close()))
	}
	if err := tryDirectoryFlock(file); err != nil {
		return nil, errors.Join(err, stripPath(root.Close()), stripPath(file.Close()))
	}
	return &DirectoryLease{root: root, lock: file, identity: info}, nil
}

// Matches binds a separate Dir to the held inode. A replacement at the old
// ambient path is a different store even when that directory has its own lock.
func (l *DirectoryLease) Matches(d *Dir) (bool, error) {
	if l == nil {
		return false, os.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false, os.ErrClosed
	}
	if d == nil || d.root == nil {
		return false, os.ErrInvalid
	}
	info, err := d.root.Stat(".")
	if err != nil {
		return false, stripPath(err)
	}
	return os.SameFile(l.identity, info), nil
}

// Close refuses while a scan is still open: callers close/join operations
// before releasing authority. It closes the lock FD LAST, never LOCK_UN.
// A close error leaves this object unusable and returns on every later Close;
// it does not assert that the kernel lease was released or retry a possibly
// recycled descriptor. Successful repeated Close is an idempotent no-op.
func (l *DirectoryLease) Close() error {
	if l == nil {
		return os.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.closeErr
	}
	if l.scanOpen {
		return ErrDirectoryLeaseInUse
	}
	l.closed = true
	rootErr := l.root.Close()
	lockErr := l.lock.Close()
	l.closeErr = errors.Join(stripPath(rootErr), stripPath(lockErr))
	return l.closeErr
}

// NameScan holds one independent name-only directory cursor under its lease.
// One active scan is a structural lane, not a deployment resource setting.
// Returned batches are caller-owned; retaining arbitrary external copies is
// outside this primitive's memory bound. Complete requires EOF AND checked
// Close. A prefix, overflow, I/O error or early close is never complete.
// Complete is EOF+checked close, not an immutable namespace snapshot. The
// caller holds the lease and excludes its own writes during the walk. Do not
// copy a NameScan after first use.
type NameScan struct {
	mu                    sync.Mutex
	lease                 *DirectoryLease
	file                  *os.File
	batch, limit, seen    int
	eof, closed, complete bool
	terminalErr, closeErr error
}

// OpenNameScan allocates no whole-history list. The caller selects both caps;
// positive values are allocation requests, not a promise an arbitrary batch
// fits RAM. Checked arithmetic rejects impossible string-slice allocation.
func (l *DirectoryLease) OpenNameScan(batch, maxNames int) (*NameScan, error) {
	if batch <= 0 || maxNames <= 0 || batch > int(^uint(0)>>1)/int(unsafe.Sizeof("")) {
		return nil, ErrNameScanLimits
	}
	if l == nil {
		return nil, os.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, os.ErrClosed
	}
	if l.scanOpen {
		return nil, ErrDirectoryLeaseInUse
	}
	file, err := l.root.Open(".")
	if err != nil {
		return nil, stripPath(err)
	}
	info, err := file.Stat()
	if err != nil || !info.IsDir() || !os.SameFile(l.identity, info) {
		if err == nil {
			err = os.ErrInvalid
		}
		return nil, errors.Join(stripPath(err), stripPath(file.Close()))
	}
	l.scanOpen = true
	return &NameScan{lease: l, file: file, batch: batch, limit: maxNames}, nil
}

// Next exposes names only, with no DirEntry.Info/ambient lookup. It may read
// one extra name to detect cap exhaustion, but never returns that partial
// overflow batch as success. Prior batches do not establish complete coverage.
func (s *NameScan) Next() ([]string, error) {
	if s == nil {
		return nil, os.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		if s.closeErr != nil {
			return nil, s.closeErr
		}
		if s.complete {
			return nil, io.EOF
		}
		return nil, os.ErrClosed
	}
	if s.terminalErr != nil {
		return nil, s.terminalErr
	}
	if s.eof {
		return nil, io.EOF
	}
	remaining := s.limit - s.seen
	if remaining < 0 {
		s.terminalErr = ErrNameScanLimit
		return nil, s.terminalErr
	}
	n := s.batch
	if remaining < n {
		n = remaining + 1
	} // remaining<batch<=MaxInt: no +1 wrap
	names, err := s.file.Readdirnames(n)
	if len(names) > n || len(names) > remaining {
		s.terminalErr = ErrNameScanLimit
		return nil, s.terminalErr
	}
	if err != nil && !errors.Is(err, io.EOF) {
		s.terminalErr = stripPath(err)
		return nil, s.terminalErr
	}
	if len(names) == 0 && err == nil {
		s.terminalErr = io.ErrNoProgress
		return nil, s.terminalErr
	}
	s.seen += len(names) // <=limit, checked before addition
	if errors.Is(err, io.EOF) {
		s.eof = true
	}
	if len(names) != 0 {
		return names, nil
	}
	return nil, io.EOF
}

func (s *NameScan) Complete() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.complete
}

func (s *NameScan) Close() error {
	if s == nil {
		return os.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = errors.Join(s.terminalErr, stripPath(s.file.Close()))
	s.complete = s.eof && s.closeErr == nil
	// No remaining FS operation uses the lease after this point. This lock
	// order is scan.mu -> lease.mu; lease methods never take scan.mu.
	s.lease.mu.Lock()
	s.lease.scanOpen = false
	s.lease.mu.Unlock()
	return s.closeErr
}
