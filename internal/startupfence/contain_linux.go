//go:build linux

package startupfence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"golang.org/x/sys/unix"
)

type containedChild struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func (c *containedChild) exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func (c *containedChild) kill() error {
	err := syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func privatePair() (*os.File, *os.File, error) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	for _, fd := range pair {
		if err := unix.SetNonblock(fd, true); err != nil {
			_ = unix.Close(pair[0])
			_ = unix.Close(pair[1])
			return nil, nil, err
		}
	}
	return os.NewFile(uintptr(pair[0]), "fence-supervisor"), os.NewFile(uintptr(pair[1]), "fence-child"), nil
}

func startContainedChild(ctx context.Context, command ChildCommand, channel, lock *os.File) (*containedChild, error) {
	if err := checkContainment(command); err != nil {
		return nil, err
	}
	child := &containedChild{done: make(chan struct{})}
	started := make(chan error, 1)
	go func() {
		// Linux Pdeathsig belongs to the parent THREAD. Keep that thread alive
		// from fork through Wait, not merely until exec.Cmd.Start returns.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		cmd := exec.CommandContext(ctx, command.Path, command.Args...)
		environment, err := childEnvironment(command)
		if err != nil {
			started <- err
			child.err = err
			close(child.done)
			return
		}
		cmd.Env = environment
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		// FD4 is the SAME open-file description holding the profile flock. Keep it
		// inherited until process exit: parent death alone cannot release that lock
		// before Pdeathsig has actually killed the old child and closed its sockets.
		cmd.ExtraFiles = []*os.File{channel, lock}
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: command.UID, Gid: command.GID}, Pdeathsig: syscall.SIGKILL, Setpgid: true}
		if command.isolated {
			cmd.SysProcAttr.Cloneflags = syscall.CLONE_NEWNET
		}
		// TERM admits normal App teardown; WaitDelay bounds a stuck direct child.
		// The deployed service's control-group policy contains descendants.
		cmd.WaitDelay = 5 * time.Second
		cmd.Cancel = func() error {
			if cmd.Process == nil {
				return nil
			}
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		child.cmd = cmd
		if err := cmd.Start(); err != nil {
			started <- err
			child.err = err
			close(child.done)
			return
		}
		started <- nil
		child.err = cmd.Wait()
		// CommandContext reports cancellation even when TERM was handled and the
		// kernel reports exit zero. Controller still requires the exact Close ACK.
		if cmd.ProcessState != nil && cmd.ProcessState.Success() && (errors.Is(child.err, context.Canceled) || errors.Is(child.err, context.DeadlineExceeded)) {
			child.err = nil
		}
		close(child.done)
	}()
	if err := <-started; err != nil {
		return nil, err
	}
	return child, nil
}

func lockBinding(node domain.PeerIdentity, _ StoreID) (*os.File, error) {
	const directory = "/run/corsa-supervisor"
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return nil, ErrUnauthorized
	}
	sum := sha256.Sum256(node[:])
	path := filepath.Join(directory, hex.EncodeToString(sum[:])+".lock")
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(filepath.Base(path), os.O_RDWR|os.O_CREATE|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := secretfile.AuditLocation(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("supervisor already owns node lifetime: %w", errors.Join(ErrChanged, err))
	}
	// Never call LOCK_UN: it would unlock the child's inherited alias too.
	return file, nil
}

func checkContainment(command ChildCommand) error {
	if _, err := childEnvironment(command); err != nil {
		return err
	}
	if os.Geteuid() != 0 || command.UID == 0 || command.GID == 0 {
		return fmt.Errorf("root supervisor and separate non-root child UID/GID required: %w", ErrUnsupported)
	}
	if !filepath.IsAbs(command.Path) {
		return ErrUnauthorized
	}
	current := command.Path
	for {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 || info.Mode()&os.ModeSymlink != 0 {
			return ErrUnauthorized
		}
		if info.Mode().Perm()&0o022 != 0 && (!info.IsDir() || info.Mode()&os.ModeSticky == 0) {
			return ErrUnauthorized
		}
		if current == command.Path && !info.Mode().IsRegular() {
			return ErrUnauthorized
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return nil
}

func verifySupervisor(file *os.File, expected string, _ domain.PeerIdentity) error {
	if file == nil || expected == "" {
		return ErrUnauthorized
	}
	cred, err := unix.GetsockoptUcred(int(file.Fd()), unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return err
	}
	if cred.Uid != 0 || int(cred.Pid) != os.Getppid() {
		return ErrUnauthorized
	}
	var lockStat unix.Stat_t
	if err := unix.Fstat(4, &lockStat); err != nil || lockStat.Uid != 0 || lockStat.Mode&0o777 != 0o600 || lockStat.Mode&unix.S_IFMT != unix.S_IFREG {
		return ErrUnauthorized
	}
	wanted, err := os.Stat(expected)
	if err != nil {
		return err
	}
	stat, ok := wanted.Sys().(*syscall.Stat_t)
	// A separate-UID child cannot inspect /proc/<root-parent>/exe under Linux
	// ptrace rules. Trust is the verified root parent on the private inherited
	// socket; the approved path is checked for ownership, not claimed to prove
	// the running executable. A malicious root is outside this failure model.
	if !ok || stat.Uid != 0 || wanted.Mode().Perm()&0o022 != 0 || !wanted.Mode().IsRegular() {
		return ErrUnauthorized
	}
	return nil
}
