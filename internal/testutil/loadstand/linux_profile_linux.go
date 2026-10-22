//go:build linux

package loadstand

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// AuditedProfileParent can only originate from this manager's newly created
// root-owned sticky parent. Copies of its pointer do not mint another owner.
type AuditedProfileParent struct {
	mu            sync.Mutex
	file          *os.File
	path          string
	device, inode uint64
}
type linuxProfileLease struct {
	file          *os.File
	path          string
	device, inode uint64
}

func CreateAuditedProfileParent(ctx context.Context, basePath string, nonce LaunchNonce) (*AuditedProfileParent, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if os.Geteuid() != 0 || !filepath.IsAbs(basePath) || filepath.Clean(basePath) != basePath || len(basePath) > 4096 || nonce == (LaunchNonce{}) {
		return nil, ErrLinuxOwnerUnsupported
	}
	base, err := openLinuxProfileAncestor(basePath)
	if err != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	defer func() { _ = base.Close() }() // root-owned construction-only descriptor
	name := "loadstand-" + nonce.String()
	if unix.Mkdirat(int(base.Fd()), name, 01777) != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	fd, err := unix.Openat(int(base.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	parent := os.NewFile(uintptr(fd), "owned-profile-parent")
	var st unix.Stat_t
	if unix.Fchmod(fd, 01777) != nil || unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Mode&07777 != 01777 {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, parent.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, parent.Close())
	}
	return &AuditedProfileParent{file: parent, path: filepath.Join(basePath, name), device: uint64(st.Dev), inode: st.Ino}, nil
}
func openLinuxProfileAncestor(path string) (*os.File, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if openErr != nil || closeErr != nil {
			if openErr == nil {
				_ = unix.Close(next)
			}
			return nil, ErrLinuxOwnerUnsupported
		}
		fd = next
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Mode&0001 == 0 || (st.Mode&0022 != 0 && st.Mode&01000 == 0) {
			return nil, errors.Join(ErrLinuxOwnerUnsupported, unix.Close(fd))
		}
	}
	return os.NewFile(uintptr(fd), "audited-profile-base"), nil
}
func (p *AuditedProfileParent) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil
	}
	err := p.file.Close()
	p.file = nil
	return err
}
func (p *AuditedProfileParent) acquire() (*linuxProfileLease, error) {
	if p == nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file == nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	var st unix.Stat_t
	if unix.Fstat(int(p.file.Fd()), &st) != nil || uint64(st.Dev) != p.device || st.Ino != p.inode || st.Uid != 0 || st.Mode&07777 != 01777 {
		return nil, ErrLinuxOwnerUnsupported
	}
	fd, err := unix.FcntlInt(p.file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	return &linuxProfileLease{file: os.NewFile(uintptr(fd), "profile-parent-lease"), path: p.path, device: p.device, inode: p.inode}, nil
}

// The inherited parent has no public FD-import constructor. Only the private
// helper bootstrap calls this after exact owner/namespace/nonce validation.
func createLinuxPrivateProfile(ctx context.Context, parent *os.File, init linuxInit) (*os.File, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var st unix.Stat_t
	if parent == nil || unix.Fstat(int(parent.Fd()), &st) != nil || st.Uid != 0 || st.Mode&07777 != 01777 || uint64(st.Dev) != init.ParentDevice || st.Ino != init.ParentInode {
		return nil, ErrLinuxOwnerUnsupported
	}
	if filepath.Base(init.Config.DataDir) != "member-1" {
		return nil, ErrLinuxOwnerUnsupported
	}
	if init.Planned == nil || init.Planned.Ordinal == 1 {
		if unix.Mkdirat(int(parent.Fd()), "member-1", 0700) != nil {
			return nil, ErrLinuxOwnerUnsupported
		}
	} else if init.Planned.ProfileDevice == 0 || init.Planned.ProfileInode == 0 {
		return nil, ErrLinuxOwnerUnsupported
	}
	fd, err := unix.Openat(int(parent.Fd()), "member-1", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrLinuxOwnerUnsupported
	}
	directory := os.NewFile(uintptr(fd), "private-member-profile")
	if init.Planned != nil && init.Planned.Ordinal > 1 {
		var original unix.Stat_t
		if unix.Fstat(fd, &original) != nil || uint64(original.Dev) != init.Planned.ProfileDevice || original.Ino != init.Planned.ProfileInode || original.Uid != uint32(init.UID) || original.Mode&07777 != 0700 {
			return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
		}
	}
	if err := auditPrivateFinalFD(directory, true); err != nil {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
	}
	opened, statErr := directory.Stat()
	pathInfo, pathErr := os.Lstat(init.Config.DataDir)
	if statErr != nil || pathErr != nil || !os.SameFile(opened, pathInfo) || !pathInfo.IsDir() {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
	}
	probeFD, err := unix.Openat(fd, ".acl-probe", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
	}
	probe := os.NewFile(uintptr(probeFD), "profile-acl-probe")
	auditErr := auditPrivateFinalFD(probe, false)
	closeErr := probe.Close()
	removeErr := unix.Unlinkat(fd, ".acl-probe", 0)
	if auditErr != nil || closeErr != nil || removeErr != nil {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
	}
	if err := preparePrivateProfile(init.Config.DataDir); err != nil {
		return nil, errors.Join(ErrLinuxOwnerUnsupported, directory.Close())
	}
	return directory, nil
}
