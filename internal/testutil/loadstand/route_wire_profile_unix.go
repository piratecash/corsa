//go:build darwin || linux

package loadstand

import (
	"context"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

type routeWireProfileIdentity struct {
	device, inode uint64
	uid, gid      uint32
}

func (p routeWireProfileIdentity) matches(st unix.Stat_t) bool {
	return p.inode != 0 && uint64(st.Dev) == p.device && st.Ino == p.inode &&
		st.Uid == p.uid && st.Gid == p.gid && st.Mode&(unix.S_IFMT|07777) == unix.S_IFDIR|0700
}

// readRouteWireProfile opens only the original private member directory under
// the retained parent. The authenticated lifecycle seal fixes the byte limit
// and content; its verifier also checks the original launch and census.
func readRouteWireProfile(ctx context.Context, parent *os.File, profile routeWireProfileIdentity, nonce LaunchNonce, seal RouteWireFinalSeal) (_ []byte, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parent == nil || nonce == (LaunchNonce{}) || !seal.validFor(seal.Census) {
		return nil, ErrFinalReportInvalid
	}
	const directoryName = "member-1"
	fd, err := unix.Openat(int(parent.Fd()), directoryName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	directory := os.NewFile(uintptr(fd), "route-wire-profile")
	defer func() { result = errors.Join(result, directory.Close()) }()
	var openedProfile unix.Stat_t
	if err := unix.Fstat(fd, &openedProfile); err != nil || !profile.matches(openedProfile) {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	if err := auditFinalACL(directory); err != nil {
		return nil, err
	}

	name := "final-" + nonce.String() + ".json"
	validFile := func(st unix.Stat_t) bool {
		return st.Mode&(unix.S_IFMT|07777) == unix.S_IFREG|0600 && st.Uid == profile.uid && st.Gid == profile.gid &&
			st.Nlink == 1 && st.Size == int64(seal.Bytes)
	}
	var before unix.Stat_t
	if err := unix.Fstatat(fd, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil || !validFile(before) {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	reportFD, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	report := os.NewFile(uintptr(reportFD), "route-wire-final")
	defer func() { result = errors.Join(result, report.Close()) }()
	sameFile := func(st unix.Stat_t) bool {
		return validFile(st) && st.Dev == before.Dev && st.Ino == before.Ino
	}
	var opened unix.Stat_t
	if err := unix.Fstat(reportFD, &opened); err != nil || !sameFile(opened) {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	if err := auditFinalACL(report); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.NewSectionReader(report, 0, int64(seal.Bytes)+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var after, named, namedProfile unix.Stat_t
	if err := errors.Join(unix.Fstat(reportFD, &after), unix.Fstatat(fd, name, &named, unix.AT_SYMLINK_NOFOLLOW),
		unix.Fstatat(int(parent.Fd()), directoryName, &namedProfile, unix.AT_SYMLINK_NOFOLLOW)); err != nil ||
		!sameFile(after) || !sameFile(named) || !profile.matches(namedProfile) {
		return nil, errors.Join(ErrUnsupportedArtifact, err)
	}
	if err := errors.Join(auditFinalACL(directory), auditFinalACL(report)); err != nil {
		return nil, err
	}
	if !seal.matches(raw, seal.Census) {
		return nil, ErrFinalReportInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return raw, nil
}
