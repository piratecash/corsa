//go:build linux

package loadstand

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"strconv"
)

var errLinuxInheritedCapability = errors.New("target inherited a forbidden bootstrap capability")

type linuxInheritedCapabilityError struct{ object linuxObjectIdentity }

func (e *linuxInheritedCapabilityError) Error() string { return errLinuxInheritedCapability.Error() }
func (e *linuxInheritedCapabilityError) Unwrap() error { return errLinuxInheritedCapability }

type linuxObjectIdentity struct {
	device, inode uint64
	kind          uint32
}

func linuxObject(fd int) (linuxObjectIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return linuxObjectIdentity{}, err
	}
	return linuxObjectIdentity{uint64(st.Dev), st.Ino, st.Mode & unix.S_IFMT}, nil
}
func linuxPacketObject(p *linuxPacketConn) (id linuxObjectIdentity, result error) {
	var statErr error
	result = p.raw.Control(func(fd uintptr) { id, statErr = linuxObject(int(fd)) })
	result = errors.Join(result, statErr)
	// Control's result and fstat's result are distinct.
	if result != nil {
		return linuxObjectIdentity{}, result
	}
	if id.inode == 0 {
		return id, ErrProcessObservation
	}
	return id, nil
}
func (s *linuxHelperSession) auditTargetDescriptors(ctx context.Context) error {
	life, e1 := linuxPacketObject(s.runtime.life)
	work, e2 := linuxPacketObject(s.runtime.work)
	ns, e3 := linuxObject(int(s.namespace.Fd()))
	parent, e4 := linuxObject(int(s.parent.Fd()))
	if errors.Join(e1, e2, e3, e4) != nil {
		return ErrProcessObservation
	}
	return s.runtime.sampler.auditDescriptors(ctx, []linuxObjectIdentity{life, work, ns, parent}, s.runtime.process.artifacts)
}

// This is a bounded post-exec observation, not an atomic all-time FD snapshot.
func (s *LinuxProcessSampler) auditDescriptors(ctx context.Context, forbidden []linuxObjectIdentity, artifacts *launchArtifacts) (result error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != processSamplerOpen || artifacts == nil || artifacts.pin == nil {
		return ErrProcessObservation
	}
	before, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || before.ID != s.expected.Incarnation {
		return ErrProcessObservation
	}
	firstExe, err := s.verifyExecutable(ctx)
	if err != nil {
		return err
	}
	expected, err := linuxObject(int(artifacts.pin.Fd()))
	if err != nil {
		return err
	}
	fd, err := unix.Openat(s.directory, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrProcessObservation
	}
	file := os.NewFile(uintptr(fd), "bounded-target-fd-audit")
	defer func() { result = errors.Join(result, file.Close()) }()
	count := 0
	finalSeen := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, readErr := file.Readdirnames(128)
		for _, name := range names {
			count++
			if count > processDescriptorLimit {
				return ErrProcessObservation
			}
			if _, err := strconv.ParseUint(name, 10, 31); err != nil {
				return ErrProcessObservation
			}
			var st unix.Stat_t
			// Follow only the kernel proc link relative to the pinned fd directory.
			if unix.Fstatat(fd, name, &st, 0) != nil {
				return ErrProcessObservation
			}
			observed := linuxObjectIdentity{uint64(st.Dev), st.Ino, st.Mode & unix.S_IFMT}
			for _, denied := range forbidden {
				if observed == denied {
					return &linuxInheritedCapabilityError{object: observed}
				}
			}
			if name == "3" {
				if observed != expected {
					return ErrProcessObservation
				}
				finalSeen = true
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return ErrProcessObservation
		}
	}
	after, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	lastExe, exeErr := s.verifyExecutable(ctx)
	if !finalSeen || err != nil || after.ID != before.ID || exeErr != nil || lastExe != firstExe {
		return ErrProcessObservation
	}
	return ctx.Err()
}
