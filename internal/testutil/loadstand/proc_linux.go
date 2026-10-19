//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const processFileLimit = 256 << 10
const processDescriptorLimit = 65536
const auxClockTicks = 17 // Linux UAPI include/uapi/linux/auxvec.h AT_CLKTCK.

type processSamplerState uint8

const (
	processSamplerUninitialised processSamplerState = iota
	processSamplerOpen
	processSamplerClosed
)

type LinuxProcessSampler struct {
	mu              sync.Mutex
	root, directory int
	expected        LinuxProcessExpectation
	state           processSamplerState
}
type processExecutableID struct {
	device, inode uint64
	size          int64
	mtime, ctime  unix.Timespec
}

// ReadLinuxIncarnation observes a PID in this helper's proc/PID namespace. It
// does not prove launch ownership: a future launcher must retain its child
// lifecycle while obtaining and passing this binding to OpenLinuxProcess.
func ReadLinuxIncarnation(ctx context.Context, pid ProcessID, uid ProcessUID) (incarnation ProcessIncarnation, resultErr error) {
	if pid <= 0 || uid == 0 || ProcessUID(os.Getuid()) != uid || ProcessUID(os.Geteuid()) != uid {
		return ProcessIncarnation{}, ErrProcessObservation
	}
	root, dir, err := openProcessDirectory(pid)
	if err != nil {
		return ProcessIncarnation{}, ErrProcessObservation
	}
	defer func() {
		if errors.Join(unix.Close(dir), unix.Close(root)) != nil {
			incarnation = ProcessIncarnation{}
			resultErr = ErrProcessObservation
		}
	}()
	first, _, err := readProcessIdentity(ctx, dir, pid, uid)
	if err != nil {
		return ProcessIncarnation{}, err
	}
	last, _, err := readProcessIdentity(ctx, dir, pid, uid)
	if err != nil || last.ID != first.ID {
		return ProcessIncarnation{}, ErrProcessObservation
	}
	return first.ID, nil
}
func OpenLinuxProcess(ctx context.Context, expected LinuxProcessExpectation) (*LinuxProcessSampler, error) {
	digest, err := hex.DecodeString(string(expected.ExecutableSHA256))
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != string(expected.ExecutableSHA256) || expected.Incarnation.PID <= 0 || expected.Incarnation.StartTicks == 0 || expected.UID == 0 || expected.MaxExecutableBytes <= 0 || expected.MaxExecutableBytes > 128<<20 || ProcessUID(os.Getuid()) != expected.UID || ProcessUID(os.Geteuid()) != expected.UID {
		return nil, ErrInvalidConfiguration
	}
	root, dir, err := openProcessDirectory(expected.Incarnation.PID)
	if err != nil {
		return nil, ErrProcessObservation
	}
	sampler := &LinuxProcessSampler{root: root, directory: dir, expected: expected, state: processSamplerOpen}
	if _, err = sampler.Sample(ctx); err != nil {
		_ = sampler.Close()
		return nil, err
	}
	return sampler, nil
}
func (s *LinuxProcessSampler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != processSamplerOpen {
		return nil
	}
	s.state = processSamplerClosed
	return errors.Join(unix.Close(s.directory), unix.Close(s.root))
}
func (s *LinuxProcessSampler) Sample(ctx context.Context) (LinuxProcessSample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != processSamplerOpen {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	if err := ctx.Err(); err != nil {
		return LinuxProcessSample{}, err
	}
	started := time.Now().UTC()
	before, status, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || before.ID != s.expected.Incarnation {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	firstExe, err := s.verifyExecutable(ctx)
	if err != nil {
		return LinuxProcessSample{}, err
	}
	memory := parseProcessMemory(status)
	descriptors := readProcessDescriptors(ctx, s.directory)
	scheduler := unavailableProcessScheduler(ProcessUnreadable)
	enabledBefore := schedulerEnabled(ctx, s.root)
	if raw, e := readProcFile(ctx, s.directory, "schedstat"); e == nil {
		scheduler = parseProcessScheduler(raw, enabledBefore && schedulerEnabled(ctx, s.root))
	}
	after, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || after.ID != before.ID || after.User < before.User || after.System < before.System {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	lastExe, err := s.verifyExecutable(ctx)
	if err != nil || lastExe != firstExe {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	final, _, err := readProcessIdentity(ctx, s.directory, s.expected.Incarnation.PID, s.expected.UID)
	if err != nil || final.ID != before.ID || final.User < after.User || final.System < after.System {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	after = final
	if err = ctx.Err(); err != nil {
		return LinuxProcessSample{}, err
	}
	completed := time.Now().UTC()
	if completed.Before(started) {
		return LinuxProcessSample{}, ErrProcessObservation
	}
	hz := linuxClockFrequency()
	return LinuxProcessSample{Provenance: LinuxProcessProvenance{Incarnation: after.ID, UID: s.expected.UID, RunningExecutableSHA256: s.expected.ExecutableSHA256, RunningExecutableVerified: true, KernelStartTimeAvailable: true}, ReadStartedAt: started, ReadCompletedAt: completed, CPU: ProcessCPU{UserTicks: after.User, SystemTicks: after.System, Frequency: hz, UserNanoseconds: processTickDuration(after.User, hz), SystemNanoseconds: processTickDuration(after.System, hz)}, Memory: memory, Descriptors: descriptors, Scheduler: scheduler, FinalWait4: unavailableProcessMetric[ProcessFinalUsage](ProcessNotWaitOwner)}, nil
}
func openProcessDirectory(pid ProcessID) (int, int, error) {
	root, err := unix.Open("/proc", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, -1, err
	}
	fail := func(err error) (int, int, error) { _ = unix.Close(root); return -1, -1, err }
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstatfs(root, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC || unix.Fstat(root, &st) != nil || st.Uid != 0 || st.Mode&022 != 0 {
		return fail(ErrProcessObservation)
	}
	name := make([]byte, 32)
	n, err := unix.Readlinkat(root, "self", name)
	if err != nil || string(name[:n]) != strconv.Itoa(os.Getpid()) {
		return fail(ErrProcessObservation)
	}
	dir, err := unix.Openat(root, strconv.Itoa(int(pid)), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	if unix.Fstatfs(dir, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		_ = unix.Close(dir)
		return fail(ErrProcessObservation)
	}
	return root, dir, nil
}
func readProcessIdentity(ctx context.Context, dir int, pid ProcessID, uid ProcessUID) (processStat, []byte, error) {
	raw, err := readProcFile(ctx, dir, "stat")
	if err != nil {
		return processStat{}, nil, ErrProcessObservation
	}
	stat, err := parseProcessStat(raw)
	if err != nil || stat.ID.PID != pid {
		return processStat{}, nil, ErrProcessObservation
	}
	status, err := readProcFile(ctx, dir, "status")
	if err != nil || !processUIDMatches(status, uid) {
		return processStat{}, nil, ErrProcessObservation
	}
	return stat, status, nil
}
func readProcFile(ctx context.Context, dir int, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrProcessObservation
	}
	file := os.NewFile(uintptr(fd), "proc-observation")
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC || unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		return nil, ErrProcessObservation
	}
	raw, readErr := readProcessBounded(ctx, file, processFileLimit)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, ErrProcessObservation
	}
	return raw, nil
}

// Context is checked between reads. A kernel-blocked read cannot be cancelled
// by this API; the enclosing owned-container lifecycle remains the hard fence.
func readProcessBounded(ctx context.Context, file *os.File, limit int64) ([]byte, error) {
	var out bytes.Buffer
	buffer := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			if int64(out.Len())+int64(n) > limit {
				return nil, ErrProcessObservation
			}
			_, _ = out.Write(buffer[:n])
		}
		if err == io.EOF {
			return out.Bytes(), nil
		}
		if err != nil {
			return nil, ErrProcessObservation
		}
		if n == 0 {
			return nil, ErrProcessObservation
		}
	}
}
func (s *LinuxProcessSampler) verifyExecutable(ctx context.Context) (processExecutableID, error) {
	if err := ctx.Err(); err != nil {
		return processExecutableID{}, err
	}
	// exe is the kernel-owned proc magic link. Its target is opened directly;
	// its pathname/readlink text is never reopened as an executable.
	fd, err := unix.Openat(s.directory, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return processExecutableID{}, ErrProcessObservation
	}
	file := os.NewFile(uintptr(fd), "running-executable")
	before, err := processExecutableIdentity(fd)
	if err != nil || before.size <= 0 || before.size > s.expected.MaxExecutableBytes {
		_ = file.Close()
		return processExecutableID{}, ErrProcessObservation
	}
	hash := sha256.New()
	header := make([]byte, 4)
	if _, err = io.ReadFull(file, header); err != nil || !bytes.Equal(header, []byte{0x7f, 'E', 'L', 'F'}) {
		_ = file.Close()
		return processExecutableID{}, ErrProcessObservation
	}
	_, _ = hash.Write(header)
	buffer := make([]byte, 32<<10)
	total := int64(4)
	for {
		if err = ctx.Err(); err != nil {
			_ = file.Close()
			return processExecutableID{}, err
		}
		n, e := file.Read(buffer)
		if n > 0 {
			total += int64(n)
			if total > s.expected.MaxExecutableBytes {
				_ = file.Close()
				return processExecutableID{}, ErrProcessObservation
			}
			_, _ = hash.Write(buffer[:n])
		}
		if e == io.EOF {
			break
		}
		if e != nil || n == 0 {
			_ = file.Close()
			return processExecutableID{}, ErrProcessObservation
		}
	}
	after, e := processExecutableIdentity(fd)
	closeErr := file.Close()
	if e != nil || closeErr != nil || before != after || total != before.size || hex.EncodeToString(hash.Sum(nil)) != string(s.expected.ExecutableSHA256) {
		return processExecutableID{}, ErrProcessObservation
	}
	return before, nil
}
func processExecutableIdentity(fd int) (processExecutableID, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return processExecutableID{}, ErrProcessObservation
	}
	return processExecutableID{device: uint64(st.Dev), inode: st.Ino, size: st.Size, mtime: st.Mtim, ctime: st.Ctim}, nil
}
func readProcessDescriptors(ctx context.Context, dir int) ProcessMetric[ProcessFDCount] {
	fd, err := unix.Openat(dir, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return unavailableProcessMetric[ProcessFDCount](ProcessUnreadable)
	}
	file := os.NewFile(uintptr(fd), "process-descriptors")
	var count ProcessFDCount
	for {
		if ctx.Err() != nil {
			_ = file.Close()
			return unavailableProcessMetric[ProcessFDCount](ProcessUnreadable)
		}
		names, e := file.Readdirnames(128)
		for _, name := range names {
			if _, err := strconv.ParseUint(name, 10, 31); err != nil {
				_ = file.Close()
				return unavailableProcessMetric[ProcessFDCount](ProcessMalformed)
			}
			count++
			if count > processDescriptorLimit {
				_ = file.Close()
				return unavailableProcessMetric[ProcessFDCount](ProcessLimitExceeded)
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			_ = file.Close()
			return unavailableProcessMetric[ProcessFDCount](ProcessUnreadable)
		}
	}
	if file.Close() != nil {
		return unavailableProcessMetric[ProcessFDCount](ProcessUnreadable)
	}
	return measuredProcessMetric(count)
}
func schedulerEnabled(ctx context.Context, root int) bool {
	raw, err := readProcFile(ctx, root, "sys/kernel/sched_schedstats")
	return err == nil && strings.TrimSpace(string(raw)) == "1"
}
func linuxClockFrequency() ProcessMetric[ProcessTickHz] {
	entries, err := unix.Auxv()
	if err != nil {
		return unavailableProcessMetric[ProcessTickHz](ProcessClockUnknown)
	}
	var hz ProcessTickHz
	for _, entry := range entries {
		if entry[0] != auxClockTicks {
			continue
		}
		if hz != 0 || entry[1] == 0 {
			return unavailableProcessMetric[ProcessTickHz](ProcessClockUnknown)
		}
		hz = ProcessTickHz(entry[1])
	}
	if hz == 0 {
		return unavailableProcessMetric[ProcessTickHz](ProcessClockUnknown)
	}
	return measuredProcessMetric(hz)
}
