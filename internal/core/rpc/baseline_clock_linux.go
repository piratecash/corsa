package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	baselineChronyc = "/usr/bin/chronyc"
	baselineSocket  = "/run/chrony/chronyd.sock"
)

type baselineClockMark struct {
	MonotonicBeforeNS int64 `json:"monotonic_before_ns"`
	BootBeforeNS      int64 `json:"boot_before_ns"`
	RealtimeNS        int64 `json:"realtime_ns"`
	BootAfterNS       int64 `json:"boot_after_ns"`
	MonotonicAfterNS  int64 `json:"monotonic_after_ns"`
}

type baselineClockDomain struct {
	BootID        string `json:"boot_id"`
	TimeNamespace string `json:"time_namespace"`
	SocketDevice  uint64 `json:"socket_device"`
	SocketInode   uint64 `json:"socket_inode"`
	SocketUID     uint32 `json:"socket_uid"`
}

type baselineClockReport struct {
	Status        string               `json:"status"`
	Reason        string               `json:"reason,omitempty"`
	Domain        *baselineClockDomain `json:"domain,omitempty"`
	Before        *baselineClockMark   `json:"before,omitempty"`
	After         *baselineClockMark   `json:"after,omitempty"`
	ChronycSHA256 string               `json:"chronyc_sha256,omitempty"`
	TrackingCSV   string               `json:"tracking_csv,omitempty"`
}

func registerPlatformBaselineClock(t *CommandTable, info CommandInfo) {
	// A caller cannot start another local subprocess while a sample is in progress.
	busy := make(chan struct{}, 1)
	t.Register(info, func(req CommandRequest) CommandResponse {
		if len(req.Args) != 0 {
			return validationError(errors.New("getBaselineClock takes no arguments"))
		}
		if response, done := ctxDone(req); done {
			return response
		}
		select {
		case busy <- struct{}{}:
			defer func() { <-busy }()
		default:
			return jsonResponse(baselineClockReport{Status: "unavailable", Reason: "sample_in_progress"})
		}
		return jsonResponse(captureBaselineClock(requestContext(req)))
	})
}

func captureBaselineClock(ctx context.Context) baselineClockReport {
	unavailable := func(reason string) baselineClockReport {
		return baselineClockReport{Status: "unavailable", Reason: reason}
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	domain, reason := readBaselineClockDomain()
	if reason != "" {
		return unavailable(reason)
	}
	digest, err := baselineChronycDigest()
	if err != nil {
		return unavailable("chronyc_binary_unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, baselineChronyc, "-n", "-c", "-h", baselineSocket, "tracking")
	cmd.Env = []string{"LC_ALL=C", "PATH=/usr/bin:/bin"}
	cmd.WaitDelay = 250 * time.Millisecond
	var stdout baselineClockOutput
	cmd.Stdout = &stdout
	// Only stdout is evidence. Discard diagnostics that can contain local paths.
	var stderr baselineClockOutput
	cmd.Stderr = &stderr
	before, err := readBaselineClockMark()
	if err != nil {
		return unavailable("clock_read_failed")
	}
	if err := cmd.Run(); err != nil || stderr.buffer.Len() != 0 {
		return unavailable("chrony_tracking_unavailable")
	}
	after, err := readBaselineClockMark()
	if err != nil {
		return unavailable("clock_read_failed")
	}
	domainAfter, reason := readBaselineClockDomain()
	if reason != "" || domainAfter != domain {
		return unavailable("clock_domain_changed")
	}
	digestAfter, err := baselineChronycDigest()
	if err != nil || digestAfter != digest {
		return unavailable("chronyc_binary_changed")
	}
	raw := stdout.buffer.String()
	if !strings.HasSuffix(raw, "\n") || strings.Count(raw, "\n") != 1 || strings.Count(raw, ",") != 13 {
		return unavailable("unexpected_chrony_tracking_format")
	}
	return baselineClockReport{Status: "sample", Domain: &domain, Before: &before,
		After: &after, ChronycSHA256: digest, TrackingCSV: raw}
}

type baselineClockOutput struct{ buffer bytes.Buffer }

func (output *baselineClockOutput) Write(raw []byte) (int, error) {
	if len(raw) > 4096-output.buffer.Len() {
		return 0, errors.New("chrony output exceeds limit")
	}
	return output.buffer.Write(raw)
}

func baselineChronycDigest() (string, error) {
	fd, err := unix.Open(baselineChronyc, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	file := os.NewFile(uintptr(fd), baselineChronyc)
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	const maxBytes = 4 * 1024 * 1024
	if !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Uid != 0 ||
		info.Mode().Perm()&0o022 != 0 || info.Size() <= 0 || info.Size() > maxBytes {
		return "", errors.New("unexpected chronyc file")
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(file, maxBytes+1))
	if err != nil || size != info.Size() {
		return "", errors.New("incomplete chronyc read")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func readBaselineClockDomain() (baselineClockDomain, string) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return baselineClockDomain{}, "boot_id_unavailable"
	}
	namespace, err := os.Readlink("/proc/thread-self/ns/time")
	if err != nil {
		return baselineClockDomain{}, "time_namespace_unavailable"
	}
	info, err := os.Lstat(baselineSocket)
	if errors.Is(err, os.ErrNotExist) {
		return baselineClockDomain{}, "chrony_socket_missing"
	}
	if err != nil {
		return baselineClockDomain{}, "chrony_socket_unavailable"
	}
	if info.Mode()&os.ModeSocket == 0 {
		return baselineClockDomain{}, "chrony_endpoint_not_socket"
	}
	stat := info.Sys().(*syscall.Stat_t)
	return baselineClockDomain{strings.TrimSpace(string(boot)), namespace, uint64(stat.Dev), stat.Ino, stat.Uid}, ""
}

func readBaselineClockMark() (baselineClockMark, error) {
	var mark baselineClockMark
	reads := []struct {
		clock int32
		value *int64
	}{
		{unix.CLOCK_MONOTONIC, &mark.MonotonicBeforeNS},
		{unix.CLOCK_BOOTTIME, &mark.BootBeforeNS},
		{unix.CLOCK_REALTIME, &mark.RealtimeNS},
		{unix.CLOCK_BOOTTIME, &mark.BootAfterNS},
		{unix.CLOCK_MONOTONIC, &mark.MonotonicAfterNS},
	}
	for _, read := range reads {
		var ts unix.Timespec
		if err := unix.ClockGettime(read.clock, &ts); err != nil {
			return baselineClockMark{}, err
		}
		*read.value = ts.Nano()
	}
	return mark, nil
}
