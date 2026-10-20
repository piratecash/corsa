//go:build linux

package loadstand

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
)

// Only the actual locked worker captures this diagnostic. It grants no work.
type linuxThreadIdentity struct {
	tid   int
	start ProcessTicks
}

func captureLinuxThread(ctx context.Context) (linuxThreadIdentity, error) {
	tid := unix.Gettid()
	root, dir, err := openProcessDirectory(ProcessID(tid))
	if err != nil {
		return linuxThreadIdentity{}, err
	}
	raw, readErr := readProcFile(ctx, dir, "stat")
	closeErr := errors.Join(unix.Close(dir), unix.Close(root))
	parsed, parseErr := parseProcessStat(raw)
	if readErr != nil || closeErr != nil || parseErr != nil || parsed.ID.PID != ProcessID(tid) || parsed.ID.StartTicks == 0 {
		return linuxThreadIdentity{}, ErrProcessObservation
	}
	return linuxThreadIdentity{tid, parsed.ID.StartTicks}, nil
}
