//go:build !windows

package node

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const k1FIFOReportErrorLimit = 1024
const k1FIFOReportSize = 12 + k1FIFOReportErrorLimit

func TestK1InitialTrustReadRejectsFIFO(t *testing.T) {
	const key = "CORSA_TEST_K1_TRUST_FIFO"
	if path := os.Getenv(key); path != "" {
		k1FIFOChild(t, path)
		return
	}
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	statusRead, statusWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var goRead, goWrite, output *os.File
	// Own the first pipe even when any subsequent acquisition fails.
	t.Cleanup(func() {
		for _, file := range []*os.File{statusRead, statusWrite, goRead, goWrite, output} {
			if file != nil {
				_ = file.Close()
			}
		}
	})
	goRead, goWrite, err = os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err = os.CreateTemp(t.TempDir(), "fifo-child-output-")
	if err != nil {
		t.Fatal(err)
	}
	// Overall containment is independent from the SAME two-second load
	// watchdog: startup5s + load2s + post-load exit2s + cleanup2s.
	ctx, cancel := context.WithTimeout(t.Context(), 11*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestK1InitialTrustReadRejectsFIFO$", "-test.v")
	command.Env = append(os.Environ(), key+"="+path)
	command.ExtraFiles = []*os.File{statusWrite, goRead}
	// Regular output avoids exec's pipe-copy goroutines extending Wait.
	command.Stdout, command.Stderr = output, output
	command.WaitDelay = 2 * time.Second
	waitDone := make(chan error, 1)
	started, joined := false, false
	ready, goSent, loadReturned := false, false, false
	phase := "startup"
	var waitErr error
	join := func(bound time.Duration) bool {
		if joined || !started {
			return true
		}
		timer := time.NewTimer(bound)
		defer timer.Stop()
		select {
		case waitErr = <-waitDone:
			joined = true
			return true
		case <-timer.C:
			return false
		}
	}
	t.Cleanup(func() {
		var killErr error
		if started && !joined {
			killErr = command.Process.Kill()
			if !join(2 * time.Second) {
				t.Error("FIFO child cleanup UNCONFIRMED: exact child did not reap")
			}
		}
		if started {
			state := "unavailable"
			if joined && command.ProcessState != nil {
				state = command.ProcessState.String()
			}
			t.Logf("FIFO child facts phase=%s pid=%d ready=%t goSent=%t loadReturned=%t joined=%t waitErr=%v state=%q killErr=%v", phase, command.Process.Pid, ready, goSent, loadReturned, joined, waitErr, state, killErr)
			if joined {
				prefix, readErr := io.ReadAll(io.NewSectionReader(output, 0, 64<<10))
				t.Logf("FIFO child output prefix(max64KiB), readErr=%v: %s", readErr, prefix)
			} else {
				t.Log("FIFO child output not read: child completion UNCONFIRMED")
			}
		}
	})
	startupDeadline := time.Now().Add(5 * time.Second)
	if err := statusRead.SetReadDeadline(startupDeadline); err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal("FIFO child Start failed", err)
	}
	started = true
	t.Logf("FIFO child started pid=%d", command.Process.Pid)
	// This is the sole exec.Wait owner on success and every failure path.
	go func() { waitDone <- command.Wait() }()
	if err := statusWrite.Close(); err != nil {
		t.Fatal(err)
	}
	if err := goRead.Close(); err != nil {
		t.Fatal(err)
	}
	var status [1]byte
	if _, err := io.ReadFull(statusRead, status[:]); err != nil || status[0] != 'R' {
		t.Fatalf("FIFO STARTUP_NOT_READY: status=%q err=%v", status, err)
	}
	if time.Now().After(startupDeadline) {
		t.Fatal("FIFO STARTUP_READY_LATE: READY observed after startup deadline")
	}
	ready, phase = true, "ready-go-setup"
	// Child cannot enter loadTrustStore before this GO; startup is excluded.
	operationDeadline := time.Now().Add(2 * time.Second)
	if err := goWrite.SetWriteDeadline(operationDeadline); err != nil {
		t.Fatal(err)
	}
	if err := statusRead.SetReadDeadline(operationDeadline); err != nil {
		t.Fatal(err)
	}
	if n, err := goWrite.Write([]byte{'G'}); n != 1 || err != nil {
		t.Fatalf("FIFO GO write failed: n=%d err=%v", n, err)
	}
	goSent = true
	if time.Now().After(operationDeadline) {
		t.Fatal("FIFO GO_SETUP_TIMEOUT: GO write completed after operation deadline")
	}
	if err := goWrite.Close(); err != nil {
		t.Fatal(err)
	}
	phase = "load-operation"
	var report [k1FIFOReportSize]byte
	if _, err := io.ReadFull(statusRead, report[:]); err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("FIFO LOAD_OPERATION_WATCHDOG: no complete load-return report observed after READY+GO within2s: %v", err)
		}
		t.Fatalf("FIFO LOAD_RETURN_PROTOCOL_FAILED: %v", err)
	}
	observedAt := time.Now()
	errorLen := int(binary.BigEndian.Uint16(report[10:12]))
	if report[0] != 1 || (report[1] != 'N' && report[1] != 'E') || errorLen == 0 || errorLen > k1FIFOReportErrorLimit {
		t.Fatal("FIFO MALFORMED_LOAD_REPORT: version/result/error length invalid")
	}
	for _, padding := range report[12+errorLen:] {
		if padding != 0 {
			t.Fatal("FIFO MALFORMED_LOAD_REPORT: nonzero padding")
		}
	}
	elapsedNanos := binary.BigEndian.Uint64(report[2:10])
	actualError := string(report[12 : 12+errorLen])
	loadReturned = true
	t.Logf("FIFO load report result=%c elapsedNanos=%d actualError=%q", report[1], elapsedNanos, actualError)
	if elapsedNanos > uint64(2*time.Second) {
		t.Fatal("FIFO LOAD_DURATION_EXCEEDED: child measured actual load beyond2s", elapsedNanos)
	}
	if observedAt.After(operationDeadline) {
		t.Fatal("FIFO LOAD_REPORT_LATE: complete report observed after operation deadline; not accepted as timely load")
	}
	if report[1] != 'N' || !strings.Contains(actualError, "trust store is not regular") {
		phase = "load-return-unexpected"
		actualExit := join(2 * time.Second)
		t.Fatalf("FIFO LOAD_RETURN_UNEXPECTED: result=%c error=%q joined=%t actualWait=%v", report[1], actualError, actualExit, waitErr)
	}
	phase = "post-load-exit"
	if !join(2 * time.Second) {
		t.Fatal("FIFO POST_LOAD_EXIT_TIMEOUT: nonregular load-return observed, child did not exit within2s")
	}
	if waitErr != nil {
		t.Fatal("FIFO child actual Wait failed after nonregular load-return", waitErr)
	}
	phase = "complete"
	// Never classify a late ctx.Err as the load outcome after actual Wait0.
}

// ExtraFiles3 is child->parent status, ExtraFiles4 is parent->child GO.
// Tokens are phase observations of this exact owned test child, not authority.
func k1FIFOChild(t *testing.T, path string) {
	t.Helper()
	status, goPipe := os.NewFile(3, "fifo-status"), os.NewFile(4, "fifo-go")
	if status == nil || goPipe == nil {
		t.Fatal("FIFO child private pipes unavailable")
	}
	defer func() { _ = status.Close() }()
	defer func() { _ = goPipe.Close() }()
	if n, err := status.Write([]byte{'R'}); n != 1 || err != nil {
		t.Fatalf("FIFO child READY failed: n=%d err=%v", n, err)
	}
	var goToken [1]byte
	if _, err := io.ReadFull(goPipe, goToken[:]); err != nil || goToken[0] != 'G' {
		t.Fatalf("FIFO child GO failed: token=%q err=%v", goToken, err)
	}
	operationStarted := time.Now()
	_, err := loadTrustStore(path, trustedContact{})
	operationElapsed := time.Since(operationStarted)
	expected := errors.Is(err, domain.ErrLocalContactIntent) && strings.Contains(err.Error(), "trust store is not regular")
	var report [k1FIFOReportSize]byte
	report[0], report[1] = 1, 'E'
	if expected && operationElapsed >= 0 {
		report[1] = 'N'
	}
	binary.BigEndian.PutUint64(report[2:10], uint64(operationElapsed))
	errorText := "<nil>"
	if err != nil {
		errorText = err.Error()
	}
	if len(errorText) > k1FIFOReportErrorLimit {
		report[1] = 'E'
		errorText = errorText[:k1FIFOReportErrorLimit-len(" [truncated]")] + " [truncated]"
	}
	binary.BigEndian.PutUint16(report[10:12], uint16(len(errorText)))
	copy(report[12:], errorText)
	if n, writeErr := status.Write(report[:]); n != len(report) || writeErr != nil {
		t.Fatalf("FIFO child load-return report failed: n=%d err=%v actualLoadError=%v", n, writeErr, err)
	}
	t.Logf("FIFO child load-return expectedNonregular=%t actualError=%v", expected, err)
	if !expected {
		t.Fatal("FIFO load did not return the specific nonregular rejection", err)
	}
}

func TestK1InitialTrustReadKeepsLegacySymlinkAndRejectsDirectory(t *testing.T) {
	directory := t.TempDir()
	regular := filepath.Join(directory, "regular")
	raw := []byte(`{"version":3,"contacts":{}}`)
	if err := os.WriteFile(regular, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "alias")
	if err := os.Symlink(regular, alias); err != nil {
		t.Fatal(err)
	}
	read, err := readInitialTrustFile(alias)
	if err != nil || string(read) != string(raw) {
		t.Fatalf("legacy symlink read: %s, %v", read, err)
	}
	if _, err := readInitialTrustFile(directory); err == nil {
		t.Fatal("directory accepted")
	}
}
