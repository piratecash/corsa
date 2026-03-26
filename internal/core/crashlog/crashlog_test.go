package crashlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoverAndLogWritesCrashFile(t *testing.T) {
	dir := t.TempDir()

	// Simulate a panic caught by recoverAndLog.
	func() {
		defer func() {
			// Catch the re-panic from recoverAndLog.
			r := recover()
			if r == nil {
				t.Fatal("expected re-panic from recoverAndLog")
			}
		}()

		defer recoverAndLog(dir)
		panic("test crash value")
	}()

	// Verify a crash file was created.
	matches, err := filepath.Glob(filepath.Join(dir, crashPrefix+"*.log"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 crash file, got %d", len(matches))
	}

	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read crash file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "test crash value") {
		t.Fatalf("crash file missing panic value, got:\n%s", content)
	}
	if !strings.Contains(content, "Stack trace:") {
		t.Fatalf("crash file missing stack trace, got:\n%s", content)
	}
}

func TestRecoverAndLogNoopWhenNoPanic(t *testing.T) {
	dir := t.TempDir()

	// Should not panic when there's nothing to recover.
	recoverAndLog(dir)

	matches, _ := filepath.Glob(filepath.Join(dir, crashPrefix+"*.log"))
	if len(matches) != 0 {
		t.Fatalf("expected no crash files, got %d", len(matches))
	}
}

func TestCleanOldCrashLogs(t *testing.T) {
	dir := t.TempDir()

	// Create 15 crash files with sequential timestamps.
	for i := 0; i < 15; i++ {
		name := filepath.Join(dir, fmt.Sprintf("%s20260101-%06d.log", crashPrefix, i))
		if err := os.WriteFile(name, []byte("crash"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	cleanOldCrashLogs(dir)

	remaining, _ := filepath.Glob(filepath.Join(dir, crashPrefix+"*.log"))
	if len(remaining) != keepCrashLogs {
		t.Fatalf("expected %d crash files after cleanup, got %d", keepCrashLogs, len(remaining))
	}
}

func TestSetupLogsStartupEvent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)

	cleanup := Setup()
	defer cleanup()

	// Read the log file and verify startup messages are present (zerolog JSON format).
	logPath := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "logging initialised") {
		t.Fatalf("log file missing 'logging initialised' message, got:\n%s", content)
	}
	if !strings.Contains(content, "application started") {
		t.Fatalf("log file missing 'application started' message, got:\n%s", content)
	}
}

func TestRedirectStderrCreatesFile(t *testing.T) {
	dir := t.TempDir()

	f := redirectStderr(dir)
	if f == nil {
		t.Fatal("redirectStderr returned nil")
	}
	defer func() { _ = f.Close() }()

	// Write to stderr — should go to the file now.
	fmt.Fprintln(os.Stderr, "test stderr capture")

	stderrPath := filepath.Join(dir, stderrFileName)
	data, err := os.ReadFile(stderrPath)
	if err != nil {
		t.Fatalf("read stderr file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "stderr redirected") {
		t.Fatalf("stderr file missing redirect header, got:\n%s", content)
	}
	if !strings.Contains(content, "test stderr capture") {
		t.Fatalf("stderr file missing test output, got:\n%s", content)
	}
}

func TestSetupCreatesStderrLog(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)

	cleanup := Setup()
	defer cleanup()

	stderrPath := filepath.Join(dir, stderrFileName)
	if _, err := os.Stat(stderrPath); os.IsNotExist(err) {
		t.Fatal("Setup() should create stderr.log")
	}
}

func TestRotateIfNeeded(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, logFileName)

	// Create a file smaller than maxLogSize — should not rotate.
	if err := os.WriteFile(logPath, []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotateIfNeeded(logPath)
	if _, err := os.Stat(logPath); err != nil {
		t.Fatal("file should still exist after no-op rotation")
	}

	// Create a file larger than maxLogSize — should rotate.
	big := make([]byte, maxLogSize+1)
	if err := os.WriteFile(logPath, big, 0o600); err != nil {
		t.Fatal(err)
	}
	rotateIfNeeded(logPath)

	// Original file should be gone (renamed).
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatal("original file should have been renamed")
	}

	// Rotated file should exist.
	matches, _ := filepath.Glob(logPath + ".*")
	if len(matches) != 1 {
		t.Fatalf("expected 1 rotated file, got %d", len(matches))
	}
}
