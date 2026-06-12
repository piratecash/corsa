package crashlog

import (
	"bytes"
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

	// Read the log file and verify startup messages are present.
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

func TestShrinkLogIfNeededKeepsTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, logFileName)

	// File smaller than maxLogSize — untouched.
	if err := os.WriteFile(logPath, []byte("small"), 0o600); err != nil {
		t.Fatal(err)
	}
	shrinkLogIfNeeded(logPath)
	data, err := os.ReadFile(logPath)
	if err != nil || string(data) != "small" {
		t.Fatalf("small file must not be shrunk, got %q err=%v", data, err)
	}

	// File larger than maxLogSize — shrunk in place to the tail.
	line := []byte("0123456789012345678901234567890123456789012345678901234567890\n")
	big := bytes.Repeat(line, maxLogSize/len(line)+2)
	lastLine := []byte("LAST LINE MARKER\n")
	big = append(big, lastLine...)
	if err := os.WriteFile(logPath, big, 0o600); err != nil {
		t.Fatal(err)
	}
	shrinkLogIfNeeded(logPath)

	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal("log file must still exist after shrink")
	}
	if info.Size() > shrinkKeepSize {
		t.Fatalf("shrunk file size %d exceeds keep limit %d", info.Size(), shrinkKeepSize)
	}

	data, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(data, lastLine) {
		t.Fatal("shrunk log must preserve the most recent lines")
	}
	if !bytes.HasPrefix(data, line) {
		t.Fatalf("shrunk log must start at a line boundary, got prefix %q", data[:16])
	}

	// No rotated copies are created.
	matches, _ := filepath.Glob(logPath + ".*")
	if len(matches) != 0 {
		t.Fatalf("shrink must not create rotated copies, got %v", matches)
	}
}

func TestRemoveLegacyRotatedLogs(t *testing.T) {
	dir := t.TempDir()

	legacy := []string{
		logFileName + ".20250101-000000",
		logFileName + ".20260101-000000",
		stderrFileName + ".20260101-000000",
	}
	keep := []string{logFileName, stderrFileName, crashPrefix + "20260101-000000.log"}
	for _, name := range append(append([]string{}, legacy...), keep...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	removeLegacyRotatedLogs(dir)

	for _, name := range legacy {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("legacy rotated log %s must be removed", name)
		}
	}
	for _, name := range keep {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("file %s must survive migration: %v", name, err)
		}
	}
}

func TestFileLogFormatDefaultsToConsole(t *testing.T) {
	t.Setenv(envLogFormat, "")
	if got := fileLogFormat(); got != logFormatConsole {
		t.Fatalf("unset %s must default to console, got %q", envLogFormat, got)
	}

	t.Setenv(envLogFormat, "garbage")
	if got := fileLogFormat(); got != logFormatConsole {
		t.Fatalf("unknown %s must fall back to console, got %q", envLogFormat, got)
	}

	t.Setenv(envLogFormat, "JSON")
	if got := fileLogFormat(); got != logFormatJSON {
		t.Fatalf("%s=JSON must select json, got %q", envLogFormat, got)
	}
}

func TestSetupWritesConsoleFormatByDefault(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)
	t.Setenv(envLogFormat, "")

	cleanup := Setup()
	defer cleanup()

	data, err := os.ReadFile(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if strings.Contains(string(data), `"message":"logging initialised"`) {
		t.Fatal("default file format must be console, found JSON records")
	}
	if !strings.Contains(string(data), "logging initialised") {
		t.Fatalf("log file missing startup banner, got:\n%s", data)
	}
}

func TestSetupWritesJSONWhenRequested(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)
	t.Setenv(envLogFormat, "json")

	cleanup := Setup()
	defer cleanup()

	data, err := os.ReadFile(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), `"message":"logging initialised"`) {
		t.Fatalf("%s=json must produce JSON records, got:\n%s", envLogFormat, data)
	}
}

func TestSetupRemovesLegacyRotatedLogs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", dir)

	legacyPath := filepath.Join(dir, logFileName+".20250101-000000")
	if err := os.WriteFile(legacyPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	cleanup := Setup()
	defer cleanup()

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatal("Setup() must remove legacy corsa.log.* copies")
	}
}
