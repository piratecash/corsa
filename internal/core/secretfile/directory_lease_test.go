package secretfile

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const directoryLeaseChildMode = "CORSA_DIRECTORY_LEASE_TEST_MODE"
const directoryLeaseChildPath = "CORSA_DIRECTORY_LEASE_TEST_PATH"

func directoryLeaseTestOpen(t *testing.T, path string) (*Dir, *DirectoryLease, func()) {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	closeDir := sync.OnceFunc(func() {
		if err := d.Close(); err != nil {
			t.Error("caller Dir close", err)
		}
	})
	t.Cleanup(closeDir)
	lease, err := d.TryDirectoryLease()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		if !errors.Is(err, ErrDirectoryLeaseUnsupported) || lease != nil {
			t.Fatal("unsupported platform fell back", lease, err)
		}
		return d, nil, closeDir
	}
	if err != nil {
		t.Fatal("directory lease", err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Error("lease cleanup", err)
		}
	})
	return d, lease, closeDir
}

// This is the SAME parent test in its owned child mode, not an extra no-op
// helper test. CombinedOutput waits/reaps the sole direct child; the context
// and WaitDelay bound this test command's normal execution and pipe cleanup.
// These test-only times are not storage/admission/deployment deadlines.
func directoryLeaseContender(t *testing.T, path, mode, marker string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestDirectoryLeaseProcess$", "-test.v", "-test.timeout=4s")
	command.WaitDelay = time.Second
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, directoryLeaseChildMode+"=") && !strings.HasPrefix(item, directoryLeaseChildPath+"=") {
			command.Env = append(command.Env, item)
		}
	}
	command.Env = append(command.Env, directoryLeaseChildMode+"="+mode, directoryLeaseChildPath+"="+path)
	output, err := command.CombinedOutput()
	if err != nil || ctx.Err() != nil || !strings.Contains(string(output), marker) {
		t.Fatal("owned separate-process contender", mode, err, ctx.Err(), string(output))
	}
}

func TestDirectoryLeaseProcess(t *testing.T) {
	if mode := os.Getenv(directoryLeaseChildMode); mode != "" {
		d, err := Open(os.Getenv(directoryLeaseChildPath))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		lease, err := d.TryDirectoryLease()
		switch mode {
		case "busy":
			if lease != nil {
				_ = lease.Close()
				t.Fatal("contender acquired held directory inode")
			}
			if !errors.Is(err, ErrDirectoryLeaseBusy) {
				t.Fatal("wanted actual kernel BUSY", err)
			}
			t.Log("DIRECTORY_LEASE_BUSY")
		case "acquire":
			if err != nil {
				t.Fatal("contender could not acquire", err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			t.Log("DIRECTORY_LEASE_ACQUIRED")
		default:
			t.Fatal("unknown owned child mode")
		}
		return
	}
	parent := t.TempDir()
	a, b := filepath.Join(parent, "A"), filepath.Join(parent, "B")
	if err := os.Mkdir(a, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"original", "second"} {
		if err := os.WriteFile(filepath.Join(a, name), []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	d, lease, closeDir := directoryLeaseTestOpen(t, a)
	if lease == nil {
		return
	} // actual unsupported error asserted above
	if match, err := lease.Matches(d); err != nil || !match {
		t.Fatal("initial inode mismatch", match, err)
	}
	directoryLeaseContender(t, a, "busy", "DIRECTORY_LEASE_BUSY")

	// Independent scanner/preflight/Dir closes must not unlock the lease FD.
	scan, err := lease.OpenNameScan(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	directoryLeaseTrackScan(t, scan, nil)
	names, err := scan.Next()
	if err != nil || len(names) != 1 || (names[0] != "original" && names[0] != "second") {
		t.Fatal("name-only scan", names, err)
	}
	if !errors.Is(lease.Close(), ErrDirectoryLeaseInUse) {
		t.Fatal("lease closed an open scan")
	}
	if err := scan.Close(); err != nil || scan.Complete() {
		t.Fatal("early scan close certified completeness", err)
	}
	if err := d.DurablePreflight(); err != nil {
		t.Fatal("adjacent real directory sync", err)
	}
	unrelated, err := Open(a)
	if err != nil {
		t.Fatal(err)
	}
	if err := unrelated.Close(); err != nil {
		t.Fatal(err)
	}
	closeDir()
	directoryLeaseContender(t, a, "busy", "DIRECTORY_LEASE_BUSY")

	if err := os.Rename(a, b); err != nil {
		t.Fatal(err)
	}
	directoryLeaseContender(t, b, "busy", "DIRECTORY_LEASE_BUSY")
	if err := os.Mkdir(a, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a, "replacement"), []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement, err := Open(a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := replacement.Close(); err != nil {
			t.Error(err)
		}
	}()
	if match, err := lease.Matches(replacement); err != nil || match {
		t.Fatal("new ambient A impersonated bound old inode", match, err)
	}
	directoryLeaseContender(t, a, "acquire", "DIRECTORY_LEASE_ACQUIRED")
	directoryLeaseContender(t, b, "busy", "DIRECTORY_LEASE_BUSY")
	relocated, err := Open(b)
	if err != nil {
		t.Fatal(err)
	}
	if match, err := lease.Matches(relocated); err != nil || !match {
		t.Fatal("relocated same inode lost binding", match, err)
	}
	if err := relocated.Close(); err != nil {
		t.Fatal(err)
	}
	// Original caller Dir is closed and its old name is a different store.
	// Scan still uses the lease-owned pinned Root, without ambient Info calls.
	pinned, err := lease.OpenNameScan(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	directoryLeaseTrackScan(t, pinned, nil)
	got := directoryLeaseReadAll(t, pinned)
	if !reflect.DeepEqual(got, []string{"original", "second"}) {
		t.Fatal("scan followed replacement A", got)
	}
	directoryLeaseContender(t, b, "busy", "DIRECTORY_LEASE_BUSY")
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal("idempotent Close", err)
	}
	if _, err := lease.OpenNameScan(1, 2); !errors.Is(err, os.ErrClosed) {
		t.Fatal("closed lease opened scan", err)
	}
	if _, err := lease.Matches(replacement); !errors.Is(err, os.ErrClosed) {
		t.Fatal("closed lease matched store", err)
	}
	directoryLeaseContender(t, b, "acquire", "DIRECTORY_LEASE_ACQUIRED")
}

func directoryLeaseTrackScan(t *testing.T, scan *NameScan, expected error) {
	t.Helper()
	t.Cleanup(func() {
		if err := scan.Close(); err != nil && (expected == nil || !errors.Is(err, expected)) {
			t.Error("owned scan cleanup", err)
		}
	})
}

func directoryLeaseReadAll(t *testing.T, scan *NameScan) []string {
	t.Helper()
	// Cleanup joins any synchronous Next before it releases the scan/lease.
	t.Cleanup(func() {
		if err := scan.Close(); err != nil {
			t.Error("scan cleanup", err)
		}
	})
	var names []string
	for {
		batch, err := scan.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal("bounded names", err)
		}
		if len(batch) > scan.batch {
			t.Fatal("batch cap exceeded")
		}
		names = append(names, batch...)
	}
	if scan.Complete() {
		t.Fatal("EOF without checked close certified scan")
	}
	if err := scan.Close(); err != nil {
		t.Fatal(err)
	}
	if !scan.Complete() {
		t.Fatal("EOF+close not complete")
	}
	sort.Strings(names)
	return names
}

func directoryLeaseNameFixture(t *testing.T) (*DirectoryLease, func()) {
	t.Helper()
	path := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(path, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, lease, closeDir := directoryLeaseTestOpen(t, path)
	return lease, closeDir
}

func TestDirectoryNameScanBounds(t *testing.T) {
	t.Run("complete-eof", func(t *testing.T) {
		lease, _ := directoryLeaseNameFixture(t)
		if lease == nil {
			return
		}
		scan, err := lease.OpenNameScan(1, 3)
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, scan, nil)
		if _, err := lease.OpenNameScan(1, 3); !errors.Is(err, ErrDirectoryLeaseInUse) {
			t.Fatal("parallel scan bypassed structural lane", err)
		}
		names := directoryLeaseReadAll(t, scan)
		if !reflect.DeepEqual(names, []string{"a", "b", "c"}) {
			t.Fatal(names)
		}
		if _, err := scan.Next(); !errors.Is(err, io.EOF) {
			t.Fatal("complete scan terminal", err)
		}
		if err := scan.Close(); err != nil {
			t.Fatal(err)
		}
		second, err := lease.OpenNameScan(2, 3)
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, second, nil)
		if err := scan.Close(); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(lease.Close(), ErrDirectoryLeaseInUse) {
			t.Fatal("stale scanner close released current scanner")
		}
		if names := directoryLeaseReadAll(t, second); !reflect.DeepEqual(names, []string{"a", "b", "c"}) {
			t.Fatal(names)
		}
		// Exact empty EOF, no lock leaf exists in this directory.
		_, empty, _ := directoryLeaseTestOpen(t, t.TempDir())
		if empty == nil {
			return
		}
		es, err := empty.OpenNameScan(1, 1)
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, es, nil)
		if batch, err := es.Next(); len(batch) != 0 || !errors.Is(err, io.EOF) || es.Complete() {
			t.Fatal("empty EOF", batch, err)
		}
		if err := es.Close(); err != nil || !es.Complete() {
			t.Fatal("empty checked completion", err)
		}
	})
	t.Run("cap-plus-one", func(t *testing.T) {
		lease, _ := directoryLeaseNameFixture(t)
		if lease == nil {
			return
		}
		for _, batch := range []int{2, 4} {
			scan, err := lease.OpenNameScan(batch, 2)
			if err != nil {
				t.Fatal(err)
			}
			directoryLeaseTrackScan(t, scan, ErrNameScanLimit)
			names, err := scan.Next()
			if batch == 2 {
				if err != nil || len(names) != 2 {
					t.Fatal("prefix fixture", names, err)
				}
				names, err = scan.Next()
			}
			if !errors.Is(err, ErrNameScanLimit) || len(names) != 0 || scan.Complete() {
				t.Fatal("cap+1 certified partial batch", batch, names, err)
			}
			if _, err := scan.Next(); !errors.Is(err, ErrNameScanLimit) {
				t.Fatal("overflow was not sticky", err)
			}
			if !errors.Is(lease.Close(), ErrDirectoryLeaseInUse) {
				t.Fatal("overflow discarded scan ownership")
			}
			if err := scan.Close(); !errors.Is(err, ErrNameScanLimit) || scan.Complete() {
				t.Fatal("overflow close", err)
			}
		}
	})
	t.Run("invalid-overflow", func(t *testing.T) {
		lease, _ := directoryLeaseNameFixture(t)
		if lease == nil {
			return
		}
		for _, limits := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}, {int(^uint(0) >> 1), 1}} {
			if scan, err := lease.OpenNameScan(limits[0], limits[1]); scan != nil || !errors.Is(err, ErrNameScanLimits) {
				t.Fatal("invalid limits allocated scanner", limits, scan, err)
			}
		}
		// Large total cursor with tiny batch does not compute maxNames+1.
		scan, err := lease.OpenNameScan(1, int(^uint(0)>>1))
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, scan, nil)
		if names := directoryLeaseReadAll(t, scan); len(names) != 3 {
			t.Fatal(names)
		}
	})
	t.Run("io-error-early-close", func(t *testing.T) {
		lease, _ := directoryLeaseNameFixture(t)
		if lease == nil {
			return
		}
		scan, err := lease.OpenNameScan(1, 3)
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, scan, os.ErrClosed)
		// Deliberate FD-close fault, not a real disk/powerloss injection.
		if err := scan.file.Close(); err != nil {
			t.Fatal(err)
		}
		// Readdirnames may expose a platform-specific closing error rather
		// than os.ErrClosed. Measure the original closed-file operation;
		// the bounded API must retain that exact cause, not just any error.
		_, referenceErr := scan.file.Readdirnames(1)
		var referencePathError *os.PathError
		if !errors.As(referenceErr, &referencePathError) || referencePathError.Err == nil || errors.Is(referenceErr, io.EOF) {
			t.Fatal("closed-file reference did not fail as a filesystem read", referenceErr)
		}
		names, readErr := scan.Next()
		if len(names) != 0 || !errors.Is(readErr, referencePathError.Err) {
			t.Fatal("bounded read lost the original closed-file cause", names, readErr, referenceErr)
		}
		if scan.Complete() {
			t.Fatal("failed read certified complete")
		}
		if names, err := scan.Next(); len(names) != 0 || !errors.Is(err, readErr) || !errors.Is(err, referencePathError.Err) || scan.Complete() {
			t.Fatal("closed-file read failure was not sticky", names, err)
		}
		if err := scan.Close(); !errors.Is(err, readErr) || !errors.Is(err, referencePathError.Err) || !errors.Is(err, os.ErrClosed) || scan.Complete() {
			t.Fatal("read/close fault hidden", err)
		}
		early, err := lease.OpenNameScan(1, 3)
		if err != nil {
			t.Fatal(err)
		}
		directoryLeaseTrackScan(t, early, nil)
		if _, err := early.Next(); err != nil {
			t.Fatal(err)
		}
		if err := early.Close(); err != nil || early.Complete() {
			t.Fatal("early close certified prefix", err)
		}
		if _, err := early.Next(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("early closed scan resumed", err)
		}
	})
}

func TestDirectoryLeaseCloseError(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	lease, err := d.TryDirectoryLease()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		if !errors.Is(err, ErrDirectoryLeaseUnsupported) || lease != nil {
			t.Fatal("unsupported fallback", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() }) // expected injected close error below
	// Force the Go FD to be closed first. A failed Close cannot silently say
	// authority is held/released or retry a possibly recycled descriptor.
	if err := lease.lock.Close(); err != nil {
		t.Fatal(err)
	}
	first := lease.Close()
	if !errors.Is(first, os.ErrClosed) {
		t.Fatal("close ambiguity hidden", first)
	}
	second := lease.Close()
	if !errors.Is(second, os.ErrClosed) || first != second {
		t.Fatal("close error was not cached", first, second)
	}
	if _, err := lease.OpenNameScan(1, 1); !errors.Is(err, os.ErrClosed) {
		t.Fatal("uncertain close reused lease", err)
	}
	if _, err := lease.Matches(d); !errors.Is(err, os.ErrClosed) {
		t.Fatal("uncertain close claimed identity", err)
	}
}
