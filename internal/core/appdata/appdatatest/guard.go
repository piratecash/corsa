// Package appdatatest holds the test-side guard of the default data
// directory: under `go test` appdata.DefaultDir is the relative `.corsa` in
// the package's working directory, and nothing a package's tests do may
// write there.
package appdatatest

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/appdata"
)

// maxReported caps how many offending paths the guard lists; the count is
// always reported in full.
const maxReported = 20

// RunGuardingDefaultDir runs the package's tests and fails the run when any
// of them created or modified anything under the default data directory —
// `.corsa` in the package directory under `go test`. A test that does is
// writing outside its t.TempDir(): its node was built without a data
// directory of its own (config/configtest.WithTempDataDirs is the fix).
//
// What it checks: a directory that did not exist before the run and exists
// after it, and every entry under it — files and directories alike — whose
// modification time is at or after the start of the run, plus every entry
// that vanished while the check was reading it. A new file in an existing
// subdirectory shows twice (the file, and the directory whose entry list
// changed); rewriting an existing file shows through its own time. A
// directory created and removed again within the run leaves nothing to see
// and is not reported. Nothing is created, changed or removed: files left by
// earlier runs stay where they are and, being older than this run, are not
// reported.
//
// The start is rounded down to the second: on file systems that keep
// modification times to the second (HFS+, FAT, some network file systems) a
// write in the run's first second would otherwise carry a time before the
// start and pass unseen. The boundary is therefore conservative — a write
// made in the same second just before the run is reported too, which is
// preferred over missing one made inside it.
//
// What a report means: during the run something wrote or removed entries
// there. Once no test writes there, that is a regression — in this run, or in
// another `go test` of the same package running in the same directory at the
// same time, which sees the same directory — or someone removing old files
// by hand or from an IDE while the run was going.
//
// The directory is resolved to an absolute path before the tests run, so a
// test that changes the working directory and does not change it back cannot
// send the check somewhere else.
//
// Use from TestMain: os.Exit(appdatatest.RunGuardingDefaultDir(m)).
func RunGuardingDefaultDir(m *testing.M) int {
	dir, err := filepath.Abs(appdata.DefaultDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "appdatatest: cannot resolve the default data directory: %v\n", err)
		return 1
	}
	_, statErr := os.Stat(dir)
	existedBefore := statErr == nil
	since := time.Now().Truncate(time.Second)

	code := m.Run()

	written, err := writtenSince(dir, since, existedBefore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "appdatatest: cannot check %s: %v\n", dir, err)
		return failed(code)
	}
	if len(written) == 0 {
		return code
	}
	fmt.Fprintf(os.Stderr, "FAIL: tests wrote or removed %d entries under the default data directory %s instead of using their t.TempDir():\n", len(written), dir)
	for i, path := range written {
		if i == maxReported {
			fmt.Fprintf(os.Stderr, "  … and %d more\n", len(written)-maxReported)
			break
		}
		fmt.Fprintf(os.Stderr, "  %s\n", path)
	}
	return failed(code)
}

// writtenSince lists every entry under dir modified at or after since, every
// entry that vanished while it was being read, and dir itself when the run
// created it.
func writtenSince(dir string, since time.Time, existedBefore bool) ([]string, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil
	}
	var written []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			written = append(written, path+" (removed during the run)")
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if os.IsNotExist(err) {
			written = append(written, path+" (removed during the run)")
			return nil
		}
		if err != nil {
			return err
		}
		created := path == dir && !existedBefore
		if created || !info.ModTime().Before(since) {
			written = append(written, path)
		}
		return nil
	})
	return written, err
}

func failed(code int) int {
	if code != 0 {
		return code
	}
	return 1
}
