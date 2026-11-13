package capture

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxCaptureStoredBytes   int64 = 512 << 20
	maxCaptureStoredFiles         = 1024
	maxCaptureStoredEntries       = 2 * maxCaptureStoredFiles
	minCaptureFreeBytes           = 64 << 20
	captureCreateHeadroom         = 64 << 10
)

var (
	errCaptureStorageLimit = errors.New("capture storage limit reached")
	errCaptureFreeSpace    = errors.New("capture free space reserve reached")
)

// captureStorage accounts for one manager's files. Existing files are counted
// before the first writer opens. While writers are open, their reservations
// are the source of truth. Starting again after all writers close refreshes
// the count, so the operator can remove or archive completed captures.
type captureStorage struct {
	mu       sync.Mutex
	baseDir  string
	free     func(string) (uint64, error)
	active   int
	inFlight int64
	stored   captureInventory
}

type captureInventory struct {
	dirs  map[string]struct{}
	files int
	bytes int64
}

func (s *captureStorage) create(path string) (Writer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == 0 {
		stored, err := scanCaptureStorage(s.baseDir)
		if err != nil {
			return nil, err
		}
		s.stored = stored
	}
	dir := filepath.Dir(path)
	_, dirKnown := s.stored.dirs[dir]
	needed := 1
	if !dirKnown {
		needed++
	}
	if s.stored.files >= maxCaptureStoredFiles || s.stored.bytes >= maxCaptureStoredBytes ||
		needed > maxCaptureStoredEntries-s.stored.files-len(s.stored.dirs) {
		return nil, errCaptureStorageLimit
	}
	existing, err := captureExistingDirectory(dir)
	if err != nil {
		return nil, err
	}
	if err := s.checkFreeSpace(existing, captureCreateHeadroom); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("capture directory: %w", err)
	}
	s.stored.dirs[dir] = struct{}{}
	w, err := openCaptureFile(path)
	if err != nil {
		return nil, err
	}
	w.storage = s
	s.active++
	s.stored.files++
	return w, nil
}

func (s *captureStorage) reserve(dir string, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > maxCaptureStoredBytes-s.stored.bytes {
		return errCaptureStorageLimit
	}
	if err := s.checkFreeSpace(dir, size); err != nil {
		return err
	}
	s.stored.bytes += size
	s.inFlight += size
	return nil
}

func (s *captureStorage) finish(size, written int64) {
	s.mu.Lock()
	s.inFlight -= size
	s.stored.bytes -= size - written
	s.mu.Unlock()
}

// checkFreeSpace runs under mu. Bytes reserved by writes that have not
// returned may not yet appear in the filesystem's free-space count.
func (s *captureStorage) checkFreeSpace(dir string, size int64) error {
	free, err := s.free(dir)
	if err != nil {
		return fmt.Errorf("capture free space check: %w", err)
	}
	if free < minCaptureFreeBytes+uint64(s.inFlight)+uint64(size) {
		return errCaptureFreeSpace
	}
	return nil
}

func captureExistingDirectory(dir string) (string, error) {
	for {
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return "", errors.New("capture path is not a directory")
			}
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if !errors.Is(err, os.ErrNotExist) || parent == dir {
			return "", err
		}
		dir = parent
	}
}

func (s *captureStorage) writerClosed() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

func scanCaptureStorage(base string) (captureInventory, error) {
	stored := captureInventory{dirs: make(map[string]struct{})}
	info, err := os.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return stored, nil
	}
	if err != nil {
		return stored, err
	}
	if !info.IsDir() {
		return stored, errors.New("capture storage root must be a directory")
	}
	countFile := func(entry os.DirEntry) error {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("unexpected entry in capture storage")
		}
		if stored.files >= maxCaptureStoredFiles || stored.files+len(stored.dirs) >= maxCaptureStoredEntries ||
			info.Size() > maxCaptureStoredBytes-stored.bytes {
			return errCaptureStorageLimit
		}
		stored.files++
		stored.bytes += info.Size()
		return nil
	}
	err = readCaptureEntries(base, func(entry os.DirEntry) error {
		if !entry.IsDir() {
			return countFile(entry)
		}
		if stored.files+len(stored.dirs) >= maxCaptureStoredEntries {
			return errCaptureStorageLimit
		}
		dir := filepath.Join(base, entry.Name())
		stored.dirs[dir] = struct{}{}
		return readCaptureEntries(dir, countFile)
	})
	return stored, err
}

// Read in bounded batches so a large pre-existing directory cannot allocate
// a slice proportional to its complete inventory.
func readCaptureEntries(path string, visit func(os.DirEntry) error) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	for {
		entries, err := dir.ReadDir(64)
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return io.ErrNoProgress
		}
	}
}
