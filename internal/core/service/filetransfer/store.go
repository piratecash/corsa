package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"corsa/internal/core/domain"
)

// FileStore manages content-addressed file storage in the transmit directory.
// Files are stored as <sha256>.<ext> where sha256 is the hex-encoded hash of
// the file content. This enables automatic deduplication: sending the same
// file to multiple recipients only stores it once.
//
// Concurrency: all methods are safe for concurrent use.
type FileStore struct {
	mu      sync.RWMutex
	baseDir string
	refs    map[string]int // sha256 hash → reference count
	pending map[string]int // sha256 hash → in-flight announce count (pre-Acquire)
}

// NewFileStore creates a file store rooted at the given directory.
// The directory is created if it does not exist.
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return nil, fmt.Errorf("create transmit dir %s: %w", baseDir, err)
	}
	return &FileStore{
		baseDir: baseDir,
		refs:    make(map[string]int),
		pending: make(map[string]int),
	}, nil
}

// StoreFile copies a source file into the transmit directory with content-addressed naming.
// Returns the SHA-256 hash of the file content.
//
// Deduplication is keyed on the content hash alone. If the same bytes are
// stored twice with different source extensions (e.g. .pdf and .txt), only
// the first physical copy is kept and its extension becomes canonical. The
// ref count tracks how many senders reference that hash — Release deletes
// the single canonical file when the count reaches zero.
func (fs *FileStore) StoreFile(sourcePath string) (string, error) {
	hash, err := hashFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("hash source file: %w", err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Already stored (any extension): increment ref count only.
	// This prevents creating duplicate physical files for the same content
	// under different extensions.
	if _, err := fs.resolvePathLocked(hash); err == nil {
		fs.refs[hash]++
		return hash, nil
	}

	ext := filepath.Ext(sourcePath)
	dstName := hash + ext
	dstPath := filepath.Join(fs.baseDir, dstName)

	if err := copyFile(sourcePath, dstPath, hash); err != nil {
		return "", fmt.Errorf("copy file to transmit: %w", err)
	}

	fs.refs[hash]++
	return hash, nil
}

// EnsureStored copies the source file into the transmit directory with
// content-addressed naming (like StoreFile) but does NOT increment the ref
// count. The caller is expected to go through the Prepare → Commit lifecycle
// which calls Acquire to take ownership. Between EnsureStored and Acquire,
// the pending counter (set by ReservePendingIfExists inside PrepareFileAnnounce)
// protects the blob from RemoveUnreferenced.
//
// This is the correct entry point for the desktop attach flow: the file must
// exist on disk for ReservePendingIfExists to find it, but the ownership ref
// belongs to the sender mapping created by Commit, not to the attach step.
func (fs *FileStore) EnsureStored(sourcePath string) (string, error) {
	hash, err := hashFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("hash source file: %w", err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Already stored (any extension): return hash, no ref increment.
	if _, err := fs.resolvePathLocked(hash); err == nil {
		return hash, nil
	}

	ext := filepath.Ext(sourcePath)
	dstName := hash + ext
	dstPath := filepath.Join(fs.baseDir, dstName)

	if err := copyFile(sourcePath, dstPath, hash); err != nil {
		return "", fmt.Errorf("copy file to transmit: %w", err)
	}

	return hash, nil
}

// ReadChunk reads a chunk of data from the stored file at the given offset.
func (fs *FileStore) ReadChunk(fileHash string, offset uint64, size uint32) ([]byte, error) {
	path, err := fs.resolvePath(fileHash)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open transmit file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}

	buf := make([]byte, size)
	n, err := io.ReadFull(f, buf)
	if errors.Is(err, io.ErrUnexpectedEOF) || err == io.EOF {
		return buf[:n], nil // last chunk may be shorter
	}
	if err != nil {
		return nil, fmt.Errorf("read chunk at offset %d: %w", offset, err)
	}
	return buf[:n], nil
}

// FileSize returns the size of the stored file in bytes.
func (fs *FileStore) FileSize(fileHash string) (uint64, error) {
	path, err := fs.resolvePath(fileHash)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat transmit file: %w", err)
	}
	return uint64(info.Size()), nil
}

// Acquire increments the ref count for an existing file in the store.
// Returns an error if the file does not exist on disk. This is used when
// a new sender mapping references a file that was copied into the transmit
// directory externally (e.g. by the desktop file attach flow).
func (fs *FileStore) Acquire(fileHash string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, err := fs.resolvePathLocked(fileHash); err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	fs.refs[fileHash]++
	return nil
}

// ReservePending increments the pending-announce counter for a file hash.
// Must be called synchronously before spawning the goroutine that will
// eventually call Acquire. This prevents RemoveUnreferenced from deleting
// the transmit blob while another announce for the same hash is still
// in flight between pre-validation and Acquire.
func (fs *FileStore) ReservePending(fileHash string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.pending[fileHash]++
}

// ReservePendingIfExists atomically checks that the transmit blob exists on
// disk and increments the pending-announce counter in a single critical
// section. Returns true if the file was found and the reservation was placed,
// false otherwise. This eliminates the TOCTOU window between a separate
// HasFile check and a later ReservePending call, where a concurrent Rollback
// could delete the blob between the two steps.
func (fs *FileStore) ReservePendingIfExists(fileHash string) bool {
	if domain.ValidateFileHash(fileHash) != nil {
		return false
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	path, err := fs.resolvePathLocked(fileHash)
	if err != nil {
		return false
	}
	if _, err := os.Stat(path); err != nil {
		return false
	}

	fs.pending[fileHash]++
	return true
}

// ReleasePending decrements the pending-announce counter without acquiring
// a committed ref. Called on failure paths where Acquire was never reached
// (send error, nil response) and on the success path after Acquire has
// already incremented refs.
func (fs *FileStore) ReleasePending(fileHash string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.pending[fileHash] > 0 {
		fs.pending[fileHash]--
		if fs.pending[fileHash] == 0 {
			delete(fs.pending, fileHash)
		}
	}
}

// RemoveUnreferenced deletes files for the given hash only if no active
// sender mapping holds a reference and no announce goroutine has a pending
// reservation. This is used to clean up transmit blobs copied by the desktop
// attach flow when the file_announce DM or sender registration fails before
// Acquire is called. If another sender has already acquired a ref or
// reserved a pending announce for the same hash (concurrent send of
// identical content), the file is left intact.
func (fs *FileStore) RemoveUnreferenced(fileHash string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.refs[fileHash] > 0 || fs.pending[fileHash] > 0 {
		return
	}
	delete(fs.refs, fileHash)
	delete(fs.pending, fileHash)
	fs.removeAllForHash(fileHash)
}

// Release decrements the ref count for a file hash. When ref count reaches
// zero AND no pending announce reservation exists, all files matching the
// hash are deleted from the transmit directory. If a pending reservation
// is active (another announce for the same content is between
// PrepareFileAnnounce and Commit), the blob is preserved — the pending
// goroutine will either Acquire a new ref or Rollback and clean up itself.
// Removing all matches (not just the first glob result) handles orphaned
// duplicates that may exist from earlier versions where the same content
// could be stored under multiple extensions.
func (fs *FileStore) Release(fileHash string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.refs[fileHash]--
	if fs.refs[fileHash] <= 0 {
		delete(fs.refs, fileHash)
		if fs.pending[fileHash] > 0 {
			return
		}
		fs.removeAllForHash(fileHash)
	}
}

// removeAllForHash deletes every file matching <hash>.* inside the transmit
// directory. Must be called with fs.mu held.
func (fs *FileStore) removeAllForHash(fileHash string) {
	if domain.ValidateFileHash(fileHash) != nil {
		return
	}
	pattern := filepath.Join(fs.baseDir, fileHash+".*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	for _, path := range matches {
		if err := ensureWithinDir(fs.baseDir, path); err != nil {
			continue
		}
		_ = os.Remove(path)
	}
}

// HasFile returns true if the file with the given hash exists in the store.
func (fs *FileStore) HasFile(fileHash string) bool {
	if domain.ValidateFileHash(fileHash) != nil {
		return false
	}
	path, err := fs.resolvePath(fileHash)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// resolvePath finds the transmit file for a given hash (any extension).
func (fs *FileStore) resolvePath(fileHash string) (string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.resolvePathLocked(fileHash)
}

// resolvePathLocked finds the file path without acquiring a lock.
// Must be called with fs.mu held (read or write).
func (fs *FileStore) resolvePathLocked(fileHash string) (string, error) {
	// Validate hash is strict hex to prevent glob injection (*, ?, [)
	// and path traversal (../) through crafted hash values.
	if err := domain.ValidateFileHash(fileHash); err != nil {
		return "", fmt.Errorf("resolve transmit path: %w", err)
	}

	// Glob for <hash>.* to find the file regardless of extension.
	pattern := filepath.Join(fs.baseDir, fileHash+".*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("glob transmit files: %w", err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("transmit file not found for hash %s", fileHash)
	}

	// Verify the resolved path stays within the transmit directory.
	if err := ensureWithinDir(fs.baseDir, matches[0]); err != nil {
		return "", fmt.Errorf("resolve transmit path: %w", err)
	}

	return matches[0], nil
}

// ValidateOnStartup rebuilds ref counts from the persisted file mapping
// state. Blob cleanup is NOT performed here — transmit blobs are deleted
// only when identity is removed (CleanupPeerTransfers) or the sender
// mapping is explicitly removed (RemoveSenderMapping).
func (fs *FileStore) ValidateOnStartup(activeHashes map[string]int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Set ref counts from active mappings. Skip invalid hashes to prevent
	// injection from corrupted persistence files.
	for hash, count := range activeHashes {
		if domain.ValidateFileHash(hash) != nil {
			continue
		}
		fs.refs[hash] = count
	}
}

// ensureWithinDir verifies that the resolved path is contained within the
// expected base directory. This prevents path traversal attacks where a
// crafted file name could escape the intended directory.
func ensureWithinDir(baseDir, resolvedPath string) error {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return fmt.Errorf("resolve base dir: %w", err)
	}
	absResolved, err := filepath.Abs(resolvedPath)
	if err != nil {
		return fmt.Errorf("resolve target path: %w", err)
	}

	// Evaluate symlinks to prevent symlink-based escapes. On macOS the temp
	// directory is a symlink (/var → /private/var), so base and child paths
	// must be resolved through the same symlink chain. When the child path
	// does not exist yet (e.g. a file about to be created), EvalSymlinks
	// fails; in that case resolve the closest existing ancestor and
	// re-append the remaining suffix so both paths share the same real prefix.
	if realBase, err := filepath.EvalSymlinks(absBase); err == nil {
		absBase = realBase
	}
	if realResolved, err := filepath.EvalSymlinks(absResolved); err == nil {
		absResolved = realResolved
	} else {
		// Walk up from absResolved to find the deepest existing ancestor,
		// resolve its symlinks, then re-append the non-existent tail.
		// This handles paths like /var/.../base/subdir/file.txt where
		// neither subdir nor file.txt exist yet.
		absResolved = evalSymlinksPartial(absResolved)
	}

	// The resolved path must be within the base directory.
	rel, err := filepath.Rel(absBase, absResolved)
	if err != nil {
		return fmt.Errorf("path escapes base directory")
	}
	if strings.HasPrefix(rel, "..") {
		return fmt.Errorf("path escapes base directory: %s", rel)
	}
	return nil
}

// evalSymlinksPartial resolves symlinks for as much of the path as exists on
// disk. Starting from the full path it walks upward until it finds an ancestor
// that exists, resolves that ancestor through EvalSymlinks, then re-appends the
// non-existent tail segments. This is needed because filepath.EvalSymlinks
// fails on paths where any component does not exist, but on macOS the temp
// directory prefix (/var → /private/var) must be resolved for correct prefix
// comparison.
func evalSymlinksPartial(p string) string {
	// Collect non-existent tail segments.
	var tail []string
	current := p
	for {
		if real, err := filepath.EvalSymlinks(current); err == nil {
			// Found an existing ancestor — rebuild with resolved prefix.
			result := real
			for i := len(tail) - 1; i >= 0; i-- {
				result = filepath.Join(result, tail[i])
			}
			return result
		}
		parent := filepath.Dir(current)
		if parent == current {
			// Reached filesystem root without finding an existing path.
			break
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
	return p
}

// verifyNotSymlink checks that the file at path is not a symlink and that the
// open file descriptor f refers to the same file. This defends against two
// attack vectors:
//
//  1. Pre-existing symlink: Lstat reports ModeSymlink — rejected immediately.
//  2. TOCTOU race: attacker replaces the regular file with a symlink between
//     OpenFile and this check. Lstat sees the new symlink while Fstat (via
//     f.Stat()) sees the symlink target. os.SameFile compares device+inode,
//     detecting the mismatch.
//
// Must be called after os.OpenFile and before any writes.
func verifyNotSymlink(path string, f *os.File) error {
	// Lstat does NOT follow symlinks — if path is a symlink, Mode includes
	// ModeSymlink.
	linfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat: %w", err)
	}
	if linfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path is a symlink: %s", path)
	}

	// Stat the open fd (always resolves to the actual file, never a symlink).
	finfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("fstat: %w", err)
	}

	// os.SameFile compares device and inode — works cross-platform.
	if !os.SameFile(linfo, finfo) {
		return fmt.Errorf("file identity mismatch (possible TOCTOU race): %s", path)
	}

	return nil
}

// hashFile computes the SHA-256 hash of a file's content.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	return hashReader(f)
}

// hashReader computes the SHA-256 hash from an io.Reader.
// Used both by hashFile (path-based) and verifyPartialIntegrity (fd-based)
// to avoid duplicating hashing logic.
func hashReader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// verifyPartialIntegrity opens the partial file, verifies it is not a symlink
// via fd-level identity checks, and computes a SHA-256 hash — all from the
// same file descriptor. This eliminates the TOCTOU window that exists when
// checking the path and then reopening it for hashing: a local attacker cannot
// swap the file between the symlink check and the hash computation because
// both operate on the same already-open fd.
//
// Returns the os.FileInfo obtained via Fstat on the verified fd. The caller
// should use this with verifyFileIdentity before any subsequent path-based
// operation (e.g. os.Rename) to ensure the file was not swapped after the
// fd was closed.
func verifyPartialIntegrity(path string, expectedHash string) (os.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open partial file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Verify the open fd points to the same regular file as the path.
	// If a local attacker replaced the file with a symlink between the
	// directory listing and our Open call, Lstat sees the symlink while
	// Fstat sees the target — verifyNotSymlink detects this mismatch.
	if err := verifyNotSymlink(path, f); err != nil {
		return nil, fmt.Errorf("partial file symlink check: %w", err)
	}

	// Capture fd identity before hashing — the caller can later compare
	// this with a fresh Lstat to detect post-verification swaps.
	finfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("fstat partial file: %w", err)
	}

	actual, err := hashReader(f)
	if err != nil {
		return nil, fmt.Errorf("compute file hash: %w", err)
	}
	if actual != expectedHash {
		return nil, fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actual)
	}
	return finfo, nil
}

// verifyFileIdentity checks that the file at path still has the same
// device+inode as the previously captured os.FileInfo. This closes the
// TOCTOU window between verifyPartialIntegrity (which closes its fd) and
// a subsequent path-based operation like os.Rename: if a local attacker
// swaps the file in that gap, the inode will differ and this returns an error.
func verifyFileIdentity(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat for identity check: %w", err)
	}
	if current.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path is now a symlink: %s", path)
	}
	if !os.SameFile(expected, current) {
		return fmt.Errorf("file identity changed (possible TOCTOU race): %s", path)
	}
	return nil
}

// copyFile copies src to dst atomically: write to tmp → fsync → rename.
// The fsync before rename ensures durability: if the process crashes or
// power is lost right after rename, the content-addressed blob is fully
// written on disk — not a zero-length or truncated file carrying a valid
// SHA-256 filename.
//
// expectedHash is the SHA-256 hash that the caller computed from a prior
// read of the source file. copyFile computes the hash of the bytes it
// actually copies (via TeeReader, zero extra I/O) and compares the two.
// If the source file changed between the caller's hashFile and this copy,
// the mismatch is detected, the temp file is removed, and an error is
// returned. This closes the TOCTOU window where hashing and copying read
// the source file in two separate passes.
func copyFile(src, dst, expectedHash string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	h := sha256.New()
	tee := io.TeeReader(in, h)

	if _, err := io.Copy(out, tee); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	if actualHash != expectedHash {
		_ = os.Remove(tmp)
		return fmt.Errorf("source file changed during copy: expected hash %s, got %s", expectedHash, actualHash)
	}

	return os.Rename(tmp, dst)
}

// partialDownloadPath returns the path for a partially downloaded file.
// The fileID is sanitised to prevent path traversal.
func partialDownloadPath(baseDir string, fileID domain.FileID) string {
	// FileID is a UUID-like value, but sanitise to be safe.
	safe := domain.SanitizeFileName(string(fileID) + ".part")
	return filepath.Join(baseDir, "partial", safe)
}

// hashPrefix returns the first 6 hex characters of a SHA-256 hash.
// Used to build content-addressable suffixes for download file names.
func hashPrefix(fileHash string) string {
	if len(fileHash) >= 6 {
		return fileHash[:6]
	}
	return fileHash
}

// resolveExistingDownload locates a completed download on disk given the
// original file name and expected SHA-256 hash.
//
// Lookup order:
//  1. Exact name — if it exists and hash matches, return it.
//  2. Hash-suffixed name "base (hash6).ext" — deterministic, created by
//     the current completedDownloadPath when exact name was occupied.
//  3. Legacy numeric suffixes "base (1).ext", "base (2).ext" — for files
//     saved before the hash-suffix scheme was introduced. Each candidate
//     is verified by computing its SHA-256.
//
// If expectedHash is empty, hash verification is skipped (legacy mode).
// Returns the path of the first verified match, or empty string.
func resolveExistingDownload(baseDir, fileName, expectedHash string) string {
	// Sanitize the file name to prevent path traversal.
	fileName = domain.SanitizeFileName(fileName)

	verifyHash := expectedHash != ""

	// 1. Exact name.
	exact := filepath.Join(baseDir, fileName)
	if _, err := os.Stat(exact); err == nil {
		if !verifyHash {
			return exact
		}
		if h, err := hashFile(exact); err == nil && h == expectedHash {
			return exact
		}
	}

	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)

	// 2. Hash-suffixed name (current scheme).
	if verifyHash {
		hashName := fmt.Sprintf("%s (%s)%s", base, hashPrefix(expectedHash), ext)
		hashPath := filepath.Join(baseDir, hashName)
		if _, err := os.Stat(hashPath); err == nil {
			if h, err := hashFile(hashPath); err == nil && h == expectedHash {
				return hashPath
			}
		}
	}

	// 3. Legacy scan: "base (" prefix, ")" + ext suffix.
	prefix := base + " ("
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ")"+ext) {
			candidate := filepath.Join(baseDir, name)
			if !verifyHash {
				return candidate
			}
			if h, err := hashFile(candidate); err == nil && h == expectedHash {
				return candidate
			}
		}
	}
	return ""
}

// completedDownloadPath returns a unique path for the completed download.
//
// If the exact file name is free, it is used as-is. If another file already
// occupies that name, the hash of the existing file is compared with
// fileHash. If they match (same content, same transfer), the existing path
// is returned — no duplicate is created. If they differ (name collision from
// a different transfer), a content-addressable suffix is appended:
//
//	"report (a1b2c3).pdf"   — first 6 hex chars of fileHash
//
// This makes the mapping between hash and file name deterministic, so
// resolveExistingDownload can locate it without a full directory scan.
//
// If fileHash is empty, falls back to sequential numeric suffixes for
// backward compatibility.
func completedDownloadPath(baseDir, fileName, fileHash string) string {
	// Sanitize the file name to prevent path traversal.
	fileName = domain.SanitizeFileName(fileName)

	candidate := filepath.Join(baseDir, fileName)
	if _, err := os.Stat(candidate); os.IsNotExist(err) {
		return candidate
	}

	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)

	// If hash is available, use content-addressable suffix.
	if fileHash != "" {
		// Check whether the existing file is the same content (idempotent).
		if h, err := hashFile(candidate); err == nil && h == fileHash {
			return candidate
		}

		hashSuffix := hashPrefix(fileHash)
		candidate = filepath.Join(baseDir, fmt.Sprintf("%s (%s)%s", base, hashSuffix, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
		// Hash-suffixed name exists — verify it's ours.
		if h, err := hashFile(candidate); err == nil && h == fileHash {
			return candidate
		}
		// Extremely unlikely hash-prefix collision: fall through to numeric.
	}

	// Legacy / fallback: sequential numeric suffix.
	for i := 1; i < 1000; i++ {
		candidate = filepath.Join(baseDir, fmt.Sprintf("%s (%d)%s", base, i, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
	return filepath.Join(baseDir, fmt.Sprintf("%s (%d)%s", base, time.Now().UnixNano(), ext))
}
