package filetransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Path traversal in completedDownloadPath — vulnerability #1
// ---------------------------------------------------------------------------

func TestCompletedDownloadPathTraversalBlocked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// An attacker sends a file_announce with a traversal file name.
	malicious := "../../../etc/cron.d/evil-rule"
	hash := sha256Hex([]byte("payload"))

	path := completedDownloadPath(dir, malicious, hash)

	// The result must stay within dir.
	if !strings.HasPrefix(path, dir) {
		t.Fatalf("completedDownloadPath escaped base dir: %q", path)
	}
	// Must not contain traversal sequences.
	if strings.Contains(path, "..") {
		t.Fatalf("completedDownloadPath contains ..: %q", path)
	}
}

func TestCompletedDownloadPathTraversalVariants(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	hash := sha256Hex([]byte("payload"))

	cases := []string{
		"../secret.txt",
		"../../etc/passwd",
		"../../../tmp/exploit.sh",
		"..\\..\\Windows\\System32\\evil.dll",
		"/etc/shadow",
		"foo/../../../bar.txt",
	}

	for _, malicious := range cases {
		path := completedDownloadPath(dir, malicious, hash)
		if !strings.HasPrefix(path, dir) {
			t.Errorf("completedDownloadPath(%q) escaped: %q", malicious, path)
		}
	}
}

// ---------------------------------------------------------------------------
// Path traversal in resolveExistingDownload — vulnerability #2
// ---------------------------------------------------------------------------

func TestResolveExistingDownloadTraversalBlocked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Even if an attacker somehow got a traversal name into transfers JSON,
	// resolveExistingDownload must not escape the downloads directory.
	malicious := "../../../etc/passwd"
	got := resolveExistingDownload(dir, malicious, "")
	if got != "" && !strings.HasPrefix(got, dir) {
		t.Fatalf("resolveExistingDownload escaped dir: %q", got)
	}
}

// ---------------------------------------------------------------------------
// Hash validation in resolvePath — vulnerability #3 (glob injection)
// ---------------------------------------------------------------------------

func TestResolvePathRejectsGlobCharsInHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file that the glob might match.
	if err := os.WriteFile(filepath.Join(dir, "abcdef.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Attempt to use glob wildcards in the hash.
	_, err = fs.resolvePath("*")
	if err == nil {
		t.Fatal("resolvePath should reject glob chars")
	}

	_, err = fs.resolvePath("abc???")
	if err == nil {
		t.Fatal("resolvePath should reject short/non-hex hashes")
	}
}

func TestResolvePathRejectsTraversalInHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.resolvePath("../../../etc/passwd/aaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err == nil {
		t.Fatal("resolvePath should reject path traversal in hash")
	}
}

// ---------------------------------------------------------------------------
// ensureWithinDir tests — the core containment check
// ---------------------------------------------------------------------------

func TestEnsureWithinDirAllows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	child := filepath.Join(dir, "subdir", "file.txt")
	if err := ensureWithinDir(dir, child); err != nil {
		t.Fatalf("should allow child path: %v", err)
	}
}

func TestEnsureWithinDirBlocksTraversal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	escaped := filepath.Join(dir, "..", "escaped.txt")
	if err := ensureWithinDir(dir, escaped); err == nil {
		t.Fatal("should reject traversal path")
	}
}

func TestEnsureWithinDirBlocksAbsoluteEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := ensureWithinDir(dir, "/etc/passwd"); err == nil {
		t.Fatal("should reject absolute escape")
	}
}

// ---------------------------------------------------------------------------
// partialDownloadPath sanitisation
// ---------------------------------------------------------------------------

func TestPartialDownloadPathTraversalBlocked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// An attacker injects a fileID with path traversal.
	maliciousID := domain.FileID("../../../etc/cron.d/evil")
	path := partialDownloadPath(dir, maliciousID)

	if strings.Contains(path, "..") {
		t.Fatalf("partialDownloadPath contains ..: %q", path)
	}
	if !strings.HasPrefix(path, filepath.Join(dir, "partial")) {
		t.Fatalf("partialDownloadPath escaped: %q", path)
	}
}

// ---------------------------------------------------------------------------
// HasFile rejects invalid hashes
// ---------------------------------------------------------------------------

func TestHasFileRejectsInvalidHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file that looks like a match for a wildcard.
	if err := os.WriteFile(filepath.Join(dir, "anything.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	if fs.HasFile("*") {
		t.Fatal("HasFile should reject wildcard hash")
	}
	if fs.HasFile("../../../etc/passwd") {
		t.Fatal("HasFile should reject traversal hash")
	}
}

// ---------------------------------------------------------------------------
// ValidateOnStartup skips invalid hashes
// ---------------------------------------------------------------------------

func TestValidateOnStartupSkipsInvalidHashes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create some files.
	validHash := sha256HexStr([]byte("content"))
	if err := os.WriteFile(filepath.Join(dir, validHash+".txt"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "invalid.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Simulate corrupted persistence with invalid hash values.
	activeHashes := map[string]int{
		validHash:             1,
		"../../../etc/passwd": 1, // traversal attempt
		"*":                   1, // glob injection attempt
		"short":               1, // too short
	}

	fs.ValidateOnStartup(activeHashes)

	// Only the valid hash should be in refs.
	if fs.refs[validHash] != 1 {
		t.Fatalf("valid hash should have ref count 1, got %d", fs.refs[validHash])
	}
	if _, ok := fs.refs["../../../etc/passwd"]; ok {
		t.Fatal("traversal hash should not be in refs")
	}
	if _, ok := fs.refs["*"]; ok {
		t.Fatal("glob hash should not be in refs")
	}
}

// ---------------------------------------------------------------------------
// End-to-end: malicious file announce → safe download
// ---------------------------------------------------------------------------

func TestMaliciousFileNameSanitizedAtRegistration(t *testing.T) {
	t.Parallel()

	// Simulate what happens when RegisterFileReceive gets a malicious name.
	malicious := "../../../etc/cron.d/evil-rule"
	safe := domain.SanitizeFileName(malicious)

	if strings.ContainsAny(safe, "/\\") {
		t.Fatalf("sanitized name contains separators: %q", safe)
	}
	if strings.Contains(safe, "..") {
		t.Fatalf("sanitized name contains ..: %q", safe)
	}
	if safe == "" {
		t.Fatal("sanitized name should not be empty")
	}
}

// ---------------------------------------------------------------------------
// Integer overflow in writeChunkToFile
// ---------------------------------------------------------------------------

func TestWriteChunkToFileOverflow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.part")

	// offset near uint64 max + data length would wrap around to a small number.
	offset := ^uint64(0) - 5 // MaxUint64 - 5
	data := make([]byte, 100)
	expectedSize := uint64(1000)

	err := writeChunkToFile(path, offset, data, expectedSize)
	if err == nil {
		t.Fatal("writeChunkToFile should reject overflow")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("expected overflow error, got: %v", err)
	}
}

func TestWriteChunkToFileExceedsFileSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.part")

	// Offset + data exceeds expected file size.
	err := writeChunkToFile(path, 900, make([]byte, 200), 1000)
	if err == nil {
		t.Fatal("writeChunkToFile should reject chunk exceeding file size")
	}
}

// ---------------------------------------------------------------------------
// Symlink detection in partial file
// ---------------------------------------------------------------------------

func TestPartialFileSymlinkDetection(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a target file and a symlink to it.
	target := filepath.Join(dir, "sensitive.txt")
	if err := os.WriteFile(target, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	symlink := filepath.Join(dir, "evil.part")
	if err := os.Symlink(target, symlink); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	// Lstat should detect the symlink.
	info, err := os.Lstat(symlink)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("Lstat should detect symlink")
	}
}

// TestWriteChunkToFileRejectsSymlink verifies that writeChunkToFile refuses
// to write through a symlinked .part file. Before the fix, os.OpenFile
// followed symlinks silently, allowing a local attacker to redirect chunk
// writes into an arbitrary writable target (e.g. ~/.bashrc).
func TestWriteChunkToFileRejectsSymlink(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a target file that the attacker wants to overwrite.
	target := filepath.Join(dir, "sensitive.txt")
	original := []byte("original content")
	if err := os.WriteFile(target, original, 0o644); err != nil {
		t.Fatal(err)
	}

	// Symlink the .part path to the sensitive target.
	partPath := filepath.Join(dir, "evil.part")
	if err := os.Symlink(target, partPath); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	poison := []byte("OVERWRITTEN")
	err := writeChunkToFile(partPath, 0, poison, 1024)
	if err == nil {
		t.Fatal("writeChunkToFile should reject symlinked partial path")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error should mention symlink, got: %v", err)
	}

	// Verify the sensitive file was NOT modified.
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("sensitive file was modified: got %q, want %q", got, original)
	}
}

// TestWriteChunkToFileRejectsSymlinkToNonexistent verifies that
// writeChunkToFile refuses to write through a symlink even when the target
// does not yet exist. Before the O_NOFOLLOW fix, os.OpenFile with O_CREATE
// would follow the symlink and create the target file at an arbitrary path
// chosen by the attacker — the existing verifyNotSymlink check ran too late
// because the file had already been created by the kernel.
func TestWriteChunkToFileRejectsSymlinkToNonexistent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// The attacker's chosen target does NOT exist yet.
	target := filepath.Join(dir, "created-by-attacker.txt")

	// Symlink the .part path to the non-existent target.
	partPath := filepath.Join(dir, "evil.part")
	if err := os.Symlink(target, partPath); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	poison := []byte("PAYLOAD")
	err := writeChunkToFile(partPath, 0, poison, 1024)
	if err == nil {
		t.Fatal("writeChunkToFile should reject symlinked partial path to non-existent target")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error should mention symlink, got: %v", err)
	}

	// The target file must NOT have been created.
	if _, statErr := os.Lstat(target); statErr == nil {
		t.Fatalf("symlink target was created at %s — O_NOFOLLOW failed to prevent creation", target)
	}
}

// TestWriteChunkToFileAcceptsRegularFile verifies that writeChunkToFile
// works normally for regular (non-symlinked) partial files.
func TestWriteChunkToFileAcceptsRegularFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	partPath := filepath.Join(dir, "normal.part")
	data := []byte("chunk data here")

	if err := writeChunkToFile(partPath, 0, data, 1024); err != nil {
		t.Fatalf("writeChunkToFile should accept regular file: %v", err)
	}

	got, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

// ---------------------------------------------------------------------------
// verifyPartialIntegrity — fd-based symlink + hash verification
// ---------------------------------------------------------------------------

// TestVerifyPartialIntegrityRejectsSymlink verifies that
// verifyPartialIntegrity refuses a symlinked partial file even when the
// symlink target has the correct hash. Before the fix, onDownloadComplete
// used a two-step Lstat+reopen pattern that left a TOCTOU window between
// the symlink check and hash computation.
func TestVerifyPartialIntegrityRejectsSymlink(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a target file with known content.
	content := []byte("legitimate file content")
	expectedHash := sha256HexStr(content)

	target := filepath.Join(dir, "target.bin")
	if err := os.WriteFile(target, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Symlink the .part path to the target.
	symPath := filepath.Join(dir, "evil.part")
	if err := os.Symlink(target, symPath); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	_, err := verifyPartialIntegrity(symPath, expectedHash)
	if err == nil {
		t.Fatal("verifyPartialIntegrity should reject symlinked partial file")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error should mention symlink, got: %v", err)
	}
}

// TestVerifyPartialIntegrityAcceptsRegularFile verifies that
// verifyPartialIntegrity accepts a regular file with the correct hash.
func TestVerifyPartialIntegrityAcceptsRegularFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	content := []byte("valid partial file data for hashing")
	expectedHash := sha256HexStr(content)

	partPath := filepath.Join(dir, "valid.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	info, err := verifyPartialIntegrity(partPath, expectedHash)
	if err != nil {
		t.Fatalf("verifyPartialIntegrity should accept regular file with correct hash: %v", err)
	}
	if info == nil {
		t.Fatal("verifyPartialIntegrity should return non-nil FileInfo on success")
	}
}

// TestVerifyPartialIntegrityRejectsHashMismatch verifies that
// verifyPartialIntegrity rejects a regular file whose hash does not match.
func TestVerifyPartialIntegrityRejectsHashMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	content := []byte("some content")
	wrongHash := sha256HexStr([]byte("different content"))

	partPath := filepath.Join(dir, "mismatch.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := verifyPartialIntegrity(partPath, wrongHash)
	if err == nil {
		t.Fatal("verifyPartialIntegrity should reject hash mismatch")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("error should mention hash mismatch, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// verifyFileIdentity — post-verification inode revalidation
// ---------------------------------------------------------------------------

// TestVerifyFileIdentityDetectsSwap verifies that verifyFileIdentity detects
// when a file is replaced between the initial verification and a subsequent
// path-based operation. This closes the TOCTOU window between
// verifyPartialIntegrity and os.Rename.
func TestVerifyFileIdentityDetectsSwap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	content := []byte("original verified content")
	expectedHash := sha256HexStr(content)

	partPath := filepath.Join(dir, "verified.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	// Verify the file — capture its identity.
	verifiedInfo, err := verifyPartialIntegrity(partPath, expectedHash)
	if err != nil {
		t.Fatalf("verifyPartialIntegrity: %v", err)
	}

	// Simulate an attacker replacing the file between verify and rename.
	if err := os.Remove(partPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath, []byte("attacker content"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Identity check must detect the swap.
	err = verifyFileIdentity(partPath, verifiedInfo)
	if err == nil {
		t.Fatal("verifyFileIdentity should detect file swap")
	}
	if !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("error should mention identity changed, got: %v", err)
	}
}

// TestVerifyFileIdentityDetectsSymlinkSwap verifies that verifyFileIdentity
// detects when a regular file is replaced with a symlink after verification.
func TestVerifyFileIdentityDetectsSymlinkSwap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	content := []byte("original file for symlink swap test")
	expectedHash := sha256HexStr(content)

	partPath := filepath.Join(dir, "verified.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	verifiedInfo, err := verifyPartialIntegrity(partPath, expectedHash)
	if err != nil {
		t.Fatalf("verifyPartialIntegrity: %v", err)
	}

	// Replace with symlink.
	if err := os.Remove(partPath); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target.bin")
	if err := os.WriteFile(target, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, partPath); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	err = verifyFileIdentity(partPath, verifiedInfo)
	if err == nil {
		t.Fatal("verifyFileIdentity should detect symlink swap")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error should mention symlink, got: %v", err)
	}
}

// TestVerifyFileIdentityPassesForSameFile verifies that verifyFileIdentity
// succeeds when the file has not been tampered with.
func TestVerifyFileIdentityPassesForSameFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	content := []byte("stable file content")
	expectedHash := sha256HexStr(content)

	partPath := filepath.Join(dir, "stable.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	verifiedInfo, err := verifyPartialIntegrity(partPath, expectedHash)
	if err != nil {
		t.Fatalf("verifyPartialIntegrity: %v", err)
	}

	// File is untouched — identity check must pass.
	if err := verifyFileIdentity(partPath, verifiedInfo); err != nil {
		t.Fatalf("verifyFileIdentity should pass for untouched file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sha256HexStr(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
