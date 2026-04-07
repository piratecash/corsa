package desktop

import (
	"encoding/json"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// fileAttachResult holds the metadata for a file that has been
// successfully stored in the transmit directory.
type fileAttachResult struct {
	FileName    string
	FileSize    uint64
	FileHash    string // hex-encoded SHA-256
	ContentType string
}

// prepareFileForTransmit validates the source file, stores it in the transmit
// directory via the provided storeFile function (which handles content-addressed
// dedup and ref-counting), and returns the metadata needed for a file_announce DM.
//
// storeFile is expected to be DesktopClient.StoreFileForTransmit — it copies the
// source file into the transmit directory and returns the SHA-256 hash. Using the
// central FileStore guarantees that identical content is stored only once regardless
// of the original filename or extension.
//
// storedFileSize returns the byte size of the blob already persisted in the
// transmit store for the given hash. This ensures that FileSize in the announce
// describes the exact bytes the receiver will get, not a stale os.Stat snapshot
// that may have raced with a concurrent write to the source file.
//
// removeOrphan is called when storeFile succeeds but a subsequent step fails,
// leaving a blob with refs=0 and pending=0. It mirrors the orphan-cleanup
// pattern in PrepareAndSend for the pre-token failure path.
func prepareFileForTransmit(
	storeFile func(string) (string, error),
	storedFileSize func(string) (uint64, error),
	removeOrphan func(string),
	srcPath string,
) (*fileAttachResult, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("stat source file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("cannot send a directory")
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("cannot send an empty file")
	}

	// Delegate hashing and copy to the central FileStore which handles
	// content-addressed naming, dedup, and ref-counting.
	fileHash, err := storeFile(srcPath)
	if err != nil {
		return nil, fmt.Errorf("store file for transmit: %w", err)
	}

	// Read the size from the stored blob — not from the source file.
	// The source file may have changed between the os.Stat above and
	// the storeFile call. The stored blob and the hash describe the
	// same content, so FileSize and FileHash are always consistent.
	fileSize, err := storedFileSize(fileHash)
	if err != nil {
		removeOrphan(fileHash)
		return nil, fmt.Errorf("read stored file size: %w", err)
	}

	// Detect MIME type from extension; fall back to octet-stream.
	ext := filepath.Ext(info.Name())
	contentType := mime.TypeByExtension(ext)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return &fileAttachResult{
		FileName:    info.Name(),
		FileSize:    fileSize,
		FileHash:    fileHash,
		ContentType: contentType,
	}, nil
}

// buildFileAnnounceOutgoing constructs an OutgoingDM for a file_announce message.
// If caption is non-empty, it is used as the DM body (user-visible text
// alongside the file card). Otherwise, the sentinel "[file]" is used.
func buildFileAnnounceOutgoing(result *fileAttachResult, caption string) (domain.OutgoingDM, error) {
	payload := domain.FileAnnouncePayload{
		FileName:    result.FileName,
		FileSize:    result.FileSize,
		ContentType: result.ContentType,
		FileHash:    result.FileHash,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return domain.OutgoingDM{}, fmt.Errorf("marshal file announce payload: %w", err)
	}

	body := strings.TrimSpace(caption)
	if body == "" {
		body = domain.FileDMBodySentinel
	}

	return domain.OutgoingDM{
		Body:        body,
		Command:     domain.FileActionAnnounce,
		CommandData: string(data),
	}, nil
}

// formatFileSize formats a byte count into a human-readable string.
func formatFileSize(size uint64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := uint64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(size)/float64(div), suffixes[exp])
}
