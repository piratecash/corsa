package desktop

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/piratecash/corsa/internal/core/appdata"
)

// attachTmpDirName is the app-data subdirectory where picker streams
// without a filesystem path are materialized before the transmit-import
// copy. A staged copy lives exactly as long as its single owner — the
// live composer slot, a conversation draft, a failed-send retry entry,
// or an in-flight send goroutine — and every owner-drop site frees it
// through releaseStagedAttachment (see that function for the full list).
// cleanupAttachTmp additionally wipes the directory once at startup as a
// backstop for copies orphaned by a crash or kill.
const attachTmpDirName = "attach-tmp"

// cleanupAttachTmp removes staged attachments orphaned by a previous
// run's crash or kill — during normal operation releaseStagedAttachment
// frees each copy the moment its last owner drops it. Called once from
// Run before the UI starts, so it can never race a live pick: at that
// point nothing can reference the directory (drafts and failed sends
// live only in process memory; Preferences persists just language +
// aliases). Best-effort: on error stale entries simply survive until the
// next start.
func cleanupAttachTmp() {
	_ = os.RemoveAll(filepath.Join(appdata.DefaultDir(), attachTmpDirName))
}

// isStagedAttachment reports whether path was produced by
// materializeAttachment (i.e. lives under the attach-tmp staging root).
func isStagedAttachment(path string) bool {
	if path == "" {
		return false
	}
	root := filepath.Join(appdata.DefaultDir(), attachTmpDirName)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// releaseStagedAttachment deletes path's per-pick staging directory if —
// and only if — the path was produced by materializeAttachment. Regular
// filesystem paths (desktop picker results) are never touched, so the
// call is safe at every reference-drop site without platform checks.
//
// Staged paths have single-owner semantics: a path is referenced by
// exactly one slot at a time (live composer attachment, a conversation
// draft, a failed-send retry entry, or an in-flight send goroutine) and
// moves between them without duplicating. Call this exactly where the
// last owner lets go: overwrite/cancel of an attachment slot, rejected
// restore, dismissed or epoch-dropped failed-send entry, contact
// removal, abandoned in-flight send, or fully settled successful send.
func releaseStagedAttachment(path string) {
	if !isStagedAttachment(path) {
		return
	}
	_ = os.RemoveAll(filepath.Dir(path))
}

// displayNamer is implemented by explorer's mobile File types
// (gioui.org/x/explorer File on Android/iOS), which expose the picker's
// display name — content streams there have no filesystem path.
type displayNamer interface {
	Name() string
}

// sizer is implemented by explorer's mobile File types, which report the
// picked document's size from the platform metadata.
type sizer interface {
	Size() int64
}

// attachSpaceMarginBytes is the free-space headroom materializeAttachment
// refuses to eat into: the chat DB, crash logs and the node keep writing
// while a transfer runs, and filling internal storage to the brim bricks
// more than the send.
const attachSpaceMarginBytes = 64 << 20

// attachStagingBudget returns the largest staging copy the target dir can
// take. The staging copy is the FIRST of two copies of the same bytes —
// the transmit import makes the second — so it may use at most half of
// the free space, minus the margin. ok=false means free space could not
// be determined (unsupported platform); the caller then copies
// unguarded, as before.
func attachStagingBudget(dir string) (int64, bool) {
	free, ok := attachDirFreeBytes(dir)
	if !ok {
		return 0, false
	}
	budget := int64(free/2) - attachSpaceMarginBytes
	if budget < 0 {
		budget = 0
	}
	return budget, true
}

// materializeAttachment copies a pathless picker stream into a temp file
// under the app data dir and returns its path, so the rest of the attach
// pipeline (SHA-256 hashing, filename extraction, copy to the transmit
// dir) can keep working with real paths on every platform. The display
// name is preserved as the file's base name because it becomes the
// receiver-visible filename.
//
// The returned path enters single-owner lifecycle tracking: it is freed
// by releaseStagedAttachment at the exact point its last owner lets go —
// attachment replaced or cancelled, restore rejected, retry entry
// dismissed or dropped, contact removed, send abandoned or fully
// settled. See the attachTmpDirName comment.
func materializeAttachment(rc io.Reader) (string, error) {
	name := "attachment"
	if n, ok := rc.(displayNamer); ok {
		// filepath.Base guards against path separators or traversal in a
		// picker-supplied display name.
		if base := filepath.Base(strings.TrimSpace(n.Name())); base != "" && base != "." && base != string(filepath.Separator) {
			name = base
		}
	}

	root := filepath.Join(appdata.DefaultDir(), attachTmpDirName)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("attach tmp dir: %w", err)
	}

	// Guard against filling internal storage: the SAF stream is
	// unbounded, and these bytes will exist TWICE until the send settles
	// (staging + transmit store). Reject up front when the platform
	// reports the size, and cap the copy regardless — metadata can lie.
	budget, budgetKnown := attachStagingBudget(root)
	if budgetKnown {
		if s, ok := rc.(sizer); ok && s.Size() > 0 && s.Size() > budget {
			return "", fmt.Errorf("not enough free space for a %d MB attachment (%d MB usable)",
				s.Size()>>20, budget>>20)
		}
	}

	// One directory per pick keeps the original base name intact even
	// when the same file is attached twice.
	dir, err := os.MkdirTemp(root, "pick-*")
	if err != nil {
		return "", fmt.Errorf("attach tmp dir: %w", err)
	}

	dst := filepath.Join(dir, name)
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("attach tmp create: %w", err)
	}

	src := io.Reader(rc)
	if budgetKnown {
		src = io.LimitReader(rc, budget+1)
	}
	written, err := io.Copy(out, src)
	if err != nil {
		_ = out.Close()
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("attach copy: %w", err)
	}
	if budgetKnown && written > budget {
		_ = out.Close()
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("attachment exceeds free space budget (%d MB usable)", budget>>20)
	}
	if err := out.Close(); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("attach close: %w", err)
	}

	return dst, nil
}
