package runjournal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

// publishNoReplace makes content appear under dir/name, complete, or not at
// all — and never in place of something already there.
//
// The file is written and synced under a pending name, then hard-linked onto
// the final name. link(2) fails with EEXIST when the name exists, atomically,
// in the kernel: of any number of concurrent writers exactly one succeeds, and
// the rest get an error satisfying errors.Is(err, fs.ErrExist). A rename would
// not do — rename REPLACES an existing target — and a stat before it is the
// window this function exists to close.
//
// Linking a finished file, rather than creating the final name with O_EXCL and
// then writing into it, also means a reader never sees a record that exists
// but is still being written: the name appears with all its bytes.
//
// The directory itself is not fsynced. The journal's promise is "never
// replaced, never half-visible", not durability across power loss: a link lost
// in a crash leaves the configuration missing, and a missing configuration is
// simply run again.
func publishNoReplace(ctx context.Context, dir, name string, content []byte, link linkFunc) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("creating the journal directory: %w", err)
	}
	pending, err := os.CreateTemp(dir, pendingPrefix+"*")
	if err != nil {
		return fmt.Errorf("creating a pending record for %s: %w", name, err)
	}
	pendingPath := pending.Name()
	// The pending name is removed in every outcome: after a successful link the
	// record lives on under its final name, after a failure nothing should. A
	// removal error is dropped on purpose — the dot-prefixed leftover is ignored
	// by every reader, and reporting it would misreport a record that WAS
	// published as a failed write.
	defer func() { _ = os.Remove(pendingPath) }()

	if err := writeSynced(pending, content); err != nil {
		return fmt.Errorf("writing a pending record for %s: %w", name, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publishing %s: %w", name, err)
	}
	if err := link(pendingPath, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("publishing %s: %w", name, err)
	}
	return nil
}

// probeHardLinks proves, before anything is measured, that the journal's
// filesystem does what publishNoReplace relies on: a link onto a free name
// succeeds, and a link onto a taken name fails with EEXIST. Some filesystems
// (FAT, several network and FUSE mounts) refuse links outright; a broken one
// could "link" by replacing. Either way the no-overwrite rule would not hold,
// and the place to learn that is here — not after the first hour of a sweep,
// when the result it measured cannot be written.
func probeHardLinks(ctx context.Context, dir string, link linkFunc) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("probing hard links: %w", err)
	}
	token, err := attemptToken()
	if err != nil {
		return err
	}
	// The pending prefix makes a probe that outlives a crash show up in the
	// ledger as a leftover temporary rather than as a stranger.
	name := pendingPrefix + "probe-" + token
	content := []byte("runjournal hard-link probe\n")

	if err := publishNoReplace(ctx, dir, name, content, link); err != nil {
		return classifyProbeFailure(err)
	}
	again := publishNoReplace(ctx, dir, name, content, link)
	removeErr := os.Remove(filepath.Join(dir, name))
	switch {
	case again == nil:
		return fmt.Errorf("%w: a second link onto %s succeeded instead of failing", ErrNoHardLinks, name)
	case !errors.Is(again, fs.ErrExist):
		return classifyProbeFailure(again)
	case removeErr != nil:
		return fmt.Errorf("removing the hard-link probe: %w", removeErr)
	}
	return nil
}

// noHardLinkErrnos are the answers of a filesystem that does not do hard links
// here at all: refused (EPERM, and ENOTSUP/EOPNOTSUPP/ENOSYS via
// errors.ErrUnsupported), across devices (EXDEV), or out of link slots (EMLINK).
var noHardLinkErrnos = []error{errors.ErrUnsupported, syscall.EPERM, syscall.EXDEV, syscall.EMLINK}

// classifyProbeFailure blames the filesystem's hard links only when the link
// itself said so. A full disk, a permission problem on the directory, or a
// cancel between the two links is an ordinary failure with its own cause, and
// reporting it as "no hard links here" would send the operator to move the
// journal to another filesystem for nothing.
func classifyProbeFailure(err error) error {
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) && slices.ContainsFunc(noHardLinkErrnos, func(errno error) bool {
		return errors.Is(linkErr.Err, errno)
	}) {
		return fmt.Errorf("%w: %w", ErrNoHardLinks, err)
	}
	return fmt.Errorf("probing hard links: %w", err)
}

// writeSynced writes, syncs and closes. The file has to be on disk before its
// name is published: a link to a file whose bytes are still in the page cache
// could survive a crash as an empty record.
func writeSynced(file *os.File, content []byte) error {
	if _, err := file.Write(content); err != nil {
		// The write error is the cause; a close error after it would only hide it.
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close() // as above: the sync error is the one that explains the failure
		return err
	}
	return file.Close()
}

// attemptToken names one failed attempt. It comes from crypto/rand rather
// than a counter or a clock: two processes retrying one configuration in the
// same instant must not pick one name, and neither can see the other's counter.
func attemptToken() (string, error) {
	var buf [attemptTokenHexLen / 2]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("naming a failed attempt: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}
