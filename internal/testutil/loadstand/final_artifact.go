package loadstand

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
)

// A launch owns this capability until post-Done reduction or explicit cleanup.
// No operation recovers it by reopening the profile pathname.
type launchArtifacts struct {
	root     *os.Root
	binding  launchBinding
	name     string
	original fs.FileInfo
	writer   *os.File
	// Parent-only CLOEXEC capability pins the original inode even after every
	// child/writer handle closes. Cached dev/ino alone permits inode reuse.
	pin *os.File
}

func newLaunchArtifacts(ctx context.Context, directory string, binding launchBinding) (_ *launchArtifacts, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if binding.nonce == (LaunchNonce{}) || binding.fingerprint == (LaunchFingerprint{}) {
		return nil, ErrInvalidConfiguration
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	a := &launchArtifacts{root: root, binding: binding, name: "final-" + binding.nonce.String() + ".json"}
	defer func() {
		if err != nil {
			err = errors.Join(err, a.close())
		}
	}()
	if err := a.auditDirectory(); err != nil {
		return nil, err
	}
	a.writer, err = root.OpenFile(a.name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := auditPrivateFinalFD(a.writer, false); err != nil {
		return nil, err
	}
	a.original, err = a.writer.Stat()
	if err != nil {
		return nil, err
	}
	a.pin, err = duplicateFinalFD(a.writer)
	if err != nil {
		return nil, err
	}
	return a, nil
}
func (a *launchArtifacts) close() error {
	if a == nil {
		return nil
	}
	var err error
	if a.writer != nil {
		err = a.writer.Close()
		a.writer = nil
	}
	if a.pin != nil {
		err = errors.Join(err, a.pin.Close())
		a.pin = nil
	}
	if a.root != nil {
		err = errors.Join(err, a.root.Close())
		a.root = nil
	}
	return err
}
func (a *launchArtifacts) read(ctx context.Context) (out []byte, err error) {
	if a == nil || a.root == nil || a.pin == nil {
		return nil, ErrFinalAccountingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.auditDirectory(); err != nil {
		return nil, err
	}
	if err := auditPrivateFinalFD(a.pin, false); err != nil {
		return nil, err
	}
	pinned, err := a.pin.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(pinned, a.original) {
		return nil, ErrUnsupportedArtifact
	}
	before, err := a.root.Lstat(a.name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || !os.SameFile(before, pinned) {
		return nil, ErrUnsupportedArtifact
	}
	file, err := openVerifiedArtifact(ctx, a.root, a.name, pinned)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	out, err = readPrivateFinalFD(ctx, file, FinalReportLimit)
	if err != nil {
		return nil, err
	}
	after, err := a.root.Lstat(a.name)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	pinnedAfter, pinErr := a.pin.Stat()
	if pinErr != nil {
		return nil, pinErr
	}
	if err := auditPrivateFinalFD(a.pin, false); err != nil {
		return nil, err
	}
	if !after.Mode().IsRegular() || !os.SameFile(after, pinnedAfter) || !os.SameFile(pinnedAfter, pinned) || !os.SameFile(after, opened) || after.Size() != before.Size() || opened.Size() != before.Size() {
		return nil, ErrUnsupportedArtifact
	}
	if err := a.auditDirectory(); err != nil {
		return nil, err
	}
	return out, nil
}
func (a *launchArtifacts) auditDirectory() error {
	dir, err := a.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(auditPrivateFinalFD(dir, true), dir.Close())
}
func readPrivateFinalFD(ctx context.Context, file *os.File, limit ArtifactReadLimit) ([]byte, error) {
	if limit <= 0 || int64(limit) == 1<<63-1 {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := auditPrivateFinalFD(file, false); err != nil {
		return nil, err
	}
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if before.Size() < 0 || before.Size() > int64(limit) {
		return nil, ErrArtifactTooLarge
	}
	// ReadAt leaves a duplicated descriptor's shared file offset untouched.
	raw, err := io.ReadAll(io.NewSectionReader(file, 0, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(raw)) > int64(limit) {
		return nil, ErrArtifactTooLarge
	}
	if err := auditPrivateFinalFD(file, false); err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() || int64(len(raw)) != before.Size() {
		return nil, ErrUnsupportedArtifact
	}
	return raw, nil
}
