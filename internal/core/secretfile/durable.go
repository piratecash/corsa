package secretfile

import (
	"errors"
	"fmt"
	"io/fs"
)

// DurableCommit distinguishes whether an error preceded or followed visibility.
type DurableCommit uint8

const (
	DurableNotCommitted DurableCommit = iota + 1
	DurableCommitted
	DurableIndeterminate
)

var ErrDurabilityUnsupported = errors.New("strict durable storage unsupported")

type durableStep string

const (
	durablePreflight     durableStep = "preflight"
	durableCreate        durableStep = "create"
	durableWrite         durableStep = "write"
	durableFileSync      durableStep = "file_sync"
	durableFileClose     durableStep = "file_close"
	durableRename        durableStep = "rename"
	durableUnlink        durableStep = "unlink"
	durableDirectorySync durableStep = "directory_sync"
)

func (d *Dir) durableFailure(step durableStep) error {
	if d.durableFault != nil {
		return d.durableFault(step)
	}
	return nil
}

// DurablePreflight rejects unsupported durability before any file mutation.
func (d *Dir) DurablePreflight() error {
	if err := d.durableFailure(durablePreflight); err != nil {
		return err
	}
	return d.durablePlatformPreflight()
}

// DurableWrite syncs an owner-only temp before replacement, then syncs the
// pinned parent directory. An error after replacement is Indeterminate.
func (d *Dir) DurableWrite(name string, raw []byte) (DurableCommit, error) {
	if err := checkSingleName(name); err != nil {
		return DurableNotCommitted, err
	}
	if err := d.DurablePreflight(); err != nil {
		return DurableNotCommitted, err
	}
	temporary, err := d.writeDurableTemp(raw)
	if err != nil {
		return DurableNotCommitted, err
	}
	// A failed replacement may leave only this owner-only temp. Cleanup is best
	// effort and never changes the target; startup already sweeps old temps.
	defer func() { _ = d.root.Remove(temporary) }()
	if err := d.durableFailure(durableRename); err != nil {
		return DurableNotCommitted, err
	}
	if err := d.root.Rename(temporary, name); err != nil {
		return DurableNotCommitted, stripPath(err)
	}
	if err := d.syncDurableParent(); err != nil {
		return DurableIndeterminate, err
	}
	return DurableCommitted, nil
}

func (d *Dir) writeDurableTemp(raw []byte) (string, error) {
	if err := d.durableFailure(durableCreate); err != nil {
		return "", err
	}
	file, name, err := d.createTemp()
	if errors.Is(err, fs.ErrPermission) {
		return "", fmt.Errorf("%w (%s): %w", ErrDirectoryNotWritable, describeDirectory(d), err)
	}
	if err != nil {
		return "", err
	}
	completed := false
	defer func() {
		// The explicit close below is checked; these are cleanup after failure.
		_ = file.Close()
		if !completed {
			_ = d.root.Remove(name)
		}
	}()
	if err := d.durableFailure(durableWrite); err != nil {
		return "", err
	}
	if _, err := file.Write(raw); err != nil {
		return "", stripPath(err)
	}
	if err := d.durableFailure(durableFileSync); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", stripPath(err)
	}
	if err := d.durableFailure(durableFileClose); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", stripPath(err)
	}
	completed = true
	return name, nil
}

// DurableRemove confirms the absence of one entry by syncing its pinned parent.
// An error after unlink is Indeterminate, including when unlink was visible.
func (d *Dir) DurableRemove(name string) (DurableCommit, error) {
	if err := checkSingleName(name); err != nil {
		return DurableNotCommitted, err
	}
	if err := d.DurablePreflight(); err != nil {
		return DurableNotCommitted, err
	}
	if err := d.durableFailure(durableUnlink); err != nil {
		return DurableNotCommitted, err
	}
	if err := d.root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return DurableNotCommitted, stripPath(err)
	}
	if err := d.syncDurableParent(); err != nil {
		return DurableIndeterminate, err
	}
	return DurableCommitted, nil
}

func (d *Dir) syncDurableParent() error {
	if err := d.durableFailure(durableDirectorySync); err != nil {
		return err
	}
	return d.syncDurableDirectory()
}
