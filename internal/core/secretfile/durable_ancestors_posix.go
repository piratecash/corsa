//go:build !windows

package secretfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

const durableAncestorSync durableStep = "ancestor_sync"

// SyncAncestors confirms the namespace route to this already pinned directory.
// It syncs each real directory, leaf to root, including parents passed while
// resolving symlinks, then checks that the route still names the same objects.
// It creates nothing and changes no permissions. It is not a lease against a
// trusted administrator changing the path after this call.
func (d *Dir) SyncAncestors(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return stripPath(err)
	}
	before, err := resolveRoute(absolute)
	if err != nil {
		return err
	}
	held, err := d.root.Stat(".")
	if err != nil {
		return stripPath(err)
	}
	if before.missingLevels != 0 || !os.SameFile(before.deepestInfo, held) {
		return fs.ErrInvalid
	}
	for i := len(before.steps) - 1; i >= 0; i-- {
		step := before.steps[i]
		if !step.info.IsDir() {
			continue
		}
		ancestor, err := Open(step.path)
		if err != nil {
			return err
		}
		opened, inspectErr := ancestor.root.Stat(".")
		if inspectErr == nil && !os.SameFile(step.info, opened) {
			inspectErr = fs.ErrInvalid
		}
		if inspectErr == nil {
			inspectErr = d.durableFailure(durableAncestorSync)
		}
		if inspectErr == nil {
			inspectErr = ancestor.syncDurableDirectory()
		}
		if err := errors.Join(stripPath(inspectErr), stripPath(ancestor.Close())); err != nil {
			return err
		}
	}
	after, err := resolveRoute(absolute)
	if err != nil {
		return err
	}
	if after.missingLevels != 0 || before.deepest != after.deepest || len(before.steps) != len(after.steps) || !os.SameFile(after.deepestInfo, held) {
		return fs.ErrInvalid
	}
	for i, step := range before.steps {
		current := after.steps[i]
		previousOwner, err := uidOf(step.info)
		if err != nil {
			return err
		}
		currentOwner, err := uidOf(current.info)
		if err != nil {
			return err
		}
		if step.path != current.path || !os.SameFile(step.info, current.info) || step.info.Mode() != current.info.Mode() || previousOwner != currentOwner {
			return fs.ErrInvalid
		}
	}
	return nil
}
