//go:build !windows

package secretfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

const durableAncestorSync durableStep = "ancestor_sync"

// SyncAncestors confirms the namespace route to this already pinned directory.
// It syncs real directories, leaf to root, including parents passed while
// resolving symlinks, then checks that the route still names the same objects.
// On Android the platform owns persistence of the protected system-managed
// ancestors. Apps may traverse those directories without being able to read
// them or open them for fsync. The app-owned directories still require fsync.
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
		if runtime.GOOS == "android" && !os.SameFile(step.info, held) {
			owner, group, err := ownerAndGroup(step.info)
			if err != nil {
				return err
			}
			if androidManagedAncestor(owner, group, step.info.Mode()) {
				continue
			}
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
		previousOwner, previousGroup, err := ownerAndGroup(step.info)
		if err != nil {
			return err
		}
		currentOwner, currentGroup, err := ownerAndGroup(current.info)
		if err != nil {
			return err
		}
		if step.path != current.path || !os.SameFile(step.info, current.info) || step.info.Mode() != current.info.Mode() || previousOwner != currentOwner || previousGroup != currentGroup {
			return fs.ErrInvalid
		}
	}
	return nil
}

func androidManagedAncestor(owner, group uint32, mode fs.FileMode) bool {
	if owner != rootUID && owner != androidSystemID {
		return false
	}
	return mode.Perm()&0o002 == 0 && (mode.Perm()&0o020 == 0 || group == rootUID || group == androidSystemID)
}
