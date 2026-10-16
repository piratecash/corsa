//go:build !windows

package secretfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// audit_posix.go is the POSIX half of AuditLocation: the file's owner and
// mode, and every directory and symlink the kernel passes through to reach
// it, judged by owner and mode bits.
//
// The route is resolved component by component rather than with
// filepath.EvalSymlinks, because a link is judged by who can change it — the
// directory that holds it — and EvalSymlinks returns only where the links
// lead. macOS reaches every temp and data directory through /var ->
// private/var, so a link on the way is not a violation by itself.
//
// Known limit: only owner and mode bits are judged. macOS (and Linux with
// POSIX ACLs) can grant others access through an extended ACL — read on the
// file, add_file or delete_child on a directory — that the mode does not
// show, and this audit does not read ACLs: on darwin there is no way to read
// one without cgo (getattrlist is not exposed by x/sys, and the ACL's
// extended attribute is refused to user space). A location that passes is
// therefore safe as far as its mode bits go, not proven free of ACL grants.

// rootUID is root's uid: a component root owns is one only root can change,
// which is not a violation by itself.
const rootUID uint32 = 0

// maxLinkHops bounds link resolution, as the kernel's own ELOOP does.
const maxLinkHops = 40

var errLinkLoop = errors.New("secretfile: audit: too many symlinks on the way to the file")

// chainEntry is one component as the judgement sees it.
type chainEntry struct {
	component LocationComponent
	owner     uint32
	mode      fs.FileMode
	// parentShared: the directory holding this entry is sticky and others
	// may write it, so it protects this entry only if this user owns it.
	parentShared bool
}

// routeStep is a component the resolution passed, with the path it was
// reached under — kept only to work out the role, never reported.
type routeStep struct {
	path         string
	info         fs.FileInfo
	parentShared bool
}

// route is the resolution of the file's directory: every step, the deepest
// directory that exists, and how many levels below it the file would be.
type route struct {
	steps []routeStep
	// deepest is the resolved path of the deepest existing directory, the
	// one holding the file when nothing on the way is missing.
	deepest     string
	deepestInfo fs.FileInfo
	// missingLevels counts the directories still to be created between
	// deepest and the file; 0 when the file's directory exists.
	missingLevels int
}

// auditLocation resolves the file's directory first and only then looks at
// the file, at its resolved place: a link loop on the way is reported as
// errLinkLoop by the resolution rather than as whatever the kernel's ELOOP
// on the full path happens to read like.
func auditLocation(abs string) error {
	resolved, err := resolveRoute(filepath.Dir(abs))
	if err != nil {
		return err
	}
	entries, err := resolved.entries()
	if err != nil {
		return err
	}
	if resolved.missingLevels > 0 {
		return unsafeOrNil(judgeChain(entries, euid()))
	}
	file, present, err := inspectFile(filepath.Join(resolved.deepest, filepath.Base(abs)))
	if err != nil {
		return err
	}
	if present {
		owner, err := uidOf(file)
		if err != nil {
			return err
		}
		entries = append(entries, chainEntry{
			component:    fileComponent(),
			owner:        owner,
			mode:         file.Mode(),
			parentShared: sharedSticky(resolved.deepestInfo),
		})
	}
	return unsafeOrNil(judgeChain(entries, euid()))
}

// resolveRoute walks dir from the root the way the kernel does, Lstat-ing
// every component and following links, and stops at the first component that
// does not exist yet.
func resolveRoute(dir string) (route, error) {
	rootInfo, err := os.Lstat("/")
	if err != nil {
		return route{}, fmt.Errorf("secretfile: audit the root directory: %w", stripPath(err))
	}
	resolved := route{steps: []routeStep{{path: "/", info: rootInfo}}}
	current, currentInfo := "/", rootInfo
	pending := splitPath(dir)
	hops := 0
	for len(pending) > 0 {
		name := pending[0]
		pending = pending[1:]
		if name == ".." {
			current = filepath.Dir(current)
			if currentInfo, err = os.Lstat(current); err != nil {
				return route{}, fmt.Errorf("secretfile: audit a directory on the way to the file: %w", stripPath(err))
			}
			continue
		}
		next := filepath.Join(current, name)
		info, err := os.Lstat(next)
		if errors.Is(err, fs.ErrNotExist) {
			resolved.missingLevels = 1 + countNames(pending)
			break
		}
		if err != nil {
			return route{}, fmt.Errorf("secretfile: audit a directory on the way to the file: %w", stripPath(err))
		}
		resolved.steps = append(resolved.steps, routeStep{path: next, info: info, parentShared: sharedSticky(currentInfo)})
		if info.Mode()&fs.ModeSymlink != 0 {
			hops++
			if hops > maxLinkHops {
				return route{}, errLinkLoop
			}
			target, err := os.Readlink(next)
			if err != nil {
				return route{}, fmt.Errorf("secretfile: audit a symlink on the way to the file: %w", stripPath(err))
			}
			if filepath.IsAbs(target) {
				current, currentInfo = "/", rootInfo
			}
			pending = append(splitPath(target), pending...)
			continue
		}
		if !info.IsDir() {
			return route{}, fmt.Errorf("%w: a component on the way to the file is not a directory", ErrNotADirectory)
		}
		current, currentInfo = next, info
	}
	resolved.deepest, resolved.deepestInfo = current, currentInfo
	return resolved, nil
}

// entries gives every step its role: a directory on the resolved location
// by its distance from the file, anything else as part of a link's route.
func (r route) entries() ([]chainEntry, error) {
	entries := make([]chainEntry, 0, len(r.steps))
	deepestDepth := len(splitPath(r.deepest))
	for _, step := range r.steps {
		owner, err := uidOf(step.info)
		if err != nil {
			return nil, err
		}
		component := LocationComponent{Kind: ComponentLinkRoute}
		if step.info.IsDir() && onPath(step.path, r.deepest) {
			component = LocationComponent{
				Kind:        ComponentDirectory,
				LevelsAbove: deepestDepth - len(splitPath(step.path)) + 1 + r.missingLevels,
			}
		}
		entries = append(entries, chainEntry{component: component, owner: owner, mode: step.info.Mode(), parentShared: step.parentShared})
	}
	return entries, nil
}

// judgeChain returns the violation nearest the file, or nil. entries run from
// the root to the file; the judgement reads them the other way so the report
// names what is closest to the secret.
func judgeChain(entries []chainEntry, euid uint32) *UnsafeLocationError {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		violation, found := judgeEntry(entry, euid)
		if found {
			return &UnsafeLocationError{Component: entry.component, Violation: violation, Owner: uidName(entry.owner), Mode: entry.mode}
		}
	}
	return nil
}

func judgeEntry(entry chainEntry, euid uint32) (LocationViolation, bool) {
	if entry.component.Kind == ComponentFile {
		return judgeFile(entry, euid)
	}
	switch {
	case entry.owner != euid && entry.owner != rootUID:
		return ViolationForeignOwner, true
	case entry.parentShared && entry.owner != euid:
		return ViolationStickyChildNotOwned, true
	case entry.mode.IsDir() && writableByOthers(entry.mode) && entry.mode&fs.ModeSticky == 0:
		return ViolationWritableByOthers, true
	}
	return "", false
}

// judgeFile holds the file to what the writer creates: this user's, 0600.
// Root ownership, acceptable for a directory, is not for the file — a file
// root owns is one this node cannot replace in place.
func judgeFile(entry chainEntry, euid uint32) (LocationViolation, bool) {
	switch {
	case entry.owner != euid:
		return ViolationForeignOwner, true
	case entry.mode.Perm() != 0o600:
		return ViolationFileNotOwnerOnly, true
	}
	return "", false
}

func writableByOthers(mode fs.FileMode) bool { return mode.Perm()&0o022 != 0 }

// sharedSticky reports a directory that others may write but that protects
// the entries of their owners: /tmp, 1777.
func sharedSticky(info fs.FileInfo) bool {
	return info.IsDir() && writableByOthers(info.Mode()) && info.Mode()&fs.ModeSticky != 0
}

func onPath(dir, deepest string) bool {
	return dir == deepest || dir == "/" || strings.HasPrefix(deepest, dir+"/")
}

func splitPath(path string) []string {
	var names []string
	for _, name := range strings.Split(path, "/") {
		if name != "" && name != "." {
			names = append(names, name)
		}
	}
	return names
}

func countNames(names []string) int {
	count := 0
	for _, name := range names {
		if name != ".." {
			count++
		}
	}
	return count
}

// euid is this process's effective uid. os.Geteuid is -1 only on Windows,
// which does not build this file.
func euid() uint32 { return uint32(os.Geteuid()) }

func uidOf(info fs.FileInfo) (uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("secretfile: audit: this filesystem reports no owner")
	}
	return stat.Uid, nil
}

func uidName(uid uint32) string { return fmt.Sprintf("uid %d", uid) }

// ownerOf names a component's owner for an error; "" when the filesystem
// reports none.
func ownerOf(info fs.FileInfo) string {
	uid, err := uidOf(info)
	if err != nil {
		return ""
	}
	return uidName(uid)
}

// describeDirectory names the pinned directory's owner and mode for
// ErrDirectoryNotWritable, read through the handle rather than the name.
func describeDirectory(dir *Dir) string {
	info, err := dir.root.Stat(".")
	if err != nil {
		return "owner and mode unreadable"
	}
	owner := ownerOf(info)
	if owner == "" {
		owner = "not reported"
	}
	return fmt.Sprintf("owner %s, mode %s, process euid %d", owner, info.Mode(), os.Geteuid())
}
