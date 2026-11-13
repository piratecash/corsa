package secretfile

import (
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

// Darwin libSystem ACL constants from sys/acl.h.
const (
	fileSecACL     = 5
	darwinACLAllow = 1
	darwinACLDeny  = 2
	aclFirst       = 0
	aclNext        = -1
	aclLast        = -2
	aclMaxEntries  = 128

	// These directory rights do not permit changing the route or the secret.
	// Any rights inherited onto a new file are checked again on that file.
	aclDirectoryReadRights = 1<<1 | 1<<3 | 1<<7 | 1<<9 | 1<<11 | 1<<20
)

type darwinACL struct {
	initSec   func() unsafe.Pointer
	freeSec   func(unsafe.Pointer)
	statSec   func(int32, *unix.Stat_t, unsafe.Pointer) int32
	querySec  func(unsafe.Pointer, int32, *int32) int32
	getSec    func(unsafe.Pointer, int32, *unsafe.Pointer) int32
	free      func(unsafe.Pointer) int32
	entry     func(unsafe.Pointer, int32, *unsafe.Pointer) int32
	tag       func(unsafe.Pointer, *int32) int32
	rights    func(unsafe.Pointer, *uint64) int32
	qualifier func(unsafe.Pointer) unsafe.Pointer
	userUUID  func(uint32, *[16]byte) int32
}

var loadDarwinACL = sync.OnceValues(func() (darwinACL, error) {
	var api darwinACL
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return api, fmt.Errorf("secretfile: load macOS ACL interface: %w", err)
	}
	statSymbol := "fstatx_np"
	if runtime.GOARCH == "amd64" {
		statSymbol += "$INODE64"
	}
	for _, binding := range []struct {
		name   string
		target any
	}{
		{"filesec_init", &api.initSec}, {"filesec_free", &api.freeSec},
		{statSymbol, &api.statSec}, {"filesec_query_property", &api.querySec},
		{"filesec_get_property", &api.getSec}, {"acl_free", &api.free},
		{"acl_get_entry", &api.entry}, {"acl_get_tag_type", &api.tag},
		{"acl_get_permset_mask_np", &api.rights}, {"acl_get_qualifier", &api.qualifier},
		{"mbr_uid_to_uuid", &api.userUUID},
	} {
		address, err := purego.Dlsym(lib, binding.name)
		if err != nil {
			_ = purego.Dlclose(lib)
			return darwinACL{}, fmt.Errorf("secretfile: bind macOS ACL interface: %w", err)
		}
		purego.RegisterFunc(binding.target, address)
	}
	return api, nil
})

func auditPathACL(path string, expected fs.FileInfo, component LocationComponent) error {
	flags := unix.O_RDONLY | unix.O_NONBLOCK | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if expected.Mode()&fs.ModeSymlink != 0 {
		flags = flags&^unix.O_NOFOLLOW | unix.O_SYMLINK
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return fmt.Errorf("secretfile: open component to audit ACL: %w", err)
	}
	file := os.NewFile(uintptr(fd), "secret-acl-audit")
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return stripPath(err)
	}
	owner, err := uidOf(opened)
	if err != nil {
		return err
	}
	expectedOwner, err := uidOf(expected)
	if err != nil {
		return err
	}
	if !os.SameFile(opened, expected) || opened.Mode() != expected.Mode() || owner != expectedOwner {
		return errSecretChanged
	}
	return auditDescriptorACL(file, opened, component)
}

func auditCreationDirectoryACL(dir *Dir) error {
	file, err := dir.root.Open(".")
	if err != nil {
		return stripPath(err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return stripPath(err)
	}
	return auditDescriptorACL(file, info, LocationComponent{Kind: ComponentDirectory, LevelsAbove: 1})
}

func auditNewFileACL(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return stripPath(err)
	}
	return auditDescriptorACL(file, info, fileComponent())
}

func auditDescriptorACL(file *os.File, info fs.FileInfo, component LocationComponent) error {
	api, err := loadDarwinACL()
	if err != nil {
		return err
	}
	acl, err := api.read(int32(file.Fd()))
	runtime.KeepAlive(file)
	if err != nil {
		return err
	}
	if acl == nil {
		return nil
	}
	defer func() { _ = api.free(acl) }()

	allowed, err := api.trustedGrants(acl, info.IsDir())
	if err != nil {
		return err
	}
	if !allowed {
		return &UnsafeLocationError{Component: component, Violation: ViolationExtendedACL, Owner: ownerOf(info), Mode: info.Mode()}
	}
	return nil
}

func (api darwinACL) read(fd int32) (unsafe.Pointer, error) {
	sec := api.initSec()
	if sec == nil {
		return nil, fmt.Errorf("secretfile: could not allocate macOS file security descriptor")
	}
	defer api.freeSec(sec)
	var info unix.Stat_t
	if api.statSec(fd, &info, sec) != 0 {
		return nil, fmt.Errorf("secretfile: could not inspect open file security on macOS")
	}
	var present int32
	if api.querySec(sec, fileSecACL, &present) != 0 {
		return nil, fmt.Errorf("secretfile: could not query macOS ACL presence")
	}
	if present == 0 {
		return nil, nil
	}
	var acl unsafe.Pointer
	// filesec also uses (void *)1 as an ACL removal marker.
	if api.getSec(sec, fileSecACL, &acl) != 0 || uintptr(acl) <= 1 {
		return nil, fmt.Errorf("secretfile: could not read present macOS ACL")
	}
	return acl, nil
}

func (api darwinACL) trustedGrants(acl unsafe.Pointer, directory bool) (bool, error) {
	var last unsafe.Pointer
	if api.entry(acl, aclLast, &last) != 0 || last == nil {
		return false, fmt.Errorf("secretfile: could not identify last macOS ACL entry")
	}
	position := int32(aclFirst)
	for range aclMaxEntries {
		var entry unsafe.Pointer
		if api.entry(acl, position, &entry) != 0 || entry == nil {
			return false, fmt.Errorf("secretfile: could not read macOS ACL entry")
		}
		trusted, err := api.trustedEntry(entry, directory)
		if err != nil || !trusted {
			return trusted, err
		}
		if entry == last {
			return true, nil
		}
		position = aclNext
	}
	return false, fmt.Errorf("secretfile: too many macOS ACL entries")
}

func (api darwinACL) trustedEntry(entry unsafe.Pointer, directory bool) (bool, error) {
	var tag int32
	if api.tag(entry, &tag) != 0 {
		return false, fmt.Errorf("secretfile: could not read macOS ACL entry type")
	}
	if tag == darwinACLDeny {
		return true, nil
	}
	if tag != darwinACLAllow {
		return false, fmt.Errorf("secretfile: unknown macOS ACL entry type")
	}
	var rights uint64
	if api.rights(entry, &rights) != 0 {
		return false, fmt.Errorf("secretfile: could not read macOS ACL permissions")
	}
	if rights == 0 || directory && rights&^aclDirectoryReadRights == 0 {
		return true, nil
	}
	qualifier := api.qualifier(entry)
	if qualifier == nil {
		return false, fmt.Errorf("secretfile: could not read macOS ACL principal")
	}
	subject := *(*[16]byte)(qualifier)
	_ = api.free(qualifier)
	var own [16]byte
	if api.userUUID(euid(), &own) != 0 {
		return false, fmt.Errorf("secretfile: could not resolve macOS user identity")
	}
	if subject == own {
		return true, nil
	}
	if directory && euid() != rootUID {
		var root [16]byte
		if api.userUUID(rootUID, &root) != 0 {
			return false, fmt.Errorf("secretfile: could not resolve macOS root identity")
		}
		return subject == root, nil
	}
	return false, nil
}
