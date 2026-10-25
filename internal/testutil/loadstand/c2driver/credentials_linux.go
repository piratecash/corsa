//go:build linux

package main

import (
	"golang.org/x/sys/unix"
	"os"
)

func readCredentials() (credentials, error) {
	root, e := unix.Open("/run/c1", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return credentials{}, errRefused
	}
	defer func() { _ = unix.Close(root) }()
	fd, e := unix.Openat(root, "auth.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return credentials{}, errRefused
	}
	f := os.NewFile(uintptr(fd), "owned-native-auth")
	defer func() { _ = f.Close() }()
	var a, b unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &a) != nil || unix.Fstatfs(fd, &fs) != nil || a.Mode&unix.S_IFMT != unix.S_IFREG || a.Mode&07777 != 0600 || a.Uid != 501 || a.Gid != 20 || a.Nlink != 1 || a.Size <= 0 || a.Size > 1024 || fs.Flags&unix.ST_RDONLY == 0 {
		return credentials{}, errRefused
	}
	raw := make([]byte, a.Size)
	if _, e = f.ReadAt(raw, 0); e != nil {
		return credentials{}, errRefused
	}
	if unix.Fstat(fd, &b) != nil || a.Dev != b.Dev || a.Ino != b.Ino || a.Size != b.Size || a.Mode != b.Mode || a.Uid != b.Uid || a.Gid != b.Gid || a.Nlink != b.Nlink || a.Mtim != b.Mtim || a.Ctim != b.Ctim {
		return credentials{}, errRefused
	}
	return parseCredentials(raw)
}
