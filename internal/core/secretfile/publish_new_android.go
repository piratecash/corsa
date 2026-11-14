package secretfile

import "golang.org/x/sys/unix"

// Android SELinux blocks hard links in app-private storage. Rename within
// the pinned directory with RENAME_NOREPLACE to publish the completed file
// atomically and preserve an existing destination.
func (d *Dir) publishNew(tmp, name string) error {
	directory, err := d.root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	fd := int(directory.Fd())
	return unix.Renameat2(fd, tmp, fd, name, unix.RENAME_NOREPLACE)
}
