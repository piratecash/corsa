package secretfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// OpenAuditedDestination pins the directory that will receive a secret and
// checks the location against that directory before returning it.
func OpenAuditedDestination(path string) (*Dir, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(abs)
	dir, err := Open(parent)
	if err != nil {
		return nil, err
	}
	if err := dir.verifyDestination(abs, parent); err != nil {
		_ = dir.Close()
		return nil, err
	}
	return dir, nil
}

func (d *Dir) verifyDestination(path, parent string) error {
	opened, err := d.root.Stat(".")
	if err != nil {
		return stripPath(err)
	}
	if err := AuditLocation(path); err != nil {
		return err
	}
	current, err := os.Stat(parent)
	if err != nil {
		return stripPath(err)
	}
	if !os.SameFile(opened, current) {
		return errSecretChanged
	}
	return nil
}

// OpenAuditedRead opens an existing secret and checks the opened file before
// returning it. The caller closes both the file and its containing directory.
func OpenAuditedRead(path string) (*Dir, *os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	if err := AuditLocation(abs); err != nil {
		return nil, nil, err
	}
	named, err := os.Lstat(abs)
	if err != nil {
		return nil, nil, stripPath(err)
	}
	if !named.Mode().IsRegular() {
		return nil, nil, &UnsafeLocationError{Component: fileComponent(), Violation: ViolationNotRegularFile}
	}
	dir, err := Open(filepath.Dir(abs))
	if err != nil {
		// An existing name that disappears during open is not absence.
		return nil, nil, openedSecretError(err)
	}
	file, err := dir.openAuditedFile(filepath.Base(abs))
	if err != nil {
		_ = dir.Close()
		return nil, nil, openedSecretError(err)
	}
	if err := verifyAuditedFile(abs, named, file); err != nil {
		_ = file.Close()
		_ = dir.Close()
		return nil, nil, openedSecretError(err)
	}
	return dir, file, nil
}

func verifyAuditedFile(path string, named fs.FileInfo, file *os.File) error {
	opened, err := file.Stat()
	if err != nil {
		return stripPath(err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(named, opened) {
		return errSecretChanged
	}
	if err := auditOpenedFile(file, opened); err != nil {
		return err
	}
	if err := AuditLocation(path); err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil {
		return stripPath(err)
	}
	if !current.Mode().IsRegular() || !os.SameFile(opened, current) {
		return errSecretChanged
	}
	return nil
}

var errSecretChanged = errors.New("secretfile: secret changed while being opened")

func openedSecretError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return errSecretChanged
	}
	return err
}
