package desktop

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// downloads_dir.go answers "where does a saved file go, and under what name"
// for the viewer's save button, which saves without asking.
//
// It is deliberately small and conservative: this application may put a file
// in the one folder the platform means by "downloads", and nowhere else. Any
// doubt about which folder that is — an unset home directory, a folder that
// is not there — is reported, and the caller falls back to asking the user
// through the system picker.

// errNoDownloadsDir means the platform's downloads folder could not be
// resolved. Not a failure to save: the caller asks the user instead.
var errNoDownloadsDir = errors.New("no downloads directory")

// userDownloadsDir is the folder saved files go to, as the platform means
// it. The resolution itself is per-platform: Windows keeps it in the Known
// Folders registry, where a user who moved the folder moved it; the desktops
// that follow the XDG specification keep it in a file of their own.
func userDownloadsDir() (string, error) {
	return platformDownloadsDir()
}

// homeDownloadsDir is the last resort every platform shares: the folder named
// "Downloads" under the home directory, which is what an untouched macOS,
// Windows and Linux all have.
func homeDownloadsDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("%w: %w", errNoDownloadsDir, err)
	}
	dir := filepath.Join(home, "Downloads")
	if !isDirectory(dir) {
		return "", fmt.Errorf("%w: %s is not a directory", errNoDownloadsDir, dir)
	}
	return dir, nil
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// createUniqueDownload creates a file in directory under a name nothing else
// holds: "photo.png", then "photo (2).png", "photo (3).png" and so on. It
// returns the open file and the name it took.
//
// Choosing the name and creating the file are ONE step, done with O_EXCL and
// retried on collision. Split in two they are a race — two saves started at
// once pick the same free name, and one of them fails on a file the other
// just created rather than moving to the next name.
//
// Overwriting was the other option and it is not one: two different pictures
// sent under the same name are two files, and the second save must not take
// the first one's place in a folder the user keeps things in.
func createUniqueDownload(directory, name string) (*os.File, string, error) {
	extension := filepath.Ext(name)
	stem := strings.TrimSuffix(name, extension)
	for attempt := 1; attempt <= maxDownloadNameAttempts; attempt++ {
		candidate := name
		if attempt > 1 {
			candidate = stem + " (" + strconv.Itoa(attempt) + ")" + extension
		}
		file, err := os.OpenFile(filepath.Join(directory, candidate),
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, candidate, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", fmt.Errorf("create %s: %w", candidate, err)
		}
	}
	return nil, "", fmt.Errorf("no free name for %q in %s", name, directory)
}

// maxDownloadNameAttempts bounds the search for a free name. A folder that
// already holds a thousand copies of one picture is not a case worth walking
// forever for; the save fails and says so.
const maxDownloadNameAttempts = 1000

// copyIntoDirectory copies source into directory under a name nothing else
// holds, and returns the path it wrote.
//
// The copy is chunked and checks stop between chunks, like the picker export
// does: a plain io.Copy cannot be interrupted, and a shutdown in the middle
// of a large file would leave a truncated one behind with nobody waiting to
// clean it up. What this one can do and the picker cannot is remove its own
// partial file — the destination is an ordinary path we chose.
func copyIntoDirectory(source, directory, name string, stop <-chan struct{}) (string, error) {
	in, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", filepath.Base(source), err)
	}
	defer func() { _ = in.Close() }()

	out, unique, err := createUniqueDownload(directory, name)
	if err != nil {
		return "", err
	}
	destination := filepath.Join(directory, unique)

	err = copyChunked(out, in, stop)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(destination)
		return "", err
	}
	return destination, nil
}

// downloadCopyChunkBytes is the copy's working buffer, the same 256KB the
// picker export uses.
const downloadCopyChunkBytes = 256 << 10

func copyChunked(out io.Writer, in io.Reader, stop <-chan struct{}) error {
	buffer := make([]byte, downloadCopyChunkBytes)
	for {
		select {
		case <-stop:
			return errors.New("save aborted: the application is shutting down")
		default:
		}
		read, readErr := in.Read(buffer)
		if read > 0 {
			if _, writeErr := out.Write(buffer[:read]); writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read: %w", readErr)
		}
	}
}
