package desktop

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// downloads_dir_windows.go asks Windows where the downloads folder is,
// instead of assuming.
//
// %USERPROFILE%\Downloads is only the DEFAULT. The folder is a Known Folder:
// the user can move it (Properties → Location), OneDrive redirects it as part
// of Backup, and domain policy relocates it wholesale. When that has happened
// the old path usually still exists — empty, or holding whatever was left
// behind — so guessing does not fail loudly, it quietly saves into a folder
// nobody looks at.

// platformDownloadsDir returns the Known Folder path for Downloads, falling
// back to the default location when the shell cannot answer.
//
// KF_FLAG_DEFAULT, so the answer is where the folder IS rather than where it
// would be by default, and the path is verified to exist — the fallback below
// is what handles a folder that was moved to a disconnected drive.
func platformDownloadsDir() (string, error) {
	path, err := windows.KnownFolderPath(windows.FOLDERID_Downloads, windows.KF_FLAG_DEFAULT)
	if err == nil && isDirectory(path) {
		return path, nil
	}
	dir, homeErr := homeDownloadsDir()
	if homeErr == nil {
		return dir, nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: known folder: %w", homeErr, err)
	}
	return "", homeErr
}
