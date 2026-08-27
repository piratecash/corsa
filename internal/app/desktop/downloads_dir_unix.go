//go:build !windows

package desktop

import (
	"os"
	"path/filepath"
	"strings"
)

// downloads_dir_unix.go resolves the downloads folder the way the desktops
// that follow the XDG base-directory specification publish it — which is also
// the right answer on macOS, where nothing publishes anything and the folder
// is simply ~/Downloads.

// platformDownloadsDir is the folder saved files go to.
//
// Three sources, in the order of how well each one knows the answer. The
// XDG_DOWNLOAD_DIR environment variable is first because a session that
// exports it means it. Next is ~/.config/user-dirs.dirs, which is where the
// XDG user dirs actually live — the variable is usually NOT exported, and
// this file is the only place that knows a folder the user moved or that the
// desktop created under a localized name ("Загрузки", "Téléchargements").
// Last is ~/Downloads.
//
// Going straight to ~/Downloads saves into a folder the user's file manager
// does not show them, and on a localized desktop creates a second one beside
// the real one.
func platformDownloadsDir() (string, error) {
	home, homeErr := os.UserHomeDir()

	if dir := expandUserDir(strings.TrimSpace(os.Getenv("XDG_DOWNLOAD_DIR")), home); isDirectory(dir) {
		return dir, nil
	}
	if homeErr == nil {
		if dir := xdgUserDir("XDG_DOWNLOAD_DIR", home); isDirectory(dir) {
			return dir, nil
		}
	}
	return homeDownloadsDir()
}

// xdgUserDirsFile is where xdg-user-dirs-update writes what the desktop's
// folders are called and where they are.
const xdgUserDirsFile = "user-dirs.dirs"

// xdgUserDir reads one entry out of the XDG user-dirs configuration, or
// returns "" when there is no such file or no such entry.
//
// The format is a handful of shell assignments, one per line:
//
//	# comment
//	XDG_DOWNLOAD_DIR="$HOME/Загрузки"
//
// Parsed rather than sourced: this is configuration, not a script to run, and
// the only substitution it is allowed is the $HOME the file itself uses.
func xdgUserDir(name, home string) string {
	configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
	if configHome == "" {
		configHome = filepath.Join(home, ".config")
	}
	content, err := os.ReadFile(filepath.Join(configHome, xdgUserDirsFile))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || strings.TrimSpace(key) != name {
			continue
		}
		return expandUserDir(strings.Trim(strings.TrimSpace(value), `"'`), home)
	}
	return ""
}

// expandUserDir resolves the one variable an XDG user-dirs value may carry
// and rejects anything that is not an absolute path afterwards — a relative
// folder here would resolve against the working directory, which for a
// desktop application is wherever it happened to be started.
func expandUserDir(value, home string) string {
	if value == "" {
		return ""
	}
	if home != "" {
		if rest, found := strings.CutPrefix(value, "$HOME"); found {
			value = filepath.Join(home, strings.TrimPrefix(rest, "/"))
		} else if rest, found := strings.CutPrefix(value, "${HOME}"); found {
			value = filepath.Join(home, strings.TrimPrefix(rest, "/"))
		}
	}
	if !filepath.IsAbs(value) {
		return ""
	}
	return value
}
