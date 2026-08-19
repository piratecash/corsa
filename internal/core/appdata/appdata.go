package appdata

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// RunningUnderGoTest reports whether the current process is a `go test` binary.
func RunningUnderGoTest() bool {
	// Windows names the binary "<pkg>.test.exe", so a plain ".test" suffix
	// check reports false there — and DefaultDir then resolves to the real
	// %AppData%\CorsaCore, with the user's identity, state database and
	// message history in it.
	name := filepath.Base(os.Args[0])
	return strings.HasSuffix(name, ".test") || strings.HasSuffix(name, ".test.exe")
}

// baseDirOverride, when non-empty, wins over all platform detection in
// DefaultDir. Mobile entry points set it at startup. On Android the
// default branch below does resolve (Gio sets HOME=filesDir before
// calling main), but to $HOME/.corsacore — inside the directory Android
// Auto Backup copies, which must never hold identity keys or the chat
// log. cmd/corsa-android therefore anchors this to the app's no-backup
// dir; see cmd/corsa-android/datadir_android.go, which also migrates
// data left behind at the old locations.
var baseDirOverride string

// SetDir overrides the directory returned by DefaultDir. Call it before
// anything queries DefaultDir (first thing in main, before crashlog
// setup), from a single goroutine; it is not synchronized.
func SetDir(dir string) {
	baseDirOverride = strings.TrimSpace(dir)
}

// DefaultDir returns the default application data directory for the current
// platform. Tests intentionally use a local `.corsa` directory in the working
// tree to keep fixtures and cleanup self-contained.
func DefaultDir() string {
	if baseDirOverride != "" {
		return baseDirOverride
	}
	if RunningUnderGoTest() {
		return ".corsa"
	}
	switch runtime.GOOS {
	case "windows":
		if dir, err := os.UserConfigDir(); err == nil && strings.TrimSpace(dir) != "" {
			return filepath.Join(dir, "CorsaCore")
		}
	case "darwin":
		if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
			return filepath.Join(home, "Library", "Application Support", "CorsaCore")
		}
	default:
		if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
			return filepath.Join(home, ".corsacore")
		}
	}
	return ".corsa"
}
