package appdata

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// RunningUnderGoTest reports whether the current process is a `go test` binary.
func RunningUnderGoTest() bool {
	return strings.HasSuffix(filepath.Base(os.Args[0]), ".test")
}

// DefaultDir returns the default application data directory for the current
// platform. Tests intentionally use a local `.corsa` directory in the working
// tree to keep fixtures and cleanup self-contained.
func DefaultDir() string {
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
