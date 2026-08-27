//go:build !windows

package desktop

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUserDownloadsDirReadsTheXDGConfig(t *testing.T) {
	home := t.TempDir()
	config := filepath.Join(home, ".config")
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// The localized folder a desktop actually creates, named in the file
	// that holds the XDG user dirs — the variable itself is not exported.
	localized := filepath.Join(home, "Загрузки")
	if err := os.MkdirAll(localized, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, "Downloads"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	contents := strings.Join([]string{
		"# generated",
		`XDG_DESKTOP_DIR="$HOME/Desktop"`,
		`XDG_DOWNLOAD_DIR="$HOME/Загрузки"`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(config, xdgUserDirsFile), []byte(contents), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := xdgUserDir("XDG_DOWNLOAD_DIR", home); got != localized {
		t.Fatalf("xdgUserDir = %q, want %q", got, localized)
	}
	if got := xdgUserDir("XDG_VIDEOS_DIR", home); got != "" {
		t.Fatalf("missing entry = %q, want empty", got)
	}

	t.Setenv("XDG_CONFIG_HOME", config)
	t.Setenv("HOME", home)
	got, err := userDownloadsDir()
	if err != nil {
		t.Fatalf("userDownloadsDir: %v", err)
	}
	if got != localized {
		t.Fatalf("dir = %q, want the folder the desktop published (%q)", got, localized)
	}
}

func TestExpandUserDirRejectsRelativePaths(t *testing.T) {
	home := "/home/user"
	if got := expandUserDir("$HOME/Downloads", home); got != "/home/user/Downloads" {
		t.Fatalf("$HOME = %q", got)
	}
	if got := expandUserDir("${HOME}/Downloads", home); got != "/home/user/Downloads" {
		t.Fatalf("${HOME} = %q", got)
	}
	if got := expandUserDir("Downloads", home); got != "" {
		t.Fatalf("relative path = %q, want empty: it would resolve against the working directory", got)
	}
	if got := expandUserDir("", home); got != "" {
		t.Fatalf("empty value = %q", got)
	}
}

func TestUserDownloadsDirPrefersTheDesktopSetting(t *testing.T) {
	published := t.TempDir()
	t.Setenv("XDG_DOWNLOAD_DIR", published)
	got, err := userDownloadsDir()
	if err != nil || got != published {
		t.Fatalf("dir = %q, %v; want the published folder %q", got, err, published)
	}

	// A setting that names something that is not a folder falls through to
	// the platform default rather than saving into nowhere.
	t.Setenv("XDG_DOWNLOAD_DIR", filepath.Join(published, "not-a-directory"))
	if got, _ := userDownloadsDir(); got == filepath.Join(published, "not-a-directory") {
		t.Fatal("a stale XDG_DOWNLOAD_DIR must not be used")
	}
}
