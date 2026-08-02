//go:build !android

package desktop

import (
	"net/url"
	"os/exec"
	"path/filepath"
	"runtime"
)

// isAndroid mirrors runtime.GOOS == "android" as a build-tagged
// constant. Needed where the runtime package is unusable — desktop.Run
// shadows the identifier with its local NodeRuntime variable — and lets
// the compiler drop dead branches.
const isAndroid = false

// exportFileName returns the name to hand to the platform save dialog.
// Only the Android exporter needs a MIME-driven adjustment (see
// open_android.go); desktop dialogs take any name as-is.
func exportFileName(name string) string {
	return name
}

// openBrowser opens url in the system default browser.
// On macOS: open, on Windows: rundll32, on Linux: xdg-open.
// The Android variant (an ACTION_VIEW intent) lives in open_android.go.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

// openFile opens a local file with the system default application.
// On macOS: open, on Windows: rundll32, on Linux: xdg-open.
func openFile(path string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", path)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", path)
	default:
		cmd = exec.Command("xdg-open", path)
	}
	_ = cmd.Start()
}

// revealFileInDir opens the system file manager with the file selected
// (highlighted). On macOS Finder selects the file via "open -R". On
// Windows Explorer selects via "/select,". On Linux there is no universal
// "select file" protocol, so we open the containing directory and, as a
// best-effort, try dbus-based file selection (Nautilus/Dolphin/Thunar)
// before falling back to xdg-open on the parent directory.
func revealFileInDir(path string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		// -R = reveal in Finder and select the file.
		cmd = exec.Command("open", "-R", path)
	case "windows":
		// /select, highlights the file in Explorer.
		cmd = exec.Command("explorer", "/select,", path)
	default:
		// Best-effort: try dbus-send to org.freedesktop.FileManager1 which
		// is supported by Nautilus, Dolphin, Thunar, and other modern file
		// managers. If it fails, fall back to opening the directory.
		//
		// Build a properly escaped file:// URI via net/url so that paths
		// with spaces, #, %, Cyrillic, and other special characters are
		// transmitted correctly over D-Bus.
		fileURI := (&url.URL{Scheme: "file", Path: path}).String()
		dbusCmd := exec.Command("dbus-send", "--print-reply",
			"--dest=org.freedesktop.FileManager1",
			"/org/freedesktop/FileManager1",
			"org.freedesktop.FileManager1.ShowItems",
			"array:string:"+fileURI, "string:")
		if err := dbusCmd.Start(); err == nil {
			_ = dbusCmd.Wait()
			return
		}
		// Fallback: open the containing directory.
		cmd = exec.Command("xdg-open", filepath.Dir(path))
	}
	_ = cmd.Start()
}
