package appdata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoTestDetectionCoversWindowsBinaries(t *testing.T) {
	// Windows builds the test binary as "<pkg>.test.exe". A plain ".test"
	// suffix check reports false there, so DefaultDir resolves to the real
	// %AppData%\CorsaCore — and any test that cleans up after itself would be
	// cleaning up the user's identity, state database and message history.
	original := os.Args[0]
	t.Cleanup(func() { os.Args[0] = original })

	for name, want := range map[string]bool{
		"desktop.test":      true,
		"desktop.test.exe":  true,
		"corsa-desktop":     false,
		"corsa-desktop.exe": false,
	} {
		os.Args[0] = filepath.Join("some", "dir", name)
		if got := RunningUnderGoTest(); got != want {
			t.Fatalf("RunningUnderGoTest() = %t for %q, want %t", got, name, want)
		}
	}
}
