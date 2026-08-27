package desktop

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCreateUniqueDownloadNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	create := func(name string) string {
		t.Helper()
		file, taken, err := createUniqueDownload(dir, name)
		if err != nil {
			t.Fatalf("createUniqueDownload(%q): %v", name, err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		return taken
	}

	if got := create("photo.png"); got != "photo.png" {
		t.Fatalf("free name = %q, want photo.png", got)
	}
	if got := create("photo.png"); got != "photo (2).png" {
		t.Fatalf("taken name = %q, want photo (2).png", got)
	}
	if got := create("photo.png"); got != "photo (3).png" {
		t.Fatalf("twice-taken name = %q, want photo (3).png", got)
	}
	// A name with no extension keeps its shape.
	create("notes")
	if got := create("notes"); got != "notes (2)" {
		t.Fatalf("extensionless name = %q, want notes (2)", got)
	}
	// Every one of them is still there: nothing was overwritten.
	for _, name := range []string{"photo.png", "photo (2).png", "photo (3).png", "notes", "notes (2)"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s is missing: %v", name, err)
		}
	}
}

// TestCreateUniqueDownloadUnderRace: two saves of the same name started at
// once must end up as two files. Picking the name and creating the file are
// one step for exactly this reason.
func TestCreateUniqueDownloadUnderRace(t *testing.T) {
	dir := t.TempDir()
	const savers = 8
	names := make(chan string, savers)
	errs := make(chan error, savers)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < savers; i++ {
		go func() {
			start.Wait()
			file, name, err := createUniqueDownload(dir, "photo.png")
			if err != nil {
				errs <- err
				return
			}
			_ = file.Close()
			names <- name
		}()
	}
	start.Done()

	seen := map[string]bool{}
	for i := 0; i < savers; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent save failed instead of taking the next name: %v", err)
		case name := <-names:
			if seen[name] {
				t.Fatalf("%s was handed out twice", name)
			}
			seen[name] = true
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != savers {
		t.Fatalf("%d files for %d saves", len(entries), savers)
	}
}

func TestCopyIntoDirectoryWritesBesideWhatIsThere(t *testing.T) {
	source := filepath.Join(t.TempDir(), "photo.png")
	if err := os.WriteFile(source, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	destination := t.TempDir()
	stop := make(chan struct{})

	first, err := copyIntoDirectory(source, destination, "photo.png", stop)
	if err != nil {
		t.Fatalf("copyIntoDirectory: %v", err)
	}
	if content, err := os.ReadFile(first); err != nil || string(content) != "image data" {
		t.Fatalf("saved content = %q, %v", content, err)
	}

	second, err := copyIntoDirectory(source, destination, "photo.png", stop)
	if err != nil {
		t.Fatalf("second copy: %v", err)
	}
	if filepath.Base(second) != "photo (2).png" {
		t.Fatalf("second copy = %s, want photo (2).png beside the first", filepath.Base(second))
	}
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("the first save was overwritten: %v", err)
	}
}

// TestCopyIntoDirectoryLeavesNothingBehindOnAbort: a shutdown mid-copy must
// not leave a truncated file in the user's downloads folder — the
// destination is a path this code chose, so it can take it back.
func TestCopyIntoDirectoryLeavesNothingBehindOnAbort(t *testing.T) {
	source := filepath.Join(t.TempDir(), "big.bin")
	if err := os.WriteFile(source, make([]byte, 4*downloadCopyChunkBytes), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	destination := t.TempDir()
	stop := make(chan struct{})
	close(stop)

	if _, err := copyIntoDirectory(source, destination, "big.bin", stop); err == nil {
		t.Fatal("an aborted copy must report the abort")
	}
	entries, err := os.ReadDir(destination)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("the aborted copy left %d file(s) behind", len(entries))
	}
}
