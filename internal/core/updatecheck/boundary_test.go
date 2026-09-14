package updatecheck

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// This package is the one place the program opens a connection to a third
// party, outside the p2p transport, which shows that party the operator's IP.
// Consent for that lives in the desktop preferences and is asked for in the
// desktop UI — so the UI is the only thing allowed to import this package.
//
// Written as a guard rather than a sentence in a doc comment because the rule
// has to hold for code nobody has written yet. A headless node has no user to
// ask, and the day someone wires a "convenient" version check into node.Service
// or the SDK, a build of corsa-node starts phoning home and nothing says so.
// The same shape as internal/core/storage/boundary_test.go, for the same
// reason.

const importPath = "github.com/piratecash/corsa/internal/core/updatecheck"

// allowedImporterPrefixes are the trees that may import this package. The
// desktop application is the UI: it has a window, a preferences file and a
// checkbox, which is what makes consent possible there and nowhere else.
var allowedImporterPrefixes = []string{
	"internal/app/desktop",
	"internal/core/updatecheck",
}

func TestOnlyTheUIImportsTheReleaseCheck(t *testing.T) {
	root := moduleRoot(t)

	var offenders []string
	for _, tree := range []string{"internal", "sdk", "cmd"} {
		walkGoFiles(t, filepath.Join(root, tree), func(path string) {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatalf("relative path for %s: %v", path, err)
			}
			relative = filepath.ToSlash(relative)
			if allowedImporter(relative) {
				return
			}
			if importsReleaseCheck(t, path) {
				offenders = append(offenders, relative)
			}
		})
	}

	if len(offenders) > 0 {
		t.Fatalf("these files import %s, which only the UI may:\n  %s\n"+
			"a build that reaches this package without a user to consent contacts a third party unasked",
			importPath, strings.Join(offenders, "\n  "))
	}
}

func allowedImporter(relative string) bool {
	for _, prefix := range allowedImporterPrefixes {
		if relative == prefix || strings.HasPrefix(relative, prefix+"/") {
			return true
		}
	}
	return false
}

func importsReleaseCheck(t *testing.T, path string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		if value == importPath {
			return true
		}
	}
	return false
}

// walkGoFiles visits every non-test .go file under dir.
func walkGoFiles(t *testing.T, dir string, visit func(path string)) {
	t.Helper()
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		visit(path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", root, err)
	}
	return root
}
