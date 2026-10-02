package identity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The guards below hold the rules that make ParsePublicKey the single door
// for a signing key: nobody calls the stdlib verifier directly (it accepts
// small-order keys) except the one file that owns the type, and nobody builds
// a PublicKey by any route other than ParsePublicKey. They are checked on the
// syntax tree of every Go file in the module, tests included — a test that
// verifies with stdlib is a test that would pass on a forged key. The owning
// file is named, not the package: a package-wide exemption would let any new
// file of this package call stdlib, and the exemption would be invisible.

// ed25519ImportPaths are the packages whose Verify accepts small-order keys.
var ed25519ImportPaths = map[string]bool{
	"crypto/ed25519":              true,
	"golang.org/x/crypto/ed25519": true,
}

// forbiddenVerifiers are the functions of those packages that check a
// signature.
var forbiddenVerifiers = map[string]bool{
	"Verify":            true,
	"VerifyWithOptions": true,
}

const (
	identityImportPath = "github.com/piratecash/corsa/internal/core/identity"
	testutilImportPath = "github.com/piratecash/corsa/internal/testutil/"

	// publicKeyOwnerFile is the only production file that may touch the
	// stdlib verifier and PublicKey's field.
	publicKeyOwnerFile = "publickey.go"
	// stdlibHazardTestFile measures the stdlib behaviour the type exists
	// for (TestStdlibAcceptsTheUniversalSignature), so it alone among the
	// tests calls stdlib directly.
	stdlibHazardTestFile = "publickey_test.go"
)

// goFile is one parsed file of the module with where it lives.
type goFile struct {
	path       string
	file       *ast.File
	fset       *token.FileSet
	inIdentity bool
}

func (f goFile) base() string { return filepath.Base(f.path) }

func (f goFile) isTest() bool { return strings.HasSuffix(f.path, "_test.go") }

func TestNoDirectEd25519VerifyOutsideTheOwnerFile(t *testing.T) {
	t.Parallel()
	var offenders []string
	walkModuleGoFiles(t, func(f goFile) {
		if f.inIdentity && (f.base() == publicKeyOwnerFile || f.base() == stdlibHazardTestFile) {
			return
		}
		offenders = append(offenders, directVerifyReferences(f.file, f.fset)...)
	})
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Fatalf("direct ed25519 verification outside internal/core/identity/%s — use identity.ParsePublicKey(...).Verify:\n%s",
			publicKeyOwnerFile, strings.Join(offenders, "\n"))
	}
}

// TestDirectVerifyDetectionCoversEveryImportForm pins the detector itself on
// synthetic sources: both packages whose Verify accepts small-order keys, an
// alias, a function value, and a dot-import. A detector that missed one of
// these would report a clean module that is not clean.
func TestDirectVerifyDetectionCoversEveryImportForm(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"stdlib":            "package p\nimport \"crypto/ed25519\"\nvar _ = ed25519.Verify(nil, nil, nil)\n",
		"x_crypto":          "package p\nimport \"golang.org/x/crypto/ed25519\"\nvar _ = ed25519.Verify(nil, nil, nil)\n",
		"x_crypto_alias":    "package p\nimport edx \"golang.org/x/crypto/ed25519\"\nvar _ = edx.Verify(nil, nil, nil)\n",
		"function_value":    "package p\nimport \"crypto/ed25519\"\nvar verify = ed25519.Verify\n",
		"verify_with_opts":  "package p\nimport \"crypto/ed25519\"\nvar _ = ed25519.VerifyWithOptions(nil, nil, nil, nil)\n",
		"dot_import_stdlib": "package p\nimport . \"crypto/ed25519\"\nvar _ = Sign\n",
	}
	for name, src := range cases {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name+".go", src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
		if got := directVerifyReferences(file, fset); len(got) == 0 {
			t.Errorf("%s: the detector found nothing in %q", name, src)
		}
	}

	clean := "package p\nimport \"crypto/ed25519\"\nvar _ = ed25519.Sign\n"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "clean.go", clean, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("clean: parse: %v", err)
	}
	if got := directVerifyReferences(file, fset); len(got) != 0 {
		t.Fatalf("the detector flagged a file that only signs: %v", got)
	}
}

// directVerifyReferences lists every reference to a forbidden verifier in
// file, whatever the import name; a dot-import of an ed25519 package is
// reported as such, since it hides the qualifier the check reads.
func directVerifyReferences(file *ast.File, fset *token.FileSet) []string {
	names := importNames(file, ed25519ImportPaths, "ed25519")
	var out []string
	if names["."] {
		out = append(out, fset.Position(file.Package).String()+": dot-import of an ed25519 package")
	}
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && names[pkg.Name] && forbiddenVerifiers[sel.Sel.Name] {
			out = append(out, fset.Position(sel.Pos()).String())
		}
		return true
	})
	return out
}

func TestPublicKeyIsBuiltOnlyByParsePublicKey(t *testing.T) {
	t.Parallel()
	var offenders []string
	walkModuleGoFiles(t, func(f goFile) {
		if !f.inIdentity {
			offenders = append(offenders, foreignPublicKeyLiterals(f.file, f.fset)...)
			return
		}
		if f.base() == publicKeyOwnerFile {
			offenders = append(offenders, publicKeyLiteralsOutsideParse(f.file, f.fset)...)
			return
		}
		offenders = append(offenders, publicKeyFieldTouches(f, f.fset)...)
	})
	if len(offenders) > 0 {
		sort.Strings(offenders)
		offenders = slices.Compact(offenders)
		t.Fatalf("identity.PublicKey built or written outside ParsePublicKey:\n%s", strings.Join(offenders, "\n"))
	}
}

// foreignPublicKeyLiterals reports non-empty identity.PublicKey{...} literals
// in another package. The field is unexported, so today such a literal does
// not compile; the check keeps it that way if a field is ever exported.
func foreignPublicKeyLiterals(file *ast.File, fset *token.FileSet) []string {
	names := importNames(file, map[string]bool{identityImportPath: true}, "identity")
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && names[pkg.Name] && sel.Sel.Name == "PublicKey" && len(lit.Elts) > 0 {
			out = append(out, fset.Position(lit.Pos()).String())
		}
		return true
	})
	return out
}

// publicKeyFieldTouches reports, in a file of this package other than the
// owner, any non-empty PublicKey{...} literal, any assignment to a `.key`
// selector, and — in production files — any `.key` read at all. Tests may
// read a field named key on their own table structs; they may not write one
// into a PublicKey, which an assignment would be the only way to do.
func publicKeyFieldTouches(f goFile, fset *token.FileSet) []string {
	var out []string
	ast.Inspect(f.file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CompositeLit:
			if ident, ok := node.Type.(*ast.Ident); ok && ident.Name == "PublicKey" && len(node.Elts) > 0 {
				out = append(out, fset.Position(node.Pos()).String())
			}
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "key" {
					out = append(out, fset.Position(sel.Pos()).String())
				}
			}
		case *ast.SelectorExpr:
			if !f.isTest() && node.Sel.Name == "key" {
				out = append(out, fset.Position(node.Pos()).String())
			}
		}
		return true
	})
	return out
}

// publicKeyLiteralsOutsideParse reports PublicKey{...} literals in the owner
// file that are not inside ParsePublicKey. The empty literal is the zero
// value every error return needs; it holds no key and verifies nothing, so it
// is not a way around the parser.
func publicKeyLiteralsOutsideParse(file *ast.File, fset *token.FileSet) []string {
	var offenders []string
	for _, decl := range file.Decls {
		fn, isFunc := decl.(*ast.FuncDecl)
		if isFunc && fn.Recv == nil && fn.Name.Name == "ParsePublicKey" {
			continue
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := lit.Type.(*ast.Ident); ok && ident.Name == "PublicKey" && len(lit.Elts) > 0 {
				offenders = append(offenders, fset.Position(lit.Pos()).String())
			}
			return true
		})
	}
	return offenders
}

// TestProductionCodeDoesNotImportTestutil keeps forgery helpers
// (testutil/edforgery) and every other test-only package out of the binary.
func TestProductionCodeDoesNotImportTestutil(t *testing.T) {
	t.Parallel()
	var offenders []string
	walkModuleGoFiles(t, func(f goFile) {
		if f.isTest() {
			return
		}
		if strings.Contains(filepath.ToSlash(f.path), "/internal/testutil/") {
			return
		}
		for _, spec := range f.file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err == nil && strings.HasPrefix(path, testutilImportPath) {
				offenders = append(offenders, f.fset.Position(spec.Pos()).String()+": "+path)
			}
		}
	})
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Fatalf("production code imports a test-only package:\n%s", strings.Join(offenders, "\n"))
	}
}

// importNames returns the local names under which file imports any of paths;
// "." marks a dot-import. Blank imports are ignored: they bind no name.
func importNames(file *ast.File, paths map[string]bool, defaultName string) map[string]bool {
	names := map[string]bool{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || !paths[path] {
			continue
		}
		switch {
		case spec.Name == nil:
			names[defaultName] = true
		case spec.Name.Name != "_":
			names[spec.Name.Name] = true
		}
	}
	return names
}

// walkModuleGoFiles parses every Go file of the module the way the go tool
// sees the tree: directories starting with "." or "_", testdata and vendor
// are not part of any package.
func walkModuleGoFiles(t *testing.T, visit func(goFile)) {
	t.Helper()
	root := moduleRoot(t)
	identityDir := filepath.Join(root, "internal", "core", "identity")
	fset := token.NewFileSet()
	parsed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		parsed++
		visit(goFile{path: path, file: file, fset: fset, inIdentity: filepath.Dir(path) == identityDir})
		return nil
	})
	if err != nil {
		t.Fatalf("walk module: %v", err)
	}
	if parsed < 100 {
		t.Fatalf("parsed only %d Go files from %s; the guard is not looking at the module", parsed, root)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}
