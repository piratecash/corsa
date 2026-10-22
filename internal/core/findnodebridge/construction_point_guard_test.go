package findnodebridge

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func constructionPointSourceErrors(raw []byte) []string {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "bc24d3f59e3585a6e6224efe1f23ff6eea8bc74356dd3523a1bf7d544dbcc1e1" {
		return []string{"construction point exact provenance Source changed"}
	}
	return nil
}
func TestConstructionPointClosedSource(t *testing.T) {
	raw, err := os.ReadFile("construction_point.go")
	if err != nil {
		t.Fatal(err)
	}
	if failures := constructionPointSourceErrors(raw); len(failures) != 0 {
		t.Fatal(failures)
	}
	root := filepath.Join("..", "..", "..")
	refs := map[string]int{}
	declarations := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".gocache", ".gomodcache", ".golangci-lint-cache", "refactoring":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, decl := range file.Decls {
			site := rel + ":<package>"
			var definition *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = rel + ":" + fn.Name.Name
				definition = fn.Name
				if fn.Name.Name == "MatchesPoint" {
					declarations++
					if rel != "internal/core/findnodebridge/construction_point.go" {
						t.Error("foreign construction point declaration", site)
					}
				}
			}
			direct := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						direct[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "MatchesPoint" && id != definition {
					refs[site]++
					if !direct[id] {
						t.Error("construction point method-value escape", site)
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if declarations != 1 || !reflect.DeepEqual(refs, map[string]int{"internal/core/findnode/owned_bridge_bootstrap.go:NewExecutingBootstrapIntake": 2, "internal/core/findnode/owned_bridge_local_target.go:NewLocalTarget": 1, "internal/core/findnode/owned_bridge_local_target.go:WaitDelay": 1, "internal/core/findnode/owned_bridge_local_target.go:Complete": 1}) {
		t.Fatal("construction point exact caller inventory", declarations, refs)
	}
}
func TestConstructionPointSourceControls(t *testing.T) {
	raw, err := os.ReadFile("construction_point.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, before := range []string{"b.valid(consumerRole)", "at.valid()", "b.owner == at.instantDomain", "b.kind == at.instantKind"} {
		if strings.Count(string(raw), before) != 1 {
			t.Fatal("unique construction point Source anchor", before)
		}
		mutated := []byte(strings.Replace(string(raw), before, "true", 1))
		if len(constructionPointSourceErrors(mutated)) == 0 {
			t.Fatal("construction point provenance control escaped", before)
		}
	}
}
