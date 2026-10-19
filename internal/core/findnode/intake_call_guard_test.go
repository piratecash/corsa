package findnode

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This is a reviewed call/field inventory, not universal dataflow verification.
func TestCandidateInactiveCallGraphAndInventories(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	allowed := map[string]bool{"intake_owner.go": true, "intake_groups.go": true, "intake_membership.go": true, "budget.go": true, "pending.go": true, "session.go": true}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || strings.HasPrefix(entry.Name(), ".gomod") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, e := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if e != nil {
			return e
		}
		for _, spec := range file.Imports {
			if strings.Trim(spec.Path.Value, "\"") == "github.com/piratecash/corsa/internal/core/findnode" {
				t.Errorf("production import: %s", path)
			}
		}
		if filepath.Dir(path) != filepath.Join(root, "internal", "core", "findnode") {
			return nil
		}
		if !allowed[entry.Name()] {
			ast.Inspect(file, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok && (strings.HasPrefix(id.Name, "candidate") || id.Name == "intakeCoordinator" || id.Name == "captureCandidateMembership") {
					t.Errorf("inactive owner escaped allowed sources: %s %s", path, id.Name)
				}
				return true
			})
			return nil
		}
		if !strings.HasPrefix(entry.Name(), "intake_") {
			return nil
		}
		global := make(map[*ast.GenDecl]bool)
		for _, declaration := range file.Decls {
			if group, ok := declaration.(*ast.GenDecl); ok {
				global[group] = true
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.GenDecl:
				if n.Tok == token.VAR && global[n] {
					for _, declaration := range n.Specs {
						value := declaration.(*ast.ValueSpec)
						for _, name := range value.Names {
							if name.Name != "errCandidateAssumption" {
								t.Errorf("unreviewed candidate variable inventory: %s", name.Name)
							}
						}
					}
				}
			case *ast.GoStmt:
				t.Error("candidate owner acquired a worker")
			case *ast.FuncDecl:
				if n.Name.IsExported() {
					t.Errorf("exported candidate function %s", n.Name)
				}
			case *ast.CallExpr:
				if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
					switch selector.Sel.Name {
					case "Reserve", "Consume", "Cancel", "CloseSession", "Expire", "allocateID":
						t.Errorf("candidate mutating wrapper bypass: %s", selector.Sel.Name)
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][]string{
		"intakeCoordinator":   {"mu", "pending", "calendars", "groups", "profile", "last", "generation", "round", "open", "cut", "cursor", "watermark", "sweep", "rrCell", "rrSession", "counters", "published"},
		"candidateGroups":     {"named", "records", "generation", "lastUse", "used"},
		"candidatePending":    {"phase", "generation", "round", "latest", "priority", "calendar", "alias", "cell"},
		"candidateMembership": {"owner", "at", "maximum", "classes", "aged"},
	}
	for name, names := range expected {
		var source string
		switch name {
		case "intakeCoordinator", "candidatePending":
			source = "intake_owner.go"
		case "candidateGroups":
			source = "intake_groups.go"
		case "candidateMembership":
			source = "intake_membership.go"
		}
		file, e := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		var actual []string
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != name {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("not struct %s", name)
			}
			for _, field := range structure.Fields.List {
				if len(field.Names) == 0 {
					t.Errorf("hidden embedded inventory in %s", name)
				}
				for _, id := range field.Names {
					actual = append(actual, id.Name)
				}
			}
			return false
		})
		sort.Strings(actual)
		sort.Strings(names)
		if !reflect.DeepEqual(actual, names) {
			t.Errorf("unreviewed state %s: %v", name, actual)
		}
	}
}
