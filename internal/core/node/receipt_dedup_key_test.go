package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The dedup key was spelled out by hand at seven sites, and adding the
// sender to it meant editing all seven together: one that kept the old
// spelling would insert under a key the eviction path could no longer
// delete, or — worse — read a key nobody writes and treat every receipt
// as new. So the shape lives in receiptIdentity.dedupKey, and this sentinel makes
// the next site use it rather than re-derive it.
func TestEveryDedupKeyComesFromTheOnePlaceThatSpellsIt(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fromKeyBuilder := func(expr ast.Expr) bool {
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return false
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			return fun.Name == "receiptKeyOf"
		case *ast.SelectorExpr:
			// receiptIdentity.dedupKey, however the identity was obtained.
			return fun.Sel.Name == "dedupKey"
		}
		return false
	}

	var offenders []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			decl, ok := n.(*ast.FuncDecl)
			if !ok || decl.Body == nil {
				return true
			}
			// Identifiers this function assigned from the key builder.
			builtHere := map[string]bool{}
			ast.Inspect(decl.Body, func(inner ast.Node) bool {
				assign, ok := inner.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					return true
				}
				lhs, ok := assign.Lhs[0].(*ast.Ident)
				if ok && fromKeyBuilder(assign.Rhs[0]) {
					builtHere[lhs.Name] = true
				}
				return true
			})

			ast.Inspect(decl.Body, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				method, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				set, ok := method.X.(*ast.SelectorExpr)
				if !ok || set.Sel.Name != "seenReceipts" || len(call.Args) == 0 {
					return true
				}
				arg := call.Args[0]
				if fromKeyBuilder(arg) {
					return true
				}
				// A key the enclosing function received or built from the
				// builder is fine; anything else is a second spelling.
				if ident, ok := arg.(*ast.Ident); ok && (builtHere[ident.Name] || isParamNamed(decl, ident.Name)) {
					return true
				}
				offenders = append(offenders, name+":"+fset.Position(call.Pos()).String()+
					" s.seenReceipts."+method.Sel.Name+" with a key not built by receiptIdentity.dedupKey")
				return true
			})
			return true
		})
	}

	if len(offenders) > 0 {
		t.Errorf("receipt dedup keys built outside receiptIdentity.dedupKey:\n\t%s", strings.Join(offenders, "\n\t"))
	}
}

func isParamNamed(decl *ast.FuncDecl, want string) bool {
	if decl.Type.Params == nil {
		return false
	}
	for _, field := range decl.Type.Params.List {
		for _, ident := range field.Names {
			if ident.Name == want {
				return true
			}
		}
	}
	return false
}
