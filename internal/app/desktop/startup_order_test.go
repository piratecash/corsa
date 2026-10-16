package desktop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// startup_order_test.go guards that the desktop asks the node whether it may
// start before it opens any listener of its own: a node that refuses to start
// over its pin store must never be reachable, not even by pprof.

// firstCallPositions is where in Run each selector call first appears, keyed
// "receiver.Method".
func firstCallPositions(t *testing.T, file, function string) map[string]token.Pos {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	positions := make(map[string]token.Pos)
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			key := receiver.Name + "." + selector.Sel.Name
			if _, seen := positions[key]; !seen {
				positions[key] = call.Pos()
			}
			return true
		})
	}
	return positions
}

func TestDesktopChecksTheNodeRefusalBeforeAnyListener(t *testing.T) {
	t.Parallel()
	calls := firstCallPositions(t, "app.go", "Run")
	refusal, found := calls["nodeService.StartupRefusal"]
	if !found {
		t.Fatal("Run never asks nodeService.StartupRefusal")
	}
	for _, listener := range []string{"debugserver.Start", "rpcServer.StartAsync", "runtime.Start"} {
		at, found := calls[listener]
		if !found {
			t.Fatalf("Run no longer calls %s; update this guard", listener)
		}
		if at < refusal {
			t.Fatalf("%s is called before nodeService.StartupRefusal", listener)
		}
	}
}
