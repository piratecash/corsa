package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// startup_order_test.go guards that the headless node asks the service
// whether it may start before it opens any listener of its own (pprof, RPC):
// a node that refuses to start over its pin store must never be reachable.

func TestHeadlessNodeChecksTheRefusalBeforeAnyListener(t *testing.T) {
	t.Parallel()
	parsed, err := parser.ParseFile(token.NewFileSet(), "app.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	positions := make(map[string]token.Pos)
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name.Name != "Run" {
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
			name := selector.Sel.Name
			if receiver, ok := selector.X.(*ast.Ident); ok {
				name = receiver.Name + "." + name
			}
			if _, seen := positions[name]; !seen {
				positions[name] = call.Pos()
			}
			return true
		})
	}
	refusal, found := positions["StartupRefusal"]
	if !found {
		t.Fatal("Run never asks the service's StartupRefusal")
	}
	for _, listener := range []string{"debugserver.Start", "rpcServer.StartAsync"} {
		at, found := positions[listener]
		if !found {
			t.Fatalf("Run no longer calls %s; update this guard", listener)
		}
		if at < refusal {
			t.Fatalf("%s is called before StartupRefusal", listener)
		}
	}
}
