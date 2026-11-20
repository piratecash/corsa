package node

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeFullResponderTaggedFactorySourceClosure(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	pins := map[string]string{"internal/core/node/find_node_ready_construction.go": "79872a3bf79626c6ca5071ae2dbfc8cd6451d955a3d7eeaeeb45d20b92c17db5",
		"internal/core/node/service.go":                                        "7f3a84af24a53296b4ddbb8331f653a1eed05adc3af2a4968e827d33ec597aa4",
		"internal/core/node/find_node_connection.go":                           "149ae30e487317e1d895786b704b151e22e2fa6062831ece31ead0788024a55a",
		"internal/core/node/find_node_native_full_responder.go":                "47d19a41bc3201e30a601ebb764d0e207d67bb379954746fdb8805c6f54dba3e",
		"internal/testutil/loadstand/fullnode/main.go":                         "7bebd79ba4f3cad76487f694d3b392122c94aa141622943d576967c6b7fe5eec",
		"internal/testutil/loadstand/fullnode/route_wire_factory_stock.go":     "2402c39d8be8a7ee9af1c0b022bfbec404023c472a24211a92734dbf7fecaa42",
		"internal/testutil/loadstand/fullnode/route_wire_factory_responder.go": "965dbb9338ffb3e827ddb103635d4e266393bfa9b2f1bb282bb12a6ca6ebc139"}
	for name, want := range pins {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != want {
			t.Fatal("native closed source differs", name, err)
		}
	}
	for _, candidate := range []bool{false, true} {
		ctx := build.Default
		ctx.GOOS = "linux"
		ctx.GOARCH = "arm64"
		ctx.BuildTags = nil
		if candidate {
			ctx.BuildTags = []string{"d1_o1a_native_responder"}
		}
		for name, want := range map[string]bool{"internal/core/node/find_node_native_full_responder.go": candidate, "internal/testutil/loadstand/fullnode/route_wire_factory_responder.go": candidate, "internal/testutil/loadstand/fullnode/route_wire_factory_stock.go": !candidate} {
			path := filepath.Join(root, filepath.FromSlash(name))
			got, err := ctx.MatchFile(filepath.Dir(path), filepath.Base(path))
			if err != nil || got != want {
				t.Fatal("ordinary/tagged factory selection differs", candidate, name, got, err)
			}
		}
	}
	caller := "internal/testutil/loadstand/fullnode/route_wire_factory_responder.go:newRouteWireFullNode"
	calls := map[string]int{}
	declarations := 0
	for _, folder := range []string{"internal", "cmd", "sdk"} {
		err := filepath.WalkDir(filepath.Join(root, folder), func(path string, e fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if e.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			for _, decl := range file.Decls {
				scope := "<package>"
				var definition *ast.Ident
				if fn, ok := decl.(*ast.FuncDecl); ok {
					scope = fn.Name.Name
					definition = fn.Name
					if scope == "NewD1NativeFullResponderWithRouteWireCensus" {
						declarations++
						if filepath.ToSlash(rel) != "internal/core/node/find_node_native_full_responder.go" {
							return fmt.Errorf("unlisted native root declaration")
						}
					}
				}
				callee := map[ast.Node]bool{}
				selected := map[*ast.Ident]bool{}
				ast.Inspect(decl, func(n ast.Node) bool {
					if c, ok := n.(*ast.CallExpr); ok {
						callee[c.Fun] = true
					}
					if s, ok := n.(*ast.SelectorExpr); ok {
						selected[s.Sel] = true
					}
					return true
				})
				var invalid error
				ast.Inspect(decl, func(n ast.Node) bool {
					var ref ast.Node
					switch x := n.(type) {
					case *ast.SelectorExpr:
						if x.Sel.Name == "NewD1NativeFullResponderWithRouteWireCensus" {
							ref = x
						}
					case *ast.Ident:
						if x != definition && !selected[x] && x.Name == "NewD1NativeFullResponderWithRouteWireCensus" {
							ref = x
						}
					}
					if ref != nil {
						site := filepath.ToSlash(rel) + ":" + scope
						if !callee[ref] || site != caller {
							invalid = fmt.Errorf("unlisted native root call/alias: %s", site)
						}
						calls[site]++
					}
					return true
				})
				if invalid != nil {
					return invalid
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if declarations != 1 || len(calls) != 1 || calls[caller] != 1 {
		t.Fatal("native root not finite and single caller", declarations, calls)
	}
}
