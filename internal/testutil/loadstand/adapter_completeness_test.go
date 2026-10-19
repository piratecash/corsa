package loadstand

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/datagram"
)

// These guards compare the source owner's declarations with the adapter's actual
// finite maps/required fields. A newly added enum or R1 family needs an explicit
// adapter decision even when an empty fixture would otherwise keep passing.
func TestAdapterOwnerMappingCompleteness(t *testing.T) {
	for _, pair := range []struct{ owner, prefix, adapter, variable string }{
		{"../../core/routing/mode_selection.go", "AnnounceOperation", "rollout_source.go", "modeOperations"},
		{"../../core/routing/mode_selection.go", "AnnounceMode", "rollout_source.go", "announceModes"},
		{"../../core/routing/mode_selection.go", "ModeReason", "rollout_source.go", "modeReasons"},
		{"../../core/domain/resource_breakdown.go", "ResourceGauge", "resource_source.go", "resourceGaugeKinds"},
	} {
		t.Run(pair.variable, func(t *testing.T) {
			equalMapping(t, constantNames(parseSource(t, pair.owner), pair.prefix), variableSelectors(t, parseSource(t, pair.adapter), pair.variable))
		})
	}
	equalMapping(t, variableSelectors(t, parseSource(t, "../../core/node/rollout_metrics.go"), "rolloutCapabilities"), variableSelectors(t, parseSource(t, "rollout_source.go"), "neighbourCapabilities"))
	equalMapping(t, structJSONFields(t, parseSource(t, "../../core/node/ban_diagnostics.go"), "banDiagnosticsSnapshot"), structJSONFields(t, parseSource(t, "source.go"), "BanObservation"))
	equalMapping(t, structJSONFields(t, parseSource(t, "source.go"), "BanObservation"), requiredCallFields(t, parseSource(t, "bans.go"), "readCompleteBans", "commandObject", 2))
	equalMapping(t, publicFieldNames(reflect.TypeFor[datagram.MetricsSnapshot]()), requiredCallFields(t, parseSource(t, "datagram_source.go"), "decodeDatagramMetrics", "object", 1))
	// The node's private test bridge must handle every exported ban result.
	equalMapping(t, constantNames(parseSource(t, "bans.go"), "Ban"), variableSelectors(t, parseSource(t, "../../core/node/loadstand_process_source_test.go"), "processBanKinds"))
}
func parseSource(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}
func constantNames(file *ast.File, prefix string) []string {
	var names []string
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			for _, name := range spec.(*ast.ValueSpec).Names {
				if strings.HasPrefix(name.Name, prefix) {
					names = append(names, name.Name)
				}
			}
		}
	}
	return names
}
func variableSelectors(t *testing.T, file *ast.File, name string) []string {
	t.Helper()
	var names []string
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.VAR {
			continue
		}
		for _, spec := range group.Specs {
			value := spec.(*ast.ValueSpec)
			if len(value.Names) != 1 || value.Names[0].Name != name {
				continue
			}
			literal := value.Values[0].(*ast.CompositeLit)
			for _, element := range literal.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					element = pair.Key
				}
				if selector, ok := element.(*ast.SelectorExpr); ok {
					names = append(names, selector.Sel.Name)
				} else {
					t.Fatal("mapping needs explicit owner constants")
				}
			}
		}
	}
	if len(names) == 0 {
		t.Fatalf("mapping %s missing", name)
	}
	return names
}
func structJSONFields(t *testing.T, file *ast.File, name string) []string {
	t.Helper()
	var names []string
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || spec.Name.Name != name {
			return true
		}
		row := spec.Type.(*ast.StructType)
		for _, field := range row.Fields.List {
			if field.Tag == nil {
				t.Fatal("owner field lacks explicit JSON name")
			}
			tag, err := strconv.Unquote(field.Tag.Value)
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, strings.Split(reflect.StructTag(tag).Get("json"), ",")[0])
		}
		return false
	})
	if len(names) == 0 {
		t.Fatalf("owner %s missing", name)
	}
	return names
}
func requiredCallFields(t *testing.T, file *ast.File, function, callee string, skip int) []string {
	t.Helper()
	var names []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := ""
			switch f := call.Fun.(type) {
			case *ast.Ident:
				name = f.Name
			case *ast.SelectorExpr:
				name = f.Sel.Name
			}
			if name != callee || names != nil {
				return true
			}
			for _, arg := range call.Args[skip:] {
				literal, ok := arg.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					t.Fatal("required names must be explicit")
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				names = append(names, value)
			}
			return true
		})
	}
	if len(names) == 0 {
		t.Fatal("required field guard missing")
	}
	return names
}
func publicFieldNames(row reflect.Type) []string {
	names := make([]string, 0, row.NumField())
	for i := 0; i < row.NumField(); i++ {
		names = append(names, row.Field(i).Name)
	}
	return names
}
func equalMapping(t *testing.T, owner, adapter []string) {
	t.Helper()
	slices.Sort(owner)
	slices.Sort(adapter)
	if !slices.Equal(owner, adapter) {
		t.Fatalf("owner changed without complete adapter mapping: owner=%v adapter=%v", owner, adapter)
	}
}
