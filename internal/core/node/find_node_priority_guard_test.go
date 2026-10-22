package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func c5SourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"findNodePriorityProfile":        {"find_node_connection.go:<package>": 1, "find_node_connection.go:valid": 1, "find_node_connection.go:findNodePriorityMembers": 1, "find_node_connection.go:findNodePrioritySnapshot": 1, "find_node_connection.go:collectOwnedFindNodeRawFacts": 1, "find_node_bridge_capture.go:<package>": 2},
		"findNodePriorityContact":        {"find_node_connection.go:<package>": 1, "find_node_connection.go:captureFindNodePriorityClasses": 2, "find_node_connection.go:findNodePriorityClassLocked": 1, "find_node_connection.go:collectOwnedFindNodeRawFacts": 1, "find_node_connection.go:collectOwnedFindNodeRawClasses": 2},
		"findNodePriorityClassLocked":    {"find_node_connection.go:captureFindNodePriorityClasses": 1, "find_node_connection.go:collectOwnedFindNodeRawClasses": 1},
		"findNodePriorityClassFact":      {"find_node_connection.go:<package>": 1, "find_node_connection.go:captureFindNodePriorityClasses": 3},
		"findNodePriorityMemberLocked":   {"find_node_connection.go:findNodePriorityMembers": 1, "find_node_connection.go:findNodePrioritySnapshot": 1},
		"findNodePriorityMembers":        {"find_node_connection.go:findNodePrioritySnapshot": 1},
		"captureFindNodePriorityClasses": {"find_node_connection.go:findNodePrioritySnapshot": 1},
		"findNodePrioritySnapshot":       {},
		"NewPrioritySnapshot":            {"find_node_connection.go:findNodePrioritySnapshot": 1},
	}
	expected["findNodePriorityProfile"]["identity_pin_own_dm_runtime.go:NewInactiveOwnDMRuntimeCandidate"] = 1
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	fields := map[string][]string{}
	fieldWant := map[string][]string{
		"findNodePriorityProfile":   {"MaxMembers", "MaxRegistryVisits"},
		"findNodePriorityContact":   {"applicable", "confirmed", "err"},
		"findNodePriorityClassFact": {"identity", "class"},
	}
	var failures []string
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			var fn *ast.FuncDecl
			if value, ok := decl.(*ast.FuncDecl); ok {
				fn = value
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
			}
			if group, ok := decl.(*ast.GenDecl); ok && path == "find_node_connection.go" && group.Tok == token.VAR {
				failures = append(failures, "priority bridge retained package variable")
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != declaration {
					if refs, tracked := actual[id.Name]; tracked {
						refs[site]++
					}
				}
				if typ, ok := n.(*ast.TypeSpec); ok {
					if _, tracked := fieldWant[typ.Name.Name]; tracked {
						shape, ok := typ.Type.(*ast.StructType)
						if !ok {
							failures = append(failures, "priority type alias/shape")
							return true
						}
						for _, field := range shape.Fields.List {
							if len(field.Names) == 0 {
								failures = append(failures, "priority embedded field")
							}
							for _, name := range field.Names {
								fields[typ.Name.Name] = append(fields[typ.Name.Name], name.Name)
							}
						}
					}
				}
				if fn == nil || path != "find_node_connection.go" || (!strings.HasPrefix(fn.Name.Name, "findNodePriority") && fn.Name.Name != "captureFindNodePriorityClasses") {
					return true
				}
				switch n := n.(type) {
				case *ast.GoStmt, *ast.SendStmt:
					failures = append(failures, "priority observer acquired background work")
				case *ast.SelectorExpr:
					if _, tracked := actual[n.Sel.Name]; tracked && (strings.HasPrefix(n.Sel.Name, "findNodePriority") || n.Sel.Name == "NewPrioritySnapshot") && !callees[n] {
						failures = append(failures, "priority method value escape")
					}
				case *ast.CallExpr:
					if s, ok := n.Fun.(*ast.SelectorExpr); ok {
						switch s.Sel.Name {
						case "findNodeMembers", "trustedContacts", "confirmPinMetadata", "confirmLocalContacts", "stagePinMetadata", "stageSelectedPin", "stageInboundPin":
							failures = append(failures, "priority observer used unbounded/mutating facade")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "priority exact reference inventory changed")
	}
	if !reflect.DeepEqual(fields, fieldWant) {
		failures = append(failures, "priority fixed field inventory changed")
	}
	compact := func(s string) string {
		return strings.Map(func(r rune) rune {
			switch r {
			case ' ', '\t', '\n', '\r':
				return -1
			}
			return r
		}, s)
	}
	raw := compact(string(sources["find_node_connection.go"]))
	for _, fragment := range []string{
		"make([]findnode.Session, 0, profile.MaxMembers)",
		"if visits == profile.MaxRegistryVisits",
		"if len(out.sessions) == profile.MaxMembers",
		"if member.Identity() != target",
		"l.token != s.proofToken",
		"b.session != expected",
		"entry.metadata.confirmedRevision == entry.protectionRevision",
		"entry.metadata.recordMirror == contactRecordOwnerCompleted",
	} {
		if !strings.Contains(raw, compact(fragment)) {
			failures = append(failures, "priority causal shape missing "+fragment)
		}
	}
	last := strings.LastIndex(raw, compact("snapshot, err := findnode.NewPrioritySnapshot"))
	tail := strings.LastIndex(raw, compact("if err := ctx.Err(); err != nil"))
	if last < 0 || tail < last || !strings.Contains(raw[tail:], compact("return snapshot, members.coverage, nil")) {
		failures = append(failures, "priority final cancellation check missing")
	}
	return failures
}
func TestFindNodeC5ClosedReferencesAndBounds(t *testing.T) {
	if failures := c5SourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestFindNodeC5GuardRejectsEscapeAndUnboundedPort(t *testing.T) {
	original := ninN1Sources(t)
	for _, change := range []struct{ name, before, after string }{
		{"unbounded", "s.findNodePriorityMembers(ctx, profile)", "s.findNodeMembers(ctx)"},
		{"lateescape", "", "package node\nvar c5Late=(*Service).findNodePrioritySnapshot\n"},
		{"members", "if len(out.sessions) == profile.MaxMembers", "if false"},
	} {
		t.Run(change.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for p, b := range original {
				sources[p] = append([]byte(nil), b...)
			}
			if change.before == "" {
				sources["c5_extra.go"] = []byte(change.after)
			} else {
				raw := string(sources["find_node_connection.go"])
				if strings.Count(raw, change.before) != 1 {
					t.Fatal("guard fixture anchor", change.name)
				}
				sources["find_node_connection.go"] = []byte(strings.Replace(raw, change.before, change.after, 1))
			}
			if len(c5SourceErrors(sources)) == 0 {
				t.Fatal("priority source escape accepted", change.name)
			}
		})
	}
}
