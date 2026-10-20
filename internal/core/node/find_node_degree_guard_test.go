package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Closed constructor/escape/lifecycle scope. These syntax checks complement
// genuine Accept/Dial/CM/terminal tests; they do not manufacture runtime proof.
func degreeSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"degreeProfile":                    {"find_node_degree.go:<package>": 2, "find_node_degree.go:newDegreeCandidateService": 1, "find_node_degree.go:newDegreeFixtureCandidateService": 1, "find_node_degree.go:newDegreeService": 1},
		"degreeClock":                      {"find_node_degree.go:<package>": 2, "find_node_degree.go:elapsed": 1, "find_node_degree.go:newDegreeService": 2},
		"degreeKey":                        {"find_node_degree.go:<package>": 3, "find_node_degree.go:degreeKeyLess": 1, "find_node_degree.go:degreeHistoryLocked": 1, "find_node_degree.go:publishDegreeTransport": 1},
		"degreeHistoryRow":                 {"find_node_degree.go:<package>": 2, "find_node_degree.go:degreeHistoryLocked": 3},
		"degreeActiveRow":                  {"find_node_degree.go:<package>": 2, "find_node_degree.go:newDegreeService": 1},
		"degreeWitness":                    {"find_node_degree.go:<package>": 2, "proof_transport_lifetime.go:<package>": 2, "find_node_degree.go:publishDegreeTransport": 3, "find_node_degree.go:degreeEndLocked": 2},
		"degreeOwner":                      {"find_node_degree.go:<package>": 2, "service.go:<package>": 1, "find_node_degree.go:newDegreeService": 1, "find_node_degree.go:degreeNowLocked": 1, "find_node_degree.go:degreePoison": 1, "find_node_degree.go:degreeHistoryLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreeCut":                        {"find_node_degree.go:<package>": 1, "find_node_degree.go:degreeSnapshot": 6},
		"newDegreeCandidateService":        {},
		"newDegreeFixtureCandidateService": {},
		"newDegreeService":                 {"find_node_degree.go:newDegreeCandidateService": 1, "find_node_degree.go:newDegreeFixtureCandidateService": 1},
		"publishDegreeInbound":             {"session_secure.go:openInboundTransport": 1},
		"publishDegreeTransport":           {"session_secure.go:provenTransport": 1, "find_node_degree.go:publishDegreeInbound": 1},
		"degreeEndLocked":                  {"proof_transport_lifetime.go:end": 1},
		"degreeSnapshot":                   {},
		"degreeNowLocked":                  {"find_node_degree.go:publishDegreeTransport": 1, "find_node_degree.go:degreeEndLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreeHistoryLocked":              {"find_node_degree.go:publishDegreeTransport": 1, "find_node_degree.go:degreeEndLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreePoison":                     {"find_node_degree.go:publishDegreeInbound": 1, "find_node_degree.go:publishDegreeTransport": 3},
		"degreeKeyLess":                    {"find_node_degree.go:degreeHistoryLocked": 1},
	}

	// Narrow unused statistic root and fixed projection references only.
	for name, sites := range map[string]map[string]int{
		"degreeProfile":    {"peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 1},
		"degreeKey":        {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 1, "peer_state_degree_statistic.go:validDegreeStatisticKey": 1},
		"degreeHistoryRow": {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 4},
		"degreeOwner":      {"peer_state_degree_statistic.go:<package>": 1, "peer_state_degree_statistic.go:projectDegreeStatisticLocked": 1, "peer_state_degree_statistic.go:sampleDegreeStatisticClockLocked": 1},
		"newDegreeService": {"peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 1},
		"degreeKeyLess":    {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 1},
	} {
		for site, count := range sites {
			expected[name][site] = count
		}
	}
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	fields := map[string][]string{}
	fieldWant := map[string][]string{
		"degreeProfile": {"MaxActive"}, "degreeClock": {"origin", "fixture"},
		"degreeKey": {"outgoing", "group", "identity"}, "degreeHistoryRow": {"key", "last"},
		"degreeActiveRow": {"life", "witness", "generation"}, "degreeWitness": {"owner", "key", "slot", "generation", "untracked", "closed"},
		"degreeOwner": {"mu", "token", "clock", "profile", "active", "history", "historyN", "unknown", "last", "poisoned"}, "degreeCut": {"at", "outgoing", "incoming", "window"},
	}
	made := map[string]map[string]int{}
	for _, name := range []string{"degreeOwner", "degreeWitness", "degreeClock"} {
		made[name] = map[string]int{}
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
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n != declaration {
						if refs, ok := actual[n.Name]; ok {
							refs[site]++
						}
					}
				case *ast.SelectorExpr:
					if _, tracked := expected[n.Sel.Name]; tracked && (strings.HasPrefix(n.Sel.Name, "publishDegree") || n.Sel.Name == "degreeEndLocked" || n.Sel.Name == "degreeSnapshot" || n.Sel.Name == "degreeNowLocked" || n.Sel.Name == "degreeHistoryLocked" || n.Sel.Name == "degreePoison") && !callees[n] {
						failures = append(failures, "degree method value escape")
					}
				case *ast.TypeSpec:
					if _, tracked := fieldWant[n.Name.Name]; tracked {
						shape, ok := n.Type.(*ast.StructType)
						if !ok {
							failures = append(failures, "degree type escape")
							return true
						}
						for _, field := range shape.Fields.List {
							for _, name := range field.Names {
								fields[n.Name.Name] = append(fields[n.Name.Name], name.Name)
							}
						}
					}
				case *ast.CompositeLit:
					if typ, ok := n.Type.(*ast.Ident); ok {
						if refs, tracked := made[typ.Name]; tracked {
							refs[site]++
						}
					}
				case *ast.GoStmt, *ast.SendStmt:
					if path == "find_node_degree.go" {
						failures = append(failures, "degree independent worker")
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok {
						if _, tracked := expected[id.Name]; tracked && !callees[id] {
							failures = append(failures, "degree function value escape")
						}
						if id.Name == "new" && len(n.Args) == 1 {
							if typ, ok := n.Args[0].(*ast.Ident); ok {
								if _, tracked := made[typ.Name]; tracked {
									failures = append(failures, "degree new authority")
								}
							}
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "degree" && site != "find_node_degree.go:newDegreeService" {
							failures = append(failures, "degree late owner write")
						}
						if sel, ok := lhs.(*ast.SelectorExpr); ok && path == "find_node_degree.go" && (sel.Sel.Name == "origin" || sel.Sel.Name == "clock" || sel.Sel.Name == "fixture") {
							failures = append(failures, "degree clock field write")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "degree exact reference inventory changed")
	}
	if !reflect.DeepEqual(fields, fieldWant) {
		failures = append(failures, "degree fixed field inventory changed")
	}
	if !reflect.DeepEqual(made, map[string]map[string]int{"degreeOwner": {"find_node_degree.go:newDegreeService": 1}, "degreeWitness": {"find_node_degree.go:publishDegreeTransport": 1}, "degreeClock": {"find_node_degree.go:newDegreeService": 2}}) {
		failures = append(failures, "degree constructor inventory changed")
	}
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	degreeRaw := compact(string(sources["find_node_degree.go"]))
	if strings.Count(degreeRaw, "clock:=degreeClock{origin:time.Now()}") != 1 || strings.Count(degreeRaw, "iffixture!=nil{clock=degreeClock{fixture:fixture}}") != 1 || strings.Count(degreeRaw, "returnnewDegreeService(cfg,id,bus,profile,nil)") != 1 || strings.Count(degreeRaw, "returnnewDegreeService(cfg,id,bus,profile,fixture)") != 1 {
		failures = append(failures, "degree owned clock provenance changed")
	}
	common := compact(string(sources["proof_transport_lifetime.go"]))
	end := strings.Index(common, "l.ended=true")
	observe := strings.Index(common, "l.degreeEndLocked()")
	clear := strings.Index(common, "l.evidence=nil")
	if end < 0 || observe <= end || clear <= observe {
		failures = append(failures, "degree terminal not before clear")
	}
	source := compact(string(sources["session_secure.go"]))
	if strings.Count(source, "s.publishDegreeInbound(conn)") != 1 || strings.Count(source, "s.publishDegreeTransport(transport.common)") != 1 {
		failures = append(failures, "degree P1 hooks incomplete")
	}
	return failures
}
func TestDegreeClosedOriginalWitnessAndDefaultRoots(t *testing.T) {
	if failures := degreeSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestDegreeGuardRejectsLateSetterAliasAndTerminalRemoval(t *testing.T) {
	for _, kind := range []string{"alias", "setter", "terminal"} {
		t.Run(kind, func(t *testing.T) {
			sources := ninN1Sources(t)
			switch kind {
			case "alias":
				sources["degree_escape.go"] = []byte("package node\nvar leakedDegree = (*degreeOwner).degreeSnapshot\n")
			case "setter":
				sources["degree_escape.go"] = []byte("package node\nfunc lateDegree(s *Service,o *degreeOwner){s.degree=o}\n")
			case "terminal":
				sources["proof_transport_lifetime.go"] = []byte(strings.Replace(string(sources["proof_transport_lifetime.go"]), "l.degreeEndLocked()", "", 1))
			}
			if len(degreeSourceErrors(sources)) == 0 {
				t.Fatal("degree source escape accepted", kind)
			}
		})
	}
}
