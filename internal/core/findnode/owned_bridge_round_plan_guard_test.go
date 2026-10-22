package findnode

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Full raw source and finite exact AST roles, not a typed/heap proof. Formatting
// requires an independent raw/anchor/oracle rebind, never body normalization.
func ownedRoundPlanSourceErrors(sources map[string][]byte) []string {
	failures := pendingFixedSourceErrors(sources)
	pins := map[string]string{"owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "owned_bridge_round_plan.go": "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51", "owned_bridge_scratch.go": "4f2a750803e6118b187eae648f22825452a3dedab9de1da1d9e5ee77d25876a6"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned round exact raw source changed: "+path)
		}
	}
	wanted := map[string]int{"ownedRoundPlanState:owned_resources.go:<package>": 1, "prepareOwnedRoundPlan:owned_bridge_round_dispatch.go:CloseOwnedRound": 1, "ownedRoundPlan:owned_bridge_round_execute.go:consumeOwnedRoundPlan": 1, "ownedRoundPlanState:owned_bridge_round_execute.go:<package>": 1, "copyOwnedRoundPlanLocked:owned_bridge_round_plan.go:prepareOwnedRoundPlan": 1, "orderOwnedRoundPlan:owned_bridge_round_plan.go:prepareOwnedRoundPlan": 1, "ownedRoundPlan:owned_bridge_round_plan.go:prepareOwnedRoundPlan": 9, "ownedRoundPlanRow:owned_bridge_round_plan.go:<package>": 1, "ownedRoundPlanRow:owned_bridge_round_plan.go:copyOwnedRoundPlanLocked": 1, "ownedRoundPlanRow:owned_bridge_round_plan.go:ownedRoundRowLess": 1, "ownedRoundPlanState:owned_bridge_round_plan.go:copyOwnedRoundPlanLocked": 1, "ownedRoundPlanState:owned_bridge_round_plan.go:orderOwnedRoundPlan": 1, "ownedRoundPlanState:owned_bridge_round_plan.go:ownedRoundRowLess": 1, "ownedRoundPlanState:owned_bridge_round_plan.go:prepareOwnedRoundPlan": 1, "ownedRoundPlanState:owned_bridge_scratch.go:<package>": 1, "ownedRoundRowLess:owned_bridge_round_plan.go:orderOwnedRoundPlan": 1}
	types := map[string]bool{"ownedRoundPlan": true, "ownedRoundPlanRow": true, "ownedRoundPlanState": true}
	methods := map[string]bool{"prepareOwnedRoundPlan": true, "copyOwnedRoundPlanLocked": true, "ownedRoundRowLess": true, "orderOwnedRoundPlan": true}
	actual := map[string]int{}
	declaredTypes, declaredMethods := map[string]int{}, map[string]int{}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return append(failures, "owned round production parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			declarations := map[*ast.Ident]bool{}
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declarations[fn.Name] = true
				if methods[fn.Name.Name] {
					declaredMethods[fn.Name.Name]++
					if path != "owned_bridge_round_plan.go" || fn.Body == nil || fn.Name.IsExported() {
						failures = append(failures, "owned round foreign/public method")
					}
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if typ, ok := n.(*ast.TypeSpec); ok {
					declarations[typ.Name] = true
					if types[typ.Name.Name] {
						declaredTypes[typ.Name.Name]++
						if path != "owned_bridge_round_plan.go" || typ.Assign.IsValid() {
							failures = append(failures, "owned round foreign/alias type")
						}
					}
				}
				return true
			})
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
					if s, ok := c.Fun.(*ast.SelectorExpr); ok {
						callees[s.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && (types[id.Name] || methods[id.Name]) && !declarations[id] {
					actual[id.Name+":"+site]++
					if methods[id.Name] && !callees[id] {
						failures = append(failures, "owned round method value escaped")
					}
				}
				if path == "owned_bridge_round_plan.go" {
					switch v := n.(type) {
					case *ast.GoStmt, *ast.SendStmt, *ast.FuncLit, *ast.MapType, *ast.ChanType:
						failures = append(failures, "owned round worker/callback/map/channel")
					case *ast.CallExpr:
						if s, ok := v.Fun.(*ast.SelectorExpr); ok {
							switch s.Sel.Name {
							case "Sample", "TryReadClock", "ReserveClock", "BeginCapture", "Release", "advanceLocked", "selectCapacityLocked", "publishLocked", "finalizeOwnedCandidate", "commitOwnedFinalLocked":
								failures = append(failures, "owned round acquired clock/commit/accounting effect")
							}
						}
					case *ast.AssignStmt:
						for _, lhs := range v.Lhs {
							if _, local := lhs.(*ast.Ident); local {
								continue
							}
							root := ownedRoundAssignmentRoot(lhs)
							if root == "o" || root == "entry" || root == "owner" || root == "s" || root == "cut" {
								failures = append(failures, "owned round shared state assignment")
							}
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(declaredTypes, map[string]int{"ownedRoundPlan": 1, "ownedRoundPlanRow": 1, "ownedRoundPlanState": 1}) || !reflect.DeepEqual(declaredMethods, map[string]int{"prepareOwnedRoundPlan": 1, "copyOwnedRoundPlanLocked": 1, "ownedRoundRowLess": 1, "orderOwnedRoundPlan": 1}) {
		failures = append(failures, "owned round exact declarations changed")
	}
	if !reflect.DeepEqual(actual, wanted) {
		failures = append(failures, "owned round exact reference roles changed")
	}
	return failures
}

func TestOwnedRoundPlanSourceBoundaryControls(t *testing.T) {
	original := preparedSessionSources(t)
	if failures := ownedRoundPlanSourceErrors(original); len(failures) != 0 {
		t.Fatal("owned round SOURCE closure", failures)
	}
	for _, tc := range []struct{ name, file, before, after, want string }{
		{"raw-body-literal", "owned_bridge_round_plan.go", "Historical preparation only.", "Historical  preparation only.", "owned round exact raw source changed: owned_bridge_round_plan.go"},
		{"new-clock", "owned_bridge_round_plan.go", "plan := &ownedRoundPlanState{}", "plan := &ownedRoundPlanState{}; lease,_:=owner.consumer.TryReadClock(cut.stamp); if lease!=nil{lease.Release()}", "owned round exact raw source changed: owned_bridge_round_plan.go"},
		{"in-BUSY-finalizer", "owned_bridge_round_plan.go", "orderOwnedRoundPlan(plan)", "orderOwnedRoundPlan(plan); _=attempt.finalizeOwnedCandidate(candidateReservation{})", "owned round exact raw source changed: owned_bridge_round_plan.go"},
		{"shared-open-write", "owned_bridge_round_plan.go", "plan.round, plan.cut = o.round, o.cut", "plan.round, plan.cut = o.round, o.cut; o.open=false", "owned round exact raw source changed: owned_bridge_round_plan.go"},
		{"callback", "owned_bridge_round_plan.go", "orderOwnedRoundPlan(plan)", "orderOwnedRoundPlan(plan); func(){}()", "owned round exact raw source changed: owned_bridge_round_plan.go"},
		{"omit-plan-clear", "owned_bridge_scratch.go", "c.roundPlan = nil", "_ = c.roundPlan", "owned round exact raw source changed: owned_bridge_scratch.go"},
		{"OTHERFILE-caller", "owned_round_escape.go", "", "package findnode\nfunc roundCall(a ownedAttempt){_,_=a.prepareOwnedRoundPlan()}\n", "owned round exact reference roles changed"},
		{"OTHERFILE-value", "owned_round_escape.go", "", "package findnode\nvar roundValue=(ownedAttempt{}).prepareOwnedRoundPlan\n", "owned round exact reference roles changed"},
		{"OTHERFILE-type-alias", "owned_round_escape.go", "", "package findnode\ntype roundAlias=ownedRoundPlan\n", "owned round exact reference roles changed"},
		{"OTHERFILE-state-pointer", "owned_round_escape.go", "", "package findnode\nvar roundPointer *ownedRoundPlanState\n", "owned round exact reference roles changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for path, raw := range original {
				sources[path] = append([]byte(nil), raw...)
			}
			if tc.before == "" {
				if sources[tc.file] != nil {
					t.Fatal("owned round SOURCE new file already present")
				}
				sources[tc.file] = []byte(tc.after)
			} else {
				raw := string(sources[tc.file])
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("owned round SOURCE unique anchor", tc.name)
				}
				sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			found := false
			for _, failure := range ownedRoundPlanSourceErrors(sources) {
				if failure == tc.want {
					found = true
				}
			}
			if !found {
				t.Fatal("owned round SOURCE intended refusal missing", tc.name)
			}
		})
	}
}

// Follow only the assigned storage base. Selector field names and index reads
// are not storage roots: plan.cut writes plan, while cut.member writes cut.
func ownedRoundAssignmentRoot(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return ownedRoundAssignmentRoot(v.X)
	case *ast.IndexExpr:
		return ownedRoundAssignmentRoot(v.X)
	case *ast.IndexListExpr:
		return ownedRoundAssignmentRoot(v.X)
	case *ast.StarExpr:
		return ownedRoundAssignmentRoot(v.X)
	case *ast.ParenExpr:
		return ownedRoundAssignmentRoot(v.X)
	default:
		return ""
	}
}

func TestOwnedRoundPlanAssignmentRootControls(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr ast.Expr
		want string
	}{
		{"local-field-named-cut", &ast.SelectorExpr{X: ast.NewIdent("plan"), Sel: ast.NewIdent("cut")}, "plan"},
		{"original-cut-storage", &ast.SelectorExpr{X: ast.NewIdent("cut"), Sel: ast.NewIdent("at")}, "cut"},
		{"coordinator-storage", &ast.SelectorExpr{X: ast.NewIdent("o"), Sel: ast.NewIdent("open")}, "o"},
		{"local-array-shared-index-read", &ast.IndexExpr{X: &ast.SelectorExpr{X: ast.NewIdent("plan"), Sel: ast.NewIdent("rows")}, Index: &ast.SelectorExpr{X: ast.NewIdent("entry"), Sel: ast.NewIdent("id")}}, "plan"},
		{"shared-array-through-parens-and-pointer", &ast.IndexExpr{X: &ast.SelectorExpr{X: &ast.ParenExpr{X: &ast.StarExpr{X: ast.NewIdent("owner")}}, Sel: ast.NewIdent("rows")}, Index: ast.NewIdent("i")}, "owner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ownedRoundAssignmentRoot(tc.expr); got != tc.want {
				t.Fatal("assigned storage root", got, tc.want)
			}
		})
	}
}
