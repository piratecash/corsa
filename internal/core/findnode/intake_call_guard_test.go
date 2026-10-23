package findnode

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This is a reviewed call/field inventory, not universal dataflow verification.
func TestCandidateInactiveCallGraphAndInventories(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	allowed := map[string]bool{"intake_owner.go": true, "intake_capacity_epoch.go": true, "intake_source_limits.go": true, "intake_pending_fixed.go": true, "intake_groups.go": true, "intake_membership.go": true, "budget.go": true, "pending.go": true, "session.go": true}
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
			if strings.Trim(spec.Path.Value, "\"") == "github.com/piratecash/corsa/internal/core/findnode" && path != filepath.Join(root, "internal", "core", "node", "find_node_resources.go") && path != filepath.Join(root, "internal", "core", "netcore", "owned_resources.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_connection.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_bridge_capture.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_bridge_construction.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_owned_receive.go") && path != filepath.Join(root, "internal", "core", "node", "peer_management.go") && path != filepath.Join(root, "internal", "core", "node", "proof_transport_lifetime.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_owned_round_loop.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_owned_write.go") && path != filepath.Join(root, "internal", "core", "netcore", "owned_findnode_write.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_owned_answer.go") && path != filepath.Join(root, "internal", "core", "netcore", "owned_findnode_answer_write.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_owned_local_target.go") && path != filepath.Join(root, "internal", "core", "node", "identity_pin_own_dm_runtime.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_ready_construction.go") && path != filepath.Join(root, "internal", "core", "node", "find_node_ready_target_timing.go") {
				t.Errorf("production import: %s", path)
			}
		}
		if filepath.Dir(path) != filepath.Join(root, "internal", "core", "findnode") {
			return nil
		}
		if !allowed[entry.Name()] {
			narrow := map[string]int{}
			want := map[string]int{}
			if entry.Name() == "owned_bridge_bootstrap.go" {
				want = map[string]int{"NewExecutingBootstrapIntake:candidateCapacityHint": 1}
			}
			if entry.Name() == "owned_resources.go" {
				want = map[string]int{"<package>:intakeCoordinator": 1, "<package>:candidatePendingBook": 1, "<package>:candidateCapacityEpoch": 1, "<package>:candidatePending": 2}
			}
			if entry.Name() == "owned_bridge_forward_answer.go" {
				want = map[string]int{"MatchOwnedAnswer:candidateDispatching": 2, "MatchOwnedAnswer:candidateAwaitingAnswer": 1, "MatchOwnedAnswer:candidateTerminalDispatch": 1}
			}
			if entry.Name() == "owned_bridge_receive.go" {
				want = map[string]int{"OfferReceived:candidateIndex": 2, "OfferReceived:candidatePending": 1, "OfferReceived:candidateProvisional": 3, "OfferReceived:candidateReserved": 1}
			}
			if entry.Name() == "owned_bridge_round_dispatch.go" {
				want = map[string]int{"CloseOwnedRound:candidateProvisional": 1}
			}
			if entry.Name() == "owned_bridge_dispatch.go" {
				want = map[string]int{"retireOwnedForward:candidateReserved": 1, "Take:candidateReserved": 1, "Take:candidatePairTrial": 1, "Take:candidateDispatching": 1, "Finish:candidateDispatching": 1, "Finish:candidateTerminalDispatch": 2, "Finish:candidateGroupWindow": 1, "Finish:candidateAwaitingAnswer": 1}
			}
			if entry.Name() == "owned_bridge_round_plan.go" {
				want = map[string]int{"<package>:candidateGroupCells": 2, "<package>:candidateReservation": 1, "copyOwnedRoundPlanLocked:candidateGroupCells": 2, "copyOwnedRoundPlanLocked:candidateProvisional": 1, "orderOwnedRoundPlan:candidateGroupCells": 4, "ownedRoundRowLess:candidateGroupCells": 4}
			}
			if entry.Name() == "owned_bridge.go" {
				want = map[string]int{"<package>:intakeCoordinator": 1, "newDormantIntakeAt:candidateProfile": 1, "newDormantIntakeAt:candidateCapacityHint": 1, "newDormantIntake:candidateCapacityHint": 1}
			}
			if entry.Name() == "owned_bridge_scratch.go" {
				want = map[string]int{"projectOwnedMembership:candidateOlder": 1, "projectOwnedMembership:candidateGroupWindow": 1}
			}
			if entry.Name() == "owned_bridge_cut.go" {
				want = map[string]int{"Seal:candidateCapacityWindow": 1}
			}
			if entry.Name() == "owned_bridge_round_execute.go" {
				want = map[string]int{"<package>:candidateGroupCells": 1, "<package>:candidateRoundResult": 1, "<package>:candidateReservation": 1, "<package>:candidateSourceRef": 1, "consumeOwnedRoundPlan:candidateRoundResult": 9, "validateOwnedRoundExecutionLocked:candidateProvisional": 2, "recordOwnedRoundSuccessLocked:candidateGroupCells": 1, "recordOwnedRoundSuccessLocked:candidateReservation": 1, "refuseOwnedRoundRowLocked:candidateProvisional": 1}
			}
			if entry.Name() == "owned_bridge_commit.go" {
				want = map[string]int{"finalizeOwnedCandidate:candidateReservation": 1, "finalizeOwnedCandidateBusy:candidateReservation": 1, "finalizeOwnedCandidateBusy:candidateProvisional": 2, "finalizeOwnedCandidateBusy:candidateSourceWindows": 1, "finalizeOwnedCandidateBusy:candidateCapacityCut": 1, "commitOwnedFinalLocked:candidateReservation": 1, "commitOwnedFinalLocked:candidateSourceRef": 1, "commitOwnedFinalLocked:candidateProvisional": 1, "commitOwnedFinalLocked:candidateReserved": 1, "commitOwnedFinalLocked:candidateSourceWindows": 1, "commitOwnedFinalLocked:candidateCapacityCut": 2, "publishOwnedAttempt:intakeCoordinator": 1}
			}
			for _, decl := range file.Decls {
				site := "<package>"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					site = fn.Name.Name
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					if id, ok := node.(*ast.Ident); ok && (strings.HasPrefix(id.Name, "candidate") || id.Name == "intakeCoordinator" || id.Name == "captureCandidateMembership") {
						narrow[site+":"+id.Name]++
					}
					return true
				})
			}
			if !reflect.DeepEqual(narrow, want) {
				t.Errorf("inactive owner escaped exact narrow sites: %s %v", path, narrow)
			}
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
		"intakeCoordinator":         {"mu", "pending", "calendars", "groups", "sourceOwner", "profile", "capacity", "last", "generation", "round", "open", "cut", "cursor", "watermark", "sweep", "rrCell", "rrSession", "counters", "published"},
		"candidateGroups":           {"named", "namedCount", "records", "recordCount", "generation", "lastUse", "used"},
		"candidateGroupSlot":        {"key", "record"},
		"candidateCapacityDomain":   {"_"},
		"candidateCapacityHint":     {"value", "age", "present"},
		"candidateCapacityCut":      {"owner", "domain", "at", "degree", "complete"},
		"candidateCapacityEpoch":    {"owner", "domain", "origin", "last", "epoch", "cap"},
		"candidatePending":          {"phase", "generation", "round", "latest", "priority", "calendar", "alias", "cell", "source", "ownedReceived", "ownedIdentity"},
		"candidatePendingBook":      {"mu", "entries", "last", "counters", "published"},
		"candidateMembership":       {"owner", "at", "maximum", "classes", "aged"},
		"candidateSourceOwnerToken": {"_"},
		"candidateSourceWindows":    {"owner", "rows"},
		"candidateSourceTarget":     {"target", "projected", "actual", "expires", "generation", "refs"},
		"candidateSourceRef":        {"windows", "index", "generation"},
		"candidateSourcePlan":       {"windows", "index", "row", "floor", "fresh"},
		"candidatePairHistory":      {"times", "count"},
	}
	for name, names := range expected {
		var source string
		switch name {
		case "candidatePendingBook":
			source = "intake_pending_fixed.go"
		case "intakeCoordinator", "candidatePending":
			source = "intake_owner.go"
		case "candidateGroups", "candidateGroupSlot":
			source = "intake_groups.go"
		case "candidateCapacityDomain", "candidateCapacityHint", "candidateCapacityCut", "candidateCapacityEpoch":
			source = "intake_capacity_epoch.go"
		case "candidateMembership":
			source = "intake_membership.go"
		case "candidatePairHistory":
			source = "budget.go"
		default:
			source = "intake_source_limits.go"
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

// Closed source-companion roots include package declarations, method values and
// type aliases. This syntax inventory is deliberately narrower than dataflow.
func TestCandidateSourceCompanionClosedRoots(t *testing.T) {
	expected := map[string]map[string]int{
		"prepareSources":          {"intake_owner.go:sealRound": 1},
		"planSourceLocked":        {"owned_bridge_commit.go:commitOwnedFinalLocked": 1, "intake_owner.go:commitLocked": 1},
		"finalizeSourceLocked":    {"owned_bridge_commit.go:commitOwnedFinalLocked": 1, "intake_owner.go:commitLocked": 1},
		"sourceRowLocked":         {"owned_bridge_forward_answer.go:MatchOwnedAnswer": 1, "owned_bridge_dispatch.go:Take": 1, "owned_bridge_dispatch.go:Finish": 1, "intake_owner.go:currentLocked": 1, "intake_owner.go:takeForward": 1, "intake_owner.go:finishForward": 1, "intake_owner.go:answer": 1, "intake_source_limits.go:releaseSourceLocked": 1, "intake_source_limits.go:planSourceLocked": 1},
		"releaseSourceLocked":     {"intake_owner.go:releaseLocked": 1},
		"candidatePairTrial":      {"owned_bridge_dispatch.go:Take": 1, "budget.go:candidatePairProject": 2, "intake_source_limits.go:finalizeSourceLocked": 1, "intake_owner.go:takeForward": 1},
		"candidatePairProject":    {"intake_source_limits.go:planSourceLocked": 3},
		"candidatePairExpired":    {"intake_source_limits.go:planSourceLocked": 4},
		"planCombinedFloorLocked": {"owned_bridge_commit.go:commitOwnedFinalLocked": 1, "budget.go:planCombinedLocked": 1, "intake_owner.go:commitLocked": 1},
	}
	declarations := map[string]string{
		"candidateSourceOwnerToken": "intake_source_limits.go",
		"candidateSourceWindows":    "intake_source_limits.go",
		"candidateSourceTarget":     "intake_source_limits.go",
		"candidateSourceRef":        "intake_source_limits.go",
		"candidateSourcePlan":       "intake_source_limits.go",
		"candidatePairHistory":      "budget.go",
	}
	actual := make(map[string]map[string]int, len(expected))
	for name := range expected {
		actual[name] = make(map[string]int)
	}
	declared := make(map[string]string)
	fields := make(map[string]int)
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, e := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		for _, declaration := range file.Decls {
			where := path + ":<package>"
			var node ast.Node = declaration
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				where = path + ":" + fn.Name.Name
				node = fn.Body
			}
			calls := make(map[ast.Node]bool)
			selectorNames := make(map[*ast.Ident]bool)
			ast.Inspect(node, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					calls[c.Fun] = true
				}
				if selector, ok := n.(*ast.SelectorExpr); ok {
					selectorNames[selector.Sel] = true
				}
				return true
			})
			ast.Inspect(node, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.TypeSpec:
					if want, tracked := declarations[n.Name.Name]; tracked {
						if n.Assign.IsValid() || path != want || declared[n.Name.Name] != "" {
							t.Error("unreviewed companion declaration", where, n.Name.Name)
						}
						declared[n.Name.Name] = path
					} else if n.Assign.IsValid() {
						ast.Inspect(n.Type, func(v ast.Node) bool {
							if id, ok := v.(*ast.Ident); ok {
								if _, tracked := declarations[id.Name]; tracked {
									t.Error("companion type alias", where)
								}
							}
							return true
						})
					}
				case *ast.SelectorExpr:
					if counts, tracked := actual[n.Sel.Name]; tracked {
						counts[where]++
						if !calls[n] {
							t.Error("companion method-value escape", where, n.Sel.Name)
						}
					}
					if n.Sel.Name == "sourceWindows" {
						fields[where]++
					}
				case *ast.Ident:
					if selectorNames[n] {
						return true
					}
					if counts, tracked := actual[n.Name]; tracked {
						counts[where]++
						if !calls[n] {
							t.Error("companion function-value escape", where, n.Name)
						}
					}
				}
				return true
			})
		}
	}
	// Selector children were excluded from the unqualified-reference inventory.
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("source companion call roots changed", actual)
	}
	if !reflect.DeepEqual(declared, declarations) {
		t.Fatal("source companion type roots changed", declared)
	}
	if !reflect.DeepEqual(fields, map[string]int{"intake_source_limits.go:prepareSources": 1, "intake_source_limits.go:planSourceLocked": 1, "intake_source_limits.go:sourceRowLocked": 1, "intake_owner.go:commitLocked": 1, "owned_bridge_commit.go:commitOwnedFinalLocked": 2}) {
		t.Fatal("source companion field escaped", fields)
	}
}

func TestCandidateSourceTypeReferencesAndDeclarations(t *testing.T) {
	expected := map[string]map[string]bool{
		"candidateSourceOwnerToken": {"intake_owner.go:<package>": true, "intake_owner.go:newIntakeCoordinator": true, "intake_source_limits.go:<package>": true},
		"candidateSourceWindows":    {"owned_bridge_commit.go:finalizeOwnedCandidateBusy": true, "owned_bridge_commit.go:commitOwnedFinalLocked": true, "session.go:<package>": true, "intake_owner.go:commitLocked": true, "intake_source_limits.go:<package>": true, "intake_source_limits.go:prepareSources": true, "intake_source_limits.go:planSourceLocked": true},
		"candidateSourceTarget":     {"intake_source_limits.go:<package>": true, "intake_source_limits.go:planSourceLocked": true, "intake_source_limits.go:sourceRowLocked": true},
		"candidateSourceRef":        {"owned_bridge_round_execute.go:<package>": true, "owned_bridge_commit.go:commitOwnedFinalLocked": true, "intake_owner.go:<package>": true, "intake_owner.go:commitLocked": true, "intake_source_limits.go:releaseSourceLocked": true},
		"candidateSourcePlan":       {"intake_source_limits.go:planSourceLocked": true, "intake_source_limits.go:finalizeSourceLocked": true},
		"candidatePairHistory":      {"intake_source_limits.go:<package>": true, "budget.go:candidatePairTrial": true, "budget.go:candidatePairProject": true, "budget.go:candidatePairExpired": true},
	}
	actual := make(map[string]map[string]bool)
	for name := range expected {
		actual[name] = make(map[string]bool)
	}
	packageCounts := make(map[string]int)
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			where := path + ":<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				where = path + ":" + fn.Name.Name
			}
			names := make(map[*ast.Ident]bool)
			ast.Inspect(decl, func(n ast.Node) bool {
				if spec, ok := n.(*ast.TypeSpec); ok {
					names[spec.Name] = true
					if spec.Assign.IsValid() {
						ast.Inspect(spec.Type, func(n ast.Node) bool {
							if id, ok := n.(*ast.Ident); ok {
								if _, tracked := expected[id.Name]; tracked {
									t.Error("source type alias", where)
								}
							}
							return true
						})
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && !names[id] {
					if roots, tracked := actual[id.Name]; tracked {
						roots[where] = true
						if group, ok := decl.(*ast.GenDecl); ok {
							packageCounts[path+":"+id.Name]++
							if group.Tok != token.TYPE {
								t.Error("source companion package value", where, id.Name)
							}
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("source companion type references escaped", actual)
	}
	if !reflect.DeepEqual(packageCounts, map[string]int{
		"intake_owner.go:candidateSourceOwnerToken": 1, "intake_source_limits.go:candidateSourceOwnerToken": 1,
		"session.go:candidateSourceWindows": 1, "intake_source_limits.go:candidateSourceWindows": 2,
		"intake_source_limits.go:candidateSourceTarget": 2, "intake_owner.go:candidateSourceRef": 1,
		"intake_source_limits.go:candidatePairHistory": 1, "owned_bridge_round_execute.go:candidateSourceRef": 1,
	}) {
		t.Fatal("source companion package declaration reference counts changed", packageCounts)
	}
}

// Exact syntax companion for the disconnected logical-capacity root. It does
// not prove runtime clock provenance, external classification or dataflow.
func candidateCapacitySourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"newCapacityIntakeCoordinator": {"owned_bridge.go:newDormantIntakeAt": 1},
		"candidateBootstrapCap":        {"intake_capacity_epoch.go:newCapacityIntakeCoordinator": 1},
		"candidateCapacityEpochAt":     {"intake_capacity_epoch.go:selectCapacityLocked": 2, "intake_capacity_epoch.go:frozenCapacityLocked": 1},
		"selectCapacityLocked":         {"owned_bridge_commit.go:commitOwnedFinalLocked": 1, "intake_owner.go:sealRound": 1},
		"frozenCapacityLocked":         {"intake_owner.go:commitLocked": 2},
	}
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	var failures []string
	capacityWrites := 0
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, declaration := range file.Decls {
			site := path + ":<package>"
			var node ast.Node = declaration
			if function, ok := declaration.(*ast.FuncDecl); ok {
				site = path + ":" + function.Name.Name
				node = function.Body
			}
			calls := map[ast.Node]bool{}
			ast.Inspect(node, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					calls[call.Fun] = true
				}
				return true
			})
			ast.Inspect(node, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if sites, tracked := actual[id.Name]; tracked {
						sites[site]++
					}
				}
				if call, ok := n.(*ast.SelectorExpr); ok {
					if _, tracked := actual[call.Sel.Name]; tracked && !calls[call] {
						failures = append(failures, "capacity method value escape")
					}
				}
				if assignment, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assignment.Lhs {
						if field, ok := lhs.(*ast.SelectorExpr); ok && field.Sel.Name == "capacity" {
							capacityWrites++
							if site != "intake_capacity_epoch.go:newCapacityIntakeCoordinator" {
								failures = append(failures, "capacity late attachment")
							}
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "capacity exact root inventory changed")
	}
	if capacityWrites != 1 {
		failures = append(failures, "capacity construction write inventory changed")
	}
	owner := string(sources["intake_owner.go"])
	start := strings.Index(owner, "func newIntakeCoordinator(")
	end := strings.Index(owner, "func (o *intakeCoordinator) snapshot(")
	if start < 0 || end < start || strings.Contains(owner[start:end], "capacity:") || strings.Contains(owner[start:end], "o.capacity") {
		failures = append(failures, "fixed-cap constructor activated companion")
	}
	lock := strings.Index(owner, "entry.target.state.mu.Lock()")
	final := strings.Index(owner, "currentCapacity, capacityErr := o.frozenCapacityLocked(now)")
	claim := strings.Index(owner, "entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows)")
	if lock < 0 || final <= lock || claim <= final {
		failures = append(failures, "capacity final logical check missing")
	}
	return failures
}
func TestCandidateCapacityClosedRootsAndLogicalBoundary(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := fs.ReadFile(os.DirFS("."), path)
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = raw
	}
	if failures := candidateCapacitySourceErrors(sources); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestCandidateCapacityGuardRejectsActivationAndMissingFinalCheck(t *testing.T) {
	sources := map[string][]byte{}
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := fs.ReadFile(os.DirFS("."), path)
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = raw
	}
	for _, change := range []struct{ name, path, before, after string }{
		{"escape", "intake_capacity_epoch.go", "", "\nvar candidateCapacityEscape = newCapacityIntakeCoordinator\n"},
		{"final", "intake_owner.go", "currentCapacity, capacityErr := o.frozenCapacityLocked(now)", "currentCapacity, capacityErr := o.profile.nodeCap, error(nil)"},
	} {
		t.Run(change.name, func(t *testing.T) {
			mutated := map[string][]byte{}
			for name, raw := range sources {
				mutated[name] = append([]byte(nil), raw...)
			}
			raw := string(mutated[change.path])
			if change.before == "" {
				raw += change.after
			} else {
				if strings.Count(raw, change.before) != 1 {
					t.Fatal("capacity guard fixture anchor")
				}
				raw = strings.Replace(raw, change.before, change.after, 1)
			}
			mutated[change.path] = []byte(raw)
			if len(candidateCapacitySourceErrors(mutated)) == 0 {
				t.Fatal("capacity guard accepted escape or missing final check", change.name)
			}
		})
	}
}
