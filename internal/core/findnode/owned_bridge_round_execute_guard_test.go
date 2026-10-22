package findnode

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Finite Source placement and raw closure only; no Source scan is a clock,
// provenance, runtime, heap, race, latency or Ready qualification.
func ownedRoundExecutionSourceErrors(sources map[string][]byte) []string {
	failures := pendingFixedSourceErrors(sources)
	pins := map[string]string{"owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a",
		"owned_bridge_commit.go":        "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255",
		"owned_bridge_round_plan.go":    "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51",
		"owned_bridge_round_execute.go": "dedd956801a9a14580f4e22634d3a5eb579ebe22520eb45eedce564209d7eec4",
	}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned execute exact raw source changed: "+path)
		}
	}
	names := map[string]bool{"consumeOwnedRoundPlan": true, "validateOwnedRoundExecutionLocked": true, "recordOwnedRoundSuccessLocked": true, "acceptOwnedRoundReceipt": true, "refuseOwnedRoundRow": true, "refuseOwnedRoundRowLocked": true, "refuseOwnedRoundRemainder": true}
	types := map[string]bool{"ownedRoundExecution": true, "ownedRoundSuccessReceipt": true}
	declared := map[string]int{}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned execute production parse")
		}
		for _, declaration := range file.Decls {
			site := "<package>"
			var declaredName *ast.Ident
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				site = fn.Name.Name
				declaredName = fn.Name
				if names[site] {
					declared[site]++
					if path != "owned_bridge_round_execute.go" || fn.Name.IsExported() || fn.Body == nil {
						failures = append(failures, "owned execute foreign/public declaration")
					}
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(declaration, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					callees[call.Fun] = true
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				if typ, ok := n.(*ast.TypeSpec); ok && types[typ.Name.Name] {
					declared[typ.Name.Name]++
					if path != "owned_bridge_round_execute.go" || typ.Assign.IsValid() {
						failures = append(failures, "owned execute foreign/alias type")
					}
				}
				return true
			})
			ast.Inspect(declaration, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if names[id.Name] && id != declaredName {
						if !callees[id] {
							failures = append(failures, "owned execute method value escaped")
						}
						if path != "owned_bridge_round_execute.go" && (path != "owned_bridge_commit.go" || (id.Name != "validateOwnedRoundExecutionLocked" && id.Name != "recordOwnedRoundSuccessLocked")) && (path != "owned_bridge_round_dispatch.go" || id.Name != "consumeOwnedRoundPlan") {
							failures = append(failures, "owned execute foreign caller")
						}
					}
					if types[id.Name] && (path != "owned_resources.go" || site != "<package>" || id.Name != "ownedRoundExecution") && path != "owned_bridge_round_execute.go" && path != "owned_bridge_commit.go" && (path != "owned_bridge_scratch.go" || id.Name != "ownedRoundExecution") && (path != "owned_bridge_dispatch.go" || id.Name != "ownedRoundSuccessReceipt") {
						failures = append(failures, "owned execute state escaped")
					}
				}
				if path == "owned_bridge_round_execute.go" {
					switch n.(type) {
					case *ast.GoStmt, *ast.SendStmt, *ast.FuncLit, *ast.MapType, *ast.ChanType:
						failures = append(failures, "owned execute async/callback/dynamic state")
					}
					if site == "recordOwnedRoundSuccessLocked" {
						switch n.(type) {
						case *ast.CallExpr, *ast.DeferStmt:
							failures = append(failures, "owned execute suffix acquired call/wait/allocation/defer")
						}
					}
				}
				return true
			})
		}
	}
	for name := range names {
		if declared[name] != 1 {
			failures = append(failures, "owned execute exact method declarations changed")
		}
	}
	for name := range types {
		if declared[name] != 1 {
			failures = append(failures, "owned execute exact state declarations changed")
		}
	}
	commit := string(sources["owned_bridge_commit.go"])
	anchors := []string{"at, err := cut.originals.sampleAndCheckOriginals(lease)", "if source.fresh && !entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows)", "entry.intake.phase = candidateReserved", "if execution != nil {\n\t\trecordOwnedRoundSuccessLocked(execution, r, f)\n\t}"}
	previous := -1
	for _, anchor := range anchors {
		at := strings.Index(commit, anchor)
		if at <= previous || strings.Count(commit, anchor) != 1 {
			failures = append(failures, "owned execute sample/lastCAS/receipt suffix order changed")
		}
		previous = at
	}
	// Shared helper returns receipt only after its unconditional Release. The
	// receipt itself was minted in commitOwnedFinalLocked, before that Release.
	if strings.Count(commit, "lease.Release()\n\tif err != nil {\n\t\treturn ownedRoundSuccessReceipt{}, err\n\t}") != 1 {
		failures = append(failures, "owned execute original per-candidate Release changed")
	}
	return failures
}

func TestOwnedRoundExecutionClosedSource(t *testing.T) {
	if failures := ownedRoundExecutionSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal("owned execute SOURCE closure", failures)
	}
}

func TestOwnedRoundExecutionSourceBoundaryControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"omit-consume", "owned_bridge_round_execute.go", "plan.consumed = true", "_ = plan.consumed"},
		{"omit-original-generation", "owned_bridge_round_execute.go", "o.generation != plan.generation", "false"},
		{"omit-remaining-row-membership", "owned_bridge_round_execute.go", "if !found {\n\t\t\treturn ErrStaleWork\n\t\t}", "_ = found"},
		{"receipt-after-R-release", "owned_bridge_commit.go", "if execution != nil {\n\t\trecordOwnedRoundSuccessLocked(execution, r, f)\n\t}", "_ = execution"},
		{"cleanup-accepted-phase", "owned_bridge_round_execute.go", "r.entry.intake.phase == candidateProvisional", "r.entry.intake.phase == candidateReserved"},
		{"relabel-new-round", "owned_bridge_round_execute.go", "o.round != plan.round", "false"},
		{"refund-debt", "owned_bridge_round_execute.go", "o.pending.release(r.entry)", "r.entry.intake.calendar.projected = forwardBucket{}; o.pending.release(r.entry)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique causal SOURCE control", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			found := false
			for _, failure := range ownedRoundExecutionSourceErrors(sources) {
				if failure == "owned execute exact raw source changed: "+tc.path {
					found = true
				}
			}
			if !found {
				t.Fatal("causal placement control escaped raw Source closure")
			}
		})
	}
	for _, source := range []string{
		"package findnode\nfunc executeForeign(a ownedAttempt,p ownedRoundPlan){_,_=a.consumeOwnedRoundPlan(p)}\n",
		"package findnode\nvar executeMethodValue=(ownedAttempt{}).consumeOwnedRoundPlan\n",
		"package findnode\ntype executeStateAlias=ownedRoundExecution\n",
	} {
		sources := preparedSessionSources(t)
		sources["owned_execute_escape.go"] = []byte(source)
		if len(ownedRoundExecutionSourceErrors(sources)) == 0 {
			t.Fatal("foreign execution root/state alias accepted")
		}
	}
}
