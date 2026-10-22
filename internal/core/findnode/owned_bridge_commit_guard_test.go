package findnode

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Whole raw bytes and exact named role inventories, not typed/dataflow/heap
// qualification. Trusted FORMAT requires explicit reviewed literal repins.
func ownedFinalCandidateSourceErrors(sources map[string][]byte) []string {
	var failures []string
	sum := sha256.Sum256(sources["owned_bridge_commit.go"])
	if hex.EncodeToString(sum[:]) != "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255" {
		failures = append(failures, "owned final exact raw file changed")
	}
	failures = append(failures, pendingFixedSourceErrors(sources)...)
	wanted := map[string]string{
		"finalizeOwnedCandidateBusy": "func(attemptownedAttempt)finalizeOwnedCandidateBusy(rcandidateReservation,execution*ownedRoundExecution)(ownedRoundSuccessReceipt,error)",
		"finalizeOwnedCandidate":     "func(attemptownedAttempt)finalizeOwnedCandidate(rcandidateReservation)error",
		"commitOwnedFinalLocked":     "func(attemptownedAttempt)commitOwnedFinalLocked(rcandidateReservation,calendar,retired*ForwardCalendar,calendarIndexint,freshbool,prepared*candidateSourceWindows,slot*forwardSlot,capacityCuts*[1]candidateCapacityCut,priorityPriority,lease*findnodebridge.CommitReservation,execution*ownedRoundExecution)error",
		"publishOwnedAttempt":        "func(o*intakeCoordinator)publishOwnedAttempt()",
	}
	seen := map[string]string{}
	refs := map[string]int{}
	sensitive := map[string]int{}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return append(failures, "owned final production parse")
		}
		for _, declaration := range file.Decls {
			site := path + ":<package>"
			var declared *ast.Ident
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declared = fn.Name
				if _, tracked := wanted[fn.Name.Name]; tracked {
					if path != "owned_bridge_commit.go" || fn.Body == nil || seen[fn.Name.Name] != "" || fn.Name.IsExported() {
						failures = append(failures, "owned final duplicate/foreign/public method")
					} else {
						a, z := set.Position(fn.Pos()).Offset, set.Position(fn.Body.Pos()).Offset
						// No body literals are normalized; only a literal-free header.
						seen[fn.Name.Name] = strings.Join(strings.Fields(string(raw[a:z])), "")
					}
				} else if path == "owned_bridge_commit.go" {
					failures = append(failures, "owned final extra method")
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(declaration, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					callees[call.Fun] = true
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
						if path == "owned_bridge_commit.go" {
							switch sel.Sel.Name {
							case "TryReadClock", "sampleAndCheckOriginals", "Sample", "Release":
								sensitive[site+":"+sel.Sel.Name]++
							}
							if site == "owned_bridge_commit.go:commitOwnedFinalLocked" {
								switch sel.Sel.Name {
								case "Lock", "RLock", "TryLock", "TryRLock", "BeginCapture", "ReserveClock", "TryReadClock", "Sample", "Release":
									failures = append(failures, "owned final locked helper acquired wait/clock/release")
								}
							}
						}
					}
					if id, ok := call.Fun.(*ast.Ident); ok && site == "owned_bridge_commit.go:commitOwnedFinalLocked" && (id.Name == "make" || id.Name == "new" || id.Name == "append") {
						failures = append(failures, "owned final locked helper allocation")
					}
				}
				return true
			})
			ast.Inspect(declaration, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if _, tracked := wanted[id.Name]; tracked && id != declared {
						refs[id.Name+":"+site]++
						if !callees[id] {
							failures = append(failures, "owned final method value/alias escape")
						}
					}
				}
				if site == "owned_bridge_commit.go:commitOwnedFinalLocked" {
					switch n.(type) {
					case *ast.DeferStmt, *ast.FuncLit, *ast.GoStmt, *ast.SendStmt, *ast.MapType, *ast.ChanType:
						failures = append(failures, "owned final locked helper retained async/defer/dynamic state")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(seen, wanted) {
		failures = append(failures, "owned final exact three method signatures changed")
	}
	if !reflect.DeepEqual(refs, map[string]int{
		"publishOwnedAttempt:owned_bridge_commit.go:finalizeOwnedCandidateBusy": 1, "publishOwnedAttempt:owned_bridge_round_execute.go:consumeOwnedRoundPlan": 1, "finalizeOwnedCandidateBusy:owned_bridge_commit.go:finalizeOwnedCandidate": 1, "finalizeOwnedCandidateBusy:owned_bridge_round_execute.go:consumeOwnedRoundPlan": 1,
		"commitOwnedFinalLocked:owned_bridge_commit.go:finalizeOwnedCandidateBusy": 1,
	}) {
		failures = append(failures, "owned final production caller roots changed")
	}
	if !reflect.DeepEqual(sensitive, map[string]int{
		"owned_bridge_commit.go:finalizeOwnedCandidateBusy:TryReadClock":        1,
		"owned_bridge_commit.go:finalizeOwnedCandidateBusy:Release":             1,
		"owned_bridge_commit.go:commitOwnedFinalLocked:sampleAndCheckOriginals": 1,
	}) {
		failures = append(failures, "owned final one original reservation/sample/release changed")
	}
	raw := string(sources["owned_bridge_commit.go"])
	ordered := []string{
		"defer o.publishOwnedAttempt()",
		"o.mu.Lock()\n\tdefer o.mu.Unlock()\n\to.pending.mu.Lock()",
		"o.pending.mu.Lock()",
		"entry.target.state.mu.Lock()",
		"lease, err := owner.consumer.TryReadClock(cut.stamp)",
		"err = attempt.commitOwnedFinalLocked(r, calendar, retired, calendarIndex, fresh, prepared, slot, &capacityCuts, priority, lease, execution)",
		"lease.Release()",
		"at, err := cut.originals.sampleAndCheckOriginals(lease)",
		"!cut.validBefore.Contains(at)",
		"boundSource := entry.source.state.sourceWindows.Load()",
		"capacity, err := o.selectCapacityLocked(f, capacityCuts[:])",
		"source, err := o.planSourceLocked(entry, f, prepared)",
		"id, err := entry.target.peekNextIDLocked()",
		"if o.pending.targetEntryLocked(pendingKey{entry.target, id}) != nil",
		"if source.fresh && !entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows)",
		"source.windows.rows[source.index] = source.row",
		"entry.intake.phase = candidateReserved",
		"if execution != nil {\n\t\trecordOwnedRoundSuccessLocked(execution, r, f)\n\t}",
		"func (o *intakeCoordinator) publishOwnedAttempt()",
	}
	previous := -1
	for _, anchor := range ordered {
		at := strings.Index(raw, anchor)
		if strings.Count(raw, anchor) != 1 || at <= previous {
			failures = append(failures, "owned final exact lock/sample/claim/write/release source order changed")
			break
		}
		previous = at
	}
	return failures
}

func TestOwnedFinalCandidateClosedSource(t *testing.T) {
	if failures := ownedFinalCandidateSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal("owned final SOURCE closure", failures)
	}
}

func TestOwnedFinalCandidateSourceBoundaryControls(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"lease-before-target-wait", "\tentry.target.state.mu.Lock()\n\tdefer entry.target.state.mu.Unlock()\n\tlease, err := owner.consumer.TryReadClock(cut.stamp)", "\tlease, err := owner.consumer.TryReadClock(cut.stamp)\n\tentry.target.state.mu.Lock()\n\tdefer entry.target.state.mu.Unlock()"},
		{"sample-selected-only", "at, err := cut.originals.sampleAndCheckOriginals(lease)", "at, err := lease.Sample()"},
		{"frontier-equality-accepted", "!cut.validBefore.Contains(at)", "!at.NotBefore(cut.at)"},
		{"early-release", "at, err := cut.originals.sampleAndCheckOriginals(lease)", "at, err := cut.originals.sampleAndCheckOriginals(lease); lease.Release()"},
		{"donor-unlock-before-final", "defer old.mu.Unlock()", "old.mu.Unlock()"},
		{"prep-refill-credit", "calendar.projected, calendar.actual, calendar.last = bucket, bucket, f", "_ = bucket"},
		{"postclaim-allocation", "source.windows.rows[source.index] = source.row", "_ = make([]byte,1); source.windows.rows[source.index] = source.row"},
		{"in-lease-publish", "source.windows.rows[source.index] = source.row", "o.publishLocked(); source.windows.rows[source.index] = source.row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			raw := string(sources["owned_bridge_commit.go"])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("owned final SOURCE unique mutation anchor", tc.name)
			}
			sources["owned_bridge_commit.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			found := false
			for _, failure := range ownedFinalCandidateSourceErrors(sources) {
				if failure == "owned final exact raw file changed" {
					found = true
				}
			}
			if !found {
				t.Fatal("owned final SOURCE control escaped exact raw placement", tc.name)
			}
		})
	}
	for _, tc := range []struct{ name, source string }{
		{"other-file-caller", "package findnode\nfunc ownedFinalForeign(a ownedAttempt,r candidateReservation){_=a.finalizeOwnedCandidate(r)}\n"},
		{"other-file-method-value", "package findnode\nfunc ownedFinalValue(a ownedAttempt)any{return a.finalizeOwnedCandidate}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if sources["owned_final_escape.go"] != nil {
				t.Fatal("owned final foreign source path already exists")
			}
			sources["owned_final_escape.go"] = []byte(tc.source)
			found := false
			for _, failure := range ownedFinalCandidateSourceErrors(sources) {
				if failure == "owned final production caller roots changed" {
					found = true
				}
			}
			if !found {
				t.Fatal("owned final SOURCE other-file caller escaped exact roles", tc.name)
			}
		})
	}
}
