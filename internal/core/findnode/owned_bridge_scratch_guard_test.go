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

// Finite SOURCE closure with raw byte pins; no unsafe/dataflow/runtime proof.
// Trusted formatting needs exact pin and control-anchor rebinding, not a waiver.
func ownedAttemptScratchSourceErrors(sources map[string][]byte) []string {
	failures := ownedBridgeSourceErrors(sources)
	wantedRaw := "4f2a750803e6118b187eae648f22825452a3dedab9de1da1d9e5ee77d25876a6"
	types := map[string]string{
		"ownedAttemptScratch":   "ownedAttemptScratchstruct{borrowedatomic.Booltargets[MaxPendingTotal]ownedProjectionTargettargetCountintoffered[MaxPendingTotal]ownedProjectionSourceofferedCountintmaximumdomain.ConnID}",
		"ownedAttemptClaim":     "ownedAttemptClaimstruct{owner*dormantIntakeStatecut*ownedCutStatephaseatomic.Uint32projectedboolroundPlan*ownedRoundPlanStateexecution*ownedRoundExecution}",
		"ownedAttempt":          "ownedAttemptstruct{claim*ownedAttemptClaim}",
		"ownedProjectionTarget": "ownedProjectionTargetstruct{identitydomain.PeerIdentityoldest[4]Session}",
		"ownedProjectionSource": "ownedProjectionSourcestruct{sessionSessionclassSessionClass}",
	}
	signatures := map[string]string{
		"tryOwnedAttempt":         "func(intakeDormantIntake)tryOwnedAttempt(cutOwnedCut)(ownedAttempt,error)",
		"projectOwnedMembership":  "func(attemptownedAttempt)projectOwnedMembership(targets[]domain.PeerIdentity,offered[]Session)error",
		"ownedHistoricalPriority": "func(attemptownedAttempt)ownedHistoricalPriority(sourceSession,targetdomain.PeerIdentity)(Priority,error)",
		"releaseOwnedAttempt":     "func(attemptownedAttempt)releaseOwnedAttempt()error",
	}
	phases := map[string]string{"ownedAttemptLive": "ownedAttemptLiveuint32=1", "ownedAttemptBusy": "ownedAttemptBusyuint32=2", "ownedAttemptClosing": "ownedAttemptClosinguint32=3", "ownedAttemptClosed": "ownedAttemptCloseduint32=4"}
	seenTypes, seenSignatures, seenPhases := map[string]string{}, map[string]string{}, map[string]string{}
	oldScratchRefs, methodRefs, fieldRefs := map[string]int{}, map[string]int{}, map[string]int{}
	externalPhaseRefs := map[string]int{}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return []string{"scratch production parse"}
		}
		if path == "owned_bridge_scratch.go" {
			sum := sha256.Sum256(raw)
			if hex.EncodeToString(sum[:]) != wantedRaw {
				failures = append(failures, "scratch exact raw file changed")
			}
			imports := map[string]int{}
			for _, imp := range file.Imports {
				imports[strings.Trim(imp.Path.Value, "\"")]++
				if imp.Name != nil {
					failures = append(failures, "scratch import alias")
				}
			}
			if !reflect.DeepEqual(imports, map[string]int{"sync/atomic": 1, "github.com/piratecash/corsa/internal/core/domain": 1}) {
				failures = append(failures, "scratch exact imports changed")
			}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if _, tracked := signatures[fn.Name.Name]; tracked {
					if path != "owned_bridge_scratch.go" || fn.Body == nil {
						failures = append(failures, "scratch duplicate/foreign method")
					} else {
						start, end := set.Position(fn.Pos()).Offset, set.Position(fn.Body.Pos()).Offset
						if _, duplicate := seenSignatures[fn.Name.Name]; duplicate {
							failures = append(failures, "scratch duplicate method")
						}
						seenSignatures[fn.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
					}
				} else if path == "owned_bridge_scratch.go" {
					failures = append(failures, "scratch extra method/helper")
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			packageConst := false
			if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.CONST {
				packageConst = true
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if _, tracked := types[n.Name]; tracked && path != "owned_bridge_scratch.go" {
						oldScratchRefs[site+":"+n.Name]++
					}
					if _, tracked := signatures[n.Name]; tracked && n != declaration {
						methodRefs[n.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "scratch method value escape")
						}
					}
					if _, tracked := phases[n.Name]; tracked && path != "owned_bridge_scratch.go" {
						externalPhaseRefs[site+":"+n.Name]++
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "scratch" {
						fieldRefs[site]++
					}
					if path == "owned_bridge_scratch.go" {
						switch n.Sel.Name {
						case "closed", "valid", "sourceWindows", "coordinator", "profile", "pending", "calendars", "capacity", "round", "cursor", "watermark", "Sample", "TryReadClock", "Now":
							failures = append(failures, "scratch touched closed/live/accounting/time state")
						}
					}
				case *ast.TypeSpec:
					if path == "owned_bridge_scratch.go" {
						start, end := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
						if _, ok := types[n.Name.Name]; !ok || n.Assign.IsValid() {
							failures = append(failures, "scratch extra/alias type")
						}
						seenTypes[n.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
					}
				case *ast.ValueSpec:
					if path == "owned_bridge_scratch.go" && packageConst {
						if len(n.Names) != 1 {
							failures = append(failures, "scratch value shape")
						} else {
							start, end := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
							seenPhases[n.Names[0].Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
						}
					}
				case *ast.GenDecl:
					if path == "owned_bridge_scratch.go" && n.Tok == token.VAR && n.Pos() == decl.Pos() {
						failures = append(failures, "scratch mutable global")
					}
				case *ast.GoStmt, *ast.SendStmt, *ast.FuncLit, *ast.MapType, *ast.ChanType:
					if path == "owned_bridge_scratch.go" {
						failures = append(failures, "scratch acquired worker/callback/map/channel")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(seenTypes, types) || !reflect.DeepEqual(seenSignatures, signatures) || !reflect.DeepEqual(seenPhases, phases) {
		failures = append(failures, "scratch five types/four methods/four phases changed")
	}
	if !reflect.DeepEqual(oldScratchRefs, map[string]int{"owned_resources.go:<package>:ownedAttemptClaim": 1, "owned_bridge.go:<package>:ownedAttemptScratch": 1, "owned_bridge_commit.go:finalizeOwnedCandidate:ownedAttempt": 1, "owned_bridge_commit.go:commitOwnedFinalLocked:ownedAttempt": 1, "owned_bridge_commit.go:finalizeOwnedCandidateBusy:ownedAttempt": 1, "owned_bridge_round_execute.go:<package>:ownedAttemptClaim": 1, "owned_bridge_round_execute.go:consumeOwnedRoundPlan:ownedAttempt": 1, "owned_bridge_round_plan.go:<package>:ownedAttemptClaim": 1, "owned_bridge_round_plan.go:prepareOwnedRoundPlan:ownedAttempt": 1, "owned_bridge_round_plan.go:copyOwnedRoundPlanLocked:ownedAttempt": 1}) {
		failures = append(failures, "scratch type escaped sole original owner field")
	}
	if !reflect.DeepEqual(externalPhaseRefs, map[string]int{"owned_bridge_receive.go:OfferReceived:ownedAttemptLive": 2, "owned_bridge_receive.go:OfferReceived:ownedAttemptBusy": 1, "owned_bridge_round_execute.go:consumeOwnedRoundPlan:ownedAttemptLive": 2, "owned_bridge_round_execute.go:consumeOwnedRoundPlan:ownedAttemptBusy": 1, "owned_bridge_round_execute.go:consumeOwnedRoundPlan:ownedAttemptClosed": 1, "owned_bridge_round_execute.go:consumeOwnedRoundPlan:ownedAttemptClosing": 1, "owned_bridge_round_execute.go:validateOwnedRoundExecutionLocked:ownedAttemptBusy": 1, "owned_bridge_commit.go:finalizeOwnedCandidateBusy:ownedAttemptBusy": 1, "owned_bridge_round_plan.go:prepareOwnedRoundPlan:ownedAttemptLive": 2, "owned_bridge_round_plan.go:prepareOwnedRoundPlan:ownedAttemptBusy": 1, "owned_bridge_round_plan.go:prepareOwnedRoundPlan:ownedAttemptClosed": 1, "owned_bridge_round_plan.go:prepareOwnedRoundPlan:ownedAttemptClosing": 1, "owned_bridge_commit.go:finalizeOwnedCandidate:ownedAttemptLive": 2, "owned_bridge_commit.go:finalizeOwnedCandidate:ownedAttemptBusy": 1, "owned_bridge_commit.go:finalizeOwnedCandidate:ownedAttemptClosed": 1, "owned_bridge_commit.go:finalizeOwnedCandidate:ownedAttemptClosing": 1}) {
		failures = append(failures, "scratch exact external phase roles changed")
	}
	if !reflect.DeepEqual(methodRefs, map[string]int{"tryOwnedAttempt:owned_bridge_round_dispatch.go:CloseOwnedRound": 1, "releaseOwnedAttempt:owned_bridge_round_dispatch.go:CloseOwnedRound": 1, "projectOwnedMembership:owned_bridge_round_dispatch.go:CloseOwnedRound": 1, "tryOwnedAttempt:owned_bridge_receive.go:OfferReceived": 1, "projectOwnedMembership:owned_bridge_receive.go:OfferReceived": 1, "ownedHistoricalPriority:owned_bridge_receive.go:OfferReceived": 1, "releaseOwnedAttempt:owned_bridge_receive.go:OfferReceived": 1}) {
		failures = append(failures, "scratch gained production caller")
	}
	if !reflect.DeepEqual(fieldRefs, map[string]int{"owned_bridge_receive.go:OfferReceived": 1, "owned_bridge_round_plan.go:prepareOwnedRoundPlan": 1, "owned_bridge_round_plan.go:copyOwnedRoundPlanLocked": 1, "owned_bridge_scratch.go:tryOwnedAttempt": 1, "owned_bridge_scratch.go:projectOwnedMembership": 1, "owned_bridge_scratch.go:ownedHistoricalPriority": 1, "owned_bridge_scratch.go:releaseOwnedAttempt": 1, "owned_bridge_commit.go:finalizeOwnedCandidateBusy": 13, "owned_bridge_round_execute.go:consumeOwnedRoundPlan": 1, "owned_bridge_round_execute.go:validateOwnedRoundExecutionLocked": 1}) {
		failures = append(failures, "scratch field escaped exact four method sites")
	}
	if sources["owned_bridge_scratch.go"] == nil {
		failures = append(failures, "scratch source missing")
	}
	return failures
}
func TestOwnedAttemptScratchClosedSource(t *testing.T) {
	if failures := ownedAttemptScratchSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedAttemptScratchSourceGuardControls(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"extra-caller", "", "package findnode\nfunc scratchEscape(i DormantIntake,c OwnedCut){_,_=i.tryOwnedAttempt(c)}\n"},
		{"method-value-escape", "", "package findnode\nfunc scratchValue(a ownedAttempt){_=a.releaseOwnedAttempt}\n"},
		{"foreign-old-type-site", "", "package findnode\ntype scratchForeign struct{owner *dormantIntakeState}\n"},
		{"extra-exported-getter", "", "package findnode\nfunc(a ownedAttempt) Ready()bool{return true}\n"},
		{"second-coordinator", "claim := &ownedAttemptClaim{owner: owner, cut: sealed}", "claim := &ownedAttemptClaim{owner: owner, cut: sealed}; owner.coordinator=nil"},
		{"release-before-clear", "clear(s.targets[:])\n\tclear(s.offered[:])\n\ts.targetCount, s.offeredCount, s.maximum = 0, 0, 0\n\tc.projected = false\n\tc.roundPlan = nil\n\tc.execution = nil\n\tc.phase.Store(ownedAttemptClosed)\n\ts.borrowed.Store(false)", "s.borrowed.Store(false)\n\tclear(s.targets[:])\n\tclear(s.offered[:])\n\ts.targetCount, s.offeredCount, s.maximum = 0, 0, 0\n\tc.projected = false\n\tc.roundPlan = nil\n\tc.execution = nil\n\tc.phase.Store(ownedAttemptClosed)"},
		{"concurrent-release-busy", "CompareAndSwap(ownedAttemptLive, ownedAttemptClosing)", "CompareAndSwap(ownedAttemptBusy, ownedAttemptClosing)"},
		{"Session-closed-filter", "for originalIndex, source := range c.cut.originals.members {", "for originalIndex, source := range c.cut.originals.members { if source.state.closed.Load() {continue}"},
		{"tail-original-omission", "range c.cut.originals.members {", "range c.cut.originals.members[:len(c.cut.originals.members)-1] {"},
		{"target-filter-before-maximum", "s.maximum = max(s.maximum, source.ConnID())", "if source.Identity()==s.targets[0].identity {continue}; s.maximum = max(s.maximum, source.ConnID())"},
		{"fresh-clock-read", "q := Instant(c.cut.at.Elapsed())", "q := Instant(time.Now().UnixNano())"},
		{"accounting-charge", "c.projected = true", "c.owner.coordinator.round++; c.projected = true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if tc.before == "" {
				sources["owned_scratch_escape.go"] = []byte(tc.after)
			} else {
				raw := string(sources["owned_bridge_scratch.go"])
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("SOURCE scratch unique mutation anchor", tc.name)
				}
				sources["owned_bridge_scratch.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if failures := ownedAttemptScratchSourceErrors(sources); len(failures) == 0 {
				t.Fatal("SOURCE scratch control escaped exact closure")
			}
		})
	}
}
