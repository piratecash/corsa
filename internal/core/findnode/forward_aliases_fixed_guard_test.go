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

// Finite exact source/receiver closure, not whole-program typed/unsafe alias
// verification or a resource/allocation proof. No body normalization is used.
func ownedForwardAliasesSourceErrors(sources map[string][]byte) []string {
	var failures []string
	rawPins := map[string]string{
		"owned_resources.go":     "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a",
		"owned_bridge_commit.go": "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255",
		"budget.go":              "9a9b95192bd6928694d23cac19550dbb3a29ef9113151130d0d545f19d7fe4fa",
		"intake_owner.go":        "d46df9daabe08dd79f0fb8026c816f15e41694239b32eaed8522a678cb03dbbc",
		"pending.go":             "ecd63fca64c9d921e35bf4e990bdc9934918787b902ce298244884540500c9e0",
	}
	for path, want := range rawPins {
		sum := sha256.Sum256(sources[path])
		if hex.EncodeToString(sum[:]) != want {
			failures = append(failures, "owned alias raw source changed: "+path)
		}
	}
	wantedCalls := map[string]int{
		"forwardDropAliasLocked:owned_bridge_dispatch.go:Take":                      1,
		"newOwnedForwardCalendar:owned_bridge_commit.go:finalizeOwnedCandidateBusy": 1,
		"applyCombinedLocked:owned_bridge_commit.go:commitOwnedFinalLocked":         1,
		"applyCombinedLocked:intake_owner.go:commitLocked":                          1,
		"forwardAliasCountLocked:budget.go:Pending":                                 1,
		"forwardAliasCountLocked:budget.go:Reserve":                                 1,
		"forwardAliasCountLocked:budget.go:planCombinedFloorLocked":                 1,
		"forwardAliasCountLocked:budget.go:retireableLocked":                        1,
		"forwardAliasCountLocked:intake_owner.go:aliasesMatchLocked":                1,
		"forwardAliasCountLocked:intake_owner.go:publishLocked":                     1,
		"forwardDropAliasLocked:budget.go:Cancel":                                   1,
		"forwardDropAliasLocked:budget.go:Consume":                                  2,
		"forwardDropAliasLocked:intake_owner.go:releaseLocked":                      1,
		"forwardDropAliasLocked:intake_owner.go:takeForward":                        1,
		"forwardHasAliasLocked:budget.go:Consume":                                   1,
		"forwardHasAliasLocked:intake_owner.go:aliasesMatchLocked":                  1,
		"forwardInsertAliasLocked:budget.go:Reserve":                                1,
		"newOwnedForwardCalendar:intake_owner.go:commitLocked":                      1,
	}
	wantedFields := map[string]int{
		"aliases:owned_resources.go:ResourceSnapshot":                       1,
		"slots:owned_bridge_local_target.go:ReserveReceived":                3,
		"slots:owned_bridge_local_target.go:drainOwnedLocalAliasesLocked":   1,
		"aliases:owned_bridge_local_target.go:drainOwnedLocalAliasesLocked": 2,
		"aliases:owned_bridge_local_target.go:ReserveReceived":              1,
		"slots:owned_bridge_local_target.go:EndOriginalSession":             2,
		"ownedAliases:owned_bridge_commit.go:finalizeOwnedCandidateBusy":    1,
		"slots:owned_bridge_commit.go:finalizeOwnedCandidateBusy":           1,
		"aliasCount:budget.go:applyCombinedLocked":                          2,
		"aliasCount:budget.go:expireSlots":                                  3,
		"aliasCount:budget.go:forwardAliasCountLocked":                      1,
		"aliasCount:budget.go:forwardDropAliasLocked":                       3,
		"aliasCount:budget.go:forwardHasAliasLocked":                        1,
		"aliasCount:budget.go:forwardInsertAliasLocked":                     2,
		"aliases:budget.go:applyCombinedLocked":                             1,
		"aliases:budget.go:expireSlots":                                     4,
		"aliases:budget.go:forwardDropAliasLocked":                          4,
		"aliases:budget.go:forwardHasAliasLocked":                           1,
		"aliases:budget.go:forwardInsertAliasLocked":                        1,
		"aliases:budget.go:planCombinedFloorLocked":                         1,
		"aliases:intake_owner.go:publishLocked":                             1,
		"aliases:target_owner.go:drain":                                     2,
		"aliases:target_owner.go:retireAlias":                               2,
		"ownedAliases:budget.go:expireSlots":                                1,
		"ownedAliases:budget.go:forwardAliasCountLocked":                    1,
		"ownedAliases:budget.go:forwardDropAliasLocked":                     1,
		"ownedAliases:budget.go:forwardHasAliasLocked":                      1,
		"ownedAliases:budget.go:forwardInsertAliasLocked":                   1,
		"ownedAliases:budget.go:planCombinedFloorLocked":                    1,
		"ownedAliases:intake_owner.go:commitLocked":                         1,
		"slots:budget.go:expireSlots":                                       2,
		"slots:budget.go:forwardAliasCountLocked":                           1,
		"slots:budget.go:forwardDropAliasLocked":                            2,
		"slots:budget.go:forwardHasAliasLocked":                             1,
		"slots:budget.go:forwardInsertAliasLocked":                          1,
		"slots:budget.go:planCombinedFloorLocked":                           1,
		"slots:intake_owner.go:commitLocked":                                1,
		"slots:signing.go:Cancel":                                           2,
		"slots:signing.go:CloseSession":                                     2,
		"slots:signing.go:prepareConsumptionLocked":                         3,
		"slots:signing.go:consumeActualSourceLocked":                        1,
		"slots:signing.go:consumeSourceLocked":                              1,
		"slots:signing.go:Pending":                                          1,
		"slots:signing.go:Reserve":                                          2,
		"slots:signing.go:expireSlots":                                      2,
	}
	signatures := map[string]string{
		"newOwnedForwardCalendar":  "funcnewOwnedForwardCalendar(targetdomain.PeerIdentity,nowInstant,initialTokensuint32)(*ForwardCalendar,error)",
		"applyCombinedLocked":      "func(c*ForwardCalendar)applyCombinedLocked(plancombinedForwardPlan,slot*forwardSlot,nowInstant)",
		"forwardAliasCountLocked":  "func(c*ForwardCalendar)forwardAliasCountLocked()int",
		"forwardHasAliasLocked":    "func(c*ForwardCalendar)forwardHasAliasLocked(slot*forwardSlot)bool",
		"forwardDropAliasLocked":   "func(c*ForwardCalendar)forwardDropAliasLocked(slot*forwardSlot)bool",
		"forwardInsertAliasLocked": "func(c*ForwardCalendar)forwardInsertAliasLocked(slot*forwardSlot)",
	}
	calls, fields, seen, shapes := map[string]int{}, map[string]int{}, map[string]string{}, map[string]string{}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return append(failures, "owned alias production parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if _, watched := signatures[fn.Name.Name]; watched {
					if path != "budget.go" || fn.Body == nil || seen[fn.Name.Name] != "" {
						failures = append(failures, "owned alias duplicate/foreign helper")
					} else {
						a, z := set.Position(fn.Pos()).Offset, set.Position(fn.Body.Pos()).Offset
						seen[fn.Name.Name] = strings.Join(strings.Fields(string(raw[a:z])), "")
					}
				}
				if path == "budget.go" && fn.Name.Name == "NewForwardCalendar" && fn.Body != nil {
					a, z := set.Position(fn.Body.Pos()).Offset, set.Position(fn.Body.End()).Offset
					sum := sha256.Sum256(raw[a:z])
					if hex.EncodeToString(sum[:]) != "edcfa67b636bca4fa700428c3c5728493a7e0e7ff01f1c53d89e21c2fd40ff14" {
						failures = append(failures, "public map constructor body changed")
					}
				}
			}
			callees := map[ast.Node]bool{}
			selectorNames := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				if s, ok := n.(*ast.SelectorExpr); ok {
					selectorNames[s.Sel] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if _, watched := signatures[n.Name]; watched && n != declaration && !selectorNames[n] {
						calls[n.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "owned alias helper value escape")
						}
					}
				case *ast.SelectorExpr:
					if _, watched := signatures[n.Sel.Name]; watched {
						calls[n.Sel.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "owned alias method-value escape")
						}
					}
					switch n.Sel.Name {
					case "ownedAliases", "aliases", "aliasCount", "slots":
						fields[n.Sel.Name+":"+site]++
					}
				case *ast.TypeSpec:
					if n.Name.Name == "ForwardCalendar" || n.Name.Name == "forwardSlot" || n.Name.Name == "Reservation" || n.Name.Name == "combinedForwardPlan" {
						if path != "budget.go" || n.Assign.IsValid() || shapes[n.Name.Name] != "" {
							failures = append(failures, "owned alias type escaped original declaration")
						}
						a, z := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
						shapes[n.Name.Name] = strings.Join(strings.Fields(string(raw[a:z])), "")
					}
				}
				return true
			})
		}
	}
	wantedShapes := map[string]string{
		"ForwardCalendar":     "ForwardCalendarstruct{targetdomain.PeerIdentitymusync.Mutexprojected,actualforwardBucketlastInstantslotsmap[*forwardSlot]struct{}ownedAliasesboolaliases[MaxPendingPerTarget]*forwardSlotaliasCountint}",
		"forwardSlot":         "forwardSlotstruct{at,latestInstant}",
		"Reservation":         "Reservationstruct{calendar*ForwardCalendarslot*forwardSlot}",
		"combinedForwardPlan": "combinedForwardPlanstruct{projectedforwardBucketslotforwardSlot}",
	}
	if !reflect.DeepEqual(seen, signatures) || !reflect.DeepEqual(calls, wantedCalls) {
		failures = append(failures, "owned alias exact helper signatures/caller roots changed")
	}
	if !reflect.DeepEqual(fields, wantedFields) || !reflect.DeepEqual(shapes, wantedShapes) {
		failures = append(failures, "owned alias tagged storage/field sites changed")
	}
	// RAW pins close all exact writes, expiry swap/recheck, pre-CAS gates and the
	// fixed postclaim suffix. Fields and method-value checks cover foreign files.
	return failures
}
func TestOwnedForwardAliasesClosedSource(t *testing.T) {
	if failures := ownedForwardAliasesSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedForwardAliasesSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, file, before, after string }{
		{"mode-bit-flip", "budget.go", "ownedAliases: true", "ownedAliases: false"},
		{"map-allocation-owned", "budget.go", "ownedAliases: true", "ownedAliases: true, slots: make(map[*forwardSlot]struct{})"},
		{"private-constructor-escape", "alias_escape.go", "", "package findnode\nvar escapedAliasConstructor = newOwnedForwardCalendar\n"},
		{"mode-value-escape", "alias_escape.go", "", "package findnode\nvar escapedAliasMethod = (*ForwardCalendar).forwardAliasCountLocked\n"},
		{"dual-authority", "alias_escape.go", "", "package findnode\nfunc aliasDual(c *ForwardCalendar){c.slots=make(map[*forwardSlot]struct{})}\n"},
		{"wrong-fixed-bound", "budget.go", "[MaxPendingPerTarget]*forwardSlot", "[MaxPendingPerTarget+1]*forwardSlot"},
		{"array-view-escape", "alias_escape.go", "", "package findnode\nfunc aliasView(c *ForwardCalendar)[]*forwardSlot{return c.aliases[:]}\n"},
		{"persisted-index", "budget.go", "type forwardSlot struct{ at, latest Instant }", "type forwardSlot struct{ at, latest Instant; index int }"},
		{"slot-value-dedup", "budget.go", "if c.aliases[i] == slot {", "if *c.aliases[i] == *slot {"},
		{"missing-tail-clear", "budget.go", "c.aliases[last] = nil", "_ = last"},
		{"swap-skips-expired", "budget.go", "\t\t\t\tc.aliasCount--\n\t\t\t\tcontinue", "\t\t\t\tc.aliasCount--\n\t\t\t\ti++\n\t\t\t\tcontinue"},
		{"fallible-check-after-CAS", "intake_owner.go", "\tcalendar.applyCombinedLocked(plan, slot, now)", "\tif calendar.forwardAliasCountLocked()>=MaxPendingPerTarget {return ErrCapacity}\n\tcalendar.applyCombinedLocked(plan, slot, now)"},
		{"owned-map-write-after-CAS", "budget.go", "\tc.aliases[c.aliasCount] = slot\n\tc.aliasCount++\n}\n\n// retireableLocked", "\tc.slots[slot] = struct{}{}\n}\n\n// retireableLocked"},
		{"reuse-retired-calendar", "intake_owner.go", "calendar, err = newOwnedForwardCalendar(entry.request.Target, now, o.profile.initialTokens)", "calendar, err = o.calendars[calendarIndex], error(nil)\n\t\tcalendar.target = entry.request.Target"},
		{"zero-retained-slot", "budget.go", "\t\t\t\tc.aliases[last] = nil\n\t\t\t\tc.aliasCount--\n\t\t\t\treturn true", "\t\t\t\t*slot = forwardSlot{}\n\t\t\t\tc.aliases[last] = nil\n\t\t\t\tc.aliasCount--\n\t\t\t\treturn true"},
		{"public-limit-change", "budget.go", "if c.forwardAliasCountLocked() >= MaxPendingPerTarget {", "if c.forwardAliasCountLocked() > MaxPendingPerTarget {"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if tc.before == "" {
				if _, exists := sources[tc.file]; exists {
					t.Fatal("SOURCE foreign alias file exists")
				}
				sources[tc.file] = []byte(tc.after)
			} else {
				raw := string(sources[tc.file])
				count := strings.Count(raw, tc.before)
				expected := 1
				if tc.name == "slot-value-dedup" || tc.name == "missing-tail-clear" {
					expected = 2
				}
				if count != expected {
					t.Fatal("SOURCE owned alias exact mutation anchor", tc.name, count, expected)
				}
				sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if failures := ownedForwardAliasesSourceErrors(sources); len(failures) == 0 {
				t.Fatal("owned alias SOURCE control escaped closure", tc.name)
			}
		})
	}
}
