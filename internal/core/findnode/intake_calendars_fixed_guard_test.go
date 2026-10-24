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

// Reviewed finite syntax/raw closure, not universal typed/unsafe dataflow.
func candidateFixedCalendarsSourceErrors(sources map[string][]byte) []string {
	failures := []string{}
	rawPins := map[string]string{
		"owned_bridge_commit.go": "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255",
		"intake_owner.go":        "464c803f0c96ef419752c9965b217ba0d0b74c9ff0469c6b3e08efb2df60fe18",
		"budget.go":              "9a9b95192bd6928694d23cac19550dbb3a29ef9113151130d0d545f19d7fe4fa",
		"pending.go":             "ecd63fca64c9d921e35bf4e990bdc9934918787b902ce298244884540500c9e0",
	}
	signatures := map[string]string{
		"calendarLocked":      "func(o*intakeCoordinator)calendarLocked(targetdomain.PeerIdentity)*ForwardCalendar",
		"calendarCountLocked": "func(o*intakeCoordinator)calendarCountLocked()int",
	}
	wantedCalls := map[string]int{
		"calendarLocked:owned_bridge_commit.go:finalizeOwnedCandidateBusy": 1, "calendarLocked:intake_owner.go:commitLocked": 1, "calendarCountLocked:intake_owner.go:publishLocked": 1}
	wantedFields := map[string]int{"owned_resources.go:ResourceSnapshot": 1,
		"owned_bridge_commit.go:finalizeOwnedCandidateBusy": 2,
		"owned_bridge_commit.go:commitOwnedFinalLocked":     2, "intake_owner.go:calendarLocked": 1, "intake_owner.go:calendarCountLocked": 1, "intake_owner.go:commitLocked": 3, "intake_owner.go:publishLocked": 1}
	// ForwardCalendar keeps its unchanged public MAP branch in budget.go. Its
	// complete finite identifier roots also reject OTHERFILE aliases/new books.
	wantedTypes := map[string]int{"owned_resources.go:<package>": 1,
		"owned_bridge_commit.go:finalizeOwnedCandidateBusy": 1,
		"owned_bridge_commit.go:commitOwnedFinalLocked":     1,
		"intake_owner.go:<package>":                         2, "intake_owner.go:calendarLocked": 1, "intake_owner.go:aliasesMatchLocked": 1,
		"budget.go:<package>": 2, "budget.go:NewForwardCalendar": 2,
		"budget.go:newOwnedForwardCalendar": 2,
		"budget.go:forwardAliasCountLocked": 1, "budget.go:forwardHasAliasLocked": 1,
		"budget.go:forwardDropAliasLocked": 1, "budget.go:forwardInsertAliasLocked": 1,
		"budget.go:Reserve": 1, "budget.go:Cancel": 1, "budget.go:Consume": 1, "budget.go:Pending": 1,
		"budget.go:Expire": 1, "budget.go:expireSlots": 1, "budget.go:Target": 1,
		"budget.go:planCombinedLocked": 1, "budget.go:planCombinedFloorLocked": 1,
		"budget.go:applyCombinedLocked": 1, "budget.go:retireableLocked": 1,
	}
	seenSignatures := map[string]string{}
	calls, fields, typeRefs, arrays := map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	snapshotFields, ownerField := 0, ""
	for path, want := range rawPins {
		raw, exists := sources[path]
		sum := sha256.Sum256(raw)
		if !exists || hex.EncodeToString(sum[:]) != want {
			failures = append(failures, "calendar exact raw changed: "+path)
		}
	}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return append(failures, "calendar production parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if _, tracked := signatures[fn.Name.Name]; tracked {
					if path != "intake_owner.go" || fn.Body == nil || seenSignatures[fn.Name.Name] != "" {
						failures = append(failures, "calendar duplicate/foreign helper declaration")
					} else {
						start, end := set.Position(fn.Pos()).Offset, set.Position(fn.Body.Pos()).Offset
						seenSignatures[fn.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
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
					if n.Name == "ForwardCalendar" {
						typeRefs[site]++
					}
					if _, tracked := signatures[n.Name]; tracked && n != declaration && !selectorNames[n] {
						calls[n.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "calendar helper value/alias escape")
						}
					}
				case *ast.SelectorExpr:
					if _, tracked := signatures[n.Sel.Name]; tracked {
						calls[n.Sel.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "calendar helper method-value escape")
						}
					}
					if n.Sel.Name == "calendars" {
						snapshot := false
						if id, ok := n.X.(*ast.Ident); ok {
							snapshot = id.Name == "snapshot" && site == "intake_owner.go:publishLocked"
						}
						if snapshot {
							snapshotFields++
						} else {
							fields[site]++
						}
					}
				case *ast.ArrayType:
					if star, ok := n.Elt.(*ast.StarExpr); ok {
						if id, ok := star.X.(*ast.Ident); ok && id.Name == "ForwardCalendar" {
							start, end := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
							shape := strings.Join(strings.Fields(string(raw[start:end])), "")
							arrays[site+":"+shape]++
						}
					}
				case *ast.KeyValueExpr:
					if key, ok := n.Key.(*ast.Ident); ok && key.Name == "calendars" {
						failures = append(failures, "calendar table literal/reset escaped original zero construction")
					}
				case *ast.TypeSpec:
					if n.Name.Name == "intakeCoordinator" {
						structure, ok := n.Type.(*ast.StructType)
						if !ok || path != "intake_owner.go" {
							failures = append(failures, "calendar owner type escaped")
							break
						}
						for _, field := range structure.Fields.List {
							for _, name := range field.Names {
								if name.Name == "calendars" {
									start, end := set.Position(field.Pos()).Offset, set.Position(field.End()).Offset
									if ownerField != "" {
										failures = append(failures, "calendar duplicate owner field")
									}
									ownerField = strings.Join(strings.Fields(string(raw[start:end])), "")
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	if ownerField != "calendars[MaxPendingTotal]*ForwardCalendar" || !reflect.DeepEqual(arrays, map[string]int{"intake_owner.go:<package>:[MaxPendingTotal]*ForwardCalendar": 1}) {
		failures = append(failures, "calendar exact original512-pointer field/storage shape changed")
	}
	if !reflect.DeepEqual(seenSignatures, signatures) || !reflect.DeepEqual(calls, wantedCalls) {
		failures = append(failures, "calendar exact two helper declarations/call roots changed")
	}
	if !reflect.DeepEqual(fields, wantedFields) || snapshotFields != 1 {
		failures = append(failures, "calendar field escaped exact read/select/store/snapshot sites")
	}
	if !reflect.DeepEqual(typeRefs, wantedTypes) {
		failures = append(failures, "calendar type escaped standalone/original owner roots")
	}
	return failures
}
func TestCandidateCalendarTableClosedSource(t *testing.T) {
	if failures := candidateFixedCalendarsSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

type calendarSourceChange struct{ path, before, after string }

func TestCandidateCalendarTableSourceControls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changes []calendarSourceChange
	}{
		{"map-storage", []calendarSourceChange{{"intake_owner.go", "calendars         [MaxPendingTotal]*ForwardCalendar", "calendars         map[int]*ForwardCalendar"}}},
		{"slice-storage", []calendarSourceChange{{"intake_owner.go", "calendars         [MaxPendingTotal]*ForwardCalendar", "calendars         []*ForwardCalendar"}}},
		{"wrong-bound", []calendarSourceChange{{"intake_owner.go", "calendars         [MaxPendingTotal]*ForwardCalendar", "calendars         [MaxPendingTotal+1]*ForwardCalendar"}}},
		{"unbounded-growth", []calendarSourceChange{{"intake_owner.go", "calendars         [MaxPendingTotal]*ForwardCalendar", "calendars         []*ForwardCalendar"}, {"intake_owner.go", "o.calendars[calendarIndex] = calendar", "o.calendars=append(o.calendars,calendar)"}}},
		{"array-view-escape", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarTableView(o *intakeCoordinator)[]*ForwardCalendar{return o.calendars[:]}\n"}}},
		{"element-address-escape", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarTableAddress(o *intakeCoordinator)**ForwardCalendar{return &o.calendars[0]}\n"}}},
		{"other-file-alias-write", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarOtherWriter(o *intakeCoordinator,c *ForwardCalendar){alias:=o;alias.calendars[0]=c}\n"}}},
		{"parallel-book", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nimport \"github.com/piratecash/corsa/internal/core/domain\"\nvar calendarParallelBook map[domain.PeerIdentity]*ForwardCalendar\n"}}},
		{"helper-value-escape", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarValue(o *intakeCoordinator){_=o.calendarLocked}\n"}}},
		{"table-copy", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarCopy(o *intakeCoordinator){copy:=o.calendars;_=copy}\n"}}},
		{"persisted-index", []calendarSourceChange{{"intake_owner.go", "calendar          *ForwardCalendar", "calendar          *ForwardCalendar\n calendarIndex int"}}},
		{"constructor-reset", []calendarSourceChange{{"intake_owner.go", "\to.publishLocked()\n\treturn o, nil", "o.calendars=[MaxPendingTotal]*ForwardCalendar{}\n\to.publishLocked()\n\treturn o, nil"}}},
		{"post-constructor-reset", []calendarSourceChange{{"calendar_table_escape.go", "", "package findnode\nfunc calendarReset(o *intakeCoordinator){o.calendars=[MaxPendingTotal]*ForwardCalendar{}}\n"}}},
		{"omitted-nil-filter", []calendarSourceChange{{"intake_owner.go", "if calendar == nil {\n\t\t\tcontinue\n\t\t}", "// nil filter removed"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			for _, change := range tc.changes {
				if change.before == "" {
					if sources[change.path] != nil {
						t.Fatal("SOURCE calendar foreign fixture path exists")
					}
					sources[change.path] = []byte(change.after)
				} else {
					raw := string(sources[change.path])
					if strings.Count(raw, change.before) != 1 {
						t.Fatal("SOURCE calendar unique mutation anchor", tc.name)
					}
					sources[change.path] = []byte(strings.Replace(raw, change.before, change.after, 1))
				}
			}
			if failures := candidateFixedCalendarsSourceErrors(sources); len(failures) == 0 {
				t.Fatal("SOURCE calendar mutation escaped closure", tc.name)
			}
		})
	}
}
