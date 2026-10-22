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

// Finite syntax/raw closure, not typed dataflow or a runtime allocation proof.
// Trusted formatting requires literal/anchor rebinding with an exact byte diff.
func candidateFixedGroupsSourceErrors(sources map[string][]byte) []string {
	failures := []string{}
	wantedRaw := map[string]string{
		"owned_resources.go":         "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a",
		"owned_bridge_round_plan.go": "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51",
		"owned_bridge_commit.go":     "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255",
		"intake_groups.go":           "40311189acc0513c1d1486e445629bc312f5f419dc032cd50358c2f408223734",
		"intake_owner.go":            "0f413c74384d17db45866461c6bef5e2340f650ee50646eadc6e3d6e3efafe53",
	}
	types := map[string]string{
		"candidateRecordKey":   "candidateRecordKeystruct{cellinttargetdomain.PeerIdentity}",
		"candidateGroupRecord": "candidateGroupRecordstruct{expiresInstantrefsint}",
		"candidateGroupPlan":   "candidateGroupPlanstruct{groupdomain.SourceGroupcellintnameboolkeycandidateRecordKeyexpiresInstant}",
		"candidateGroupSlot":   "candidateGroupSlotstruct{keycandidateRecordKeyrecordcandidateGroupRecord}",
		"candidateGroups":      "candidateGroupsstruct{named[candidateNamedGroups]domain.SourceGroupnamedCountintrecords[candidateMaxRecords]candidateGroupSlotrecordCountintgenerationuint64lastUseInstantusedbool}",
	}
	signatures := map[string]string{
		"newCandidateGroups": "funcnewCandidateGroups()candidateGroups",
		"prospective":        "func(g*candidateGroups)prospective(groupdomain.SourceGroup)int",
		"orderCell":          "func(g*candidateGroups)orderCell(groupdomain.SourceGroup)int",
		"recordIndex":        "func(g*candidateGroups)recordIndex(keycandidateRecordKey)int",
		"record":             "func(g*candidateGroups)record(keycandidateRecordKey)(candidateGroupRecord,bool)",
		"counts":             "func(g*candidateGroups)counts()(names,recordsint)",
		"plan":               "func(g*candidateGroups)plan(groupdomain.SourceGroup,targetdomain.PeerIdentity,slotInstant,capint)(candidateGroupPlan,error)",
		"commit":             "func(g*candidateGroups)commit(plancandidateGroupPlan,nowInstant)",
		"release":            "func(g*candidateGroups)release(keycandidateRecordKey)",
		"finish":             "func(g*candidateGroups)finish(keycandidateRecordKey,atInstant)error",
		"expire":             "func(g*candidateGroups)expire(nowInstant)",
		"reset":              "func(g*candidateGroups)reset(nowInstant,rowsint)bool",
	}
	constants := map[string]string{
		"candidateNamedGroups": "candidateNamedGroups=256",
		"candidateGroupCells":  "candidateGroupCells=candidateNamedGroups+1",
		"candidateMaxRecords":  "candidateMaxRecords=3840+16*candidateGroupCells",
		"candidateGroupWindow": "candidateGroupWindow=Instant(10*time.Minute)",
	}
	wantedCalls := map[string]int{"finish:owned_bridge_dispatch.go:Finish": 1,
		"orderCell:owned_bridge_round_plan.go:copyOwnedRoundPlanLocked": 1,
		"plan:owned_bridge_commit.go:commitOwnedFinalLocked":            1,
		"record:owned_bridge_commit.go:commitOwnedFinalLocked":          1,
		"commit:owned_bridge_commit.go:commitOwnedFinalLocked":          1,
		"newCandidateGroups:intake_owner.go:newIntakeCoordinator":       1,
		"prospective:intake_groups.go:plan":                             1,
		"orderCell:intake_owner.go:less":                                2, "orderCell:intake_owner.go:sealRound": 1,
		"plan:intake_owner.go:commitLocked": 1, "commit:intake_owner.go:commitLocked": 1,
		"release:intake_owner.go:releaseLocked": 1, "finish:intake_owner.go:finishForward": 1,
		"expire:intake_owner.go:advanceLocked": 1, "reset:intake_owner.go:resetGroups": 1,
		"record:intake_groups.go:plan": 1, "record:intake_owner.go:commitLocked": 1,
		"counts:intake_owner.go:publishLocked": 1,
		"recordIndex:intake_groups.go:record":  1, "recordIndex:intake_groups.go:commit": 1,
		"recordIndex:intake_groups.go:release": 1, "recordIndex:intake_groups.go:finish": 1,
	}
	wantedFields := map[string]int{
		"owned_bridge_dispatch.go:Finish":                                 1,
		"owned_bridge_round_plan.go:copyOwnedRoundPlanLocked":             2,
		"owned_bridge_round_execute.go:validateOwnedRoundExecutionLocked": 1,
		"owned_bridge_round_execute.go:recordOwnedRoundSuccessLocked":     1,
		"owned_bridge_commit.go:commitOwnedFinalLocked":                   3,
		"intake_owner.go:less":                                            2, "intake_owner.go:sealRound": 1, "intake_owner.go:commitLocked": 3,
		"intake_owner.go:finishForward": 1, "intake_owner.go:releaseLocked": 1,
		"intake_owner.go:advanceLocked": 1, "intake_owner.go:resetGroups": 1,
		"intake_owner.go:publishLocked": 2,
	}
	seenTypes, seenSignatures, seenConstants := map[string]string{}, map[string]string{}, map[string]string{}
	calls, fields, outsideTypes := map[string]int{}, map[string]int{}, map[string]int{}
	for path, want := range wantedRaw {
		raw, ok := sources[path]
		sum := sha256.Sum256(raw)
		if !ok || hex.EncodeToString(sum[:]) != want {
			failures = append(failures, "fixed group exact raw file changed: "+path)
		}
	}
	for path, raw := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, 0)
		if err != nil {
			return append(failures, "fixed group production parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				body = fn.Body
				if path == "intake_groups.go" {
					if _, ok := signatures[fn.Name.Name]; !ok || fn.Body == nil || fn.Name.IsExported() {
						failures = append(failures, "fixed group extra/public method")
					} else {
						start, end := set.Position(fn.Pos()).Offset, set.Position(fn.Body.Pos()).Offset
						if seenSignatures[fn.Name.Name] != "" {
							failures = append(failures, "fixed group duplicate method")
						}
						seenSignatures[fn.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
					}
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.TypeSpec:
					if path == "intake_groups.go" {
						start, end := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
						if _, ok := types[n.Name.Name]; !ok || n.Assign.IsValid() || seenTypes[n.Name.Name] != "" {
							failures = append(failures, "fixed group extra/duplicate/alias type")
						}
						seenTypes[n.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
					}
				case *ast.ValueSpec:
					if path == "intake_groups.go" {
						if len(n.Names) != 1 {
							failures = append(failures, "fixed group value shape")
						} else {
							start, end := set.Position(n.Pos()).Offset, set.Position(n.End()).Offset
							if _, ok := constants[n.Names[0].Name]; !ok {
								failures = append(failures, "fixed group extra value")
							}
							seenConstants[n.Names[0].Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
						}
					}
				case *ast.Ident:
					if _, tracked := types[n.Name]; tracked && path != "intake_groups.go" {
						outsideTypes[site+":"+n.Name]++
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "groups" {
						fields[site]++
					}
					receiver := false
					if id, ok := n.X.(*ast.Ident); ok && id.Name == "g" && path == "intake_groups.go" {
						receiver = true
					}
					if owner, ok := n.X.(*ast.SelectorExpr); ok && owner.Sel.Name == "groups" {
						receiver = true
					}
					if _, tracked := signatures[n.Sel.Name]; tracked && receiver {
						calls[n.Sel.Name+":"+site]++
						if !callees[n] {
							failures = append(failures, "fixed group method-value escape")
						}
					}
					if n.Sel.Name == "named" || n.Sel.Name == "namedCount" || n.Sel.Name == "recordCount" || n.Sel.Name == "records" {
						snapshot := false
						if id, ok := n.X.(*ast.Ident); ok {
							snapshot = (id.Name == "snapshot" && n.Sel.Name == "records" && site == "intake_owner.go:publishLocked") || (id.Name == "s" && n.Sel.Name == "records" && site == "owned_resources.go:ResourceSnapshot")
						}
						if path != "intake_groups.go" && !snapshot {
							failures = append(failures, "fixed group storage escaped owner methods")
						}
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok {
						if id.Name == "newCandidateGroups" {
							calls[id.Name+":"+site]++
						}
						if path == "intake_groups.go" && (id.Name == "append" || id.Name == "make" || id.Name == "new") {
							failures = append(failures, "fixed group dynamic allocation")
						}
					}
				case *ast.GoStmt, *ast.SendStmt, *ast.FuncLit, *ast.MapType, *ast.ChanType:
					if path == "intake_groups.go" {
						failures = append(failures, "fixed group map/worker/callback/channel")
					}
				}
				return true
			})
			// Constructor values/aliases are rejected as well as duplicate calls.
			ast.Inspect(body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "newCandidateGroups" && !callees[id] {
					failures = append(failures, "fixed group constructor-value escape")
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(seenTypes, types) || !reflect.DeepEqual(seenSignatures, signatures) || !reflect.DeepEqual(seenConstants, constants) {
		failures = append(failures, "fixed group five shapes/twelve declarations/four constants changed")
	}
	if !reflect.DeepEqual(calls, wantedCalls) {
		failures = append(failures, "fixed group exact receiver/call roots changed")
	}
	if !reflect.DeepEqual(fields, wantedFields) {
		failures = append(failures, "fixed group original owner field roots changed")
	}
	if !reflect.DeepEqual(outsideTypes, map[string]int{"intake_owner.go:<package>:candidateGroups": 1, "intake_owner.go:<package>:candidateRecordKey": 1}) {
		failures = append(failures, "fixed group types escaped original owner/pending fields")
	}
	return failures
}
func TestCandidateFixedGroupsClosedSource(t *testing.T) {
	if failures := candidateFixedGroupsSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestCandidateFixedGroupsSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"production-map", "intake_groups.go", "records     [candidateMaxRecords]candidateGroupSlot", "records map[candidateRecordKey]candidateGroupRecord"},
		{"append-growth", "intake_groups.go", "g.records[index].record.refs++", "_ = append([]candidateGroupSlot{},g.records[index]);g.records[index].record.refs++"},
		{"extra-public-api", "fixed_group_escape.go", "", "package findnode\nfunc(g *candidateGroups) Ready()bool{return true}\n"},
		{"owner-replacement", "fixed_group_escape.go", "", "package findnode\nfunc fixedGroupReplace(o *intakeCoordinator){o.groups=newCandidateGroups()}\n"},
		{"parallel-book", "fixed_group_escape.go", "", "package findnode\nvar fixedGroupParallel candidateGroups\n"},
		{"physical-index-escape", "intake_groups.go", "key     candidateRecordKey\n\texpires Instant", "key     candidateRecordKey\n index int\n\texpires Instant"},
		{"missing-tail-clear", "intake_groups.go", "g.records[last] = candidateGroupSlot{}", "_ = last"},
		{"method-value-escape", "fixed_group_escape.go", "", "package findnode\nfunc fixedGroupMethod(o *intakeCoordinator){_=o.groups.record}\n"},
		{"wrong-record-lookup", "intake_groups.go", "g.records[index].key == key", "g.records[index].key.cell==key.cell"},
		{"check-after-source-CAS", "intake_owner.go", "\trecord, _ := o.groups.record(group.key)\n\tif record.refs == math.MaxInt {\n\t\treturn ErrCapacity\n\t}", "\t// moved max-ref check"},
		{"outside-storage-write", "fixed_group_escape.go", "", "package findnode\nfunc fixedGroupWrite(o *intakeCoordinator){o.groups.recordCount=0}\n"},
		{"shrink-eviction", "intake_groups.go", "cell := g.prospective(group)", "if g.recordCount>cap {g.recordCount=cap};cell := g.prospective(group)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if tc.before == "" {
				if sources[tc.path] != nil {
					t.Fatal("SOURCE fixed-group foreign path exists")
				}
				sources[tc.path] = []byte(tc.after)
			} else {
				raw := string(sources[tc.path])
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("SOURCE fixed-group unique anchor", tc.name)
				}
				sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
				if tc.name == "check-after-source-CAS" {
					raw = string(sources[tc.path])
					anchor := "\tsource.windows.rows[source.index] = source.row"
					if strings.Count(raw, anchor) != 1 {
						t.Fatal("SOURCE fixed-group CAS suffix anchor")
					}
					sources[tc.path] = []byte(strings.Replace(raw, anchor, tc.before+"\n"+anchor, 1))
				}
			}
			if failures := candidateFixedGroupsSourceErrors(sources); len(failures) == 0 {
				t.Fatal("SOURCE fixed-group control escaped closure", tc.name)
			}
		})
	}
}
