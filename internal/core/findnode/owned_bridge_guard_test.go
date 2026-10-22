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

// Finite SOURCE closure, not runtime/dataflow/unsafe proof. Raw file bytes pin
// all declarations, private shapes, signatures, body literals and comments;
// trusted FORMAT requires a reviewed explicit pin rebind, never normalization.
func ownedBridgeSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"owned_bridge_bootstrap.go": "2ee7f6b13d859326bbd2fb00e487c9530737a2f722359d762202710d1a1cd827",
		"owned_bridge_round_execute.go": "dedd956801a9a14580f4e22634d3a5eb579ebe22520eb45eedce564209d7eec4",
		"owned_bridge_round_plan.go":    "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51",
		"owned_bridge_commit.go":        "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255", "owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "owned_bridge_cut.go": "ec58d8addd9e5f7509a9da3c827fbf0d7b5ffc15ab2d37512d1f2278795c97de", "owned_bridge_scratch.go": "4f2a750803e6118b187eae648f22825452a3dedab9de1da1d9e5ee77d25876a6"}
	got := map[string]string{}
	closedTypes := map[string]bool{"DormantIntakeProfile": true, "ObservedMember": true, "ObservedDegree": true, "DormantIntake": true, "ObservationSink": true, "OwnedCut": true, "dormantIntakeState": true, "ownedCutState": true}
	functions := map[string]bool{"NewDormantIntake": true, "NewDormantFixtureIntake": true, "newDormantIntake": true, "newDormantIntakeAt": true, "Seal": true}
	refs := map[string]int{}
	oldTypeRefs := map[string]int{}
	wantOldTypeRefs := map[string]int{
		"owned_bridge_bootstrap.go:NewExecutingBootstrapIntake:DormantIntakeProfile": 1,
		"owned_bridge_bootstrap.go:NewExecutingBootstrapIntake:DormantIntake":        5,
		"owned_bridge_bootstrap.go:NewExecutingBootstrapIntake:ObservationSink":      5,
		"owned_resources.go:<package>:dormantIntakeState":                            2, "owned_resources.go:<package>:ownedCutState": 1, "owned_resources.go:holdOwnedResourceCustody:dormantIntakeState": 1, "owned_resources.go:ResourceSnapshot:DormantIntake": 1,
		"owned_bridge_forward_answer.go:<package>:dormantIntakeState":                   1,
		"owned_bridge_forward_answer.go:SealAnswer:ObservationSink":                     1,
		"owned_bridge_forward_answer.go:MatchOwnedAnswer:DormantIntake":                 1,
		"owned_bridge_local_target.go:<package>:dormantIntakeState":                     1,
		"owned_bridge_local_target.go:NewLocalTarget:DormantIntake":                     1,
		"owned_bridge_local_target.go:ReserveReceived:OwnedCut":                         1,
		"owned_bridge_answer.go:<package>:dormantIntakeState":                           1,
		"owned_bridge_answer.go:mintOwnedAnswerWork:dormantIntakeState":                 1,
		"owned_bridge_dispatch.go:<package>:dormantIntakeState":                         1,
		"owned_bridge_dispatch.go:<package>:ownedCutState":                              1,
		"owned_bridge_round_dispatch.go:<package>:dormantIntakeState":                   1,
		"owned_bridge_round_dispatch.go:NewExecutingIntake:DormantIntakeProfile":        1,
		"owned_bridge_round_dispatch.go:NewExecutingIntake:DormantIntake":               2,
		"owned_bridge_round_dispatch.go:NewExecutingIntake:ObservationSink":             2,
		"owned_bridge_round_dispatch.go:NewExecutingFixtureIntake:DormantIntakeProfile": 1,
		"owned_bridge_round_dispatch.go:NewExecutingFixtureIntake:DormantIntake":        2,
		"owned_bridge_round_dispatch.go:NewExecutingFixtureIntake:ObservationSink":      2,
		"owned_bridge_round_dispatch.go:CloseOwnedRound:DormantIntake":                  1,
		"owned_bridge_round_dispatch.go:CloseOwnedRound:OwnedCut":                       1,
		"owned_bridge_round_dispatch.go:OwnedRoundPending:DormantIntake":                1,
		"owned_bridge_round_dispatch.go:EndOriginalSession:DormantIntake":               1,
		"owned_bridge_receive.go:<package>:dormantIntakeState":                          1,
		"owned_bridge_receive.go:ownedReceivedMatches:dormantIntakeState":               1,
		"owned_bridge_receive.go:SealReceived:ObservationSink":                          1,
		"owned_bridge_receive.go:OfferReceived:DormantIntake":                           1,
		"owned_bridge_receive.go:OfferReceived:OwnedCut":                                1,
		"owned_bridge_scratch.go:tryOwnedAttempt:DormantIntake":                         1,
		"owned_bridge_scratch.go:tryOwnedAttempt:OwnedCut":                              1,
		"owned_bridge_scratch.go:<package>:dormantIntakeState":                          1,
		"owned_bridge_scratch.go:<package>:ownedCutState":                               1,
	}

	for path, raw := range sources {
		if _, ok := pins[path]; ok {
			sum := sha256.Sum256(raw)
			got[path] = hex.EncodeToString(sum[:])
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"owned bridge production parse"}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if functions[fn.Name.Name] {
					wantPath := "owned_bridge.go"
					if fn.Name.Name == "Seal" {
						wantPath = "owned_bridge_cut.go"
					}
					if path != wantPath {
						failures = append(failures, "duplicate/foreign bridge declaration: "+site)
					}
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					callees[call.Fun] = true
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if closedTypes[id.Name] && path != "owned_bridge.go" && path != "owned_bridge_cut.go" {
						oldTypeRefs[site+":"+id.Name]++
					}
					if functions[id.Name] && id != declaration {
						refs[id.Name+":"+site]++
						if !callees[id] {
							failures = append(failures, "bridge function/method value escape: "+site)
						}
					}
				}
				if _, pinned := pins[path]; pinned {
					switch n.(type) {
					case *ast.GoStmt, *ast.SendStmt:
						failures = append(failures, "bridge acquired worker")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(oldTypeRefs, wantOldTypeRefs) {
		failures = append(failures, "opaque bridge old type escaped exact four site/count additions")
	}
	if !reflect.DeepEqual(got, pins) {
		failures = append(failures, "owned bridge exact raw files changed")
	}
	if !reflect.DeepEqual(refs, map[string]int{"newDormantIntakeAt:owned_bridge.go:newDormantIntake": 1, "newDormantIntakeAt:owned_bridge_bootstrap.go:NewExecutingBootstrapIntake": 1, "NewDormantIntake:owned_bridge_round_dispatch.go:NewExecutingIntake": 1, "NewDormantFixtureIntake:owned_bridge_round_dispatch.go:NewExecutingFixtureIntake": 1, "newDormantIntake:owned_bridge.go:NewDormantIntake": 1, "newDormantIntake:owned_bridge.go:NewDormantFixtureIntake": 1}) {
		failures = append(failures, "owned bridge exact internal caller inventory changed")
	}
	return failures
}
func TestOwnedBridgeClosedSource(t *testing.T) {
	if failures := ownedBridgeSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

// Text mutations are SOURCE controls only, never FUNCTIONAL mutation proof.
func TestOwnedBridgeSourceGuardControls(t *testing.T) {
	for _, tc := range []struct{ name, file, before, after string }{
		{"duplicate-function", "", "", "package findnode\nfunc NewDormantIntake(){}\n"},
		{"direct-state", "", "", "package findnode\nvar directOwnedCut = OwnedCut{}\n"},
		{"additional-call", "", "", "package findnode\nfunc extraBridge(s ObservationSink){_,_=s.Seal(findnodebridge.CaptureStamp{},findnodebridge.OwnedInstant{},findnodebridge.OwnedBoundary{},nil,ObservedDegree{})}\n"},
		{"method-value", "", "", "package findnode\nvar escapedSeal = (ObservationSink{}).Seal\n"},
		{"getter", "owned_bridge.go", "type OwnedCut struct{ sealedCut *ownedCutState }", "type OwnedCut struct { sealedCut *ownedCutState }; func (c OwnedCut) Ready() bool{return true}"},
		{"capacity-owner", "owned_bridge.go", "nodeCap: 512", "nodeCap: 513"},
		{"raw-clock", "owned_bridge_cut.go", "q := observedAt.Elapsed()", "q := time.Since(time.Now())"},
		{"source-charge", "owned_bridge_cut.go", "owner := sink.observationOwner", "owner := sink.observationOwner; owner.coordinator.round++"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if tc.file == "" {
				sources["owned_bridge_escape.go"] = []byte(tc.after)
			} else {
				raw := string(sources[tc.file])
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("SOURCE unique anchor", tc.name)
				}
				sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if failures := ownedBridgeSourceErrors(sources); len(failures) == 0 {
				t.Fatal("SOURCE control escaped bridge closure")
			}
		})
	}
}
