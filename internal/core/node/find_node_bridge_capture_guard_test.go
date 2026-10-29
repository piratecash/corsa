package node

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

// Raw AST-offset bytes preserve literal contents, comments AND whitespace.
// Trusted formatting requires an explicit pinned-body rebind, never body
// normalization. This finite SOURCE closure does not assert resource/runtime
// or unsafe/reflection/whole-program dataflow guarantees.
func ownedNodeObservationPins() map[string]string {
	return map[string]string{
		"find_node_bridge_capture.go:sealOwnedFindNodeObservation":                 "a94eedb9c823614a0632cc1b4ea46709290c0aff77b22700949c2e70d1c7f843",
		"find_node_bridge_construction.go:newFindNodeBridgeCandidateService":       "a5d3a5f4ec4c11ed3aeb89cf10ff4e1f6cae9fec2c59f8128539726bdaefe36d",
		"find_node_bridge_construction.go:prepareFindNodeBridgeConstruction":       "90361d550ef3465b3933fffc64cc2bf067356759b01d0f5113fa00d5c645eafb",
		"find_node_bridge_capture.go:captureOwnedFindNodeCut":                      "2d2b15403155f6945ef30c2b0102e2e26f6130b1b72017a90273e358822e3bd3",
		"peer_state_degree_statistic.go:validDegreeStatisticKey":                   "e49d9d35c48b507ef09ea0aa221a40cecb45179207d6db945843387d0ebd1bff",
		"find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService": "01052f314728fba976f31147a9c46c6ffdb56b95e4176b4a8f9144f713222363",
		"find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": "2343b9c62954608a8368ff91477c7121a190ecbb82fc50277ca7684d38b4be44",
		"find_node_bridge_construction.go:claim":                                   "9ccee7db98eb74261f9f0a63ef6f88e5ffd764996347698eaa83538247538547",
		"service.go:newServiceWithConstruction":                                    "21501b1ff8e9d4de7471b3451d093e0c5a2c58b656cef53b6ba84fa1bc609d26",
		"find_node_bridge_capture.go:captureOwnedFindNodeObservation":              "f5ff7ca923852bb269e9424b7ba23233b1a5fcdcce3b345b1f2e90051638c4f4",
		"find_node_degree_bridge.go:captureBridgeDegreeProjection":                 "e71ba3a62ab7cf918bab392544da7c77adb51c1491e62e340cea51fa6456c9cb",
		"find_node_degree_bridge.go:ownedDegreeProjectionFrontier":                 "79bea910912ac92bf3a85785614dbd6e25e17163fa6c697d656772c9c1070a9f",
		"peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked":   "d493312adea10a465b75de9814523ae0070f2d7f25cc6590248b05877331294c",
		"peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked":       "c6b3834fda6998ab67d9e61917e3e867d59133cba52e616965ae61e285d80d5a",
	}
}
func ownedNodeObservationSourceErrors(sources map[string][]byte) []string {
	if !qualifiedD1MembershipSource(sources["find_node_membership_checkpoint.go"]) {
		return []string{"membership exact tag/body changed"}
	}
	if !qualifiedD1DiagnosticSource(sources["find_node_resource_diagnostic.go"]) {
		return []string{"diagnostic exact tag/body changed"}
	}
	var failures []string
	expected := map[string]map[string]int{
		"newFindNodeOwnedCaptureCandidateService": {},
		"prepareFindNodeOwnedCaptureConstruction": {"find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeBridgeConstruction": 1, "find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction": 1},
		"captureOwnedFindNodeObservation":         {"find_node_bridge_capture.go:captureOwnedFindNodeCut": 1, "find_node_owned_receive.go:dispatchOwnedFindNodeReceive": 1},
		"Seal":                                    {"find_node_bridge_capture.go:sealOwnedFindNodeObservation": 1, "dm_control_send.go:sendOne": 1},
		"NewDormantIntake":                        {"find_node_bridge_construction.go:prepareFindNodeBridgeConstruction": 1},
		"NewDormantFixtureIntake":                 {},
		"newFindNodeBridgeCandidateService":       {},
		"prepareFindNodeBridgeConstruction":       {"find_node_bridge_construction.go:newFindNodeBridgeCandidateService": 1},
		"captureOwnedFindNodeCut":                 {"find_node_owned_round_loop.go:driveOwnedFindNodeRound": 1},
		"findNodeBridgeProfile":                   {"find_node_owned_local_target.go:newFindNodeOwnedLocalTargetCandidateService": 1, "find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction": 1, "find_node_owned_round_loop.go:newFindNodeOwnedRoundCandidateService": 1, "find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction": 1, "find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeBridgeCandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeBridgeConstruction": 1},
		"captureBridgeDegreeProjection":           {"find_node_bridge_capture.go:captureOwnedFindNodeObservation": 1},
		"ownedDegreeProjectionFrontier":           {"find_node_degree_bridge.go:captureBridgeDegreeProjection": 1},
		"projectOwnedDegreeBasisLocked":           {"peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked": 1, "find_node_degree_bridge.go:captureBridgeDegreeProjection": 1},
		"findNodeOwnedCaptureProfile":             {"find_node_bridge_construction.go:<package>": 1, "find_node_bridge_capture.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1},
		"findNodeBridgeCaptureContext":            {"find_node_owned_receive.go:<package>": 2, "find_node_bridge_capture.go:<package>": 2, "find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1, "service.go:<package>": 1, "service.go:newServiceWithConstruction": 1},
		"findNodeOwnedObservation":                {"find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget": 1, "find_node_bridge_capture.go:sealOwnedFindNodeObservation": 1, "find_node_bridge_capture.go:<package>": 1, "find_node_bridge_capture.go:captureOwnedFindNodeObservation": 10},
		"bridgeDegreeProjection":                  {"find_node_degree_bridge.go:<package>": 1, "find_node_bridge_capture.go:<package>": 1, "find_node_degree_bridge.go:captureBridgeDegreeProjection": 8},
	}
	functions := map[string]bool{"Seal": true, "NewDormantIntake": true, "NewDormantFixtureIntake": true, "newFindNodeBridgeCandidateService": true, "prepareFindNodeBridgeConstruction": true, "captureOwnedFindNodeCut": true, "newFindNodeOwnedCaptureCandidateService": true, "prepareFindNodeOwnedCaptureConstruction": true, "captureOwnedFindNodeObservation": true, "captureBridgeDegreeProjection": true, "ownedDegreeProjectionFrontier": true, "projectOwnedDegreeBasisLocked": true}
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["findNodeBridgeCaptureContext"]["find_node_ready_construction.go:<package>"] = 1
	expected["prepareFindNodeOwnedCaptureConstruction"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_construction.go:newFindNodeReadyCandidateService"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_construction.go:newFindNodeReadyLoadedInboundCandidateService"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_construction.go:newFindNodeReadyLoadedContactReconciliationCandidateService"] = 1
	expected["findNodeBridgeProfile"]["identity_pin_own_dm_runtime.go:NewInactiveOwnDMRuntimeCandidate"] = 1
	expected["findNodeOwnedCaptureProfile"]["identity_pin_own_dm_runtime.go:NewInactiveOwnDMRuntimeCandidate"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_target_timing.go:newFindNodeReadyTimedLoadedInboundCandidateService"] = 1
	expected["findNodeBridgeProfile"]["find_node_ready_target_timing.go:prepareFindNodeReadyTimedTestConstruction"] = 1
	expected["findNodeBridgeProfile"]["find_node_resource_diagnostic.go:NewD1FindNodeResourceDiagnostic"] = 1
	expected["findNodeOwnedCaptureProfile"]["find_node_resource_diagnostic.go:NewD1FindNodeResourceDiagnostic"] = 1
	expected["findNodeBridgeCaptureContext"]["find_node_resource_diagnostic.go:<package>"] = 2
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	wantPins := ownedNodeObservationPins()
	gotPins := map[string]string{}
	shapes := map[string]string{
		"findNodeBridgeProfile":        "findNodeBridgeProfilestruct{CapturefindNodeOwnedCaptureProfileIntakefindnode.DormantIntakeProfile}",
		"findNodeOwnedCaptureProfile":  "findNodeOwnedCaptureProfilestruct{DegreedegreeProfilePriorityfindNodePriorityProfile}",
		"findNodeBridgeCaptureContext": "findNodeBridgeCaptureContextstruct{terminal*findNodeBridgeTerminalobserverfindnodebridge.Observerconsumerfindnodebridge.ConsumerpriorityfindNodePriorityProfileintakefindnode.DormantIntakesinkfindnode.ObservationSinkroundLoop*ownedFindNodeRoundLooplocalTargetfindnode.OwnedLocalTargetlocalLoop*ownedFindNodeLocalLoop}",
		"findNodeOwnedObservation":     "findNodeOwnedObservationstruct{owner*findNodeBridgeCaptureContextstampfindnodebridge.CaptureStampfactsownedFindNodeRawFactsdegreebridgeDegreeProjection}",
		"bridgeDegreeProjection":       "bridgeDegreeProjectionstruct{atfindnodebridge.OwnedInstantoutgoing,incoming,windowintdegreeValidBeforefindnodebridge.OwnedBoundary}",
	}
	gotShapes := map[string]string{}
	minted := map[string]int{}
	fieldRefs := map[string]int{}
	pairRefs := map[string]int{}
	for path, raw := range sources {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			return []string{"Node observation production parse"}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if _, watched := wantPins[site]; watched && fn.Body != nil {
					start, end := fset.Position(fn.Body.Pos()).Offset, fset.Position(fn.Body.End()).Offset
					sum := sha256.Sum256(raw[start:end])
					if _, duplicate := gotPins[site]; duplicate {
						failures = append(failures, "duplicate Node observation body")
					}
					gotPins[site] = hex.EncodeToString(sum[:])
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					callees[call.Fun] = true
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok {
					if sites, watched := actual[id.Name]; watched && id != declaration {
						sites[site]++
						if functions[id.Name] && !callees[id] {
							failures = append(failures, "Node observation function value escape")
						}
					}
				}
				if typ, ok := node.(*ast.TypeSpec); ok {
					if _, watched := shapes[typ.Name.Name]; watched {
						start, end := fset.Position(typ.Pos()).Offset, fset.Position(typ.End()).Offset
						if _, duplicate := gotShapes[typ.Name.Name]; duplicate {
							failures = append(failures, "duplicate Node observation type")
						}
						gotShapes[typ.Name.Name] = strings.Join(strings.Fields(string(raw[start:end])), "")
					}
				}
				if sel, ok := node.(*ast.SelectorExpr); ok && (sel.Sel.Name == "intake" || sel.Sel.Name == "sink") {
					pairRefs[site+":"+sel.Sel.Name]++
				}
				if sel, ok := node.(*ast.SelectorExpr); ok && sel.Sel.Name == "bridgeCapture" {
					fieldRefs[site]++
				}
				if call, ok := node.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "findnodebridge" && sel.Sel.Name == "NewRuntimeDomain" {
							minted[site]++
						}
					}
				}
				if assignment, ok := node.(*ast.AssignStmt); ok {
					for _, lhs := range assignment.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "bridgeCapture" {
							failures = append(failures, "Node capture late Service setter")
						}
					}
				}
				if _, watched := wantPins[site]; watched && site != "service.go:newServiceWithConstruction" {
					switch node.(type) {
					case *ast.GoStmt, *ast.SendStmt:
						failures = append(failures, "Node observation acquired worker")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "Node observation exact caller/type inventory changed")
	}
	if !reflect.DeepEqual(gotPins, wantPins) {
		failures = append(failures, "Node observation raw body changed")
	}
	if !reflect.DeepEqual(gotShapes, shapes) {
		failures = append(failures, "Node observation private shape changed")
	}
	if !reflect.DeepEqual(pairRefs, map[string]int{"find_node_membership_checkpoint.go:CheckpointForService:intake": 1, "find_node_resource_diagnostic.go:boundTo:intake": 1, "find_node_resource_diagnostic.go:SnapshotForService:intake": 2, "find_node_ready_construction.go:prepareFindNodeReadyConstruction:intake": 1, "find_node_ready_construction.go:prepareFindNodeReadyConstruction:sink": 1, "find_node_ready_construction.go:bindings:intake": 2, "find_node_ready_construction.go:bindings:sink": 1, "find_node_resources.go:ownedFindNodeResourceView:intake": 1, "find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction:intake": 1, "find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget:sink": 1, "find_node_bridge_construction.go:prepareFindNodeBridgeConstruction:intake": 1, "find_node_bridge_construction.go:prepareFindNodeBridgeConstruction:sink": 1, "find_node_bridge_construction.go:claim:intake": 1, "find_node_bridge_construction.go:claim:sink": 1, "find_node_bridge_capture.go:sealOwnedFindNodeObservation:sink": 2, "find_node_owned_receive.go:recordOwnedFindNodeLine:intake": 1, "find_node_owned_receive.go:recordOwnedFindNodeLine:sink": 1, "find_node_owned_receive.go:dispatchOwnedFindNodeReceive:intake": 1, "find_node_owned_receive.go:dispatchOwnedFindNodeReceive:sink": 1, "peer_management.go:readPeerSession:intake": 1, "find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction:intake": 1, "find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction:sink": 1, "find_node_owned_round_loop.go:runOwnedFindNodeRoundLoop:intake": 2, "find_node_owned_round_loop.go:driveOwnedFindNodeRound:intake": 1, "find_node_owned_answer.go:dispatchOwnedFindNodeAnswer:intake": 1, "find_node_owned_answer.go:dispatchOwnedFindNodeAnswer:sink": 1}) {
		failures = append(failures, "Node pair escaped exact original constructor/read sites")
	}
	if !reflect.DeepEqual(fieldRefs, map[string]int{"find_node_resource_diagnostic.go:NewD1FindNodeResourceDiagnostic": 2, "find_node_resource_diagnostic.go:boundTo": 1, "find_node_owned_local_target.go:completeOwnedFindNodeLocalSignature": 1, "find_node_ready_construction.go:bindings": 1, "find_node_resources.go:ownedFindNodeResourceView": 1, "find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget": 1, "find_node_owned_answer.go:dispatchOwnedFindNodeAnswer": 2, "find_node_bridge_capture.go:captureOwnedFindNodeObservation": 2, "find_node_degree_bridge.go:captureBridgeDegreeProjection": 2, "find_node_owned_receive.go:recordOwnedFindNodeReceive": 2, "find_node_owned_receive.go:takeOwnedFindNodeReceive": 2, "find_node_owned_receive.go:recordOwnedFindNodeLine": 3, "peer_management.go:readPeerSession": 2, "find_node_owned_round_loop.go:signalOwnedFindNodeRound": 3, "find_node_owned_round_loop.go:runOwnedFindNodeRoundLoop": 2, "find_node_owned_round_loop.go:driveOwnedFindNodeRound": 2}) {
		failures = append(failures, "Node context field read/alias/write escaped exact sites")
	}
	if !reflect.DeepEqual(minted, map[string]int{"find_node_bridge_construction.go:prepareFindNodeTerminalConstruction": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1}) {
		failures = append(failures, "Node runtime domain reminted outside original construction roots")
	}
	return failures
}
func TestOwnedNodeObservationClosedSource(t *testing.T) {
	if failures := ownedNodeObservationSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

// In-memory SOURCE controls only, not physical FUNCTIONAL qualification.
func TestOwnedNodeObservationSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, file, before, after string }{
		{"broaden-session-class", "service.go", "sessionRecords := classes == recordAdmissionChargedR|recordAdmissionImmediateK && admission.kSource == contactRecordSessionSource", "sessionRecords := true"},
		{"skip-pair", "find_node_bridge_construction.go", "b.capture.intake, b.capture.sink = intake, sink", "_ = intake; _ = sink"},
		{"drop-tail-conversion", "find_node_bridge_capture.go", "range observation.facts.allOriginal {", "range observation.facts.allOriginal[:len(observation.facts.allOriginal)-1] {"},
		{"late-pair", "", "", ""},
		{"cut-method-value", "", "", ""},
		{"zero-observer", "find_node_bridge_construction.go", "d.Observer()", "findnodebridge.Observer{}"},
		{"foreign-consumer", "find_node_bridge_construction.go", "d.Consumer()", "findnodebridge.Consumer{}"},
		{"skip-phase", "find_node_bridge_capture.go", "c.observer.BeginCapture()", "findnodebridge.CaptureStamp{}, error(nil)"},
		{"skip-domain-match", "find_node_bridge_capture.go", "c.consumer.Matches", "(findnodebridge.Consumer{}).Matches"},
		{"wrong-poison-owner", "find_node_degree_bridge.go", "o.poisoned", "s.degree.poisoned"},
		{"skip-clock-release", "find_node_degree_bridge.go", "clock.Release()", "_ = clock"},
		{"frontier-empty", "find_node_degree_bridge.go", "at.BoundaryAfter(delta)", "at.BoundaryAfter(0)"},
		{"group-space-literal", "peer_state_degree_statistic.go", "key.group == \"\"", "key.group == \" \""},
		{"origin", "peer_state_owned_degree_statistic.go", "w.origin != row.life || ", ""},
		{"extra-caller", "", "", ""},
		{"late-context-field", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			switch tc.name {
			case "late-pair":
				sources["owned_pair_escape.go"] = []byte("package node\nfunc latePair(c *findNodeBridgeCaptureContext){c.sink=findnode.ObservationSink{}}\n")
			case "cut-method-value":
				sources["owned_cut_value_escape.go"] = []byte("package node\nfunc escapeCut(s *Service){_=s.captureOwnedFindNodeCut}\n")
			case "late-context-field":
				sources["owned_node_context_escape.go"] = []byte("package node\nfunc lateNodeContext(s *Service){s.bridgeCapture.priority.MaxMembers=1}\n")
			case "extra-caller":
				sources["owned_node_escape.go"] = []byte("package node\nfunc nodeObservationEscape(s *Service){_,_=s.captureOwnedFindNodeObservation(nil)}\n")
			default:
				raw := string(sources[tc.file])
				switch tc.name {
				case "wrong-poison-owner":
					if strings.Count(raw, tc.before) != 2 {
						t.Fatal("SOURCE exact poison roles")
					}
					at := strings.LastIndex(raw, tc.before)
					sources[tc.file] = []byte(raw[:at] + tc.after + raw[at+len(tc.before):])
				case "skip-clock-release":
					if strings.Count(raw, tc.before) != 2 {
						t.Fatal("SOURCE scoped release anchors")
					}
					sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
				default:
					if strings.Count(raw, tc.before) != 1 {
						t.Fatal("SOURCE unique mutation anchor", tc.name)
					}
					sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
				}
			}
			if failures := ownedNodeObservationSourceErrors(sources); len(failures) == 0 {
				t.Fatal("Node SOURCE control escaped closed inventory")
			}
		})
	}
}
