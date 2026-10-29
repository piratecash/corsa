package node

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Finite Source closure, not a claim about arbitrary unsafe/dataflow or release.
func ownedReadySources(t *testing.T) map[string][]byte {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	sources := map[string][]byte{}
	err := filepath.WalkDir(filepath.Join(root, "internal", "core"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(rel)] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sources
}
func ownedReadySourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{"internal/core/node/find_node_owned_local_target.go": "3f72673fd435bb363ff434ef5fb728d61fc151e06f85b72a9e1a303599983915", "internal/core/node/find_node_ready_target_timing.go": "515455541e482579332a42535b4ed58ddab7633a8ef52a50e0c23138285d7667", "internal/core/node/identity_pin_own_dm_runtime.go": "39b178ddcdeb0ee8979a264484dd2f5eea37e3f9a869e9568a768a965a194abb", "internal/core/findnode/owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "internal/core/findnode/owned_bridge_bootstrap.go": "2ee7f6b13d859326bbd2fb00e487c9530737a2f722359d762202710d1a1cd827", "internal/core/node/find_node_bridge_construction.go": "09d857b53409deb6a9d2be2a2ba6a04c14d212ea0e1d7dec7415646a24e3878e", "internal/core/node/find_node_ready_construction.go": "928f01bab660f066126117dee1b756869ad323b493fcdc83a0a53694db53f6c4", "internal/core/node/service.go": "140e304fe12ffdaa532521f80059803ec30bcd54355afb253235504f6760320e"}
	pins["internal/core/node/s6_handshake_construction.go"] = "a9a411fedcafa9fc55636d028c56eec33f7ce10946b5661d25454881aeb80cd8"
	pins["internal/core/node/s6_terminal_construction.go"] = "dee11d0d4d7fde8f976740c844b4bf271992c97168de795179b295edf504b201"
	pins["internal/core/node/handshake_node_full_observation.go"] = "4278f2e98fcf9016c2db2602eab3daf618b1def9a6e888943c0721bb3d85c258"
	pins["internal/core/node/k5_handshake_segment.go"] = "100a5cd2ccf50815ddaf16567d4f5e8d39165e68289167cb6ecae73cef6908b0"
	pins["internal/core/node/k5_handshake_file_codec.go"] = "f1295f803c66fb242fffd516cca89ea88551ecd5a256f01ee7424c208f20f11c"
	pins["internal/core/node/k5_handshake_file_issuer.go"] = "a993e5c03a0c46dcde82f6e5d49a333d0ffdd3cf0fb712885b966372b7d3dd92"
	pins["internal/core/node/s6_handshake_file_construction.go"] = "8e0f7da19523c9aa3c663d0d95414ad937a10aece64363a688ed2e564607f092"
	pins["internal/core/node/inbound_handshakes.go"] = "bfcfa33aa56406500227ae3eb2fbf9f2324cdfe75ab49dc8fe7f8a8db8dac3a4"
	pins["internal/core/node/s6_connection_total_construction.go"] = "8318d3a63423f05f5764806cd494137c661f9d90ff668a61541ee403b7c9b711"
	pins["internal/core/connbudget/full_observation_export.go"] = "ea2881892339ed6296b9e56221d944876a80d83ab8cd05a0d61cd3a437eca160"
	pins["internal/core/connbudget/budget.go"] = "903dac9422edb14a2262d2f562e46ab5c35f0d76cee07b8549852aa1bd6057f3"
	pins["internal/core/connbudget/full_observation.go"] = "f011141c2c08dbaa9200f27586e4e0f16f432a8705d128a69d743a06a0d4c10e"
	pins["internal/core/connbudget/total_full_segment.go"] = "19dbe635361bba18fc5d9a77657e5c616c6c618a9cdf72a333afd29df561864f"
	pins["internal/core/connbudget/total_file_codec.go"] = "5561eb797d98217dff9c8c0ec9bcb37f5ac788df8b9825a9239a4122e702112f"
	pins["internal/core/connbudget/total_file_issuer.go"] = "dca5fceb484eeec2ae5043c94a9ec201aafa3bcbb036a83900aac9e5f548f7a7"
	pins["internal/core/connbudget/total_file_export.go"] = "c48b5447b1a77ef1e581c0e122892d4d30eb3e23fea4b547c07167c0acfa40ab"
	pins["internal/core/node/s6_connection_total_file_construction.go"] = "35e39f6a1a9560ccdb37184b127763521c7e932ff4ec813fd28ce06d7022ad6f"
	pins["internal/core/node/k5_handshake_past.go"] = "36ffbc5fce20f06ba9fcaacce2c93b49aaa94840732a278def000617b1267aba"
	pins["internal/core/connbudget/total_past.go"] = "22a97c21abe2bf31c7d29236266f6415acb9d689a3e53259ba99967d9a466a09"
	pins["internal/core/node/find_node_resource_diagnostic.go"] = "63112a5bba4b8e2832680b47ae904bdf50f64806f5c707628c3a6604f9dfe549"
	closed := map[string]bool{"ownedFindNodeSigningTestGate": true, "signingGate": true, "newFindNodeReadyTimedLoadedInboundCandidateService": true, "prepareFindNodeReadyTimedTestConstruction": true, "newFindNodeReadyLoadedContactReconciliationCandidateService": true, "captureContactReconciliationResolver": true, "captureContactReconciliationProducer": true, "contactReconciliation": true, "originalContactResolver": true, "originalContactAdmission": true, "originalContactProducer": true, "originalContactService": true, "originalContactWake": true, "findNodeReadyConstruction": true, "findNodeReadyOptions": true, "buildWithOptions": true, "installLoadedInbound": true, "newFindNodeReadyLoadedInboundCandidateService": true, "ownDMRuntimeConstruction": true, "prepareFindNodeReadyConstruction": true, "newFindNodeReadyCandidateService": true, "NewExecutingBootstrapIntake": true, "newDormantIntakeAt": true}
	tracked := map[string]bool{"newFindNodeReadyLoadedContactReconciliationCandidateService": true, "captureContactReconciliationResolver": true, "captureContactReconciliationProducer": true, "newFindNodeReadyTimedLoadedInboundCandidateService": true, "prepareFindNodeReadyTimedTestConstruction": true, "buildWithOptions": true, "installLoadedInbound": true, "newFindNodeReadyLoadedInboundCandidateService": true, "prepareFindNodeReadyConstruction": true, "newFindNodeReadyCandidateService": true, "NewExecutingBootstrapIntake": true, "newDormantIntakeAt": true}
	// The original gate factory now delegates through one pinned typed wrapper.
	// Preserve all Ready authority checks and close only these exact added roles.
	for _, name := range []string{"NewServiceWithHandshakeObservation", "prepareHandshakeObservation", "validHandshakeObservationKey", "newGate", "newObservedInboundHandshakes", "newInboundHandshakes"} {
		closed[name], tracked[name] = true, true
	}
	closed["handshakeObservationConstruction"], closed["handshakeObservation"] = true, true
	// Close only the unique typed budget factories; generic New is not tracked.
	for _, name := range []string{"NewServiceWithConnectionTotalObservation", "prepareConnectionTotalObservation", "validConnectionTotalObservationKey", "newBudget", "NewWithTotalObservation", "newObservedBudget"} {
		closed[name], tracked[name] = true, true
	}
	closed["connectionTotalObservationConstruction"], closed["connectionTotalObservation"] = true, true
	// The private V2 constructor is reached only by the pinned file factory.
	// Its ordinary owner factory and issuer authority stay inside the exact
	// frontier; tests are excluded by ownedReadySources. Do not track generic
	// New, row, finish, slot, hash or reset across Core.
	closed["newSegmentedInboundHandshakes"], tracked["newSegmentedInboundHandshakes"] = true, true
	// File mode is selected only at the one original Service gate factory.
	// These exact typed construction roles add no dynamic installer.
	for _, name := range []string{"NewServiceWithHandshakeFileObservation", "prepareHandshakeFileObservation", "newFileGate"} {
		closed[name], tracked[name] = true, true
	}
	closed["handshakeFileObservationConstruction"], closed["handshakeFileObservation"] = true, true
	closed["newFileObservedInboundHandshakes"], tracked["newFileObservedInboundHandshakes"] = true, true
	closed["handshakeFileIssuer"] = true
	for _, name := range []string{"handshakeSegmentState", "handshakeSegmentIssuer", "handshakeSegmentReceipt", "handshakeSegmentCommit", "handshakeSegmentWitness"} {
		closed[name] = true
	}
	// The private total V2 owner is reached only by the pinned file factory.
	// The file factory is reached only through the typed adapter; close its issuer
	// without tracking generic New, row, hash or slot.
	closed["newSegmentedBudget"], tracked["newSegmentedBudget"] = true, true
	closed["newFileSegmentedBudget"], tracked["newFileSegmentedBudget"] = true, true
	closed["totalFileIssuer"] = true
	for _, name := range []string{"NewWithTotalFileObservation", "NewServiceWithConnectionTotalFileObservation", "prepareConnectionTotalFileObservation", "newFileBudget"} {
		closed[name], tracked[name] = true, true
	}
	for _, name := range []string{"TotalFileObservation", "ConnectionTotalFileObservation", "connectionTotalFileObservationConstruction", "connectionTotalFileObservation"} {
		closed[name] = true
	}
	for _, name := range []string{"totalSegmentState", "totalSegmentIssuer", "totalSegmentReceipt", "totalSegmentCommit", "totalSegmentWitness", "totalSegmentDescriptor", "totalSegmentCarry", "totalSegmentStream", "totalSegmentLoan", "totalSegmentChunk", "totalSegmentRange", "totalSegmentFold"} {
		closed[name] = true
	}
	// Explicit read-only past reader: no default/non-test caller, live owner
	// installer or new generic Next/Close traversal. Its receiver is closed.
	closed["OpenHandshakePast"], tracked["OpenHandshakePast"] = true, true
	closed["HandshakePastReader"] = true
	closed["OpenTotalPast"], tracked["OpenTotalPast"] = true, true
	closed["TotalPastReader"] = true
	calls := map[string]int{}
	declarations := map[string]string{}
	var failures []string
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "Ready exact original Source changed: "+path)
		}
	}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "Ready Source parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var definition *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				definition = fn.Name
				if tracked[fn.Name.Name] {
					if declarations[fn.Name.Name] != "" {
						failures = append(failures, "Ready duplicate declaration")
					}
					declarations[fn.Name.Name] = site
				}
			}
			direct := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					switch f := call.Fun.(type) {
					case *ast.Ident:
						direct[f] = true
					case *ast.SelectorExpr:
						direct[f.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != definition && closed[id.Name] {
					if _, ok := pins[path]; !ok {
						failures = append(failures, "Ready authority outside exact frontier: "+site)
					}
					if tracked[id.Name] {
						calls[id.Name+":"+site]++
						if !direct[id] {
							failures = append(failures, "Ready factory method-value escape")
						}
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SealConstruction" {
						calls["SealConstruction:"+site]++
					}
				}
				return true
			})
		}
	}
	wantCalls := map[string]int{
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_resource_diagnostic.go:NewD1FindNodeResourceDiagnostic":                            1,
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedContactReconciliationCandidateService": 1,
		"buildWithOptions:internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedContactReconciliationCandidateService":                 1,
		"captureContactReconciliationResolver:internal/core/node/service.go:newServiceWithConstruction":                                                   1,
		"captureContactReconciliationProducer:internal/core/node/service.go:newServiceWithConstruction":                                                   1,
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_ready_target_timing.go:newFindNodeReadyTimedLoadedInboundCandidateService":         1,
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_ready_target_timing.go:prepareFindNodeReadyTimedTestConstruction":                  1,
		"buildWithOptions:internal/core/node/find_node_ready_target_timing.go:newFindNodeReadyTimedLoadedInboundCandidateService":                         1,
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedInboundCandidateService":               1,
		"buildWithOptions:internal/core/node/find_node_ready_construction.go:buildWithOwnDM":                                                              1,
		"buildWithOptions:internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedInboundCandidateService":                               1,
		"installLoadedInbound:internal/core/node/service.go:newServiceWithConstruction":                                                                   1,
		"prepareFindNodeReadyConstruction:internal/core/node/identity_pin_own_dm_runtime.go:NewInactiveOwnDMRuntimeCandidate":                             1,
		"prepareFindNodeReadyConstruction:internal/core/node/find_node_ready_construction.go:newFindNodeReadyCandidateService":                            1,
		"NewExecutingBootstrapIntake:internal/core/node/find_node_ready_construction.go:prepareFindNodeReadyConstruction":                                 1,
		"newDormantIntakeAt:internal/core/findnode/owned_bridge.go:newDormantIntake":                                                                      1,
		"newDormantIntakeAt:internal/core/findnode/owned_bridge_bootstrap.go:NewExecutingBootstrapIntake":                                                 1,
		"SealConstruction:internal/core/node/find_node_ready_construction.go:finalize":                                                                    1,
	}
	wantDeclarations := map[string]string{
		"newFindNodeReadyLoadedContactReconciliationCandidateService": "internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedContactReconciliationCandidateService",
		"captureContactReconciliationResolver":                        "internal/core/node/find_node_ready_construction.go:captureContactReconciliationResolver",
		"captureContactReconciliationProducer":                        "internal/core/node/find_node_ready_construction.go:captureContactReconciliationProducer",
		"newFindNodeReadyTimedLoadedInboundCandidateService":          "internal/core/node/find_node_ready_target_timing.go:newFindNodeReadyTimedLoadedInboundCandidateService",
		"prepareFindNodeReadyTimedTestConstruction":                   "internal/core/node/find_node_ready_target_timing.go:prepareFindNodeReadyTimedTestConstruction",
		"buildWithOptions":                              "internal/core/node/find_node_ready_construction.go:buildWithOptions",
		"installLoadedInbound":                          "internal/core/node/find_node_ready_construction.go:installLoadedInbound",
		"newFindNodeReadyLoadedInboundCandidateService": "internal/core/node/find_node_ready_construction.go:newFindNodeReadyLoadedInboundCandidateService",
		"prepareFindNodeReadyConstruction":              "internal/core/node/find_node_ready_construction.go:prepareFindNodeReadyConstruction",
		"newFindNodeReadyCandidateService":              "internal/core/node/find_node_ready_construction.go:newFindNodeReadyCandidateService",
		"NewExecutingBootstrapIntake":                   "internal/core/findnode/owned_bridge_bootstrap.go:NewExecutingBootstrapIntake",
		"newDormantIntakeAt":                            "internal/core/findnode/owned_bridge.go:newDormantIntakeAt",
	}
	wantCalls["newGate:internal/core/node/service.go:newServiceWithConstruction"] = 1
	wantCalls["prepareHandshakeObservation:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 1
	wantCalls["validHandshakeObservationKey:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 2
	wantCalls["newObservedInboundHandshakes:internal/core/node/s6_handshake_construction.go:newGate"] = 1
	wantCalls["newInboundHandshakes:internal/core/node/s6_handshake_construction.go:newGate"] = 1
	wantCalls["newInboundHandshakes:internal/core/node/handshake_node_full_observation.go:newObservedInboundHandshakes"] = 1
	wantDeclarations["NewServiceWithHandshakeObservation"] = "internal/core/node/s6_handshake_construction.go:NewServiceWithHandshakeObservation"
	wantDeclarations["prepareHandshakeObservation"] = "internal/core/node/s6_handshake_construction.go:prepareHandshakeObservation"
	wantDeclarations["validHandshakeObservationKey"] = "internal/core/node/s6_handshake_construction.go:validHandshakeObservationKey"
	wantDeclarations["newGate"] = "internal/core/node/s6_handshake_construction.go:newGate"
	wantDeclarations["newObservedInboundHandshakes"] = "internal/core/node/handshake_node_full_observation.go:newObservedInboundHandshakes"
	wantDeclarations["newInboundHandshakes"] = "internal/core/node/inbound_handshakes.go:newInboundHandshakes"
	wantCalls["newInboundHandshakes:internal/core/node/k5_handshake_segment.go:newSegmentedInboundHandshakes"] = 1
	wantDeclarations["newSegmentedInboundHandshakes"] = "internal/core/node/k5_handshake_segment.go:newSegmentedInboundHandshakes"
	wantCalls["newSegmentedInboundHandshakes:internal/core/node/k5_handshake_file_issuer.go:newFileObservedInboundHandshakes"] = 1
	wantDeclarations["newFileObservedInboundHandshakes"] = "internal/core/node/k5_handshake_file_issuer.go:newFileObservedInboundHandshakes"
	wantCalls["newFileObservedInboundHandshakes:internal/core/node/s6_handshake_file_construction.go:newFileGate"] = 1
	wantCalls["prepareHandshakeFileObservation:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 1
	wantCalls["newFileGate:internal/core/node/service.go:newServiceWithConstruction"] = 1
	wantDeclarations["NewServiceWithHandshakeFileObservation"] = "internal/core/node/s6_handshake_file_construction.go:NewServiceWithHandshakeFileObservation"
	wantDeclarations["prepareHandshakeFileObservation"] = "internal/core/node/s6_handshake_file_construction.go:prepareHandshakeFileObservation"
	wantDeclarations["newFileGate"] = "internal/core/node/s6_handshake_file_construction.go:newFileGate"
	wantCalls["newBudget:internal/core/node/service.go:newServiceWithConstruction"] = 1
	wantCalls["prepareConnectionTotalObservation:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 1
	wantCalls["validConnectionTotalObservationKey:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 2
	wantCalls["NewWithTotalObservation:internal/core/node/s6_connection_total_construction.go:newBudget"] = 1
	wantCalls["newObservedBudget:internal/core/connbudget/full_observation_export.go:NewWithTotalObservation"] = 1
	wantDeclarations["NewServiceWithConnectionTotalObservation"] = "internal/core/node/s6_connection_total_construction.go:NewServiceWithConnectionTotalObservation"
	wantDeclarations["prepareConnectionTotalObservation"] = "internal/core/node/s6_connection_total_construction.go:prepareConnectionTotalObservation"
	wantDeclarations["validConnectionTotalObservationKey"] = "internal/core/node/s6_connection_total_construction.go:validConnectionTotalObservationKey"
	wantDeclarations["newBudget"] = "internal/core/node/s6_connection_total_construction.go:newBudget"
	wantDeclarations["NewWithTotalObservation"] = "internal/core/connbudget/full_observation_export.go:NewWithTotalObservation"
	wantDeclarations["newObservedBudget"] = "internal/core/connbudget/full_observation.go:newObservedBudget"
	// One private binding call plus one exact typed acquisition chain.
	wantDeclarations["newSegmentedBudget"] = "internal/core/connbudget/total_full_segment.go:newSegmentedBudget"
	wantCalls["newSegmentedBudget:internal/core/connbudget/total_file_issuer.go:newFileSegmentedBudget"] = 1
	wantDeclarations["newFileSegmentedBudget"] = "internal/core/connbudget/total_file_issuer.go:newFileSegmentedBudget"
	wantCalls["newFileSegmentedBudget:internal/core/connbudget/total_file_export.go:NewWithTotalFileObservation"] = 1
	wantDeclarations["NewWithTotalFileObservation"] = "internal/core/connbudget/total_file_export.go:NewWithTotalFileObservation"
	wantCalls["NewWithTotalFileObservation:internal/core/node/s6_connection_total_file_construction.go:newFileBudget"] = 1
	wantDeclarations["NewServiceWithConnectionTotalFileObservation"] = "internal/core/node/s6_connection_total_file_construction.go:NewServiceWithConnectionTotalFileObservation"
	wantDeclarations["prepareConnectionTotalFileObservation"] = "internal/core/node/s6_connection_total_file_construction.go:prepareConnectionTotalFileObservation"
	wantDeclarations["newFileBudget"] = "internal/core/node/s6_connection_total_file_construction.go:newFileBudget"
	wantCalls["prepareConnectionTotalFileObservation:internal/core/node/s6_terminal_construction.go:newServiceWithObservations"] = 1
	wantCalls["newFileBudget:internal/core/node/service.go:newServiceWithConstruction"] = 1
	wantDeclarations["OpenHandshakePast"] = "internal/core/node/k5_handshake_past.go:OpenHandshakePast"
	wantDeclarations["OpenTotalPast"] = "internal/core/connbudget/total_past.go:OpenTotalPast"
	if !reflect.DeepEqual(calls, wantCalls) {
		failures = append(failures, "Ready exclusive constructor/Seal direct callers changed")
	}
	if !reflect.DeepEqual(declarations, wantDeclarations) {
		failures = append(failures, "Ready exact declarations changed")
	}
	return failures
}
func TestOwnedReadyClosedSource(t *testing.T) {
	if failures := ownedReadySourceErrors(ownedReadySources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedReadySourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"lost-contact-source", "internal/core/node/find_node_ready_construction.go", "admission.kSource != contactRecordReconciliationSource", "false"},
		{"lost-contact-wake", "internal/core/node/find_node_ready_construction.go", "producer.wake != r.originalContactWake", "false"},
		{"late-contact-capture", "internal/core/node/service.go", "construction.ready.captureContactReconciliationResolver(svc)", "_ = svc"},
		{"lost-gate-binding", "internal/core/node/find_node_ready_construction.go", "r.bridge.capture.localLoop.signingGate != r.originalOptions.signingGate", "false"},
		{"gate-lost-done", "internal/core/node/find_node_ready_target_timing.go", "case <-done:\n\t\treturn false", "case <-done:\n\t\treturn true"},
		{"timed-normal-implicit-gate", "internal/core/node/find_node_ready_target_timing.go", "findNodeReadyOptions{loadedInbound: true})", "findNodeReadyOptions{loadedInbound: true, signingGate: new(ownedFindNodeSigningTestGate)})"},
		{"lost-inbound-selection", "internal/core/node/find_node_ready_construction.go", "r.options != r.originalOptions || r.ownDMRuntime != r.originalOptions.ownDM", "false"},
		{"lost-inbound-wake", "internal/core/node/find_node_ready_construction.go", "p.wake != r.originalInboundWake", "false"},
		{"skip-loaded-attach", "internal/core/node/service.go", "construction.ready.failure = construction.ready.installLoadedInbound(svc)", "construction.ready.failure = nil"},
		{"mask-attach-error", "internal/core/node/find_node_ready_construction.go", "if r.failure != nil {\n\t\treturn nil, r.failure\n\t}", "if r.failure != nil { return nil, findnode.ErrNotReady }"},
		{"skip-Seal", "internal/core/node/find_node_ready_construction.go", "if err := b.terminal.publisher.SealConstruction(); err != nil {\n\t\treturn err\n\t}", "if false { return nil }"},
		{"lost-original-capture", "internal/core/node/find_node_ready_construction.go", "*c == r.originalCapture", "true"},
		{"missing-store-custody", "internal/core/node/service.go", "construction.ready.service, construction.ready.trust = svc, trust", "construction.ready.service = svc"},
		{"second-load", "internal/core/node/service.go", "peerState = construction.ready.peerState", "peerState, err = loadPeerState(peersStatePath)"},
		{"foreign-observer", "internal/core/findnode/owned_bridge_bootstrap.go", "!consumer.MatchesPoint(observed)", "false"},
		{"handshake-ordinary-nil-gate", "internal/core/node/s6_handshake_construction.go", "if c == nil {\n\t\treturn newInboundHandshakes(bounds)\n\t}", "if c == nil {\n\t\treturn nil\n\t}"},
		{"connection-total-ordinary-nil-budget", "internal/core/node/s6_connection_total_construction.go", "if c == nil {\n\t\treturn connbudget.New(cfg)\n\t}", "if c == nil {\n\t\treturn nil, nil\n\t}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ownedReadySources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique Ready Source control anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedReadySourceErrors(sources)) == 0 {
				t.Fatal("Ready Source control escaped", tc.name)
			}
		})
	}
	sources := ownedReadySources(t)
	sources["internal/core/node/ready_escape.go"] = []byte("package node\nvar escapedReady *findNodeReadyConstruction\n")
	if len(ownedReadySourceErrors(sources)) == 0 {
		t.Fatal("OTHERFILE Ready authority escaped")
	}

	for _, tc := range []struct{ name, source string }{
		{"handshake-method-value", "package node\nvar escapedHandshake = (*handshakeObservationConstruction).newGate\n"},
		{"connection-total-method-value", "package node\nvar escapedTotal = (*connectionTotalObservationConstruction).newBudget\n"},
		{"segment-constructor-alias", "package node\nvar escapedSegment = newSegmentedInboundHandshakes\n"},
		{"file-issuer-constructor-alias", "package node\nvar escapedFileIssuer = newFileObservedInboundHandshakes\n"},
		{"file-issuer-bound-authority", "package node\nvar escapedFileIssuerState *handshakeFileIssuer\n"},
		{"segment-issuer-implementation", "package node\ntype escapedSegmentIssuer struct{}\nfunc (*escapedSegmentIssuer) verifyHandshakeSegment(_ handshakeSegmentReceipt) (handshakeSegmentCommit, bool) { return handshakeSegmentCommit{}, true }\n"},
	} {
		sources := ownedReadySources(t)
		sources["internal/core/node/handshake_escape.go"] = []byte(tc.source)
		if len(ownedReadySourceErrors(sources)) == 0 {
			t.Fatal("OTHERFILE handshake factory/alias escaped", tc.name)
		}
	}

	// Finite plain controls preserve the existing named RUN inventory. Whole
	// pins protect exact selection/lifetime bodies; unique call maps close
	// foreign installers without globally tracking generic Close/Capture.
	for _, tc := range []struct{ name, path, before, after string }{
		{"file-lost-mutual-exclusion", "internal/core/node/s6_terminal_construction.go", "if options.Handshake != nil && options.HandshakeFile != nil {", "if false {"},
		{"file-lost-actual-gate", "internal/core/node/s6_handshake_file_construction.go", "c.readout.service, c.readout.gate, c.readout.issuer = s, gate, issuer", "c.readout.service, c.readout.gate, c.readout.issuer = s, nil, issuer"},
		{"file-lost-run-close", "internal/core/node/service.go", "defer func() { _ = s.CloseHandshakeFileObservation() }()", "defer func() {}()"},
	} {
		sources := ownedReadySources(t)
		raw := string(sources[tc.path])
		if strings.Count(raw, tc.before) != 1 {
			t.Fatal("unique file construction control anchor", tc.name)
		}
		sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
		if len(ownedReadySourceErrors(sources)) == 0 {
			t.Fatal("file construction control escaped", tc.name)
		}
	}
	for _, source := range []string{
		"package node\nvar escapedFileGate = (*handshakeFileObservationConstruction).newFileGate\n",
		"package node\nvar escapedFilePreparation = prepareHandshakeFileObservation\n",
	} {
		sources := ownedReadySources(t)
		sources["internal/core/node/file_construction_escape.go"] = []byte(source)
		if len(ownedReadySourceErrors(sources)) == 0 {
			t.Fatal("OTHERFILE file construction method-value escaped")
		}
	}

	// OTHERFILE controls leave every pinned body byte-exact. Specific checker
	// failures prove constructor/value/type closure, not only a SHA mismatch.
	// Plain assertions preserve the original 19 RUN family.
	for _, tc := range []struct{ name, source, want string }{
		{"total-segment-production-caller", "package connbudget\nfunc totalEscape() { newSegmentedBudget() }\n", "Ready exclusive constructor/Seal direct callers changed"},
		{"total-segment-constructor-value", "package connbudget\nvar escapedTotalConstructor = newSegmentedBudget\n", "Ready factory method-value escape"},
		{"total-segment-issuer-method-expression", "package connbudget\nvar escapedTotalVerifier = totalSegmentIssuer.verifyTotalSegment\n", "Ready authority outside exact frontier"},
		{"total-segment-state-type", "package connbudget\nvar escapedTotalState *totalSegmentState\n", "Ready authority outside exact frontier"},
		{"total-segment-receipt-type", "package connbudget\nvar escapedTotalReceipt totalSegmentReceipt\n", "Ready authority outside exact frontier"},
		{"total-segment-duplicate-constructor", "package connbudget\nfunc newSegmentedBudget() {}\n", "Ready duplicate declaration"},
		{"total-file-production-caller", "package connbudget\nfunc totalFileEscape() { newFileSegmentedBudget() }\n", "Ready exclusive constructor/Seal direct callers changed"},
		{"total-file-constructor-value", "package connbudget\nvar escapedFileTotalConstructor = newFileSegmentedBudget\n", "Ready factory method-value escape"},
		{"total-file-issuer-type", "package connbudget\nvar escapedFileTotalIssuer *totalFileIssuer\n", "Ready authority outside exact frontier"},
		{"total-file-duplicate-constructor", "package connbudget\nfunc newFileSegmentedBudget() {}\n", "Ready duplicate declaration"},
		{"total-file-export-caller", "package connbudget\nfunc arbitraryFile() { NewWithTotalFileObservation() }\n", "Ready exclusive constructor/Seal direct callers changed"},
		{"total-file-export-value", "package connbudget\nvar escapedTotalFile = NewWithTotalFileObservation\n", "Ready factory method-value escape"},
		{"total-file-public-handle-escape", "package connbudget\nvar forgedTotalFile *TotalFileObservation\n", "Ready authority outside exact frontier"},
		{"total-file-Node-construction-escape", "package node\nvar forgedTotalFileConstruction *connectionTotalFileObservationConstruction\n", "Ready authority outside exact frontier"},
		{"past-reader-production-caller", "package node\nfunc pastEscape(){OpenHandshakePast()}\n", "Ready exclusive constructor/Seal direct callers changed"},
		{"past-reader-constructor-value", "package node\nvar pastEscape=OpenHandshakePast\n", "Ready factory method-value escape"},
		{"past-reader-receiver-type", "package node\nvar pastEscape *HandshakePastReader\n", "Ready authority outside exact frontier"},
		{"past-reader-duplicate-constructor", "package node\nfunc OpenHandshakePast(){}\n", "Ready duplicate declaration"},
		{"total-past-reader-production-caller", "package connbudget\nfunc totalPastEscape(){OpenTotalPast()}\n", "Ready exclusive constructor/Seal direct callers changed"},
		{"total-past-reader-constructor-value", "package connbudget\nvar totalPastEscape=OpenTotalPast\n", "Ready factory method-value escape"},
		{"total-past-reader-receiver-type", "package connbudget\nvar totalPastEscape *TotalPastReader\n", "Ready authority outside exact frontier"},
		{"total-past-reader-duplicate-constructor", "package connbudget\nfunc OpenTotalPast(){}\n", "Ready duplicate declaration"},
	} {
		sources := ownedReadySources(t)
		if failures := ownedReadySourceErrors(sources); len(failures) != 0 {
			t.Fatal("total segment control baseline", tc.name, failures)
		}
		sources["internal/core/connbudget/total_segment_escape.go"] = []byte(tc.source)
		failures := ownedReadySourceErrors(sources)
		if !strings.Contains(strings.Join(failures, "\n"), tc.want) {
			t.Fatal("OTHERFILE total segment authority escaped", tc.name, tc.want, failures)
		}
	}
}
