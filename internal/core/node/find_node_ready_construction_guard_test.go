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
	pins := map[string]string{"internal/core/node/find_node_owned_local_target.go": "62ef1272aca207e7b2f74e7f89e78469281c2abf43ba2cb9b7df05413dfe4004", "internal/core/node/find_node_ready_target_timing.go": "515455541e482579332a42535b4ed58ddab7633a8ef52a50e0c23138285d7667", "internal/core/node/identity_pin_own_dm_runtime.go": "39b178ddcdeb0ee8979a264484dd2f5eea37e3f9a869e9568a768a965a194abb", "internal/core/findnode/owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "internal/core/findnode/owned_bridge_bootstrap.go": "2ee7f6b13d859326bbd2fb00e487c9530737a2f722359d762202710d1a1cd827", "internal/core/node/find_node_bridge_construction.go": "c068e0e0d7e742b39c5ae6d70051734f813e22ce6db7b4ec42ccf4594ffc5680", "internal/core/node/find_node_ready_construction.go": "928f01bab660f066126117dee1b756869ad323b493fcdc83a0a53694db53f6c4", "internal/core/node/service.go": "283517bd09b09c11e77f799a285a90e97a0756bbc48b39c418657254cbcc73bf"}
	closed := map[string]bool{"ownedFindNodeSigningTestGate": true, "signingGate": true, "newFindNodeReadyTimedLoadedInboundCandidateService": true, "prepareFindNodeReadyTimedTestConstruction": true, "newFindNodeReadyLoadedContactReconciliationCandidateService": true, "captureContactReconciliationResolver": true, "captureContactReconciliationProducer": true, "contactReconciliation": true, "originalContactResolver": true, "originalContactAdmission": true, "originalContactProducer": true, "originalContactService": true, "originalContactWake": true, "findNodeReadyConstruction": true, "findNodeReadyOptions": true, "buildWithOptions": true, "installLoadedInbound": true, "newFindNodeReadyLoadedInboundCandidateService": true, "ownDMRuntimeConstruction": true, "prepareFindNodeReadyConstruction": true, "newFindNodeReadyCandidateService": true, "NewExecutingBootstrapIntake": true, "newDormantIntakeAt": true}
	tracked := map[string]bool{"newFindNodeReadyLoadedContactReconciliationCandidateService": true, "captureContactReconciliationResolver": true, "captureContactReconciliationProducer": true, "newFindNodeReadyTimedLoadedInboundCandidateService": true, "prepareFindNodeReadyTimedTestConstruction": true, "buildWithOptions": true, "installLoadedInbound": true, "newFindNodeReadyLoadedInboundCandidateService": true, "prepareFindNodeReadyConstruction": true, "newFindNodeReadyCandidateService": true, "NewExecutingBootstrapIntake": true, "newDormantIntakeAt": true}
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
}
