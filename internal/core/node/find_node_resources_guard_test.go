package node

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Finite exact Source/custody closure, not a heap/GC proof. Named structural
// estimates stay separate from memory floors. No normalization or authority.
func ownedResourcesSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"internal/core/domain/resource_breakdown.go": "d4c6e1c5db6874739bf45c672f26c2b6850d18e58b1e2733c026241a05eceee9", "internal/core/findnode/owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "internal/core/findnode/owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "internal/core/netcore/net_core.go": "1521705a66677e6d633bff7778b2c91da9152438e266f07adb7fbfd09cf12a57", "internal/core/netcore/owned_findnode_write.go": "5ebeb88ec8e82db8034bf66927f91c9ea7b08505b4ef5e6b939cf80fe6899a96", "internal/core/netcore/owned_resources.go": "59c2d0e4123a77aa5811210e4a6f91a25cafa4412e64eac5e94ea968a40858e2", "internal/core/node/find_node_owned_round_loop.go": "9ab9dd5159c5ec06dd6fb42a2f915fbfdf8a24482c343ea00fd840493a9b1157", "internal/core/node/find_node_resources.go": "14ecd1d16f6ff4a579136a3a9e72fd4651e6f7f191bccf79443a75e353a1aeab", "internal/core/node/resource_breakdown.go": "295554e5118900ac3963ae70c08d690566315f37bc8d79fd90517fd32928c78a", "internal/core/findnode/intake_owner.go": "d46df9daabe08dd79f0fb8026c816f15e41694239b32eaed8522a678cb03dbbc", "internal/core/findnode/target_owner.go": "f6f6f51e69ace1888584e95a5d09f08cf13e8b6936cb68e6d022549d726b5511"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned resources exact Source changed: "+path)
		}
	}
	closed := map[string]bool{"HoldResources": true, "OwnedResourceCustody": true, "ownedWriterResourceCounters": true, "holdOwnedResourceCustody": true, "WriterResourceSnapshot": true, "ownedResourceSizes": true, "OwnedQueueItemBytes": true, "holdOwnedResources": true, "releaseOwnedResources": true, "claimOwnedConsumer": true, "discardOwnedItem": true, "retireUnclaimedOwnedItem": true, "resourcesHeld": true, "custodyClaim": true, "resourceCustody": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned resources production parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && closed[id.Name] && !pinned {
				failures = append(failures, "owned resources custody escaped: "+path+":"+id.Name)
			}
			if path == "internal/core/findnode/owned_resources.go" {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "Sample", "TryReadClock", "ReserveAcceptedClock", "ReserveClock", "BeginCapture", "Lock", "RLock", "Wait", "Read", "Write", "SignPayload":
						failures = append(failures, "resource view acquired authority/wait")
					}
				}
				switch n.(type) {
				case *ast.GoStmt, *ast.SendStmt:
					failures = append(failures, "resource view acquired worker")
				}
			}
			return true
		})
	}
	return failures
}
func ownedResourcesSources(t *testing.T) map[string][]byte {
	t.Helper()
	out := ownedAnswerSources(t)
	path := "internal/core/domain/resource_breakdown.go"
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	out[path] = raw
	return out
}
func TestOwnedResourcesClosedSource(t *testing.T) {
	if failures := ownedResourcesSourceErrors(ownedResourcesSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedResourcesSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{{"no-preoffer-hold", "internal/core/netcore/net_core.go", "item.owned.holdOwnedResources(pc)", "_ = pc"},
		{"nonoffer-leak", "internal/core/netcore/net_core.go", "if !offered {\n\t\t\t\titem.owned.retireUnclaimedOwnedItem()\n\t\t\t}", "if false {\n\t\t\t\titem.owned.retireUnclaimedOwnedItem()\n\t\t\t}"},
		{"postoffer-leak", "internal/core/netcore/net_core.go", "if status != SendOK && item.owned != nil", "if false && item.owned != nil"},
		{"consumer-without-claim", "internal/core/netcore/net_core.go", "if !item.owned.claimOwnedConsumer() {\n\t\t\treturn true\n\t\t}", "if false {\n\t\t\treturn true\n\t\t}"},
		{"claim-shared", "internal/core/netcore/owned_resources.go", "item.custodyClaim.CompareAndSwap(0, 1)", "true"},
		{"terminal-producer-steal", "internal/core/netcore/owned_resources.go", "!item.custodyClaim.CompareAndSwap(0, 2)", "false"},
		{"terminal-no-release", "internal/core/netcore/owned_resources.go", "item.cancelOwned()\n\titem.releaseOwnedResources()\n}\nfunc (item *ownedFindNodeSend) releaseOwnedResources()", "item.cancelOwned()\n}\nfunc (item *ownedFindNodeSend) releaseOwnedResources()"},
		{"immutable-core-header-clear", "internal/core/netcore/owned_resources.go", "item.queued = findnode.OwnedQueuedForward{}", "item.queued = findnode.OwnedQueuedForward{}\n\titem.core = nil"},
		{"wrong-owner-count", "internal/core/findnode/owned_resources.go", "owner.resources.snapshot()", "WriterResourceSnapshot{}"},
		{"graph-count-omitted", "internal/core/findnode/owned_resources.go", "ownedResourceSizes.round + ownedResourceSizes.cut", "ownedResourceSizes.cut"},
		{"batch-not-cleared", "internal/core/node/find_node_owned_round_loop.go", "defer loop.batch.Store(nil)", "defer func(){}()"},
		{"graph-estimate-as-floor", "internal/core/node/find_node_resources.go", "\"included_in_floor\": false", "\"included_in_floor\": true"},
		{"counter-authority", "internal/core/findnode/owned_resources.go", "s := owner.coordinator.snapshot()", "s := owner.coordinator.snapshot()\n\towner.coordinator.open = false"}} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ownedResourcesSources(t)
			text := string(sources[tc.path])
			if strings.Count(text, tc.before) != 1 {
				t.Fatal("exact resource control anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(text, tc.before, tc.after, 1))
			if len(ownedResourcesSourceErrors(sources)) == 0 {
				t.Fatal("resource Source control escaped", tc.name)
			}
		})
	}
	sources := ownedResourcesSources(t)
	sources["internal/core/netcore/owned_resource_escape.go"] = []byte("package netcore\nvar resourceEscape OwnedResourceCustody\n")
	if len(ownedResourcesSourceErrors(sources)) == 0 {
		t.Fatal("resource OTHERFILE custody escaped")
	}
	sources = ownedResourcesSources(t)
	sources["internal/core/findnode/owned_resource_acquisition_escape.go"] = []byte("package findnode\nfunc leak(q OwnedQueuedForward){h:=q.HoldResources();h.Release();h.Release()}\n")
	if len(ownedResourcesSourceErrors(sources)) == 0 {
		t.Fatal("resource OTHERFILE acquisition/double release escaped")
	}
}
