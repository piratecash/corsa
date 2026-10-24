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

// Finite exact Source closure, not compiler/heap/unsafe/runtime proof. Pin the
// complete actual worker, queue, writer and inverse implementations together;
// a new caller/type/role alias outside them requires a concrete reviewed edit.
func ownedFlowSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"internal/core/findnode/owned_bridge_bootstrap.go": "2ee7f6b13d859326bbd2fb00e487c9530737a2f722359d762202710d1a1cd827", "internal/core/node/find_node_ready_construction.go": "928f01bab660f066126117dee1b756869ad323b493fcdc83a0a53694db53f6c4", "internal/core/findnode/owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "internal/core/netcore/owned_resources.go": "59c2d0e4123a77aa5811210e4a6f91a25cafa4412e64eac5e94ea968a40858e2", "internal/core/node/find_node_resources.go": "14ecd1d16f6ff4a579136a3a9e72fd4651e6f7f191bccf79443a75e353a1aeab", "internal/core/netcore/owned_findnode_answer_write.go": "a8f11fa1f5398b8811ef41460e2849000a438389e39ab662af446a9c7c134d0c", "internal/core/node/find_node_owned_local_target.go": "62ef1272aca207e7b2f74e7f89e78469281c2abf43ba2cb9b7df05413dfe4004", "internal/core/findnode/owned_bridge_local_target.go": "dd2e3eea3ba16ffd60e057afb4c61a46d98a74cd872776e76882a6b0b4835e2d", "internal/core/findnode/owned_bridge_answer.go": "6f0a1c3c7129cfd838d2cd4599ee25e7c2e9eee451e28b1addf970cb5509952e", "internal/core/findnode/owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "internal/core/findnode/owned_bridge_dispatch.go": "3a6495784029c780e6cca5fa3d439a7d99fd34ddbc54fce5d97a75242d608732", "internal/core/findnode/owned_bridge_round_dispatch.go": "b9128ec6284560aea84441eab2b8619ba08297b66133139af4030a39d4478fcf", "internal/core/findnode/owned_bridge_round_execute.go": "dedd956801a9a14580f4e22634d3a5eb579ebe22520eb45eedce564209d7eec4", "internal/core/findnode/owned_bridge_scratch.go": "4f2a750803e6118b187eae648f22825452a3dedab9de1da1d9e5ee77d25876a6", "internal/core/netcore/net_core.go": "1521705a66677e6d633bff7778b2c91da9152438e266f07adb7fbfd09cf12a57", "internal/core/netcore/owned_findnode_write.go": "5ebeb88ec8e82db8034bf66927f91c9ea7b08505b4ef5e6b939cf80fe6899a96", "internal/core/node/find_node_bridge_capture.go": "920726a9421c2a8eacacd8ce8bbd665937c7f830deadb29eabc5ae0f6b5c6ba4", "internal/core/node/find_node_connection.go": "149ae30e487317e1d895786b704b151e22e2fa6062831ece31ead0788024a55a", "internal/core/node/find_node_owned_receive.go": "74305a5ace9ed0a3244ea03521ea293372fed406b32b04fabcd55a8bb1353a6f", "internal/core/node/find_node_owned_round_loop.go": "9ab9dd5159c5ec06dd6fb42a2f915fbfdf8a24482c343ea00fd840493a9b1157", "internal/core/node/find_node_owned_write.go": "a1f2a7e5c89b1fa1d44ba446e290821148e0f646d27d6164949516cbe08ae81c", "internal/core/node/proof_transport_lifetime.go": "5b12ec684cb095751c62c9b90e64593da266ed3a5e0ea50fc6801624c1939119"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned flow exact Source changed: "+path)
		}
	}
	unique := map[string]bool{"OwnedRoundBatch": true, "ownedRoundBatchState": true, "OwnedForwardWork": true, "OwnedQueuedForward": true, "OwnedWritePermit": true, "ownedForwardState": true, "ownedFindNodeRoundLoop": true, "ownedFindNodeSend": true, "finishObserver": true, "retirement": true, "roundLoop": true, "NewExecutingIntake": true, "NewExecutingFixtureIntake": true, "CloseOwnedRound": true, "OwnedRoundPending": true, "EndOriginalSession": true, "RouteTarget": true, "QueueForOriginal": true, "WaitDelay": true, "SendOwnedFindNode": true, "writeOwnedFindNode": true, "newOwnedFindNodeRoundLoop": true, "newFindNodeOwnedRoundCandidateService": true, "prepareFindNodeOwnedRoundConstruction": true, "signalOwnedFindNodeRound": true, "runOwnedFindNodeRoundLoop": true, "driveOwnedFindNodeRound": true, "queueOwnedFindNodeForward": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned flow production parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && unique[id.Name] && !pinned {
				failures = append(failures, "owned flow original type/API/role escaped: "+path+":"+id.Name)
			}
			return true
		})
	}
	return failures
}
func ownedFlowSources(t *testing.T) map[string][]byte {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	out := map[string][]byte{}
	for _, pkg := range []string{"node", "findnode", "netcore"} {
		paths, err := filepath.Glob(filepath.Join(root, "internal", "core", pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			out["internal/core/"+pkg+"/"+filepath.Base(path)] = raw
		}
	}
	return out
}
func TestOwnedFlowClosedSource(t *testing.T) {
	if failures := ownedFlowSourceErrors(ownedFlowSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedFlowSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"empty-refusal-wake", "internal/core/node/find_node_owned_receive.go", "s.signalOwnedFindNodeRound()", "_ = err"},
		{"discard-actual-batch", "internal/core/node/find_node_owned_round_loop.go", "_ = s.queueOwnedFindNodeForward(work)", "_ = work.Cancel()"},
		{"late-core-substitution", "internal/core/netcore/owned_findnode_write.go", "item.core != pc", "false"},
		{"receipt-substitution", "internal/core/findnode/owned_bridge_round_dispatch.go", "receipt.execution != execution", "false"},
		{"post-start-wait", "internal/core/netcore/owned_findnode_write.go", "written, err := pc.rawConn.Write(data)", "time.Sleep(time.Millisecond); written, err := pc.rawConn.Write(data)"},
		{"partial-nil-success", "internal/core/netcore/owned_findnode_write.go", "err == nil && written == len(data)", "err == nil"},
		{"no-Finish", "internal/core/netcore/owned_findnode_write.go", "finishErr := permit.Finish(writeOK)", "finishErr := error(nil)"},
		{"foreign-completion", "internal/core/findnode/owned_bridge_dispatch.go", "if !owner.consumer.Matches(w.cut.stamp, at) {\n\t\to.releaseLocked(entry)\n\t\treturn ErrUnauthenticated\n\t}", "if false {\n\t\to.releaseLocked(entry)\n\t\treturn ErrUnauthenticated\n\t}"},
		{"End-under-common", "internal/core/node/proof_transport_lifetime.go", "defer func() {\n\t\tl.mu.Unlock()\n\t\tif retiredIntake", "defer func() {\n\t\tif retiredIntake"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ownedFlowSources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique owned-flow Source anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedFlowSourceErrors(sources)) == 0 {
				t.Fatal("owned flow Source mutation escaped", tc.name)
			}
		})
	}
	sources := ownedFlowSources(t)
	sources["internal/core/node/owned_flow_escape.go"] = []byte("package node\nfunc escapeOwnedFlow(s *Service){s.signalOwnedFindNodeRound()}\n")
	if len(ownedFlowSourceErrors(sources)) == 0 {
		t.Fatal("owned flow OTHERFILE caller escaped")
	}
}
