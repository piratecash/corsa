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

// Finite exact current Source/custody closure, not a compiler/heap/global alias
// proof. The local and relay private mint sites share one typed answer cell;
// any new caller/type/field route needs an explicit reviewed Source change.
func ownedAnswerSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"internal/core/findnode/owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "internal/core/netcore/owned_resources.go": "59c2d0e4123a77aa5811210e4a6f91a25cafa4412e64eac5e94ea968a40858e2", "internal/core/node/find_node_resources.go": "14ecd1d16f6ff4a579136a3a9e72fd4651e6f7f191bccf79443a75e353a1aeab", "internal/core/node/find_node_owned_local_target.go": "62ef1272aca207e7b2f74e7f89e78469281c2abf43ba2cb9b7df05413dfe4004", "internal/core/findnode/owned_bridge_answer.go": "e790d8240c15baab43fde9cc48a72fda8a4c82a8ce52d9f7fa92f8bc972d009a", "internal/core/findnode/owned_bridge_forward_answer.go": "e95e514c3db17a5a552b6e7fc0cee154c34459b141f568e1f601e7db04c61265", "internal/core/findnode/owned_bridge_local_target.go": "f9a14258ded9e99bb4ef39e88e8f0e52712fd6b66aec32eae7952d56a9aa7812", "internal/core/node/find_node_owned_answer.go": "a3c07ae43ffcb1b5cae2562f6559e6b6e281abf6029805e82b8bd1b3da8e7b9e", "internal/core/node/find_node_owned_receive.go": "74305a5ace9ed0a3244ea03521ea293372fed406b32b04fabcd55a8bb1353a6f", "internal/core/node/peer_management.go": "2ea8cef1ca531a95f252166597a65fa0466c6b98af995a26838ef0045fe30204", "internal/core/netcore/net_core.go": "1521705a66677e6d633bff7778b2c91da9152438e266f07adb7fbfd09cf12a57", "internal/core/netcore/owned_findnode_write.go": "5ebeb88ec8e82db8034bf66927f91c9ea7b08505b4ef5e6b939cf80fe6899a96", "internal/core/netcore/owned_findnode_answer_write.go": "a8f11fa1f5398b8811ef41460e2849000a438389e39ab662af446a9c7c134d0c"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned answer exact Source changed: "+path)
		}
	}
	closed := map[string]bool{"OwnedAnswerWork": true, "OwnedQueuedAnswer": true, "OwnedAnswerPermit": true, "ownedAnswerState": true, "mintOwnedAnswerWork": true, "OwnedReceivedAnswer": true, "ownedReceivedAnswerState": true, "SealAnswer": true, "MatchOwnedAnswer": true, "SourceSession": true, "QueueAnswerForOriginal": true, "CancelAnswer": true, "SendOwnedFindNodeAnswer": true, "writeOwnedFindNodeAnswer": true, "dispatchOwnedFindNodeAnswer": true, "dispatchOutboundOwnedFindNode": true, "queueOwnedFindNodeAnswer": true, "cancelOwned": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned answer Source parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && closed[id.Name] && !pinned {
				failures = append(failures, "owned answer original custody escaped: "+path+":"+id.Name)
			}
			return true
		})
	}
	return failures
}
func ownedAnswerSources(t *testing.T) map[string][]byte {
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
func TestOwnedAnswerClosedSource(t *testing.T) {
	if failures := ownedAnswerSourceErrors(ownedAnswerSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedAnswerSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"answer-unclaimed", "internal/core/node/find_node_owned_receive.go", "kind != protocol.FindNodeAnswerFrameType", "true"},
		{"command-bypass", "internal/core/node/find_node_owned_answer.go", "if !s.outboundControlFrameAllowed(session)", "if false"},
		{"target-rebind", "internal/core/node/find_node_owned_answer.go", "entry.common != cell.transport.life", "false"},
		{"no-one-use", "internal/core/findnode/owned_bridge_forward_answer.go", "cell.consumed.CompareAndSwap(false, true)", "true"},
		{"body-target-mismatch", "internal/core/findnode/owned_bridge_forward_answer.go", "entry.request.Target != cell.body.Target", "false"},
		{"foreign-forwarded-id", "internal/core/findnode/owned_bridge_forward_answer.go", "pendingKey{cell.target, cell.body.ID}", "pendingKey{cell.target, 0}"},
		{"signed-body-rewrite", "internal/core/findnode/owned_bridge_forward_answer.go", "answer.ID = request.ID", "answer.ID = 0"},
		{"expiry-refresh", "internal/core/findnode/owned_bridge_forward_answer.go", "mintOwnedAnswerWork(owner, source, answer, expires)", "mintOwnedAnswerWork(owner, source, answer, expires+responderTimeout)"},
		{"writer-deadline-equality", "internal/core/findnode/owned_bridge_answer.go", "Instant(at.Elapsed()) >= a.expires", "Instant(at.Elapsed()) > a.expires"},
		{"partial-nil-success", "internal/core/netcore/owned_findnode_answer_write.go", "err == nil && written == len(data)", "err == nil"},
		{"no-mandatory-finish", "internal/core/netcore/owned_findnode_answer_write.go", "finishErr := permit.Finish(writeOK)", "finishErr := error(nil)"},
		{"union-no-answer-cancel", "internal/core/netcore/owned_findnode_write.go", "_ = item.answer.Cancel()", "_ = item.answer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ownedAnswerSources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("owned answer exact control anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedAnswerSourceErrors(sources)) == 0 {
				t.Fatal("owned answer control escaped", tc.name)
			}
		})
	}
	sources := ownedAnswerSources(t)
	sources["internal/core/node/answer_escape.go"] = []byte("package node\nfunc answerEscape(s *Service){s.queueOwnedFindNodeAnswer(findnode.OwnedAnswerWork{})}\n")
	if len(ownedAnswerSourceErrors(sources)) == 0 {
		t.Fatal("owned answer OTHERFILE custody escaped")
	}
}
