package node

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Exact production pins close constructor, original retirement, self branch,
// pre-admission queue claim and every actual-T/sign/Complete call site. The
// behavioral fixtures independently exercise observable original wire effects.
func ownedLocalNodeSourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{"find_node_owned_answer.go": "db16c461c252487076df0515d067202c0d8dbbfd84ea0a3f35fc7b795b2c202e", "find_node_ready_target_timing.go": "515455541e482579332a42535b4ed58ddab7633a8ef52a50e0c23138285d7667", "find_node_ready_construction.go": "79872a3bf79626c6ca5071ae2dbfc8cd6451d955a3d7eeaeeb45d20b92c17db5", "find_node_resources.go": "14ecd1d16f6ff4a579136a3a9e72fd4651e6f7f191bccf79443a75e353a1aeab", "find_node_owned_local_target.go": "3f72673fd435bb363ff434ef5fb728d61fc151e06f85b72a9e1a303599983915", "find_node_bridge_capture.go": "920726a9421c2a8eacacd8ce8bbd665937c7f830deadb29eabc5ae0f6b5c6ba4", "find_node_connection.go": "149ae30e487317e1d895786b704b151e22e2fa6062831ece31ead0788024a55a", "proof_transport_lifetime.go": "5b12ec684cb095751c62c9b90e64593da266ed3a5e0ea50fc6801624c1939119", "find_node_owned_receive.go": "90106224706f71d3a660a2e5d3ef6a77ae5357c1f38e4a110e938cc02b695e51"}
	pins["find_node_resource_diagnostic.go"] = "e97c7f60a858b6380569d957646ab4338cc16a1bd638dcb25494924b66e91556"
	pins["find_node_membership_checkpoint.go"] = "39209defff20031f7f093ec2bd86c2c956e105b3699fa70a8963b522290186d8"
	closed := map[string]bool{"ownedFindNodeSigningTestGate": true, "signingGate": true, "ownedFindNodeLocalLoop": true, "newOwnedFindNodeLocalLoop": true, "newFindNodeOwnedLocalTargetCandidateService": true, "prepareFindNodeOwnedLocalTargetConstruction": true, "dispatchOwnedFindNodeLocalTarget": true, "runOwnedFindNodeLocalLoop": true, "runOwnedFindNodeLocalWork": true, "completeOwnedFindNodeLocalSignature": true, "OwnedLocalTarget": true, "OwnedLocalSignWork": true, "OwnedLocalSignPermit": true, "OwnedLocalResult": true, "NewLocalTarget": true, "ReserveReceived": true, "localTarget": true, "localLoop": true, "localRetirement": true}
	var failures []string
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned Node local exact Source changed: "+path)
		}
	}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned Node local production parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && closed[id.Name] && !pinned {
				failures = append(failures, "owned Node local authority outside exact frontier: "+path+":"+id.Name)
			}
			return true
		})
	}
	return failures
}
func TestOwnedLocalNodeClosedSource(t *testing.T) {
	if failures := ownedLocalNodeSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedLocalNodeSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"missing-mandatory-completion", "find_node_owned_local_target.go", "if !completed {\n\t\t\t_, _ = permit.Complete(nil)\n\t\t}", "if false {\n\t\t\t_, _ = permit.Complete(nil)\n\t\t}"},
		{"missing-local-retirement", "proof_transport_lifetime.go", "_ = retiredLocal.EndOriginalSession(retiredSession)", "_ = retiredIntake.EndOriginalSession(retiredSession)"},
		{"drop-constructor-original", "find_node_owned_local_target.go", "b.findNode.owned.localRetirement = local", "b.findNode.owned.localRetirement = findnode.OwnedLocalTarget{}"},
		{"skip-real-T", "find_node_owned_local_target.go", "permit, input, err := work.Take()", "permit, input, err := findnode.OwnedLocalSignPermit{}, findnode.PublicSigningInput{}, error(nil)"},
		{"unbounded-request-worker", "find_node_owned_local_target.go", "s.runOwnedFindNodeLocalWork(loop, timer, work)", "go s.runOwnedFindNodeLocalWork(loop, timer, work)"},
		{"caller-selected-self", "find_node_owned_receive.go", "ingress.request.Target == domain.PeerIdentityFromWire(s.identity.Address)", "ingress.request.Target == ingress.source.Identity()"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			text := string(sources[tc.path])
			if strings.Count(text, tc.before) != 1 {
				t.Fatal("unique owned Node local control anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(text, tc.before, tc.after, 1))
			if len(ownedLocalNodeSourceErrors(sources)) == 0 {
				t.Fatal("owned Node local Source control escaped", tc.name)
			}
		})
	}
	sources := ninN1Sources(t)
	sources["owned_local_node_escape.go"] = []byte("package node\nvar copiedLocalRetirement OwnedLocalTarget\n")
	if len(ownedLocalNodeSourceErrors(sources)) == 0 {
		t.Fatal("OTHERFILE local authority escaped")
	}
}
