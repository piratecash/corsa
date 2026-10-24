package findnode

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Finite raw Source and closed reference inventory, not whole-program dataflow.
// All original receipt fields are frozen by these raw pins; only consumed is
// mutable. New callers/aliases in other production files must be reviewed.
func ownedReceiveSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"owned_bridge_forward_answer.go": "189a2b6a66e0b582b303f8adb36e24a826125583a4d1b4a0f21782b0870e4f2f", "owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "owned_bridge_local_target.go": "19e3da9a1091b13d60f7cd9998cfdd5b9f9ed143339e6858d9e44490e63b9769", "owned_bridge_local_target_stage.go": "39fde17cdadd3dfa02bc97ef40f9d21d37f18ca865abde2fed2005e680baa0c3", "owned_bridge_dispatch.go": "3a6495784029c780e6cca5fa3d439a7d99fd34ddbc54fce5d97a75242d608732", "owned_bridge_round_dispatch.go": "b9128ec6284560aea84441eab2b8619ba08297b66133139af4030a39d4478fcf", "owned_bridge_receive.go": "5de62b53e6ae476daf7563e4cede1e95ed9e1e7f30fbc4de1e06947bc4144671", "intake_owner.go": "464c803f0c96ef419752c9965b217ba0d0b74c9ff0469c6b3e08efb2df60fe18", "owned_bridge_commit.go": "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255", "owned_bridge_round_plan.go": "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51", "owned_bridge_round_execute.go": "dedd956801a9a14580f4e22634d3a5eb579ebe22520eb45eedce564209d7eec4"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned receive raw Source changed: "+path)
		}
	}
	methods := map[string]bool{"SealReceived": true, "OfferReceived": true, "ownedReceivedMatches": true}
	types := map[string]bool{"ownedReceivedIdentity": true, "ownedReceivedState": true, "OwnedReceivedRequest": true}
	got := map[string]int{}
	declared := map[string]int{}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned receive Source parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var definition *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				definition = fn.Name
				if methods[fn.Name.Name] {
					declared[fn.Name.Name]++
					if path != "owned_bridge_receive.go" {
						failures = append(failures, "foreign receive declaration")
					}
				}
			}
			callees := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok {
						callees[id] = true
					}
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if methods[id.Name] && id != definition {
						got[id.Name+":"+site]++
						if !callees[id] {
							failures = append(failures, "receive method value escape")
						}
					}
					if types[id.Name] || id.Name == "ownedReceived" || id.Name == "ownedIdentity" || id.Name == "receivedIdentity" || id.Name == "receivedRequest" {
						if _, ok := pins[path]; !ok {
							failures = append(failures, "receive opaque type escaped pinned originals")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(declared, map[string]int{"SealReceived": 1, "OfferReceived": 1, "ownedReceivedMatches": 1}) {
		failures = append(failures, "receive declarations changed")
	}
	if !reflect.DeepEqual(got, map[string]int{"ownedReceivedMatches:owned_bridge_dispatch.go:Take": 1, "ownedReceivedMatches:owned_bridge_forward_answer.go:MatchOwnedAnswer": 1, "ownedReceivedMatches:owned_bridge_round_plan.go:copyOwnedRoundPlanLocked": 1, "ownedReceivedMatches:owned_bridge_round_execute.go:validateOwnedRoundExecutionLocked": 1, "ownedReceivedMatches:owned_bridge_commit.go:commitOwnedFinalLocked": 1}) {
		failures = append(failures, "receive exact reference closure changed")
	}
	return failures
}
func TestOwnedReceiveClosedSource(t *testing.T) {
	if failures := ownedReceiveSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedReceiveSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"replay", "cell.consumed.CompareAndSwap(false, true)", "true"},
		{"refresh", "received := Instant(identity.received.Elapsed())", "received := Instant(sealed.at.Elapsed())"},
		{"late-equality", "f >= fixedCut", "f > fixedCut"},
		{"cleanup-debt", "f >= prior.expires", "false"},
		{"current-all-originals", "sealed.originals.sampleAndCheckOriginals(lease)", "lease.Sample()"},
		{"tuple-rewrite", "cell.identity != identity", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			raw := string(sources["owned_bridge_receive.go"])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique SOURCE mutation anchor")
			}
			sources["owned_bridge_receive.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedReceiveSourceErrors(sources)) == 0 {
				t.Fatal("receive SOURCE mutation escaped")
			}
		})
	}
	for _, extra := range []string{"package findnode\nvar escapedReceived *ownedReceivedState\n", "package findnode\nfunc extraOffer(i DormantIntake,r OwnedReceivedRequest,c OwnedCut,s Session){_=i.OfferReceived(r,s,c)}\n", "package findnode\nvar receiveValue=(DormantIntake{}).OfferReceived\n"} {
		sources := preparedSessionSources(t)
		sources["owned_receive_escape.go"] = []byte(extra)
		if len(ownedReceiveSourceErrors(sources)) == 0 {
			t.Fatal("receive OTHERFILE escaped")
		}
	}
}
