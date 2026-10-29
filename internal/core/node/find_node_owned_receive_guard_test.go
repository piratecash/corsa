package node

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

func ownedNodeReceiveSourceErrors(sources map[string][]byte) []string {
	var failures []string
	pins := map[string]string{"find_node_owned_local_target.go": "3f72673fd435bb363ff434ef5fb728d61fc151e06f85b72a9e1a303599983915", "find_node_owned_answer.go": "db16c461c252487076df0515d067202c0d8dbbfd84ea0a3f35fc7b795b2c202e", "peer_session_admission.go": "0c3029c707c1741848bd9a959d9ad5aeb549e378cf0483e1a8c3f8853e5c0557", "find_node_owned_receive.go": "74305a5ace9ed0a3244ea03521ea293372fed406b32b04fabcd55a8bb1353a6f", "find_node_bridge_capture.go": "920726a9421c2a8eacacd8ce8bbd665937c7f830deadb29eabc5ae0f6b5c6ba4", "service.go": "140e304fe12ffdaa532521f80059803ec30bcd54355afb253235504f6760320e", "peer_management.go": "a040983845c8a542d11f3686b39ef27a04f803309865264a27aeb68f95292520"}
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned ingress raw Source changed: "+path)
		}
	}
	methods := map[string]bool{"ParseFindNodeRequest": true, "readOutboundOwnedFindNodeBeforeCharge": true, "readSessionLineBeforeCharge": true, "admitReadSessionLine": true, "OfferReceived": true, "SealReceived": true, "dispatchOwnedFindNodeReceive": true, "readInboundOwnedFindNodeLine": true, "readOutboundOwnedFindNodeLine": true, "recordOwnedFindNodeLine": true, "recordOwnedFindNodeReceive": true, "sealOwnedFindNodeObservation": true, "takeOwnedFindNodeReceive": true}
	types := map[string]bool{"ownedFindNodeReceive": true, "ownedFindNodeReceiveCell": true, "ownedFindNodeIngress": true}
	got, declared := map[string]int{}, map[string]int{}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned ingress Source parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var definition *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				definition = fn.Name
				if methods[fn.Name.Name] {
					declared[fn.Name.Name]++
					want := "find_node_owned_receive.go"
					if fn.Name.Name == "sealOwnedFindNodeObservation" {
						want = "find_node_bridge_capture.go"
					}
					if fn.Name.Name == "readSessionLineBeforeCharge" || fn.Name.Name == "admitReadSessionLine" {
						want = "peer_session_admission.go"
					}
					if path != want {
						failures = append(failures, "foreign ingress declaration")
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
							failures = append(failures, "ingress method value escape")
						}
					}
					if types[id.Name] {
						if _, ok := pins[path]; !ok {
							failures = append(failures, "ingress receipt escaped pinned original files")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(got, map[string]int{"sealOwnedFindNodeObservation:find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget": 1, "SealReceived:find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget": 1, "ParseFindNodeRequest:find_node_owned_receive.go:takeOwnedFindNodeReceive": 1, "readOutboundOwnedFindNodeBeforeCharge:find_node_owned_receive.go:readOutboundOwnedFindNodeLine": 1, "readSessionLineBeforeCharge:peer_session_admission.go:readAdmittedSessionLine": 1, "readSessionLineBeforeCharge:find_node_owned_receive.go:readOutboundOwnedFindNodeBeforeCharge": 1, "admitReadSessionLine:peer_session_admission.go:readAdmittedSessionLine": 1, "admitReadSessionLine:find_node_owned_receive.go:readOutboundOwnedFindNodeLine": 1, "readInboundOwnedFindNodeLine:service.go:handleConn": 1, "readOutboundOwnedFindNodeLine:peer_management.go:readPeerSession": 1, "recordOwnedFindNodeLine:find_node_owned_receive.go:readInboundOwnedFindNodeLine": 1, "recordOwnedFindNodeLine:find_node_owned_receive.go:readOutboundOwnedFindNodeBeforeCharge": 1, "recordOwnedFindNodeReceive:find_node_owned_receive.go:recordOwnedFindNodeLine": 1, "takeOwnedFindNodeReceive:find_node_owned_receive.go:dispatchOwnedFindNodeReceive": 1, "dispatchOwnedFindNodeReceive:service.go:handleConn": 1, "dispatchOwnedFindNodeReceive:find_node_owned_answer.go:dispatchOutboundOwnedFindNode": 1, "sealOwnedFindNodeObservation:find_node_bridge_capture.go:captureOwnedFindNodeCut": 1, "sealOwnedFindNodeObservation:find_node_owned_receive.go:dispatchOwnedFindNodeReceive": 1, "SealReceived:find_node_owned_receive.go:dispatchOwnedFindNodeReceive": 1, "OfferReceived:find_node_owned_receive.go:dispatchOwnedFindNodeReceive": 1}) {
		failures = append(failures, "ingress exact call graph changed")
	}
	if !reflect.DeepEqual(declared, map[string]int{"readOutboundOwnedFindNodeBeforeCharge": 1, "readSessionLineBeforeCharge": 1, "admitReadSessionLine": 1, "dispatchOwnedFindNodeReceive": 1, "readInboundOwnedFindNodeLine": 1, "readOutboundOwnedFindNodeLine": 1, "recordOwnedFindNodeLine": 1, "recordOwnedFindNodeReceive": 1, "sealOwnedFindNodeObservation": 1, "takeOwnedFindNodeReceive": 1}) {
		failures = append(failures, "ingress declarations changed")
	}
	return failures
}
func TestOwnedNodeReceiveClosedSource(t *testing.T) {
	if failures := ownedNodeReceiveSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedNodeReceiveSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"discard", "service.go", "s.dispatchOwnedFindNodeReceive(ownedReceive)", "error(nil)"},
		{"original-rebind", "find_node_owned_receive.go", "entry.common != cell.transport.life", "false"},
		{"strict-parser", "find_node_owned_receive.go", "protocol.ParseFindNodeRequest([]byte(cell.wire))", "protocol.FindNodeRequest{}, error(nil)"},
		{"receive-refresh", "find_node_owned_receive.go", "received: cell.received", "received: findnodebridge.OwnedInstant{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique ingress SOURCE anchor")
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedNodeReceiveSourceErrors(sources)) == 0 {
				t.Fatal("ingress SOURCE mutation escaped")
			}
		})
	}
	sources := ninN1Sources(t)
	sources["owned_ingress_escape.go"] = []byte("package node\nvar ingressValue=(*Service).dispatchOwnedFindNodeReceive\n")
	if len(ownedNodeReceiveSourceErrors(sources)) == 0 {
		t.Fatal("ingress OTHERFILE value escaped")
	}
}
