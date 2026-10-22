package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Finite literal SOURCE closure for this registry/provenance family, not typed
// whole-program alias analysis or a Ready/full-writer proof. Raw body tokens
// include comments but ignore whitespace. These are reviewed literal pins,
// never expectations learned from the source under test.
func registryPublicationBodyPins() map[string]string {
	return map[string]string{
		"conn_registry.go:attachOutboundCoreLocked":               "73d0811dada864757456c013b2da320761ebe464f356c8ed7de660e82c73b4f3",
		"conn_registry.go:beginRegistryPublication":               "675698828ef2caa20810dd5dd401fbeced398e0c94a7cc5c2530774fc82e059f",
		"conn_registry.go:bindProofTransportLocked":               "3c9921e6eaff214bcd99da352872622b9e37b1db0778299e42d9767198d362ab",
		"conn_registry.go:publishAcceptedProofTransport":          "ff1f63812b27b42c384d7e946755bddf2ccf098260a04adcbbc8956df95522a6",
		"conn_registry.go:registerInboundConnLocked":              "7d0257aa8f1a38d5d7623deafbeaa6d0419434c7d045315993cb63a8d4eed8ab",
		"conn_registry.go:unregisterConnLocked":                   "25e1ea88a663752cd04534326337e74d57f38615886a9be30e087b1ca8b74f3d",
		"connection_manager.go:deactivateSlotLocked":              "26fb7214b4663240a888110d59bcc1261bf63b35be44130fbc6d8298d5257658",
		"connection_manager.go:handleDialSucceeded":               "c974591780d01e66e5f15bff4bc8e5e013f2da3321b3ae9acc04427bc0b71e2c",
		"connection_manager.go:tearDownSession":                   "ec0ccbf2a529b10dc4243592460479c2c01e37728ca4218fc94eee281d54fd94",
		"find_node_connection.go:prepareFindNodeCMAcceptedLocked": "967a726272bc0ad15f3fc5204b377c636cb5d2039704b080b70962586677ddf0",
		"find_node_connection.go:publishFindNodeCM":               "fb26ab47369e1d4e2b8600821acb6c5422a1c2f2ff0dbe9226dbdc2cbed8ebb1",
		"find_node_connection.go:publishFindNodeLocked":           "7320628cd8080ffb5cb220eeaf6ee7e8e207209ac77d2d68177e138d551f251b",
		"find_node_degree_terminal_bridge.go:endBridgeLocked":     "9041c5822dd74678779831771eb89168f94faf027e30f1c822bc5ab5936c94aa",
		"peer_management.go:ensurePeerSessions":                   "fc8d38cf4005fa7747e240256370a508cea2f83c3848b1301f45f43ec1b299d4",
		"peer_sessions.go:finishPeerSessionForCM":                 "4f4455791018300e3b8c70ddafeaffe13362707d8ecd318b45ed8ddc9ce859c1",
		"peer_sessions.go:markSessionHandshakeComplete":           "e2b420d83dd3323649516ee3a96811b7d1104b7a86b9dc008ed0e45c073544db",
		"peer_sessions.go:onCMSessionEstablished":                 "2f0be224e191ea75661559f9bd4fc23893e28cb2907857fd06a46d5ea4f20118",
		"peer_sessions.go:onCMSessionTeardown":                    "cb0307a59598d48258ff876422403c0de0ccbdb637d5ab1a1818bb86a55b34a4",
		"peer_sessions.go:openPeerSession":                        "7cccb903c56c714336db7a9fbb879200302c7b31b23397526f097f951ec607e7",
		"peer_sessions.go:registerCMSession":                      "b171ae1e0a5fcb5ac0245f3b6db90514504de5a928247222c6fe048da20fd7da",
		"peer_sessions.go:runPeerSession":                         "76c8da4a86cce3cabe4adf12512ae5d57f33e62264311c96e9d22bfc2e9ae957",
		"peer_sessions.go:withdrawRefusedCMSession":               "d6cc1b785aeebeb1acdb6b12cec39f0a7be545d1648b9d1fa03fdde9a60d1563",
		"peer_sessions.go:withdrawServedCMSession":                "bd507447d2807fb85bfa391eea80d8704b77561f736a7695667806cd3c79e6aa",
		"proof_transport_lifetime.go:proofTransportLifetime.end":  "d7a35159e88915a2454b8abcbead4d94577b49621981d24ec42866f71ef5891d",
		"service.go:attachOutboundNetCore":                        "c048453b27dca845954bd0a98e5109e6c1477f58eec0d05675bb35ccfb16c2ad",
		"service.go:closeRegisteredInboundCore":                   "b5c1512b53ce645304a027093e64906b053234969585c36e2f611041c497c82f",
		"service.go:unregisterInboundConn":                        "763a4f11373ee2f9454a13a508a9fdec3fe5d61e87aaa81ad75aaa4ed5be8070",
	}
}
func registryPublicationSourceErrors(sources map[string][]byte) []string {
	expected := map[string]int{
		"conn_registry.go:bindProofTransportLocked":     1,
		"conn_registry.go:registerInboundConnLocked":    1,
		"conn_registry.go:attachOutboundCoreLocked":     1,
		"conn_registry.go:unregisterConnLocked":         1,
		"peer_sessions.go:runPeerSession":               1,
		"peer_sessions.go:openPeerSession":              1,
		"peer_sessions.go:markSessionHandshakeComplete": 2,
		"peer_sessions.go:finishPeerSessionForCM":       1,
		"peer_sessions.go:onCMSessionEstablished":       1,
		"peer_sessions.go:registerCMSession":            1,
		"peer_sessions.go:withdrawRefusedCMSession":     1,
		"peer_sessions.go:withdrawServedCMSession":      1,
		"peer_sessions.go:onCMSessionTeardown":          1,
		"peer_management.go:ensurePeerSessions":         1,
	}
	// Direct original paired registry/session mutation membership: no wildcard.
	expectedWrites := map[string]int{
		"conn_registry.go:registerInboundConnLocked:conns:assign":           1,
		"conn_registry.go:registerInboundConnLocked:connIDByNetConn:assign": 1,
		"conn_registry.go:attachOutboundCoreLocked:conns:assign":            1,
		"conn_registry.go:attachOutboundCoreLocked:connIDByNetConn:assign":  1,
		"conn_registry.go:unregisterConnLocked:conns:delete":                1,
		"conn_registry.go:unregisterConnLocked:connIDByNetConn:delete":      1,
		"peer_sessions.go:runPeerSession:sessions:delete":                   1,
		"peer_sessions.go:openPeerSession:sessions:assign":                  1,
		"peer_sessions.go:registerCMSession:sessions:assign":                1,
		"peer_sessions.go:withdrawRefusedCMSession:sessions:delete":         1,
		"peer_sessions.go:withdrawServedCMSession:sessions:delete":          1,
		"peer_sessions.go:onCMSessionTeardown:sessions:delete":              1,
		"peer_management.go:ensurePeerSessions:sessions:delete":             1,
	}
	pins := registryPublicationBodyPins()
	gotPins := map[string]string{}
	refs, calls, writes := map[string]int{}, map[string]int{}, map[string]int{}
	var failures []string
	field := func(e ast.Expr) string {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return ""
		}
		owner, ok := sel.X.(*ast.Ident)
		if !ok || owner.Name != "s" {
			return ""
		}
		switch sel.Sel.Name {
		case "conns", "connIDByNetConn", "sessions":
			return sel.Sel.Name
		}
		return ""
	}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			failures = append(failures, "registry SOURCE parse")
			continue
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if path == "proof_transport_lifetime.go" && fn.Name.Name == "end" && fn.Recv != nil && len(fn.Recv.List) == 1 {
					if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
						if typ, ok := star.X.(*ast.Ident); ok {
							site = path + ":" + typ.Name + ".end"
						}
					}
				}
				if _, watched := pins[site]; watched && fn.Body != nil {
					body := raw[int(fn.Body.Pos())-1 : int(fn.Body.End())-1]
					sum := sha256.Sum256([]byte(strings.Join(strings.Fields(string(body)), "")))
					if _, duplicate := gotPins[site]; duplicate {
						failures = append(failures, "duplicate registry body")
					}
					gotPins[site] = hex.EncodeToString(sum[:])
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "beginRegistryPublication" {
					refs[site]++
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "beginRegistryPublication" {
						calls[site]++
					}
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "delete" && len(call.Args) == 2 {
						if key := field(call.Args[0]); key != "" {
							writes[site+":"+key+":delete"]++
						}
					}
				}
				if assign, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assign.Lhs {
						if idx, ok := lhs.(*ast.IndexExpr); ok {
							if key := field(idx.X); key != "" {
								writes[site+":"+key+":assign"]++
							}
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(gotPins, pins) {
		failures = append(failures, "registry exact body/order/nil-guard/lease scope changed")
	}
	if !reflect.DeepEqual(refs, expected) || !reflect.DeepEqual(calls, expected) {
		failures = append(failures, "registry publisher reference/call escaped exact15 sites")
	}
	if !reflect.DeepEqual(writes, expectedWrites) {
		failures = append(failures, "registry paired/session mutation membership changed")
	}
	return failures
}
func TestRegistryPublicationClosedSource(t *testing.T) {
	if failures := registryPublicationSourceErrors(pinPublicationSources(t)); len(failures) != 0 {
		t.Fatal("closed registry publication SOURCE", failures)
	}
}

// Exact AST-bounded function edits; an ambiguous anchor is infrastructure
// refusal, never accepted as causal SOURCE evidence.
func registryPublicationMutate(t *testing.T, sources map[string][]byte, path, name, before, after string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for n, raw := range sources {
		out[n] = bytes.Clone(raw)
	}
	raw := out[path]
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name && fn.Body != nil {
			start, end := int(fn.Body.Pos())-1, int(fn.Body.End())-1
			body := string(raw[start:end])
			if strings.Count(body, before) != 1 {
				t.Fatal("registry negative anchor must be unique", path, name, before)
			}
			out[path] = append(append(bytes.Clone(raw[:start]), []byte(strings.Replace(body, before, after, 1))...), raw[end:]...)
			count++
		}
	}
	if count != 1 {
		t.Fatal("registry negative needs exact one original function", path, name)
	}
	return out
}
func TestRegistryPublicationSourceRejectsEscapes(t *testing.T) {
	sources := pinPublicationSources(t)
	if failures := registryPublicationSourceErrors(sources); len(failures) != 0 {
		t.Fatal("registry baseline SOURCE", failures)
	}
	for _, c := range []struct{ name, path, fn, before, after string }{
		{"outer-W-before-End", "conn_registry.go", "unregisterConnLocked", "entry.common.end()", "outer := s.beginRegistryPublication(); entry.common.end(); if outer != nil { outer.Complete() }"},
		{"lock-after-W", "conn_registry.go", "registerInboundConnLocked", "s.conns[id] = entry", "s.deliveryMu.Lock(); s.conns[id] = entry; s.deliveryMu.Unlock()"},
		{"Complete-before-last-write", "conn_registry.go", "registerInboundConnLocked", "s.connIDByNetConn[conn] = id", "if publication != nil { publication.Complete() }; s.connIDByNetConn[conn] = id"},
		{"omitted-Complete", "conn_registry.go", "attachOutboundCoreLocked", "publication.Complete()", "_ = publication"},
		{"nil-Complete", "conn_registry.go", "registerInboundConnLocked", "if publication != nil {", "if true {"},
		{"lease-escape", "conn_registry.go", "attachOutboundCoreLocked", "publication.Complete()", "go publication.Complete()"},
		{"cleanup-veto", "conn_registry.go", "unregisterConnLocked", "id, ok := s.connIDByNetConn[conn]", "if s.runCtx.Err() != nil { return }; id, ok := s.connIDByNetConn[conn]"},
		{"receive-inside-W", "conn_registry.go", "unregisterConnLocked", "delete(s.conns, id)", "if entry := s.conns[id]; entry != nil && entry.receive != nil { entry.receive.Close() }; delete(s.conns, id)"},
		{"budget-inside-W", "conn_registry.go", "unregisterConnLocked", "delete(s.conns, id)", "if entry := s.conns[id]; entry != nil { entry.budget.Release() }; delete(s.conns, id)"},
		{"core-close-inside-W", "conn_registry.go", "unregisterConnLocked", "delete(s.conns, id)", "if entry := s.conns[id]; entry != nil { entry.core.Close() }; delete(s.conns, id)"},
		{"receipt-allocation-inside-W", "peer_sessions.go", "onCMSessionEstablished", "session.selectedGeneration = info.SlotGeneration", "receipt = s.prepareFindNodeCMAcceptedLocked(info); session.selectedGeneration = info.SlotGeneration"},
		{"receipt-wrong-common", "find_node_connection.go", "prepareFindNodeCMAcceptedLocked", "common: p.common", "common: nil"},
		{"receipt-wrong-generation", "find_node_connection.go", "prepareFindNodeCMAcceptedLocked", "generation: info.SlotGeneration", "generation: 1"},
		{"receipt-wrong-address", "find_node_connection.go", "prepareFindNodeCMAcceptedLocked", "address: info.Address", "address: p.address"},
		{"drop-CM-owner", "peer_sessions.go", "onCMSessionTeardown", "info.Session != nil && s.sessions[addr] == info.Session", "info.Session != nil"},
		{"wrong-publisher", "conn_registry.go", "beginRegistryPublication", "s.bridgeTerminal.publisher.BeginPublication()", "s.degree.bridgePublisher.BeginPublication()"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if failures := registryPublicationSourceErrors(registryPublicationMutate(t, sources, c.path, c.fn, c.before, c.after)); len(failures) == 0 {
				t.Fatal("registry SOURCE escape accepted", c.name)
			}
		})
	}
	t.Run("method-value-other-file", func(t *testing.T) {
		changed := map[string][]byte{}
		for n, raw := range sources {
			changed[n] = bytes.Clone(raw)
		}
		changed["late_registry.go"] = []byte("package node; func lateRegistry(s *Service){ _ = s.beginRegistryPublication }")
		if len(registryPublicationSourceErrors(changed)) == 0 {
			t.Fatal("registry late method-value alias accepted")
		}
	})
}
