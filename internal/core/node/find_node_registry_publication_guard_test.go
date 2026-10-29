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
		"conn_registry.go:attachOutboundCoreLocked":               "f6896ff8421a950391b57733369523a12b27cd042e9ce216822d166c8c4128d2",
		"conn_registry.go:beginRegistryPublication":               "675698828ef2caa20810dd5dd401fbeced398e0c94a7cc5c2530774fc82e059f",
		"conn_registry.go:bindProofTransportLocked":               "3c9921e6eaff214bcd99da352872622b9e37b1db0778299e42d9767198d362ab",
		"conn_registry.go:publishAcceptedProofTransport":          "ff1f63812b27b42c384d7e946755bddf2ccf098260a04adcbbc8956df95522a6",
		"conn_registry.go:registerInboundConnLocked":              "68628eedbf2b825a37b83f4f3ec30b288421763eed7730e8e3f7e50b166e8c88",
		"conn_registry.go:unregisterConnLocked":                   "f59796f1bcdbf93cd9792866515aa24e381f95a00b394dcbdbcf0aad525e3e8c",
		"connection_manager.go:deactivateSlotLocked":              "26fb7214b4663240a888110d59bcc1261bf63b35be44130fbc6d8298d5257658",
		"connection_manager.go:handleDialSucceeded":               "0b7f0d2942f6ca4be107a1898bc5b501d6266d643e61f1e9c2112c75b6fe0fbf",
		"connection_manager.go:tearDownSession":                   "610654b3b56e7214b2cb5bee40ea6d7c55fce5a2fbc58649e63dac1eab42d6f6",
		"find_node_connection.go:prepareFindNodeCMAcceptedLocked": "967a726272bc0ad15f3fc5204b377c636cb5d2039704b080b70962586677ddf0",
		"find_node_connection.go:publishFindNodeCM":               "fb26ab47369e1d4e2b8600821acb6c5422a1c2f2ff0dbe9226dbdc2cbed8ebb1",
		"find_node_connection.go:publishFindNodeLocked":           "7320628cd8080ffb5cb220eeaf6ee7e8e207209ac77d2d68177e138d551f251b",
		"find_node_degree_terminal_bridge.go:endBridgeLocked":     "9041c5822dd74678779831771eb89168f94faf027e30f1c822bc5ab5936c94aa",
		"peer_management.go:ensurePeerSessions":                   "30d2bfef119c0bf5326f447c2af3f8e614f585c7df9b07b443c7e15ef65f5cb7",
		"peer_sessions.go:finishPeerSessionForCM":                 "63652fc54b1e8131853b504289e1756f2c217bf32c9ddb6b55c44fd1b0a8fd77",
		"peer_sessions.go:markSessionHandshakeComplete":           "e2b420d83dd3323649516ee3a96811b7d1104b7a86b9dc008ed0e45c073544db",
		"peer_sessions.go:onCMSessionEstablished":                 "53c9fe71b8dadf30d172e76bb1ffc6037f76ddf82f4ed4b530fb4f0e37bd7dc7",
		"peer_sessions.go:onCMSessionTeardown":                    "c686bf792bb9c2ee635c074c6d7080badc11b68e436f2e54679203d7d31147f0",
		"peer_sessions.go:openPeerSession":                        "eff15faa2d5800b9dfe7b63a3fa62503bd5b9cb03b9d8960713ff09e05bf6a08",
		"peer_sessions.go:registerCMSession":                      "de4aef0ffb9b4df33dcba494a6c65c77a75bafdd2bae5ecf9f5e2eab331b9901",
		"peer_sessions.go:runPeerSession":                         "69bf61f2a6fbdc5b53bd358729800c5f2f590af4bc857b6403c228446fdb11ba",
		"peer_sessions.go:withdrawRefusedCMSession":               "867668ec2d3cbe8c775b19005dd9ecd8fc3d7f2abccab07499025b6569901be7",
		"peer_sessions.go:withdrawServedCMSession":                "83c1ebd42e48ac3b9521c54922a16b7416ef615a18cd26910fdb6fefe92b3e7a",
		"proof_transport_lifetime.go:proofTransportLifetime.end":  "d7a35159e88915a2454b8abcbead4d94577b49621981d24ec42866f71ef5891d",
		"service.go:attachOutboundNetCore":                        "b39aaf980a30ac8831da707263bcfee18191d4f8decf17b396dfb7cae3073916",
		"service.go:closeRegisteredInboundCore":                   "7400f110299be697ca96fccab8ed58d490179da64796c4689f50bf73147fc780",
		"service.go:unregisterInboundConn":                        "abd806894a6a0a9b27eafb48912299769427b5670e6f76d7132f9cd74f537746",
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
		{"budget-before-close", "connection_manager.go", "tearDownSession", "_ = teardown.info.Session.closeAs(teardown.closer)", "teardown.reservation.Release(); _ = teardown.info.Session.closeAs(teardown.closer)"},
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
