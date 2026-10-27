package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Exact conditional SOURCE contract; no Ready, full capture or typed/unsafe
// alias closure is inferred from this finite syntax inventory.
func ownedTerminalSourceErrors(sources map[string][]byte) []string {
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	var failures []string
	need := func(path, fragment, label string) {
		if strings.Count(compact(string(sources[path])), compact(fragment)) != 1 {
			failures = append(failures, label)
		}
	}
	need("proof_transport_lifetime.go", "if l.ended { return } if l.bridgeTerminal != nil { l.endBridgeLocked() return } l.ended = true l.degreeEndLocked()", "owned End dispatch must precede legacy transition")
	need("find_node_degree.go", "w := &degreeWitness{origin:l, owner:o, key:key, slot:-1} o.mu.Lock()", "sole genuine mint must allocate before owner/W")
	need("find_node_degree.go", "if o.bridgePublisher != (findnodebridge.Publisher{}) { return 0, false } now := o.clock.elapsed()", "owned legacy raw clock embargo")
	need("find_node_degree.go", "if o == nil || o.bridgePublisher != (findnodebridge.Publisher{}) { return degreeCut{}, errDegreeUnavailable }", "owned legacy complete-cut embargo")
	need("peer_state_degree_statistic.go", "if o == nil || o.bridgePublisher != (findnodebridge.Publisher{}) { return 0, time.Time{}, false }", "owned legacy statistical raw clock embargo")
	need("peer_state_degree_statistic.go", "if o == nil || o.bridgePublisher != (findnodebridge.Publisher{}) { return 0,0,0,errDegreeUnavailable }", "owned legacy projector write embargo")
	need("session_secure.go", "&proofTransportLifetime{token:s.proofToken, bridgeTerminal:s.bridgeTerminal, role:sessionv2.RoleListener}", "original accepted lifetime attachment")
	need("session_secure.go", "&proofTransportLifetime{token:s.proofToken, bridgeTerminal:s.bridgeTerminal, role:sessionv2.RoleDialer,outbound:true,id:transport.connID}", "original dialed lifetime attachment")
	need("service.go", "svc := &Service{ proofToken:proofToken, degree:degree, bridgeTerminal:terminal,", "first Service literal attachment")
	need("secure_session_store.go", "store := newSecureSessionStoreWithPublisher(opts.clock,opts.logger,opts.bridgePublisher)", "store attachment must precede disk/refusal")
	need("session_secure.go", "store := openSecureSessionStore(secureSessionStoreOptions{bridgePublisher:opts.bridgePublisher,", "secure store option forwarding")
	need("find_node_degree_terminal_bridge.go", "type findNodeBridgeTerminal struct { publisher findnodebridge.Publisher owner *degreeOwner token *proofTransportServiceToken }", "immutable terminal closed shape")
	need("find_node_bridge_construction.go", "type findNodeTerminalConstruction struct { ctx context.Context cancel context.CancelFunc token *proofTransportServiceToken owner *degreeOwner terminal *findNodeBridgeTerminal used atomic.Bool statistic *ownedDegreeStatisticProducer statisticInstalled bool findNode *findNodeAdapter capture *findNodeBridgeCaptureContext }", "immutable construction closed shape")
	need("find_node_bridge_construction.go", "type serviceConstruction struct { k3Advertised *k3AdvertisedRequest pinMembership *pinMembershipRequest k3Registered *k3RegisteredRequest record recordAdmissionConstruction bridge *findNodeTerminalConstruction ready *findNodeReadyConstruction pendingObservation *pendingObservationConstruction terminalObservation *terminalObservationConstruction guardObservation *guardObservationConstruction handshakeObservation *handshakeObservationConstruction handshakeFileObservation *handshakeFileObservationConstruction connectionTotalObservation *connectionTotalObservationConstruction connectionTotalFileObservation *connectionTotalFileObservationConstruction }", "construction envelope closed shape")
	typeRefs := map[string]int{}
	trackedTypes := map[string]bool{"findNodeBridgeTerminal": true, "findNodeTerminalConstruction": true, "serviceConstruction": true}
	bodies := map[string]string{}
	literals := map[string]int{}
	calls := map[string]int{}
	refs := map[string]int{}
	trackedFunctions := map[string]bool{"newFindNodeBridgeCandidateService": true, "prepareFindNodeBridgeConstruction": true, "newFindNodeTerminalConstruction": true, "newFindNodeTerminalCandidateService": true, "newServiceWithConstruction": true, "endBridgeLocked": true, "clearBridgeTerminalLocked": true, "constructionPublisher": true, "loadTrustStoreForConstruction": true, "newFindNodeTerminalStatisticCandidateService": true, "prepareFindNodeTerminalConstruction": true, "newFindNodeOwnedF1CandidateService": true, "newFindNodeOwnedCaptureCandidateService": true, "prepareFindNodeOwnedCaptureConstruction": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			failures = append(failures, "owned terminal parse")
			continue
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
				if fn.Body != nil {
					bodies[site] = compact(pinPublicationPrinted(fn.Body))
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n != declaration && trackedFunctions[n.Name] {
						refs[n.Name+":"+site]++
					}
					if trackedTypes[n.Name] {
						typeRefs[n.Name+":"+site]++
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && (sel.Sel.Name == "bridgeTerminal" || sel.Sel.Name == "bridgePublisher") {
							failures = append(failures, "owned attachment late write")
						}
					}
				case *ast.CompositeLit:
					if typ, ok := n.Type.(*ast.Ident); ok && (typ.Name == "findNodeBridgeTerminal" || typ.Name == "findNodeTerminalConstruction") {
						literals[typ.Name+":"+site]++
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && trackedFunctions[id.Name] {
						calls[id.Name+":"+site]++
					}
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && trackedFunctions[sel.Sel.Name] {
						calls[sel.Sel.Name+":"+site]++
					}
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SealConstruction" && site != "find_node_ready_construction.go:finalize" {
						failures = append(failures, "terminal production root must remain Constructing")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(literals, map[string]int{"findNodeBridgeTerminal:find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1, "findNodeTerminalConstruction:find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1}) {
		failures = append(failures, "terminal constructor set changed")
	}
	if !reflect.DeepEqual(calls, map[string]int{"newServiceWithConstruction:s6_terminal_construction.go:newServiceWithObservations": 1, "prepareFindNodeOwnedCaptureConstruction:find_node_ready_construction.go:prepareFindNodeReadyConstruction": 1, "newServiceWithConstruction:find_node_ready_construction.go:buildWithOptions": 1, "newServiceWithConstruction:find_node_owned_local_target.go:newFindNodeOwnedLocalTargetCandidateService": 1, "prepareFindNodeOwnedCaptureConstruction:find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction": 1, "newServiceWithConstruction:find_node_owned_round_loop.go:newFindNodeOwnedRoundCandidateService": 1, "prepareFindNodeBridgeConstruction:find_node_bridge_construction.go:newFindNodeBridgeCandidateService": 1, "prepareFindNodeOwnedCaptureConstruction:find_node_bridge_construction.go:prepareFindNodeBridgeConstruction": 1, "newServiceWithConstruction:find_node_bridge_construction.go:newFindNodeBridgeCandidateService": 1, "prepareFindNodeOwnedCaptureConstruction:find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService": 1, "newFindNodeTerminalConstruction:find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1, "newServiceWithConstruction:find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService": 1, "prepareFindNodeTerminalConstruction:find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService": 1, "newServiceWithConstruction:find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService": 1, "newFindNodeTerminalConstruction:find_node_bridge_construction.go:prepareFindNodeTerminalConstruction": 1, "prepareFindNodeTerminalConstruction:find_node_bridge_construction.go:newFindNodeTerminalCandidateService": 1, "prepareFindNodeTerminalConstruction:find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "newServiceWithConstruction:find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "newServiceWithConstruction:find_node_bridge_construction.go:newFindNodeTerminalCandidateService": 1, "newServiceWithConstruction:service.go:newServiceWithRecordAdmissionClasses": 1, "endBridgeLocked:proof_transport_lifetime.go:end": 1, "clearBridgeTerminalLocked:find_node_degree_terminal_bridge.go:endBridgeLocked": 2, "constructionPublisher:service.go:newServiceWithConstruction": 1, "loadTrustStoreForConstruction:service.go:newServiceWithConstruction": 1}) {
		failures = append(failures, "terminal root/call closure changed")
	}
	wantTypeRefs := map[string]int{"findNodeBridgeTerminal:find_node_ready_construction.go:<package>": 1, "findNodeTerminalConstruction:find_node_ready_construction.go:<package>": 1, "serviceConstruction:find_node_ready_construction.go:buildWithOptions": 1, "findNodeTerminalConstruction:find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction": 1, "serviceConstruction:find_node_owned_local_target.go:newFindNodeOwnedLocalTargetCandidateService": 1,
		"findNodeTerminalConstruction:find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction":      1,
		"serviceConstruction:find_node_owned_round_loop.go:newFindNodeOwnedRoundCandidateService":               1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:prepareFindNodeBridgeConstruction":       1,
		"serviceConstruction:find_node_bridge_construction.go:newFindNodeBridgeCandidateService":                1,
		"findNodeBridgeTerminal:find_node_bridge_capture.go:<package>":                                          1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1,
		"serviceConstruction:find_node_bridge_construction.go:newFindNodeOwnedCaptureCandidateService":          1,
		"findNodeBridgeTerminal:find_node_connection.go:<package>":                                              1,
		"findNodeBridgeTerminal:find_node_degree_terminal_bridge.go:<package>":                                  1,
		"findNodeBridgeTerminal:find_node_bridge_construction.go:<package>":                                     1,
		"findNodeBridgeTerminal:find_node_bridge_construction.go:newFindNodeTerminalConstruction":               1,
		"findNodeBridgeTerminal:proof_transport_lifetime.go:<package>":                                          1,
		"findNodeBridgeTerminal:service.go:<package>":                                                           1,
		"findNodeBridgeTerminal:service.go:newServiceWithConstruction":                                          1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:<package>":                               2,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:newFindNodeTerminalConstruction":         2,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:claim":                                   1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:prepareFindNodeTerminalConstruction":     1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:constructionPublisher":                   1,
		"findNodeTerminalConstruction:find_node_bridge_construction.go:loadTrustStoreForConstruction":           1,
		"serviceConstruction:find_node_bridge_construction.go:<package>":                                        1,
		"serviceConstruction:find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService":               1,
		"serviceConstruction:find_node_bridge_construction.go:newFindNodeTerminalCandidateService":              1,
		"serviceConstruction:find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService":     1,
		"serviceConstruction:service.go:newServiceWithRecordAdmissionClasses":                                   1,
		"serviceConstruction:service.go:newServiceWithConstruction":                                             1,
		"serviceConstruction:s6_terminal_construction.go:newServiceWithObservations":                            1,
	}
	if !reflect.DeepEqual(typeRefs, wantTypeRefs) {
		failures = append(failures, "terminal/construction exact type references changed")
	}
	if !reflect.DeepEqual(refs, calls) {
		failures = append(failures, "terminal function or method value escape")
	}
	wantTerminal := `{
 t:=l.bridgeTerminal
 w:=l.degreeWitness
 if w==nil { publication:=t.publisher.BeginPublication() l.clearBridgeTerminalLocked() publication.Complete() return }
 o:=t.owner
 o.mu.Lock()
 defer o.mu.Unlock()
 valid:=t.token!=nil&&t.token==l.token&&o.token==t.token&&o.bridgePublisher==t.publisher
 valid=valid&&w.origin!=nil&&w.origin==l&&w.owner==o&&!w.closed&&validDegreeStatisticKey(w.key)
 if valid { if w.untracked { valid=w.slot==-1&&w.generation==0&&o.unknown>0 } else { valid=w.slot>=0&&w.slot<len(o.active)&&w.generation>0 if valid { row:=&o.active[w.slot] valid=row.life==l&&row.witness==w&&row.generation==w.generation } } }
 publication:=t.publisher.BeginClockedPublication()
 point,err:=publication.Sample()
 clockValid:=err==nil&&point.Elapsed()>=o.last
 if clockValid { o.last=point.Elapsed() }
 if !clockValid||!valid { o.poisoned=true }
 if valid { w.closed=true if w.untracked { o.unknown-- } else { row:=&o.active[w.slot] row.life,row.witness=nil,nil } if clockValid&&!o.poisoned { o.degreeHistoryLocked(w.key,point.Elapsed()) } }
 l.degreeWitness=nil
 l.clearBridgeTerminalLocked()
 publication.Complete()
}`
	wantClear := `{ l.ended=true if l.findNode!=nil { l.findNode.end() } if w:=l.nin;w!=nil { w.phase=inboundTerminal w.binding=identityPinEntryBinding{} w.session=sessionv2.ProvenSession{} w.producer=nil w.group="" w.ready=0 } l.evidence=nil l.entry=nil l.peer=nil }`
	if bodies["find_node_degree_terminal_bridge.go:endBridgeLocked"] != compact(wantTerminal) {
		failures = append(failures, "exact terminal owner/origin/W/sample/retirement window changed")
	}
	if bodies["find_node_degree_terminal_bridge.go:clearBridgeTerminalLocked"] != compact(wantClear) {
		failures = append(failures, "exact mandatory common/F1 cleanup changed")
	}
	return failures
}

func TestOwnedTerminalClosedSourceContract(t *testing.T) {
	if failures := ownedTerminalSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedTerminalGuardRejectsForeignOriginLateBindingAndClockFallback(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"origin", "find_node_degree_terminal_bridge.go", "w.origin == l && ", ""},
		{"backfill", "find_node_degree_terminal_bridge.go", "valid := t.token", "w.origin = l\n\tvalid := t.token"},
		{"foreign-owner", "find_node_degree_terminal_bridge.go", "o := t.owner", "o := w.owner"},
		{"early-ended", "proof_transport_lifetime.go", "if l.bridgeTerminal != nil {", "l.ended = true\n\tif l.bridgeTerminal != nil {"},
		{"accepted-late", "session_secure.go", "token: s.proofToken, bridgeTerminal: s.bridgeTerminal, role: sessionv2.RoleListener", "token: s.proofToken, role: sessionv2.RoleListener"},
		{"clock-fallback", "find_node_degree.go", "return 0, false\n\t}\n\tnow := o.clock.elapsed()", "return o.clock.elapsed(), true\n\t}\n\tnow := o.clock.elapsed()"},
		{"skip-cleanup", "find_node_degree_terminal_bridge.go", "l.findNode.end()", "_ = l.findNode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("owned terminal selfcase anchor changed", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if failures := ownedTerminalSourceErrors(sources); len(failures) == 0 {
				t.Fatal("owned terminal source escape accepted", tc.name)
			}
		})
	}
}
