package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Closed constructor/escape/lifecycle scope. These syntax checks complement
// genuine Accept/Dial/CM/terminal tests; they do not manufacture runtime proof.
func degreeSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"degreeProfile":                    {"find_node_degree.go:<package>": 2, "find_node_degree.go:newDegreeCandidateService": 1, "find_node_degree.go:newDegreeFixtureCandidateService": 1, "find_node_degree.go:newDegreeService": 1},
		"degreeClock":                      {"find_node_degree.go:<package>": 2, "find_node_degree.go:elapsed": 1, "find_node_degree.go:newDegreeService": 2},
		"degreeKey":                        {"find_node_degree.go:<package>": 3, "find_node_degree.go:degreeKeyLess": 1, "find_node_degree.go:degreeHistoryLocked": 1, "find_node_degree.go:publishDegreeTransport": 1},
		"degreeHistoryRow":                 {"find_node_degree.go:<package>": 2, "find_node_degree.go:degreeHistoryLocked": 3},
		"degreeActiveRow":                  {"find_node_degree.go:<package>": 2, "find_node_degree.go:newDegreeService": 1},
		"degreeWitness":                    {"find_node_degree.go:<package>": 2, "proof_transport_lifetime.go:<package>": 2, "find_node_degree.go:publishDegreeTransport": 3, "find_node_degree.go:degreeEndLocked": 2},
		"degreeOwner":                      {"find_node_degree.go:<package>": 2, "service.go:<package>": 1, "find_node_degree.go:newDegreeService": 1, "find_node_degree.go:degreeNowLocked": 1, "find_node_degree.go:degreePoison": 1, "find_node_degree.go:degreeHistoryLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreeCut":                        {"find_node_degree.go:<package>": 1, "find_node_degree.go:degreeSnapshot": 6},
		"newDegreeCandidateService":        {},
		"newDegreeFixtureCandidateService": {},
		"newDegreeService":                 {"find_node_degree.go:newDegreeCandidateService": 1, "find_node_degree.go:newDegreeFixtureCandidateService": 1},
		"publishDegreeInbound":             {"session_secure.go:openInboundTransport": 1},
		"publishDegreeTransport":           {"session_secure.go:provenTransport": 1, "find_node_degree.go:publishDegreeInbound": 1},
		"degreeEndLocked":                  {"proof_transport_lifetime.go:end": 1},
		"degreeSnapshot":                   {},
		"degreeNowLocked":                  {"find_node_degree.go:publishDegreeTransport": 1, "find_node_degree.go:degreeEndLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreeHistoryLocked":              {"find_node_degree.go:publishDegreeTransport": 1, "find_node_degree.go:degreeEndLocked": 1, "find_node_degree.go:degreeSnapshot": 1},
		"degreePoison":                     {"find_node_degree.go:publishDegreeInbound": 1, "find_node_degree.go:publishDegreeTransport": 3},
		"degreeKeyLess":                    {"find_node_degree.go:degreeHistoryLocked": 1},
	}

	// Narrow unused statistic root and fixed projection references only.
	for name, sites := range map[string]map[string]int{
		"degreeProfile":    {"peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 1},
		"degreeKey":        {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 1, "peer_state_degree_statistic.go:validDegreeStatisticKey": 1},
		"degreeHistoryRow": {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 4},
		"degreeOwner":      {"peer_state_degree_statistic.go:<package>": 1, "peer_state_degree_statistic.go:projectDegreeStatisticLocked": 1, "peer_state_degree_statistic.go:sampleDegreeStatisticClockLocked": 1},
		"newDegreeService": {"peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 1},
		"degreeKeyLess":    {"peer_state_degree_statistic.go:insertDegreeStatisticKey": 1},
	} {
		for site, count := range sites {
			expected[name][site] = count
		}
	}
	// Coherent Constructing terminal/P1 slice: exact new type/call sites only.
	for name, sites := range map[string]map[string]int{
		"degreeProfile":       {"find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService": 1, "find_node_bridge_construction.go:newFindNodeTerminalCandidateService": 1, "find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1, "find_node_bridge_construction.go:newFindNodeTerminalStatisticCandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeTerminalConstruction": 1},
		"degreeActiveRow":     {"find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1},
		"degreeOwner":         {"peer_state_owned_degree_statistic.go:<package>": 1, "peer_state_owned_degree_statistic.go:projectOwnedDegreeStatisticLocked": 1, "peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked": 1, "find_node_bridge_construction.go:<package>": 1, "find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1, "find_node_degree_terminal_bridge.go:<package>": 1, "service.go:newServiceWithConstruction": 1},
		"degreeWitness":       {"find_node_degree_terminal_bridge.go:endBridgeLocked": 2},
		"degreeHistoryLocked": {"find_node_degree_terminal_bridge.go:endBridgeLocked": 1},
	} {
		for site, count := range sites {
			expected[name][site] = count
		}
	}
	expected["degreeProfile"]["find_node_bridge_capture.go:<package>"] = 1
	expected["degreeProfile"]["identity_pin_own_dm_runtime.go:NewInactiveOwnDMRuntimeCandidate"] = 1
	expected["degreeHistoryRow"]["peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked"] = 8
	expected["degreeHistoryRow"]["find_node_degree_bridge.go:ownedDegreeProjectionFrontier"] = 1
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["degreeOwner"]["find_node_ready_construction.go:<package>"] = 1
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	fields := map[string][]string{}
	fieldWant := map[string][]string{
		"degreeProfile": {"MaxActive"}, "degreeClock": {"origin", "fixture"},
		"degreeKey": {"outgoing", "group", "identity"}, "degreeHistoryRow": {"key", "last"},
		"degreeActiveRow": {"life", "witness", "generation"}, "degreeWitness": {"origin", "owner", "key", "slot", "generation", "untracked", "closed"},
		"degreeOwner": {"mu", "token", "clock", "profile", "active", "history", "historyN", "unknown", "last", "poisoned", "bridgePublisher"}, "degreeCut": {"at", "outgoing", "incoming", "window"},
	}
	made := map[string]map[string]int{}
	for _, name := range []string{"degreeOwner", "degreeWitness", "degreeClock"} {
		made[name] = map[string]int{}
	}
	var failures []string
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n != declaration {
						if refs, ok := actual[n.Name]; ok {
							refs[site]++
						}
					}
				case *ast.SelectorExpr:
					if _, tracked := expected[n.Sel.Name]; tracked && (strings.HasPrefix(n.Sel.Name, "publishDegree") || n.Sel.Name == "degreeEndLocked" || n.Sel.Name == "degreeSnapshot" || n.Sel.Name == "degreeNowLocked" || n.Sel.Name == "degreeHistoryLocked" || n.Sel.Name == "degreePoison") && !callees[n] {
						failures = append(failures, "degree method value escape")
					}
				case *ast.TypeSpec:
					if _, tracked := fieldWant[n.Name.Name]; tracked {
						shape, ok := n.Type.(*ast.StructType)
						if !ok {
							failures = append(failures, "degree type escape")
							return true
						}
						for _, field := range shape.Fields.List {
							for _, name := range field.Names {
								fields[n.Name.Name] = append(fields[n.Name.Name], name.Name)
								if n.Name.Name == "degreeWitness" && name.Name == "origin" {
									pointer, ok := field.Type.(*ast.StarExpr)
									if !ok {
										failures = append(failures, "degree witness origin type changed")
									} else if named, ok := pointer.X.(*ast.Ident); !ok || named.Name != "proofTransportLifetime" {
										failures = append(failures, "degree witness origin type changed")
									}
								}
							}
						}
					}
				case *ast.CompositeLit:
					if typ, ok := n.Type.(*ast.Ident); ok {
						if refs, tracked := made[typ.Name]; tracked {
							refs[site]++
						}
						if typ.Name == "degreeWitness" {
							origins := 0
							for _, element := range n.Elts {
								pair, ok := element.(*ast.KeyValueExpr)
								if !ok {
									continue
								}
								key, ok := pair.Key.(*ast.Ident)
								if !ok || key.Name != "origin" {
									continue
								}
								origins++
								value, ok := pair.Value.(*ast.Ident)
								if site != "find_node_degree.go:publishDegreeTransport" || !ok || value.Name != "l" {
									failures = append(failures, "degree witness origin mint changed")
								}
							}
							if origins != 1 {
								failures = append(failures, "degree witness origin mint changed")
							}
						}
					}
				case *ast.GoStmt, *ast.SendStmt:
					if path == "find_node_degree.go" {
						failures = append(failures, "degree independent worker")
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok {
						if _, tracked := expected[id.Name]; tracked && !callees[id] {
							failures = append(failures, "degree function value escape")
						}
						if id.Name == "new" && len(n.Args) == 1 {
							if typ, ok := n.Args[0].(*ast.Ident); ok {
								if _, tracked := made[typ.Name]; tracked {
									failures = append(failures, "degree new authority")
								}
							}
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "degree" && site != "find_node_degree.go:newDegreeService" {
							failures = append(failures, "degree late owner write")
						}
						if sel, ok := lhs.(*ast.SelectorExpr); ok && path == "find_node_degree.go" && sel.Sel.Name == "origin" {
							failures = append(failures, "degree origin late write")
						}
						if sel, ok := lhs.(*ast.SelectorExpr); ok && path == "find_node_degree.go" && (sel.Sel.Name == "origin" || sel.Sel.Name == "clock" || sel.Sel.Name == "fixture") {
							failures = append(failures, "degree clock field write")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "degree exact reference inventory changed")
	}
	if !reflect.DeepEqual(fields, fieldWant) {
		failures = append(failures, "degree fixed field inventory changed")
	}
	if !reflect.DeepEqual(made, map[string]map[string]int{"degreeOwner": {"find_node_degree.go:newDegreeService": 1, "find_node_bridge_construction.go:newFindNodeTerminalConstruction": 1}, "degreeWitness": {"find_node_degree.go:publishDegreeTransport": 1}, "degreeClock": {"find_node_degree.go:newDegreeService": 2}}) {
		failures = append(failures, "degree constructor inventory changed")
	}
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	degreeRaw := compact(string(sources["find_node_degree.go"]))
	if strings.Count(degreeRaw, "clock:=degreeClock{origin:time.Now()}") != 1 || strings.Count(degreeRaw, "iffixture!=nil{clock=degreeClock{fixture:fixture}}") != 1 || strings.Count(degreeRaw, "returnnewDegreeService(cfg,id,bus,profile,nil)") != 1 || strings.Count(degreeRaw, "returnnewDegreeService(cfg,id,bus,profile,fixture)") != 1 {
		failures = append(failures, "degree owned clock provenance changed")
	}
	common := compact(string(sources["proof_transport_lifetime.go"]))
	end := strings.Index(common, "l.ended=true")
	observe := strings.Index(common, "l.degreeEndLocked()")
	clear := strings.Index(common, "l.evidence=nil")
	if end < 0 || observe <= end || clear <= observe {
		failures = append(failures, "degree terminal not before clear")
	}
	source := compact(string(sources["session_secure.go"]))
	if strings.Count(source, "s.publishDegreeInbound(conn)") != 1 || strings.Count(source, "s.publishDegreeTransport(transport.common)") != 1 {
		failures = append(failures, "degree P1 hooks incomplete")
	}
	failures = append(failures, degreeOriginReferenceErrors(sources)...)
	return failures
}
func TestDegreeClosedOriginalWitnessAndDefaultRoots(t *testing.T) {
	if failures := degreeSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestDegreeGuardRejectsLateSetterAliasAndTerminalRemoval(t *testing.T) {
	for _, kind := range []string{"alias", "setter", "terminal"} {
		t.Run(kind, func(t *testing.T) {
			sources := ninN1Sources(t)
			switch kind {
			case "alias":
				sources["degree_escape.go"] = []byte("package node\nvar leakedDegree = (*degreeOwner).degreeSnapshot\n")
			case "setter":
				sources["degree_escape.go"] = []byte("package node\nfunc lateDegree(s *Service,o *degreeOwner){s.degree=o}\n")
			case "terminal":
				sources["proof_transport_lifetime.go"] = []byte(strings.Replace(string(sources["proof_transport_lifetime.go"]), "l.degreeEndLocked()", "", 1))
			}
			if len(degreeSourceErrors(sources)) == 0 {
				t.Fatal("degree source escape accepted", kind)
			}
		})
	}
}

// H7 closes only the immutable field and authentic mint. The full terminal
// bridge remains separate; these syntax counterexamples create no live proof.
func TestDegreeWitnessOriginGuardRejectsOmissionNilAndLateRewrite(t *testing.T) {
	original := ninN1Sources(t)
	if failures := degreeSourceErrors(original); len(failures) != 0 {
		t.Fatal("degree origin baseline source contract", failures)
	}
	for _, kind := range []string{"omitted", "nil", "late-rewrite"} {
		t.Run(kind, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources["find_node_degree.go"])
			switch kind {
			case "omitted":
				raw = strings.Replace(raw, "degreeWitness{origin: l,", "degreeWitness{", 1)
			case "nil":
				raw = strings.Replace(raw, "degreeWitness{origin: l,", "degreeWitness{origin: nil,", 1)
			case "late-rewrite":
				raw = strings.Replace(raw, "l.degreeWitness = w", "w.origin = l\n\tl.degreeWitness = w", 1)
			}
			sources["find_node_degree.go"] = []byte(raw)
			if len(degreeSourceErrors(sources)) == 0 {
				t.Fatal("degree witness origin escape accepted", kind)
			}
		})
	}
}

// Exact static Node origin identifier/role/line closure, including unrelated
// clocks and routing provenance. Expected rows are literal reviewed inputs.
// Two exact terminal origin predicate reads are added; all prior97 rows remain pinned.
// This is not whole-program unsafe/reflection or dynamic alias analysis.
func degreeOriginReferenceErrors(sources map[string][]byte) []string {
	expected := map[string]int{
		"address_identities.go:<package>|field|origindomain.PeerIdentityOrigin":                                                                                            1,
		"address_identities.go:claimedAddressIdentity|key|returnaddressIdentity{identity:id,origin:domain.PeerIdentityClaimed}":                                            1,
		"address_identities.go:record|selector/read|ifpresent&&current.origin==domain.PeerIdentityProvenV2&&next.origin!=domain.PeerIdentityProvenV2{":                     2,
		"find_node_connection.go:<package>|field|origintime.Time":                                                                                                          1,
		"find_node_connection.go:findNodePrioritySnapshot|selector/read|at:=time.Since(adapter.origin)":                                                                    1,
		"find_node_connection.go:newFindNodeCandidateService|key|s.findNode=&findNodeAdapter{origin:time.Now()}":                                                           1,
		"find_node_connection.go:newFindNodeInboundCandidateService|key|s.findNode=&findNodeAdapter{origin:time.Now()}":                                                    1,
		"find_node_connection.go:publishFindNodeLocked|selector/read|elapsed:=time.Since(s.findNode.origin)":                                                               1,
		"find_node_degree.go:<package>|field|origin*proofTransportLifetime":                                                                                                1,
		"find_node_degree.go:<package>|field|origintime.Time":                                                                                                              1,
		"find_node_degree.go:elapsed|selector/read|returntime.Since(c.origin)":                                                                                             1,
		"find_node_degree.go:newDegreeService|key|clock:=degreeClock{origin:time.Now()}":                                                                                   1,
		"find_node_degree.go:publishDegreeTransport|key|w:=&degreeWitness{origin:l,owner:o,key:key,slot:-1}":                                                               1,
		"find_node_degree_terminal_bridge.go:endBridgeLocked|selector/read|valid=valid&&w.origin!=nil&&w.origin==l&&w.owner==o&&!w.closed&&validDegreeStatisticKey(w.key)": 2,
		"peer_state_owned_degree_statistic.go:projectOwnedDegreeBasisLocked|selector/read|ifw==nil||w.origin!=row.life||w.owner!=o||w.closed||w.untracked||w.slot!=i||w.generation==0||w.generation!=row.generation||row.life.token!=o.token||!validDegreeStatisticKey(w.key){": 1,
		"global_messages.go:PublishGlobalMessage|key|msg:=incomingMessage{origin:messageOriginOwnGlobal,ID:id,Topic:string(domain.GlobalMessageTopic),Sender:s.identity.Address,Recipient:\"*\",Flag:protocol.MessageFlagImmutable,Body:string(body),CreatedAt:at}":             1,
		"identity_intent_store.go:addMemory|selector/read|ifaction.origin==identityLookupOriginNone||row.action==action{":                                                                                                                                                       1,
		"identity_pin_outgoing_provenance.go:<package>|field|origin*r2aDispatch":                                                                                                                                                                                                                                                                                       1,
		"identity_pin_outgoing_provenance.go:captureR2aFirstDirectedItem|selector/read|d:=route.origin":                                                                                                                                                                                                                                                                1,
		"identity_pin_outgoing_provenance.go:snapshotR2aDirectedSession|selector/assignment-lhs|route.origin.r2ResolvedHop=expected":                                                                                                                                                                                                                                   1,
		"identity_pin_outgoing_provenance.go:snapshotR2aDirectedSession|selector/read|expected:=route.origin.expected.sealed":                                                                                                                                                                                                                                          1,
		"identity_pin_outgoing_provenance.go:snapshotR2aDirectedSession|selector/read|ifroute==nil||route.origin==nil||strings.HasPrefix(string(route.address),\"inbound:\"){":                                                                                                                                                                                         1,
		"identity_pin_relation.go:stageClassifiedRelationRecordLocked|field|func(s*secureSessionStore)stageClassifiedRelationRecordLocked(observedpinOwnerObservation,origindomain.IdentityPinOrigin,referencedomain.IdentityPinEvidenceReference)(relationPinStage,error){":                                                                                           1,
		"identity_pin_relation.go:stageClassifiedRelationRecordLocked|reference/read|iforigin!=domain.IdentityPinOriginUserLookup&&origin!=domain.IdentityPinOriginOutgoingSend&&origin!=domain.IdentityPinOriginProvenNextHop{":                                                                                                                                       3,
		"identity_pin_relation.go:stageClassifiedRelationRecordLocked|reference/read|next.Relation=domain.IdentityPinAllocation{State:domain.IdentityPinAllocationEligible,Origin:origin,At:s.now(),Reference:reference}":                                                                                                                                              1,
		"identity_record_admission.go:<package>|field|typerelationAdmissionLifetimestruct{originrelationObservedTime}":                                                                                                                                                                                                                                                 1,
		"identity_record_admission.go:live|selector/read|returnnow.elapsed>=l.origin.elapsed&&now.elapsed-l.origin.elapsed<=identityLookupTaskLifetime&&!now.wall.Round(0).After(l.origin.wall.Round(0).Add(identityLookupTaskLifetime))":                                                                                                                              3,
		"identity_record_admission_own_dm.go:bindOwnDMResidence|field|func(s*Service)bindOwnDMResidence(preparedr2aPreparedOwnDM,origin*ownDMResidenceOrigin)r2aPreparedOwnDM{":                                                                                                                                                                                        1,
		"identity_record_admission_own_dm.go:bindOwnDMResidence|reference/read|iforigin==nil||origin.owner.service!=s||prepared.sealed==nil||prepared.sealed.owner!=s{":                                                                                                                                                                                                2,
		"identity_record_admission_own_dm.go:bindOwnDMResidence|reference/read|value:=*origin":                                                                                                                                                                                                                                                                         1,
		"identity_record_admission_own_dm.go:registerOwnDMRecordAdmission|reference/assignment-lhs|origin:=stamp.prepared.r2Residence":                                                                                                                                                                                                                                 1,
		"identity_record_admission_own_dm.go:registerOwnDMRecordAdmission|reference/read|if!ok||!origin.liveLocked(r,now)||ctx.Err()!=nil||!s.r2aCurrentLocked(stamp)||stamp.consumed||stamp.candidate==nil||!stamp.candidate.offered||s.deliveryFrozenLocked(stamp.prepared.message.ID)||store.pinMetadataMutationRefusalLocked()!=nil||!c.ownersMatchLocked(store){": 1,
		"identity_record_admission_own_dm.go:registerOwnDMRecordAdmission|reference/read|r:=origin.owner.resolver":                                                                                                                                                                                                                                                     1,
		"identity_record_admission_own_dm.go:stageOwnDMRelationRecordLocked|reference/assignment-lhs|origin:=domain.IdentityPinOriginOutgoingSend":                                                                                                                                                                                                                     1,
		"identity_record_admission_own_dm.go:stageOwnDMRelationRecordLocked|reference/assignment-lhs|origin=domain.IdentityPinOriginProvenNextHop":                                                                                                                                                                                                                     1,
		"identity_record_admission_own_dm.go:stageOwnDMRelationRecordLocked|reference/read|returns.stageClassifiedRelationRecordLocked(observed,origin,domain.IdentityPinEvidenceReference(hex.EncodeToString(digest[:])))":                                                                                                                                            1,
		"identity_record_admission_relation.go:reserveRelationAttemptLocked|selector/read|if!a.admitsR()||a.started==nil||a.closing||entry.action.origin!=identityLookupOriginWindowUser||entry.owner.recordLifetime==nil{":                                                                                                                                            1,
		"identity_relation_origin.go:<package>|field|originidentityLookupOrigin":                                                                                                                                                                                                                                                                                       1,
		"identity_relation_origin.go:cancelUserAction|selector/read|returnkey.reason==identityIntentReason{Type:identityIntentReasonUIChat}&&((row.action.generation==action.generation&&row.action.origin==identityLookupOriginWindowUser)||clearedRuntimeAction)":                                                                                                    1,
		"identity_relation_origin.go:nextActionLocked|key|returnidentityLookupActionStamp{origin:identityLookupOriginWindowUser,generation:r.actionGen},nil":                                                                                                                                                                                                           1,
		"identity_relation_origin.go:revokeActionLocked|selector/read|ifentry.dst!=target||entry.action.generation!=generation||entry.action.origin==identityLookupOriginNone{":                                                                                                                                                                                        1,
		"identity_resolver.go:HandleAnswer|selector/read|if!current.revoked&&current.action.origin==identityLookupOriginWindowUser&&current.action==action&&recordRequiresSessionV2(body.MinSession){":                                                                                                                                                                 1,
		"identity_resolver.go:cancelReasons|selector/read|ifrevoked.origin!=identityLookupOriginNone{":                                                                                                                                                                                                                                                                 1,
		"identity_resolver.go:cancelReasons|selector/read|ifrow.action.origin!=identityLookupOriginNone{":                                                                                                                                                                                                                                                              1,
		"identity_resolver.go:startResolution|key|lifetime=&relationAdmissionLifetime{origin:sample}":                                                                                                                                                                                                                                                                  1,
		"identity_resolver.go:startResolution|selector/read|ifaction.origin==identityLookupOriginNone{":                                                                                                                                                                                                                                                                1,
		"network_consumer.go:logUnregisteredWriteRaw|field|funclogUnregisteredWriteRaw(addr,originstring){":                                                                                                                                                                                                                                                            1,
		"network_consumer.go:logUnregisteredWriteRaw|reference/read|Str(\"origin\",origin).":                                                                                                                                                                                                                                                                           1,
		"network_consumer.go:sendSessionFrameViaNetwork|reference/assignment-lhs|origin:=\"sendSessionFrameViaNetwork.carveout\"":                                                                                                                                                                                                                                      1,
		"network_consumer.go:sendSessionFrameViaNetwork|reference/assignment-lhs|origin=\"sendSessionFrameViaNetwork.carveout.marshal_fallback\"":                                                                                                                                                                                                                      1,
		"network_consumer.go:sendSessionFrameViaNetwork|reference/read|logUnregisteredWrite(addr,frame,origin)":                                                                                                                                                                                                                                                        1,
		"peer_management.go:buildPeerEntriesLocked|selector/read|IdentityOrigin:s.peerIDs[peer.Address].origin,":                                                                                                                                                                                                                                                       1,
		"peer_management.go:buildPeerEntriesLocked|selector/read|entry.Identity,entry.IdentityOrigin=recorded.identity,recorded.origin":                                                                                                                                                                                                                                1,
		"peer_management.go:learnPeerFromFrame|field|func(s*Service)learnPeerFromFrame(observedAddrstring,frameprotocol.Frame,origindomain.PeerIdentityOrigin){":                                                                                                                                                                                                       1,
		"peer_management.go:learnPeerFromFrame|key|s.addPeerID(normalizedAddr,addressIdentity{identity:domain.PeerIdentityFromWire(frame.Address),origin:origin})":                                                                                                                                                                                                     1,
		"peer_management.go:learnPeerFromFrame|reference/read|s.addPeerID(normalizedAddr,addressIdentity{identity:domain.PeerIdentityFromWire(frame.Address),origin:origin})":                                                                                                                                                                                          1,
		"peer_management.go:notePersistedIdentityLocked|selector/read|persisted.Identity,persisted.IdentityOrigin=recorded.identity,recorded.origin":                                                                                                                                                                                                                   1,
		"peer_management.go:resolveHealthAddress|reference/assignment-lhs|iforigin,ok:=s.dialOrigin[address];ok{":                                                                                                                                                                                                                                                      1,
		"peer_management.go:resolveHealthAddress|reference/read|returnorigin":                                                                                                                                                                                                                                                                                          1,
		"peer_sessions.go:onCMSessionEstablished|key|s.addPeerID(healthKey,addressIdentity{identity:session.peerIdentity,origin:identityOriginOf(proven)})":                                                                                                                                                                                                            1,
		"peer_sessions.go:openPeerSession|key|s.addPeerID(healthKey,addressIdentity{identity:session.peerIdentity,origin:identityOriginOf(proven)})":                                                                                                                                                                                                                   1,
		"peer_state_degree_statistic.go:sampleDegreeStatisticClockLocked|selector/read|ifo==nil||o.poisoned||o.token==nil||o.clock.fixture!=nil||o.clock.origin.IsZero(){":                                                                                                                                                                                             1,
		"peer_state_degree_statistic.go:sampleDegreeStatisticClockLocked|selector/read|q:=sample.Sub(o.clock.origin)":                                                                                                                                                                                                                                                  1,
		"relay.go:sendTableRoutedRelayMessageWithR2a|field|func(s*Service)sendTableRoutedRelayMessageWithR2a(addressdomain.PeerAddress,msgprotocol.Envelope,dispatchedAttime.Time,origin*r2aDirectedDispatch)relaySendOutcome{":                                                                                                                                        1,
		"relay.go:sendTableRoutedRelayMessageWithR2a|reference/read|candidate:=s.captureR2aFirstDirectedItem(origin,session,item)":                                                                                                                                                                                                                                     1,
		"relay.go:sendTableRoutedRelayMessageWithR2a|reference/read|session:=s.snapshotR2aDirectedSession(origin)":                                                                                                                                                                                                                                                     1,
		"routing_relay.go:sendTableDirectedRelayWithR2a|field|func(s*Service)sendTableDirectedRelayWithR2a(ctxcontext.Context,msgprotocol.Envelope,nextHopIdentitydomain.PeerIdentity,validatedAddressdomain.PeerAddress,nextHopHopsint,dispatchedAttime.Time,origin*r2aDispatch)relaySendOutcome{":                                                                    1,
		"routing_relay.go:sendTableDirectedRelayWithR2a|key|selected=&r2aDirectedDispatch{origin:origin,nextHop:nextHopIdentity,address:address,hops:nextHopHops}":                                                                                                                                                                                                     1,
		"routing_relay.go:sendTableDirectedRelayWithR2a|reference/read|iforigin!=nil&&origin.early{":                                                                                                                                                                                                                                                                   2,
		"routing_relay.go:sendTableDirectedRelayWithR2a|reference/read|selected=&r2aDirectedDispatch{origin:origin,nextHop:nextHopIdentity,address:address,hops:nextHopHops}":                                                                                                                                                                                          1,
		"routing_relay.go:sendTableRoutedRelayToAddressWithR2a|field|func(s*Service)sendTableRoutedRelayToAddressWithR2a(ctxcontext.Context,addressdomain.PeerAddress,msgprotocol.Envelope,dispatchedAttime.Time,origin*r2aDirectedDispatch)relaySendOutcome{":                                                                                                         1,
		"routing_relay.go:sendTableRoutedRelayToAddressWithR2a|reference/read|iforigin==nil{":                                                                                                                                                                                                                                                                          1,
		"routing_relay.go:sendTableRoutedRelayToAddressWithR2a|reference/read|returns.sendTableRoutedRelayMessageWithR2a(address,msg,dispatchedAt,origin)":                                                                                                                                                                                                             1,
		"service.go:<package>|field|originmessageOrigin": 1,
		"service.go:applyTrustedContactOrigin|field|func(s*Service)applyTrustedContactOrigin(ctxcontext.Context,address,pubKey,boxKey,boxSig,sourcestring,origincontactApplyOrigin)(bool,localContactRowStamp,error){": 1,
		"service.go:applyTrustedContactOrigin|reference/read|},origin)": 1,
		"service.go:completeInboundAuth|key|s.addPeerID(addr,addressIdentity{identity:domain.PeerIdentityFromWire(verified.Hello.Address),origin:identityOriginOf(proven)})":                                         1,
		"service.go:learnIdentityFromWelcome|field|func(s*Service)learnIdentityFromWelcome(frameprotocol.Frame,dialAddressdomain.PeerAddress,origindomain.PeerIdentityOrigin){":                                      1,
		"service.go:learnIdentityFromWelcome|key|s.addPeerID(normalizedAddr,addressIdentity{identity:domain.PeerIdentityFromWire(frame.Address),origin:origin})":                                                     1,
		"service.go:learnIdentityFromWelcome|reference/read|s.addPeerID(normalizedAddr,addressIdentity{identity:domain.PeerIdentityFromWire(frame.Address),origin:origin})":                                          1,
		"service.go:logUnregisteredWrite|field|funclogUnregisteredWrite(addrstring,frameprotocol.Frame,originstring){":                                                                                               1,
		"service.go:logUnregisteredWrite|reference/read|Str(\"origin\",origin).":                                                                                                                                     1,
		"service.go:newServiceWithConstruction|key|restoredPeerIDs[addr]=addressIdentity{identity:entry.Identity,origin:restoredIdentityOrigin(entry.IdentityOrigin)}":                                               1,
		"service.go:shouldRouteStoredMessage|selector/read|ifmsg.origin==messageOriginOwnGlobal&&msg.Topic==string(domain.GlobalMessageTopic)&&msg.Sender==s.identity.Address&&msg.Recipient==\"*\"&&msg.Via==\"\"{": 1,
		"trust.go:rememberContactOrigin|field|func(s*trustStore)rememberContactOrigin(ctxcontext.Context,contacttrustedContact,origincontactApplyOrigin)(storedbool,rowStamplocalContactRowStamp,errerror){":         1,
		"trust.go:rememberContactOrigin|reference/read|rowStamp=s.bindLocalContactRowLocked(&contact,origin)":                                                                                                        1,
		"trust.go:rememberContactOrigin|reference/read|rowStamp=s.bindLocalContactRowLocked(&existing,origin)":                                                                                                       1,
		"trust_local_contact.go:bindLocalContactRowLocked|field|func(s*trustStore)bindLocalContactRowLocked(row*trustedContact,origincontactApplyOrigin)localContactRowStamp{":                                       1,
		"trust_local_contact.go:bindLocalContactRowLocked|reference/read|iforigin!=contactApplyExplicitLocal{":                                                                                                       1,
		"v2_identity_claims.go:forgetClaimedAddressesLocked|key|ifpersisted!=nil&&claimedOnly(address,addressIdentity{identity:persisted.Identity,origin:restoredIdentityOrigin(persisted.IdentityOrigin)}){":        1,
		"v2_identity_claims.go:forgetClaimedAddressesLocked|selector/read|returnrecorded.identity==id&&recorded.origin!=domain.PeerIdentityProvenV2&&!bound.covers(address)":                                         1,
		"v2_identity_claims.go:recordAddressIdentityLocked|selector/read|ifrecorded.origin!=domain.PeerIdentityProvenV2&&s.identityPinnedV2(recorded.identity){":                                                     1,
	}
	actual := map[string]int{}
	var failures []string
	for path, raw := range sources {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		lines := strings.Split(string(raw), "\n")
		for _, decl := range file.Decls {
			site := path + ":<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
			}
			var stack []ast.Node
			ast.Inspect(decl, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				id, isOrigin := n.(*ast.Ident)
				if isOrigin && id.Name == "origin" {
					var parent ast.Node
					if len(stack) != 0 {
						parent = stack[len(stack)-1]
					}
					role := "reference/read"
					switch parent := parent.(type) {
					case *ast.SelectorExpr:
						if parent.Sel == id {
							role = "selector/read"
						}
					case *ast.KeyValueExpr:
						if parent.Key == id {
							role = "key"
						}
					case *ast.Field:
						for _, name := range parent.Names {
							if name == id {
								role = "field"
							}
						}
					}
					if role == "selector/read" || role == "reference/read" {
						for _, ancestor := range stack {
							if assign, ok := ancestor.(*ast.AssignStmt); ok {
								for _, lhs := range assign.Lhs {
									ast.Inspect(lhs, func(part ast.Node) bool {
										if part == id {
											role = strings.TrimSuffix(role, "/read") + "/assignment-lhs"
										}
										return true
									})
								}
							}
						}
					}
					line := fset.Position(id.Pos()).Line
					actual[site+"|"+role+"|"+strings.Join(strings.Fields(lines[line-1]), "")]++
				}
				if address, ok := n.(*ast.UnaryExpr); ok && address.Op == token.AND && degreeOriginStorageExpr(address.X) {
					failures = append(failures, "degree origin address alias")
				}
				stack = append(stack, n)
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "degree all-Node origin reference closure changed")
	}
	return failures
}
func degreeOriginStorageExpr(expr ast.Expr) bool {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name == "origin"
	case *ast.SelectorExpr:
		return expr.Sel.Name == "origin" || degreeOriginStorageExpr(expr.X)
	case *ast.ParenExpr:
		return degreeOriginStorageExpr(expr.X)
	case *ast.StarExpr:
		return degreeOriginStorageExpr(expr.X)
	case *ast.IndexExpr:
		return degreeOriginStorageExpr(expr.X)
	}
	return false
}
func TestDegreeWitnessOriginGuardRejectsCrossFileWriteAndAliases(t *testing.T) {
	if failures := degreeSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal("degree all-Node origin baseline source contract", failures)
	}
	for _, tc := range []struct{ name, source string }{
		{"other-file-write", "package node\nfunc lateOrigin(s *Service){ s.degree.active[0].witness.origin = nil }\n"},
		{"address-alias", "package node\nfunc lateOrigin(s *Service){ p := &s.degree.active[0].witness.origin; *p = nil }\n"},
		{"receiver-alias", "package node\nfunc lateOrigin(s *Service){ w := s.degree.active[0].witness; w.origin = nil }\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			sources["degree_escape.go"] = []byte(tc.source)
			if len(degreeSourceErrors(sources)) == 0 {
				t.Fatal("cross-file degree origin escape accepted", tc.name)
			}
		})
	}
}
