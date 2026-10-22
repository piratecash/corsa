package node

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Exact finite references and complete bounded W windows. Existing callbacks,
// snapshots, session cache, saves, random IO and future owner waits are outside.
func trustPublicationSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"trustPublisher":              {"trust.go:<package>": 1, "trust.go:loadTrustStoreWithPublisher": 1, "trust.go:beginTrustPublication": 2},
		"loadTrustStoreWithPublisher": {"trust.go:loadTrustStoreWithFiles": 1, "find_node_bridge_construction.go:loadTrustStoreForConstruction": 1},
		"beginTrustPublication":       {"trust.go:recordLastOnlineAt": 1, "trust.go:rememberContactOrigin": 2, "trust.go:forget": 1, "trust.go:mergeRecord": 1, "trust_local_contact.go:stageBoundLocalContact": 1, "trust_local_contact.go:confirmLocalContacts": 1, "trust_local_contact.go:captureLocalContactProof": 1, "identity_pin_contact.go:promote": 1},
	}
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["trustPublisher"]["find_node_ready_construction.go:bindings"] = 1
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	api := map[string]int{}
	qualified := map[string]int{}
	contactRefs := map[string]int{}
	mapWrites := map[string]int{}
	trustLiterals := map[string]int{}
	bodies := map[string]string{}
	windows := map[string][]string{}
	constructorUses := 0
	var constructorDecl *ast.FuncDecl
	var failures []string
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			failures = append(failures, "trust publication parse: "+name)
			continue
		}
		inspect := func(n ast.Node, site string) {
			ast.Inspect(n, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if sites, watched := actual[id.Name]; watched {
						sites[name+":"+site]++
					}
				}
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if sel.Sel.Name == "contacts" {
						contactRefs[name+":"+site]++
					}
					if sel.Sel.Name == "BeginPublication" || sel.Sel.Name == "BeginClockedPublication" || sel.Sel.Name == "SealConstruction" {
						api[name+":"+site+":"+sel.Sel.Name]++
					}
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "findnodebridge" && name == "trust.go" {
						qualified[site+":"+sel.Sel.Name]++
					}
				}
				if lit, ok := n.(*ast.CompositeLit); ok {
					if typ, ok := lit.Type.(*ast.Ident); ok && typ.Name == "trustStore" {
						trustLiterals[name+":"+site]++
					}
				}
				contactMap := func(n ast.Expr) bool { sel, ok := n.(*ast.SelectorExpr); return ok && sel.Sel.Name == "contacts" }
				if assignment, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assignment.Lhs {
						if indexed, ok := lhs.(*ast.IndexExpr); ok {
							if contactMap(indexed.X) {
								mapWrites[name+":"+site]++
							}
						} else if contactMap(lhs) {
							mapWrites[name+":"+site]++
						}
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "delete" && len(call.Args) == 2 && contactMap(call.Args[0]) {
						mapWrites[name+":"+site]++
					}
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			f, ok := decl.(*ast.FuncDecl)
			if !ok {
				inspect(decl, "<package>")
				continue
			}
			inspect(f.Type, f.Name.Name)
			if f.Recv != nil {
				inspect(f.Recv, f.Name.Name)
			}
			inspect(f.Body, f.Name.Name)
			if f.Body == nil {
				continue
			}
			site := name + ":" + f.Name.Name
			bodies[site] = pinPublicationPrinted(f.Body)
			if site == "trust.go:loadTrustStoreWithPublisher" {
				constructorDecl = f
				ast.Inspect(f.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "publisher" {
						constructorUses++
					}
					return true
				})
			}
			ast.Inspect(f.Body, func(n ast.Node) bool {
				block, ok := n.(*ast.BlockStmt)
				if !ok {
					return true
				}
				for i, statement := range block.List {
					first := pinPublicationPrinted(statement)
					if first != "publication := s.beginTrustPublication()" && first != "publication := s.trust.beginTrustPublication()" {
						continue
					}
					parts := []string{first}
					complete := false
					for _, tail := range block.List[i+1:] {
						printed := pinPublicationPrinted(tail)
						parts = append(parts, printed)
						if printed == "if publication != nil { publication.Complete() }" {
							complete = true
							break
						}
					}
					if !complete {
						failures = append(failures, "unterminated trust publication: "+site)
					}
					windows[site] = append(windows[site], strings.Join(parts, " "))
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "trust constructor or writer reference escape")
	}
	// Narrow current contact selectors include snapshot copies; typed/unsafe alias closure is not claimed.
	wantContactRefs := map[string]int{"find_node_connection.go:captureFindNodePriorityClasses": 1, "find_node_connection.go:collectOwnedFindNodeRawClasses": 1, "identity_pin_contact.go:AddLocalContact": 1, "identity_pin_contact.go:promote": 3, "identity_pin_contact.go:capture": 1, "identity_pin_contact_reconciliation.go:captureReconciliation": 2, "identity_pin_contact_record.go:observeContactRecord": 1, "identity_pin_contact_record.go:stageContactRecordWithAuthority": 1, "identity_record_admission_own_dm.go:stageOwnDMRelationAdmission": 1, "identity_record_admission_relation.go:stageRelationAdmission": 1, "trust.go:loadTrustStoreWithPublisher": 5, "trust.go:recordLastOnlineAt": 2, "trust.go:rememberContactOrigin": 3, "trust.go:isPersistentRecordAddressLocked": 1, "trust.go:persistentUsage": 1, "trust.go:forget": 2, "trust.go:mergeRecord": 2, "trust.go:contactBoxKey": 1, "trust.go:isTrustedContact": 1, "trust.go:trustedContacts": 2, "trust.go:snapshotLocked": 2, "trust.go:encodeTrustSnapshot": 1, "trust_local_contact.go:stageBoundLocalContact": 2, "trust_local_contact.go:confirmLocalContacts": 3, "trust_local_contact.go:localContactStatus": 1, "trust_local_contact.go:captureLocalContactProof": 2}
	wantWrites := map[string]int{"trust.go:loadTrustStoreWithPublisher": 3, "trust.go:recordLastOnlineAt": 1, "trust.go:rememberContactOrigin": 2, "trust.go:forget": 1, "trust.go:mergeRecord": 1, "trust_local_contact.go:stageBoundLocalContact": 1, "trust_local_contact.go:confirmLocalContacts": 1, "trust_local_contact.go:captureLocalContactProof": 1, "identity_pin_contact.go:promote": 1}
	if !reflect.DeepEqual(contactRefs, wantContactRefs) || !reflect.DeepEqual(mapWrites, wantWrites) || !reflect.DeepEqual(trustLiterals, map[string]int{"trust.go:loadTrustStoreWithPublisher": 1}) {
		failures = append(failures, "trust map writer, alias reference or constructor set changed")
	}

	if !reflect.DeepEqual(api, map[string]int{"find_node_ready_construction.go:prepareFindNodeReadyConstruction:BeginClockedPublication": 1, "find_node_ready_construction.go:finalize:SealConstruction": 1, "conn_registry.go:beginRegistryPublication:BeginPublication": 1, "find_node_degree_bridge.go:captureBridgeDegreeProjection:BeginPublication": 1, "find_node_connection.go:publishFindNodeLocked:BeginClockedPublication": 1, "secure_session_store.go:beginPinPublication:BeginPublication": 1, "trust.go:beginTrustPublication:BeginPublication": 1, "find_node_degree.go:degreePoison:BeginPublication": 1, "find_node_degree.go:publishDegreeTransport:BeginClockedPublication": 1, "find_node_degree_terminal_bridge.go:endBridgeLocked:BeginPublication": 1, "find_node_degree_terminal_bridge.go:endBridgeLocked:BeginClockedPublication": 1, "peer_state_owned_degree_statistic.go:installOwnedDegreeStatistic:BeginClockedPublication": 1, "peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush:BeginClockedPublication": 1}) {
		failures = append(failures, "trust slice permits only two nonclock publication helpers")
	}
	wantQualified := map[string]int{"<package>:Publisher": 1, "loadTrustStoreWithFiles:Publisher": 1, "loadTrustStoreWithPublisher:Publisher": 1, "beginTrustPublication:Publication": 1, "beginTrustPublication:Publisher": 1}
	if !reflect.DeepEqual(qualified, wantQualified) || constructorUses != 1 {
		failures = append(failures, "concrete trust Publisher escaped initial dependency")
	}
	if bodies["trust.go:loadTrustStoreWithFiles"] != "{ return loadTrustStoreWithPublisher(path, self, files, findnodebridge.Publisher{}) }" {
		failures = append(failures, "ordinary trust root must pass zero")
	}
	if bodies["trust.go:beginTrustPublication"] != "{ if s.trustPublisher == (findnodebridge.Publisher{}) { return nil } return s.trustPublisher.BeginPublication() }" {
		failures = append(failures, "mandatory nonclock trust helper changed")
	}
	initialTrustLiteral := false
	if constructorDecl != nil && constructorDecl.Body != nil && len(constructorDecl.Body.List) > 0 {
		if assignment, ok := constructorDecl.Body.List[0].(*ast.AssignStmt); ok && assignment.Tok == token.DEFINE && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
			if storeName, ok := assignment.Lhs[0].(*ast.Ident); ok && storeName.Name == "store" {
				if address, ok := assignment.Rhs[0].(*ast.UnaryExpr); ok && address.Op == token.AND {
					if literal, ok := address.X.(*ast.CompositeLit); ok && len(literal.Elts) > 0 {
						if typ, ok := literal.Type.(*ast.Ident); ok && typ.Name == "trustStore" {
							if first, ok := literal.Elts[0].(*ast.KeyValueExpr); ok {
								key, keyOK := first.Key.(*ast.Ident)
								value, valueOK := first.Value.(*ast.Ident)
								initialTrustLiteral = keyOK && valueOK && key.Name == "trustPublisher" && value.Name == "publisher"
							}
						}
					}
				}
			}
		}
	}
	if !initialTrustLiteral {
		failures = append(failures, "trust attachment must precede load and callbacks")
	}
	end := " if publication != nil { publication.Complete() }"
	// ast.Inspect visits the outer add block before the nested refresh block.
	want := map[string][]string{
		"trust.go:recordLastOnlineAt":                     {"publication := s.beginTrustPublication() s.contacts[address] = contact updated++" + end},
		"trust.go:rememberContactOrigin":                  {"publication := s.beginTrustPublication() s.contacts[contact.Address] = contact" + end, "publication := s.beginTrustPublication() s.contacts[contact.Address] = existing" + end},
		"trust.go:forget":                                 {"publication := s.beginTrustPublication() delete(s.contacts, address) delete(s.conflicts, address)" + end},
		"trust.go:mergeRecord":                            {"publication := s.beginTrustPublication() s.contacts[key.address] = contact delete(s.conflicts, key.address)" + end},
		"trust_local_contact.go:stageBoundLocalContact":   {"publication := s.beginTrustPublication() s.factRevision++ state.revision = s.factRevision row.LocalContact = state s.contacts[address] = row s.authorityObserved = true" + end},
		"trust_local_contact.go:confirmLocalContacts":     {"publication := s.beginTrustPublication() s.contacts[address] = current" + end},
		"trust_local_contact.go:captureLocalContactProof": {"publication := s.beginTrustPublication() s.contacts[address] = row" + end},
		"identity_pin_contact.go:promote":                 {"publication := s.trust.beginTrustPublication() s.trust.contacts[address] = row" + end},
	}
	if !reflect.DeepEqual(windows, want) {
		failures = append(failures, "trust W memory windows changed or contain callback, IO, allocation or wait")
	}
	stage := bodies["trust_local_contact.go:stageBoundLocalContact"]
	prepare := "wallAt := s.clock() state := &localContactState{fact: localContactFact{Version: 1, ID: factID, Network: domain.NetworkID(networkName), Identity: id, At: wallAt}} address := id.String() publication := s.beginTrustPublication()"
	if !strings.Contains(stage, prepare) || strings.Index(stage, "s.mu.Lock()") >= strings.Index(stage, "wallAt := s.clock()") {
		failures = append(failures, "stage clock, preallocation or final trust wait order changed")
	}
	ack := bodies["trust_local_contact.go:confirmLocalContacts"]
	refusal := "if err != nil || outcome != secretfile.DurableCommitted { return errors.Join(domain.ErrLocalContactFactUnconfirmed, err) } s.mu.Lock()"
	cas := "if !ok || !stamp.matches(captured, s) || !stamp.matches(current.LocalContact, s) { continue } state := *current.LocalContact state.confirmed = state.revision current.LocalContact = &state publication := s.beginTrustPublication()"
	if !strings.Contains(ack, refusal) || !strings.Contains(ack, cas) {
		failures = append(failures, "strict ACK lost committed outcome, current CAS or pre-W COW")
	}
	promote := bodies["identity_pin_contact.go:promote"]
	if strings.Index(promote, "s.secureSessions.store.stageContactPin") >= strings.Index(promote, "s.trust.beginTrustPublication()") || !strings.Contains(promote, "if err == nil || errors.Is(err, errPinMetadataSnapshotStale)") {
		failures = append(failures, "proof-clear publication moved over store stage or refusal")
	}
	return failures
}
func TestTrustBridgeClosedConstructorAndMemoryWindows(t *testing.T) {
	if failures := trustPublicationSourceErrors(pinPublicationSources(t)); len(failures) != 0 {
		t.Fatal("closed trust publication source contract", failures)
	}
}
func TestTrustBridgeGuardRejectsCallbackAndAttachmentEscapes(t *testing.T) {
	original := pinPublicationSources(t)
	if failures := trustPublicationSourceErrors(original); len(failures) != 0 {
		t.Fatal("baseline trust publication contract", failures)
	}
	cases := []struct{ name, path, before, after string }{
		{"unfenced_alias", "trust.go", "\n// recordLastOnlineAt stores", "\nfunc (s *trustStore) unreviewedContacts(){alias:=s.contacts;alias[\"x\"]=trustedContact{}}\n// recordLastOnlineAt stores"},
		{"late_setter", "trust.go", "\n// recordLastOnlineAt stores", "\nfunc (s *trustStore) lateTrustPublisher(p findnodebridge.Publisher){s.trustPublisher=p}\n// recordLastOnlineAt stores"},
		{"callback_under_W", "trust_local_contact.go", "\ts.factRevision++\n\tstate.revision", "\t_ = s.clock()\n\ts.factRevision++\n\tstate.revision"},
		{"missing_ACK_map", "trust_local_contact.go", "s.contacts[address] = current", "_ = current"},
		{"premature_Ready", "trust.go", "\tstore := &trustStore{", "\t_ = publisher.SealConstruction()\n\tstore := &trustStore{"},
		{"pre_literal_statement", "trust.go", "func loadTrustStoreWithPublisher(path string, self trustedContact, files func() (localContactDurableFiles, error), publisher findnodebridge.Publisher) (*trustStore, error) {\n\tstore := &trustStore{", "func loadTrustStoreWithPublisher(path string, self trustedContact, files func() (localContactDurableFiles, error), publisher findnodebridge.Publisher) (*trustStore, error) {\n\t_ = 0\n\tstore := &trustStore{"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			altered := map[string][]byte{}
			for name, raw := range original {
				altered[name] = bytes.Clone(raw)
			}
			if strings.Count(string(altered[tc.path]), tc.before) != 1 {
				t.Fatal("trust guard control anchor not unique", tc.name)
			}
			altered[tc.path] = []byte(strings.Replace(string(altered[tc.path]), tc.before, tc.after, 1))
			if len(trustPublicationSourceErrors(altered)) == 0 {
				t.Fatal("trust guard admitted authority escape", tc.name)
			}
		})
	}
}
