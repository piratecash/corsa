package node

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func pinPublicationPrinted(n any) string {
	var b bytes.Buffer
	if err := format.Node(&b, token.NewFileSet(), n); err != nil {
		return "<format error>"
	}
	return strings.Join(strings.Fields(b.String()), " ")
}
func pinPublicationSources(t *testing.T) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]byte)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		out[name] = raw
	}
	return out
}

// Literal reference multisets cover aliases/signatures/package declarations,
// not only calls. No source-derived allowlist or wildcard admits late binding.
func pinPublicationSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"pinPublisher":                       {"secure_session_store.go:<package>": 1, "secure_session_store.go:newSecureSessionStoreWithPublisher": 1, "secure_session_store.go:beginPinPublication": 2},
		"newSecureSessionStoreWithPublisher": {"secure_session_store.go:newSecureSessionStore": 1, "secure_session_store.go:openSecureSessionStore": 1},
		"beginPinPublication":                {"identity_pin_selected.go:putPinEntryLocked": 1, "identity_pin_selected.go:deletePinEntryLocked": 1, "identity_pin_transaction.go:rollbackIdentityPinRemoval": 1},
	}
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["pinPublisher"]["find_node_ready_construction.go:bindings"] = 1
	actual := make(map[string]map[string]int)
	for name := range expected {
		actual[name] = make(map[string]int)
	}
	bodies := make(map[string]string)
	var constructorDecl *ast.FuncDecl
	var failures []string
	publisherCalls := map[string]int{}
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			failures = append(failures, "pin publication parse: "+name)
			continue
		}
		count := func(n ast.Node, site string) {
			ast.Inspect(n, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if sites, watched := actual[id.Name]; watched {
						sites[name+":"+site]++
					}
				}
				if sel, ok := n.(*ast.SelectorExpr); ok && (sel.Sel.Name == "BeginPublication" || sel.Sel.Name == "BeginClockedPublication") {
					publisherCalls[name+":"+site+":"+sel.Sel.Name]++
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok {
				count(f.Type, f.Name.Name)
				if f.Recv != nil {
					count(f.Recv, f.Name.Name)
				}
				count(f.Body, f.Name.Name)
				if name == "secure_session_store.go" && f.Name.Name == "newSecureSessionStoreWithPublisher" {
					constructorDecl = f
				}
				if f.Body != nil {
					bodies[name+":"+f.Name.Name] = pinPublicationPrinted(f.Body)
				}
			} else {
				count(decl, "<package>")
			}
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "constructor or publication reference escape")
	}
	if !reflect.DeepEqual(publisherCalls, map[string]int{"find_node_ready_construction.go:prepareFindNodeReadyConstruction:BeginClockedPublication": 1, "conn_registry.go:beginRegistryPublication:BeginPublication": 1, "find_node_degree_bridge.go:captureBridgeDegreeProjection:BeginPublication": 1, "find_node_connection.go:publishFindNodeLocked:BeginClockedPublication": 1, "secure_session_store.go:beginPinPublication:BeginPublication": 1, "trust.go:beginTrustPublication:BeginPublication": 1, "find_node_degree.go:degreePoison:BeginPublication": 1, "find_node_degree.go:publishDegreeTransport:BeginClockedPublication": 1, "find_node_degree_terminal_bridge.go:endBridgeLocked:BeginPublication": 1, "find_node_degree_terminal_bridge.go:endBridgeLocked:BeginClockedPublication": 1, "peer_state_owned_degree_statistic.go:installOwnedDegreeStatistic:BeginClockedPublication": 1, "peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush:BeginClockedPublication": 1}) {
		failures = append(failures, "publication API escaped final memory helper")
	}
	wrapper := bodies["secure_session_store.go:newSecureSessionStore"]
	if wrapper != "{ return newSecureSessionStoreWithPublisher(clock, logger, findnodebridge.Publisher{}) }" {
		failures = append(failures, "ordinary constructor must supply zero Publisher")
	}
	helper := bodies["secure_session_store.go:beginPinPublication"]
	if helper != "{ if s.pinPublisher == (findnodebridge.Publisher{}) { return nil } return s.pinPublisher.BeginPublication() }" {
		failures = append(failures, "nonclock mandatory publication helper changed")
	}
	initialPinLiteral := false
	if constructorDecl != nil && constructorDecl.Body != nil && len(constructorDecl.Body.List) == 1 {
		if result, ok := constructorDecl.Body.List[0].(*ast.ReturnStmt); ok && len(result.Results) == 1 {
			if address, ok := result.Results[0].(*ast.UnaryExpr); ok && address.Op == token.AND {
				if literal, ok := address.X.(*ast.CompositeLit); ok && len(literal.Elts) > 0 {
					if typ, ok := literal.Type.(*ast.Ident); ok && typ.Name == "secureSessionStore" {
						if first, ok := literal.Elts[0].(*ast.KeyValueExpr); ok {
							key, keyOK := first.Key.(*ast.Ident)
							value, valueOK := first.Value.(*ast.Ident)
							initialPinLiteral = keyOK && valueOK && key.Name == "pinPublisher" && value.Name == "publisher"
						}
					}
				}
			}
		}
	}
	if !initialPinLiteral {
		failures = append(failures, "Publisher must attach only in initial store literal")
	}
	for _, name := range []string{"putPinEntryLocked", "deletePinEntryLocked"} {
		body := bodies["identity_pin_selected.go:"+name]
		added, mutation := "true", "s.identities[id]=entry"
		if name == "deletePinEntryLocked" {
			added, mutation = "false", "delete(s.identities,id)"
		}
		// Exact two-branch grammar: Before precedes W, finish is deferred
		// first, returned-Complete is marked after Complete, and the normal
		// leaf sentinel follows the actual final membership write. Compacting
		// whitespace adds no wildcard/statement allowance to this prefix.
		prefix := "{ticket:=s.beginPinMembershipLocked(id," + added + ")ifticket!=nil{deferticket.finish(s)}publication:=s.beginPinPublication()ifpublication!=nil{ifticket==nil{deferpublication.Complete()}else{deferfunc(){publication.Complete()ticket.publicationReturned=true}()}}elseifticket!=nil{ticket.publicationReturned=true}"
		compactBody := strings.Join(strings.Fields(body), "")
		suffix := mutation + "ifticket!=nil{ticket.leafReturned=true}}"
		if !strings.HasPrefix(compactBody, prefix) || !strings.HasSuffix(compactBody, suffix) || strings.Contains(body, ".Lock(") || strings.Contains(body, ".RLock(") || strings.Count(body, ".Complete()") != 2 || strings.Count(body, "ticket.finish(s)") != 1 || strings.Count(compactBody, "ticket.publicationReturned=true") != 2 || strings.Count(compactBody, "ticket.leafReturned=true") != 1 {
			failures = append(failures, "pin map final memory guard: "+name)
		}
	}
	fatal := bodies["identity_pin_transaction.go:rollbackIdentityPinRemoval"]
	sequence := "failure := errors.Join(domain.ErrIdentityPinRecoveryRequired, cause, err) nextRecovery := &identityPinRecoveryState{permit: &permit, cause: failure, fatal: true} s.mu.Lock() publication := s.beginPinPublication() s.pinRecovery = nextRecovery if publication != nil { publication.Complete() } s.mu.Unlock() s.logIdentityPinOutcome(permit, result.Outcome, failure)"
	if !strings.Contains(fatal, sequence) {
		failures = append(failures, "fatal memory publication scope or preparation order changed")
	}
	return failures
}
func TestPinBridgeClosedConstructorAndMemoryLeaves(t *testing.T) {
	if failures := pinPublicationSourceErrors(pinPublicationSources(t)); len(failures) != 0 {
		t.Fatal("closed pin publication source contract", failures)
	}
}
func TestPinBridgeGuardRejectsLateAttachmentAndMissingLeaves(t *testing.T) {
	original := pinPublicationSources(t)
	if failures := pinPublicationSourceErrors(original); len(failures) != 0 {
		t.Fatal("baseline pin publication contract", failures)
	}
	cases := []struct{ name, path, before, after string }{
		{"put", "identity_pin_selected.go", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tif ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n"},
		{"veto", "secure_session_store.go", "return s.pinPublisher.BeginPublication()", "return nil"},
		{"late_setter", "secure_session_store.go", "\n// startFromDisk audits", "\nfunc (s *secureSessionStore) latePublisher(p findnodebridge.Publisher) { s.pinPublisher=p }\n// startFromDisk audits"},
		{"fatal_scope", "identity_pin_transaction.go", "\t\tif publication != nil {\n\t\t\tpublication.Complete()\n\t\t}\n\t\ts.mu.Unlock()", "\t\ts.mu.Unlock()\n\t\tif publication != nil {\n\t\t\tpublication.Complete()\n\t\t}"},
		{"pre_literal_statement", "secure_session_store.go", "func newSecureSessionStoreWithPublisher(clock *trustedclock.Clock, logger zerolog.Logger, publisher findnodebridge.Publisher) *secureSessionStore {\n\treturn &secureSessionStore{", "func newSecureSessionStoreWithPublisher(clock *trustedclock.Clock, logger zerolog.Logger, publisher findnodebridge.Publisher) *secureSessionStore {\n\t_ = 0\n\treturn &secureSessionStore{"},
	}
	cases = append(cases, []struct{ name, path, before, after string }{
		{"membership-Before-inside-W", "identity_pin_selected.go", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tvar ticket *pinMembershipTicket\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n\tticket = s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n"},
		{"membership-After-before-W-Complete", "identity_pin_selected.go", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n"},
		{"membership-Complete-returned-too-early", "identity_pin_selected.go", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tpublication.Complete()\n\t\t\t\tticket.publicationReturned = true\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n", "func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {\n\tticket := s.beginPinMembershipLocked(id, true)\n\tif ticket != nil {\n\t\tdefer ticket.finish(s)\n\t}\n\tpublication := s.beginPinPublication()\n\tif publication != nil {\n\t\tif ticket == nil {\n\t\t\tdefer publication.Complete()\n\t\t} else {\n\t\t\tdefer func() {\n\t\t\t\tticket.publicationReturned = true\n\t\t\t\tpublication.Complete()\n\t\t\t}()\n\t\t}\n\t} else if ticket != nil {\n\t\tticket.publicationReturned = true\n\t}\n"},
		{"membership-leaf-returned-before-write", "identity_pin_selected.go", "\ts.identities[id] = entry\n\tif ticket != nil {\n\t\tticket.leafReturned = true\n\t}", "\tif ticket != nil {\n\t\tticket.leafReturned = true\n\t}\n\ts.identities[id] = entry"},
	}...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			altered := make(map[string][]byte)
			for n, raw := range original {
				altered[n] = bytes.Clone(raw)
			}
			if strings.Count(string(altered[tc.path]), tc.before) != 1 {
				t.Fatal("pin publication self-control anchor not unique", tc.name)
			}
			altered[tc.path] = []byte(strings.Replace(string(altered[tc.path]), tc.before, tc.after, 1))
			if len(pinPublicationSourceErrors(altered)) == 0 {
				t.Fatal("pin publication authority escape accepted", tc.name)
			}
		})
	}
}
