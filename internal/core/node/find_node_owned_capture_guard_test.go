package node

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Explicitly reviewed raw-body literal pins. Whitespace only is excluded, as
// in the existing registry guard; comments remain part of this SOURCE contract.
func ownedRawF1BodyPins() map[string]string {
	return map[string]string{
		"captureFindNodePriorityClasses":      "a00a39b9da884c369dc91cd130097710caa2eec5ab2d72bf3fcaa2d1cb19f111",
		"findNodePriorityClassLocked":         "fdc282ac4b05126ef8907cda068723047dd8851abc43f564142839868bd6c54e",
		"collectOwnedFindNodeRawFacts":        "31f55472d219f674ed3347256a4c6913223158cb04d170ebecf4fdc39d6f7319",
		"collectOwnedFindNodeRawMemberLocked": "1bed1522d764a57a438dae6cf5b68db4a424b30a15ce083f36b206ee9547c2c7",
		"collectOwnedFindNodeRawClasses":      "2ba143b36c07e9771f7431be5f5a997248c9aa986994f765ceac218b1a04a732",
	}
}

func ownedRawF1SourceErrors(sources map[string][]byte) []string {
	if !qualifiedD1MembershipSource(sources["find_node_membership_checkpoint.go"]) {
		return []string{"membership exact tag/body changed"}
	}
	expected := map[string]map[string]int{
		"ownedFindNodeRawFacts":               {"find_node_connection.go:<package>": 1, "find_node_connection.go:collectOwnedFindNodeRawFacts": 9, "find_node_bridge_capture.go:<package>": 1},
		"ownedFindNodeRawWordsPerMember":      {"find_node_connection.go:<package>": 1, "find_node_connection.go:collectOwnedFindNodeRawFacts": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1, "find_node_bridge_construction.go:claim": 1},
		"collectOwnedFindNodeRawFacts":        {"find_node_bridge_capture.go:captureOwnedFindNodeObservation": 1},
		"collectOwnedFindNodeRawMemberLocked": {"find_node_connection.go:collectOwnedFindNodeRawFacts": 1, "find_node_owned_receive.go:takeOwnedFindNodeReceive": 1, "find_node_owned_answer.go:dispatchOwnedFindNodeAnswer": 1},
		"collectOwnedFindNodeRawClasses":      {"find_node_connection.go:collectOwnedFindNodeRawFacts": 1},
		"findNodePriorityClassLocked":         {"find_node_connection.go:captureFindNodePriorityClasses": 1, "find_node_connection.go:collectOwnedFindNodeRawClasses": 1},
	}
	expected["collectOwnedFindNodeRawMemberLocked"]["find_node_membership_checkpoint.go:CheckpointForService"] = 1
	functions := map[string]bool{"collectOwnedFindNodeRawFacts": true, "collectOwnedFindNodeRawMemberLocked": true, "collectOwnedFindNodeRawClasses": true, "findNodePriorityClassLocked": true}
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	pins, gotPins := ownedRawF1BodyPins(), map[string]string{}
	var failures []string
	var fields []string
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"raw F1 SOURCE parse"}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			var fn *ast.FuncDecl
			if value, ok := decl.(*ast.FuncDecl); ok {
				fn = value
				declaration = fn.Name
				site = path + ":" + fn.Name.Name
			}
			if fn != nil && path == "find_node_connection.go" && fn.Body != nil {
				if _, watched := pins[fn.Name.Name]; watched {
					body := raw[int(fn.Body.Pos())-1 : int(fn.Body.End())-1]
					sum := sha256.Sum256([]byte(strings.Join(strings.Fields(string(body)), "")))
					if _, duplicate := gotPins[fn.Name.Name]; duplicate {
						failures = append(failures, "duplicate raw F1 body")
					}
					gotPins[fn.Name.Name] = hex.EncodeToString(sum[:])
				}
			}
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					callees[call.Fun] = true
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != declaration {
					if refs, tracked := actual[id.Name]; tracked {
						refs[site]++
						if functions[id.Name] && !callees[id] {
							failures = append(failures, "raw F1 function value escape")
						}
					}
				}
				if sel, ok := n.(*ast.SelectorExpr); ok && functions[sel.Sel.Name] && !callees[sel] {
					failures = append(failures, "raw F1 method value escape")
				}
				if typ, ok := n.(*ast.TypeSpec); ok && typ.Name.Name == "ownedFindNodeRawFacts" {
					shape, ok := typ.Type.(*ast.StructType)
					if !ok {
						failures = append(failures, "raw facts type escape")
						return true
					}
					for _, field := range shape.Fields.List {
						if len(field.Names) != 1 {
							failures = append(failures, "raw facts embedded/combined fields")
						}
						for _, name := range field.Names {
							fields = append(fields, name.Name)
						}
					}
				}
				if fn == nil || !functions[fn.Name.Name] {
					return true
				}
				switch v := n.(type) {
				case *ast.GoStmt, *ast.SendStmt:
					failures = append(failures, "raw F1 acquired background authority")
				case *ast.CallExpr:
					if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "BeginCapture", "ReserveClock", "TryReadClock", "Sample", "BeginPublication", "BeginClockedPublication", "SealConstruction", "NewSession", "PrepareSession", "Bind", "NewPrioritySnapshot", "findNodeMembers", "findNodePriorityMembers":
							failures = append(failures, "raw F1 acquired observation or replacement authority")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "raw F1 exact unused/reference inventory changed")
	}
	if !reflect.DeepEqual(fields, []string{"allOriginal", "classes"}) {
		failures = append(failures, "raw facts authority/field shape changed")
	}
	if !reflect.DeepEqual(gotPins, pins) {
		failures = append(failures, "raw F1 bounded/preallocation/tuple/class body changed")
	}
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	frontdoor := compact("if s.findNode != nil && s.findNode.owned != nil { return findnode.PrioritySnapshot{}, 0, findnode.ErrNotReady } if s.findNode == nil")
	if strings.Count(compact(string(sources["find_node_connection.go"])), frontdoor) != 1 {
		failures = append(failures, "owned historical frontdoor reached class state")
	}
	return failures
}

func TestOwnedRawF1ClosedUnusedSource(t *testing.T) {
	if failures := ownedRawF1SourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

func TestOwnedRawF1GuardRejectsMetadataConsumerAndPartialCapture(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"tracked", "if l.ended || b == nil || !b.published {", "if l.ended || b == nil || !b.published || !e.tracked {"},
		{"auth", "e.common != l || l.entry != e || l.id != id || id == 0", "e.core.Auth() == nil || e.common != l || l.entry != e || l.id != id || id == 0"},
		{"netcore-identity", "b.identity.IsZero() || b.identity != b.session.Identity()", "b.identity.IsZero() || b.identity != e.core.Identity()"},
		{"partial-M", "if count == profile.MaxMembers {\n\t\t\twalkErr = findnode.ErrCapacity", "if count == profile.MaxMembers {\n\t\t\twalkErr = nil"},
		{"unbounded-V", "if visits == profile.MaxRegistryVisits {", "if false && visits == profile.MaxRegistryVisits {"},
		{"class-callback", "class, err := findNodePriorityClassLocked(entry, present, contacts[i])", "class, err := findNodePriorityClassLocked(entry, present, findNodePriorityContact{})"},
		{"target-filter", "originals[count] = member", "if member.Identity() == (domain.PeerIdentity{253}) { return true }; originals[count] = member"},
		{"online-caller", "", "\nfunc rawF1OnlineEscape(s *Service,ctx context.Context,p findNodePriorityProfile){_,_=s.collectOwnedFindNodeRawFacts(ctx,p)}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources["find_node_connection.go"])
			if tc.before == "" {
				sources["raw_f1_extra.go"] = []byte("package node\nimport \"context\"\n" + tc.after)
			} else {
				// The historical l.ended predicate also exists: select the exact
				// new member body for that case instead of weakening multiplicity.
				if tc.name == "tracked" || tc.name == "unbounded-V" {
					function := "collectOwnedFindNodeRawMemberLocked"
					if tc.name == "unbounded-V" {
						function = "collectOwnedFindNodeRawFacts"
					}
					start := strings.Index(raw, "func (s *Service) "+function+"(")
					if start < 0 {
						t.Fatal("raw member body anchor")
					}
					tail := raw[start:]
					if strings.Count(tail, tc.before) != 1 {
						t.Fatal("raw SOURCE scoped exact anchor", tc.name)
					}
					sources["find_node_connection.go"] = []byte(raw[:start] + strings.Replace(tail, tc.before, tc.after, 1))
				} else {
					if strings.Count(raw, tc.before) != 1 {
						t.Fatal("raw SOURCE exact anchor", tc.name)
					}
					sources["find_node_connection.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
				}
			}
			if len(ownedRawF1SourceErrors(sources)) == 0 {
				t.Fatal("raw F1 SOURCE escape accepted", tc.name)
			}
		})
	}
}
