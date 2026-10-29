package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// Closed S1 syntax roles only. Registry write closure, Ready, admission and
// runtime causal sensitivity remain independent obligations.
func ownedF1SourceErrors(sources map[string][]byte) []string {
	if !qualifiedD1DiagnosticSource(sources["find_node_resource_diagnostic.go"]) {
		return []string{"diagnostic exact tag/body changed"}
	}
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	var failures []string
	bodies := map[string]string{}
	refs := map[string]int{}
	owned := map[string]int{}
	literals := map[string]int{}
	for path, raw := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"owned F1 parse"}
		}
		for _, decl := range f.Decls {
			site := path + ":<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if fn.Body != nil {
					bodies[site] = compact(pinPublicationPrinted(fn.Body))
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n.Name == "findNodeOwnedAdapter" {
						refs[site]++
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "owned" {
						owned[site]++
					}
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok && id.Name == "findNodeOwnedAdapter" {
						literals[site]++
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "owned" {
							failures = append(failures, "owned F1 late adapter replacement")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(refs, map[string]int{"find_node_connection.go:<package>": 2, "find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1}) {
		failures = append(failures, "owned F1 exact adapter roles changed")
	}
	if !reflect.DeepEqual(owned, map[string]int{"find_node_resource_diagnostic.go:boundTo": 5, "find_node_ready_construction.go:prepareFindNodeReadyConstruction": 2, "find_node_ready_construction.go:bindings": 5, "find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction": 1, "find_node_connection.go:publishFindNodeLocked": 1, "find_node_connection.go:findNodePrioritySnapshot": 1, "find_node_connection.go:collectOwnedFindNodeRawFacts": 2, "find_node_bridge_construction.go:claim": 3, "find_node_bridge_capture.go:captureOwnedFindNodeObservation": 2, "find_node_connection.go:captureFindNodeSeed": 3, "find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction": 1}) {
		failures = append(failures, "owned F1 field alias/role escape")
	}
	if !reflect.DeepEqual(literals, map[string]int{"find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService": 1, "find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction": 1}) {
		failures = append(failures, "owned F1 constructor root changed")
	}
	wantOpen := `{
	if s.findNode == nil || l.ended || l.findNode == nil {
		return false
	}
	b := l.findNode
	if b.published {
		return true
	}
	if a := s.findNode.owned; a != nil {
		t := a.terminal
		if t == nil || t != s.bridgeTerminal || t != l.bridgeTerminal || t.owner == nil || t.owner != s.degree || t.token == nil || t.token != s.proofToken || l.token != t.token || t.owner.token != t.token || t.owner.bridgePublisher != t.publisher || a.done == nil {
			return false
		}
		publication := t.publisher.BeginClockedPublication()
		point, err := publication.Sample()
		if err != nil {
			publication.Complete()
			return false
		}
		select {
		case <-a.done:
			publication.Complete()
			return false
		default:
		}
		session, err := b.seed.Bind(l.id, b.group, findnode.Instant(point.Elapsed()))
		if err != nil {
			publication.Complete()
			return false
		}
		b.session, b.published, b.seed = session, true, findnode.SessionSeed{}
		publication.Complete()
		return true
	}
	elapsed := time.Since(s.findNode.origin)
	if elapsed < 0 || s.runCtx.Err() != nil {
		return false
	}
	session, err := b.seed.Bind(l.id, b.group, findnode.Instant(elapsed))
	if err != nil {
		return false
	}
	b.session = session
	b.published = true
	b.seed = findnode.SessionSeed{}
	return true
}`
	if bodies["find_node_connection.go:publishFindNodeLocked"] != compact(wantOpen) {
		failures = append(failures, "owned F1 exact duplicate/anchor/clock-W/sample/Done/prepared-Bind/publication window changed")
	}
	wantRoot := `{ b,err:=prepareFindNodeTerminalConstruction(profile) if err!=nil{return nil,err} b.findNode=&findNodeAdapter{owned:&findNodeOwnedAdapter{terminal:b.terminal,done:b.ctx.Done()}} return newServiceWithConstruction(cfg,id,bus,nil,productionDatagramConstructor{},serviceConstruction{bridge:b}),nil }`
	if bodies["find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService"] != compact(wantRoot) {
		failures = append(failures, "owned F1 dependency must precede builder")
	}
	need := func(path, fragment, label string) {
		if strings.Count(compact(string(sources[path])), compact(fragment)) != 1 {
			failures = append(failures, label)
		}
	}
	need("service.go", "ownedStatistic = bridge.statistic findNode = bridge.findNode", "owned F1 immutable local capture")
	need("service.go", "svc := &Service{proofToken:proofToken,degree:degree,bridgeTerminal:terminal,ownedDegreeStatisticProducer:ownedStatistic,findNode:findNode,", "owned F1 FIRST Service literal")
	need("find_node_bridge_construction.go", `if b != nil && b.findNode != nil && (b.findNode.owned == nil || b.findNode.owned.terminal != b.terminal || b.ctx == nil || b.findNode.owned.done != b.ctx.Done()) { panic("invalid owned F1 construction") }`, "owned F1 lifecycle and concrete terminal claim")
	need("find_node_connection.go", "if s.findNode != nil && s.findNode.owned != nil { return findnode.PrioritySnapshot{},0,findnode.ErrNotReady } if s.findNode == nil", "owned F1 legacy observer embargo")
	// Preserve exact original receipt and registry/entry reciprocity. No fake
	// mirrored generation can replace actual CM acceptance.
	need("find_node_connection.go", `if r == nil || r.peer != p || r.common != l || r.address != address || r.generation != generation || p.selectedGeneration != generation || s.sessions[p.address] != p || l.peer != p || l.entry != e || e == nil || e.common != l || e.core != p.netCore || l.id != p.connID || !p.authOK || !l.outbound || l.role != sessionv2.RoleDialer { return false }`, "owned F1 exact CM original receipt and registry checks")
	return failures
}
func TestOwnedF1ClosedPreparedOpenSource(t *testing.T) {
	if failures := ownedF1SourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedF1GuardRejectsLateBindingLegacyClockAndWindowCalls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"late-other-file", "peer_management.go", "\nfunc ", "\nfunc lateOwnedF1(s *Service){s.findNode.owned=nil}\nfunc "},
		{"late-first-literal", "service.go", "findNode: findNode,", "findNode: nil,"},
		{"legacy-clock", "find_node_connection.go", "findnode.Instant(point.Elapsed())", "findnode.Instant(time.Since(s.findNode.origin))"},
		{"allocation-under-W", "find_node_connection.go", "session, err := b.seed.Bind(l.id, b.group, findnode.Instant(point.Elapsed()))", "_ = new(findnode.SessionSeed)\n\tsession, err := b.seed.Bind(l.id, b.group, findnode.Instant(point.Elapsed()))"},
		{"context-callback", "find_node_connection.go", "case <-a.done:", "case <-s.runCtx.Done():"},
		{"second-sample", "find_node_connection.go", "point, err := publication.Sample()", "_, _ = publication.Sample()\n\t\tpoint, err := publication.Sample()"},
		{"skip-duplicate", "find_node_connection.go", "if b.published {", "if false && b.published {"},
		{"foreign-terminal", "find_node_connection.go", "t != s.bridgeTerminal || t != l.bridgeTerminal || t.owner == nil", "t != s.bridgeTerminal || t.owner == nil"},
		{"stale-receipt", "find_node_connection.go", "r == nil || r.peer != p || r.common != l || r.address != address", "r == nil || r.common != l || r.address != address"},
		{"wrong-address", "find_node_connection.go", "r.address != address || ", ""},
		{"wrong-generation", "find_node_connection.go", "r.generation != generation || ", ""},
		{"wrong-common", "find_node_connection.go", "r.peer != p || r.common != l || r.address != address", "r.peer != p || r.address != address"},
		{"wrong-registry", "find_node_connection.go", "p.selectedGeneration != generation || s.sessions[p.address] != p || l.peer != p", "p.selectedGeneration != generation || l.peer != p"},
		{"wrong-entry", "find_node_connection.go", "l.peer != p || l.entry != e || ", "l.peer != p || "},
		{"wrong-peer", "find_node_connection.go", "l.peer != p || ", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources[tc.path])
			switch tc.name {
			case "late-other-file":
				sources[tc.path] = append(sources[tc.path], []byte("\nfunc lateOwnedF1(s *Service){s.findNode.owned=nil}\n")...)
			case "late-first-literal":
				anchor := regexp.MustCompile(`findNode\s*:\s*findNode,`)
				if len(anchor.FindAllStringIndex(raw, -1)) != 1 {
					t.Fatal("owned F1 literal anchor")
				}
				sources[tc.path] = []byte(anchor.ReplaceAllString(raw, "findNode: nil,"))
			default:
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("owned F1 selfcase anchor", tc.name)
				}
				sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if len(ownedF1SourceErrors(sources)) == 0 {
				t.Fatal("owned F1 SOURCE escape accepted", tc.name)
			}
		})
	}
}
