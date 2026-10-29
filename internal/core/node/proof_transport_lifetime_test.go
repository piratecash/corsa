package node

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestProofTransportTerminalBeforeAndAfterAtomicBind(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "end-before-bind", false: "bind-before-end"}[before], func(t *testing.T) {
			c := &peekedConn{reader: strings.NewReader("")}
			l := &proofTransportLifetime{}
			if before {
				if _, err := c.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			}
			if !c.bindProofTransport(l) {
				t.Fatal("original bind not idempotent")
			}
			if !c.bindProofTransport(l) {
				t.Fatal("original bind not idempotent")
			}
			if !before {
				if _, err := c.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			}
			l.mu.Lock()
			ended := l.ended
			l.mu.Unlock()
			if !ended {
				t.Fatal("pre-attach terminal was lost")
			}
			other := &proofTransportLifetime{}
			if c.bindProofTransport(other) || c.common.Load() != l {
				t.Fatal("original cell overwritten")
			}
		})
	}
	// Close and Write failures can also precede proof-cell creation. The
	// primitive uses owned in-memory pipes; these are not TLS/kernel evidence.
	for _, op := range []string{"close", "write"} {
		local, peer := net.Pipe()
		c := &peekedConn{Conn: local}
		if op == "close" {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			_ = peer.Close()
			if _, err := c.Write([]byte("x")); err == nil {
				t.Fatal("closed pipe write succeeded")
			}
		}
		l := &proofTransportLifetime{}
		if !c.bindProofTransport(l) || !f1Ended(l) {
			t.Fatal("terminal before bind resurrected", op)
		}
		_ = local.Close()
		_ = peer.Close()
	}
	// Concurrent SC interleavings: each run owns exactly one reader and binder.
	for range 100 {
		c := &peekedConn{reader: strings.NewReader("")}
		l := &proofTransportLifetime{}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = c.Read(make([]byte, 1)) }()
		go func() {
			defer wg.Done()
			if !c.bindProofTransport(l) {
				t.Error("initial CAS refused")
			}
		}()
		wg.Wait()
		l.mu.Lock()
		ended := l.ended
		l.mu.Unlock()
		if !ended {
			t.Fatal("concurrent terminal-before-bind resurrected")
		}
	}
}
func TestProofTransportEndClearsSharedAuthorityAndCanonicalSession(t *testing.T) {
	f := f1Pair(t, true)
	f.accepted.publishAcceptedProofTransport(f.inbound)
	sessions := f1Members(t, f.accepted)
	if len(sessions.sessions) != 1 {
		t.Fatal("actual proof not published")
	}
	original := f.inbound.life
	arrival := acceptedTransportArrivalContext(t.Context(), f.accepted, f.inbound.id, f.inbound)
	f.inbound.end()
	f.inbound.end()
	_, viewErr := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, sessions.sessions, findnode.Instant(time.Since(f.accepted.findNode.origin)+time.Second), func(findnode.SessionInfo) (findnode.SessionClass, error) { return findnode.ClassOrdinary, nil })
	if viewErr == nil {
		t.Fatal("End left externally retained canonical Session valid")
	}

	original.mu.Lock()
	retained := !original.ended || original.entry != nil || original.peer != nil || original.evidence != nil
	original.mu.Unlock()
	if retained {
		t.Fatal("terminal retained authority")
	}
	if arrival.Value(proofTransportArrivalKey{}) != f.inbound || f.inbound.life != original {
		t.Fatal("opaque alias replaced")
	}
	if len(f1Members(t, f.accepted).sessions) != 0 {
		t.Fatal("ended original still a member")
	}
	f.accepted.publishAcceptedProofTransport(f.inbound)
	if len(f1Members(t, f.accepted).sessions) != 0 {
		t.Fatal("late publication resurrected")
	}
	// Session/seed are shallow memory values; no full ProvenSession is retained.
	typ := reflect.TypeFor[proofTransportLifetime]()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		switch field.Type.Kind() {
		case reflect.Map, reflect.Slice, reflect.Chan:
			t.Fatal("growing inventory", field.Name)
		}
	}
	t.Logf("common %dB, termination handle %dB, seed/binding %dB, peer/registry pointer aliases additional; O(current readers), delayed external aliases unbounded by this observation; not resource admission", unsafe.Sizeof(*original), unsafe.Sizeof(*f.inbound), unsafe.Sizeof(findNodeConnectionBinding{}))
}
func TestProofTransportEndBeforePublicationUsesSharedGate(t *testing.T) {
	f := f1Pair(t, true)
	f.inbound.end()
	f.accepted.publishAcceptedProofTransport(f.inbound)
	if f.inbound.life.findNode.published {
		t.Fatal("End-first permitted seed Bind")
	}
	if f.inbound.life.evidence != nil {
		t.Fatal("prepublication End retained full TLS tuple")
	}
}
func TestProofTransportPublicationElapsedAfterCommonWait(t *testing.T) {
	f := f1Pair(t, true)
	l := f.inbound.life
	l.mu.Lock()
	held := true
	done := make(chan struct{})
	gid := make(chan uint64, 1)
	defer func() {
		if held {
			l.mu.Unlock()
		}
		f1Await(t, done, "publication elapsed cleanup join")
	}()
	go func() {
		defer close(done)
		gid <- relationGateCurrentGID()
		f.accepted.publishAcceptedProofTransport(f.inbound)
	}()
	var publisherGID uint64
	select {
	case publisherGID = <-gid:
	case <-time.After(2 * time.Second):
		t.Fatal("publication owner did not start")
	}
	// Exact original publisher/common-mutex callsite in this frozen source.
	// Mutex waits are NOT durable synctest blocking; use an actual GID/stack
	// barrier and the existing immutable real-clock origin, without fake time.
	n1WaitMutexFrame(t, publisherGID, "conn_registry.go:767")
	releaseBoundary := findnode.Instant(time.Since(f.accepted.findNode.origin))
	l.mu.Unlock()
	held = false
	f1Await(t, done, "publication elapsed completion")
	l.mu.Lock()
	published := l.findNode.published
	session := l.findNode.session
	l.mu.Unlock()
	if !published {
		t.Fatal("post-wait publish refused")
	}
	var opened findnode.Instant
	_, err := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, []findnode.Session{session}, findnode.Instant(time.Since(f.accepted.findNode.origin)), func(info findnode.SessionInfo) (findnode.SessionClass, error) {
		opened = info.Opened
		return findnode.ClassOrdinary, nil
	})
	if err != nil || opened < releaseBoundary {
		t.Fatal("elapsed sampled before common gate wait", opened, releaseBoundary, err)
	}
}

func TestProofTransportOutboundArrivalCannotBorrowReplacement(t *testing.T) {
	f := f1Pair(t, true)
	p := f.outbound
	original := captureSessionTransportOwner(f.dialer, p)
	if original == nil {
		t.Fatal("provisional actual dialer capsule absent")
	}
	ctx := sessionTransportArrivalContext(context.Background(), f.dialer, p)
	if ctx.Value(proofTransportArrivalKey{}) == nil || ctx.Value(proofTransportArrivalKey{}).(*proofTransportOwner).life != p.common {
		t.Fatal("arrival rebound")
	}
	b := &peerSession{connID: p.connID, common: &proofTransportLifetime{token: f.dialer.proofToken, id: p.connID, role: sessionv2.RoleDialer, outbound: true}}
	b.common.peer = b
	f.dialer.peerMu.Lock()
	f.dialer.sessions[p.address] = b
	f.dialer.peerMu.Unlock()
	original.end()
	if !p.common.ended || b.common.ended {
		t.Fatal("captured A ended current B")
	}
	if captureSessionTransportOwner(f.dialer, p) != nil {
		t.Fatal("late original capture revived A")
	}
}

// Literal closed shapes are reviewed authority/retention contracts, not a
// dynamically learned golden. Extra fields/aliases inside an allowed file fail.
func f1ShapeErrors(path string, raw []byte) []string {
	expected := map[string]map[string]string{
		"proof_transport_lifetime.go": {
			"proofTransportLifetime":     `type proofTransportLifetime struct{mu sync.Mutex;ended bool;token *proofTransportServiceToken;role sessionv2.Role;outbound bool;id domain.ConnID;entry *connEntry;peer *peerSession;evidence *proofTransportEvidence;nin *matureInboundWitness;findNode *findNodeConnectionBinding;degreeWitness *degreeWitness;bridgeTerminal *findNodeBridgeTerminal}`,
			"proofTransportOwner":        `type proofTransportOwner struct{life *proofTransportLifetime;id domain.ConnID;token *proofTransportServiceToken}`,
			"proofTransportServiceToken": `type proofTransportServiceToken struct{_ byte}`,
		},
		"identity_pin_inbound.go": {"matureInboundWitness": `type matureInboundWitness struct{transport *proofTransportLifetime;phase inboundLifetimePhase;producer *inboundPinProducer;binding identityPinEntryBinding;session sessionv2.ProvenSession;group domain.SourceGroup;ready time.Duration}`},
		"session_secure.go": {
			"peekedConn":             `type peekedConn struct{net.Conn;reader io.Reader;common atomic.Pointer[proofTransportLifetime];terminal atomic.Bool}`,
			"proofTransportEvidence": `type proofTransportEvidence struct{session sessionv2.ProvenSession;protected net.Conn;raw,wire *peekedConn;group domain.SourceGroup;identity domain.PeerIdentity}`,
		},
		"find_node_connection.go": {
			"findNodeAdapter":           `type findNodeAdapter struct{origin time.Time;owned *findNodeOwnedAdapter}`,
			"findNodeOwnedAdapter":      `type findNodeOwnedAdapter struct{terminal *findNodeBridgeTerminal;done <-chan struct{};retirement findnode.DormantIntake;localRetirement findnode.OwnedLocalTarget}`,
			"findNodeCMReceipt":         `type findNodeCMReceipt struct{peer *peerSession;common *proofTransportLifetime;address domain.PeerAddress;generation uint64}`,
			"findNodeConnectionBinding": `type findNodeConnectionBinding struct{seed findnode.SessionSeed;session findnode.Session;published bool;group domain.SourceGroup;identity domain.PeerIdentity;retirement findnode.DormantIntake;localRetirement findnode.OwnedLocalTarget}`,
		},
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		return []string{err.Error()}
	}
	canonical := func(n ast.Node) string {
		var b bytes.Buffer
		err := ast.Fprint(&b, nil, n, func(name string, v reflect.Value) bool {
			return v.Type() != reflect.TypeFor[token.Pos]() && name != "Obj" && name != "Doc" && name != "Comment"
		})
		if err != nil {
			return "FORMAT_ERROR"
		}
		return b.String()
	}
	actual := map[string][]string{}
	ast.Inspect(file, func(n ast.Node) bool {
		if typ, ok := n.(*ast.TypeSpec); ok {
			if _, tracked := expected[path][typ.Name.Name]; tracked {
				actual[typ.Name.Name] = append(actual[typ.Name.Name], canonical(typ))
				if typ.Assign.IsValid() {
					actual[typ.Name.Name] = append(actual[typ.Name.Name], "alias")
				}
			}
		}
		return true
	})
	var failures []string
	for name, text := range expected[path] {
		f, e := parser.ParseFile(token.NewFileSet(), "literal.go", "package node;"+text, 0)
		if e != nil {
			return []string{e.Error()}
		}
		want := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
		if len(actual[name]) != 1 || actual[name][0] != canonical(want) {
			failures = append(failures, "F1 original authority shape changed:"+name)
		}
	}
	return failures
}
func TestProofTransportClosedShapesAndReferenceEscapes(t *testing.T) {
	sources := ninN1Sources(t)
	for _, path := range []string{"proof_transport_lifetime.go", "identity_pin_inbound.go", "find_node_connection.go", "session_secure.go"} {
		if failures := f1ShapeErrors(path, sources[path]); len(failures) != 0 {
			t.Fatal(failures)
		}
	}
	for _, extra := range []string{
		`type escape = proofTransportOwner`,
		`var escape=(*Service).captureFindNodeSeed`,
		`func extra(s *Service){_,_=newFindNodeCandidateService(s.cfg,s.identity,nil)}`,
		`func extra(s *Service){_=s.findNodeMembers}`,
		`func extra(){_=new(proofTransportLifetime)}`,
		`func extra(s *Service){_=s.publishFindNodeCM(nil,"",1)}`,
	} {
		copySources := map[string][]byte{}
		for k, v := range sources {
			copySources[k] = v
		}
		copySources["find_node_connection.go"] = append(append([]byte(nil), sources["find_node_connection.go"]...), []byte("\n"+extra+"\n")...)
		if len(ninN1SourceErrors(copySources)) == 0 {
			t.Fatal("allowed-file authority escape accepted", extra)
		}
	}
	raw := sources["proof_transport_lifetime.go"]
	fieldSet := token.NewFileSet()
	fieldFile, err := parser.ParseFile(fieldSet, "proof_transport_lifetime.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	fieldMatches, start, end := 0, 0, 0
	ast.Inspect(fieldFile, func(n ast.Node) bool {
		typ, ok := n.(*ast.TypeSpec)
		if !ok || typ.Name.Name != "proofTransportLifetime" {
			return true
		}
		shape, ok := typ.Type.(*ast.StructType)
		if !ok {
			t.Fatal("original lifetime must remain a struct")
		}
		for _, field := range shape.Fields.List {
			if len(field.Names) != 1 || field.Names[0].Name != "degreeWitness" {
				continue
			}
			pointer, ok := field.Type.(*ast.StarExpr)
			if !ok {
				t.Fatal("degree witness field lost pointer type")
			}
			identifier, ok := pointer.X.(*ast.Ident)
			if !ok || identifier.Name != "degreeWitness" {
				t.Fatal("degree witness field lost exact named pointee")
			}
			fieldMatches++
			start, end = fieldSet.Position(identifier.Pos()).Offset, fieldSet.Position(identifier.End()).Offset
		}
		return false
	})
	if fieldMatches != 1 || start < 0 || end > len(raw) || string(raw[start:end]) != "degreeWitness" {
		t.Fatal("degree witness pointer field mutation did not match exactly once")
	}
	wrongDegreeWitness := append([]byte(nil), raw[:start]...)
	wrongDegreeWitness = append(wrongDegreeWitness, []byte("proofTransportLifetime")...)
	wrongDegreeWitness = append(wrongDegreeWitness, raw[end:]...)
	if failures := f1ShapeErrors("proof_transport_lifetime.go", wrongDegreeWitness); len(failures) != 1 || failures[0] != "F1 original authority shape changed:proofTransportLifetime" {
		t.Fatal("wrong degree witness pointer type accepted", failures)
	}

}
func TestProofTransportSchedulingAndPublicationEdges(t *testing.T) {
	sources := ninN1Sources(t)
	function := func(path, name string) *ast.FuncDecl {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), path, sources[path], 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == name {
				return f
			}
		}
		t.Fatal("missing exact F1 root", path, name)
		return nil
	}
	// Closed topology, not a generic socket-unwrapping facility: the actual
	// sealed session's Role chooses direct listener R or ONE Dial wrapper.
	unwrapShape := func(root *ast.FuncDecl) bool {
		calls := map[string]int{}
		var actualRole, initialRaw, matchedRaw token.Pos
		var roleSwitch *ast.SwitchStmt
		loops := false
		ast.Inspect(root, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.ForStmt, *ast.RangeStmt:
				loops = true
			case *ast.CallExpr:
				if fun, ok := node.Fun.(*ast.SelectorExpr); ok && fun.Sel.Name == "NetConn" {
					if recv, ok := fun.X.(*ast.Ident); ok {
						calls[recv.Name]++
					} else {
						calls["nonliteral-receiver"]++
					}
				}
			case *ast.AssignStmt:
				if len(node.Lhs) == 2 && len(node.Rhs) == 1 {
					if lhs, ok := node.Lhs[0].(*ast.Ident); ok && lhs.Name == "role" {
						if call, ok := node.Rhs[0].(*ast.CallExpr); ok {
							if method, ok := call.Fun.(*ast.SelectorExpr); ok && method.Sel.Name == "Role" {
								if recv, ok := method.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "session" {
									if owner, ok := recv.X.(*ast.Ident); ok && owner.Name == "e" {
										actualRole = node.Pos()
									}
								}
							}
						}
					}
				}
				if len(node.Lhs) == 1 && len(node.Rhs) == 1 && node.Tok == token.DEFINE {
					if lhs, ok := node.Lhs[0].(*ast.Ident); ok && lhs.Name == "rawInput" {
						if call, ok := node.Rhs[0].(*ast.CallExpr); ok {
							if fun, ok := call.Fun.(*ast.SelectorExpr); ok && fun.Sel.Name == "NetConn" {
								if recv, ok := fun.X.(*ast.Ident); ok && recv.Name == "inner" {
									initialRaw = node.Pos()
								}
							}
						}
					}
				}
			case *ast.SwitchStmt:
				if tag, ok := node.Tag.(*ast.Ident); ok && tag.Name == "role" {
					if roleSwitch != nil {
						loops = true // duplicate topology dispatch is not the contract
					}
					roleSwitch = node
				}
			case *ast.IfStmt:
				if cond, ok := node.Cond.(*ast.BinaryExpr); ok && cond.Op == token.NEQ {
					if left, ok := cond.X.(*ast.Ident); ok && left.Name == "rawInput" {
						if right, ok := cond.Y.(*ast.SelectorExpr); ok && right.Sel.Name == "raw" {
							if owner, ok := right.X.(*ast.Ident); ok && owner.Name == "e" && len(node.Body.List) == 1 {
								if _, ok := node.Body.List[0].(*ast.ReturnStmt); ok {
									matchedRaw = node.Pos()
								}
							}
						}
					}
				}
			}
			return true
		})
		if loops || roleSwitch == nil || actualRole == token.NoPos || initialRaw <= actualRole || roleSwitch.Pos() <= initialRaw || matchedRaw <= roleSwitch.End() || !reflect.DeepEqual(calls, map[string]int{"inner": 1, "first": 1}) || len(roleSwitch.Body.List) != 3 {
			return false
		}
		for index, label := range []string{"RoleListener", "RoleDialer", "default"} {
			clause, ok := roleSwitch.Body.List[index].(*ast.CaseClause)
			if !ok {
				return false
			}
			if label == "default" {
				if len(clause.List) != 0 || len(clause.Body) != 1 {
					return false
				}
				if _, ok := clause.Body[0].(*ast.ReturnStmt); !ok {
					return false
				}
				continue
			}
			if len(clause.List) != 1 {
				return false
			}
			name, ok := clause.List[0].(*ast.SelectorExpr)
			if !ok || name.Sel.Name != label {
				return false
			}
			pkg, ok := name.X.(*ast.Ident)
			if !ok || pkg.Name != "sessionv2" || (index == 0 && len(clause.Body) != 0) {
				return false
			}
			if index == 1 {
				if len(clause.Body) != 3 {
					return false
				}
				last, ok := clause.Body[2].(*ast.AssignStmt)
				if !ok || len(last.Lhs) != 1 || len(last.Rhs) != 1 {
					return false
				}
				left, ok := last.Lhs[0].(*ast.Ident)
				if !ok || left.Name != "rawInput" {
					return false
				}
				call, ok := last.Rhs[0].(*ast.CallExpr)
				if !ok {
					return false
				}
				fun, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || fun.Sel.Name != "NetConn" {
					return false
				}
				recv, ok := fun.X.(*ast.Ident)
				if !ok || recv.Name != "first" {
					return false
				}
			}
		}
		return true
	}
	if !unwrapShape(function("conn_registry.go", "bindProofTransportLocked")) {
		t.Fatal("proof transport raw unwrap escaped exact role-derived 0/1 topology")
	}
	for _, change := range [][2]string{{"switch role {", "switch sessionv2.RoleDialer {"}, {"rawInput = first.NetConn()", "for range 1 { rawInput = first.NetConn() }"}} {
		raw := strings.Replace(string(sources["conn_registry.go"]), change[0], change[1], 1)
		file, err := parser.ParseFile(token.NewFileSet(), "conn_registry.go", raw, 0)
		if err != nil {
			t.Fatal("topology negative input did not parse", err)
		}
		for _, decl := range file.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == "bindProofTransportLocked" && unwrapShape(f) {
				t.Fatal("topology guard accepted forced role or recursive/loop-shaped unwrap", change[1])
			}
		}
	}
	f := function("peer_sessions.go", "completeOutboundAuth")
	var captured token.Pos
	var scheduled token.Pos
	var ownerRead int
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if fun.Name == "captureSessionTransportOwner" {
				captured = call.Pos()
				ownerRead++
			}
		case *ast.SelectorExpr:
			if fun.Sel.Name == "sendInitialIdentityPush" {
				t.Error("outbound initial push lost original owner")
			}
			if fun.Sel.Name == "goBackground" {
				ast.Inspect(call, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "sendInitialIdentityPushOwned" {
						scheduled = call.Pos()
					}
					return true
				})
			}
		}
		return true
	})
	if ownerRead != 1 || captured == token.NoPos || scheduled == token.NoPos || captured >= scheduled {
		t.Fatal("original owner must be captured BEFORE background scheduling")
	}
	pub := function("peer_sessions.go", "publishAndServeCMSession")
	var reg, publish, ready token.Pos
	ast.Inspect(pub, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "registerCMSession":
					reg = call.Pos()
				case "publishFindNodeCM":
					publish = call.Pos()
				case "EmitSlot":
					if len(call.Args) == 1 {
						if lit, ok := call.Args[0].(*ast.CompositeLit); ok {
							if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "SessionInitReady" {
								ready = call.Pos()
							}
						}
					}
				}
			}
		}
		return true
	})
	if reg == token.NoPos || publish <= reg || ready <= publish {
		t.Fatal("F1 publication moved out of actual post-setup SessionInitReady edge")
	}
	for _, pair := range [][3]string{{"datagram_integration.go", "dispatchSessionDatagramLine", "sessionTransportArrivalContext"}, {"datagram_integration.go", "dispatchInboundDatagramLineOwned", "acceptedTransportArrivalContext"}} {
		root := function(pair[0], pair[1])
		var capture, pipeline token.Pos
		ast.Inspect(root, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Name == pair[2] {
						capture = call.Pos()
					}
				case *ast.SelectorExpr:
					if fun.Sel.Name == "handleDatagramFrame" {
						pipeline = call.Pos()
						if id, ok := call.Args[0].(*ast.Ident); !ok || id.Name != "ctx" {
							t.Error("pipeline lost original context")
						}
					}
				}
			}
			return true
		})
		if capture == token.NoPos || pipeline <= capture {
			t.Fatal("arrival capture moved after pipeline", pair)
		}
	}
}

func TestProofTransportControlledSameKeySendCloseNeverRetargets(t *testing.T) {
	// Controlled same-key registry replacement, NOT a claim that real monotonic
	// production ConnIDs are reused. The genuine F1/NinOff service owns selection;
	// this fixture isolates exact A->B->C send/Close consistency from TLS I/O.
	s := f1Service(t)
	key := domain.ConnID(913701)
	type controlled struct {
		entry *connEntry
		core  *netcore.NetCore
		peer  net.Conn
	}
	makeOwner := func() controlled {
		local, peer := net.Pipe()
		core := newTestNetCore(t, key, local, netcore.Outbound, netcore.Options{})
		entry := &connEntry{core: core}
		entry.common = &proofTransportLifetime{token: s.proofToken, id: key, entry: entry, role: sessionv2.RoleDialer, outbound: true}
		t.Cleanup(func() { _ = peer.Close() })
		return controlled{entry, core, peer}
	}
	a, b, c := makeOwner(), makeOwner(), makeOwner()
	put := func(v controlled) { s.peerMu.Lock(); s.conns[key] = v.entry; s.peerMu.Unlock() }
	put(a)
	bridge := s.Network().(*networkBridge)
	selectedA := bridge.selectInboundSend(key)
	a.core.ShutSendQueue()
	put(b)
	original, err := selectedA.sendCapturedInbound(t.Context(), []byte("unused\n"), false)
	if !errors.Is(err, netcore.ErrSendChanClosed) || original == nil || original.life != a.entry.common {
		t.Fatal("SEND selected current replacement instead of A", err)
	}
	original.end()
	if !f1Ended(a.entry.common) || f1Ended(b.entry.common) {
		t.Fatal("SEND result retargeted B")
	}
	selectedB := bridge.selectInboundClose(key)
	if selectedB.core != b.core || !f1Ended(b.entry.common) || writerReturned(b.core) {
		t.Fatal("Close selection did not fence original B before core close")
	}
	put(c)
	if err := selectedB.closeCapturedInbound(); err != nil {
		t.Fatal(err)
	}
	if !writerReturned(b.core) || writerReturned(c.core) || f1Ended(c.entry.common) {
		t.Fatal("delayed Close relooked up C")
	}
	original.end()
	if f1Ended(c.entry.common) {
		t.Fatal("original replay retired C")
	}
}
