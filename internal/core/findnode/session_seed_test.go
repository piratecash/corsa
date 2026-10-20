package findnode

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func seedProof(t *testing.T, caps []string) sessionv2.ProvenSession {
	t.Helper()
	a, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	al, err := sessionv2.NewLocal(a, "seed-fixture")
	if err != nil {
		t.Fatal(err)
	}
	bl, err := sessionv2.NewLocal(b, "seed-fixture")
	if err != nil {
		t.Fatal(err)
	}
	certs, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := listener.Accept(); accepted <- c }()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	server := <-accepted
	_ = listener.Close()
	if server == nil {
		t.Fatal("accept failed")
	}
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	intro := protocol.Frame{Version: 31, Capabilities: caps}
	go func() { _, e := sessionv2.Accept(ctx, server, bl, intro, certs, sessionv2.DefaultTimeouts); done <- e }()
	p, err := sessionv2.Dial(ctx, client, al, intro, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return p
}
func TestSessionSeedOwnsCapabilitiesAndOneShotConsumption(t *testing.T) {
	p := seedProof(t, []string{string(domain.CapOverlayFindNodeV1)})
	local := []domain.Capability{domain.CapOverlayFindNodeV1}
	seed, err := PrepareSession(p, local)
	if err != nil {
		t.Fatal(err)
	}
	copySeed := seed
	local[0] = domain.CapMeshRelayV1
	peer, err := p.Peer()
	if err != nil {
		t.Fatal(err)
	}
	peer.Intro.Capabilities[0] = "changed-after-proof"
	if _, err := seed.Bind(0, "loopback", 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid argument consumed/misclassified", err)
	}
	session, err := copySeed.Bind(7, "loopback", 9)
	if err != nil {
		t.Fatal("captured facts changed after proof", err)
	}
	if _, err := seed.Bind(8, "loopback", 10); err == nil {
		t.Fatal("seed copy minted second Session")
	}
	session.End()
	session.End()
	if session.valid() {
		t.Fatal("End did not close canonical Session")
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[sessionSeedState](), reflect.TypeFor[sessionState]()} {
		for i := 0; i < typ.NumField(); i++ {
			if typ.Field(i).Type == reflect.TypeFor[sessionv2.ProvenSession]() {
				t.Fatal("seed/session retains full TLS proof", typ)
			}
		}
	}
}
func TestSessionSeedRejectsMissingNegotiationAndZeroProof(t *testing.T) {
	if _, err := PrepareSession(sessionv2.ProvenSession{}, []domain.Capability{domain.CapOverlayFindNodeV1}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
	p := seedProof(t, nil)
	if _, err := PrepareSession(p, []domain.Capability{domain.CapOverlayFindNodeV1}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("missing remote cap accepted", err)
	}
	p = seedProof(t, []string{string(domain.CapOverlayFindNodeV1)})
	if _, err := PrepareSession(p, nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("missing local cap accepted", err)
	}
}

func TestSessionSeedClosedConstructorRootsAndStateShapes(t *testing.T) {
	for _, entry := range []struct {
		typ   reflect.Type
		names []string
	}{
		{reflect.TypeFor[sessionSeedState](), []string{"identity", "proof", "consumed"}},
		{reflect.TypeFor[SessionSeed](), []string{"state"}},
		{reflect.TypeFor[sessionState](), []string{"connID", "identity", "identityProof", "group", "opened", "closed", "sourceWindows", "mu", "nextID", "exhausted", "signer", "signingProjected", "signingActual"}},
	} {
		if entry.typ.NumField() != len(entry.names) {
			t.Fatal("session/seed field inventory changed", entry.typ)
		}
		for i, want := range entry.names {
			if entry.typ.Field(i).Name != want {
				t.Fatal("session/seed field inventory changed", entry.typ, i)
			}
		}
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	made := map[string]map[string]int{"sessionSeedState": {}, "sessionState": {}}
	refs := map[string]map[string]int{"PrepareSession": {}, "Bind": {}, "End": {}}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			where := entry.Name() + ":<package>"
			var node ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				where = entry.Name() + ":" + fn.Name.Name
				node = fn.Body
			}
			calls := map[ast.Node]bool{}
			ast.Inspect(node, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					calls[c.Fun] = true
				}
				return true
			})
			ast.Inspect(node, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok {
						if counts, tracked := made[id.Name]; tracked {
							counts[where]++
						}
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "new" && len(n.Args) == 1 {
						if typ, ok := n.Args[0].(*ast.Ident); ok {
							if _, tracked := made[typ.Name]; tracked {
								t.Error("unreviewed nonzero session allocator", where)
							}
						}
					}
				case *ast.Ident:
					if n.Name == "PrepareSession" {
						refs[n.Name][where]++
						if !calls[n] {
							t.Error("proof constructor function-value escape", where)
						}
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == "Bind" || n.Sel.Name == "End" {
						refs[n.Sel.Name][where]++
						if !calls[n] {
							t.Error("seed/session method-value escape", where)
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(made, map[string]map[string]int{"sessionSeedState": {"session.go:PrepareSession": 1}, "sessionState": {"session.go:Bind": 1}}) {
		t.Fatal("nonzero seed/session constructor escaped", made)
	}
	if !reflect.DeepEqual(refs, map[string]map[string]int{"PrepareSession": {"session.go:NewSession": 1}, "Bind": {"session.go:NewSession": 1}, "End": {"session.go:close": 1}}) {
		t.Fatal("session seed/end call roots changed", refs)
	}
}
