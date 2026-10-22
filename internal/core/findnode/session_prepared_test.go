package findnode

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestPreparedSessionSeedConcurrentCopiesOneExactWinner(t *testing.T) {
	seed, err := PrepareSession(seedProof(t, []string{string(domain.CapOverlayFindNodeV1)}), []domain.Capability{domain.CapOverlayFindNodeV1})
	if err != nil {
		t.Fatal(err)
	}
	prepared := seed.state.prepared
	if prepared == nil || prepared.connID != 0 {
		t.Fatal("state was not prepared unpublished")
	}
	for _, args := range []struct {
		id    domain.ConnID
		group domain.SourceGroup
		at    Instant
	}{{0, "loopback", 0}, {1, "", 0}, {1, "loopback", -1}} {
		if _, err := seed.Bind(args.id, args.group, args.at); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid argument accepted", err)
		}
		if seed.state.consumed.Load() || prepared.connID != 0 {
			t.Fatal("invalid argument consumed or filled state")
		}
	}
	const workers = 16 // test contention only; no product capacity/default
	start := make(chan struct{})
	winners := make(chan Session, workers)
	var joined sync.WaitGroup
	for i := 0; i < workers; i++ {
		joined.Add(1)
		go func(copySeed SessionSeed) {
			defer joined.Done()
			<-start
			s, e := copySeed.Bind(17, "loopback", 23)
			if e == nil {
				winners <- s
			} else if !errors.Is(e, ErrInvalid) {
				t.Error(e)
			}
		}(seed)
	}
	close(start)
	joined.Wait()
	close(winners)
	var winner Session
	count := 0
	for s := range winners {
		count++
		winner = s
	}
	if count != 1 || winner.state != prepared || winner.ConnID() != 17 || winner.state.opened != 23 {
		t.Fatal("seed copies did not share one original prepared winner", count)
	}
	if _, err := seed.Bind(17, "loopback", 23); !errors.Is(err, ErrInvalid) {
		t.Fatal("same tuple replaced canonical session", err)
	}
	winner.End()
	if !prepared.closed.Load() {
		t.Fatal("End lost prepared original")
	}
}

// SOURCE complement only: allocation/latch/alias closure, not measured heap use.
func preparedSessionSourceErrors(sources map[string][]byte) []string {
	var failures []string
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	wantBind := `{ if seed.state==nil||seed.state.prepared==nil{return Session{},ErrUnauthenticated} if connID==0||group==""||opened<0{return Session{},ErrInvalid} if !seed.state.consumed.CompareAndSwap(false,true){return Session{},ErrInvalid} s:=seed.state.prepared s.connID,s.identity,s.identityProof=connID,seed.state.identity,seed.state.proof s.group,s.opened=group,opened return Session{state:s},nil }`
	seen := 0
	preparedRefs := map[string]int{}
	for path, raw := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"prepared seed parse"}
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			site := "<package>"
			if ok {
				site = fn.Name.Name
			}
			if path == "session.go" && site == "Bind" {
				var out bytes.Buffer
				_ = printer.Fprint(&out, token.NewFileSet(), fn.Body)
				if compact(out.String()) != compact(wantBind) {
					failures = append(failures, "Bind must fill preallocated state after shared CAS without calls or allocation")
				}
				seen++
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					if n.Sel.Name == "prepared" {
						preparedRefs[path+":"+site]++
					}
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok && (id.Name == "sessionState" || id.Name == "sessionSeedState") && (path != "session.go" || site != "PrepareSession") {
						failures = append(failures, "session memory allocated outside genuine preparation")
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "prepared" {
							failures = append(failures, "prepared session late replacement")
						}
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(preparedRefs, map[string]int{"session.go:Bind": 2}) {
		failures = append(failures, "prepared session exact read/alias roles changed")
	}
	if seen != 1 {
		failures = append(failures, "missing unique Bind body")
	}
	return failures
}
func preparedSessionSources(t *testing.T) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
			b, err := os.ReadFile(e.Name())
			if err != nil {
				t.Fatal(err)
			}
			out[e.Name()] = b
		}
	}
	return out
}
func TestPreparedSessionClosedAllocationAndSharedLatchSource(t *testing.T) {
	if errors := preparedSessionSourceErrors(preparedSessionSources(t)); len(errors) != 0 {
		t.Fatal(errors)
	}
}
func TestPreparedSessionGuardRejectsLateAllocationAndPrivateLatch(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"prepared-alias", "", ""},
		{"late-allocation", "s := seed.state.prepared", "s := &sessionState{}"},
		{"private-latch", "seed.state.consumed.CompareAndSwap(false, true)", "new(atomic.Bool).CompareAndSwap(false, true)"},
		{"consume-invalid", "if connID == 0 || group == \"\" || opened < 0 {", "seed.state.consumed.Store(true)\n\tif connID == 0 || group == \"\" || opened < 0 {"},
		{"late-replacement", "s := seed.state.prepared", "seed.state.prepared = &sessionState{}\n\ts := seed.state.prepared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			raw := string(sources["session.go"])
			if tc.name == "prepared-alias" {
				sources["prepared_escape.go"] = []byte("package findnode\nfunc latePrepared(seed SessionSeed)*sessionState{return seed.state.prepared}\n")
			} else {
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("prepared seed selfcase anchor")
				}
				sources["session.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if len(preparedSessionSourceErrors(sources)) == 0 {
				t.Fatal("prepared seed SOURCE escape accepted")
			}
		})
	}
}
