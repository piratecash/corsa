package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// netcore_fixture_guard_test.go pins that every NetCore a test in this
// package builds releases its writer goroutine when that test ends. Closing
// the pipe ends is not enough: an idle writerLoop only returns on Close, so a
// test or helper without it leaked one goroutine and its buffers per call,
// and a stress test that calls such a helper a thousand times per run kept a
// thousand writers alive per run.
//
// The invariant has two halves. newTestNetCore closes the core on cleanup —
// pinned at run time below, for the constructor itself and for the helpers
// built on it — and it is the only place test code may call netcore.New —
// pinned structurally by TestNetCoresInTestsAreBuiltOnlyByNewTestNetCore.
//
// The run-time observable is WriterDone: on a core that never wrote, it closes
// only when writerLoop returns, so "closed after the subtest's cleanup" is
// exactly "the writer goroutine is gone". (failedNetCore is not covered here:
// its writer has already failed a write by design, which closes WriterDone
// while the loop is still alive, and the loop's own exit signal is not
// exported.)

func TestNetCoreTestHelpersReleaseTheirWriterOnCleanup(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T) *netcore.NetCore{
		"newTestNetCore": func(t *testing.T) *netcore.NetCore {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = remote.Close() })
			return newTestNetCore(t, netcore.ConnID(9600), local, netcore.Outbound, netcore.Options{})
		},
		"attachPushObserver": func(t *testing.T) *netcore.NetCore {
			svc := newInitMapsTestService(t, nil)
			_ = attachPushObserver(t, svc, "cleanup-recipient", netcore.ConnID(9601))
			return svc.netCoreForID(domain.ConnID(9601))
		},
		"legacyInboundConn": func(t *testing.T) *netcore.NetCore {
			svc := newInitMapsTestService(t, nil)
			connID := legacyInboundConn(t, svc, netcore.ConnID(9602),
				&net.TCPAddr{IP: net.IPv4(203, 0, 113, 9), Port: 64646}, domain.PeerIdentity{})
			return svc.netCoreForID(connID)
		},
		"newQueueTestSession": func(t *testing.T) *netcore.NetCore {
			session, _ := newQueueTestSession(t, domain.PeerAddress("10.0.0.96:64646"), 4)
			return session.netCore
		},
		"attachTestNetCore": func(t *testing.T) *netcore.NetCore {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close() })
			t.Cleanup(func() { _ = remote.Close() })
			session := &peerSession{address: domain.PeerAddress("10.0.0.97:64646"), conn: local}
			attachTestNetCore(t, session)
			return session.netCore
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			var core *netcore.NetCore
			t.Run("helper", func(t *testing.T) {
				core = build(t)
				if core == nil {
					t.Fatal("fixture: the helper built no NetCore")
				}
			})
			// The inner subtest has returned, so its cleanups have run.
			select {
			case <-core.WriterDone():
			default:
				t.Fatalf("%s left its NetCore writer goroutine running after the test's cleanup", name)
			}
		})
	}
}

// netcorePackagePath is the import path the structural guard below resolves
// the netcore constructors through, so an aliased import cannot hide a call
// from it.
const netcorePackagePath = "github.com/piratecash/corsa/internal/core/netcore"

// newTestNetCoreName is the function that alone may call netcore.New in this
// package's tests.
const newTestNetCoreName = "newTestNetCore"

// netcoreConstructors are the netcore functions that start a writer goroutine.
// NewBootstrap is included because it is New with a different write deadline:
// a test building a core through it would leak the same way.
var netcoreConstructors = map[string]bool{"New": true, "NewBootstrap": true}

// TestNetCoresInTestsAreBuiltOnlyByNewTestNetCore keeps the closing of every
// test-built NetCore in one place. A core built by a direct constructor call
// in a test leaks its writer goroutine unless that test remembered to close
// it, and nothing at run time says it forgot — the leak only shows up as
// memory a whole package run never gives back. newTestNetCore closes the core
// on cleanup, so the invariant is structural: no test file outside it may
// reach netcore.New or netcore.NewBootstrap — called or passed as a value,
// under any of the names the file imports netcore by, or bound to a local
// symbol by a go:linkname directive.
//
// What it cannot see is a non-test function of package node that calls a
// constructor: production code must call netcore.New, so such a call cannot
// be banned, and a core a test obtains through production code (for example
// registerInboundConn) is owned by that code, not by the test.
func TestNetCoresInTestsAreBuiltOnlyByNewTestNetCore(t *testing.T) {
	t.Parallel()

	names, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("list the package's test files: %v", err)
	}
	fset := token.NewFileSet()
	var offenders []string
	constructorRefs := 0
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		offenders = append(offenders, linknamedConstructors(fset, file)...)
		locals := netcoreImportNames(file)
		if locals["."] {
			offenders = append(offenders, name+": dot-imports netcore, which hides its constructors from this check")
		}
		for _, decl := range file.Decls {
			inConstructor := isFuncNamed(decl, newTestNetCoreName)
			for _, pos := range netcoreConstructorReferences(decl, locals) {
				if inConstructor {
					constructorRefs++
					continue
				}
				offenders = append(offenders, fset.Position(pos).String())
			}
		}
	}

	if len(offenders) > 0 {
		t.Errorf("%d test sites build a NetCore through a netcore constructor instead of %s, so nothing guarantees their writer goroutine is closed:\n%s",
			len(offenders), newTestNetCoreName, strings.Join(offenders, "\n"))
	}
	if constructorRefs != 1 {
		t.Errorf("%s references the netcore constructors %d times, want exactly 1 (its own netcore.New call): the guard above is only meaningful while that one call exists",
			newTestNetCoreName, constructorRefs)
	}
}

// netcoreImportNames returns every name file refers to the netcore package by.
// A file may import the same path more than once under different names, and
// each of them reaches the constructors.
func netcoreImportNames(file *ast.File) map[string]bool {
	names := make(map[string]bool)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != netcorePackagePath {
			continue
		}
		if spec.Name != nil {
			names[spec.Name.Name] = true
			continue
		}
		names[filepath.Base(netcorePackagePath)] = true
	}
	return names
}

// linknamedConstructors reports every go:linkname directive in file that binds
// a local symbol to a netcore constructor, which builds a NetCore without any
// selector expression to find.
func linknamedConstructors(fset *token.FileSet, file *ast.File) []string {
	var found []string
	for _, group := range file.Comments {
		for _, comment := range group.List {
			fields := strings.Fields(comment.Text)
			if len(fields) != 3 || fields[0] != "//go:linkname" {
				continue
			}
			target, ok := strings.CutPrefix(fields[2], netcorePackagePath+".")
			if ok && netcoreConstructors[target] {
				found = append(found, fset.Position(comment.Pos()).String()+": go:linkname to netcore."+target)
			}
		}
	}
	return found
}

func isFuncNamed(decl ast.Decl, name string) bool {
	fn, ok := decl.(*ast.FuncDecl)
	return ok && fn.Recv == nil && fn.Name.Name == name
}

// netcoreConstructorReferences returns the position of every reference to one
// of netcoreConstructors through any of locals in decl, whether it is called
// or used as a function value.
func netcoreConstructorReferences(decl ast.Decl, locals map[string]bool) []token.Pos {
	var found []token.Pos
	ast.Inspect(decl, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok || !netcoreConstructors[sel.Sel.Name] {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && locals[pkg.Name] {
			found = append(found, sel.Pos())
		}
		return true
	})
	return found
}
