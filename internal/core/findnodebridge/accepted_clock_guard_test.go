package findnodebridge

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// This new port is accepted-original execution only, never first admission or
// snapshot selection. Literal complete domain and exact caller closure keep
// its current-generation authority from replacing old cut-bound admission.
func acceptedClockSourceErrors(sources map[string][]byte) []string {
	var failures []string
	if fmt.Sprintf("%x", sha256.Sum256(sources["internal/core/findnodebridge/domain.go"])) != "65baaa7284945055fb4a2967035baaa3f1567a2b9e4e5310029b877350d22a24" {
		failures = append(failures, "accepted original complete domain Source changed")
	}
	refs := map[string]int{}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "accepted original Source parse")
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			var definition *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if fn.Name.Name == "ReserveAcceptedClock" {
					definition = fn.Name
					if path != "internal/core/findnodebridge/domain.go" {
						failures = append(failures, "accepted port foreign declaration")
					}
				}
			}
			callees := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "ReserveAcceptedClock" && id != definition {
					refs[site]++
					if !callees[id] {
						failures = append(failures, "accepted port method value escaped")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(refs, map[string]int{"internal/core/findnode/owned_bridge_forward_answer.go:MatchOwnedAnswer": 1, "internal/core/findnode/owned_bridge_dispatch.go:Take": 1, "internal/core/findnode/owned_bridge_local_target.go:Take": 1, "internal/core/findnode/owned_bridge_answer.go:Take": 1}) {
		failures = append(failures, "accepted port used outside exact original Take")
	}
	return failures
}
func acceptedClockSources(t *testing.T) map[string][]byte {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	out := map[string][]byte{}
	for _, pkg := range []string{"findnodebridge", "findnode", "node", "netcore"} {
		paths, err := filepath.Glob(filepath.Join(root, "internal", "core", pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			out["internal/core/"+pkg+"/"+filepath.Base(path)] = raw
		}
	}
	return out
}
func TestAcceptedClockClosedSource(t *testing.T) {
	if failures := acceptedClockSourceErrors(acceptedClockSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestAcceptedClockSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"R-before-clock", "d.clock.Lock()\n\td.auth.RLock()", "d.auth.RLock()\n\td.clock.Lock()"},
		{"stale-generation", "stamp := CaptureStamp{captureDomain: d, captureKind: b.kind, generation: d.generation}", "stamp := CaptureStamp{captureDomain: d, captureKind: b.kind, generation: 1}"},
		{"transient-try", "d.auth.RLock()", "if !d.auth.TryRLock(){d.clock.Unlock();return CaptureStamp{},nil,errBusy}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := acceptedClockSources(t)
			path := "internal/core/findnodebridge/domain.go"
			raw := string(sources[path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("accepted control exact raw anchor", tc.name)
			}
			sources[path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(acceptedClockSourceErrors(sources)) == 0 {
				t.Fatal("accepted clock control escaped", tc.name)
			}
		})
	}
	for _, foreign := range []string{"package findnode\nfunc badAccepted(c interface{ReserveAcceptedClock()}){c.ReserveAcceptedClock()}\n", "package findnode\nfunc badValue(c interface{ReserveAcceptedClock()}){_=c.ReserveAcceptedClock}\n"} {
		sources := acceptedClockSources(t)
		sources["internal/core/findnode/accepted_escape.go"] = []byte(foreign)
		if len(acceptedClockSourceErrors(sources)) == 0 {
			t.Fatal("foreign accepted authority escaped")
		}
	}
}
