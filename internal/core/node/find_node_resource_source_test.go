package node

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

func qualifiedD1DiagnosticSource(raw []byte) bool {
	return bytes.HasPrefix(raw, []byte("//go:build d1_resource_probe\n\npackage node\n")) && fmt.Sprintf("%x", sha256.Sum256(raw)) == "63112a5bba4b8e2832680b47ae904bdf50f64806f5c707628c3a6604f9dfe549"
}

// Finite repository Source closure. No claim about arbitrary unsafe/dynamic
// dataflow; exact writer bytes, build selection and known callers are checked.
func d1DiagnosticSources(t *testing.T) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join("../../..", top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("Source symlink")
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel("../../..", path)
			if err != nil {
				return err
			}
			result[filepath.ToSlash(relative)] = raw
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func d1DiagnosticSourceErrors(sources map[string][]byte) []string {
	const name = "NewD1FindNodeResourceDiagnostic"
	const core = "internal/core/node/find_node_resource_diagnostic.go"
	const app = "internal/app/node/d1_resource_diagnostic.go"
	var failures []string
	if !qualifiedD1DiagnosticSource(sources[core]) {
		failures = append(failures, "diagnostic core exact tag/body")
	}
	raw := sources[app]
	if !bytes.HasPrefix(raw, []byte("//go:build d1_resource_probe\n\npackage node\n")) || fmt.Sprintf("%x", sha256.Sum256(raw)) != "ad89647006826542e67057dd0142b76b6f3fe82de3d8fa18b85981a6b90fb3ef" {
		failures = append(failures, "diagnostic App exact tag/body")
	}
	declarations, calls := 0, 0
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"diagnostic Source parse"}
		}
		for _, decl := range file.Decls {
			site := path + ":<package>"
			allowed := map[*ast.Ident]bool{}
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if fn.Name.Name == name {
					allowed[fn.Name] = true
					declarations++
					if site != core+":"+name && site != app+":"+name {
						failures = append(failures, "diagnostic constructor declaration escaped")
					}
				}
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					var id *ast.Ident
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						id = fun
					case *ast.SelectorExpr:
						id = fun.Sel
					}
					if id != nil && id.Name == name {
						allowed[id] = true
						calls++
						if site != app+":"+name && site != "internal/core/node/find_node_resource_diagnostic_test.go:newD1ResourceForTest" && site != "internal/app/node/d1_resource_diagnostic_test.go:newD1ResourceAppForTest" {
							failures = append(failures, "diagnostic constructor caller escaped")
						}
					}
				}
				if id, ok := n.(*ast.Ident); ok && id.Name == name && !allowed[id] {
					failures = append(failures, "diagnostic constructor reference escaped")
				}
				return true
			})
		}
	}
	if declarations != 2 || calls != 3 {
		failures = append(failures, "diagnostic exact declaration/call inventory")
	}
	return failures
}

func TestD1FindNodeResourceDiagnosticSourceClosure(t *testing.T) {
	sources := d1DiagnosticSources(t)
	if failures := d1DiagnosticSourceErrors(sources); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, path, before, after string }{
		{"core drop tag", "internal/core/node/find_node_resource_diagnostic.go", "//go:build d1_resource_probe\n\n", ""},
		{"core broaden tag", "internal/core/node/find_node_resource_diagnostic.go", "//go:build d1_resource_probe", "//go:build d1_resource_probe || linux"},
		{"App drop tag", "internal/app/node/d1_resource_diagnostic.go", "//go:build d1_resource_probe\n\n", ""},
		{"App broaden tag", "internal/app/node/d1_resource_diagnostic.go", "//go:build d1_resource_probe", "//go:build d1_resource_probe || darwin"},
		{"profile mutation", "internal/core/node/find_node_resource_diagnostic.go", "MaxReservations: 8", "MaxReservations: 4"},
		{"guard bypass", "internal/core/node/find_node_resource_diagnostic.go", "if !r.boundTo(expected) {", "if false {"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := map[string][]byte{}
			for path, raw := range sources {
				mutated[path] = bytes.Clone(raw)
			}
			if bytes.Count(mutated[tc.path], []byte(tc.before)) != 1 {
				t.Fatal("mutation not unique")
			}
			mutated[tc.path] = bytes.Replace(mutated[tc.path], []byte(tc.before), []byte(tc.after), 1)
			if len(d1DiagnosticSourceErrors(mutated)) == 0 {
				t.Fatal("tag/body escape accepted")
			}
		})
	}
	for _, raw := range []string{"package main\nfunc escape(){node.NewD1FindNodeResourceDiagnostic()}\n", "package main\nvar escape=node.NewD1FindNodeResourceDiagnostic\n"} {
		mutated := map[string][]byte{}
		for path, body := range sources {
			mutated[path] = body
		}
		mutated["cmd/d1_escape.go"] = []byte(raw)
		if len(d1DiagnosticSourceErrors(mutated)) == 0 {
			t.Fatal("caller escape accepted")
		}
	}
	// The original advertisement guard must independently refuse tag weakening.
	pilot := bareV2PilotSources(t)
	for _, replacement := range []string{"", "//go:build d1_resource_probe || linux\n\n"} {
		mutated := map[string][]byte{}
		for path, raw := range pilot {
			mutated[path] = bytes.Clone(raw)
		}
		mutated["find_node_resource_diagnostic.go"] = bytes.Replace(mutated["find_node_resource_diagnostic.go"], []byte("//go:build d1_resource_probe\n\n"), []byte(replacement), 1)
		if len(bareV2PilotSourceErrors(mutated)) == 0 {
			t.Fatal("original writer guard ignored diagnostic tag")
		}
	}
}

// Runs under both default and diagnostic-tag selection. No Service.Run or
// listener is started. Ordinary Ready is also F1-off after genuine Seal.
func TestD1FindNodeResourceDiagnosticOrdinaryFactoriesOff(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	makeConfig := func() config.Node {
		return configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true, Type: domain.NodeTypeFull})
	}
	stock := NewService(makeConfig(), id, nil)
	t.Cleanup(func() { stock.runCancel(); stock.closeBackgroundAdmission(); stock.WaitBackground() })
	cfg := makeConfig()
	r, err := prepareFindNodeReadyConstruction(cfg, id, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250000000, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	ready, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Service{stock, ready} {
		for _, name := range s.nodeHelloFrame().Capabilities {
			if name == string(domain.CapOverlayFindNodeV1) {
				t.Fatal("ordinary factory advertised F1")
			}
		}
	}
	if stock.bridgeCapture != nil || stock.findNode != nil {
		t.Fatal("ordinary Service installed FindNode")
	}
}

// All affected pre-existing authority guards must accept only the reviewed
// exact diagnostic roles. Dropped tag gives no frontier/inventory allowance.
func TestD1FindNodeResourceDiagnosticExistingGuardBoundaries(t *testing.T) {
	for _, guard := range []struct {
		name  string
		check func(map[string][]byte) []string
	}{
		{"degree", degreeSourceErrors}, {"priority", c5SourceErrors}, {"terminal", ownedTerminalSourceErrors},
		{"F1", ownedF1SourceErrors}, {"observation", ownedNodeObservationSourceErrors},
		{"local", ownedLocalNodeSourceErrors}, {"N1", ninN1SourceErrors},
	} {
		t.Run(guard.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			if failures := guard.check(sources); len(failures) != 0 {
				t.Fatal("reviewed diagnostic roles refused", failures)
			}
			sources["find_node_resource_diagnostic.go"] = bytes.Replace(sources["find_node_resource_diagnostic.go"], []byte("//go:build d1_resource_probe\n\n"), nil, 1)
			if len(guard.check(sources)) == 0 {
				t.Fatal("untagged diagnostic authority accepted")
			}
		})
	}

	// Existing K5 descriptor origin is a distinct, reviewed role. Keep exact
	// missing/duplicate/unlisted/alias refusals rather than exempting the file.
	for _, tc := range []struct{ name, before, after, cause string }{
		{"K5-missing-origin-read", "int64(r.origin.coverageStart)", "int64(r.target.coverageStart)", "degree all-Node origin reference closure changed"},
		{"K5-duplicate-assignment", "\tr.origin = origin\n", "\tr.origin = origin\n\tr.origin = origin\n", "degree all-Node origin reference closure changed"},
		{"K5-unlisted-read", "return handshakePastCursor{at: int64(r.origin.coverageStart)}", "_ = r.origin\n\treturn handshakePastCursor{at: int64(r.origin.coverageStart)}", "degree all-Node origin reference closure changed"},
		{"K5-address-alias", "return handshakePastCursor{at: int64(r.origin.coverageStart)}", "_ = &r.origin\n\treturn handshakePastCursor{at: int64(r.origin.coverageStart)}", "degree origin address alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			path := "k5_handshake_past.go"
			if bytes.Count(sources[path], []byte(tc.before)) != 1 {
				t.Fatal("K5 origin mutation not unique")
			}
			sources[path] = bytes.Replace(sources[path], []byte(tc.before), []byte(tc.after), 1)
			failures := strings.Join(degreeOriginReferenceErrors(sources), "\n")
			if !strings.Contains(failures, tc.cause) {
				t.Fatal("K5 origin role escape accepted", failures)
			}
		})
	}

	for _, guard := range []struct {
		name   string
		source func(*testing.T) map[string][]byte
		check  func(map[string][]byte) []string
	}{
		{"Ready", ownedReadySources, ownedReadySourceErrors}, {"flow", ownedFlowSources, ownedFlowSourceErrors},
	} {
		t.Run(guard.name, func(t *testing.T) {
			sources := guard.source(t)
			if failures := guard.check(sources); len(failures) != 0 {
				t.Fatal("reviewed diagnostic frontier refused", failures)
			}
			path := "internal/core/node/find_node_resource_diagnostic.go"
			sources[path] = bytes.Replace(sources[path], []byte("//go:build d1_resource_probe\n\n"), nil, 1)
			if len(guard.check(sources)) == 0 {
				t.Fatal("untagged diagnostic frontier accepted")
			}
		})
	}
}
