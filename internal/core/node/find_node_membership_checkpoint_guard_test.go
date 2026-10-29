package node

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func qualifiedD1MembershipSource(raw []byte) bool {
	return bytes.HasPrefix(raw, []byte("//go:build d1_resource_probe\n\npackage node\n")) && fmt.Sprintf("%x", sha256.Sum256(raw)) == "39209defff20031f7f093ec2bd86c2c956e105b3699fa70a8963b522290186d8"
}

func d1MembershipSourceErrors(sources map[string][]byte) []string {
	const core = "internal/core/node/find_node_membership_checkpoint.go"
	const app = "internal/app/node/d1_membership_checkpoint.go"
	var failures []string
	if !qualifiedD1MembershipSource(sources[core]) {
		failures = append(failures, "membership Core exact tag/body")
	}
	raw := sources[app]
	if !bytes.HasPrefix(raw, []byte("//go:build d1_resource_probe\n\npackage node\n")) || fmt.Sprintf("%x", sha256.Sum256(raw)) != "871774d6e716f9120db1da47684238fa0d2c90e5e9e8f85c3b1f8c6fe0e629dc" {
		failures = append(failures, "membership App exact tag/body")
	}
	declarations := map[string]int{}
	calls := map[string]int{}
	for path, raw := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"membership parse"}
		}
		for _, decl := range f.Decls {
			site := path + ":<package>"
			allowed := map[*ast.Ident]bool{}
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if fn.Name.Name == "CheckpointForService" || fn.Name.Name == "CheckpointForApp" {
					declarations[site]++
					allowed[fn.Name] = true
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
					if id != nil && (id.Name == "CheckpointForService" || id.Name == "CheckpointForApp") {
						allowed[id] = true
						calls[site+":"+id.Name]++
					}
				}
				if id, ok := n.(*ast.Ident); ok && (id.Name == "CheckpointForService" || id.Name == "CheckpointForApp") && !allowed[id] {
					failures = append(failures, "membership method reference escaped")
				}
				return true
			})
		}
	}
	wantDecl := map[string]int{core + ":CheckpointForService": 1, app + ":CheckpointForApp": 1}
	wantCalls := map[string]int{app + ":CheckpointForApp:CheckpointForService": 1, "internal/core/node/find_node_membership_checkpoint_test.go:readD1Checkpoint:CheckpointForService": 1, "internal/app/node/d1_membership_checkpoint_test.go:readD1AppCheckpoint:CheckpointForApp": 1}
	if len(declarations) != len(wantDecl) || len(calls) != len(wantCalls) {
		failures = append(failures, "membership exact caller inventory")
	}
	for key, count := range wantDecl {
		if declarations[key] != count {
			failures = append(failures, "membership declaration escaped")
		}
	}
	for key, count := range wantCalls {
		if calls[key] != count {
			failures = append(failures, "membership caller escaped")
		}
	}
	return failures
}

func TestD1MembershipCheckpointSourceClosure(t *testing.T) {
	baseline := d1DiagnosticSources(t)
	if failures := d1MembershipSourceErrors(baseline); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct{ name, path, before, after string }{
		{"drop-Core-tag", "internal/core/node/find_node_membership_checkpoint.go", "//go:build d1_resource_probe\n\n", ""},
		{"broaden-Core-tag", "internal/core/node/find_node_membership_checkpoint.go", "//go:build d1_resource_probe", "//go:build d1_resource_probe || linux"},
		{"drop-App-tag", "internal/app/node/d1_membership_checkpoint.go", "//go:build d1_resource_probe\n\n", ""},
		{"broaden-App-tag", "internal/app/node/d1_membership_checkpoint.go", "//go:build d1_resource_probe", "//go:build d1_resource_probe || linux"},
		{"bypass-binding", "internal/core/node/find_node_membership_checkpoint.go", "if !r.boundTo(expected) {", "if false {"},
		{"bypass-generation", "internal/core/node/find_node_membership_checkpoint.go", "after != before", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for path, raw := range baseline {
				sources[path] = bytes.Clone(raw)
			}
			if bytes.Count(sources[tc.path], []byte(tc.before)) != 1 {
				t.Fatal("membership mutation not unique")
			}
			sources[tc.path] = bytes.Replace(sources[tc.path], []byte(tc.before), []byte(tc.after), 1)
			if len(d1MembershipSourceErrors(sources)) == 0 {
				t.Fatal("membership guard bypass accepted")
			}
		})
	}
	for _, raw := range []string{"package node\nfunc escape(r *D1FindNodeResourceReadout,s *Service){r.CheckpointForService(s)}\n", "package node\nfunc escape(r *D1FindNodeResourceReadout){_=r.CheckpointForService}\n"} {
		sources := map[string][]byte{}
		for path, body := range baseline {
			sources[path] = body
		}
		sources["internal/core/node/membership_escape.go"] = []byte(raw)
		if len(d1MembershipSourceErrors(sources)) == 0 {
			t.Fatal("membership caller escape accepted")
		}
	}
	// Original exact role guards stay active and refuse dropped diagnostic tags.
	for _, guard := range []struct {
		name  string
		check func(map[string][]byte) []string
	}{{"degree", degreeSourceErrors}, {"raw", ownedRawF1SourceErrors}, {"observation", ownedNodeObservationSourceErrors}, {"N1", ninN1SourceErrors}, {"local", ownedLocalNodeSourceErrors}} {
		t.Run(guard.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			if failures := guard.check(sources); len(failures) != 0 {
				t.Fatal("membership exact roles refused", failures)
			}
			sources["find_node_membership_checkpoint.go"] = bytes.Replace(sources["find_node_membership_checkpoint.go"], []byte("//go:build d1_resource_probe\n\n"), nil, 1)
			if len(guard.check(sources)) == 0 {
				t.Fatal("untagged membership role accepted")
			}
		})
	}
	if failures := ownedFlowSourceErrors(ownedFlowSources(t)); len(failures) != 0 {
		t.Fatal("membership flow frontier", strings.Join(failures, ";"))
	}
}
