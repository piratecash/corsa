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

// Finite Source closure: exact construction/advertisement/dispatch code and
// no unlisted typed role reference, setter, alias or reflection-name escape.
func findNodeLocalRoleSourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{
		"find_node_local_role.go":          "783cd95a4c74a9e0461c5befef72da6b5ed33bc2ace1b9376d3d3df567d4b466",
		"find_node_bridge_construction.go": "8aea4067463cb259a8829b41d6bd18e58c920927f90f84fb2a0187c5d03762be",
		"find_node_ready_construction.go":  "bddfb4bb52e603d57769e55fa0fc4f86a4dec0ee5175579e56959d19c617c9f8",
		"service.go":                       "db3c6ab49afae46703dae27a18d8f8a3cc960a0405c6c98949dd94b2babf114f",
		"capabilities.go":                  "78d8d37d7afa6fb5ca4cc1b77cd45b9ee306d1c1f05991aa1ea3aa152fff8137",
		"find_node_owned_receive.go":       "90106224706f71d3a660a2e5d3ef6a77ae5357c1f38e4a110e938cc02b695e51",
		"find_node_resource_diagnostic.go": "e97c7f60a858b6380569d957646ab4338cc16a1bd638dcb25494924b66e91556",
	}
	tracked := map[string]bool{"findNodeLocalRole": true, "findNodeRoleOff": true, "findNodeRoleTarget": true, "findNodeRoleResponder": true, "localFindNodeRole": true, "LocalRole": true, "localRole": true, "originalLocalRole": true}
	var failures []string
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "local role exact Source changed: "+path)
		}
	}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "local role Source parse")
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && tracked[id.Name] {
				if _, closed := pins[path]; !closed {
					failures = append(failures, "unlisted local role reference")
				}
			}
			if literal, ok := n.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				for name := range tracked {
					if strings.Contains(literal.Value, name) {
						failures = append(failures, "local role reflection/name escape")
					}
				}
			}
			return true
		})
	}
	return failures
}

func TestFindNodeLocalRoleClosedSource(t *testing.T) {
	if failures := findNodeLocalRoleSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestFindNodeLocalRoleSourceControls(t *testing.T) {
	baseline := ninN1Sources(t)
	if failures := findNodeLocalRoleSourceErrors(baseline); len(failures) != 0 {
		t.Fatal("local role Source requires clean baseline", failures)
	}
	for _, tc := range []struct{ name, path, before, after string }{
		{"forward gate", "find_node_owned_receive.go", "if s.localFindNodeRole == findNodeRoleOff || (!selfTarget && s.localFindNodeRole != findNodeRoleResponder) {", "if false {"},
		{"first literal", "service.go", "localFindNodeRole:            construction.localFindNodeRole,", "localFindNodeRole:            findNodeRoleResponder,"},
		{"Ready role binding", "find_node_ready_construction.go", "s.localFindNodeRole != r.localRole", "false"},
		{"client responder", "find_node_local_role.go", "case findNodeRoleResponder:\n\t\treturn kind == domain.NodeTypeFull", "case findNodeRoleResponder:\n\t\treturn true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := map[string][]byte{}
			for path, raw := range baseline {
				mutated[path] = bytes.Clone(raw)
			}
			if bytes.Count(mutated[tc.path], []byte(tc.before)) != 1 {
				t.Fatal("local role unique source anchor", tc.name)
			}
			mutated[tc.path] = bytes.Replace(mutated[tc.path], []byte(tc.before), []byte(tc.after), 1)
			if len(findNodeLocalRoleSourceErrors(mutated)) == 0 {
				t.Fatal("local role mutation accepted")
			}
		})
	}
	for _, source := range []string{
		"package node\nvar escapedRole=findNodeRoleResponder\n",
		"package node\nfunc escape(s *Service){s.localFindNodeRole=findNodeRoleResponder}\n",
		"package node\nfunc escape(){_=reflect.ValueOf(Service{}).FieldByName(\"localFindNodeRole\")}\n",
	} {
		mutated := map[string][]byte{}
		for path, raw := range baseline {
			mutated[path] = raw
		}
		mutated["role_escape.go"] = []byte(source)
		if len(findNodeLocalRoleSourceErrors(mutated)) == 0 {
			t.Fatal("OTHERFILE local role escape accepted")
		}
	}
}
