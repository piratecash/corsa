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

// Finite exact Source closure for two test-only retained cells. No callback,
// unbounded collector, mutable clock or new operational capability is allowed.
func automaticEndpointTraceSourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{"service.go": "4b51bea99468339ff35e5ce9192a093d629a8c73f707d554961d0ae1aedfd01e", "find_node_owned_answer.go": "db16c461c252487076df0515d067202c0d8dbbfd84ea0a3f35fc7b795b2c202e", "find_node_owned_local_target.go": "3f72673fd435bb363ff434ef5fb728d61fc151e06f85b72a9e1a303599983915", "find_node_automatic_endpoint_trace_test.go": "85bd2c0c28681872c43a1b2d56d0f06f9f62c9a40f4c3035c98cf21869b15dc9"}
	var failures []string
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "automatic endpoint exact Source changed: "+path)
		}
	}
	closed := map[string]bool{"ownedFindNodeEndpointTest": true, "ownedFindNodeEndpointTestSlot": true, "relayTraceClaim": true, "relayTraceReady": true, "relayTraceWork": true, "targetTraceClaim": true, "targetTraceReady": true, "targetTraceWork": true, "newOwnedAutomaticTraceService": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "automatic endpoint Source parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && closed[id.Name] && !pinned {
				failures = append(failures, "automatic endpoint retention escaped: "+path+":"+id.Name)
			}
			return true
		})
	}
	return failures
}
func TestOwnedAutomaticEndpointTraceClosedSource(t *testing.T) {
	if failures := automaticEndpointTraceSourceErrors(bareV2PilotSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedAutomaticEndpointTraceSourceControls(t *testing.T) {
	baseline := bareV2PilotSources(t)
	if failures := automaticEndpointTraceSourceErrors(baseline); len(failures) != 0 {
		t.Fatal("automatic endpoint baseline", failures)
	}
	for _, tc := range []struct{ name, file, before, after string }{
		{"relay-without-one-shot", "find_node_owned_answer.go", "slot.relayTraceClaim.CompareAndSwap(false, true)", "true"},
		{"target-without-one-shot", "find_node_owned_local_target.go", "slot.targetTraceClaim.CompareAndSwap(false, true)", "true"},
		{"relay-publish-before-handle", "find_node_owned_answer.go", "slot.relayTraceWork = work\n\t\tslot.relayTraceReady.Store(true)", "slot.relayTraceReady.Store(true)\n\t\tslot.relayTraceWork = work"},
		{"target-publish-before-handle", "find_node_owned_local_target.go", "slot.targetTraceWork = result.Work\n\t\t\tslot.targetTraceReady.Store(true)", "slot.targetTraceReady.Store(true)\n\t\t\tslot.targetTraceWork = result.Work"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for name, raw := range baseline {
				sources[name] = bytes.Clone(raw)
			}
			raw := string(sources[tc.file])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique automatic endpoint control", tc.name)
			}
			sources[tc.file] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(automaticEndpointTraceSourceErrors(sources)) == 0 {
				t.Fatal("automatic endpoint control escaped", tc.name)
			}
		})
	}
	for _, tc := range []struct{ name, source string }{
		{"normal-initializer", "package node\nvar traceEscape=&Service{ownedFindNodeEndpointTest:new(ownedFindNodeEndpointTestSlot)}\n"},
		{"late-setter", "package node\nfunc traceEscape(s *Service){s.ownedFindNodeEndpointTest=new(ownedFindNodeEndpointTestSlot)}\n"},
		{"field-address", "package node\nfunc traceEscape(s *Service){_= &s.ownedFindNodeEndpointTest}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for name, raw := range baseline {
				sources[name] = bytes.Clone(raw)
			}
			sources["automatic_endpoint_escape.go"] = []byte(tc.source)
			if len(automaticEndpointTraceSourceErrors(sources)) == 0 {
				t.Fatal("automatic endpoint OTHERFILE escaped", tc.name)
			}
		})
	}
}
