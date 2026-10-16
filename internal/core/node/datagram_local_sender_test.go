package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// datagram_local_sender_test.go pins WHO each local datagram send is charged
// to (docs/rpc/datagram.md, `local_senders`).
//
// The sender is a positional argument of SendLocal, so forgetting it does not
// compile — but passing the WRONG one does, and a prober sending under the
// resolver's name would put its quota refusals on the subsystem it is starving,
// which is exactly the reading the attribution exists to make impossible.

// localSenderOfFile is the one sender each calling file may name. A file not
// listed here that calls SendLocal fails the guard, and so does a listed file
// that no longer calls it.
var localSenderOfFile = map[string]string{
	"identity_resolver.go":  "LocalSenderIdentityResolver",
	"presence_prober.go":    "LocalSenderPresenceProber",
	"identity_discovery.go": "LocalSenderIdentityDiscovery",
	"dm_control_send.go":    "LocalSenderDMControl",
}

// TestEverySendLocalNamesItsOwnSender walks every non-test file of the package
// and checks each SendLocal call passes datagram.<the constant of its file>.
func TestEverySendLocalNamesItsOwnSender(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	callers := make(map[string]int)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "SendLocal" {
				return true
			}
			callers[name]++
			want, listed := localSenderOfFile[name]
			if !listed {
				t.Errorf("%s calls SendLocal but has no sender of its own in localSenderOfFile", name)
				return true
			}
			if got := senderArgument(call); got != want {
				t.Errorf("%s passes sender %q to SendLocal, want datagram.%s", name, got, want)
			}
			return true
		})
	}
	for name := range localSenderOfFile {
		if callers[name] == 0 {
			t.Errorf("%s is listed as a local sender but no longer calls SendLocal", name)
		}
	}
}

// senderArgument renders the second argument of SendLocal when it is a
// datagram.<Constant> selector, and "" for anything else — a variable or an
// expression is refused too, since only a constant can be checked here.
func senderArgument(call *ast.CallExpr) string {
	if len(call.Args) < 2 {
		return ""
	}
	selector, ok := call.Args[1].(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "datagram" {
		return ""
	}
	return selector.Sel.Name
}
