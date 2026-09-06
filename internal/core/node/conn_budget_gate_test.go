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

// conn_budget_gate_test.go is the guard that keeps the shared connection
// ceiling from acquiring side doors.
//
// ⚠️ It exists because review found two: syncPeer and the notice fallback both
// opened real sockets without a reservation, so "hard shared ceiling" was true
// only of the paths that happened to be wired. Worse, the tests written for
// the fix exercised the budgeted HELPERS — reverting the call sites to a raw
// dial left every one of them green. A test that checks the neighbour of the
// path it claims to protect protects nothing.
//
// So this guard checks the CALL SITES, as a closed set: every raw dial in the
// package must be one of the entries below, and anything new turns the test
// red until somebody decides consciously whether it should be budgeted.
//
// It cannot prove a dial is correctly budgeted — only that no new unreviewed
// one appeared. That is the same shape, and the same limit, as the capability
// guard in anonymity_claim_guard_test.go.
func TestRawDialCallSitesAreAClosedSet(t *testing.T) {
	// file → function that is allowed to dial raw, with the reason.
	allowed := map[string]map[string]string{
		"conn_budget_dial.go": {
			"dialPeerWithBudget":    "the budgeted wrapper itself",
			"dialAddressWithBudget": "the budgeted wrapper itself",
		},
		"peer_sessions.go": {
			// Paid for by the connection manager's slot reservation, taken
			// before the dial starts (slot.reservation). Budgeting it here
			// too would charge one connection twice and halve the ceiling.
			"openPeerSessionForCM": "already covered by the CM slot reservation",
		},
		"socks5.go": {
			"dialPeer": "the transport primitive the wrappers build on",
		},
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		var enclosing string
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				enclosing = node.Name.Name
			case *ast.CallExpr:
				dial, ok := rawDialName(node)
				if !ok {
					return true
				}
				if reason, permitted := allowed[name][enclosing]; permitted {
					_ = reason
					return true
				}
				t.Errorf("%s: %s calls %s directly.\n"+
					"Every outbound socket must draw on the shared connection budget "+
					"(dialPeerWithBudget / dialAddressWithBudget, conn_budget_dial.go), "+
					"otherwise the ceiling has a side door and the diagnostic cannot see the connection.\n"+
					"If this call is genuinely already budgeted elsewhere, add it to the allow-list in "+
					"this test together with the reason.",
					name, enclosing, dial)
			}
			return true
		})
	}
}

// rawDialName reports whether the call is one of the raw dial primitives this
// gate watches, and which one.
func rawDialName(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	switch sel.Sel.Name {
	case "dialPeer":
		return "dialPeer", true
	case "DialTimeout", "Dial":
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "net" {
			return "net." + sel.Sel.Name, true
		}
	}
	return "", false
}

// TestInboundReservationIsTakenInTheAcceptLoop guards the ORDER that the
// inbound ceiling depends on, structurally.
//
// ⚠️ The behavioural test beside it (TestAcceptedSocketsAreAccountedBefore-
// Registration) drives a stand-in for the accept loop, because the real one
// needs a listener, a running Service and a lock held across a flood. A
// stand-in proves the policy is right and proves nothing about where the
// policy is applied — and "the test exercised the neighbour of the path it
// protects" is a mistake this file already exists because of.
//
// So this checks the real accept loop, as source: the reservation must be
// taken there, before the handler goroutine is launched, and
// registerInboundConn must NOT take one of its own — a second reservation
// would mean the socket is charged twice, and reserving only there would
// restore the queue of unaccounted sockets waiting on peerMu.
func TestInboundReservationIsTakenInTheAcceptLoop(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "service.go", nil, 0)
	if err != nil {
		t.Fatalf("parse service.go: %v", err)
	}

	var (
		runBody      *ast.BlockStmt
		registerBody *ast.BlockStmt
	)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		switch fn.Name.Name {
		case "Run":
			runBody = fn.Body
		case "registerInboundConn":
			registerBody = fn.Body
		}
	}
	if runBody == nil || registerBody == nil {
		t.Fatal("could not find Run and registerInboundConn in service.go")
	}

	reservePos := token.NoPos
	ast.Inspect(runBody, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && isBudgetReserve(call) && !reservePos.IsValid() {
			reservePos = call.Pos()
		}
		return true
	})
	if !reservePos.IsValid() {
		t.Fatal("the accept loop does not reserve inbound capacity.\n" +
			"It must reserve BEFORE launching the handler goroutine: the handler blocks on peerMu, " +
			"and while it waits the loop keeps accepting sockets that are open, hold descriptors, " +
			"and are invisible to the budget.")
	}

	handlerPos := token.NoPos
	ast.Inspect(runBody, func(n ast.Node) bool {
		goStmt, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		ast.Inspect(goStmt, func(inner ast.Node) bool {
			if call, ok := inner.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "handleConn" {
					handlerPos = goStmt.Pos()
				}
			}
			return true
		})
		return true
	})
	if !handlerPos.IsValid() {
		t.Fatal("could not find the handler goroutine in the accept loop")
	}

	if reservePos > handlerPos {
		t.Fatalf("the inbound reservation is taken AFTER the handler goroutine is launched "+
			"(reserve at %s, goroutine at %s).\nEvery socket accepted while the handler waits for "+
			"peerMu would be open and unaccounted.",
			fset.Position(reservePos), fset.Position(handlerPos))
	}

	ast.Inspect(registerBody, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && isBudgetReserve(call) {
			t.Errorf("registerInboundConn takes its own reservation at %s.\n"+
				"The unit is reserved by the accept loop and handed in; taking a second one here "+
				"charges the same socket twice.", fset.Position(call.Pos()))
		}
		return true
	})
}

// isBudgetReserve reports whether the call is a reservation against the shared
// connection budget.
func isBudgetReserve(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Reserve" {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "connBudget"
}
