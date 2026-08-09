package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// run_goroutine_discipline_test.go is the STRUCTURAL BAR on the goroutines this
// package starts.
//
// Six lifecycle findings in six rounds were the same gap in a different place: a
// goroutine that the shutdown asked to stop but never waited for. Cancellation
// only asks. A goroutine that is merely asked can still be inside a dial, a
// handshake, a socket write or a store call when Run returns — and Run returning
// is what the runtime treats as permission to close those underneath it.
//
// This guard has now been wrong twice in the same way, and the shape of the
// error is worth stating because it is what this version is built against.
// Both previous versions worked from an ALLOW-LIST of syntactic forms:
//
//   - v1 looked for `go` statements lexically inside Run. A pool started by a
//     HELPER (gossip dispatch: a supervisor and thirty-six workers) was
//     invisible;
//   - v2 looked at `go func(){...}()` and `s.goBackground(func(){...})`. A
//     METHOD VALUE — `go s.inboundHeartbeat(...)` — was not a function literal,
//     so it was never even examined.
//
// Each time the guard passed while the very class it existed for walked past.
// So this version INVERTS the rule: every `go` statement in the package is
// classified, and a form this guard cannot resolve is a FAILURE rather than a
// silent pass. An unknown shape is loud.

// lifecycleMarker is the comment that vouches for a goroutine this guard would
// otherwise flag. It has to name the group that joins it and why.
const lifecycleMarker = "lifecycle:"

// maxFollowDepth bounds how far longLived chases a wait through package-local
// calls. Two levels covers "the literal calls the loop" and "the loop calls the
// select"; deeper turns every send helper in the package into a wait.
const maxFollowDepth = 2

// otherLifecycles are the goroutines that wait and are correctly NOT on Run's
// group, listed by the function that starts them because the file they live in
// is owned elsewhere and cannot carry an inline marker.
//
// Each entry is a verdict, not a suppression: it says which lifetime owns the
// goroutine. A new one appearing in these functions still has to be added here
// deliberately, and anywhere else in the package still fails.
var otherLifecycles = map[string]string{
	// A per-CONNECTION watcher: it waits on the SESSION's context, not Run's,
	// and closing the socket is what ends it. Run joins the session teardown
	// through connWg and the ConnectionManager, so putting these on runLoopsWg
	// would give one goroutine two owners.
	"openPeerSession":      "per-session context; ends with the session's own teardown",
	"openPeerSessionForCM": "per-session context; ends with the session's own teardown",

	// The ConnectionManager's dial goroutines. cm.dialWg tracks every one of
	// them and cm.shutdown() waits on it, so they have a join — the CM's, not
	// Run's, which is right because a dial belongs to the slot that asked for
	// it. Verified at connection_manager.go:253 ("dialWg tracks in-flight dial
	// goroutines. shutdown() waits for all").
	"handleManualPeer":        "cm.dialWg; joined by ConnectionManager.shutdown",
	"handleActiveSessionLost": "cm.dialWg; joined by ConnectionManager.shutdown",
	"handleDialFailed":        "cm.dialWg; joined by ConnectionManager.shutdown",
	"fill":                    "cm.dialWg; joined by ConnectionManager.shutdown",

	// The five hot-read snapshot tickers. hotReadsRefreshLoop does wg.Add(5)
	// and wg.Wait() around them, and that loop is itself on runLoopsWg — so
	// they are joined transitively, one level down. Verified at
	// hot_reads_refresh.go:113-133.
	"hotReadsRefreshLoop": "local WaitGroup the loop waits on; the loop is on runLoopsWg",

	// The per-session serve goroutine and the pending-drain it spawns: both
	// belong to the SESSION, whose teardown Run joins through connWg and the
	// ConnectionManager.
	"onCMSessionEstablished": "per-session goroutine; ends with the session",
	"servePeerSession":       "one-shot pending drain for a session that just became reachable",

	// The relay state TTL ticker, started by relayStates.start() and stopped by
	// relayStates.stop() — a defer Run registers before the plane, so it runs
	// after every lifecycle join.
	"start": "relayStates' own start/stop pair, sequenced by Run's defers",

	// One-shot fire-and-forget jobs. They are flagged only because this guard
	// follows package-local calls two levels deep and their send helpers
	// contain selects; none of them loops.
	"applyAnnounceEntries":                "one-shot pending drain",
	"triggerDrainForExposed":              "one-shot pending drain",
	"invalidateTransitOnQuarantineLocked": "one-shot transit invalidation",
}

// ---------------------------------------------------------------------------
// What this guard covers, and what it does not
// ---------------------------------------------------------------------------
//
// A goroutine is treated as needing a join when it WAITS — a select, a channel
// receive, or a package-local call within maxFollowDepth that does either. That
// is the observable property of outliving the call that started it.
//
// REMAINING COVERAGE, stated without the pretence of completeness. Two previous
// lists read as exhaustive and each omitted the thing that broke next, so this
// one says plainly what kind of confidence it can offer:
//
//   - the guard is SYNTACTIC and package-local. It resolves `go` targets by
//     NAME within this package. It does not use type information, so a method
//     value on a non-Service receiver resolves to whatever function shares that
//     name, and a call through an interface or a stored func value resolves to
//     nothing — those are reported as UNRECOGNISED and fail, rather than
//     passing;
//   - it follows package-local calls to depth 2. A wait that is three calls
//     down is not seen;
//   - it says nothing about goroutines started in OTHER packages, including
//     ones this package constructs and hands a context to;
//   - it proves MEMBERSHIP, never that a group is actually waited on. The
//     blocking lifecycle tests prove that half;
//   - a goroutine that waits on nothing at all — no select, no receive — is
//     invisible to it. Such a goroutine cannot be asked to stop, which is a
//     worse defect than the one this guard catches, and no join would fix it.
//
// What this version does guarantee, and the previous two did not, is that no
// `go` statement in the package is skipped WITHOUT BEING NAMED. If the guard
// cannot classify something, the test fails and prints it.

// TestNoLongLivedGoroutineEscapesTheLifecycleGroup classifies every goroutine
// this package starts and refuses the ones nothing joins.
//
// The mutation this kills: starting a waiting goroutine — in any syntactic form
// — with a bare `go` or with s.goBackground instead of a joined group.
func TestNoLongLivedGoroutineEscapesTheLifecycleGroup(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list package files: %v", err)
	}

	functions := packageFunctions(t)
	var offences []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		offences = append(offences, unjoinedWaitingGoroutines(t, name, functions)...)
	}

	if len(offences) > 0 {
		t.Fatalf("goroutine(s) outside every join group:\n  %s\n\n"+
			"A goroutine that waits is one the shutdown must WAIT FOR, not merely ask to stop: it can be "+
			"inside a dial, a handshake or a store call when Run returns, and the runtime closes those "+
			"underneath it.\n"+
			"Start it with s.goRunLoop, which is the ONE lifecycle group and is joined by stopRunLifecycle. "+
			"If it belongs to another lifetime — a connection, a session — join it THERE and put a "+
			"`// %s <which group joins it and why>` comment on the line above.\n"+
			"An UNRECOGNISED FORM is also a failure: this guard will not pass a goroutine it cannot classify.",
			strings.Join(offences, "\n  "), lifecycleMarker)
	}
}

// goroutineTarget is what a `go` statement will actually run, or an explicit
// admission that the target could not be resolved.
type goroutineTarget struct {
	body       *ast.BlockStmt
	kind       string
	unresolved bool
}

// resolveGoStatement classifies one `go` statement. EVERY form lands somewhere:
// the ones this guard understands carry a body, the rest are unresolved and
// fail.
func resolveGoStatement(call *ast.CallExpr, functions map[string]*ast.FuncDecl) goroutineTarget {
	switch fun := call.Fun.(type) {
	case *ast.FuncLit:
		return goroutineTarget{body: fun.Body, kind: "go func literal"}
	case *ast.Ident:
		if decl, ok := functions[fun.Name]; ok {
			return goroutineTarget{body: decl.Body, kind: "go " + fun.Name + "(...)"}
		}
		return goroutineTarget{kind: "go " + fun.Name + "(...)", unresolved: true}
	case *ast.SelectorExpr:
		// `go s.method(...)`: the form that hid the inbound heartbeat from the
		// previous guard.
		if decl, ok := functions[fun.Sel.Name]; ok {
			return goroutineTarget{body: decl.Body, kind: "go x." + fun.Sel.Name + "(...)"}
		}
		return goroutineTarget{kind: "go x." + fun.Sel.Name + "(...)", unresolved: true}
	default:
		return goroutineTarget{kind: "go <unrecognised call form>", unresolved: true}
	}
}

// unjoinedWaitingGoroutines reports the goroutines in one file that no group
// joins, plus every `go` statement whose target could not be resolved.
func unjoinedWaitingGoroutines(
	t *testing.T, file string, functions map[string]*ast.FuncDecl,
) []string {
	t.Helper()

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	justified := justifiedLines(fset, parsed)

	var offences []string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if _, owned := otherLifecycles[fn.Name.Name]; owned {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			target, pos, isGoroutine := unjoinedGoroutine(node, functions)
			if !isGoroutine {
				return true
			}
			line := fset.Position(pos).Line
			if justified[line] {
				return true
			}
			switch {
			case target.unresolved:
				offences = append(offences, file+":"+itoa(line)+" in "+fn.Name.Name+
					" — UNRECOGNISED FORM ("+target.kind+"): this guard cannot tell whether it waits")
			case longLived(target.body, functions, 0):
				offences = append(offences, file+":"+itoa(line)+" in "+fn.Name.Name+" ("+target.kind+")")
			}
			return true
		})
	}
	return offences
}

// unjoinedGoroutine returns the target of a goroutine that no lifecycle group
// joins: a `go` statement in any form, or a fire-and-forget goBackground.
func unjoinedGoroutine(
	node ast.Node, functions map[string]*ast.FuncDecl,
) (goroutineTarget, token.Pos, bool) {
	switch typed := node.(type) {
	case *ast.GoStmt:
		return resolveGoStatement(typed.Call, functions), typed.Pos(), true
	case *ast.CallExpr:
		selector, ok := typed.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "goBackground" || len(typed.Args) != 1 {
			return goroutineTarget{}, token.NoPos, false
		}
		switch argument := typed.Args[0].(type) {
		case *ast.FuncLit:
			return goroutineTarget{
				body: argument.Body,
				kind: "goBackground, which Run does not join",
			}, typed.Pos(), true
		case *ast.Ident:
			if decl, found := functions[argument.Name]; found {
				return goroutineTarget{
					body: decl.Body,
					kind: "goBackground(" + argument.Name + "), which Run does not join",
				}, typed.Pos(), true
			}
		}
		return goroutineTarget{
			kind:       "goBackground with an unresolvable argument",
			unresolved: true,
		}, typed.Pos(), true
	}
	return goroutineTarget{}, token.NoPos, false
}

// longLived reports whether a goroutine WAITS — the property that separates a
// loop from a one-shot job — following package-local calls to maxFollowDepth,
// because the wait is often one call away from the goroutine's own body.
func longLived(body *ast.BlockStmt, functions map[string]*ast.FuncDecl, depth int) bool {
	if body == nil {
		return false
	}
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if found {
			return false
		}
		switch typed := node.(type) {
		case *ast.SelectStmt:
			found = true
			return false
		case *ast.UnaryExpr:
			if typed.Op == token.ARROW {
				found = true
				return false
			}
		case *ast.CallExpr:
			if depth >= maxFollowDepth {
				return true
			}
			name, ok := calleeName(typed)
			if !ok {
				return true
			}
			decl, known := functions[name]
			if !known || decl.Body == body {
				return true
			}
			if longLived(decl.Body, functions, depth+1) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// calleeName names the package-local function a call targets, when it has one.
func calleeName(call *ast.CallExpr) (string, bool) {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name, true
	case *ast.SelectorExpr:
		return fun.Sel.Name, true
	}
	return "", false
}

// packageFunctions indexes every function and method of the package by name, so
// a `go` statement naming one can be followed into its body.
func packageFunctions(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list package files: %v", err)
	}
	functions := make(map[string]*ast.FuncDecl)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				functions[fn.Name.Name] = fn
			}
		}
	}
	return functions
}

// justifiedLines collects the lines a `lifecycle:` comment vouches for — the
// line immediately after the comment block that carries the marker.
func justifiedLines(fset *token.FileSet, parsed *ast.File) map[int]bool {
	lines := make(map[int]bool)
	for _, group := range parsed.Comments {
		if !strings.Contains(group.Text(), lifecycleMarker) {
			continue
		}
		lines[fset.Position(group.End()).Line+1] = true
	}
	return lines
}

// itoa keeps the failure message free of a fmt import in a file that otherwise
// needs none.
func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
