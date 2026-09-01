package node

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryJournalWriteGoesThroughTheEmissionLane keeps the lane from
// becoming advisory.
//
// The lane's promise — a pre-wire withdrawal waits for at most the one
// statement in flight — holds only while every emission write is inside
// it. One direct call to s.emissionJournal elsewhere puts a statement on a
// connection the lane never counted, and the promise silently becomes
// "whatever SQLite's busy handler decides", which is the state this whole
// mechanism exists to leave. That regression would pass every other test
// in the package, because it changes only WHO waits for whom.
//
// So the shape is checked structurally: a selector on emissionJournal must
// sit inside a function literal, and that literal must be the argument of
// runPreWire or runBookkeeping.
func TestEveryJournalWriteGoesThroughTheEmissionLane(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		// Every function literal that a lane call takes as its argument.
		insideLane := map[ast.Node]struct{}{}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "runPreWire" && sel.Sel.Name != "runBookkeeping") {
				return true
			}
			for _, arg := range call.Args {
				if lit, ok := arg.(*ast.FuncLit); ok {
					insideLane[lit] = struct{}{}
				}
			}
			return true
		})

		// Every write on the journal, and the literal it lives in.
		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			stack = append(stack, n)
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := sel.X.(*ast.SelectorExpr)
			if !ok || receiver.Sel.Name != "emissionJournal" {
				return true
			}
			if sel.Sel.Name != "ClearNeverEmitted" && sel.Sel.Name != "MarkOnWire" {
				return true
			}
			for _, ancestor := range stack {
				if _, ok := insideLane[ancestor]; ok {
					return true
				}
			}
			offenders = append(offenders, fmt.Sprintf("%s:%d calls %s outside the lane",
				name, fset.Position(sel.Pos()).Line, sel.Sel.Name))
			return true
		})
	}

	for _, offender := range offenders {
		t.Errorf("%s — wrap it in emissionLane.runPreWire (a write a frame waits on) "+
			"or runBookkeeping (a record of a frame already gone)", offender)
	}
}
