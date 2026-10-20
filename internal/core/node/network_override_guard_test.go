package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// networkOverrideField is the Service field this guard protects.
const networkOverrideField = "networkOverride"

// networkOverrideSite names a function by the file it is declared in and its
// name; a method is named without its receiver.
type networkOverrideSite struct {
	file string
	fn   string
}

func (s networkOverrideSite) String() string { return s.file + ":" + s.fn }

// networkOverrideReaders are the only functions allowed to mention the field
// as a selector, and each of them only reads it:
//   - Network returns the override in place of the bridge;
//   - refuseRealConnectionUnderNetworkOverride refuses to own a connection
//     when one is set.
//
// A selector is how every write is spelled — plain or parenthesised
// assignment, a range clause, taking its address — so allowing the selector
// only here leaves no write a pattern list could miss. Being a reader is
// itself checked: a write inside these functions fails the guard too.
var networkOverrideReaders = []networkOverrideSite{
	{file: "network_bridge.go", fn: "Network"},
	{file: "network_bridge.go", fn: "refuseRealConnectionUnderNetworkOverride"},
}

// networkOverrideConstructors are the only functions allowed to set the field,
// as a key of the struct literal that builds the Service:
//   - newServiceWithRecordAdmissionClasses is the one production construction
//     path (behind newService, NewService and NewServiceWithNetwork);
//   - newTestServiceWithRoutingOverNetwork is the routing tests' struct-literal
//     fixture.
//
// A key anywhere else could overwrite a live Service (`*svc = Service{...}`).
var networkOverrideConstructors = []networkOverrideSite{
	{file: "service.go", fn: "newServiceWithRecordAdmissionClasses"},
	{file: "routing_integration_test.go", fn: "newTestServiceWithRoutingOverNetwork"},
}

// TestNetworkOverrideIsSetOnlyAtConstruction fails on any mention of
// Service.networkOverride outside the allow-lists above, in production or test
// code of this package.
//
// Two facts rest on this. Service.Network() and
// refuseRealConnectionUnderNetworkOverride read the field without a lock,
// which is safe only for a value fixed before the Service exists for any other
// goroutine. And the refusals that keep every registered core reachable by the
// Network that closes it are taken at registration: an override installed
// after a connection was registered would receive that connection's Close,
// answer ErrUnknownConn, and leave its socket and writer open.
//
// The limit of a syntactic check: unsafe pointer arithmetic and reflect can
// still reach the field without naming it. Neither is used on Service, and
// either would be its own review finding.
func TestNetworkOverrideIsSetOnlyAtConstruction(t *testing.T) {
	t.Parallel()

	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %v", err)
	}
	readers := siteSet(networkOverrideReaders)
	constructors := siteSet(networkOverrideConstructors)
	seen := make(map[networkOverrideSite]bool)
	fset := token.NewFileSet()
	var offenders []string
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			site := networkOverrideSite{file: name, fn: declName(decl)}
			ast.Inspect(decl, func(n ast.Node) bool {
				if readers[site] {
					for _, target := range writeTargets(n) {
						if selectsNetworkOverride(target) {
							offenders = append(offenders, fset.Position(target.Pos()).String()+": writes "+networkOverrideField+" inside a function allowed only to read it")
						}
					}
				}
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if node.Sel.Name != networkOverrideField {
						return true
					}
					if !readers[site] {
						offenders = append(offenders, fset.Position(node.Pos()).String()+": "+networkOverrideField+" selector outside its readers")
						return true
					}
					seen[site] = true
				case *ast.KeyValueExpr:
					key, ok := node.Key.(*ast.Ident)
					if !ok || key.Name != networkOverrideField {
						return true
					}
					if !constructors[site] {
						offenders = append(offenders, fset.Position(node.Pos()).String()+": "+networkOverrideField+" literal key outside its constructors")
						return true
					}
					seen[site] = true
				}
				return true
			})
		}
	}

	if len(offenders) > 0 {
		t.Errorf("%d sites reach %s outside the allow-lists; give it to a constructor (NewServiceWithNetwork, or a struct-literal fixture added to networkOverrideConstructors):\n%s",
			len(offenders), networkOverrideField, strings.Join(offenders, "\n"))
	}
	// The allow-lists are only meaningful while every site on them exists and
	// still mentions the field; a renamed function would otherwise leave a
	// stale entry that permits nothing and proves nothing.
	var missing []string
	for _, site := range append(append([]networkOverrideSite(nil), networkOverrideReaders...), networkOverrideConstructors...) {
		if !seen[site] {
			missing = append(missing, site.String())
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("allow-listed sites that no longer mention %s: %s", networkOverrideField, strings.Join(missing, ", "))
	}
}

// writeTargets returns the expressions n writes or exposes for writing: the
// left-hand side of an assignment, the key and value of a range clause, the
// operand of an address-of.
func writeTargets(n ast.Node) []ast.Expr {
	switch node := n.(type) {
	case *ast.AssignStmt:
		return node.Lhs
	case *ast.RangeStmt:
		return []ast.Expr{node.Key, node.Value}
	case *ast.UnaryExpr:
		if node.Op == token.AND {
			return []ast.Expr{node.X}
		}
	}
	return nil
}

// selectsNetworkOverride reports whether expr, through any parentheses, is a
// selector of the field.
func selectsNetworkOverride(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	sel, ok := ast.Unparen(expr).(*ast.SelectorExpr)
	return ok && sel.Sel.Name == networkOverrideField
}

func siteSet(sites []networkOverrideSite) map[networkOverrideSite]bool {
	set := make(map[networkOverrideSite]bool, len(sites))
	for _, site := range sites {
		set[site] = true
	}
	return set
}

// declName is the name of a function declaration, without its receiver, and
// empty for every other declaration — package-level vars included, so a
// mention there matches no allow-list entry.
func declName(decl ast.Decl) string {
	fn, ok := decl.(*ast.FuncDecl)
	if !ok {
		return ""
	}
	return fn.Name.Name
}
