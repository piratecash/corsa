package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// resource_breakdown_exclusion_test.go pins that a domain the breakdown does
// not report is left out ON PURPOSE and says so.
//
// The breakdown's coverage guard walks the ResourceSubsystem enum, and that is
// exactly as strong as the enum: state owned by a domain that has no subsystem
// at all is invisible to it. The file domain is such a domain: it has no
// subsystem of its own, so its omission is recorded as an exclusion with a
// reason rather than left as a silence an operator would read as "holds
// nothing".

// TestFileDomainIsAnExplicitBreakdownExclusion fails in each of the ways the
// exclusions can stop being true. Only the FILE domain is required to be
// accounted for; the other entries are checked for being well-formed, not
// demanded — the test does not require every domain to be reported or
// excluded.
//
//   - the fileMu entry is deleted while no line reports the file domain — the
//     omission goes back to being silent;
//   - a line starts reporting an excluded domain while its entry stays — the
//     list claims an exclusion that no longer exists;
//   - an entry names a mutex Service does not have — a rename left it pointing
//     at nothing;
//   - an entry carries no reason.
func TestFileDomainIsAnExplicitBreakdownExclusion(t *testing.T) {
	t.Parallel()

	domainMutexes := serviceRWMutexFields(t)
	readByBreakdown := domainMutexesReadByBreakdown(t)

	for mutex, reason := range breakdownExcludedDomains {
		if !domainMutexes[mutex] {
			t.Fatalf("exclusion names %q, which is not a sync.RWMutex field of Service", mutex)
		}
		if strings.TrimSpace(reason) == "" {
			t.Fatalf("exclusion of %q carries no reason", mutex)
		}
		if readByBreakdown[mutex] {
			t.Fatalf("%q is excluded but resource_breakdown.go reads under it: drop the exclusion", mutex)
		}
	}

	const fileDomain = "fileMu"
	_, excluded := breakdownExcludedDomains[fileDomain]
	if !excluded && !readByBreakdown[fileDomain] {
		t.Fatalf("%s is neither reported by the breakdown nor listed as an exclusion: file state would read as nobody's",
			fileDomain)
	}
}

// serviceRWMutexFields reads the Service struct declaration and returns the
// names of its sync.RWMutex fields — the candidates for a domain the breakdown
// can report or exclude.
func serviceRWMutexFields(t *testing.T) map[string]bool {
	t.Helper()

	file := parseNodeFile(t, "service.go")
	fields := make(map[string]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "Service" {
			return true
		}
		structType, ok := spec.Type.(*ast.StructType)
		if !ok {
			t.Fatal("Service is not a struct")
		}
		for _, field := range structType.Fields.List {
			if !isSyncRWMutex(field.Type) {
				continue
			}
			for _, name := range field.Names {
				fields[name.Name] = true
			}
		}
		return false
	})
	if len(fields) == 0 {
		t.Fatal("found no sync.RWMutex field in Service: the parse is broken, not the exclusion")
	}
	return fields
}

// domainMutexesReadByBreakdown returns every Service mutex resource_breakdown.go
// takes, by any of s.<mu>.RLock / Lock.
func domainMutexesReadByBreakdown(t *testing.T) map[string]bool {
	t.Helper()

	file := parseNodeFile(t, "resource_breakdown.go")
	read := make(map[string]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.SelectorExpr)
		if !ok || (call.Sel.Name != "RLock" && call.Sel.Name != "Lock") {
			return true
		}
		field, ok := call.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if receiver, ok := field.X.(*ast.Ident); ok && receiver.Name == "s" {
			read[field.Sel.Name] = true
		}
		return true
	})
	return read
}

func isSyncRWMutex(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "RWMutex" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "sync"
}

func parseNodeFile(t *testing.T, name string) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return file
}
