package findnode

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Closed SOURCE inventory, not a universal dataflow or runtime proof. Body
// hashes preserve exact body bytes, including whitespace, comments and literals.
// Raw AST offsets define the boundaries; trusted formatting requires explicit
// pin rebinding. No printer/formatter or body normalization is used.
func ownedOriginalSourceErrors(sources map[string][]byte) []string {
	var failures []string
	finalSum := sha256.Sum256(sources["owned_bridge_commit.go"])
	if hex.EncodeToString(finalSum[:]) != "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255" {
		failures = append(failures, "originals exact final caller raw changed")
	}
	wanted := map[string]string{
		"prepareOwnedOriginalSet": "2f79d5d908f11f7199cda039fc6cb224962777796d560b2130942d55c26ff472",
		"sampleAndCheckOriginals": "b0591f4fc843c4ec0f6e4b7d3b35b3135b23937e3139d236956688a1eeb99fb2",
	}
	signatures := map[string]string{
		"prepareOwnedOriginalSet": "funcprepareOwnedOriginalSet(originals[]Session,maxMembersint)(ownedOriginalSet,error)",
		"sampleAndCheckOriginals": "func(setownedOriginalSet)sampleAndCheckOriginals(lease*findnodebridge.CommitReservation)(findnodebridge.OwnedInstant,error)",
	}
	seen := map[string]int{}
	allowedPrepare, allowedType, allowedSample := map[*ast.Ident]bool{}, map[*ast.Ident]bool{}, map[*ast.Ident]bool{}
	prepareCalls, cutTypes, sampleCalls := 0, 0, 0
	for path, raw := range sources {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			return []string{"originals production parse"}
		}
		declarations := map[*ast.Ident]bool{}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				declarations[fn.Name] = true
			}
		}
		// Only exact Seal preparation, its retained cut field and the typed final sample call extend S3B1.
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && ((path == "owned_bridge_commit.go" && fn.Name.Name == "commitOwnedFinalLocked") || (path == "owned_bridge_receive.go" && fn.Name.Name == "OfferReceived") || (path == "owned_bridge_local_target.go" && fn.Name.Name == "ReserveReceived")) {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok || len(call.Args) != 1 {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "sampleAndCheckOriginals" {
						return true
					}
					receiver, ok := sel.X.(*ast.SelectorExpr)
					if !ok || receiver.Sel.Name != "originals" {
						return true
					}
					cut, ok := receiver.X.(*ast.Ident)
					lease, lok := call.Args[0].(*ast.Ident)
					if ok && lok && (cut.Name == "cut" || (path == "owned_bridge_local_target.go" && cut.Name == "c") || (path == "owned_bridge_receive.go" && cut.Name == "sealed")) && lease.Name == "lease" {
						allowedSample[sel.Sel] = true
						sampleCalls++
					}
					return true
				})
			}
			if fn, ok := decl.(*ast.FuncDecl); ok && path == "owned_bridge_cut.go" && fn.Name.Name == "Seal" {
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "prepareOwnedOriginalSet" {
							allowedPrepare[id] = true
							prepareCalls++
						}
					}
					return true
				})
			}
			if group, ok := decl.(*ast.GenDecl); ok && path == "owned_bridge_cut.go" {
				for _, spec := range group.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok && typ.Name.Name == "ownedCutState" {
						if structure, ok := typ.Type.(*ast.StructType); ok {
							for _, field := range structure.Fields.List {
								if len(field.Names) == 1 && field.Names[0].Name == "originals" {
									if id, ok := field.Type.(*ast.Ident); ok && id.Name == "ownedOriginalSet" {
										allowedType[id] = true
										cutTypes++
									}
								}
							}
						}
					}
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			if _, tracked := wanted[id.Name]; tracked && (!declarations[id] || path != "owned_originals.go") && !allowedPrepare[id] && !allowedSample[id] {
				failures = append(failures, "originals helper gained production caller, duplicate declaration or value escape: "+path)
			}
			if id.Name == "ownedOriginalSet" && path != "owned_originals.go" && !allowedType[id] {
				failures = append(failures, "originals set escaped private unused source: "+path)
			}
			return true
		})
		if path != "owned_originals.go" {
			continue
		}
		imports := map[string]int{}
		for _, imp := range file.Imports {
			imports[strings.Trim(imp.Path.Value, "\"")]++
			if imp.Name != nil {
				failures = append(failures, "originals import alias")
			}
		}
		if !reflect.DeepEqual(imports, map[string]int{"strconv": 1, "github.com/piratecash/corsa/internal/core/domain": 1, "github.com/piratecash/corsa/internal/core/findnodebridge": 1}) {
			failures = append(failures, "originals exact imports changed")
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				want, ok := wanted[decl.Name.Name]
				if !ok || decl.Body == nil {
					failures = append(failures, "originals extra function")
					continue
				}
				seen[decl.Name.Name]++
				signature := raw[fset.Position(decl.Pos()).Offset:fset.Position(decl.Body.Pos()).Offset]
				if strings.Join(strings.Fields(string(signature)), "") != signatures[decl.Name.Name] {
					failures = append(failures, "originals exact signature changed: "+decl.Name.Name)
				}
				start, end := fset.Position(decl.Body.Pos()).Offset, fset.Position(decl.Body.End()).Offset
				sum := sha256.Sum256(raw[start:end])
				if hex.EncodeToString(sum[:]) != want {
					failures = append(failures, "originals exact body changed: "+decl.Name.Name)
				}
			case *ast.GenDecl:
				if decl.Tok == token.IMPORT {
					continue
				}
				if decl.Tok != token.TYPE || len(decl.Specs) != 1 {
					failures = append(failures, "originals extra state declaration")
					continue
				}
				typ, ok := decl.Specs[0].(*ast.TypeSpec)
				if !ok || typ.Name.Name != "ownedOriginalSet" {
					failures = append(failures, "originals extra type")
					continue
				}
				seen["ownedOriginalSet"]++
				start, end := fset.Position(typ.Pos()).Offset, fset.Position(typ.End()).Offset
				if strings.Join(strings.Fields(string(raw[start:end])), "") != "ownedOriginalSetstruct{members[]Session}" {
					failures = append(failures, "originals members-only shape changed")
				}
			default:
				failures = append(failures, "originals extra declaration")
			}
		}
	}
	if prepareCalls != 1 || cutTypes != 1 || sampleCalls != 3 {
		failures = append(failures, "originals exact Seal prepare/cut type inventory changed")
	}
	if !reflect.DeepEqual(seen, map[string]int{"prepareOwnedOriginalSet": 1, "sampleAndCheckOriginals": 1, "ownedOriginalSet": 1}) {
		failures = append(failures, "originals declaration inventory changed")
	}
	return failures
}

func TestOwnedOriginalsClosedSourceAndNoProductionCallers(t *testing.T) {
	if failures := ownedOriginalSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

// In-memory SOURCE mutations only. These are not FUNCTIONAL mutation results.
func TestOwnedOriginalsSourceGuardControls(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"sample-bypass", "at, err := lease.Sample()", "var at findnodebridge.OwnedInstant; var err error"},
		{"tail-omitted", "range set.members {", "range set.members[:len(set.members)-1] {"},
		{"closed-check-removed", "if !member.valid() {", "if false && !member.valid() {"},
		{"borrowed-vector", "members := make([]Session, len(originals))", "members := originals"},
		{"duplicate-allowed", "if _, duplicate := seen[member.state.connID]; duplicate {", "if _, duplicate := seen[member.state.connID]; false && duplicate {"},
		{"group-space-literal", "member.state.group == \"\"", "member.state.group == \" \""},
		{"early-release", "return at, nil", "lease.Release(); return at, nil"},
		{"final-allocation", "return at, nil", "_ = make([]Session, 1); return at, nil"},
		{"extra-authority", "members []Session", "members []Session; ready bool"},
		{"caller-escape", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if tc.name == "caller-escape" {
				sources["originals_escape.go"] = []byte("package findnode\nvar originalsEscape = prepareOwnedOriginalSet\n")
			} else {
				raw := string(sources["owned_originals.go"])
				if strings.Count(raw, tc.before) != 1 {
					t.Fatal("SOURCE control anchor is not unique", tc.before)
				}
				sources["owned_originals.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			}
			if failures := ownedOriginalSourceErrors(sources); len(failures) == 0 {
				t.Fatal("SOURCE control escaped closed inventory")
			}
		})
	}
}
