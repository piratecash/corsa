package node

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

// Exact syntactic ownership inventory, not a universal data-flow proof. No
// production constructor reference may enable the fixture-only branch.
func recordAdmissionClosedSource(name string, src []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		return err
	}
	for _, decl := range file.Decls {
		var inspected ast.Node
		functionOwner := "<package>"
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Body == nil {
				continue
			}
			functionOwner, inspected = decl.Name.Name, decl.Body
		case *ast.GenDecl:
			inspected = decl
		default:
			continue
		}
		var refused error
		ast.Inspect(inspected, func(n ast.Node) bool {
			if refused != nil {
				return false
			}
			switch n := n.(type) {
			case *ast.Ident:
				if n.Name == "newIdentityResolverWithRecordAdmission" {
					refused = fmt.Errorf("live opt-in reference in %s", functionOwner)
				}
			case *ast.CompositeLit:
				if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "relationRecordAdmission" && (name != "identity_record_admission.go" || functionOwner != "newIdentityResolverWithRecordAdmission") {
					refused = fmt.Errorf("alternate owner constructor")
				}
			case *ast.TypeSpec:
				if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "relationRecordAdmission" {
					refused = fmt.Errorf("alternate owner type")
				}
			case *ast.KeyValueExpr:
				key, ok := n.Key.(*ast.Ident)
				if !ok {
					break
				}
				if key.Name == "recordAdmission" {
					refused = fmt.Errorf("unreviewed carrier constructor")
				}
				if key.Name == "openFiles" && (name != "identity_record_admission.go" || functionOwner != "newIdentityResolverWithRecordAdmission") {
					refused = fmt.Errorf("unreviewed backend constructor")
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if ok && sel.Sel.Name == "openFiles" {
						refused = fmt.Errorf("backend dependency mutated after construction")
						continue
					}
					if !ok || sel.Sel.Name != "recordAdmission" {
						continue
					}
					permitted := (name == "identity_record_admission.go" && functionOwner == "newIdentityResolverWithRecordAdmission") || (name == "identity_resolver.go" && (functionOwner == "sendOwnedAttempt" || functionOwner == "HandleAnswer"))
					if !permitted {
						refused = fmt.Errorf("unreviewed carrier assignment in %s", functionOwner)
					}
				}
			case *ast.SelectorExpr:
				allowed := map[string]struct{ file, function string }{
					"reserveRelationAttemptLocked":  {"identity_resolver.go", "sendOwnedAttempt"},
					"captureRelationAnswerLocked":   {"identity_resolver.go", "HandleAnswer"},
					"stageRelationAdmission":        {"identity_record_admission.go", "processRelationSlot"},
					"completeOwedRelationRecord":    {"identity_record_admission.go", "processRelationSlot"},
					"confirmRelationRecordCoverage": {"identity_pin_relation_completion.go", "completeOwedRelationRecord"},
					"runRecordAdmission":            {"service.go", "Run"},
					"beginRecordAdmissionRun":       {"service.go", "Run"},
					"processRelationSlot":           {"identity_record_admission.go", "runRecordAdmission"},
					"selectRelationSlotLocked":      {"identity_record_admission.go", "runRecordAdmission"},
				}
				if required, protected := allowed[n.Sel.Name]; protected && (name != required.file || functionOwner != required.function) {
					refused = fmt.Errorf("unreviewed authority reference %s in %s", n.Sel.Name, functionOwner)
				}
			}
			return true
		})
		if refused != nil {
			return refused
		}
	}
	return nil
}
func TestRecordAdmissionClosedProductionConstructionAndReferences(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := recordAdmissionClosedSource(name, src); err != nil {
			t.Fatal(name, err)
		}
	}
	for _, src := range []string{
		`package node;func live(){ _ = newIdentityResolverWithRecordAdmission }`,
		`package node;func live(){ r.recordAdmission = source }`,
		`package node;func live(){ _ = &relationRecordAdmission{} }`,
		`package node;func live(){ _ = r.stageRelationAdmission }`,
		`package node;func live(){ owner.openFiles = replacement }`,
	} {
		if err := recordAdmissionClosedSource("live.go", []byte(src)); err == nil {
			t.Fatal("closed-call guard accepted authority escape", src)
		}
	}
}
func TestRecordAdmissionNoHiddenInventoryAndPhysicalBounds(t *testing.T) {
	expected := map[string][]string{
		"relationRecordAdmission": {"openFiles", "clock", "lastObserved", "clockFailed", "started", "closing", "slots", "rate", "fifo", "wake"},
		"relationAdmissionSlot":   {"generation", "phase", "processing", "cancelled", "protectionHeld", "label", "attemptGeneration", "resolution", "action", "observed", "lifetime", "registered", "queuedAt", "queueAccepted", "verifiedAt", "verified", "fifo", "body", "bodyLen", "signature", "proof", "owed", "completion"},
	}
	check := func(src []byte) error {
		file, err := parser.ParseFile(token.NewFileSet(), "inventory.go", src, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				typ, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				want, tracked := expected[typ.Name.Name]
				if !tracked {
					continue
				}
				structure, ok := typ.Type.(*ast.StructType)
				if !ok {
					return fmt.Errorf("owner not struct")
				}
				names := []string{}
				for _, field := range structure.Fields.List {
					if len(field.Names) == 0 {
						return fmt.Errorf("anonymous hidden state")
					}
					for _, name := range field.Names {
						names = append(names, name.Name)
					}
				}
				if !reflect.DeepEqual(names, want) {
					return fmt.Errorf("unreviewed inventory %s %v", typ.Name.Name, names)
				}
			}
		}
		return nil
	}
	src, err := os.ReadFile("identity_record_admission.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := check(src); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{"hidden map[string]int", "hiddenHistory"} {
		bad := strings.Replace(string(src), "type relationRecordAdmission struct {", "type relationRecordAdmission struct {\n"+extra+"\n", 1)
		bad = strings.ReplaceAll(bad, `\n`, "\n")
		if err := check([]byte(bad)); err == nil {
			t.Fatal("hidden owner state accepted")
		}
	}
	var a relationRecordAdmission
	if len(a.slots) != 256 || len(a.rate.events) != 30 || len(a.slots[0].body) != 2048 || len(a.slots[0].signature) != 64 || len(a.slots[0].proof) != 64 {
		t.Fatal("physical shared bound changed")
	}
	if got := len(a.slots) * (len(a.slots[0].body) + len(a.slots[0].signature) + len(a.slots[0].proof)); got != 557056 {
		t.Fatal("raw bound", got)
	}
	t.Logf("fixed owner bytes %d; raw arrays557056; no whole-process RSS acceptance", unsafe.Sizeof(a))
}
func TestRecordAdmissionStrictWriterCapturesNinStampBeforeIO(t *testing.T) {
	raw, err := os.ReadFile("identity_pin_relation_completion.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	capture := strings.Index(src, "ninStamp := file.InboundQuota.stamp()")
	write := strings.Index(src, "files.WriteMain(ctx, raw)")
	ack := strings.Index(src, "s.recordNinSnapshotWrittenLocked(ninStamp, true)")
	if capture < 0 || write <= capture || ack <= write || !strings.Contains(src, "err != nil || commit != secretfile.DurableCommitted") {
		t.Fatal("strict fixed-stamp ACK ordering changed")
	}
}

// Critic control: the alias is typechecked, then the actual frozen guard must refuse it.
func TestCriticRecordAdmissionPackageAliasCannotEscape(t *testing.T) {
	src := []byte(`package node
 type identityResolver struct{}
 func newIdentityResolverWithRecordAdmission() *identityResolver { return nil }
 var enabled = newIdentityResolverWithRecordAdmission
 func live() { _ = enabled() }
 `)
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "alias.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := types.Config{}
	if _, err := cfg.Check("fixture", fs, []*ast.File{f}, nil); err != nil {
		t.Fatal("counterexample does not compile", err)
	}
	if err := recordAdmissionClosedSource("alias.go", src); err == nil {
		t.Fatal("compiled package-level factory alias escaped frozen closed-authority guard")
	}
}

// Every fixture is typechecked before the SAME actual source guard inspects it.
func TestRecordAdmissionPackageDeclarationsCannotHideAuthority(t *testing.T) {
	cases := []string{
		`package node;type relationRecordAdmission struct{};var leaked = &relationRecordAdmission{}`,
		`package node;type owner struct{};func (*owner) stageRelationAdmission(){};var source = &owner{};var leaked = source.stageRelationAdmission`,
		`package node;type relationRecordAdmission struct{};type alias = relationRecordAdmission;var leaked = &alias{}`,
		`package node;type identityResolver struct{recordAdmission *int};var source int;var leaked = identityResolver{recordAdmission:&source}`,
	}
	for _, src := range cases {
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, "package-authority.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		cfg := types.Config{}
		if _, err := cfg.Check("fixture", fs, []*ast.File{file}, nil); err != nil {
			t.Fatal("counterexample does not compile", err)
		}
		if err := recordAdmissionClosedSource("package-authority.go", []byte(src)); err == nil {
			t.Fatal("compiled package authority escaped actual guard", src)
		}
	}
}
