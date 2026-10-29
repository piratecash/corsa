package node

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
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
				if (n.Name == "newIdentityResolverWithRecordAdmission" || n.Name == "newIdentityResolverWithSessionRecordAdmission") && (name != "service.go" || functionOwner != "newServiceWithConstruction") {
					refused = fmt.Errorf("live opt-in reference in %s", functionOwner)
				}
			case *ast.CompositeLit:
				if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "relationRecordAdmission" && (name != "identity_record_admission.go" || (functionOwner != "newIdentityResolverWithRecordAdmission" && functionOwner != "newIdentityResolverWithContactRecordAdmission" && functionOwner != "newIdentityResolverWithSessionRecordAdmission")) {
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
				if key.Name == "openFiles" && (name != "identity_record_admission.go" || (functionOwner != "newIdentityResolverWithRecordAdmission" && functionOwner != "newIdentityResolverWithSessionRecordAdmission")) {
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
					permitted := (name == "identity_record_admission.go" && (functionOwner == "newIdentityResolverWithRecordAdmission" || functionOwner == "newIdentityResolverWithContactRecordAdmission" || functionOwner == "newIdentityResolverWithSessionRecordAdmission")) || (name == "identity_resolver.go" && (functionOwner == "sendOwnedAttempt" || functionOwner == "HandleAnswer"))
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
		`package node;func live(){ _ = newIdentityResolverWithSessionRecordAdmission }`,
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
		"relationRecordAdmission":              {"classes", "kSource", "service", "resolver", "network", "kStageMu", "kReady", "kClosing", "kFiles", "openFiles", "clock", "lastObserved", "clockFailed", "started", "closing", "slots", "rate", "fifo", "r2PendingCursor", "wake"},
		"contactRecordReconciliationAuthority": {"admission", "resolver", "service", "network", "index", "generation", "ctx", "cancel", "candidate", "revoked", "finished", "staged"},
		"contactRecordStageAuthority":          {"admission", "resolver", "service", "network", "index", "generation", "ctx", "cancel", "candidate", "revoked", "finished", "staged"},
		"recordAdmissionConstruction":          {"classes", "kSource", "borrowedKFiles"},
		"relationAdmissionSlot":                {"generation", "phase", "processing", "cancelled", "protectionHeld", "label", "attemptGeneration", "resolution", "action", "observed", "lifetime", "registered", "queuedAt", "queueAccepted", "verifiedAt", "verified", "fifo", "body", "bodyLen", "signature", "proof", "owed", "completion", "kAuthority", "kReconciliation", "r2Claim", "r2Role", "r2Verified"},
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
	reconciliationSource, err := os.ReadFile("identity_pin_contact_reconciliation.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := check(reconciliationSource); err != nil {
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

// Closed syntactic reference multiset. No function value, GenDecl alias or
// alternative capsule/preparation construction becomes a new authority root.
func contactPushSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"newContactRecordPushCandidateService":           {},
		"newContactRecordReconciliationCandidateService": {},
		"newRelationRecordCandidateService":              {},
		"newSessionRecordCandidateService":               {},
		"newIdentityResolverWithSessionRecordAdmission":  {"service.go:newServiceWithConstruction": 1},
		"newIdentityResolverWithRecordAdmission":         {"service.go:newServiceWithConstruction": 1},
		"hasContactRecordPushTransportCapability":        {"session_secure.go:acceptSecureInbound": 1, "session_secure.go:dialV2": 1, "session_secure.go:provenTransport": 1},
		"newServiceWithRecordAdmissionClasses":           {"service.go:newServiceWithDatagramConstructor": 1, "service.go:newContactRecordPushCandidateService": 1, "service.go:newRelationRecordCandidateService": 1, "service.go:newContactRecordReconciliationCandidateService": 1, "service.go:newSessionRecordCandidateService": 1, "identity_record_admission_own_dm.go:NewInactiveOwnDMAdmissionCandidate": 1},
		"newIdentityResolverWithContactRecordAdmission":  {"service.go:newServiceWithConstruction": 1},
		"prepareContactRecordPush":                       {"identity_discovery.go:Handle": 1},
		"reserveContactRecordPush":                       {"identity_record_admission.go:prepareContactRecordPush": 1},
		"finishContactRecordPush":                        {"identity_record_admission.go:prepareContactRecordPush": 2, "identity_discovery.go:Handle": 1},
		"settleContactRecordPush":                        {"identity_discovery.go:Handle": 1},
		"beginContactRecordPushRun":                      {"service.go:Run": 1},
		"closeContactRecordPushes":                       {"service.go:Run": 1},
		"revokeContactRecordPushesLocked":                {"identity_record_admission.go:stopRecordAdmissionLocked": 1, "identity_record_admission.go:closeContactRecordPushes": 1},
		"stageContactRecordPush":                         {"identity_record_admission.go:settleContactRecordPush": 1},
		"stageContactRecordWithAuthority":                {"identity_pin_contact_record.go:stageContactRecord": 1, "identity_pin_contact_record.go:stageContactRecordPush": 1, "identity_pin_contact_reconciliation.go:stageContactRecordReconciliation": 1},
		"prepareContactRecordStageLocked":                {"identity_pin_contact_record.go:stageContactRecordWithAuthority": 1},
		"validForStageLocked":                            {"identity_pin_contact_record.go:stageContactRecordWithAuthority": 1},
		"contactRecordStageAuthority":                    {"identity_record_admission.go:<package>": 1, "identity_record_admission.go:reserveContactRecordPush": 2, "identity_record_admission.go:finishContactRecordPush": 1, "identity_record_admission.go:prepareContactRecordPush": 1, "identity_record_admission.go:settleContactRecordPush": 1, "identity_discovery.go:Handle": 1, "identity_pin_contact_record.go:stageContactRecordPush": 1, "identity_pin_contact_record.go:stageContactRecordWithAuthority": 1, "identity_pin_contact_record.go:validForStageLocked": 1},
		"scanReconciliation":                             {"identity_pin_contact.go:scan": 1},
		"captureReconciliation":                          {"identity_pin_contact_reconciliation.go:scanReconciliation": 1},
		"reserveContactRecordReconciliation":             {"identity_pin_contact_reconciliation.go:scanReconciliation": 1},
		"finishContactRecordReconciliation":              {"identity_pin_contact_reconciliation.go:scanReconciliation": 1},
		"stageContactRecordReconciliation":               {"identity_pin_contact_reconciliation.go:scanReconciliation": 1},
		"validForReconciliationStageLocked":              {"identity_pin_contact_record.go:stageContactRecordWithAuthority": 1},
		"admitsContactPush":                              {"identity_record_admission.go:hasContactRecordPushTransportCapability": 1, "identity_record_admission.go:reserveContactRecordPush": 1, "identity_pin_contact_record.go:stageContactRecordWithAuthority": 1, "identity_pin_contact_record.go:validForStageLocked": 1},
		"admitsContactReconciliation":                    {"identity_pin_contact_reconciliation.go:scanReconciliation": 1, "identity_pin_contact_reconciliation.go:reserveContactRecordReconciliation": 1, "identity_pin_contact_reconciliation.go:validForReconciliationStageLocked": 1, "identity_pin_contact_record.go:stageContactRecordWithAuthority": 1},
		"contactRecordReconciliationAuthority":           {"identity_record_admission.go:<package>": 1, "identity_pin_contact_reconciliation.go:reserveContactRecordReconciliation": 2, "identity_pin_contact_reconciliation.go:finishContactRecordReconciliation": 1, "identity_pin_contact_reconciliation.go:stageContactRecordReconciliation": 1, "identity_pin_contact_reconciliation.go:validForReconciliationStageLocked": 1, "identity_pin_contact_record.go:stageContactRecordWithAuthority": 1},
		"preparedContactRecordStage":                     {"identity_pin_contact_record.go:prepareContactRecordStageLocked": 2, "identity_pin_contact_record.go:matchesLocked": 1, "identity_pin_contact_record.go:stageContactRecordLocked": 1},
	}
	constructors := map[string]map[string]int{
		"contactRecordStageAuthority":          {"identity_record_admission.go:reserveContactRecordPush": 1},
		"contactRecordReconciliationAuthority": {"identity_pin_contact_reconciliation.go:reserveContactRecordReconciliation": 1},
		"preparedContactRecordStage":           {"identity_pin_contact_record.go:prepareContactRecordStageLocked": 1},
		"contactRecordCandidate":               {"identity_pin_contact_record.go:observeContactRecord": 1, "identity_pin_contact_reconciliation.go:captureReconciliation": 1},
		"recordAdmissionConstruction":          {"find_node_ready_construction.go:buildWithOptions": 3, "service.go:newServiceWithDatagramConstructor": 1, "service.go:newContactRecordPushCandidateService": 1, "service.go:newRelationRecordCandidateService": 1, "service.go:newContactRecordReconciliationCandidateService": 1, "service.go:newSessionRecordCandidateService": 1, "identity_record_admission_own_dm.go:NewInactiveOwnDMAdmissionCandidate": 1},
		"relationRecordAdmission":              {"identity_record_admission.go:newIdentityResolverWithRecordAdmission": 1, "identity_record_admission.go:newIdentityResolverWithContactRecordAdmission": 1, "identity_record_admission.go:newIdentityResolverWithSessionRecordAdmission": 1},
	}
	combinedClasses := func(expression ast.Expr) bool {
		binary, ok := expression.(*ast.BinaryExpr)
		if !ok || binary.Op != token.OR {
			return false
		}
		left, l := binary.X.(*ast.Ident)
		right, r := binary.Y.(*ast.Ident)
		return l && r && left.Name == "recordAdmissionChargedR" && right.Name == "recordAdmissionImmediateK"
	}
	actual, made := map[string]map[string]int{}, map[string]map[string]int{}
	for key := range expected {
		actual[key] = map[string]int{}
	}
	for key := range constructors {
		made[key] = map[string]int{}
	}
	var failures []string
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range file.Decls {
			owner := "<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner, declaration = fn.Name.Name, fn.Name
			}
			site := name + ":" + owner
			callees := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					callees[call.Fun] = true
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n == declaration {
						return true
					}
					typeDeclaration := false
					ast.Inspect(decl, func(parent ast.Node) bool {
						if typ, ok := parent.(*ast.TypeSpec); ok && typ.Name == n {
							typeDeclaration = true
						}
						return true
					})
					if typeDeclaration {
						return true
					}
					if refs, ok := actual[n.Name]; ok {
						refs[site]++
						direct := callees[n]
						ast.Inspect(decl, func(parent ast.Node) bool {
							if sel, ok := parent.(*ast.SelectorExpr); ok && sel.Sel == n {
								direct = callees[sel]
							}
							return true
						})
						if n.Name != "contactRecordStageAuthority" && n.Name != "contactRecordReconciliationAuthority" && n.Name != "preparedContactRecordStage" && !direct {
							failures = append(failures, "K function-value escape "+site+":"+n.Name)
						}
					}
				case *ast.CompositeLit:
					if typ, ok := n.Type.(*ast.Ident); ok {
						if refs, tracked := made[typ.Name]; tracked {
							refs[site]++
						}
						if typ.Name == "recordAdmissionConstruction" {
							if name == "find_node_ready_construction.go" && owner == "buildWithOptions" {
								fields := map[string]string{}
								for _, elt := range n.Elts {
									kv, ok := elt.(*ast.KeyValueExpr)
									if !ok {
										continue
									}
									key, k := kv.Key.(*ast.Ident)
									value, v := kv.Value.(*ast.Ident)
									if k && v {
										fields[key.Name] = value.Name
									}
								}
								valid := len(n.Elts) == 0 || len(n.Elts) == 1 && reflect.DeepEqual(fields, map[string]string{"classes": "recordAdmissionChargedR"}) || len(n.Elts) == 2 && reflect.DeepEqual(fields, map[string]string{"classes": "recordAdmissionImmediateK", "kSource": "contactRecordReconciliationSource"})
								if !valid {
									failures = append(failures, "Ready closed record option changed")
								}
							}
							if owner == "newServiceWithDatagramConstructor" && len(n.Elts) != 0 {
								failures = append(failures, "ordinary constructor selected admission")
							}
							if owner == "newSessionRecordCandidateService" {
								fields := map[string]ast.Expr{}
								for _, elt := range n.Elts {
									if kv, ok := elt.(*ast.KeyValueExpr); ok {
										if key, ok := kv.Key.(*ast.Ident); ok {
											fields[key.Name] = kv.Value
										}
									}
								}
								source, ok := fields["kSource"].(*ast.Ident)
								if len(n.Elts) != 2 || len(fields) != 2 || !combinedClasses(fields["classes"]) || !ok || source.Name != "contactRecordSessionSource" {
									failures = append(failures, "session record constructor option changed")
								}
							}
							if owner == "newContactRecordReconciliationCandidateService" {
								fields := map[string]string{}
								for _, elt := range n.Elts {
									kv, ok := elt.(*ast.KeyValueExpr)
									if !ok {
										continue
									}
									key, k := kv.Key.(*ast.Ident)
									value, v := kv.Value.(*ast.Ident)
									if k && v {
										fields[key.Name] = value.Name
									}
								}
								if len(n.Elts) != 2 || !reflect.DeepEqual(fields, map[string]string{"classes": "recordAdmissionImmediateK", "kSource": "contactRecordReconciliationSource"}) {
									failures = append(failures, "reconciliation constructor option changed")
								}
							}
							if owner == "newContactRecordPushCandidateService" || owner == "newRelationRecordCandidateService" || (name == "identity_record_admission_own_dm.go" && owner == "NewInactiveOwnDMAdmissionCandidate") {
								valid := len(n.Elts) == 1
								if valid {
									kv, ok := n.Elts[0].(*ast.KeyValueExpr)
									valid = ok
									if valid {
										key, k := kv.Key.(*ast.Ident)
										value, v := kv.Value.(*ast.Ident)
										want := "recordAdmissionImmediateK"
										if owner == "newRelationRecordCandidateService" || owner == "NewInactiveOwnDMAdmissionCandidate" {
											want = "recordAdmissionChargedR"
										}
										valid = k && v && key.Name == "classes" && value.Name == want
									}
								}
								if !valid {
									failures = append(failures, "K factory constructor option changed")
								}
							}
						}
						if typ.Name == "relationRecordAdmission" {
							want := "recordAdmissionChargedR"
							if owner == "newIdentityResolverWithContactRecordAdmission" {
								want = "recordAdmissionImmediateK"
							}
							count := 0
							for _, elt := range n.Elts {
								if kv, ok := elt.(*ast.KeyValueExpr); ok {
									if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "classes" {
										if owner == "newIdentityResolverWithSessionRecordAdmission" {
											if combinedClasses(kv.Value) {
												count++
											}
										} else if value, ok := kv.Value.(*ast.Ident); ok && value.Name == want {
											count++
										}
									}
								}
							}
							if count != 1 {
								failures = append(failures, "immutable owner class literal changed "+site)
							}
							sourceCount := 0
							for _, elt := range n.Elts {
								kv, ok := elt.(*ast.KeyValueExpr)
								if !ok {
									continue
								}
								key, ok := kv.Key.(*ast.Ident)
								if !ok || key.Name != "kSource" {
									continue
								}
								value, ok := kv.Value.(*ast.Ident)
								if (owner == "newIdentityResolverWithContactRecordAdmission" && ok && value.Name == "source") || (owner == "newIdentityResolverWithSessionRecordAdmission" && ok && value.Name == "contactRecordSessionSource") {
									sourceCount++
								} else {
									failures = append(failures, "immutable K source changed "+site)
								}
							}
							if (owner == "newIdentityResolverWithContactRecordAdmission" || owner == "newIdentityResolverWithSessionRecordAdmission") && sourceCount != 1 {
								failures = append(failures, "immutable K source missing "+site)
							}

						}
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "new" && len(n.Args) == 1 {
						if typ, ok := n.Args[0].(*ast.Ident); ok && made[typ.Name] != nil {
							failures = append(failures, "alternate K allocation "+site)
						}
					}
				case *ast.TypeSpec:
					if _, structure := n.Type.(*ast.StructType); !structure {
						ast.Inspect(n.Type, func(inner ast.Node) bool {
							if typ, ok := inner.(*ast.Ident); ok && (made[typ.Name] != nil || typ.Name == "recordAdmissionClasses" || typ.Name == "contactRecordAdmissionSource") {
								failures = append(failures, "K authority alias "+site)
							}
							return true
						})
					}
				case *ast.KeyValueExpr:
					if key, ok := n.Key.(*ast.Ident); ok && key.Name == "borrowedKFiles" {
						failures = append(failures, "production borrowed backend supplied "+site)
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && (sel.Sel.Name == "classes" || sel.Sel.Name == "kFiles" || sel.Sel.Name == "borrowedKFiles" || sel.Sel.Name == "kSource") {
							failures = append(failures, "mutable class authority "+site)
						}
					}
				}
				return true
			})
		}
	}
	for key, want := range expected {
		if !reflect.DeepEqual(actual[key], want) {
			failures = append(failures, fmt.Sprintf("K reference %s got%v want%v", key, actual[key], want))
		}
	}
	for key, want := range constructors {
		if !reflect.DeepEqual(made[key], want) {
			failures = append(failures, "K alternate constructor "+key)
		}
	}
	return failures
}
func TestContactPushClosedAuthorityAndFinalLeafPlacement(t *testing.T) {
	names, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, entry := range names {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = raw
	}
	if failures := contactPushSourceErrors(sources); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, extra := range []string{
		`package node;func escape(){ _ = s.prepareContactRecordPush }`,
		`package node;var escaped = newContactRecordPushCandidateService`,
		`package node;var escaped = newRelationRecordCandidateService`,
		`package node;var escaped = newSessionRecordCandidateService`,
		`package node;var escaped = newIdentityResolverWithSessionRecordAdmission`,
		`package node;var escaped = newIdentityResolverWithRecordAdmission`,
		`package node;type Alias = contactRecordStageAuthority`,
		`package node;func escape(){ _ = &preparedContactRecordStage{} }`,
		`package node;func escape(){ a.classes = recordAdmissionImmediateK }`,
		`package node;var escaped = newContactRecordReconciliationCandidateService`,
		`package node;func escape(){ _ = p.scanReconciliation }`,
		`package node;func escape(){ _ = &contactRecordReconciliationAuthority{} }`,
		`package node;type Alias = contactRecordReconciliationAuthority`,
		`package node;func escape(){ a.kSource = contactRecordReconciliationSource }`,
	} {
		sources["escape.go"] = []byte(extra)
		if len(contactPushSourceErrors(sources)) == 0 {
			t.Fatal("K closure accepted escape", extra)
		}
		delete(sources, "escape.go")
	}
	// Exact function-level checks complement the reference inventory; they do
	// not claim arbitrary dataflow or universal wait-freedom.
	file, err := parser.ParseFile(token.NewFileSet(), "identity_pin_contact_record.go", sources["identity_pin_contact_record.go"], 0)
	if err != nil {
		t.Fatal(err)
	}
	foundBody, foundPrepare := false, false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name != "stageContactRecordLocked" && fn.Name.Name != "prepareContactRecordStageLocked" {
			continue
		}
		if fn.Name.Name == "stageContactRecordLocked" {
			foundBody = true
		} else {
			foundPrepare = true
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && fn.Name.Name == "stageContactRecordLocked" {
				switch sel.Sel.Name {
				case "now", "Restore", "Lock", "RLock", "WriteMain", "Preflight", "Marshal", "Unmarshal", "Sum256":
					t.Fatal("blocking/clock/encoding work inside final memory body", sel.Sel.Name)
				}
			}
			return true
		})
	}
	if !foundBody || !foundPrepare {
		t.Fatal("shared sealed preparation/body missing")
	}
	discovery, err := parser.ParseFile(token.NewFileSet(), "identity_discovery.go", sources["identity_discovery.go"], 0)
	if err != nil {
		t.Fatal(err)
	}
	foundPush := false
	for _, decl := range discovery.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Handle" || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		typ, ok := star.X.(*ast.Ident)
		if !ok || typ.Name != "pushIdentityHandler" {
			continue
		}
		foundPush = true
		positions := map[string][]token.Pos{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "prepareContactRecordPush", "importVerifiedIdentityRecord", "settleContactRecordPush":
						positions[sel.Sel.Name] = append(positions[sel.Sel.Name], call.Pos())
					}
				}
			}
			return true
		})
		if len(positions["prepareContactRecordPush"]) != 1 || len(positions["importVerifiedIdentityRecord"]) != 1 || len(positions["settleContactRecordPush"]) != 1 {
			t.Fatal("push capture/import/stage multiplicity changed")
		}
		if positions["prepareContactRecordPush"][0] >= positions["importVerifiedIdentityRecord"][0] || positions["importVerifiedIdentityRecord"][0] >= positions["settleContactRecordPush"][0] {
			t.Fatal("push original capture must precede the one ordinary import and optional stage")
		}
	}
	if !foundPush {
		t.Fatal("actual push handler absent")
	}

}

// Three distinct existing proof gates; this is a finite source contract, not
// generic data-flow authority. Nin remains accepted-only without FindNode/K.
func contactPushTransportConditions(raw []byte) (map[string]ast.Expr, []string) {
	file, err := parser.ParseFile(token.NewFileSet(), "session_secure.go", raw, 0)
	if err != nil {
		return nil, []string{err.Error()}
	}
	expected := map[string]string{
		"acceptSecureInbound": "s.inboundPins != nil || s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability()",
		"dialV2":              "s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability()",
		"provenTransport":     "(s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability()) && transport.proofRaw != nil",
	}
	conditions := map[string]ast.Expr{}
	var failures []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		want, tracked := expected[fn.Name.Name]
		if !tracked {
			continue
		}
		found := 0
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			statement, ok := n.(*ast.IfStmt)
			if !ok {
				return true
			}
			hasFindNode := false
			ast.Inspect(statement.Cond, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "findNode" {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "s" {
						hasFindNode = true
					}
				}
				return true
			})
			if !hasFindNode {
				return true
			}
			found++
			conditions[fn.Name.Name] = statement.Cond
			var printed bytes.Buffer
			if err := format.Node(&printed, token.NewFileSet(), statement.Cond); err != nil {
				failures = append(failures, err.Error())
				return true
			}
			if strings.Join(strings.Fields(printed.String()), " ") != want {
				failures = append(failures, "original K proof gate changed: "+fn.Name.Name)
			}
			return true
		})
		if found != 1 {
			failures = append(failures, "original proof gate multiplicity changed: "+fn.Name.Name)
		}
	}
	for name := range expected {
		if conditions[name] == nil {
			failures = append(failures, "original K proof gate absent: "+name)
		}
	}
	return conditions, failures
}

// Evaluate only the explicitly enumerated actual predicate nodes. Unsupported
// source syntax refuses the fixture; no runtime class selector is introduced.
func evaluateContactPushTransportCondition(t *testing.T, expression ast.Expr, s *Service, proofRaw bool) bool {
	t.Helper()
	switch expression := expression.(type) {
	case *ast.ParenExpr:
		return evaluateContactPushTransportCondition(t, expression.X, s, proofRaw)
	case *ast.BinaryExpr:
		if expression.Op == token.LOR {
			return evaluateContactPushTransportCondition(t, expression.X, s, proofRaw) || evaluateContactPushTransportCondition(t, expression.Y, s, proofRaw)
		}
		if expression.Op == token.LAND {
			return evaluateContactPushTransportCondition(t, expression.X, s, proofRaw) && evaluateContactPushTransportCondition(t, expression.Y, s, proofRaw)
		}
		if expression.Op == token.NEQ {
			nilID, isNil := expression.Y.(*ast.Ident)
			sel, isSelector := expression.X.(*ast.SelectorExpr)
			if isNil && nilID.Name == "nil" && isSelector {
				if base, ok := sel.X.(*ast.Ident); ok {
					switch base.Name + "." + sel.Sel.Name {
					case "s.inboundPins":
						return s.inboundPins != nil
					case "s.findNode":
						return s.findNode != nil
					case "s.degree":
						return s.degree != nil
					case "transport.proofRaw":
						return proofRaw
					}
				}
			}
		}
	case *ast.CallExpr:
		if sel, ok := expression.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "hasContactRecordPushTransportCapability" && len(expression.Args) == 0 {
			if base, ok := sel.X.(*ast.Ident); ok && base.Name == "s" {
				return s.hasContactRecordPushTransportCapability()
			}
		}
	}
	t.Fatalf("unsupported actual transport predicate %T", expression)
	return false
}

func TestContactPushThreeOriginalTransportGatesKeepKAndNinBoundaries(t *testing.T) {
	raw, err := os.ReadFile("session_secure.go")
	if err != nil {
		t.Fatal(err)
	}
	if _, failures := contactPushTransportConditions(raw); len(failures) != 0 {
		t.Fatal(failures)
	}
	cases := []struct{ name, old, next, diagnostic string }{
		{"accepted-K-missing", "if s.inboundPins != nil || s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {", "if s.inboundPins != nil || s.findNode != nil || s.degree != nil {", "acceptSecureInbound"},
		{"dial-raw-K-missing", "if s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {", "if s.findNode != nil || s.degree != nil {", "dialV2"},
		{"proven-bind-K-missing", "if (s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability()) && transport.proofRaw != nil {", "if (s.findNode != nil || s.degree != nil) && transport.proofRaw != nil {", "provenTransport"},
		{"nin-outbound-overreach", "if s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {", "if s.inboundPins != nil || s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {", "dialV2"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if strings.Count(string(raw), test.old) != 1 {
				t.Fatal("transport control premise changed", test.name)
			}
			variant := strings.Replace(string(raw), test.old, test.next, 1)
			_, failures := contactPushTransportConditions([]byte(variant))
			expected := "original K proof gate changed: " + test.diagnostic
			found := false
			for _, failure := range failures {
				if failure == expected {
					found = true
				}
			}
			if !found {
				t.Fatal("individual original proof gate control escaped", test.name, failures)
			}
		})
	}
}

// The immutable R backend method value may only capture the final real store.
func TestRecordAdmissionServiceResolverBindsAfterFinalStore(t *testing.T) {
	raw, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	late := strings.Index(src, "svc.identityResolver = newIdentityResolverWithRecordAdmission(")
	store := strings.Index(src, "svc.secureSessions = newSecureSessions(")
	record := strings.Index(src, "svc.selfRecordBody = selfRecordBody")
	if late < 0 || store < 0 || record < store || late <= record || strings.Count(src, "svc.identityResolver = newIdentityResolverWithRecordAdmission(") != 1 {
		t.Fatal("R backend bound before final store/self record or through another root")
	}
	// Exact source-placement control, separate from the actual callback-join test.
	owned := "if s.identityResolver.beginRecordAdmissionRun() {\n\t\t\ts.goRunLoop(func() { s.identityResolver.runRecordAdmission(ctx) })\n\t\t}"
	if strings.Count(src, owned) != 1 {
		t.Fatal("R startup/worker is not the exact existing Run-owned placement")
	}
}

// Finite reviewed shape: one original capture, exact ACK, one checked stage.
// This complements the closed reference multiset, not arbitrary dataflow proof.
func contactReconciliationSourceErrors(sources map[string][]byte) []string {
	raw := sources["identity_pin_contact_reconciliation.go"]
	file, err := parser.ParseFile(token.NewFileSet(), "identity_pin_contact_reconciliation.go", raw, 0)
	if err != nil {
		return []string{err.Error()}
	}
	expected := map[string]map[string]int{
		"captureReconciliation": {"cloneSignedIdentityRecord": 1, "pinOwnerObservationLocked": 1},
		"scanReconciliation":    {"reserveContactRecordReconciliation": 1, "finishContactRecordReconciliation": 1, "captureReconciliation": 1, "verifyContactMinSessionRecord": 1, "confirmLocalContacts": 1, "localContactStatus": 1, "stageContactRecordReconciliation": 1},
	}
	positions := map[string]map[string]token.Pos{}
	var failures []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		wanted, tracked := expected[fn.Name.Name]
		actual := map[string]int{}
		positions[fn.Name.Name] = map[string]token.Pos{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.GoStmt); ok {
				failures = append(failures, "reconciliation introduced independent worker")
			}
			if call, ok := n.(*ast.CallExpr); ok {
				name := ""
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					name = callee.Name
				case *ast.SelectorExpr:
					name = callee.Sel.Name
				}
				if name == "observeContactRecord" || name == "stageContactRecord" || name == "prepareContactRecordPush" || name == "importVerifiedIdentityRecord" {
					failures = append(failures, "reconciliation borrowed another authority")
				}
				if _, ok := wanted[name]; tracked && ok {
					actual[name]++
					positions[fn.Name.Name][name] = call.Pos()
				}
			}
			return true
		})
		if tracked && !reflect.DeepEqual(actual, wanted) {
			failures = append(failures, "reconciliation finite flow changed "+fn.Name.Name)
		}
	}
	order := []string{"reserveContactRecordReconciliation", "captureReconciliation", "verifyContactMinSessionRecord", "confirmLocalContacts", "localContactStatus", "stageContactRecordReconciliation"}
	for i := 1; i < len(order); i++ {
		if positions["scanReconciliation"][order[i-1]] >= positions["scanReconciliation"][order[i]] {
			failures = append(failures, "reconciliation original capture/ACK/stage order changed")
		}
	}
	if !bytes.Contains(raw, []byte("len(current.record.Body) > domain.MaxIdentityRecordBodyBytes")) || !bytes.Contains(raw, []byte("len(current.record.Sig) != 64")) {
		failures = append(failures, "reconciliation raw bound before clone missing")
	}
	common := sources["identity_pin_contact_record.go"]
	if !bytes.Contains(common, []byte("bytes.Equal(current.record.Sig, candidate.record.Sig)")) {
		failures = append(failures, "exact current signature gate missing")
	}
	return failures
}
func TestContactReconciliationClosedSourceOrderAndCausalShapes(t *testing.T) {
	sources := map[string][]byte{}
	for _, name := range []string{"identity_pin_contact_reconciliation.go", "identity_pin_contact_record.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = raw
	}
	if failures := contactReconciliationSourceErrors(sources); len(failures) != 0 {
		t.Fatal(failures)
	}
	cases := []struct{ name, path, old, next string }{
		{"missing-ACK", "identity_pin_contact_reconciliation.go", "s.trust.confirmLocalContacts(g.ctx, candidate.fact)", "s.trust.preflightLocalContact(g.ctx)"},
		{"borrow-push", "identity_pin_contact_reconciliation.go", "p.captureReconciliation(g.ctx)", "s.observeContactRecord(g.ctx, id, record)"},
		{"drop-sig", "identity_pin_contact_record.go", "bytes.Equal(current.record.Sig, candidate.record.Sig)", "true"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			before := sources[test.path]
			if bytes.Count(before, []byte(test.old)) != 1 {
				t.Fatal("causal shape premise changed")
			}
			sources[test.path] = bytes.Replace(before, []byte(test.old), []byte(test.next), 1)
			defer func() { sources[test.path] = before }()
			if len(contactReconciliationSourceErrors(sources)) == 0 {
				t.Fatal("reconciliation causal shape escaped", test.name)
			}
		})
	}
}

// The finite assignment/constructor inventory is a syntactic Source boundary,
// not a universal pointer-alias/dataflow proof. It covers actual protected
// field writes, including the existing two diagnostic store-view assignments.
func sessionRecordSourceErrors(sources map[string][]byte) []string {
	failures := contactPushSourceErrors(sources)
	writes := map[string]int{}
	constructors := map[string]int{}
	var actualBackend string
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			return append(failures, err.Error())
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			site := name + ":" + fn.Name.Name
			if name == "identity_record_admission.go" && fn.Name.Name == "newIdentityResolverWithSessionRecordAdmission" {
				var printed bytes.Buffer
				if err := format.Node(&printed, token.NewFileSet(), fn); err != nil {
					return append(failures, err.Error())
				}
				actualBackend = printed.String()
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if assignment, ok := n.(*ast.AssignStmt); ok {
					for i, lhs := range assignment.Lhs {
						selector, ok := lhs.(*ast.SelectorExpr)
						if !ok || (selector.Sel.Name != "identityResolver" && selector.Sel.Name != "secureSessions" && selector.Sel.Name != "store") {
							continue
						}
						key := site + ":" + selector.Sel.Name
						if selector.Sel.Name != "store" {
							callee := "<non-call>"
							if len(assignment.Lhs) == len(assignment.Rhs) {
								if call, ok := assignment.Rhs[i].(*ast.CallExpr); ok {
									if id, ok := call.Fun.(*ast.Ident); ok {
										callee = id.Name
									}
								}
							}
							key += ":" + callee
						}
						writes[key]++
					}
				}
				if literal, ok := n.(*ast.CompositeLit); ok {
					if typ, ok := literal.Type.(*ast.Ident); ok && (typ.Name == "secureSessions" || typ.Name == "identityResolver") {
						constructors[site+":"+typ.Name]++
					}
				}
				if field, ok := n.(*ast.KeyValueExpr); ok {
					if key, ok := field.Key.(*ast.Ident); ok && (key.Name == "identityResolver" || key.Name == "secureSessions") {
						failures = append(failures, "alternate Service receiver binding "+site)
					}
				}
				return true
			})
		}
	}
	expectedWrites := map[string]int{
		"service.go:newServiceWithConstruction:identityResolver:newIdentityResolver":                           1,
		"service.go:newServiceWithConstruction:identityResolver:newIdentityResolverWithRecordAdmission":        1,
		"service.go:newServiceWithConstruction:identityResolver:newIdentityResolverWithContactRecordAdmission": 1,
		"service.go:newServiceWithConstruction:identityResolver:newIdentityResolverWithSessionRecordAdmission": 1,
		"service.go:newServiceWithConstruction:secureSessions:newSecureSessions":                               1,
		"service.go:newServiceWithConstruction:store":                                                          1,
		"s6_terminal_construction.go:newServiceWithObservations:store":                                         1,
	}
	expectedConstructors := map[string]int{
		"session_secure.go:buildSecureSessions:secureSessions":      1,
		"session_secure.go:legacyOnlySessions:secureSessions":       1,
		"identity_resolver.go:newIdentityResolver:identityResolver": 1,
	}
	if !reflect.DeepEqual(writes, expectedWrites) {
		failures = append(failures, "original receiver write frontier changed")
	}
	if !reflect.DeepEqual(constructors, expectedConstructors) {
		failures = append(failures, "original receiver construction frontier changed")
	}
	// Exact closed delayed backend: sameService, nil refusal, no setter,
	// replacement store, arbitrary dependency, alternate resolver or I/O at New.
	const backend = `func newIdentityResolverWithSessionRecordAdmission(svc *Service, intents *identityIntentStore, network domain.NetworkID) *identityResolver {
	r := newIdentityResolver(svc, intents, network)
	epoch := time.Now()
	r.recordAdmission = &relationRecordAdmission{classes: recordAdmissionChargedR | recordAdmissionImmediateK, kSource: contactRecordSessionSource, service: svc, resolver: r, network: network, clock: func() relationObservedTime {
		now := time.Now()
		return relationObservedTime{wall: now, elapsed: now.Sub(epoch)}
	}, wake: make(chan struct{}, 1), openFiles: func() (identityPinDurableFiles, error) {
		store := svc.localContactPinStore()
		if store == nil {
			return nil, domain.ErrIdentityPinStoreUnavailable
		}
		return store.openIdentityPinFiles()
	}}
	return r
}`
	parsed, err := parser.ParseFile(token.NewFileSet(), "contract.go", "package node\n"+backend, 0)
	if err != nil {
		return append(failures, err.Error())
	}
	var expected bytes.Buffer
	if err := format.Node(&expected, token.NewFileSet(), parsed.Decls[0]); err != nil {
		return append(failures, err.Error())
	}
	if actualBackend != expected.String() {
		failures = append(failures, "closed combined backend changed")
	}
	return failures
}

func TestSessionRecordSourceClosedFactoryAndOriginalReceivers(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources[name], err = os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
	}
	if failures := sessionRecordSourceErrors(sources); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, mutation := range []struct{ name, file, old, new string }{
		{"factory-class", "service.go", "classes: recordAdmissionChargedR | recordAdmissionImmediateK, kSource: contactRecordSessionSource", "classes: recordAdmissionImmediateK, kSource: contactRecordSessionSource"},
		{"ordinary-class", "service.go", "recordAdmissionConstruction{}", "recordAdmissionConstruction{classes: recordAdmissionChargedR | recordAdmissionImmediateK, kSource: contactRecordSessionSource}"},
		{"backend", "identity_record_admission.go", "return store.openIdentityPinFiles()", "return replacement()"},
		{"backend-nil", "identity_record_admission.go", "if store == nil {", "if false {"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			original := sources[mutation.file]
			if !bytes.Contains(original, []byte(mutation.old)) {
				t.Fatal("mutation preimage missing")
			}
			sources[mutation.file] = bytes.Replace(original, []byte(mutation.old), []byte(mutation.new), 1)
			defer func() { sources[mutation.file] = original }()
			if len(sessionRecordSourceErrors(sources)) == 0 {
				t.Fatal("closed source accepted mutation")
			}
		})
	}
	for _, extra := range []string{
		`package node;func escape(){s.identityResolver=replacement()}`,
		`package node;func escape(){s.secureSessions=replacement()}`,
		`package node;func escape(){sessions.store=replacement}`,
		`package node;func escape(){_=newSessionRecordCandidateService}`,
	} {
		sources["escape.go"] = []byte(extra)
		if len(sessionRecordSourceErrors(sources)) == 0 {
			t.Fatal("receiver/caller frontier escaped")
		}
		delete(sources, "escape.go")
	}
}
