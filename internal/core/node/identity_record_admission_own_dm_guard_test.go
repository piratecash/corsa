package node

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// Finite source boundaries supplement actual Run/CM/socket/backend tests.
// No function value, renamed same-count caller or late constructor is allowed.
var ownDMAdmissionClosedReferences = map[string]map[r2aReferenceSite]int{
	"r2Claim":         {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_pin_outgoing_provenance.go", "settleR2aOwnHop"}: 1, {"node/identity_record_admission.go", "expireRelationSlotsLocked"}: 2, {"node/identity_record_admission.go", "selectRelationSlotLocked"}: 1, {"node/identity_record_admission.go", "processRelationSlot"}: 1, {"node/identity_record_admission.go", "retainStoppedRelationSlotLocked"}: 1, {"node/identity_record_admission_relation.go", "stageRelationAdmission"}: 1, {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 3, {"node/identity_record_admission_own_dm.go", "ownDMClaimCurrentLocked"}: 2, {"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 4},
	"r2Role":          {{"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 1, {"node/identity_record_admission_own_dm.go", "ownDMClaimCurrentLocked"}: 2, {"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 3},
	"r2Verified":      {{"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_record_admission.go", "retainStoppedRelationSlotLocked"}: 1, {"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 2},
	"r2PendingCursor": {{"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_record_admission.go", "selectRelationSlotLocked"}: 2},

	"NewInactiveOwnDMAdmissionCandidate":  {},
	"captureOwnDMResidence":               {{"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1},
	"bindOwnDMResidence":                  {{"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1},
	"registerOwnDMRecordAdmission":        {{"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1},
	"prepareOwnDMRelation":                {{"node/identity_record_admission.go", "processRelationSlot"}: 1},
	"stageOwnDMRelationAdmission":         {{"node/identity_record_admission_relation.go", "stageRelationAdmission"}: 1},
	"stageOwnDMRelationRecordLocked":      {{"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 1},
	"verifyOwnDMRelationRecord":           {{"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1},
	"stageClassifiedRelationRecordLocked": {{"node/identity_pin_relation.go", "stageRelationRecordLocked"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationRecordLocked"}: 1},
	"lockOwnDMRecord":                     {{"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 2, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 1},
	"ownDMClaimCurrentLocked":             {{"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 2, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 1},
	"ownersMatchLocked":                   {{"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 1, {"node/identity_record_admission_own_dm.go", "ownDMClaimCurrentLocked"}: 1},
	"ownDMObservation":                    {{"node/identity_record_admission_own_dm.go", "ownersMatchLocked"}: 1, {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 2, {"node/identity_record_admission_own_dm.go", "verifyOwnDMRelationRecord"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationRecordLocked"}: 1},
	"r2Admission":                         {{"node/identity_pin_own_dm_runtime.go", "install"}: 1, {"node/identity_pin_own_dm_runtime.go", "bindings"}: 1, {"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1, {"node/identity_record_admission_own_dm.go", "captureOwnDMResidence"}: 2},
	"r2Residence":                         {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "bindOwnDMResidence"}: 1, {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 2, {"node/identity_record_admission_own_dm.go", "ownDMClaimCurrentLocked"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationRecordLocked"}: 1, {"node/identity_record_admission.go", "expireRelationSlotsLocked"}: 1},
}

func ownDMAdmissionSourceErrors(sources map[string][]byte) []string {
	failures := ownDMGuardErrors(sources, ownDMAdmissionClosedReferences)
	constructors := map[string]r2aReferenceSite{
		"ownDMRecordAdmission":        {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"},
		"ownDMResidenceOrigin":        {"node/identity_record_admission_own_dm.go", "captureOwnDMResidence"},
		"ownDMRelationClaim":          {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"},
		"ownDMEarnedRole":             {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"},
		"verifiedOwnDMRelationRecord": {"node/identity_record_admission_own_dm.go", "verifyOwnDMRelationRecord"},
	}
	writes := map[r2aReferenceSite]map[string]int{}
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		for _, decl := range file.Decls {
			site := r2aReferenceSite{name, "<package>"}
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site.function = fn.Name.Name
				body = fn.Body
			}
			if body == nil {
				continue
			}
			ast.Inspect(body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.CompositeLit); ok && len(lit.Elts) > 0 {
					if typ, ok := lit.Type.(*ast.Ident); ok {
						if want, tracked := constructors[typ.Name]; tracked && site != want {
							failures = append(failures, fmt.Sprintf("own-DM admission mint %s at %v", typ.Name, site))
						}
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "new" && len(call.Args) == 1 {
						if typ, ok := call.Args[0].(*ast.Ident); ok {
							if _, tracked := constructors[typ.Name]; tracked {
								failures = append(failures, "alternate own-DM admission allocation")
							}
						}
					}
				}
				if typ, ok := n.(*ast.TypeSpec); ok {
					if _, structure := typ.Type.(*ast.StructType); !structure {
						ast.Inspect(typ.Type, func(inner ast.Node) bool {
							if id, ok := inner.(*ast.Ident); ok {
								if _, tracked := constructors[id.Name]; tracked {
									failures = append(failures, "own-DM admission alias")
								}
							}
							return true
						})
					}
				}
				if assignment, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assignment.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok {
							switch sel.Sel.Name {
							case "r2Admission", "r2Residence", "r2Claim", "r2Role", "r2Verified":
								if writes[site] == nil {
									writes[site] = map[string]int{}
								}
								writes[site][sel.Sel.Name]++
							}
						}
					}
				}
				return true
			})
		}
	}
	want := map[r2aReferenceSite]map[string]int{
		{"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}:        {"r2Admission": 1},
		{"node/identity_record_admission_own_dm.go", "bindOwnDMResidence"}:           {"r2Residence": 1},
		{"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: {"r2Claim": 1},
		{"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}:         {"r2Verified": 1},
		{"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}:  {"r2Verified": 1},
		{"node/identity_record_admission.go", "retainStoppedRelationSlotLocked"}:     {"r2Claim": 1, "r2Verified": 1},
	}
	if !reflect.DeepEqual(writes, want) {
		failures = append(failures, fmt.Sprintf("own-DM admission private writes %v want %v", writes, want))
	}
	return failures
}
func TestOwnDMAdmissionClosedConstructionAndAuthority(t *testing.T) {
	if failures := ownDMAdmissionSourceErrors(ownDMProductionSources(t)); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}
func TestOwnDMAdmissionSourceGuardRejectsEscapeAndLateMint(t *testing.T) {
	baseline := ownDMProductionSources(t)
	if failures := ownDMAdmissionSourceErrors(baseline); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, source := range []string{
		"package node;var escaped=NewInactiveOwnDMAdmissionCandidate",
		"package node;func ordinary(){_ = s.registerOwnDMRecordAdmission}",
		"package node;func ordinary(){_ = s.stageClassifiedRelationRecordLocked}",
		"package node;func ordinary(){o.r2Admission=other}",
		"package node;func ordinary(){_ = ownDMResidenceOrigin{owner:o}}",
		"package node;type leaked=ownDMRelationClaim",
	} {
		sources := map[string][]byte{}
		for name, raw := range baseline {
			sources[name] = raw
		}
		sources["node/escape.go"] = []byte(source)
		if len(ownDMAdmissionSourceErrors(sources)) == 0 {
			t.Fatal("own-DM admission source escape accepted")
		}
	}
}

func TestOwnDMAdmissionClosedTypesAndFixedState(t *testing.T) {
	types := map[string]map[r2aReferenceSite]int{
		"OwnDMRuntimeProfile":         {{"node/identity_pin_own_dm_runtime.go", "NewInactiveOwnDMRuntimeCandidate"}: 1, {"sdk/runtime.go", "newWithOwnDMRuntime"}: 1, {"sdk/runtime.go", "newWithContext"}: 1},
		"ownDMRuntimeConstruction":    {{"node/identity_pin_own_dm_runtime.go", "NewInactiveOwnDMRuntimeCandidate"}: 1, {"node/identity_pin_own_dm_runtime.go", "install"}: 1, {"node/identity_pin_own_dm_runtime.go", "bindings"}: 1, {"node/find_node_ready_construction.go", "<package>"}: 2, {"node/find_node_ready_construction.go", "buildWithOwnDM"}: 1},
		"OwnDMResidenceProfile":       {{"node/identity_record_admission_own_dm.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "valid"}: 1, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1, {"node/identity_record_admission_own_dm.go", "NewInactiveOwnDMAdmissionCandidate"}: 1, {"node/identity_pin_own_dm_runtime.go", "<package>"}: 2},
		"ownDMRecordAdmission":        {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1, {"node/identity_pin_own_dm_runtime.go", "<package>"}: 1},
		"ownDMResidenceOrigin":        {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "captureOwnDMResidence"}: 2, {"node/identity_record_admission_own_dm.go", "bindOwnDMResidence"}: 1, {"node/identity_record_admission_own_dm.go", "liveLocked"}: 1},
		"ownDMRelationClaim":          {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 1, {"node/identity_record_admission_own_dm.go", "ownDMObservation"}: 1, {"node/identity_record_admission_own_dm.go", "ownersMatchLocked"}: 1, {"node/identity_record_admission_own_dm.go", "verifyOwnDMRelationRecord"}: 1},
		"ownDMEarnedRole":             {{"node/identity_record_admission_own_dm.go", "<package>"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 1},
		"verifiedOwnDMRelationRecord": {{"node/identity_record_admission.go", "<package>"}: 1, {"node/identity_record_admission.go", "retainStoppedRelationSlotLocked"}: 1, {"node/identity_record_admission_own_dm.go", "verifyOwnDMRelationRecord"}: 4, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationRecordLocked"}: 1, {"node/identity_record_admission_own_dm.go", "stageOwnDMRelationAdmission"}: 1},
	}
	inventory := map[string][]string{
		"OwnDMRuntimeProfile":         {"Residence", "MaxActive", "MaxMembers", "MaxRegistryVisits", "Intake", "Signing"},
		"ownDMRuntimeConstruction":    {"profile", "owner", "resolver", "admission", "port"},
		"OwnDMResidenceProfile":       {"MaxPrestage"},
		"ownDMRecordAdmission":        {"service", "resolver", "admission", "profile"},
		"ownDMResidenceOrigin":        {"owner", "started", "at", "deadline", "done", "ttlExpiry"},
		"ownDMRelationClaim":          {"stamp", "refs", "earned", "delegated"},
		"ownDMEarnedRole":             {"ref", "binding"},
		"verifiedOwnDMRelationRecord": {"claim", "role", "target", "network", "sequence"},
	}
	actual := map[string]map[r2aReferenceSite]int{}
	for name := range types {
		actual[name] = map[r2aReferenceSite]int{}
	}
	seen := map[string]bool{}
	for name, raw := range ownDMProductionSources(t) {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			site := r2aReferenceSite{name, "<package>"}
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site.function = fn.Name.Name
				declaration = fn.Name
			}
			definitions := map[*ast.Ident]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if typ, ok := n.(*ast.TypeSpec); ok {
					definitions[typ.Name] = true
					if want, tracked := inventory[typ.Name.Name]; tracked {
						seen[typ.Name.Name] = true
						structure, ok := typ.Type.(*ast.StructType)
						if !ok {
							t.Fatal("admission type alias", typ.Name.Name)
						}
						names := []string{}
						for _, field := range structure.Fields.List {
							if len(field.Names) == 0 {
								t.Fatal("hidden embedded admission state")
							}
							for _, id := range field.Names {
								names = append(names, id.Name)
							}
						}
						if !reflect.DeepEqual(names, want) {
							t.Fatal("unreviewed admission state", typ.Name.Name, names)
						}
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != declaration && !definitions[id] {
					if refs, tracked := actual[id.Name]; tracked {
						refs[site]++
					}
				}
				return true
			})
		}
	}
	for name, want := range types {
		if !reflect.DeepEqual(actual[name], want) {
			t.Fatalf("own-DM type references %s %v want %v", name, actual[name], want)
		}
	}
	if len(seen) != len(inventory) {
		t.Fatal("admission state inventory incomplete")
	}
}
