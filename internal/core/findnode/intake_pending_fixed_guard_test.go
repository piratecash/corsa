package findnode

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Exact raw production closure catches field/call/declaration/method-value and
// package GenDecl changes, including new files. It does not infer dataflow,
// compiler allocations or actual final-lease provenance from this syntax.
func pendingFixedSourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{"owned_bridge_local_target_timing.go": "69356ebd3196f7b042d465cb72eb880ded354cc49ed2e334c3aebb3f179e0a1d", "owned_bridge_bootstrap.go": "2ee7f6b13d859326bbd2fb00e487c9530737a2f722359d762202710d1a1cd827", "owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "owned_bridge_forward_answer.go": "189a2b6a66e0b582b303f8adb36e24a826125583a4d1b4a0f21782b0870e4f2f", "owned_bridge_local_target.go": "dd2e3eea3ba16ffd60e057afb4c61a46d98a74cd872776e76882a6b0b4835e2d", "owned_bridge_local_target_stage.go": "39fde17cdadd3dfa02bc97ef40f9d21d37f18ca865abde2fed2005e680baa0c3", "owned_bridge_answer.go": "6f0a1c3c7129cfd838d2cd4599ee25e7c2e9eee451e28b1addf970cb5509952e",
		"owned_bridge_dispatch.go":       "3a6495784029c780e6cca5fa3d439a7d99fd34ddbc54fce5d97a75242d608732",
		"owned_bridge_round_dispatch.go": "b9128ec6284560aea84441eab2b8619ba08297b66133139af4030a39d4478fcf",
		"owned_bridge_receive.go":        "5de62b53e6ae476daf7563e4cede1e95ed9e1e7f30fbc4de1e06947bc4144671",
		"owned_bridge_round_execute.go":  "dedd956801a9a14580f4e22634d3a5eb579ebe22520eb45eedce564209d7eec4",
		"owned_bridge_round_plan.go":     "8948ec210326d25339c8475b8d7a2889c01ab15dbf3b320d9be4ab65c403cb51",
		"owned_bridge_commit.go":         "61dfd28229d06a03012b4a0276e1bf3961e7aa05a6aca80154ae672bd3be5255",
		"budget.go":                      "9a9b95192bd6928694d23cac19550dbb3a29ef9113151130d0d545f19d7fe4fa",
		"intake_capacity_epoch.go":       "b3b1453ed9a017bbaf1a528e18c55d93bc9c0ac4f1a78428a6b287424ebfb302",
		"intake_groups.go":               "40311189acc0513c1d1486e445629bc312f5f419dc032cd50358c2f408223734",
		"intake_membership.go":           "638df96cd6913445c472142abf8635ea21f141d38efd3cd52f3f1b740c396a6f",
		"intake_owner.go":                "464c803f0c96ef419752c9965b217ba0d0b74c9ff0469c6b3e08efb2df60fe18",
		"intake_pending_fixed.go":        "05d24971ea6863543099cb5dbe57e11eb5c0bbd1381012355755590e6aad4ffb",
		"intake_source_limits.go":        "592bedf2ab49cb7e60622a2d11f3ebd119a1c4f4d5c87cfe128922de83872004",
		"owned_bridge.go":                "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57",
		"owned_bridge_cut.go":            "ec58d8addd9e5f7509a9da3c827fbf0d7b5ffc15ab2d37512d1f2278795c97de",
		"owned_bridge_scratch.go":        "4f2a750803e6118b187eae648f22825452a3dedab9de1da1d9e5ee77d25876a6",
		"owned_originals.go":             "24fd02310c47a8346113a3c21c4aace02d3c8b646d2e3cdc91edf1d3d61a53b4",
		"pending.go":                     "ecd63fca64c9d921e35bf4e990bdc9934918787b902ce298244884540500c9e0",
		"session.go":                     "8968f6d526239bc11510c4f091539dda84bed91c345d74a4606b397ddd525dc1",
		"signing.go":                     "c0389bb2c6e127ff266860eaf0eb24a429c7c1cee9081fad0844483a7938b40b",
		"target.go":                      "15c107401df68ec9527f6ce46e4d6311c6cbd79ffea7ac42b3d3bbbe2fbe6d81",
		"target_owner.go":                "f6f6f51e69ace1888584e95a5d09f08cf13e8b6936cb68e6d022549d726b5511",
	}
	var failures []string
	if len(sources) != len(pins) {
		failures = append(failures, "pending closed production pathset changed")
	}
	for name, want := range pins {
		raw, ok := sources[name]
		if !ok || fmt.Sprintf("%x", sha256.Sum256(raw)) != want {
			failures = append(failures, "pending exact production SOURCE changed: "+name)
		}
	}
	for name := range sources {
		if _, ok := pins[name]; !ok {
			failures = append(failures, "pending foreign production SOURCE: "+name)
		}
	}
	// Keep independently checked structural facts as well as raw closure pins.
	book, err := parser.ParseFile(token.NewFileSet(), "intake_pending_fixed.go", sources["intake_pending_fixed.go"], 0)
	if err != nil {
		return append(failures, "pending SOURCE syntax: "+err.Error())
	}
	declared := 0
	for _, declaration := range book.Decls {
		ast.Inspect(declaration, func(node ast.Node) bool {
			typ, ok := node.(*ast.TypeSpec)
			if !ok || typ.Name.Name != "candidatePendingBook" {
				return true
			}
			declared++
			if typ.Assign.IsValid() {
				failures = append(failures, "pending book type alias")
			}
			structure, ok := typ.Type.(*ast.StructType)
			if !ok {
				failures = append(failures, "pending book not struct")
				return false
			}
			var fields []string
			entriesShape := false
			for _, field := range structure.Fields.List {
				if len(field.Names) == 0 {
					failures = append(failures, "pending book embedded authority")
				}
				for _, id := range field.Names {
					fields = append(fields, id.Name)
					if id.Name == "entries" {
						array, ok := field.Type.(*ast.ArrayType)
						if ok {
							bound, bok := array.Len.(*ast.Ident)
							pointer, pok := array.Elt.(*ast.StarExpr)
							if bok && pok {
								element, eok := pointer.X.(*ast.Ident)
								entriesShape = eok && bound.Name == "MaxPendingTotal" && element.Name == "pendingEntry"
							}
						}
					}
				}
			}
			if !entriesShape || !reflect.DeepEqual(fields, []string{"mu", "entries", "last", "counters", "published"}) {
				failures = append(failures, "pending fixed512 pointer/field shape changed")
			}
			return false
		})
	}
	if declared != 1 {
		failures = append(failures, "pending book declaration count changed")
	}
	owner := string(sources["intake_owner.go"])
	claim := strings.Index(owner, "entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows)")
	check := strings.Index(owner, "sourceIndex := o.pending.sourceIndexLocked(pendingKey{entry.source, entry.request.ID})")
	collision := strings.Index(owner, "if o.pending.targetEntryLocked(pendingKey{entry.target, id}) != nil")
	final := strings.Index(owner, "currentCapacity, capacityErr := o.frozenCapacityLocked(now)")
	if check < 0 || collision <= check || final <= collision || claim <= final {
		failures = append(failures, "pending final joint preclaim order changed")
	}
	if strings.Contains(owner, "o.pending.byTarget") || strings.Contains(owner, "o.pending.bySource") || strings.Contains(owner, "o.pending.sources") || strings.Contains(owner, "o.pending.targets") {
		failures = append(failures, "pending old owned map authority survived")
	}
	return failures
}
func pendingFixedSources(t *testing.T) map[string][]byte {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, name := range paths {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		out[name] = raw
	}
	return out
}
func TestCandidatePendingBookClosedSourceInventory(t *testing.T) {
	if failures := pendingFixedSourceErrors(pendingFixedSources(t)); len(failures) != 0 {
		t.Fatal("private pending SOURCE closure", failures)
	}
}
func TestCandidatePendingBookGuardCausalSelfControls(t *testing.T) {
	original := pendingFixedSources(t)
	if failures := pendingFixedSourceErrors(original); len(failures) != 0 {
		t.Fatal("pending SOURCE self-control baseline", failures)
	}
	for _, change := range []struct{ name, path, before, after string }{
		{"map-storage", "intake_pending_fixed.go", "entries   [MaxPendingTotal]*pendingEntry", "entries   map[int]*pendingEntry"},
		{"slice-storage", "intake_pending_fixed.go", "entries   [MaxPendingTotal]*pendingEntry", "entries   []*pendingEntry"},
		{"wrong-bound", "intake_pending_fixed.go", "entries   [MaxPendingTotal]*pendingEntry", "entries   [MaxPendingTotal+1]*pendingEntry"},
		{"embedded-reuse", "intake_pending_fixed.go", "entries   [MaxPendingTotal]*pendingEntry", "entries   [MaxPendingTotal]pendingEntry"},
		{"package-constructor-alias", "pending_escape.go", "", "package findnode\nvar pendingConstructorEscape = newCandidatePendingBook\n"},
		{"method-value", "pending_escape.go", "", "package findnode\nfunc pendingMethodEscape(p *candidatePendingBook) any { return p.sourceIndexLocked }\n"},
		{"foreign-write", "pending_escape.go", "", "package findnode\nfunc pendingForeignWrite(p *candidatePendingBook) { p.entries[0]=nil }\n"},
		{"public-mode", "pending.go", "type PendingTable struct {", "type PendingTable struct {\n owned *candidatePendingBook"},
		{"after-CAS-growth", "intake_owner.go", "source.windows.rows[source.index] = source.row", "_ = make(map[int]int)\n source.windows.rows[source.index] = source.row"},
		{"owner-literal", "intake_owner.go", "candidate dispatch assumption violated", "candidate  dispatch assumption violated"},
		{"book-reset", "intake_owner.go", "o.pending.entries[index] = entry", "o.pending = newCandidatePendingBook()\n o.pending.entries[index] = entry"},
		{"new-callback", "intake_pending_fixed.go", "n := 0", "n := 0; pendingCallback := func() {}; pendingCallback()"},
	} {
		t.Run(change.name, func(t *testing.T) {
			mutated := map[string][]byte{}
			for name, raw := range original {
				mutated[name] = bytes.Clone(raw)
			}
			if change.before == "" {
				if _, ok := mutated[change.path]; ok {
					t.Fatal("pending selfcase foreign path not absent")
				}
				mutated[change.path] = []byte(change.after)
			} else {
				raw := string(mutated[change.path])
				if change.name == "new-callback" {
					if strings.Count(raw, change.before) != 3 {
						t.Fatal("pending callback test anchor changed")
					}
				} else if strings.Count(raw, change.before) != 1 {
					t.Fatal("pending SOURCE self-control anchor changed", change.name)
				}
				mutated[change.path] = []byte(strings.Replace(raw, change.before, change.after, 1))
			}
			failures := pendingFixedSourceErrors(mutated)
			found := false
			for _, failure := range failures {
				if strings.HasPrefix(failure, "pending exact production SOURCE changed:") || strings.HasPrefix(failure, "pending foreign production SOURCE:") {
					found = true
				}
			}
			sort.Strings(failures)
			if !found {
				t.Fatal("pending SOURCE self-control lacked exact placement refusal", change.name, failures)
			}
		})
	}
}
