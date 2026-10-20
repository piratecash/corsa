package node

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/netip"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func loadStandAddressFixture(t *testing.T, fulls, edges loadStandNodeCount) (loadStandPlan, []loadStandPlannedAddress) {
	t.Helper()
	plan := mustBuildLoadStandPlan(t, loadStandPlanOpts{
		Seed: 1, FullNodes: fulls, Hubs: 2, EdgeNodes: edges, EdgeUplinks: 1,
		Duration: time.Second, SampleInterval: time.Second,
		Churn: loadStandChurnOpts{Profile: loadStandChurnQuiet},
	})
	return plan, loadStandAddressFixtureRows(plan)
}

func loadStandAddressFixtureRows(plan loadStandPlan) []loadStandPlannedAddress {
	nodes := plan.Nodes()
	rows := make([]loadStandPlannedAddress, len(nodes))
	byID := make(map[loadStandNodeID]domain.PeerAddress, len(nodes))
	for i, node := range nodes {
		// One literal in each /16 of 240/4, including the boundary groups.
		rows[i] = loadStandPlannedAddress{
			Node: node.ID(), Address: domain.PeerAddress(fmt.Sprintf("%d.%d.0.1:64646", 240+i/256, i%256)),
		}
		byID[node.ID()] = rows[i].Address
	}
	for i, node := range nodes {
		for _, target := range node.DialTargets() {
			rows[i].Bootstrap = append(rows[i].Bootstrap, byID[target])
		}
	}
	return rows
}

func TestLoadStandAddressPrimaryUsesActualTuples(t *testing.T) {
	plan, rows := loadStandAddressFixture(t, 2, 2)
	// Input order does not set the result order or the node's role.
	slices.Reverse(rows)
	check, err := validateLoadStandPrimaryIPv4(plan, rows)
	if err != nil {
		t.Fatalf("valid primary addresses refused: %v", err)
	}
	if check.Digest() != plan.Digest() {
		t.Fatal("result lost its exact plan digest")
	}
	got := check.Rows()
	if len(got) != 4 {
		t.Fatalf("rows = %d, want 4", len(got))
	}
	for i, row := range got {
		wantIP := netip.MustParseAddr(fmt.Sprintf("240.%d.0.1", i))
		if row.Node != plan.Nodes()[i].ID() || row.Role != plan.Nodes()[i].Role() || row.Hub != plan.Nodes()[i].IsHub() {
			t.Fatalf("row %d lost plan identity/role/hub", i)
		}
		if row.CanonicalIP != domain.PeerIP(wantIP) || row.NetGroup != domain.NetGroupIPv4 || row.NonRoutable {
			t.Fatalf("row %d has wrong classification: %+v", i, row)
		}
		if row.Subnet != netip.MustParsePrefix(fmt.Sprintf("240.%d.0.0/24", i)) || row.SourceGroup != domain.SourceGroup(fmt.Sprintf("240.%d.0.0/16", i)) {
			t.Fatalf("row %d has wrong grouping: %+v", i, row)
		}
		if row.Address != domain.PeerAddress(fmt.Sprintf("240.%d.0.1:64646", i)) || len(row.Bootstrap) == 0 {
			t.Fatalf("row %d lost its explicit address/bootstrap", i)
		}
	}
	if got[2].Role != loadStandRoleEdge {
		t.Fatal("SDK planned source address was treated as a listening FULL role")
	}
}

func TestLoadStandAddressRejectsIdentityAndPlanChanges(t *testing.T) {
	tests := map[string]func(*loadStandPlan, *[]loadStandPlannedAddress){
		"missing row": func(_ *loadStandPlan, rows *[]loadStandPlannedAddress) { *rows = (*rows)[:3] },
		"extra row": func(_ *loadStandPlan, rows *[]loadStandPlannedAddress) {
			*rows = append(*rows, loadStandPlannedAddress{Node: "unknown", Address: "240.8.0.1:64646"})
		},
		"duplicate row ID": func(_ *loadStandPlan, rows *[]loadStandPlannedAddress) { (*rows)[3].Node = (*rows)[2].Node },
		"unknown row ID":   func(_ *loadStandPlan, rows *[]loadStandPlannedAddress) { (*rows)[3].Node = "unknown" },
		"stale digest":     func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) { plan.nodes[0].hub = false },
		"empty digest":     func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) { plan.digest = "" },
		"duplicate plan ID": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			plan.nodes[1].id = plan.nodes[0].id
			plan.digest = plan.computeDigest()
		},
		"noncanonical plan ID": func(plan *loadStandPlan, rows *[]loadStandPlannedAddress) {
			plan.nodes[2].id = " edge-000"
			(*rows)[2].Node = plan.nodes[2].id
			plan.digest = plan.computeDigest()
		},
		"unknown role": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			plan.nodes[2].role = "other"
			plan.digest = plan.computeDigest()
		},
		"edge hub": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			plan.nodes[2].hub = true
			plan.digest = plan.computeDigest()
		},
		"old version": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			for i := range plan.params {
				if plan.params[i].Name == "plan_version" {
					plan.params[i].Value = "2"
				}
			}
			plan.digest = plan.computeDigest()
		},
		"duplicate version": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			plan.params = append(plan.params, loadStandParam("plan_version", "3"))
			plan.digest = plan.computeDigest()
		},
		"missing version": func(plan *loadStandPlan, _ *[]loadStandPlannedAddress) {
			for i, param := range plan.params {
				if param.Name == "plan_version" {
					plan.params = slices.Delete(plan.params, i, i+1)
					break
				}
			}
			plan.digest = plan.computeDigest()
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			plan, rows := loadStandAddressFixture(t, 2, 2)
			change(&plan, &rows)
			assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressInvalid)
		})
	}
	assertLoadStandAddressRefused(t, loadStandPlan{}, nil, errLoadStandAddressInvalid)
}

func TestLoadStandAddressRejectsAliasesAndWrongRange(t *testing.T) {
	for _, address := range []domain.PeerAddress{
		"", "240.2.0.1", "240.2.0.1:", "240.2.0.1:0", "240.2.0.1:65536", "240.2.0.1:-1",
		"240.2.0.1:+64646", "240.2.0.1:064646", "240.02.0.1:64646", "[240.2.0.1]:64646",
		" 240.2.0.1:64646", "240.2.0.1:64646 ", "240.2.0.1:64646,240.1.0.1:64646",
		"localhost:64646", "240.2.0.1.example:64646", "[::ffff:240.2.0.1]:64646",
		"[fe80::1%lo0]:64646", "[2001:db8::1]:64646", "127.0.0.1:64646", "10.0.0.1:64646",
		"192.168.1.1:64646", "0.0.0.0:64646", "239.255.0.1:64646", "65.108.204.190:64646",
	} {
		t.Run(string(address), func(t *testing.T) {
			plan, rows := loadStandAddressFixture(t, 2, 2)
			rows[2].Address = address
			assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressInvalid)
		})
	}
	for _, address := range []domain.PeerAddress{"240.0.0.1:1", "240.0.0.1:65535", "255.255.0.1:64646"} {
		t.Run("valid boundary "+string(address), func(t *testing.T) {
			plan, rows := loadStandAddressFixture(t, 2, 2)
			old := rows[0].Address
			rows[0].Address = address
			for i := range rows {
				for j, target := range rows[i].Bootstrap {
					if target == old {
						rows[i].Bootstrap[j] = address
					}
				}
			}
			if _, err := validateLoadStandPrimaryIPv4(plan, rows); err != nil {
				t.Fatalf("canonical primary boundary refused: %v", err)
			}
		})
	}
}

func TestLoadStandAddressRejectsSharedGroups(t *testing.T) {
	for _, address := range []domain.PeerAddress{
		"240.0.0.1:64646", // Same endpoint.
		"240.0.0.1:64647", // Same IP, another endpoint.
		"240.0.0.2:64646", // Another IP, same /24.
		"240.0.1.1:64646", // Another /24, same /16.
	} {
		t.Run(string(address), func(t *testing.T) {
			plan, rows := loadStandAddressFixture(t, 2, 2)
			rows[2].Address = address // SDKs also consume a distinct source group.
			assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressInvalid)
		})
	}
}

func TestLoadStandAddressBootstrapIsExactAndNonempty(t *testing.T) {
	tests := map[string]func(*loadStandPlan, []loadStandPlannedAddress){
		"empty hub": func(_ *loadStandPlan, rows []loadStandPlannedAddress) { rows[0].Bootstrap = nil },
		"self":      func(_ *loadStandPlan, rows []loadStandPlannedAddress) { rows[0].Bootstrap[0] = rows[0].Address },
		"unknown":   func(_ *loadStandPlan, rows []loadStandPlannedAddress) { rows[0].Bootstrap[0] = "240.9.0.1:64646" },
		"SDK":       func(_ *loadStandPlan, rows []loadStandPlannedAddress) { rows[0].Bootstrap[0] = rows[2].Address },
		"duplicate": func(_ *loadStandPlan, rows []loadStandPlannedAddress) {
			rows[0].Bootstrap = append(rows[0].Bootstrap, rows[0].Bootstrap[0])
		},
		"alias": func(_ *loadStandPlan, rows []loadStandPlannedAddress) { rows[0].Bootstrap[0] = " 240.1.0.1:64646" },
		"wrong full": func(_ *loadStandPlan, rows []loadStandPlannedAddress) {
			if rows[2].Bootstrap[0] == rows[0].Address {
				rows[2].Bootstrap[0] = rows[1].Address
			} else {
				rows[2].Bootstrap[0] = rows[0].Address
			}
		},
		"plan self target": func(plan *loadStandPlan, _ []loadStandPlannedAddress) {
			plan.nodes[0].dialTargets[0] = plan.nodes[0].id
		},
		"plan SDK target": func(plan *loadStandPlan, rows []loadStandPlannedAddress) {
			plan.nodes[0].dialTargets[0] = plan.nodes[2].id
			rows[0].Bootstrap[0] = rows[2].Address
		},
		"plan unknown target": func(plan *loadStandPlan, _ []loadStandPlannedAddress) { plan.nodes[0].dialTargets[0] = "unknown" },
		"plan duplicate target": func(plan *loadStandPlan, rows []loadStandPlannedAddress) {
			plan.nodes[0].dialTargets = append(plan.nodes[0].dialTargets, plan.nodes[0].dialTargets[0])
			rows[0].Bootstrap = append(rows[0].Bootstrap, rows[0].Bootstrap[0])
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			plan, rows := loadStandAddressFixture(t, 2, 2)
			change(&plan, rows)
			plan.digest = plan.computeDigest()
			assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressInvalid)
		})
	}
	t.Run("ordered targets", func(t *testing.T) {
		plan, rows := loadStandAddressFixture(t, 3, 0)
		slices.Reverse(rows[2].Bootstrap)
		assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressInvalid)
	})
	t.Run("single hub empty planned bootstrap", func(t *testing.T) {
		opts := loadStandPlanOpts{
			Seed: 1, FullNodes: 2, Hubs: 1, EdgeUplinks: 1,
			Duration: time.Second, SampleInterval: time.Second,
			Churn: loadStandChurnOpts{Profile: loadStandChurnQuiet},
		}
		plan := mustBuildLoadStandPlan(t, opts)
		assertLoadStandAddressRefused(t, plan, loadStandAddressFixtureRows(plan), errLoadStandAddressInvalid)
	})
}

// Construct only a topology/digest boundary fixture. Replaying the generator's
// contact selection over 10000 nodes would add quadratic fixture allocations
// unrelated to this linear address check. No processes or runtime profile run.
func loadStandAddressCapacityFixture(t *testing.T, edges loadStandNodeCount) (loadStandPlan, []loadStandPlannedAddress) {
	t.Helper()
	plan, _ := loadStandAddressFixture(t, 2, 0)
	for i := range int(edges) {
		plan.nodes = append(plan.nodes, loadStandPlanNode{
			id: loadStandEdgeNodeID(i), role: loadStandRoleEdge,
			dialTargets: []loadStandNodeID{loadStandFullNodeID(0)},
		})
	}
	for i, param := range plan.params {
		if param.Name == "edge_nodes" {
			plan.params[i] = loadStandParam("edge_nodes", fmt.Sprint(edges))
		}
	}
	plan.digest = plan.computeDigest()
	return plan, loadStandAddressFixtureRows(plan)
}

func TestLoadStandAddressCapacityCountsSDKs(t *testing.T) {
	plan, rows := loadStandAddressCapacityFixture(t, 4094)
	check, err := validateLoadStandPrimaryIPv4(plan, rows)
	if err != nil || len(check.Rows()) != 4096 {
		t.Fatalf("4096 total distinct groups refused: count=%d err=%v", len(check.Rows()), err)
	}
	for _, edges := range []loadStandNodeCount{4095, 9998} {
		plan, rows = loadStandAddressCapacityFixture(t, edges)
		assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressCapacity)
	}
	// Candidate count has its own early capacity check, even for a small plan.
	plan, rows = loadStandAddressFixture(t, 2, 2)
	rows = append(rows, make([]loadStandPlannedAddress, 4093)...)
	assertLoadStandAddressRefused(t, plan, rows, errLoadStandAddressCapacity)
}

func TestLoadStandAddressSnapshotsDoNotAliasInputsOrGetters(t *testing.T) {
	plan, rows := loadStandAddressFixture(t, 2, 2)
	beforePlan, beforeRows := plan.computeDigest(), cloneLoadStandPlannedAddresses(rows)
	check, err := validateLoadStandPrimaryIPv4(plan, rows)
	if err != nil {
		t.Fatal(err)
	}
	if plan.computeDigest() != beforePlan || !reflect.DeepEqual(rows, beforeRows) {
		t.Fatal("validation mutated its inputs")
	}
	want := append([]loadStandAddressRow(nil), check.Rows()...)
	// The oracle must own its nested slices independently of Rows: a shallow
	// getter must not mutate the expectation together with the retained value.
	for i := range want {
		want[i].Bootstrap = append([]domain.PeerAddress(nil), want[i].Bootstrap...)
	}
	rows[0].Node = "changed"
	rows[0].Address = "changed"
	rows[0].Bootstrap[0] = "changed"
	plan.nodes[0].dialTargets[0] = "changed"
	got := check.Rows()
	got[0].Node = "changed"
	got[0].Bootstrap[0] = "changed"
	if !reflect.DeepEqual(check.Rows(), want) || check.Digest() != beforePlan {
		t.Fatal("input or getter mutation changed retained address check")
	}
}

func assertLoadStandAddressRefused(t *testing.T, plan loadStandPlan, rows []loadStandPlannedAddress, want error) {
	t.Helper()
	beforePlan, beforeRows := plan.computeDigest(), cloneLoadStandPlannedAddresses(rows)
	check, err := validateLoadStandPrimaryIPv4(plan, rows)
	if !errors.Is(err, want) {
		t.Fatalf("want refusal %v, got %v", want, err)
	}
	if len(check.Rows()) != 0 || check.Digest() != "" {
		t.Fatal("refusal returned a partial result")
	}
	if plan.computeDigest() != beforePlan || !reflect.DeepEqual(rows, beforeRows) {
		t.Fatal("refusal mutated the plan or explicit candidate rows")
	}
}

// A constant tuple could satisfy today's 240/4 examples while ceasing to use
// the source label's real classifier. Pin the direct same-tree calls as well
// as behavior; a copied HEAD implementation is not an old-label measurement.
func TestLoadStandAddressSameTreeClassifierCalls(t *testing.T) {
	source, err := os.ReadFile("loadstand_address_plan_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "loadstand_address_plan_test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"canonicalIPFromHost": false, "classifyHost": false, "isNonRoutableIP": false,
		"subnetGroupKey": false, "sourceGroup": false,
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "loadStandClassifyPrimaryAddress" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok {
				if _, expected := want[id.Name]; expected {
					want[id.Name] = true
				}
			}
			return true
		})
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("same-tree direct classifier call missing: %s", name)
		}
	}
	for _, importSpec := range file.Imports {
		if strings.Contains(importSpec.Path.Value, "testutil/loadstand") {
			t.Fatal("static address validation gained a runtime owner dependency")
		}
	}
}
