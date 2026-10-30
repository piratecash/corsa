package node

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// Exact existing cross-package caller/type closure plus six reviewed original
// composition bodies. Formatting requires a finite rebind, not normalization.
func ownDMRuntimeSourceErrors(sources map[string][]byte) []string {
	failures := ownDMGuardErrors(sources, ownDMClosedReferences)
	pins := map[string]string{
		"node/runtime_unstarted_cleanup.go":        "eb2270929fc80c9ce846c32925529be6d8e5f6042735842448bb4ee7cf242c03",
		"node/identity_record_admission_own_dm.go": "8037b80af62d390fef07e9601362c653c7c4bf1e3eed8bffaf801e10fd19805b",
		"node/identity_pin_own_dm_runtime.go":      "39b178ddcdeb0ee8979a264484dd2f5eea37e3f9a869e9568a768a965a194abb",
		"node/find_node_ready_construction.go":     "928f01bab660f066126117dee1b756869ad323b493fcdc83a0a53694db53f6c4",
		"node/service.go":                          "1de4f9d12201bfe12d10f0f431b810a3d1c333e369a8b1254e55975af69b2d2e",
		"service/desktop.go":                       "37c9aee24a9d25282b6a41023ee3bb841577575ce1151727fe0c2ae3739ce565",
		"sdk/runtime.go":                           "3916f6f435eb601af40ebf40fcec100649562912bc6089804a9024c475482671",
	}
	for p, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[p])) != want {
			failures = append(failures, "own-DM Runtime original composition changed: "+p)
		}
	}
	return failures
}
func TestOwnDMRuntimeClosedOriginalComposition(t *testing.T) {
	if failures := ownDMRuntimeSourceErrors(ownDMProductionSources(t)); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}
func TestOwnDMRuntimeRejectsDefaultActivationAndAlternateMint(t *testing.T) {
	baseline := ownDMProductionSources(t)
	for _, tc := range []struct{ file, old, new string }{
		{"sdk/runtime.go", "if !nodeStarted && r.cancelConstruction != nil", "if r.cancelConstruction != nil"},
		{"sdk/runtime.go", "return newWithContext(ctx, cfg, nil)", "return newWithContext(ctx, cfg, &profile)"},
		{"node/identity_pin_own_dm_runtime.go", "return s, own.port, newUnstartedRuntimeCleanup(s, ready.originalCancel), nil", "return s, own.port, newUnstartedRuntimeCleanup(s, foreignCancel), nil"},
		{"node/find_node_ready_construction.go", "*r.ownDMRuntime != r.originalOwnDMRuntime", "false"},
		{"service/desktop.go", "c.prober = NewNodeProber(c.rpc, c.dm, c.info)", "c.prober = NewNodeProber(c.rpc, NewDMCrypto(c.rpc,c.chatlog,c.id), c.info)"},
		// The pre-M1 constructor must not regain acceptance after the pin rebind.
		{"sdk/runtime.go", "\t\tif !internalCfg.RPC.AuthEnabled() {\n\t\t\tabandon()\n\t\t\treturn nil, fmt.Errorf(\"create rpc server: %w\", ErrRPCAuthenticationRequired)\n\t\t}\n", ""},
	} {
		copy := map[string][]byte{}
		for p, b := range baseline {
			copy[p] = b
		}
		raw := string(copy[tc.file])
		if strings.Count(raw, tc.old) != 1 {
			t.Fatal("Runtime control anchor missing", tc.file)
		}
		copy[tc.file] = []byte(strings.Replace(raw, tc.old, tc.new, 1))
		if len(ownDMRuntimeSourceErrors(copy)) == 0 {
			t.Fatal("Runtime composition fault accepted", tc.file)
		}
	}
	copy := map[string][]byte{}
	for p, b := range baseline {
		copy[p] = b
	}
	copy["sdk/escape.go"] = []byte("package sdk;func escape(){_=corsanode.NewInactiveOwnDMRuntimeCandidate}")
	if len(ownDMRuntimeSourceErrors(copy)) == 0 {
		t.Fatal("alternate root escaped exact caller census")
	}
}
