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
		"node/identity_record_admission_own_dm.go": "80542c362e3abf08da6463869ff1402a19b475754a3f164f2c88d999c35c3ade",
		"node/identity_pin_own_dm_runtime.go":      "39b178ddcdeb0ee8979a264484dd2f5eea37e3f9a869e9568a768a965a194abb",
		"node/find_node_ready_construction.go":     "9d0318eb4799ec9b6df1ffe756c74ed7ae9a843f908cc7bd4e85856ebe8b9aad",
		"node/service.go":                          "68841af478e783ec4f833092393f7ec2ebfe4270e6a3e1666066bbf66ec0ac4f",
		"service/desktop.go":                       "86ca0fbe70740c5210d64fd19dda0d5d87f0c7f89a736afab142038bef076735",
		"sdk/runtime.go":                           "2a3c1184b8c411bdd034933564fa68a0e0131115660adbcef8cb815b9080fd1f",
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
