package node

import (
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

// Node, Service and SDK, including function values. Only the explicit private
// Runtime helper selects the paired root; public defaults remain zero-origin.
var ownDMClosedReferences = map[string]map[r2aReferenceSite]int{
	"NewInactiveOwnDMProvenanceCandidate": {},
	"NewServiceForRuntime":                {{"sdk/runtime.go", "newWithContext"}: 1},
	"newUnstartedRuntimeCleanup":          {{"node/runtime_unstarted_cleanup.go", "NewServiceForRuntime"}: 1, {"node/identity_pin_own_dm_runtime.go", "NewInactiveOwnDMRuntimeCandidate"}: 1},
	"NewInactiveOwnDMRuntimeCandidate":    {{"sdk/runtime.go", "newWithContext"}: 1},
	"newOwnDMAdmissionPort":               {{"node/identity_record_admission_own_dm.go", "NewInactiveOwnDMAdmissionCandidate"}: 1, {"node/identity_pin_own_dm_runtime.go", "install"}: 1},
	"newWithOwnDMRuntime":                 {},
	"newWithContext":                      {{"sdk/runtime.go", "NewWithContext"}: 1, {"sdk/runtime.go", "newWithOwnDMRuntime"}: 1},
	"NewDesktopClientWithOwnDMProvenance": {{"sdk/runtime.go", "newWithContext"}: 1},
	"newDesktopClient":                    {{"service/desktop.go", "NewDesktopClient"}: 1, {"service/desktop.go", "NewDesktopClientWithOwnDMProvenance"}: 1},
	"wireSubServicesWithOwnDM":            {{"service/desktop.go", "wireSubServices"}: 1, {"service/desktop.go", "newDesktopClient"}: 1},
	"NewInactiveOwnDMAdmissionCandidate":  {},
	"newDMCryptoWithOwnDMProvenance":      {{"service/desktop.go", "wireSubServicesWithOwnDM"}: 1},
	"ValidFor":                            {{"service/desktop.go", "newDesktopClient"}: 1, {"node/identity_pin_own_dm_runtime.go", "bindings"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"service/dm_own_provenance.go", "newDMCryptoWithOwnDMProvenance"}: 1, {"service/dm_own_provenance.go", "submitOwnDM"}: 1},
	"Submit":                              {},
	"SubmitWithResidence":                 {{"service/dm_own_provenance.go", "submitOwnDM"}: 1},
	"submitWithResidence":                 {{"node/identity_pin_own_dm_port.go", "Submit"}: 1, {"node/identity_pin_own_dm_port.go", "SubmitWithResidence"}: 1},
	"SendDirectMessageWithOwnDMResidence": {{"sdk/messages.go", "SendDirectMessage"}: 1},
	"sendDirectMessageWithResidence":      {{"service/dm_crypto.go", "SendDirectMessage"}: 1, {"service/desktop.go", "SendDirectMessageWithOwnDMResidence"}: 1},
	"TrySettle":                           {}, "RecipientIdentity": {}, "NearestHopIdentity": {},
	"publishR2OwnDMHop": {{"node/peer_sessions.go", "publishAndServeCMSession"}: 1},
	"submitOwnDM":       {{"service/dm_crypto.go", "sendDirectMessageWithResidence"}: 1},
	"r2OwnDMOwner":      {{"node/identity_pin_own_dm_runtime.go", "install"}: 2, {"node/identity_pin_own_dm_runtime.go", "bindings"}: 1, {"node/find_node_ready_construction.go", "bindings"}: 1, {"node/service.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"}: 1, {"node/identity_pin_own_dm_port.go", "ValidFor"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1, {"node/identity_pin_own_dm_port.go", "publishR2OwnDMHop"}: 1, {"node/peer_sessions.go", "finishPeerSessionForCM"}: 1, {"node/identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}: 1, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 2},
	"r2OwnDMHop":        {{"node/service.go", "<package>"}: 1, {"node/peer_sessions.go", "finishPeerSessionForCM"}: 1, {"node/identity_pin_own_dm_port.go", "publishR2OwnDMHop"}: 1, {"node/identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}: 1},
	"ownDMPort":         {{"service/dm_crypto.go", "<package>"}: 1, {"service/dm_own_provenance.go", "newDMCryptoWithOwnDMProvenance"}: 1, {"service/dm_own_provenance.go", "submitOwnDM"}: 2},
	"ownDMObserver":     {{"service/dm_crypto.go", "<package>"}: 1, {"service/dm_own_provenance.go", "newDMCryptoWithOwnDMProvenance"}: 1, {"service/dm_own_provenance.go", "submitOwnDM"}: 2},
	"r2Capability":      {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"}: 1, {"node/identity_pin_own_dm_port.go", "ValidFor"}: 4, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 5, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1, {"node/identity_record_admission_own_dm.go", "captureOwnDMResidence"}: 3},
	"r2Service":         {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"}: 1, {"node/identity_pin_own_dm_port.go", "ValidFor"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 2, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 4, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1},
	"r2Identity":        {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"}: 1, {"node/identity_pin_own_dm_port.go", "ValidFor"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1, {"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}: 1},
	"r2Submission":      {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1},
	"r2Owner":           {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 7},
	"r2Result":          {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1},
	"r2Refusal":         {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 2},
	"r2Recipient":       {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1, {"node/identity_pin_own_dm_port.go", "RecipientIdentity"}: 1},
	"r2Hop":             {{"node/identity_pin_own_dm_port.go", "<package>"}: 1, {"node/identity_pin_own_dm_port.go", "TrySettle"}: 1, {"node/identity_pin_own_dm_port.go", "NearestHopIdentity"}: 1},
	"r2PublishedReady":  {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}: 1, {"node/identity_pin_own_dm_port.go", "publishR2OwnDMHop"}: 1},
	"r2ResolvedHop":     {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}: 1, {"node/identity_pin_outgoing_provenance.go", "claimR2aFirstDirectedItemLocked"}: 2},
	"r2Dispatched":      {{"node/identity_pin_outgoing_provenance.go", "<package>"}: 1, {"node/identity_pin_outgoing_provenance.go", "submitR2aOwnDM"}: 1, {"node/identity_pin_own_dm_port.go", "submitWithResidence"}: 1},
}
var ownDMClosedConstructors = map[string]r2aReferenceSite{
	"OwnDMProvenancePort":  {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"},
	"ownDMProvenanceOwner": {"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"},
	"OwnDMSubmission":      {"node/identity_pin_own_dm_port.go", "submitWithResidence"},
	"ownDMSubmission":      {"node/identity_pin_own_dm_port.go", "submitWithResidence"},
	"OwnDMWriteEvidence":   {"node/identity_pin_own_dm_port.go", "TrySettle"},
}

func ownDMGuardErrors(sources map[string][]byte, expected map[string]map[r2aReferenceSite]int) []string {
	actual := map[string]map[r2aReferenceSite]int{}
	for name := range expected {
		actual[name] = map[r2aReferenceSite]int{}
	}
	var failures []string
	writes := map[r2aReferenceSite]map[string]int{}
	for file, source := range sources {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, source, 0)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		for _, decl := range parsed.Decls {
			site := r2aReferenceSite{file, "<package>"}
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site.function = fn.Name.Name
				body = fn.Body
			}
			if body == nil {
				continue
			}
			ast.Inspect(body, func(n ast.Node) bool {
				if assignment, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assignment.Lhs {
						if field, ok := lhs.(*ast.SelectorExpr); ok {
							if _, tracked := expected[field.Sel.Name]; tracked {
								if writes[site] == nil {
									writes[site] = map[string]int{}
								}
								writes[site][field.Sel.Name]++
							}
						}
					}
				}
				if id, ok := n.(*ast.Ident); ok {
					if counts, tracked := actual[id.Name]; tracked {
						counts[site]++
					}
				}
				if literal, ok := n.(*ast.CompositeLit); ok && len(literal.Elts) > 0 {
					name := ""
					switch typ := literal.Type.(type) {
					case *ast.Ident:
						name = typ.Name
					case *ast.SelectorExpr:
						name = typ.Sel.Name
					}
					if allowed, tracked := ownDMClosedConstructors[name]; tracked && allowed != site && ((name != "OwnDMProvenancePort" && name != "ownDMProvenanceOwner") || site != (r2aReferenceSite{"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"})) {
						failures = append(failures, fmt.Sprintf("opaque own-DM constructor %s at %v", name, site))
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "new" && len(call.Args) == 1 {
						if typ, ok := call.Args[0].(*ast.Ident); ok {
							if _, tracked := ownDMClosedConstructors[typ.Name]; tracked {
								failures = append(failures, "opaque own-DM new at "+site.function)
							}
						}
					}
				}
				return true
			})
		}
	}
	if len(expected) == len(ownDMClosedReferences) {
		want := map[r2aReferenceSite]map[string]int{
			{"node/identity_record_admission_own_dm.go", "newOwnDMAdmissionPort"}:       {"r2OwnDMOwner": 1},
			{"node/identity_pin_own_dm_port.go", "NewInactiveOwnDMProvenanceCandidate"}: {"r2OwnDMOwner": 1},
			{"node/identity_pin_own_dm_port.go", "publishR2OwnDMHop"}:                   {"r2PublishedReady": 1},
			{"node/identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}:  {"r2ResolvedHop": 1},
			{"service/dm_own_provenance.go", "newDMCryptoWithOwnDMProvenance"}:          {"ownDMPort": 1, "ownDMObserver": 1},
		}
		if !reflect.DeepEqual(writes, want) {
			failures = append(failures, fmt.Sprintf("own-DM owner writes %v, want %v", writes, want))
		}
	}
	for name, want := range expected {
		if !reflect.DeepEqual(actual[name], want) {
			failures = append(failures, fmt.Sprintf("own-DM %s references %v, want %v", name, actual[name], want))
		}
	}
	sort.Strings(failures)
	return failures
}
func ownDMProductionSources(t *testing.T) map[string][]byte {
	t.Helper()
	sources := map[string][]byte{}
	for _, dir := range []string{".", "../service", "../../../sdk"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			prefix := "node/"
			switch dir {
			case "../service":
				prefix = "service/"
			case "../../../sdk":
				prefix = "sdk/"
			}
			sources[prefix+filepath.Base(file)] = raw
		}
	}
	return sources
}
func TestOwnDMPortClosedCrossPackageAuthority(t *testing.T) {
	if failures := ownDMGuardErrors(ownDMProductionSources(t), ownDMClosedReferences); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}
func TestOwnDMPortGuardRejectsMintEscapeAndLateAttach(t *testing.T) {
	for _, source := range []string{
		"package service;func ordinary(){_=node.NewInactiveOwnDMProvenanceCandidate}",
		"package service;func ordinary(){_=newDMCryptoWithOwnDMProvenance}",
		"package service;func ordinary(){_ = node.OwnDMProvenancePort{owner:owner}}",
		"package service;func ordinary(){d.ownDMPort=p}",
		"package service;func ordinary(){_ = p.Submit}",
	} {
		if len(ownDMGuardErrors(map[string][]byte{"service/escape.go": []byte(source)}, map[string]map[r2aReferenceSite]int{"NewInactiveOwnDMProvenanceCandidate": {}, "newDMCryptoWithOwnDMProvenance": {}, "Submit": {}, "ownDMPort": {}})) == 0 {
			t.Fatal("own-DM guard accepted mint/escape/late attach")
		}
	}
}
