package node

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type r2aReferenceSite struct{ file, function string }

// Exact multisets, reviewed with the inactive foundation. A same-count caller
// replacement, including a method value rather than a call, changes membership.
var r2aClosedReferences = map[string]map[r2aReferenceSite]int{
	"settleR2aOwnHopLocked":  {{"identity_pin_outgoing_provenance.go", "settleR2aOwnHop"}: 1, {"identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1},
	"prepareR2aFixtureOwnDM": {}, "submitR2aFixtureOwnDM": {}, "openR2aFixtureHop": {}, "settleR2aOwnHop": {{"identity_pin_own_dm_port.go", "TrySettle"}: 1},
	"prepareR2aOwnDM":                      {{"identity_pin_outgoing_provenance.go", "prepareR2aFixtureOwnDM"}: 1, {"identity_pin_own_dm_port.go", "Submit"}: 1},
	"submitR2aOwnDM":                       {{"identity_pin_outgoing_provenance.go", "submitR2aFixtureOwnDM"}: 1, {"identity_pin_own_dm_port.go", "Submit"}: 1},
	"storeIncomingMessageWithR2a":          {{"service.go", "storeIncomingMessage"}: 1, {"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"}: 1},
	"finishPeerSessionForCM":               {{"peer_sessions.go", "openPeerSessionForCM"}: 1, {"identity_pin_outgoing_provenance.go", "openR2aFixtureHop"}: 1},
	"captureR2aHopPublication":             {{"identity_pin_outgoing_provenance.go", "openR2aFixtureHop"}: 1, {"peer_sessions.go", "finishPeerSessionForCM"}: 1},
	"registerR2aOrigin":                    {{"service.go", "storeIncomingMessageWithR2a"}: 1},
	"sealR2aWireProjection":                {{"service.go", "storeIncomingMessageWithR2a"}: 1},
	"r2aUpdateInitialHold":                 {{"service.go", "storeIncomingMessageWithR2a"}: 1},
	"existingR2aOrigin":                    {{"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"}: 1},
	"snapshotR2aDirectedSession":           {{"relay.go", "sendTableRoutedRelayMessageWithR2a"}: 1},
	"captureR2aFirstDirectedItem":          {{"relay.go", "sendTableRoutedRelayMessageWithR2a"}: 1},
	"claimR2aFirstDirectedItemLocked":      {{"identity_pin_outgoing_provenance.go", "captureR2aFirstDirectedItem"}: 1},
	"r2aRecordOffer":                       {{"relay.go", "sendTableRoutedRelayMessageWithR2a"}: 1},
	"sendTableDirectedRelayWithR2a":        {{"service.go", "storeIncomingMessageWithR2a"}: 1, {"routing_relay.go", "sendTableDirectedRelay"}: 1},
	"sendTableRoutedRelayToAddressWithR2a": {{"routing_relay.go", "sendTableDirectedRelayWithR2a"}: 1, {"routing_relay.go", "sendTableRoutedRelayToAddress"}: 1},
	"sendTableRoutedRelayMessage":          {{"routing_relay.go", "sendTableRoutedRelayToAddressWithR2a"}: 1},
	"sendTableRoutedRelayMessageWithR2a":   {{"relay.go", "sendTableRoutedRelayMessage"}: 1, {"routing_relay.go", "sendTableRoutedRelayToAddressWithR2a"}: 1},
}
var r2aConstructors = map[string]r2aReferenceSite{
	"r2aPreparedOwnDM":      {"identity_pin_outgoing_provenance.go", "prepareR2aOwnDM"},
	"r2aPreparedEnvelope":   {"identity_pin_outgoing_provenance.go", "prepareR2aOwnDM"},
	"r2aHopPublication":     {"identity_pin_outgoing_provenance.go", "captureR2aHopPublication"},
	"r2aPublishedHop":       {"identity_pin_outgoing_provenance.go", "captureR2aHopPublication"},
	"r2aDispatch":           {"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"},
	"r2aOwnSendStamp":       {"identity_pin_outgoing_provenance.go", "registerR2aOrigin"},
	"r2aFirstWrite":         {"identity_pin_outgoing_provenance.go", "claimR2aFirstDirectedItemLocked"},
	"r2aDirectedDispatch":   {"routing_relay.go", "sendTableDirectedRelayWithR2a"},
	"r2aWireProjection":     {"identity_pin_outgoing_provenance.go", "sealR2aWireProjection"},
	"r2aSubmissionResult":   {"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"},
	"r2aRecipientEvidence":  {"identity_pin_outgoing_provenance.go", "settleR2aOwnHopLocked"},
	"r2aNearestHopEvidence": {"identity_pin_outgoing_provenance.go", "settleR2aOwnHopLocked"},
}

func r2aGuardErrors(sources map[string][]byte, expected map[string]map[r2aReferenceSite]int) []string {
	actual := map[string]map[r2aReferenceSite]int{}
	fields := map[string]map[r2aReferenceSite]int{"outgoingProvenance": {}, "ack": {}, "sealed": {}, "prepared": {}}
	var failures []string
	writes := map[r2aReferenceSite]map[string]int{}
	for name := range expected {
		actual[name] = map[r2aReferenceSite]int{}
	}
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
			ast.Inspect(body, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if sites, tracked := fields[selector.Sel.Name]; tracked {
						sites[site]++
					}
				}
				if assignment, ok := node.(*ast.AssignStmt); ok && file == "identity_pin_outgoing_provenance.go" {
					for _, lhs := range assignment.Lhs {
						if _, ok := lhs.(*ast.SelectorExpr); ok {
							var rendered bytes.Buffer
							if err := format.Node(&rendered, token.NewFileSet(), lhs); err != nil {
								failures = append(failures, err.Error())
							}
							if writes[site] == nil {
								writes[site] = map[string]int{}
							}
							writes[site][rendered.String()]++
						}
					}
				}
				if id, ok := node.(*ast.Ident); ok {
					if sites, tracked := actual[id.Name]; tracked {
						sites[site]++
					}
				}
				if literal, ok := node.(*ast.CompositeLit); ok && len(literal.Elts) > 0 {
					if name, ok := literal.Type.(*ast.Ident); ok {
						if allowed, tracked := r2aConstructors[name.Name]; tracked && site != allowed && (name.Name != "r2aPreparedOwnDM" || site != (r2aReferenceSite{"identity_record_admission_own_dm.go", "bindOwnDMResidence"})) {
							failures = append(failures, fmt.Sprintf("%s constructed at %v", name.Name, site))
						}
					}
				}
				if call, ok := node.(*ast.CallExpr); ok {
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "new" && len(call.Args) == 1 {
						if typ, ok := call.Args[0].(*ast.Ident); ok {
							if _, tracked := r2aConstructors[typ.Name]; tracked {
								failures = append(failures, "opaque constructor through new at "+site.function)
							}
						}
					}
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "close" && len(call.Args) == 1 {
						if field, ok := call.Args[0].(*ast.SelectorExpr); ok && field.Sel.Name == "ack" && file == "identity_pin_outgoing_provenance.go" {
							failures = append(failures, "producer closed owned ACK")
						}
					}
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok && len(call.Args) > 0 {
						zeroWrappers := map[string]string{"storeIncomingMessage": "storeIncomingMessageWithR2a", "sendTableDirectedRelay": "sendTableDirectedRelayWithR2a", "sendTableRoutedRelayToAddress": "sendTableRoutedRelayToAddressWithR2a", "sendTableRoutedRelayMessage": "sendTableRoutedRelayMessageWithR2a"}
						if target, tracked := zeroWrappers[site.function]; tracked && target == selector.Sel.Name {
							value, ok := call.Args[len(call.Args)-1].(*ast.Ident)
							if !ok || value.Name != "nil" {
								failures = append(failures, "ordinary wrapper gained nonzero origin: "+site.function)
							}
						}
					}
				}
				return true
			})
		}
	}
	for name, want := range expected {
		if !reflect.DeepEqual(actual[name], want) {
			failures = append(failures, fmt.Sprintf("%s references %v, want %v", name, actual[name], want))
		}
	}
	if len(expected) == len(r2aClosedReferences) {
		wantFields := map[string]map[r2aReferenceSite]int{
			"outgoingProvenance": {{"identity_pin_outgoing_provenance.go", "registerR2aOrigin"}: 1, {"identity_pin_outgoing_provenance.go", "r2aCurrentLocked"}: 1, {"identity_pin_outgoing_provenance.go", "existingR2aOrigin"}: 2},
			"ack":                {{"identity_pin_outgoing_provenance.go", "settleR2aOwnHopLocked"}: 1, {"relay.go", "sendTableRoutedRelayMessageWithR2a"}: 1},
			"sealed":             {{"identity_record_admission_own_dm.go", "bindOwnDMResidence"}: 4, {"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"}: 5, {"identity_pin_outgoing_provenance.go", "openR2aFixtureHop"}: 1, {"identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}: 1, {"peer_sessions.go", "finishPeerSessionForCM"}: 3, {"identity_pin_own_dm_port.go", "Submit"}: 3},
			"prepared":           {{"identity_pin_outgoing_provenance.go", "registerR2aOrigin"}: 3, {"identity_pin_outgoing_provenance.go", "r2aCurrentLocked"}: 4, {"identity_pin_outgoing_provenance.go", "claimR2aFirstDirectedItemLocked"}: 8, {"identity_pin_outgoing_provenance.go", "settleR2aOwnHop"}: 1, {"identity_pin_outgoing_provenance.go", "settleR2aOwnHopLocked"}: 5, {"identity_record_admission.go", "expireRelationSlotsLocked"}: 1, {"identity_record_admission_own_dm.go", "ownDMObservation"}: 1, {"identity_record_admission_own_dm.go", "registerOwnDMRecordAdmission"}: 3, {"identity_record_admission_own_dm.go", "ownDMClaimCurrentLocked"}: 2, {"identity_record_admission_own_dm.go", "stageOwnDMRelationRecordLocked"}: 3},
		}

		for field, want := range wantFields {
			if !reflect.DeepEqual(fields[field], want) {
				failures = append(failures, fmt.Sprintf("%s field references %v want%v", field, fields[field], want))
			}
		}
		wantWrites := map[r2aReferenceSite]map[string]int{
			{"identity_pin_outgoing_provenance.go", "submitR2aOwnDM"}:                  {"dispatch.stamp": 1},
			{"identity_pin_outgoing_provenance.go", "registerR2aOrigin"}:               {"d.failure": 3, "entry.outgoingProvenance": 1, "d.stamp": 1, "d.early": 1},
			{"identity_pin_outgoing_provenance.go", "r2aUpdateInitialHold"}:            {"d.stamp.entry.Hold": 1},
			{"identity_pin_outgoing_provenance.go", "openR2aFixtureHop"}:               {"publication.sealed.session": 1},
			{"identity_pin_outgoing_provenance.go", "claimR2aFirstDirectedItemLocked"}: {"d.stamp.candidate": 1},
			{"identity_pin_outgoing_provenance.go", "r2aRecordOffer"}:                  {"candidate.offered": 1},
			{"identity_pin_outgoing_provenance.go", "settleR2aOwnHopLocked"}:           {"stamp.consumed": 1},
			{"identity_pin_outgoing_provenance.go", "sealR2aWireProjection"}:           {"d.projection": 1},
			{"identity_pin_outgoing_provenance.go", "snapshotR2aDirectedSession"}:      {"route.origin.r2ResolvedHop": 1},
		}
		if !reflect.DeepEqual(writes, wantWrites) {
			failures = append(failures, fmt.Sprintf("private owner writes%v want%v", writes, wantWrites))
		}
	}
	sort.Strings(failures)
	return failures
}
func TestR2aClosedProductionReferences(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		sources[file] = raw
	}
	if failures := r2aGuardErrors(sources, r2aClosedReferences); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}
func TestR2aReferenceGuardRejectsFunctionValueAndSameCountReplacement(t *testing.T) {
	expected := map[string]map[r2aReferenceSite]int{"captureR2aHopPublication": {{"sample.go", "openR2aFixtureHop"}: 1}}
	positive := map[string][]byte{"sample.go": []byte("package node;func openR2aFixtureHop(){s.captureR2aHopPublication(t)}")}
	if got := r2aGuardErrors(positive, expected); len(got) != 0 {
		t.Fatal(got)
	}
	for _, source := range []string{
		"package node;func ordinary(){s.captureR2aHopPublication(t)}",
		"package node;func ordinary(){leak:=s.captureR2aHopPublication;_ = leak}",
		"package node;var leak = (*Service).captureR2aHopPublication",
	} {
		if len(r2aGuardErrors(map[string][]byte{"sample.go": []byte(source)}, expected)) == 0 {
			t.Fatal("source guard accepted caller substitution/escape")
		}
	}
}
