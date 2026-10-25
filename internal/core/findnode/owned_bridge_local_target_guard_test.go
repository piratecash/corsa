package findnode

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// Finite Source closure. Raw pins bind exact constructor, staging, T and
// completion bodies; functional tests independently check observable causality.
func ownedLocalTargetSourceErrors(sources map[string][]byte) []string {
	pins := map[string]string{"owned_bridge_local_target_timing.go": "69356ebd3196f7b042d465cb72eb880ded354cc49ed2e334c3aebb3f179e0a1d", "owned_resources.go": "7b3347cbe548e78ab4a92ebcf4c857545edd33d81d3a3af39c25a079b3aa036a", "owned_bridge_forward_answer.go": "c96366c955ce0c2ae5f90e1caee46c697d7672c7ed0bb7e852833bd97eb5e139", "owned_bridge.go": "b88656fb08a5dd33b0f2bdb7b9e52f636eb1b320291e1a390c2c9b683a593f57", "signing.go": "c0389bb2c6e127ff266860eaf0eb24a429c7c1cee9081fad0844483a7938b40b", "target.go": "15c107401df68ec9527f6ce46e4d6311c6cbd79ffea7ac42b3d3bbbe2fbe6d81", "target_owner.go": "f6f6f51e69ace1888584e95a5d09f08cf13e8b6936cb68e6d022549d726b5511", "owned_bridge_local_target.go": "19e3da9a1091b13d60f7cd9998cfdd5b9f9ed143339e6858d9e44490e63b9769", "owned_bridge_local_target_stage.go": "39fde17cdadd3dfa02bc97ef40f9d21d37f18ca865abde2fed2005e680baa0c3", "owned_bridge_answer.go": "6cf27edfd3d0f5ce5595dd602cd8aac74f7848a3e85392dd20377affe8f6043e"}
	var failures []string
	for path, want := range pins {
		if fmt.Sprintf("%x", sha256.Sum256(sources[path])) != want {
			failures = append(failures, "owned local exact Source changed: "+path)
		}
	}
	closed := map[string]bool{"OwnedRelayEndpointClock": true, "OwnedRelayEndpointClockUnknown": true, "OwnedRelayEndpointClockFixture": true, "OwnedRelayEndpointClockRuntime": true, "OwnedRelayWriteOutcome": true, "OwnedRelayWriteUnreported": true, "OwnedRelayWriteReportedOK": true, "OwnedRelayWriteReportedFailed": true, "OwnedRelayEndpointObservation": true, "RelayEndpointObservation": true, "OwnedTargetEndpointClock": true, "OwnedTargetEndpointClockUnknown": true, "OwnedTargetEndpointClockFixture": true, "OwnedTargetEndpointClockRuntime": true, "OwnedTargetEndpointObservation": true, "TargetEndpointObservation": true, "ownedRelayEndpointObservation": true, "relayEndpointObservation": true, "ownedAnswerWriteObservation": true, "ownedAnswerWriteUnreported": true, "ownedAnswerWriteReportedOK": true, "ownedAnswerWriteReportedFailed": true, "OwnedTargetTimingProfile": true, "OwnedTargetTimingRefusal": true, "ownedTargetServiceWindow": true, "startBoundary": true, "readyBoundary": true, "NewLocalTargetWithTiming": true, "newLocalTarget": true, "OwnedLocalTarget": true, "ownedLocalTargetState": true, "OwnedLocalSignWork": true, "OwnedLocalSignPermit": true, "ownedLocalSignState": true, "OwnedAnswerWork": true, "ownedAnswerState": true, "OwnedQueuedAnswer": true, "OwnedAnswerPermit": true, "mintOwnedAnswerWork": true, "ownedAnswerReady": true, "ownedAnswerPreparing": true, "ownedAnswerQueued": true, "ownedAnswerTaking": true, "ownedAnswerStarted": true, "ownedAnswerDone": true, "ownedAnswerCancelled": true, "OwnedLocalResult": true, "ownedLocalEntryInstall": true, "ownedLocalAdmissionStage": true, "localTarget": true, "ownedLocalSignReady": true, "ownedLocalSignTaking": true, "ownedLocalSignConsumed": true, "ownedLocalSignCompleting": true, "ownedLocalSignDone": true, "ownedLocalSignCancelled": true, "NewLocalTarget": true, "ReserveReceived": true, "stageOwnedLocalAdmissionLocked": true, "ownedLocalOriginal": true, "cancelOwnedLocalOriginal": true, "drainOwnedLocalAliasesLocked": true, "consumeSourceLocked": true, "prepareConsumptionLocked": true, "consumeActualSourceLocked": true}
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return append(failures, "owned local Source parse")
		}
		_, pinned := pins[path]
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && closed[id.Name] && !pinned {
				failures = append(failures, "owned local authority escaped exact Source: "+path+":"+id.Name)
			}
			return true
		})
	}
	return failures
}
func TestOwnedLocalTargetClosedSource(t *testing.T) {
	if failures := ownedLocalTargetSourceErrors(preparedSessionSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestOwnedLocalTargetSourceControls(t *testing.T) {
	for _, tc := range []struct{ name, path, before, after string }{
		{"timed-projected-equality", "owned_bridge_local_target.go", "slotAt >= serviceWindow.latestSignStart", "slotAt > serviceWindow.latestSignStart"},
		{"timed-start-fallback", "owned_bridge_local_target_timing.go", "return c.serviceWindow.latestSignStart", "return c.signBy"},
		{"timed-ready-fallback", "owned_bridge_local_target_timing.go", "return c.serviceWindow.signatureReadyBy", "return c.signBy"},
		{"reply-expiry-service-shortening", "owned_bridge_local_target.go", "answerExpires := w.entry.coordinated.signBy", "answerExpires := w.entry.coordinated.readyBoundary()"},
		{"accepted-historical-stamp", "owned_bridge_local_target.go", "stamp, lease, err := owner.consumer.ReserveAcceptedClock()", "stamp := w.identity.stamp\n\tlease, err := owner.consumer.TryReadClock(stamp)"},
		{"strict-projected-deadline", "owned_bridge_local_target.go", "if slotAt >= deadline", "if slotAt > deadline"},
		{"retained-L1-downgrade", "target.go", "if entry.phase == targetReserved && entry.coordinated != nil", "if false && entry.phase == targetReserved && entry.coordinated != nil"},
		{"closed-L1-reference", "target.go", "if original == session {\n\t\t\t\t\tstate.protected[index] = Session{}\n\t\t\t\t}", "if original == session && false {\n\t\t\t\t\tstate.protected[index] = Session{}\n\t\t\t\t}"},
		{"constructor-caller-time", "owned_bridge_local_target.go", "NewTargetCoordinator(self, network, profile, Instant(at.Elapsed()))", "NewTargetCoordinator(self, network, profile, Instant(0))"},
		{"constructor-foreign-point", "owned_bridge_local_target.go", "if !owner.consumer.MatchesPoint(at) {\n\t\treturn OwnedLocalTarget{}, ErrUnauthenticated\n\t}", "if false {\n\t\treturn OwnedLocalTarget{}, ErrUnauthenticated\n\t}"},
		{"recipient-map-alias", "owned_bridge_local_target_stage.go", "copied.recipients = make(map[Session]domain.FindNodeRequestID, len(entry.recipients)+1)", "copied.recipients = entry.recipients"},
		{"coordinated-state-alias", "owned_bridge_local_target_stage.go", "state := *entry.coordinated\n\t\t\tcopied.coordinated = &state", "copied.coordinated = entry.coordinated"},
		{"early-release", "owned_bridge_local_target.go", "if err = o.calendar.consumeSourceLocked(alias, f);", "lease.Release(); if err = o.calendar.consumeSourceLocked(alias, f);"},
		{"omit-actual-source-debt", "signing.go", "state.signingActual.tokens -= forwardToken", "state.signingActual.tokens -= 0"},
		{"completion-current-lease", "owned_bridge_local_target.go", "clock, err := owner.finishObserver.ReserveClock()\n\tif err != nil {\n\t\tretire()\n\t\treturn nil, err\n\t}", "_, clock, err := owner.consumer.ReserveAcceptedClock()\n\tif err != nil {\n\t\tretire()\n\t\treturn nil, err\n\t}"},
		{"extend-deadline", "owned_bridge_local_target.go", "Instant(id.received.Elapsed()).plus(responderTimeout)", "Instant(c.at.Elapsed()).plus(responderTimeout)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			raw := string(sources[tc.path])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("unique owned-local Source anchor", tc.name)
			}
			sources[tc.path] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			if len(ownedLocalTargetSourceErrors(sources)) == 0 {
				t.Fatal("owned local Source control escaped", tc.name)
			}
		})
	}
	sources := preparedSessionSources(t)
	sources["owned_local_escape.go"] = []byte("package findnode\nvar escapedOwnedLocal *ownedLocalSignState\n")
	if len(ownedLocalTargetSourceErrors(sources)) == 0 {
		t.Fatal("owned local OTHERFILE authority escaped")
	}
}
