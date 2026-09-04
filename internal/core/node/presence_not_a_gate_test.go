package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// presence_not_a_gate_test.go holds ONE rule, and it is narrower than "presence
// and delivery never meet":
//
//	presence may WAKE delivery; it must never DECIDE for it.
//
// The distinction is the whole of it. Presence noticing that a contact came
// back is the best trigger this node has, and it rings the existing bell —
// kickDeliveryRetriesForReachable, from refreshPresenceSnapshot. What the
// retry then does is re-ask ROUTING, exactly as before, because routing is
// what knows whether a frame can be handed over.
//
// Why the asymmetry matters: presence is allowed to be wrong. It says "we think
// they are away" and a person shrugs. A wrong wake costs one wasted pass. A
// wrong GATE costs a message: a contact whose liveness cannot be proven — an
// old build, a node with the datagram layer off, a contact whose record was
// never resolved — would become permanently unreachable. Same class of bug this
// work removes, with a worse blast radius.
//
// So the guard below scans the files that DECIDE whether a message goes out,
// and requires that none of them can read presence. The waking happens in the
// other direction, from presence_service.go into delivery's own entry point,
// which is why that file is not in the list.
//
// The invariant is stated in docs/protocol/presence.md §6.

// presenceDeliveryOwnedFiles are the paths that decide whether a message goes
// out. If presence appears in any of them, either the invariant was broken or
// this list is stale — and either way somebody has to look.
var presenceDeliveryOwnedFiles = []string{
	"delivery_retry.go",
	"outbound_delivery_gate.go",
	"emission_lane.go",
	"delivery_cancel.go",
	"envelope_retention.go",
	"relay.go",
}

// presenceForbiddenInDelivery are the identifiers that would mean presence has
// been consulted. Deliberately the accessors and not the word "presence": the
// delivery files legitimately mention presence in prose (they explain why they
// do NOT use it) and a substring match on the word would make this test fire on
// a comment.
var presenceForbiddenInDelivery = []string{
	"PresenceSnapshot(",
	"presenceProjector",
	"presenceSnap",
	"domain.PresenceState",
	"domain.PresenceSet",
	"domain.Presence{",
}

func TestPresenceNeverGatesDelivery(t *testing.T) {
	for _, name := range presenceDeliveryOwnedFiles {
		path := filepath.Join(".", name)
		source, err := os.ReadFile(path)
		if err != nil {
			// A renamed or removed delivery file is itself worth a failure:
			// the guard would otherwise quietly stop covering it.
			t.Fatalf("delivery-owned file %s is unreadable (%v). If it moved, "+
				"update presenceDeliveryOwnedFiles — do not drop the entry.", name, err)
		}
		for _, forbidden := range presenceForbiddenInDelivery {
			if !strings.Contains(string(source), forbidden) {
				continue
			}
			t.Fatalf(
				"%s reads %q: a delivery decision has been made to depend on presence.\n"+
					"Presence may WAKE delivery — refreshPresenceSnapshot already calls "+
					"kickDeliveryRetriesForReachable when a contact becomes present — but the "+
					"decision to send must keep asking ROUTING (recipientHasPath). Presence is "+
					"allowed to be wrong; a wrong wake costs one pass, a wrong gate makes every "+
					"contact whose liveness cannot be proven permanently unreachable. "+
					"See docs/protocol/presence.md.",
				name, forbidden,
			)
		}
	}
}

// TestPresenceGuardWouldActuallyFire is the guard on the guard.
//
// A source-scanning test that never fires is decoration, and this codebase has
// been bitten by exactly that (an AST watchdog that passed because nothing it
// looked for could occur). So: feed the matcher a file that DOES contain a
// forbidden reference and assert it is caught.
func TestPresenceGuardWouldActuallyFire(t *testing.T) {
	sample := "func send() { _ = svc.PresenceSnapshot() }"

	caught := false
	for _, forbidden := range presenceForbiddenInDelivery {
		if strings.Contains(sample, forbidden) {
			caught = true
			break
		}
	}
	if !caught {
		t.Fatal("the delivery guard does not recognise a presence reference: " +
			"TestPresenceNeverGatesDelivery cannot fail and is therefore proving nothing")
	}
}
