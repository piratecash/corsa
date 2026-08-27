package service

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestLogIDDoesNotCarryTheIdentifier is the whole contract: a delete log line
// must be tie-able to the other lines of the same operation and to nothing
// else. If the value itself, or any prefix of it, survived into the file, the
// log would be the record of deletions that the database is not.
func TestLogIDDoesNotCarryTheIdentifier(t *testing.T) {
	t.Parallel()

	const id = "6c1f0a6e-6f1f-4a9d-9b7e-1c2d3e4f5a6b"

	digest := logID(id)
	if digest == "" {
		t.Fatal("a non-empty identifier rendered as nothing")
	}
	if strings.Contains(id, digest) || strings.Contains(digest, id[:8]) {
		t.Fatalf("the log value %q still carries part of the identifier", digest)
	}
	if len(digest) != logIDLength {
		t.Errorf("length = %d, want %d", len(digest), logIDLength)
	}

	// Stable inside one run: two lines about the same message can be tied
	// together, which is what an investigation actually needs.
	if logID(id) != digest {
		t.Error("the same identifier rendered differently twice in one process")
	}
	// And different identifiers do not collide into one story.
	if logID("7d2f1b7f-7f2f-4b8e-8c6d-2d3e4f5a6b7c") == digest {
		t.Error("two identifiers rendered the same")
	}
	// An absent identifier reads as absent, not as a digest of nothing.
	if logID("") != "" {
		t.Error("an empty identifier rendered as a digest")
	}
}

// TestADeletionWritesNoIdentifierToTheLog is the contract as the file sees it,
// not as the helper does: a whole deletion is driven with the logger captured,
// and neither the message id nor the peer may appear anywhere in what came out.
//
// The helper test above proves the digest is opaque; this one proves the paths
// actually use it — including the error strings, which reach the log through
// .Err(err) and used to carry the id inside their text.
func TestADeletionWritesNoIdentifierToTheLog(t *testing.T) {
	ctx := context.Background()

	var captured bytes.Buffer
	restore := log.Logger
	// TRACE, not Info. The node's lock-wait diagnostics sit at that level and
	// they name the peer of every call they bracket — including the ones only
	// a deletion makes — so a test that watched Info would miss exactly the
	// lines nobody thinks about.
	log.Logger = zerolog.New(&captured).Level(zerolog.TraceLevel)
	t.Cleanup(func() { log.Logger = restore })

	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const target = "d1000000-2222-4444-8888-cccccccccccc"
	seedConversation(t, c, myAddr, peer, target)

	// From here on: only what the DELETION writes. Building the fixture starts
	// a node, and its own startup lines are not what this is about.
	captured.Reset()

	// The real cancellation path, not the seam: this is where the node's own
	// lines are written, and they used to name the message and the recipient.
	if _, err := r.client.CancelMessageDelivery(ctx, peer, domain.MessageID(target)); err != nil {
		// A test node has nothing queued for that peer, so the cancellation
		// reports nothing cancelled — the lines are written either way, which
		// is what this test reads.
		t.Logf("CancelMessageDelivery: %v", err)
	}
	if _, err := r.SendMessageDelete(ctx, peer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	// The refusal set re-reads the deletions still owed on the reaper's tick.
	// There is one outstanding by now — the delete just sent — so the line it
	// writes is a line written about a deletion, on a timer, whether or not
	// anyone asked for diagnostics.
	r.wipeTombstones.Hydrate(ctx, time.Now().UTC())

	// The answer, the sweep and the refusal of a replayed copy all log too.
	r.processDeleteRetryDue(ctx, time.Now().UTC())
	ack, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: domain.MessageID(target),
		Status:   domain.MessageDeleteStatusDeleted,
	})
	if err != nil {
		t.Fatalf("marshal the ack: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, ack)

	// A copy of the deleted message coming back and being turned away. It is
	// the one deletion path that runs on an ARRIVAL rather than on a click, so
	// nothing else in this test reaches it.
	r.wipeTombstones.Note([]domain.MessageID{domain.MessageID(target)}, time.Now().UTC())
	if !r.suppressIfWipeTombstoned(protocol.LocalChangeEvent{MessageID: target, Topic: "dm"}) {
		t.Fatal("the re-delivered copy was not refused; the line under test never ran")
	}

	// The freeze is part of a wipe, so drive one: the node used to write the
	// recipient's identity in the clear when it took the freeze, on a line that
	// exists only because a conversation is being erased.
	if _, err := r.client.FreezeConversationDelivery(ctx, peer, []domain.MessageID{domain.MessageID(target)}); err != nil {
		t.Logf("FreezeConversationDelivery: %v", err)
	}

	written := captured.String()
	for _, secret := range []struct{ what, value string }{
		{"the message id", target},
		{"the peer identity", peer.String()},
		{"our own identity", myAddr.String()},
	} {
		if strings.Contains(written, secret.value) {
			t.Errorf("%s appears in the log of a deletion", secret.what)
		}
	}

	// And no line says a deletion happened at all.
	//
	// Opaque identifiers were not enough, which is the point of this half.
	// "message_delete completed, 3 removed, 14:07:22" states that this user
	// destroyed something, how much, and when — in a plain-text file that no
	// checkpoint, no secure_delete and no migration ever touches. The digests
	// made those lines anonymous; they did not make them absent.
	for _, line := range []string{
		"message deleted locally",
		"local copy of the message removed",
		"applied inbound message_delete",
		"message_delete completed",
		// The node's own half of a wipe. It lives in another package, which is
		// exactly why it was missed the first time.
		"delivery_freeze_taken",
		"delivery_freeze_released",
		"cancel_outgoing_delivery",
		"cancel_outgoing_deliveries_to_recipient",
		// The refusal set re-reads the deletions still owed on a timer. That
		// line used to be an ordinary Info with a count of the ones that had
		// SETTLED since the last read — the number of deletions that finished,
		// and the five-minute window they finished in, written down on a
		// schedule whether or not anything was happening.
		"the messages of the deletions still owed are refused again",
		// The refusal set overflowing. The line is about capacity, but the
		// number in it is a count of deletions this user performed, and its
		// timestamp is when their protection ended.
		"the refusal set is full",
		// Turning away a re-delivered copy. Saying it was suppressed is saying
		// the message was deleted here, and when.
		"suppressed re-delivery of wiped message",
	} {
		if strings.Contains(written, line) {
			t.Errorf("the log states that a deletion happened: %q", line)
		}
	}
}

// TestDeletionDiagnosticsCanBeTurnedBackOn is the other half of the contract:
// silence is the DEFAULT, not the only setting. A support case that needs to
// know what the deletion paths did has a way to ask, and the answer still
// carries digests rather than identifiers.
func TestDeletionDiagnosticsCanBeTurnedBackOn(t *testing.T) {
	ctx := context.Background()

	t.Setenv(deletionDiagnosticsEnv, "1")
	// The variable is read once per process, so the test drives the gate
	// itself rather than depending on which test ran first.
	restoreGate := deletionLog
	// Reads the global at CALL time: the buffer is installed below.
	deletionLog = func() *zerolog.Logger { return &log.Logger }
	t.Cleanup(func() { deletionLog = restoreGate })

	var captured bytes.Buffer
	restore := log.Logger
	log.Logger = zerolog.New(&captured).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = restore })

	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = "d2000000-2222-4444-8888-cccccccccccc"
	seedConversation(t, c, myAddr, peer, target)
	captured.Reset()

	if _, err := r.SendMessageDelete(ctx, peer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	written := captured.String()
	if !strings.Contains(written, "message deleted locally") {
		t.Fatalf("the diagnostics flag wrote nothing about the deletion:\n%s", written)
	}
	if strings.Contains(written, target) || strings.Contains(written, peer.String()) {
		t.Error("the diagnostics wrote an identifier, not a digest")
	}
}
