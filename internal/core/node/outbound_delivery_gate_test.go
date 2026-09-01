package node

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestEveryInboundWriteGoesThroughTheDeliveryGate is the guard that ends a
// class of defect rather than one instance of it.
//
// Five review rounds found the same bug five times, each in a different
// send path: a frame carrying one of OUR messages reached a writer without
// passing clearedToWrite, so it could be handed to the recipient AFTER its
// author recalled it — and the deletion, reading a message nothing had
// emitted, scheduled no peer-side delete. The copy stayed with them.
//
// Session-queued frames are safe by construction now: enqueueSessionSendItem
// fills the delivery reference in, so a producer cannot omit it. The other
// door is sendFrameViaNetwork, the single Network() consumer, and that one
// still takes a raw frame. This test enumerates its callers and fails when a
// new one appears, so the next send path has to decide — deliberately, at
// review time — whether it can carry a message.
//
// To add a caller: if the frame can carry a user message, route it through
// writeDeliveryFrameToInbound instead. If it provably cannot (acks, pings,
// notices, announce-plane traffic, the handshake), add it below with the
// reason.
func TestEveryInboundWriteGoesThroughTheDeliveryGate(t *testing.T) {
	t.Parallel()

	// Callers that provably carry no user message. The value is why.
	permitted := map[string]string{
		"writeDeliveryFrameToInbound": "the gate itself",
		"sendAckDeleteByID":           "ack_delete — a control ack, no message body",
		"sendNoticeToPeer":            "push_notice — the broadcast notice plane",
		"inboundHeartbeat":            "ping",
		"handleRouteProbe":            "route probe answer — announce plane",
		"handleRouteQuery":            "route query answer — announce plane",
		"handleRouteSyncDigest":       "route sync digest — announce plane",
		"sendWelcomeFrame":            "handshake",
		// The inbound RPC dispatcher. Everything it writes is an ANSWER to
		// a frame it just read — pong, error, announce_peer_ack,
		// relay_hop_ack, the ack_delete reply — and answers carry no
		// message body. A user message leaving this node goes out through
		// the gossip, relay or session paths instead, never from here.
		"dispatchNetworkFrame":        "answers to inbound requests — acks, errors, hop-acks",
		"dispatchPeerSessionFrame":    "route_sync_summary — announce plane",
		"peerSessionRequest":          "pong on the session's own read loop",
		"dispatchInboundDatagramWire": "auth-required error",
		"emitPeerBannedNoticeByID":    "connection_notice — the ban notice, sent as the socket closes",
		"handleConn":                  "rate-limited error, before any command is read",
		"handleCommand":               "invalid-json error",
		"endInboundReadLoop":          "frame-too-large / read errors at the end of the loop",
		// These two DO carry user messages, and both take the gate — just
		// not by calling writeDeliveryFrameToInbound, because each has a
		// transport contract of its own that the shared helper would break.
		//
		// writeFrameToInboundConnErr is the SYNC flush the relay path needs
		// for fail-fast inbound delivery; sendRelayToAddress gates before
		// it and classifies its error after (that error, and not a bool, is
		// why the Err variant exists).
		//
		// writePushFrame is the subscriber push; pushToSubscriberSnapshot
		// gates the whole fan-out once via noteOwnEnvelopeEmitted, and the
		// frame's fate comes back as (sent, provenNotWritten).
		"writeFrameToInboundConnErr": "relay sync flush — gated and classified by sendRelayToAddress",
		"writePushFrame":             "subscriber push — gated by pushToSubscriberSnapshot",
		// The announce plane, end to end. sendAnnouncePlaneFrame is its
		// only remaining door into writeFrameToInbound, and every frame it
		// carries is routing state — announce_routes, route sync, probes.
		// A user message reaching an inbound connection goes through
		// writeDeliveryFrameToInbound or sendFrameToAddress instead.
		"sendAnnouncePlaneFrame":                    "announce plane only",
		"writeFrameToInbound":                       "ungated wrapper — its callers are what this test checks",
		"dispatchInboundAnnouncePlaneFrameWithCaps": "announce plane, capability-bound to one connID",
	}

	// Every helper that hands bytes to the Network surface. Watching only
	// the frame-shaped one left three doors open — the sync variant, the
	// raw-bytes pair and the handshake reply — and one of them was already
	// in use on a relay path.
	rawSenders := map[string]struct{}{
		"sendFrameViaNetwork":          {},
		"sendFrameViaNetworkSync":      {},
		"sendFrameBytesViaNetwork":     {},
		"sendFrameBytesViaNetworkSync": {},
		"sendHandshakeReplyViaNetwork": {},
		"sendSessionFrameViaNetwork":   {},
		// The UNGATED wrappers count as raw senders too. Watching only the
		// direct callers of the helpers above missed a whole call graph:
		// tryFailoverRelay → sendFrameToAddress → writeFrameToInbound
		// re-sent a relay frame with no gate at all, and every function in
		// that chain looked innocent because none of them touched a raw
		// helper by name.
		"writeFrameToInbound":     {},
		"writeFrameToInboundConn": {},
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	offenders := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, raw := rawSenders[sel.Sel.Name]; !raw {
					return true
				}
				if _, allowed := permitted[fn.Name.Name]; !allowed {
					offenders[fn.Name.Name] = fmt.Sprintf("%s:%d (%s)", name, fset.Position(call.Pos()).Line, sel.Sel.Name)
				}
				return true
			})
		}
	}
	if len(offenders) == 0 {
		return
	}
	names := make([]string, 0, len(offenders))
	for fn := range offenders {
		names = append(names, fn+" ("+offenders[fn]+")")
	}
	sort.Strings(names)
	t.Fatalf("these functions write to an inbound connection without the delivery gate:\n  %s\n\n"+
		"If the frame can carry a user message, send it through writeDeliveryFrameToInbound: a frame that\n"+
		"reaches a writer without clearedToWrite can be handed to the recipient after its author recalled\n"+
		"it, and the deletion will schedule no peer-side delete. If it provably cannot carry one, add it to\n"+
		"the permitted map in this test with the reason.", strings.Join(names, "\n  "))
}

// TestFrameEnvelopeReadsBothWireShapes: the identity of a message lives in
// two places on the wire — flat fields on the relay frames, a nested Item
// on push_message — and every place that open-coded the choice got a
// different subset right. One reader, one set of answers.
func TestFrameEnvelopeReadsBothWireShapes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame protocol.Frame
		want  protocol.Envelope
	}{{
		name: "flat relay frame",
		frame: protocol.Frame{
			Type: "relay_message", Topic: "dm",
			ID: "m-1", Address: "sender-a", Recipient: "peer-b",
		},
		want: protocol.Envelope{ID: "m-1", Topic: "dm", Sender: "sender-a", Recipient: "peer-b"},
	}, {
		// The shape gossipPushFrame actually builds. The recipient is the
		// field the old readers dropped, and the queued → sent event needs
		// it: an emission event with an empty recipient matches no
		// conversation, so the badge stayed on queued until the receipt.
		name: "nested push_message",
		frame: protocol.Frame{
			Type: "push_message", Topic: "dm",
			Item: &protocol.MessageFrame{ID: "m-2", Sender: "sender-a", Recipient: "peer-b"},
		},
		want: protocol.Envelope{ID: "m-2", Topic: "dm", Sender: "sender-a", Recipient: "peer-b"},
	}, {
		name:  "receipt carries no message",
		frame: protocol.Frame{Type: "relay_delivery_receipt", ID: "m-3", Recipient: "peer-b"},
		want:  protocol.Envelope{},
	}, {
		name:  "announce-plane frame carries no message",
		frame: protocol.Frame{Type: "announce_routes"},
		want:  protocol.Envelope{},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := frameEnvelope(tc.frame)
			if got.ID != tc.want.ID || got.Topic != tc.want.Topic ||
				got.Sender != tc.want.Sender || got.Recipient != tc.want.Recipient {
				t.Errorf("frameEnvelope() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestDeliveryRefIsFilledInAtAdmission is the property the refactor exists
// for: a producer that knows nothing about deliveries still cannot enqueue
// one of our messages ungated.
//
// The online-trigger drain was the fifth instance of this bug — it reached
// enqueuePeerFrame, which built a bare item, so a message the user had
// deleted could still be pushed when they came online.
func TestDeliveryRefIsFilledInAtAdmission(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const addr = domain.PeerAddress("admission-target:64660")
	sendCh := attachCapableRelayPeer(t, svc, string(addr), domain.PeerIdentityFromWire(peerID.Address))

	const target = protocol.MessageID("admission-filled-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: peerID.Address,
		CreatedAt: time.Now().UTC(),
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()

	// The bare producer: a frame and an address, nothing else. This is
	// what drainPendingForIdentities has.
	if !svc.enqueuePeerFrame(addr, gossipPushFrame(envelope)) {
		t.Fatal("the queue refused the frame")
	}

	item := awaitQueuedItem(t, sendCh, string(target))
	if !item.carriesDelivery() {
		t.Fatal("a bare producer put one of our messages on a session queue with no delivery to answer for; the serve loop will neither gate it nor confirm it")
	}
	if item.delivery.Envelope.Recipient != peerID.Address {
		t.Errorf("the filled-in delivery names recipient %q, want %q", item.delivery.Envelope.Recipient, peerID.Address)
	}
}

// TestConfirmationStampsTheRowAndRefusalWritesNothing is the whole of the
// two-bit model in one test.
//
// A refusal has nothing to record: the row already reads as not-on-wire
// (the badge's question) and as may-have-been-handed-to-a-writer (the
// deletion's question), because its claim came off before the attempt.
// Only a CONFIRMATION adds a fact, and it adds it in the one direction its
// bit moves. There is no queue, no correction and therefore no ordering
// for a refusal racing a confirmation to get wrong.
func TestConfirmationStampsTheRowAndRefusalWritesNothing(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const target = protocol.MessageID("two-bit-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		CreatedAt: time.Now().UTC(),
	}
	markOnDisk(t, outbox, target)
	first := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, first, false)
	svc.deliveryMu.Unlock()

	// Attempt one: the gate withdraws the claim, then the writer refuses.
	if !svc.clearedToWrite(deliveryDispatchRef{Envelope: envelope, DispatchedAt: first}, first) {
		t.Fatal("the gate withheld a message nothing has objected to")
	}
	svc.recordDeliveryRefusedByWriter(
		deliveryPeerSendItem(protocol.Frame{Type: "push_message"}, envelope, first),
		netcore.SendBufferFull)
	svc.WaitBackground()

	if outbox.marked(target) {
		t.Error("the refusal put the never-emitted claim back; the bit is supposed to be monotone")
	}
	if outbox.onWireStamped(target) {
		t.Fatal("a refused frame was stamped as on the wire")
	}

	// Attempt two succeeds — and a LATE refusal from attempt one, arriving
	// afterwards, still cannot unsay it.
	second := first.Add(30 * time.Second)
	svc.confirmEnvelopeOnWire(envelope, second)
	svc.recordDeliveryRefusedByWriter(
		deliveryPeerSendItem(protocol.Frame{Type: "push_message"}, envelope, first),
		netcore.SendBufferFull)
	// The stamp is written off the session's writer loop — the bit needs no
	// ordering, so parking a peer's loop on a contended SQLite statement
	// would buy nothing.
	svc.WaitBackground()

	if !outbox.onWireStamped(target) {
		t.Fatal("the confirmation did not stamp the row")
	}
	if outbox.marked(target) {
		t.Error("a stale refusal un-said a confirmation; that is the race the two-bit model removes")
	}
}

// TestRecalledMessageIsRefusedEvenWithNoRetryEntry closes the gate from
// the side the entry-shaped check left open.
//
// A recall REMOVES the retry entry and leaves a withdrawal shadow behind.
// So requiring an entry to decide "is this ours" meant a frame extracted
// from the pending ring a moment before the recall got the zero reference,
// passed the gate untouched, and handed the recalled message over — the
// very thing the gate exists to prevent, reached from the other side.
func TestRecalledMessageIsRefusedEvenWithNoRetryEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	const target = protocol.MessageID("recalled-no-entry")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address,
		CreatedAt: time.Now().UTC(),
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()

	// A drain has the frame in hand...
	frame := gossipPushFrame(envelope)

	// ...and the author recalls the message before it is written. The entry
	// is gone; only the withdrawal shadow remains.
	if _, err := svc.CancelOutgoingDelivery(target, recipient); err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}
	svc.deliveryMu.RLock()
	_, stillTracked := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if stillTracked {
		t.Fatal("the recall left the retry entry in place, so this test proves nothing")
	}

	now := time.Now().UTC()
	ref := svc.deliveryRefForFrame(frame, now)
	if ref.Envelope.ID == "" {
		t.Fatal("the frame was not recognised as one of ours, so the gate will wave it through")
	}
	if svc.clearedToWrite(ref, now) {
		t.Fatal("a recalled message was cleared to go out because its retry entry was already gone")
	}
}

// TestWithdrawalRemovesPendingGossipFrames: a recalled message must leave
// the pending ring with everything else.
//
// The ring's bookkeeping read only the FLAT frame id and only two frame
// types, so a push_message — whose id lives in Item — was never matched.
// The message stayed queued, the in-memory withdrawal shadow expired after
// five minutes, and the next flush handed the recalled message over.
func TestWithdrawalRemovesPendingGossipFrames(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	const target = protocol.MessageID("pending-gossip-recalled")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address,
		CreatedAt: time.Now().UTC(),
	}
	const addr = domain.PeerAddress("ring-holder:64662")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), true)
	svc.pending[addr] = []pendingFrame{{Frame: gossipPushFrame(envelope), QueuedAt: time.Now().UTC()}}
	counted := svc.countPendingFramesLocked(target)
	svc.deliveryMu.Unlock()

	if counted != 1 {
		t.Fatalf("countPendingFramesLocked saw %d frames for the message, want 1", counted)
	}
	if _, err := svc.CancelOutgoingDelivery(target, recipient); err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}

	svc.deliveryMu.RLock()
	left := len(svc.pending[addr])
	svc.deliveryMu.RUnlock()
	if left != 0 {
		t.Errorf("%d frames for the recalled message are still in the ring; the flush will hand it over once the withdrawal shadow expires", left)
	}
}

// TestPeerReturningWakesAnUnconfirmedDelivery: a dispatch nobody took is
// parked on the poll interval, and the kick no longer moves it — a route
// merely being re-confirmed must not overrule the pacing. But the peer
// actually leaving and coming back is not a route reconfirmation, and it is
// the one event that makes the earlier "they were reachable" answer stale.
func TestPeerReturningWakesAnUnconfirmedDelivery(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["dispatched-nobody-took-it"] = &deliveryRetryEntry{
		Envelope: protocol.Envelope{
			ID: "dispatched-nobody-took-it", Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
		},
		Attempts:      4,
		NextAttemptAt: now.Add(deliveryHoldPollInterval),
		Hold:          holdUnconfirmed,
	}
	svc.deliveryMu.Unlock()

	svc.noteRecipientWentOffline(recipient)

	svc.deliveryMu.RLock()
	reopened := svc.awaitingDelivered["dispatched-nobody-took-it"].Hold
	svc.deliveryMu.RUnlock()
	if reopened != holdUnreachable {
		t.Fatalf("hold = %d after the recipient left, want holdUnreachable so their return can wake it", reopened)
	}
}

// TestDeliveryRefIsNotFilledInForOtherPeoplesMessages keeps the admission
// point off transit traffic: confirming a message this node does not own
// would charge and announce a delivery that is not ours to account for.
func TestDeliveryRefIsNotFilledInForOtherPeoplesMessages(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const addr = domain.PeerAddress("admission-transit:64661")
	sendCh := attachCapableRelayPeer(t, svc, string(addr), domain.PeerIdentityFromWire(peerID.Address))

	const target = protocol.MessageID("admission-transit-1")
	if !svc.enqueuePeerFrame(addr, gossipPushFrame(protocol.Envelope{
		ID: target, Topic: "dm", Sender: senderID.Address, Recipient: peerID.Address,
		CreatedAt: time.Now().UTC(),
	})) {
		t.Fatal("the queue refused the frame")
	}

	if awaitQueuedItem(t, sendCh, string(target)).carriesDelivery() {
		t.Fatal("the admission point claimed a delivery for a message this node does not own")
	}
}
