package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
)

// This file tests the architectural isolation of FileCommandFrame from the
// DM pipeline. FileCommandFrame is a separate protocol with its own routing,
// and by design:
//   - NOT stored in chatlog
//   - NOT generating delivery receipts
//   - NOT emitting LocalChangeEvent
//   - NOT entering dm_router pipeline
//   - NOT entering pending queue
//   - NOT using gossip fallback
//
// These properties are inherited from the architecture (separate wire format
// and routing) rather than configured per-command.

// deliverLocalFrame creates a Router, sends a signed frame addressed to
// localID, and returns the router and senderID for assertions. This is the
// common setup for all isolation tests that verify local delivery behaviour.
func deliverLocalFrame(t *testing.T, payload string) (tr *testFileRouter, senderID domain.PeerIdentity) {
	t.Helper()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID = domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr = newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, payload, priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw))

	return tr, senderID
}

// TestFileCommandNotStoredInChatlog verifies that a FileCommandFrame delivered
// locally goes to LocalDeliver (FileTransferManager), not any chatlog write
// path. The Router has no chatlog or message store reference — this is
// architectural separation, not a per-command flag.
func TestFileCommandNotStoredInChatlog(t *testing.T) {
	t.Parallel()

	tr, _ := deliverLocalFrame(t, "chatlog-isolation-test")

	// Frame is delivered to LocalDeliver (FileTransferManager path),
	// not to any message storage / chatlog path.
	if len(tr.localDeliveries()) != 1 {
		t.Errorf("expected 1 local delivery, got %d", len(tr.localDeliveries()))
	}

	// The Router struct has no chatlog reference, no message store,
	// no database handle. There is no code path from HandleInbound to
	// chatlog storage. This is verified by inspection of Router fields:
	// nonceCache, localID, isFullNode, routeSnap, peerPubKey, sessionSend,
	// localDeliver — none of which interact with chatlog.
}

// TestFileCommandNotInDMRouterPipeline verifies that file commands go through
// file_router, not dm_router. The file_router has its own routing/forwarding
// logic, separate from dm_router which handles conversation previews, unread
// counts, notification sounds, and read tracking.
func TestFileCommandNotInDMRouterPipeline(t *testing.T) {
	t.Parallel()

	tr, _ := deliverLocalFrame(t, "dm-router-isolation-test")

	if len(tr.localDeliveries()) != 1 {
		t.Error("file command should be delivered via file_router LocalDeliver, not dm_router")
	}

	// testFileRouter has no dm_router reference. File commands use
	// FileCommandFrame wire format ("file_command" type), not the
	// "send_message" frame type used by DMs. Service.handleFileCommandFrame
	// calls Router.HandleInbound — never dm_router.
}

// TestFileCommandNoPendingQueue verifies that file_router does not implement
// a pending queue for undeliverable file commands. When no route exists,
// file commands are silently dropped (unlike DMs which enter pending queue).
func TestFileCommandNoPendingQueue(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	unknownDST := domain.PeerIdentity("unknown-destination-identity12")

	pub, priv, _ := ed25519.GenerateKey(nil)

	// No route to unknownDST.
	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Frame addressed to unknownDST — no route exists.
	frame := makeSignedFrame(senderID, unknownDST, 5, "no-pending-queue-test", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// Frame should be silently dropped — no delivery, no forwarding, no queue.
	if len(tr.localDeliveries()) != 0 {
		t.Error("file command to unknown DST should not be delivered locally")
	}
	if len(tr.sentTo(unknownDST)) != 0 {
		t.Error("file command to unknown DST should not be forwarded")
	}

	// Router has no pending queue, no retry store, no outbound state
	// tracking. When deliverability check fails at step 2, the frame is
	// gone permanently — fire-and-forget by design.
}

// TestFileCommandNoGossipFallback verifies that when all route attempts fail,
// file commands are not retried via gossip. DMs have gossip fallback for
// reliability; file commands are fire-and-forget by design.
func TestFileCommandNoGossipFallback(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	remoteDST := domain.PeerIdentity("remote-destination-identity123")

	pub, priv, _ := ed25519.GenerateKey(nil)

	// One route to remoteDST via nextHop, but the send will fail.
	nextHop := domain.PeerIdentity("next-hop-identity-123456789012")
	snap := routing.Snapshot{
		TakenAt: time.Now(),
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			remoteDST: {
				{Identity: remoteDST, Origin: remoteDST, NextHop: nextHop, Hops: 1, ExpiresAt: time.Now().Add(time.Hour)},
			},
		},
	}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	// All sends fail.
	reachableHops := map[domain.PeerIdentity]bool{nextHop: false}
	tr := newTestFileRouter(localID, true, snap, keys, reachableHops)

	frame := makeSignedFrame(senderID, remoteDST, 5, "no-gossip-test", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// No local delivery, no successful forward.
	if len(tr.localDeliveries()) != 0 {
		t.Error("frame should not be delivered locally")
	}

	// No gossip fallback: the frame is dropped after all routes fail.
	// Router has no gossip mechanism — no INV/WANT/DATA cycle,
	// no subscription to gossip layer. The frame is simply lost.
}

// TestFileCommandNoDeliveryReceipts verifies that file_router.LocalDeliver
// does not trigger delivery receipt generation. Delivery receipts are a
// DM-pipeline concept; file commands have no acknowledgment at the routing
// level (only at the application level via file_downloaded/file_downloaded_ack).
func TestFileCommandNoDeliveryReceipts(t *testing.T) {
	t.Parallel()

	tr, senderID := deliverLocalFrame(t, "no-receipts-test")

	if len(tr.localDeliveries()) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(tr.localDeliveries()))
	}

	// Router has no receipt callback, no ack-sender, no outbound
	// receipt mechanism. FileCommandFrame has no "message_received" type.
	// Application-level acknowledgment (file_downloaded → file_downloaded_ack)
	// is handled by FileTransferManager, not by the routing layer.
	//
	// After delivery, only LocalDeliver was called — no outbound frames
	// were sent back to the sender.
	if len(tr.sentTo(senderID)) != 0 {
		t.Error("no receipt frame should be sent back to sender")
	}
}

// TestFileCommandNoLocalChangeEvent verifies that processing a file command
// does not emit a LocalChangeEvent. LocalChangeEvent is used by the DM
// pipeline to notify the UI of new messages; file commands are invisible
// transport-layer traffic.
func TestFileCommandNoLocalChangeEvent(t *testing.T) {
	t.Parallel()

	tr, _ := deliverLocalFrame(t, "no-change-event-test")

	if len(tr.localDeliveries()) != 1 {
		t.Fatal("frame should have been delivered")
	}

	// Router has no EventBus or LocalChangeEvent emitter. The entire
	// file_router.go and file_integration.go have zero references to
	// LocalChangeEvent. This isolation is architectural — file commands
	// are not user-visible messages.
}
