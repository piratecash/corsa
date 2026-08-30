package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestClearingAChatTravelsOverTheWire is the wipe as the two users experience
// it: two identities, two databases, two nodes on a socket, and no seam
// anywhere in between.
//
// The other A=2/B=3 test builds the request with MarshalConversationDeletePayload
// and hands it to the inbound handler, which proves the two halves agree about
// JSON and nothing more. This one starts at Alice's SendConversationDelete and
// ends with Bob's thread empty and Alice's request settled, having gone through
// box encryption for Bob's key, the send_control_message frame, Alice's node,
// the TCP session between the nodes, Bob's node, the local-change event it
// emits, decryption with Bob's identity, sender verification against his
// contacts, and command dispatch — and then all of that again, backwards, for
// the acknowledgement.
//
// The disagreement is the point of the fixture: Bob holds a message Alice never
// received, stamped LATER than anything she has. It is the message still in a
// relay's buffer when she clears the chat, and it is what a boundary taken only
// from Alice's own rows would leave standing on his side.
func TestClearingAChatTravelsOverTheWire(t *testing.T) {
	ctx := context.Background()

	aliceAddress := freeLoopbackAddress(t)
	bobAddress := freeLoopbackAddress(t)
	aliceNode := startClientNode(t, aliceAddress, bobAddress)
	bobNode := startClientNode(t, bobAddress, aliceAddress)
	trustEachOther(t, aliceNode, bobNode)

	aliceAddr := domain.PeerIdentityFromWire(aliceNode.id.Address)
	bobAddr := domain.PeerIdentityFromWire(bobNode.id.Address)

	alice := newRouterOver(t, aliceNode.DesktopClient)
	bob := newRouterOver(t, bobNode.DesktopClient)

	const (
		mineBothHave   = "e1000000-2222-4444-8888-cccccccccccc"
		theirsBothHave = "e2000000-2222-4444-8888-cccccccccccc"
		theirsOnlyThey = "e3000000-2222-4444-8888-cccccccccccc"
	)
	written := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	writtenLater := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)

	// Here: TWO messages.
	insertChatlogEntry(t, aliceNode.chatlog, bobAddr, chatlog.Entry{
		ID: mineBothHave, Sender: aliceAddr.String(), Recipient: bobAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})
	insertChatlogEntry(t, aliceNode.chatlog, bobAddr, chatlog.Entry{
		ID: theirsBothHave, Sender: bobAddr.String(), Recipient: aliceAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})
	// There: THREE, the third newer than anything here, and all under the
	// author-only policy an older build stamped on every message.
	for _, row := range []struct{ id, sender, recipient, createdAt string }{
		{mineBothHave, aliceAddr.String(), bobAddr.String(), written},
		{theirsBothHave, bobAddr.String(), aliceAddr.String(), written},
		{theirsOnlyThey, bobAddr.String(), aliceAddr.String(), writtenLater},
	} {
		insertChatlogEntry(t, bobNode.chatlog, aliceAddr, chatlog.Entry{
			ID: row.id, Sender: row.sender, Recipient: row.recipient,
			Body: "ciphertext", CreatedAt: row.createdAt, Flag: string(protocol.MessageFlagSenderDelete),
		})
	}

	waitForSession(t, aliceNode, bobAddr)

	if err := alice.SendConversationDelete(ctx, bobAddr); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	// Alice's side is empty the moment she clicks.
	here, err := aliceNode.chatlog.Store().Read(ctx, "dm", bobAddr)
	if err != nil {
		t.Fatalf("read the cleared thread: %v", err)
	}
	if len(here) != 0 {
		t.Fatalf("%d rows survived the wipe here: %+v", len(here), here)
	}

	// The sweep dispatches the request for real. Bob's node hands it up as a
	// control DM; his router decrypts and applies it.
	alice.processDeleteRetryDue(ctx, time.Now().UTC())
	bob.onControlMessage(awaitControlDM(t, bobNode))

	there, err := bobNode.chatlog.Store().Read(ctx, "dm", aliceAddr)
	if err != nil {
		t.Fatalf("read the peer's thread: %v", err)
	}
	if len(there) != 0 {
		t.Fatalf("%d rows survived on the peer's side: %+v — including a message the requester never had", len(there), there)
	}

	// And the answer comes back the same way and settles the request.
	alice.onControlMessage(awaitControlDM(t, aliceNode))
	if _, found, err := aliceNode.chatlog.Store().ConversationDeleteIntentForPeer(ctx, bobAddr); err != nil || found {
		t.Fatalf("the request outlived the peer's answer: found=%v err=%v", found, err)
	}
}

// TestClearingAChatSurvivesAPeerWhoseClockRunsAhead keeps the case that broke
// three successive boundary designs, now that there is no boundary to break:
// the second message exists ONLY on Bob's side and is stamped ten minutes into
// Alice's future because Bob's clock runs ahead.
//
// Neither half is exotic. A message only Bob has is the ordinary outcome of a
// copy still sitting in a relay's buffer; a phone whose clock is minutes off is
// the ordinary state of a phone. Under every version of the command that
// carried a moment, that combination left the message standing after a wipe
// both users believed had finished. The request carries no moment now, so what
// this pins is that no future arithmetic gets reintroduced: whatever the
// stamps say, the conversation goes.
func TestClearingAChatSurvivesAPeerWhoseClockRunsAhead(t *testing.T) {
	ctx := context.Background()

	aliceAddress := freeLoopbackAddress(t)
	bobAddress := freeLoopbackAddress(t)
	aliceNode := startClientNode(t, aliceAddress, bobAddress)
	bobNode := startClientNode(t, bobAddress, aliceAddress)
	trustEachOther(t, aliceNode, bobNode)

	aliceAddr := domain.PeerIdentityFromWire(aliceNode.id.Address)
	bobAddr := domain.PeerIdentityFromWire(bobNode.id.Address)
	alice := newRouterOver(t, aliceNode.DesktopClient)
	bob := newRouterOver(t, bobNode.DesktopClient)

	now := time.Now().UTC()
	// Bob's clock: ten minutes ahead of Alice's.
	const bobSkew = 10 * time.Minute

	insertChatlogEntry(t, aliceNode.chatlog, bobAddr, chatlog.Entry{
		ID: "f1000000-2222-4444-8888-cccccccccccc", Sender: aliceAddr.String(), Recipient: bobAddr.String(),
		Body: "ciphertext", CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	for _, row := range []struct {
		id, sender, recipient string
		createdAt             time.Time
	}{
		// The pair both sides have, stamped by Alice.
		{"f1000000-2222-4444-8888-cccccccccccc", aliceAddr.String(), bobAddr.String(), now.Add(-time.Minute)},
		// The one only Bob has, stamped by BOB — ten minutes into Alice's
		// future, and later than anything she holds or her clock will read
		// when she clicks.
		{"f2000000-2222-4444-8888-cccccccccccc", bobAddr.String(), aliceAddr.String(), now.Add(bobSkew)},
	} {
		insertChatlogEntry(t, bobNode.chatlog, aliceAddr, chatlog.Entry{
			ID: row.id, Sender: row.sender, Recipient: row.recipient,
			Body: "ciphertext", CreatedAt: row.createdAt.Format(time.RFC3339Nano),
			Flag: string(protocol.MessageFlagSenderDelete),
		})
	}

	waitForSession(t, aliceNode, bobAddr)
	if err := alice.SendConversationDelete(ctx, bobAddr); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	alice.processDeleteRetryDue(ctx, time.Now().UTC())

	// Bob applies it with HIS clock, which is what the arriving envelope is
	// measured against.
	bob.presenceClock = func() time.Time { return time.Now().UTC().Add(bobSkew) }
	bob.onControlMessage(awaitControlDM(t, bobNode))

	there, err := bobNode.chatlog.Store().Read(ctx, "dm", aliceAddr)
	if err != nil {
		t.Fatalf("read the peer's thread: %v", err)
	}
	if len(there) != 0 {
		t.Fatalf("%d rows survived a peer's clock running ahead: %+v", len(there), there)
	}
}

// newRouterOver builds the router the way the composition root does, minus the
// loops: no dispatch seams, so every control DM this test moves is one the
// production path encrypted and sent.
func newRouterOver(t *testing.T, c *DesktopClient) *DMRouter {
	t.Helper()
	r := &DMRouter{
		client:          c,
		seenMessageIDs:  make(map[string]messageGate),
		peers:           make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:         make(map[domain.PeerIdentity]uint64),
		cache:           NewConversationCache(),
		convDeleteRetry: newConversationDeleteRetryState(),
		uiEvents:        make(chan UIEvent, 32),
		startupDone:     make(chan struct{}),
		withdrawals:     newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	r.removals = c.removals
	// The router's own view of who is online is built by its event loops,
	// which this fixture does not run. The NODES are really connected — the
	// test waits for the session — so the honest answer here is yes, and
	// leaving it to the empty peer map would park the request instead of
	// dispatching it.
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	return r
}

// TestClearingOneChatLeavesTheOtherAlone is the blast-radius test, and it
// exists because of what the operation is rather than because of a suspicion
// about the code.
//
// "Clear the chat" is the most destructive thing a user can ask this
// application to do, and it is answered by a peer over the network: the request
// carries no message ids, so what it reaches is decided entirely by the
// receiver's own query. Bob holds two conversations. Alice asks for hers to be
// cleared. Carol's — her messages, the reactions on them, and the attachment
// hanging off one of them — must come out untouched, and that has to be
// pinned by a test rather than by reading the WHERE clause.
func TestClearingOneChatLeavesTheOtherAlone(t *testing.T) {
	ctx := context.Background()

	aliceAddress := freeLoopbackAddress(t)
	bobAddress := freeLoopbackAddress(t)
	aliceNode := startClientNode(t, aliceAddress, bobAddress)
	bobNode := startClientNode(t, bobAddress, aliceAddress)
	trustEachOther(t, aliceNode, bobNode)

	aliceAddr := domain.PeerIdentityFromWire(aliceNode.id.Address)
	bobAddr := domain.PeerIdentityFromWire(bobNode.id.Address)
	// Carol never connects: her conversation is on Bob's disk and nothing
	// about this wipe involves her, which is the whole point.
	carol := domain.PeerIdentityFromWire("cccccccccccccccccccccccccccccccccccccccc")

	alice := newRouterOver(t, aliceNode.DesktopClient)
	bob := newRouterOver(t, bobNode.DesktopClient)

	written := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	const (
		aliceMessage = "a1000000-2222-4444-8888-cccccccccccc"
		carolMessage = "c1000000-2222-4444-8888-cccccccccccc"
		carolFile    = "c2000000-2222-4444-8888-cccccccccccc"
	)

	insertChatlogEntry(t, aliceNode.chatlog, bobAddr, chatlog.Entry{
		ID: aliceMessage, Sender: aliceAddr.String(), Recipient: bobAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})
	insertChatlogEntry(t, bobNode.chatlog, aliceAddr, chatlog.Entry{
		ID: aliceMessage, Sender: aliceAddr.String(), Recipient: bobAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})
	// Carol's thread on Bob's side: a message, an attachment announcement, and
	// a reaction Bob made on her message.
	for _, id := range []string{carolMessage, carolFile} {
		insertChatlogEntry(t, bobNode.chatlog, carol, chatlog.Entry{
			ID: id, Sender: carol.String(), Recipient: bobAddr.String(),
			Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
		})
	}
	store := bobNode.chatlog.Store()
	// A REAL file-transfer mapping for Carol's attachment, registered through
	// the node the way an incoming announce registers one. A second chatlog row
	// would prove nothing: what a wipe reaches in the file subsystem is decided
	// by CleanupTransferByMessageID, which knows nothing about conversations
	// and takes an id.
	if err := bobNode.localNode.RegisterIncomingFileTransfer(
		domain.FileID(carolFile), strings.Repeat("ab", 32), "holiday.png", "image/png", 1024, carol,
	); err != nil {
		t.Fatalf("register Carol's attachment: %v", err)
	}
	// Bob's router must be able to reach the file subsystem, or the test would
	// pass because nothing was wired rather than because nothing was touched.
	bob.fileBridge = NewFileTransferBridge(bobNode.DesktopClient)

	if _, err := store.ApplyReactionFact(ctx, domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(carol),
		Key:   domain.ReactionKey{MessageID: carolMessage, Actor: bobAddr, Emoji: "👍"},
		Op:    domain.ReactionSet,
		Clock: 1,
	}, time.Now().UTC()); err != nil {
		t.Fatalf("react in Carol's conversation: %v", err)
	}

	waitForSession(t, aliceNode, bobAddr)
	if err := alice.SendConversationDelete(ctx, bobAddr); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	alice.processDeleteRetryDue(ctx, time.Now().UTC())
	bob.onControlMessage(awaitControlDM(t, bobNode))

	// Alice's thread is gone from Bob's side.
	if left, err := store.Read(ctx, "dm", aliceAddr); err != nil || len(left) != 0 {
		t.Fatalf("Alice's thread on Bob's side: %d rows (err=%v)", len(left), err)
	}

	// Carol's is exactly as it was.
	carolThread, err := store.Read(ctx, "dm", carol)
	if err != nil {
		t.Fatalf("read Carol's thread: %v", err)
	}
	if len(carolThread) != 2 {
		t.Fatalf("Carol's conversation has %d rows after somebody else's wipe, want 2: %+v", len(carolThread), carolThread)
	}
	facts, err := store.ReactionFacts(ctx, carolMessage)
	if err != nil {
		t.Fatalf("read the reactions of Carol's message: %v", err)
	}
	if len(facts) != 1 {
		t.Errorf("the reaction on Carol's message went with somebody else's wipe: %+v", facts)
	}
	// Carol's attachment is still registered. The wipe of another conversation
	// reached the file subsystem for its own ids — that is what the bridge is
	// for — and this is the assertion that it stopped there.
	if _, _, _, found := bobNode.localNode.FileTransferProgress(domain.FileID(carolFile), false); !found {
		t.Error("Carol's attachment was released by somebody else's wipe")
	}

	// And the request Alice's wipe wrote names Bob, not Carol.
	if _, found, err := store.ConversationDeleteIntentForPeer(ctx, carol); err != nil || found {
		t.Errorf("a wipe request was written for Carol: found=%v err=%v", found, err)
	}
}
