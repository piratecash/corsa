package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// seenRecorder stands in for the receipt RPC and remembers every batch it was
// handed, in order.
type seenRecorder struct {
	mu      sync.Mutex
	batches [][]domain.MessageID
	fail    bool
}

func (s *seenRecorder) record(_ context.Context, _ domain.PeerIdentity, batch []DirectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, messageIDsOf(batch))
	if s.fail {
		return errors.New("receipt rpc unavailable")
	}
	return nil
}

// forget drops what was recorded so far.
func (s *seenRecorder) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = nil
}

func (s *seenRecorder) sent() [][]domain.MessageID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]domain.MessageID(nil), s.batches...)
}

// newReaderTestRouter is a conversation with peer as a completed open leaves
// it: loaded, on screen, scrolled to its end, nothing unread.
func newReaderTestRouter(t *testing.T, peer domain.PeerIdentity, messages []DirectMessage) (*DMRouter, *seenRecorder) {
	t.Helper()
	r := newTestRouter()
	recorder := &seenRecorder{}
	r.markConversationSeenFn = recorder.record
	r.cache.Load(peer, messages, 0)
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.reader = openedReader()
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()
	// An open asks to be shown from its end; that request is spent already.
	r.ConsumePendingActions()
	return r, recorder
}

// openedReader is the reader of a conversation whose open has completed: the
// load that brought it on screen has run, so nothing is waiting for it.
func openedReader() openReader {
	reader := openReaderFor(nil)
	reader.awaitingOpenLoad = false
	return reader
}

func incomingFrom(peer domain.PeerIdentity, id string, at time.Time) DirectMessage {
	return DirectMessage{ID: id, Sender: peer, Recipient: domaintest.ID("me"), Body: id, Timestamp: at}
}

// deliverIncoming hands the router a decrypted message for the open
// conversation, the way the live event path does.
func deliverIncoming(t *testing.T, r *DMRouter, peer domain.PeerIdentity, id string, at time.Time) {
	t.Helper()
	msg := incomingFrom(peer, id, at)
	if !r.deliverDecryptedMessage(&msg, peer, r.peerStampOf(peer)) {
		t.Fatalf("message %s was not delivered to the open conversation", id)
	}
}

// settle waits for every background receipt the router started.
func settle(t *testing.T, r *DMRouter) {
	t.Helper()
	if !r.ShutdownDrain(2 * time.Second) {
		t.Fatal("background work did not finish")
	}
}

func unreadOf(r *DMRouter, peer domain.PeerIdentity) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers[peer].Unread
}

func markerOf(r *DMRouter) (domain.MessageID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reader.marker.FirstUnread()
}

func idsEqual(got []domain.MessageID, want ...domain.MessageID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestArrivalAtTheEndIsReadAsItLands: with the end of the conversation on
// screen, a new message is on screen the moment it lands — no badge, its
// receipt sent, and no request to scroll: the list is already pinned there.
func TestArrivalAtTheEndIsReadAsItLands(t *testing.T) {
	peer := domaintest.ID("reader-at-end")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})

	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	if r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("an arrival asked to scroll — a reader at the end stays there without it, and one elsewhere must not be moved")
	}
	settle(t, r)

	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — the reader had the message on screen", got)
	}
	if sent := recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m2") {
		t.Fatalf("receipts = %v, want exactly [m2]", sent)
	}
	if _, placed := markerOf(r); placed {
		t.Fatal("a message read as it landed placed the unread divider")
	}
}

// TestArrivalBelowTheReaderStaysUnread is the bug: the reader has scrolled
// up, a message arrives, and it was marked read and the reader was pulled
// down to it. It must be badged, marked by the divider, and left alone.
func TestArrivalBelowTheReaderStaysUnread(t *testing.T) {
	peer := domaintest.ID("reader-scrolled-up")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{
		incomingFrom(peer, "m1", start),
		incomingFrom(peer, "m2", start.Add(time.Minute)),
	})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})

	deliverIncoming(t, r, peer, "m3", start.Add(2*time.Minute))
	deliverIncoming(t, r, peer, "m4", start.Add(3*time.Minute))
	if r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("an arrival scrolled a reader who was reading further up")
	}
	settle(t, r)

	if got := unreadOf(r, peer); got != 2 {
		t.Fatalf("unread = %d, want 2 — neither arrival was on screen", got)
	}
	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none — nothing new was seen", sent)
	}
	if first, placed := markerOf(r); !placed || first != "m3" {
		t.Fatalf("divider = %q (placed %v), want above m3, the first of the run", first, placed)
	}
	// The snapshot is composed by notify, and the live path (onNewMessage)
	// notifies after the delivery this test drives directly.
	r.notify(UIEventMessagesUpdated)
	if first, placed := r.Snapshot().UnreadMarker.FirstUnread(); !placed || first != "m3" {
		t.Fatalf("snapshot divider = %q (placed %v), want above m3", first, placed)
	}
}

// TestScrollingDownReadsWhatCameIntoView: messages count as read as the
// reader reaches them, and only those — and the divider stays where the run
// began while the reader works through it.
func TestScrollingDownReadsWhatCameIntoView(t *testing.T) {
	peer := domaintest.ID("reader-scrolls-down")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	deliverIncoming(t, r, peer, "m3", start.Add(2*time.Minute))
	deliverIncoming(t, r, peer, "m4", start.Add(3*time.Minute))

	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m3", AtEnd: false})
	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread after scrolling to m3 = %d, want 1 — only m4 is still below the screen", got)
	}
	if first, _ := markerOf(r); first != "m2" {
		t.Fatalf("divider moved to %q while the run it marks was being read, want m2", first)
	}

	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m4", AtEnd: true})
	settle(t, r)
	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread at the end = %d, want 0", got)
	}
	// Each report sends its batch from its own background goroutine, so the
	// two batches reach the RPC in either order. What matters is that each
	// carries exactly what that report brought on screen.
	sent := recorder.sent()
	inOrder := len(sent) == 2 && idsEqual(sent[0], "m2", "m3") && idsEqual(sent[1], "m4")
	swapped := len(sent) == 2 && idsEqual(sent[0], "m4") && idsEqual(sent[1], "m2", "m3")
	if !inOrder && !swapped {
		t.Fatalf("receipts = %v, want the batches [m2 m3] and [m4]", sent)
	}
	if first, _ := markerOf(r); first != "m2" {
		t.Fatalf("divider = %q after the run was read, want it kept above m2", first)
	}
}

// TestANewUnreadRunMovesTheDivider: once a run is read, the next message the
// reader misses starts a new one, and the divider goes to it.
func TestANewUnreadRunMovesTheDivider(t *testing.T) {
	peer := domaintest.ID("reader-second-run")
	start := time.Now().Add(-time.Hour)
	r, _ := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})

	deliverIncoming(t, r, peer, "m3", start.Add(2*time.Minute))
	settle(t, r)

	if first, _ := markerOf(r); first != "m3" {
		t.Fatalf("divider = %q, want above m3, the start of the new run", first)
	}
}

// TestFailedReceiptPutsTheBadgeBack: the badge drops as the reader arrives,
// and a receipt that does not go out means the database still calls the
// message unread — so the badge returns.
func TestFailedReceiptPutsTheBadgeBack(t *testing.T) {
	peer := domaintest.ID("reader-receipt-fails")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	recorder.fail = true
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})
	settle(t, r)

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1 — the receipt failed", got)
	}
}

// TestReaderReportForAnotherConversationIsDropped: the UI can trail a switch
// by a frame, and its report then describes the conversation being left.
func TestReaderReportForAnotherConversationIsDropped(t *testing.T) {
	peer := domaintest.ID("reader-open")
	other := domaintest.ID("reader-left")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	r.ReportReaderPosition(other, ReaderPosition{NewestSeen: "m2", AtEnd: true})
	settle(t, r)

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1 — the report was about another conversation", got)
	}
	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestReclickingTheOpenConversationTakesTheReaderToTheEnd: clicking the chat
// that is already open with messages waiting below is how the reader asks to
// go down to them — it shows the end, and everything there is read.
func TestReclickingTheOpenConversationTakesTheReaderToTheEnd(t *testing.T) {
	peer := domaintest.ID("reader-reclick")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	r.SelectPeer(peer)

	if !r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("re-clicking the open conversation with unread below did not take the reader to the end")
	}
	r.mu.RLock()
	atEnd := r.reader.atEnd
	r.mu.RUnlock()
	if !atEnd {
		t.Fatal("the reader taken to the end is still considered elsewhere")
	}
	settle(t, r)
	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d after the reader was taken to the end, want 0", got)
	}
	sent := recorder.sent()
	if len(sent) != 1 || !containsID(sent[0], "m2") {
		t.Fatalf("receipts = %v, want one batch carrying m2", sent)
	}
}

func containsID(ids []domain.MessageID, want domain.MessageID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestOpeningAConversationShowsItsEndAndMarksWhereUnreadBegins: an opened
// conversation is shown from its end, with the divider above the first
// message that was unread when it was opened — and a later reload of the same
// open conversation moves neither the reader nor the divider.
func TestOpeningAConversationShowsItsEndAndMarksWhereUnreadBegins(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	sender := knownContact(t, client)
	peer := domain.PeerIdentityFromWire(sender.Address)
	start := time.Now().UTC().Add(-time.Hour)
	for i, msgID := range []string{"o1", "o2", "o3"} {
		appendIncomingSealed(t, client, sender, id, msgID, start.Add(time.Duration(i)*time.Minute), chatlog.StatusDelivered)
	}

	r := newTestRouter()
	r.client = client
	recorder := &seenRecorder{}
	r.markConversationSeenFn = recorder.record
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, "o2")
	r.markUnreadLocked(peer, "o3")
	r.mu.Unlock()

	r.SelectPeer(peer)
	if !pollCondition(3*time.Second, func() bool { return r.cache.MatchesPeer(peer) }) {
		t.Fatal("the conversation never loaded")
	}
	settle(t, r)

	if !r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("an opened conversation was not shown from its end")
	}
	if first, placed := markerOf(r); !placed || first != "o2" {
		t.Fatalf("divider = %q (placed %v), want above o2, the first unread at open", first, placed)
	}
	// The open reads the conversation once — the load that carried it out
	// does, and the selection does not read it again after.
	if sent := recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "o1", "o2", "o3") {
		t.Fatalf("receipts = %v, want one batch [o1 o2 o3]", sent)
	}
	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d after the open, want 0", got)
	}

	if !r.loadConversation(peer, r.peerEpochsOf(peer)) {
		// settle shut the router down; the load itself does not need it.
		t.Fatal("reload of the open conversation failed")
	}
	if r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("a reload of the conversation on screen pulled the reader to its end")
	}
	if first, _ := markerOf(r); first != "o2" {
		t.Fatalf("divider = %q after a reload, want it kept above o2", first)
	}
}

// knownContact is a peer whose keys the node holds, so what it sends can be
// decrypted when the conversation is read back from the store. A conversation
// of rows nobody can decrypt loads empty — FetchConversation drops them — and
// an empty conversation has nowhere to put a divider.
func knownContact(t *testing.T, client *DesktopClient) *identity.Identity {
	t.Helper()
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	reply, err := client.rpc.LocalRequestFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: sender.Address,
			PubKey:  identity.PublicKeyBase64(sender.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(sender),
		}},
	})
	if err != nil || reply.Type != "contacts_imported" {
		t.Fatalf("import contact: %v %v", reply.Type, err)
	}
	return sender
}

// appendIncomingSealed stores a message from sender to me the way the node
// does: encrypted for me, with the delivery status the database holds for it.
func appendIncomingSealed(t *testing.T, client *DesktopClient, sender, me *identity.Identity, messageID string, at time.Time, status string) {
	t.Helper()
	ciphertext, err := directmsg.EncryptForParticipants(sender, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(me.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(me.BoxPublicKey),
	}, domain.OutgoingDM{Body: messageID})
	if err != nil {
		t.Fatalf("encrypt %s: %v", messageID, err)
	}
	if err := client.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(me.Address), chatlog.Entry{
		ID: messageID, Sender: sender.Address, Recipient: me.Address,
		Body: ciphertext, CreatedAt: at.Format(time.RFC3339Nano),
		DeliveryStatus: status,
	}); err != nil {
		t.Fatalf("append %s: %v", messageID, err)
	}
}
