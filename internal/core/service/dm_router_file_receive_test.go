package service

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// validTestFileHash is a 64-hex-char placeholder that satisfies
// domain.ValidateFileHash. The bytes are arbitrary; only the
// well-formedness matters for these tests.
const validTestFileHash = "0000000000000000000000000000000000000000000000000000000000000000"

// fakeManagerRegisterIncoming returns a registerIncomingFn closure
// that delegates to a real filetransfer.Manager. Combined with the
// FileTransferBridge.registerIncomingFn seam this lets us exercise
// the full registration → snapshot path without standing up a node.Service.
func fakeManagerRegisterIncoming(m *filetransfer.Manager) func(domain.FileID, string, string, string, uint64, domain.PeerIdentity) error {
	return func(fileID domain.FileID, fileHash, fileName, contentType string, fileSize uint64, sender domain.PeerIdentity) error {
		return m.RegisterFileReceive(fileID, fileHash, fileName, contentType, fileSize, sender)
	}
}

// newTestFileTransferManager builds a minimal filetransfer.Manager
// suitable for in-process registration / snapshot tests. Persistence
// is disabled (MappingsPath == "") so saveMappingsLocked is a no-op
// and we don't need to clean up disk artefacts. DownloadDir is set
// to t.TempDir() so any incidental code path that materialises a
// partialDownloadPath has somewhere valid to land.
func newTestFileTransferManager(t *testing.T) *filetransfer.Manager {
	t.Helper()
	return filetransfer.NewFileTransferManager(filetransfer.Config{
		DownloadDir:  t.TempDir(),
		MappingsPath: "",
	})
}

// TestTryRegisterFileReceiveRegistersAndPublishesOnSuccess is the
// snapshot-level regression test for the "non-active conversations
// can miss the File tab" reviewer P2. It drives tryRegisterFileReceive
// against a real Manager (wired through the FileTransferBridge
// registerIncomingFn seam) and asserts BOTH that:
//
//  1. AllTransfersSnapshot returns the receiver row, with the same
//     metadata the announce carried — proving that the file tab,
//     which reads this snapshot, will surface the transfer regardless
//     of which conversation is active.
//  2. TopicFileReceived fires exactly once with the matching FileID
//     and Sender — proving the file tab is invalidated for the new
//     row.
//
// Without the inline tryRegisterFileReceive call in
// updateSidebarFromEvent, an inbound file_announce delivered to a
// non-active chat would fail BOTH assertions. The combination locks
// in the fix at the layer where the bug manifests.
func TestTryRegisterFileReceiveRegistersAndPublishesOnSuccess(t *testing.T) {
	t.Parallel()

	const (
		fileID   = domain.FileID("file-msg-id-snapshot-1")
		fileName = "report.pdf"
		fileSize = uint64(2048)
		ctype    = "application/pdf"
		sender   = domain.PeerIdentity("peer-incoming-12345678abcd")
	)

	bus := ebus.New()
	var (
		mu       sync.Mutex
		received []ebus.FileReceivedResult
	)
	bus.Subscribe(ebus.TopicFileReceived, func(r ebus.FileReceivedResult) {
		mu.Lock()
		received = append(received, r)
		mu.Unlock()
	}, ebus.WithSync())

	mgr := newTestFileTransferManager(t)
	r := newTestRouter()
	r.eventBus = bus
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	payload := `{"file_hash":"` + validTestFileHash +
		`","file_name":"` + fileName +
		`","content_type":"` + ctype +
		`","file_size":2048}`

	msg := &DirectMessage{
		ID:          string(fileID),
		Sender:      sender,
		Recipient:   r.client.Address(),
		Command:     domain.DMCommandFileAnnounce,
		CommandData: payload,
		Timestamp:   time.Now(),
	}

	r.tryRegisterFileReceive(msg)

	// Snapshot-level invariant: the manager has a receiver row.
	snap := mgr.AllTransfersSnapshot()
	if len(snap) != 1 {
		t.Fatalf("AllTransfersSnapshot: got %d entries, want 1", len(snap))
	}
	row := snap[0]
	if row.FileID != fileID {
		t.Errorf("snapshot row FileID = %q, want %q", row.FileID, fileID)
	}
	if row.Peer != sender {
		t.Errorf("snapshot row Peer = %q, want %q (sender of inbound announce)", row.Peer, sender)
	}
	if row.Direction != "receive" {
		t.Errorf("snapshot row Direction = %q, want %q", row.Direction, "receive")
	}
	if row.FileName != fileName {
		t.Errorf("snapshot row FileName = %q, want %q", row.FileName, fileName)
	}
	if row.FileSize != fileSize {
		t.Errorf("snapshot row FileSize = %d, want %d", row.FileSize, fileSize)
	}

	// Event-level invariant: subscribers were notified exactly once.
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("TopicFileReceived: got %d events, want 1", len(received))
	}
	if received[0].FileID != fileID {
		t.Errorf("FileReceivedResult.FileID = %q, want %q", received[0].FileID, fileID)
	}
	if received[0].From != sender {
		t.Errorf("FileReceivedResult.From = %q, want %q", received[0].From, sender)
	}
}

// TestTryRegisterFileReceiveSkipsPublishOnRegistrationFailure is the
// reviewer P3a pin: when RegisterIncoming returns an error
// (malformed metadata, no localNode in standalone-RPC mode,
// manager-side validation rejection), TopicFileReceived MUST NOT
// fire — the event contract is "a row appeared in
// AllTransfersSnapshot", and a failed registration produces no row.
func TestTryRegisterFileReceiveSkipsPublishOnRegistrationFailure(t *testing.T) {
	t.Parallel()

	bus := ebus.New()
	var seen int32
	bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) {
		atomic.AddInt32(&seen, 1)
	}, ebus.WithSync())

	r := newTestRouter()
	r.eventBus = bus
	// Force registration to fail.
	r.fileBridge.registerIncomingFn = func(domain.FileID, string, string, string, uint64, domain.PeerIdentity) error {
		return errNoLocalNode
	}

	msg := &DirectMessage{
		ID:        "fail-1",
		Sender:    "peer-x",
		Recipient: r.client.Address(),
		Command:   domain.DMCommandFileAnnounce,
		CommandData: `{"file_hash":"` + validTestFileHash +
			`","file_name":"x","file_size":1}`,
	}
	r.tryRegisterFileReceive(msg)

	if got := atomic.LoadInt32(&seen); got != 0 {
		t.Fatalf("TopicFileReceived fired %d times after registration failure; want 0 — subscribers must not see events for transfers AllTransfersSnapshot will not list", got)
	}
}

// TestTryRegisterFileReceiveSkipsPublishOnMalformedPayload covers the
// JSON-parse-error branch of RegisterIncoming. The reviewer flagged
// this specifically: malformed file_announce payloads must not emit
// TopicFileReceived because no mapping is created.
func TestTryRegisterFileReceiveSkipsPublishOnMalformedPayload(t *testing.T) {
	t.Parallel()

	bus := ebus.New()
	var seen int32
	bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) {
		atomic.AddInt32(&seen, 1)
	}, ebus.WithSync())

	mgr := newTestFileTransferManager(t)
	r := newTestRouter()
	r.eventBus = bus
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	msg := &DirectMessage{
		ID:          "malformed-1",
		Sender:      "peer-x",
		Recipient:   r.client.Address(),
		Command:     domain.DMCommandFileAnnounce,
		CommandData: `{not json`, // malformed — RegisterIncoming returns parse error
	}
	r.tryRegisterFileReceive(msg)

	if got := atomic.LoadInt32(&seen); got != 0 {
		t.Fatalf("TopicFileReceived fired %d times for malformed payload; want 0", got)
	}
	if snap := mgr.AllTransfersSnapshot(); len(snap) != 0 {
		t.Fatalf("malformed payload created a snapshot row: %+v", snap)
	}
}

// TestTryRegisterFileReceiveSkipsOutgoing pins the "we are the
// sender, not the receiver" guard. Our own announce already has a
// sender mapping committed by SendFileAnnounce; calling the
// receiver-registration helper for our own announce would either
// double-register or noisily publish a "received" event for a
// transfer the UI already owns from the sender side. Skipping at
// the helper keeps the event semantics clean — TopicFileReceived
// means "inbound file announce from a peer", never "we just sent one".
func TestTryRegisterFileReceiveSkipsOutgoing(t *testing.T) {
	t.Parallel()

	bus := ebus.New()
	var seen int32
	bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) {
		atomic.AddInt32(&seen, 1)
	}, ebus.WithSync())

	mgr := newTestFileTransferManager(t)
	r := newTestRouter()
	r.eventBus = bus
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	msg := &DirectMessage{
		ID:        "outgoing-announce",
		Sender:    r.client.Address(), // we are the sender
		Recipient: "peer-recipient",
		Command:   domain.DMCommandFileAnnounce,
		CommandData: `{"file_hash":"` + validTestFileHash +
			`","file_name":"x","file_size":1}`,
	}
	r.tryRegisterFileReceive(msg)

	if got := atomic.LoadInt32(&seen); got != 0 {
		t.Fatalf("TopicFileReceived fired %d times for an outgoing announce; want 0", got)
	}
	if snap := mgr.AllTransfersSnapshot(); len(snap) != 0 {
		t.Fatalf("outgoing announce created receiver row: %+v", snap)
	}
}

// TestTryRegisterFileReceiveSkipsNonFileAnnounce verifies that plain
// DMs (no Command, or non-file-announce Command) and file-announces
// with empty CommandData do not trigger the receiver-registration
// path. Without these gates every inbound DM would emit
// TopicFileReceived and the file tab would invalidate on every chat
// message — burning cycles for nothing.
func TestTryRegisterFileReceiveSkipsNonFileAnnounce(t *testing.T) {
	t.Parallel()

	bus := ebus.New()
	var seen int32
	bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) {
		atomic.AddInt32(&seen, 1)
	}, ebus.WithSync())

	r := newTestRouter()
	r.eventBus = bus

	cases := []struct {
		name string
		msg  *DirectMessage
	}{
		{
			name: "plain_dm_no_command",
			msg: &DirectMessage{
				ID:        "plain-1",
				Sender:    "peer-x",
				Recipient: r.client.Address(),
				Body:      "hello world",
			},
		},
		{
			name: "file_announce_with_empty_command_data",
			msg: &DirectMessage{
				ID:          "fa-empty-1",
				Sender:      "peer-x",
				Recipient:   r.client.Address(),
				Command:     domain.DMCommandFileAnnounce,
				CommandData: "", // missing payload — must skip
			},
		},
	}
	for _, tc := range cases {
		r.tryRegisterFileReceive(tc.msg)
	}

	if got := atomic.LoadInt32(&seen); got != 0 {
		t.Fatalf("TopicFileReceived fired %d times for non-file-announce / empty-payload messages; want 0", got)
	}
}

// TestApplyDecryptedMessageToSidebarRegistersFileReceiveForNonActive
// is the non-active-path regression pin called out by reviewer P3.
// Earlier tests called tryRegisterFileReceive directly; a regression
// that REMOVED the new tryRegisterFileReceive call from
// updateSidebarFromEvent / applyDecryptedMessageToSidebar would have
// gone unnoticed. This test drives the actual sidebar-apply path
// (the same function the production inline-decrypt branch calls)
// with a non-active peer and a synthetic file_announce DM, then
// asserts that AllTransfersSnapshot includes the receiver row.
//
// The split between updateSidebarFromEvent (decrypt) and
// applyDecryptedMessageToSidebar (apply) was introduced precisely
// to make this test possible without standing up a real DMCrypto
// keychain — see updateSidebarFromEvent's doc comment.
func TestApplyDecryptedMessageToSidebarRegistersFileReceiveForNonActive(t *testing.T) {
	t.Parallel()

	const (
		fileID   = domain.FileID("file-nonactive-regression-1")
		fileName = "doc.pdf"
		fileSize = uint64(4096)
		ctype    = "application/pdf"
		sender   = domain.PeerIdentity("peer-nonactive-12345678abcd")
		// peerID identifies the conversation that is NOT currently
		// active. By construction (newTestRouter has activePeer = ""
		// zero value), this is a non-active conversation.
		peerID = sender
	)

	bus := ebus.New()
	var (
		mu       sync.Mutex
		received []ebus.FileReceivedResult
	)
	bus.Subscribe(ebus.TopicFileReceived, func(r ebus.FileReceivedResult) {
		mu.Lock()
		received = append(received, r)
		mu.Unlock()
	}, ebus.WithSync())

	mgr := newTestFileTransferManager(t)
	r := newTestRouter()
	r.eventBus = bus
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	// Sanity: the active peer is not this peer — without this, the
	// test would silently degrade into the active-conversation path
	// where the bug never manifested. newTestRouter leaves activePeer
	// at the zero value; this assertion locks that invariant in.
	if r.ActivePeer() == peerID {
		t.Fatalf("test setup invariant violated: peerID %q must NOT be the active conversation", peerID)
	}

	payload := `{"file_hash":"` + validTestFileHash +
		`","file_name":"` + fileName +
		`","content_type":"` + ctype +
		`","file_size":4096}`

	msg := &DirectMessage{
		ID:          string(fileID),
		Sender:      sender,
		Recipient:   r.client.Address(),
		Command:     domain.DMCommandFileAnnounce,
		CommandData: payload,
		Timestamp:   time.Now(),
	}

	// Drive the same code path the production inline-decrypt branch
	// runs. If a future regression removes tryRegisterFileReceive
	// from this path, the snapshot assertion below will fail.
	r.applyDecryptedMessageToSidebar(msg, peerID)

	// Snapshot-level invariant: the file tab (which reads
	// AllTransfersSnapshot) sees the receiver row even though the
	// conversation is not active.
	snap := mgr.AllTransfersSnapshot()
	if len(snap) != 1 {
		t.Fatalf("non-active conversation: AllTransfersSnapshot has %d rows, want 1 — receiver mapping was not registered for non-active path", len(snap))
	}
	row := snap[0]
	if row.FileID != fileID {
		t.Errorf("snapshot row FileID = %q, want %q", row.FileID, fileID)
	}
	if row.Direction != "receive" {
		t.Errorf("snapshot row Direction = %q, want %q", row.Direction, "receive")
	}
	if row.Peer != sender {
		t.Errorf("snapshot row Peer = %q, want %q", row.Peer, sender)
	}

	// Event-level invariant: subscribers (the file tab's ebus handler)
	// are notified so they can invalidate.
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("TopicFileReceived: got %d events, want 1", len(received))
	}

	// Sidebar invariant: the non-active peer's preview is updated
	// AND its unread badge is incremented (the original purpose of
	// the inline decrypt path). This double-checks that the file
	// registration did not regress the existing sidebar behaviour.
	r.mu.RLock()
	ps, ok := r.peers[peerID]
	var preview ConversationPreview
	var unread int
	if ok && ps != nil {
		preview = ps.Preview
		unread = ps.Unread
	}
	r.mu.RUnlock()
	if !ok {
		t.Fatal("peer entry was not created for non-active inbound message")
	}
	if preview.Sender != sender {
		t.Errorf("preview.Sender = %q, want %q", preview.Sender, sender)
	}
	if unread != 1 {
		t.Errorf("Unread = %d, want 1 (non-active inbound from peer)", unread)
	}
}

// TestTryRegisterFileReceiveDefensiveNils verifies that a nil
// message OR a router with no fileBridge does not panic and does not
// publish. Both can occur in production: nil msg from a decrypt
// failure caller forgot to guard, no fileBridge in standalone
// configurations that don't enable file transfer.
func TestTryRegisterFileReceiveDefensiveNils(t *testing.T) {
	t.Parallel()

	bus := ebus.New()
	var seen int32
	bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) {
		atomic.AddInt32(&seen, 1)
	}, ebus.WithSync())

	// Nil message — must not panic, must not publish.
	r := newTestRouter()
	r.eventBus = bus
	r.tryRegisterFileReceive(nil)

	// Router with no fileBridge — must not panic, must not publish.
	r2 := newTestRouter()
	r2.eventBus = bus
	r2.fileBridge = nil
	msg := &DirectMessage{
		Sender:      "peer-x",
		Recipient:   r2.client.Address(),
		Command:     domain.DMCommandFileAnnounce,
		CommandData: `{"file_id":"x"}`,
	}
	r2.tryRegisterFileReceive(msg)

	if got := atomic.LoadInt32(&seen); got != 0 {
		t.Fatalf("TopicFileReceived fired %d times for nil/no-bridge cases; want 0", got)
	}
}
