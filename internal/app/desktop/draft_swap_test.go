package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

// TestDraftSwapTextRoundTripsAcrossPeers is the core feature: text typed for A
// is preserved when the user switches to B and restored on returning to A, and
// B keeps its own independent draft. This is the A → B → A sequence that had no
// coverage before.
func TestDraftSwapTextRoundTripsAcrossPeers(t *testing.T) {
	w := &Window{}
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")

	// Open A (draftPeer starts zero), type a draft.
	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	w.messageEditor.SetText("hello to A")

	// Switch to B: A's text is stashed and the composer is cleared for B.
	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "" {
		t.Fatalf("composer entering B = %q, want empty", got)
	}
	w.messageEditor.SetText("hi B")

	// Back to A: A's draft is restored.
	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "hello to A" {
		t.Fatalf("restored A draft = %q, want %q", got, "hello to A")
	}

	// And B still holds its own draft.
	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "hi B" {
		t.Fatalf("restored B draft = %q, want %q", got, "hi B")
	}
}

// TestDraftSwapFileRoundTripsAcrossPeers verifies the selected-but-unsent
// attachment is stashed and restored with the conversation, independently of
// the text draft.
func TestDraftSwapFileRoundTripsAcrossPeers(t *testing.T) {
	w := &Window{}
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")

	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	w.attachedFile = "/tmp/a.bin"

	// Switch to B: A's attachment stashed, live composer's attachment cleared.
	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()
	if w.attachedFile != "" {
		t.Fatalf("attachedFile entering B = %q, want empty", w.attachedFile)
	}

	// Back to A: attachment restored.
	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	if w.attachedFile != "/tmp/a.bin" {
		t.Fatalf("restored A attachment = %q, want /tmp/a.bin", w.attachedFile)
	}
}

// TestDraftSwapTextAndFileTogether verifies a draft carrying both text and an
// attachment survives the round trip as a unit.
func TestDraftSwapTextAndFileTogether(t *testing.T) {
	w := &Window{}
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")

	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	w.messageEditor.SetText("caption")
	w.attachedFile = "/tmp/a.bin"

	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()

	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "caption" {
		t.Fatalf("restored text = %q, want caption", got)
	}
	if w.attachedFile != "/tmp/a.bin" {
		t.Fatalf("restored attachment = %q, want /tmp/a.bin", w.attachedFile)
	}
}

// TestDraftSwapEmptyComposerClearsStashedDraft verifies that leaving a
// conversation with an empty composer drops any previously stashed draft, so a
// cleared message does not silently reappear later.
func TestDraftSwapEmptyComposerClearsStashedDraft(t *testing.T) {
	w := &Window{}
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")

	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	w.messageEditor.SetText("draft")

	// A -> B -> A so "draft" is stashed and restored.
	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()
	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "draft" {
		t.Fatalf("precondition: A draft = %q, want draft", got)
	}

	// Clear the composer, then leave A: the empty composer must drop A's slot.
	w.messageEditor.SetText("")
	w.snap = service.RouterSnapshot{ActivePeer: peerB}
	w.swapComposerDraftOnPeerChange()
	if _, ok := w.drafts[peerA]; ok {
		t.Fatalf("drafts[A] present after leaving with an empty composer, want cleared")
	}

	// Returning to A shows an empty composer.
	w.snap = service.RouterSnapshot{ActivePeer: peerA}
	w.swapComposerDraftOnPeerChange()
	if got := w.messageEditor.Text(); got != "" {
		t.Fatalf("A composer after clear = %q, want empty", got)
	}
}
