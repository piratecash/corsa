package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// newTestWindow returns a Window struct with just enough state to drive
// applyPendingAttach. It deliberately avoids any Gio / router dependencies —
// the attachment routing/generation logic operates purely on the fields
// assigned here. draftPeer defaults to the zero peer, so a message whose peer
// is also the zero peer exercises the live-composer path.
func newTestWindow() *Window {
	return &Window{
		pendingAttach: make(chan pendingAttachMsg, 64),
		attachGen:     make(map[domain.PeerIdentity]uint64),
		drafts:        make(map[domain.PeerIdentity]composerDraft),
	}
}

// TestApplyPendingAttachUserPickWinsOverEmpty verifies that a fresh user
// pick replaces an empty composer slot and bumps the peer's generation.
func TestApplyPendingAttachUserPickWinsOverEmpty(t *testing.T) {
	w := newTestWindow()

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: false})

	if w.attachedFile != "/tmp/a.bin" {
		t.Errorf("attachedFile = %q, want /tmp/a.bin", w.attachedFile)
	}
	if w.attachGen[w.draftPeer] != 1 {
		t.Errorf("attachGen = %d, want 1 after first pick", w.attachGen[w.draftPeer])
	}
}

// TestApplyPendingAttachUserPickOverwritesExisting verifies that a new user
// pick replaces a prior attachment and bumps the generation — this is the
// path that later invalidates any in-flight restore from the previous send.
func TestApplyPendingAttachUserPickOverwritesExisting(t *testing.T) {
	w := newTestWindow()
	w.attachedFile = "/tmp/a.bin"
	w.attachGen[w.draftPeer] = 1

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/b.bin", restore: false})

	if w.attachedFile != "/tmp/b.bin" {
		t.Errorf("attachedFile = %q, want /tmp/b.bin", w.attachedFile)
	}
	if w.attachGen[w.draftPeer] != 2 {
		t.Errorf("attachGen = %d, want 2", w.attachGen[w.draftPeer])
	}
}

// TestApplyPendingAttachRestoreHappyPath verifies that a restore delivered
// while the composer is empty and the generation still matches replays the
// attachment for the user to retry.
func TestApplyPendingAttachRestoreHappyPath(t *testing.T) {
	w := newTestWindow()
	// User picked, triggerFileSend captured gen=1 and cleared attachedFile.
	w.attachedFile = ""
	w.attachGen[w.draftPeer] = 1

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 1})

	if w.attachedFile != "/tmp/a.bin" {
		t.Errorf("attachedFile = %q, want /tmp/a.bin (restore should succeed)", w.attachedFile)
	}
	if w.attachGen[w.draftPeer] != 1 {
		t.Errorf("attachGen = %d, want 1 (restore must not bump)", w.attachGen[w.draftPeer])
	}
}

// TestApplyPendingAttachRestoreDroppedWhenGenerationBumped verifies a late
// restore from a failed old send MUST NOT overwrite a newer user-selected
// attachment for the same conversation.
func TestApplyPendingAttachRestoreDroppedWhenGenerationBumped(t *testing.T) {
	w := newTestWindow()
	// gen=1 picked /tmp/a.bin, send captured sendGen=1 and cleared; gen=2
	// picked /tmp/b.bin (still present).
	w.attachedFile = "/tmp/b.bin"
	w.attachGen[w.draftPeer] = 2

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 1})

	if w.attachedFile != "/tmp/b.bin" {
		t.Errorf("attachedFile = %q, want /tmp/b.bin (newer pick must survive)", w.attachedFile)
	}
	if w.attachGen[w.draftPeer] != 2 {
		t.Errorf("attachGen = %d, want 2 (restore must not bump)", w.attachGen[w.draftPeer])
	}
}

// TestApplyPendingAttachRestoreDroppedWhenCancelled verifies that an explicit
// attachment cancel (which bumps the peer's generation) invalidates any
// in-flight restore.
func TestApplyPendingAttachRestoreDroppedWhenCancelled(t *testing.T) {
	w := newTestWindow()
	// Picked gen=1, send captured sendGen=1 and cleared; cancel bumped to 2.
	w.attachedFile = ""
	w.attachGen[w.draftPeer] = 2

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 1})

	if w.attachedFile != "" {
		t.Errorf("attachedFile = %q, want empty (cancel must invalidate stale restore)", w.attachedFile)
	}
}

// TestApplyPendingAttachRestoreDroppedWhenSlotNonEmpty is defense-in-depth:
// if attachedFile is non-empty when a restore arrives, the restore must not
// overwrite it even at a matching generation.
func TestApplyPendingAttachRestoreDroppedWhenSlotNonEmpty(t *testing.T) {
	w := newTestWindow()
	w.attachedFile = "/tmp/newer.bin"
	w.attachGen[w.draftPeer] = 1

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 1})

	if w.attachedFile != "/tmp/newer.bin" {
		t.Errorf("attachedFile = %q, want /tmp/newer.bin (slot must not be overwritten)", w.attachedFile)
	}
}

// TestApplyPendingAttachUserPickForOtherPeerGoesToDraft verifies that a pick
// resolving after the user switched conversation lands in the originating
// conversation's draft, not the composer that is now open.
func TestApplyPendingAttachUserPickForOtherPeerGoesToDraft(t *testing.T) {
	w := newTestWindow()
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")
	w.draftPeer = peerB // user is now looking at B

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: false, peer: peerA})

	if w.attachedFile != "" {
		t.Errorf("attachedFile = %q, want empty (B's live composer must be untouched)", w.attachedFile)
	}
	if got := w.drafts[peerA].attachedFile; got != "/tmp/a.bin" {
		t.Errorf("drafts[A].attachedFile = %q, want /tmp/a.bin", got)
	}
	if w.attachGen[peerA] != 1 {
		t.Errorf("attachGen[A] = %d, want 1", w.attachGen[peerA])
	}
}

// TestApplyPendingAttachRestoreForOtherPeerFillsEmptyDraft verifies that a
// failed-send restore for a conversation the user has left is stashed into
// that conversation's draft (rather than lost) when its generation matches.
func TestApplyPendingAttachRestoreForOtherPeerFillsEmptyDraft(t *testing.T) {
	w := newTestWindow()
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")
	w.draftPeer = peerB
	w.attachGen[peerA] = 3 // generation captured by the send

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 3, peer: peerA})

	if got := w.drafts[peerA].attachedFile; got != "/tmp/a.bin" {
		t.Errorf("drafts[A].attachedFile = %q, want /tmp/a.bin (restore should fill draft)", got)
	}
}

// TestApplyPendingAttachRestoreForOtherPeerRejectedWhenDraftHasFile verifies
// that a stale restore never clobbers a newer attachment already stashed in
// the target conversation's draft.
func TestApplyPendingAttachRestoreForOtherPeerRejectedWhenDraftHasFile(t *testing.T) {
	w := newTestWindow()
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")
	w.draftPeer = peerB
	w.attachGen[peerA] = 3
	w.drafts[peerA] = composerDraft{attachedFile: "/tmp/newer.bin"}

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 3, peer: peerA})

	if got := w.drafts[peerA].attachedFile; got != "/tmp/newer.bin" {
		t.Errorf("drafts[A].attachedFile = %q, want /tmp/newer.bin (must not overwrite)", got)
	}
}

// TestApplyPendingAttachGenerationIsPerPeer verifies the core #4 fix: an
// attachment action in one conversation must not invalidate a valid restore
// for a different conversation. peerB's generation is advanced, yet peerA's
// restore (whose own generation is unchanged) still applies.
func TestApplyPendingAttachGenerationIsPerPeer(t *testing.T) {
	w := newTestWindow()
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")
	w.draftPeer = peerA // A is open
	w.attachedFile = ""
	w.attachGen[peerA] = 0 // A never had an attachment action
	w.attachGen[peerB] = 5 // B has, repeatedly — must not affect A

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: true, generation: 0, peer: peerA})

	if w.attachedFile != "/tmp/a.bin" {
		t.Errorf("attachedFile = %q, want /tmp/a.bin (peerB's generation must not reject peerA's restore)", w.attachedFile)
	}
}

// TestApplyPendingAttachDropsDeliveryForRemovedPeer verifies that a delivery
// whose conversation was removed while the file dialog / send was in flight
// (its forget-epoch has since advanced) is dropped, rather than resurrecting a
// draft for the deleted contact.
func TestApplyPendingAttachDropsDeliveryForRemovedPeer(t *testing.T) {
	w := newTestWindow()
	peerA := domaintest.ID("peer-a")
	peerB := domaintest.ID("peer-b")
	w.draftPeer = peerB
	// A was removed since the dialog opened: forget-epoch advanced past the
	// epoch (0) captured by the in-flight pick.
	w.peerForgetEpoch = map[domain.PeerIdentity]uint64{peerA: 1}

	w.applyPendingAttach(pendingAttachMsg{path: "/tmp/a.bin", restore: false, peer: peerA, epoch: 0})

	if _, ok := w.drafts[peerA]; ok {
		t.Errorf("drafts[A] present, want none (delivery for a removed peer must be dropped)")
	}
}
