package desktop

import (
	"image"
	"testing"

	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/domain"
)

// composerFrame is the context the composer card is laid out in: a desktop
// width, room to grow, and one pixel per dp so the numbers below are the ones
// the code asks for.
func composerFrame() layout.Context {
	return layout.Context{
		Ops:    new(op.Ops),
		Metric: unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{
			Max: image.Pt(720, 900),
		},
	}
}

// TestAttachingAFileGrowsTheComposerByTheChip: the card is painted at one
// size and its content laid out at another, and when the content is the
// larger of the two the editor row draws past the bottom edge — over the
// border, which is why the outline disappears under an attachment.
//
// The assertion is a relation, not a number: whatever the chip costs, the
// card has to grow by at least that much. A budget written as a constant
// beside a chip built from insets is exactly how the two drifted apart.
func TestAttachingAFileGrowsTheComposerByTheChip(t *testing.T) {
	t.Parallel()

	w := newIdentityLayoutTestWindow(t)
	// A zero recipient keeps the router out of it — the send button asks
	// about a wipe only for a real peer — and the header is then one line in
	// both passes, so the difference measured below is the chip alone.
	var nobody domain.PeerIdentity

	gtx := composerFrame()
	maxInput, footerReserve := 300, 40

	bare := w.messageInputCard(gtx, nobody, maxInput, footerReserve)

	w.attachedFile = "/tmp/Screenshot 2026-09-09 at 11.03.04.png"
	withFile := w.messageInputCard(composerFrame(), nobody, maxInput, footerReserve)
	chip := w.layoutAttachedFilePreview(composerFrame())

	grew := withFile.Size.Y - bare.Size.Y
	if grew < chip.Size.Y {
		t.Errorf("the card grew by %d for a chip of %d: the content is %d taller than what is painted behind it",
			grew, chip.Size.Y, chip.Size.Y-grew)
	}
}

// TestComposerLeavesRoomForTheFooterWithAnAttachment: the emoji picker is
// sized from what is left after the card's own chrome, so an estimate that
// runs short of the chrome is a picker that takes the footer's reserve with
// it. While the card was painted at a height decided in advance the overrun
// went into the border instead; it has to come out of neither.
func TestComposerLeavesRoomForTheFooterWithAnAttachment(t *testing.T) {
	t.Parallel()

	const available = 360
	const footerReserve = 44

	w := newIdentityLayoutTestWindow(t)
	w.attachedFile = "/tmp/Screenshot 2026-09-09 at 11.03.04.png"
	w.emojiPicker.visible = true

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(720, available)},
	}
	// The composer's own budget, as layoutComposerCard computes it.
	maxInput := max(available/3-gtx.Dp(unit.Dp(76)), gtx.Dp(unit.Dp(62)))

	var nobody domain.PeerIdentity
	card := w.messageInputCard(gtx, nobody, maxInput, footerReserve)

	if card.Size.Y+footerReserve > available {
		t.Errorf("card %d + footer %d = %d in %d available: the footer is pushed out by %d",
			card.Size.Y, footerReserve, card.Size.Y+footerReserve, available,
			card.Size.Y+footerReserve-available)
	}
}

// TestComposerCardIsAsTallAsWhatItDraws pins the rule the card had broken:
// what it reports — and therefore what its border is painted around, and
// what the layout above it reserves — is the size of its content.
func TestComposerCardIsAsTallAsWhatItDraws(t *testing.T) {
	t.Parallel()

	w := newIdentityLayoutTestWindow(t)
	var nobody domain.PeerIdentity
	w.attachedFile = "/tmp/photo.png"

	card := w.messageInputCard(composerFrame(), nobody, 300, 40)
	chip := w.layoutAttachedFilePreview(composerFrame())

	// The chip cannot be taller than the card that contains it, border,
	// padding, header and editor row included.
	if card.Size.Y <= chip.Size.Y {
		t.Fatalf("card = %d, chip alone = %d", card.Size.Y, chip.Size.Y)
	}
}
