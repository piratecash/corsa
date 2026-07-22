package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func newFailedTestWindow() *Window {
	return &Window{
		failedSends: make(map[domain.PeerIdentity][]failedSend),
		failedShown: make(map[domain.PeerIdentity]int),
	}
}

// TestAddFailedSendAppendsAndDropsEmpty verifies empty entries (no body, no
// file) are ignored and everything else is appended in order.
func TestAddFailedSendAppendsAndDropsEmpty(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "one"})
	w.addFailedSend(peer, failedSend{}) // empty: dropped
	w.addFailedSend(peer, failedSend{file: "/tmp/f.bin"})

	list := w.failedSends[peer]
	if len(list) != 2 {
		t.Fatalf("len = %d, want 2 (empty entry dropped)", len(list))
	}
	if list[0].body != "one" || list[1].file != "/tmp/f.bin" {
		t.Fatalf("entries = %+v, want text then file", list)
	}
}

// TestDismissKeepsUnseenTail is the P1 scenario: a failure that arrived after
// the banner was rendered (not part of the shown prefix) must survive Dismiss.
func TestDismissKeepsUnseenTail(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "seen-1"})
	w.addFailedSend(peer, failedSend{body: "seen-2"})
	w.failedShown[peer] = 2 // banner rendered these two

	// A new failure lands between the render and the click.
	w.addFailedSend(peer, failedSend{body: "unseen"})

	w.dismissShownFailedSends(peer)

	got := w.failedSends[peer]
	if len(got) != 1 || got[0].body != "unseen" {
		t.Fatalf("after dismiss = %+v, want only the unseen entry", got)
	}
}

// TestRetryScopedToShownPrefix verifies the shown/unseen split: only the
// rendered prefix is taken for retry; the tail stays for the next frame.
func TestRetryScopedToShownPrefix(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "seen"})
	w.failedShown[peer] = 1
	w.addFailedSend(peer, failedSend{body: "unseen"})

	shown, unseen := w.shownFailedPrefix(peer)
	if len(shown) != 1 || shown[0].body != "seen" {
		t.Fatalf("shown = %+v, want [seen]", shown)
	}
	if len(unseen) != 1 || unseen[0].body != "unseen" {
		t.Fatalf("unseen = %+v, want [unseen]", unseen)
	}
}

// TestDoubleDismissSameFrameKeepsUnseen is the P2 regression: two Dismiss (or
// Retry) clicks can be queued in one frame with no layout in between. After the
// first click, setFailedSends must have reset failedShown to 0 so the second
// click cannot consume the unseen tail the first click preserved.
func TestDoubleDismissSameFrameKeepsUnseen(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "seen"})
	w.failedShown[peer] = 1
	w.addFailedSend(peer, failedSend{body: "unseen"})

	// First click removes "seen", keeps "unseen".
	w.dismissShownFailedSends(peer)
	if w.failedShown[peer] != 0 {
		t.Fatalf("failedShown after first dismiss = %d, want 0 until next layout", w.failedShown[peer])
	}
	// Second click queued in the SAME frame (no layoutFailedSends ran): no-op.
	w.dismissShownFailedSends(peer)

	got := w.failedSends[peer]
	if len(got) != 1 || got[0].body != "unseen" {
		t.Fatalf("after double dismiss = %+v, want the unseen entry preserved", got)
	}
}

// TestShownFailedPrefixClampsWhenListShrank guards the defensive clamp: a stale
// failedShown larger than the current list must not slice out of range.
func TestShownFailedPrefixClampsWhenListShrank(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "only"})
	w.failedShown[peer] = 5 // stale, larger than len(list)

	shown, unseen := w.shownFailedPrefix(peer)
	if len(shown) != 1 || len(unseen) != 0 {
		t.Fatalf("shown=%d unseen=%d, want 1 and 0 (clamped)", len(shown), len(unseen))
	}
}

// TestSetFailedSendsEmptyDeletesKeys verifies clearing the list drops both the
// failedSends and failedShown entries so no stale bookkeeping lingers.
func TestSetFailedSendsEmptyDeletesKeys(t *testing.T) {
	w := newFailedTestWindow()
	peer := domaintest.ID("peer")

	w.addFailedSend(peer, failedSend{body: "x"})
	w.failedShown[peer] = 1

	w.setFailedSends(peer, nil)

	if _, ok := w.failedSends[peer]; ok {
		t.Fatalf("failedSends[peer] present, want deleted")
	}
	if _, ok := w.failedShown[peer]; ok {
		t.Fatalf("failedShown[peer] present, want deleted")
	}
}
