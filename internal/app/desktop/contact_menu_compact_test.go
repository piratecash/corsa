package desktop

import (
	"image"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/service"
)

// contactRowHarness lays a single contact row out frame by frame and taps it
// with REAL pointer events. That is the whole point: the "⋯" button sits inside
// the row's own Clickable and Gio delivers the press to both, so a tap on it
// produces a menu click AND a row click. A programmatic Clickable.Click() feeds
// only one of the two and would let the bug this suite covers pass unseen.
type contactRowHarness struct {
	w    *Window
	peer domain.PeerIdentity
	rt   *input.Router
	ops  *op.Ops
	now  time.Time
}

// newClosedTestRouter is a real router over a node-less client, with its
// operation gate closed: selections are recorded in full — which is what the
// assertions read — while the background conversation load, which would want a
// node and a database, never starts.
func newClosedTestRouter(t *testing.T) *service.DMRouter {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	router := service.NewDMRouter(service.NewDesktopClient(config.App{}, config.Node{}, id, nil, nil), nil, nil, nil)
	router.ShutdownDrain(time.Second)
	return router
}

func newContactRowHarness(t *testing.T, peer domain.PeerIdentity) *contactRowHarness {
	t.Helper()
	w := newIdentityLayoutTestWindow(t)
	w.menuBtnRects = make(map[*widget.Clickable]image.Rectangle)
	w.snap.Peers = map[domain.PeerIdentity]*service.RouterPeerState{peer: {}}
	w.router = newClosedTestRouter(t)
	return &contactRowHarness{
		w:    w,
		peer: peer,
		rt:   new(input.Router),
		ops:  new(op.Ops),
		now:  time.Date(2026, time.August, 29, 9, 0, 0, 0, time.UTC),
	}
}

func (h *contactRowHarness) frame() {
	h.ops.Reset()
	h.now = h.now.Add(16 * time.Millisecond)
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.rt.Source(),
		Now:         h.now,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(360, 120)},
	}
	h.w.layoutRecipientButton(gtx, service.NodeStatus{}, h.peer, true)
	h.rt.Frame(h.ops)
}

// tap presses and releases at pos, one frame apart — 16 ms, far short of the
// 500 ms that would make it a long-press.
func (h *contactRowHarness) tap(pos image.Point) {
	at := f32.Pt(float32(pos.X), float32(pos.Y))
	h.rt.Queue(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
	h.frame()
	h.rt.Queue(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})
	h.frame()
}

// menuButtonCenter finds the "⋯" from the semantic tree, so the tap lands on the
// real button wherever the row's flex puts it.
func (h *contactRowHarness) menuButtonCenter(t *testing.T) image.Point {
	t.Helper()
	for _, node := range h.rt.AppendSemantics(nil) {
		if node.Desc.Description != h.w.t("context.menu_button_contact") {
			continue
		}
		b := node.Desc.Bounds
		if b.Empty() {
			t.Fatalf("the %q button has no bounds to tap", node.Desc.Description)
		}
		return b.Min.Add(b.Max.Sub(b.Min).Div(2))
	}
	t.Fatal("no contact menu button in the row's semantic tree")
	return image.Point{}
}

// The reported bug: on a phone, tapping "⋯" swapped the contact list for the
// conversation, as if the row itself had been tapped — because it had been. In
// the single-pane layout a selection IS that swap, so the menu takes none: it
// would also clear the unread badge and send seen receipts for a conversation
// nobody has looked at.
func TestCompactMenuButtonTapOpensOnlyTheMenu(t *testing.T) {
	peer := domaintest.ID("compact-menu-peer")
	h := newContactRowHarness(t, peer)
	h.w.paneCompact = true

	h.frame() // register the row's widgets and their semantics
	h.tap(h.menuButtonCenter(t))

	if h.w.contextMenuPeer != peer {
		t.Fatalf("contextMenuPeer = %q, want the menu open for %q", h.w.contextMenuPeer, peer)
	}
	if got := h.w.router.ActivePeer(); !got.IsZero() {
		t.Fatalf("active peer = %q, want none: selecting is what opens the chat on one pane", got)
	}
}

// Two panes have room for both, and this is the half a phone gives up: there
// the chat appears beside the list the menu was opened from.
func TestTwoPaneMenuButtonTapAlsoSelectsTheContact(t *testing.T) {
	peer := domaintest.ID("two-pane-menu-peer")
	h := newContactRowHarness(t, peer)
	h.w.paneCompact = false

	h.frame()
	h.tap(h.menuButtonCenter(t))

	if h.w.contextMenuPeer != peer {
		t.Fatalf("contextMenuPeer = %q, want the menu open for %q", h.w.contextMenuPeer, peer)
	}
	if got := h.w.router.ActivePeer(); got != peer {
		t.Fatalf("active peer = %q, want the menu's contact selected (%q)", got, peer)
	}
}

// The control case: without it the fix above could be "a contact row does
// nothing at all".
func TestContactRowTapSelectsTheConversation(t *testing.T) {
	peer := domaintest.ID("compact-row-peer")
	h := newContactRowHarness(t, peer)
	h.w.paneCompact = true

	h.frame()
	// Far from the "⋯", which sits at the row's trailing edge.
	h.tap(image.Pt(24, 30))

	if !h.w.contextMenuPeer.IsZero() {
		t.Fatalf("a plain row tap opened the menu for %q", h.w.contextMenuPeer)
	}
	if got := h.w.router.ActivePeer(); got != peer {
		t.Fatalf("active peer = %q, want the tapped contact (%q)", got, peer)
	}
}

// The other way into the menu on a touch screen: the long-press timer opens it
// while the finger is still down, and the finger's Release then completes the
// row's own Clickable a frame later. That late click carries the same selection
// and has to be dropped just like the "⋯" one.
func TestCompactLongPressTailClickDoesNotSelect(t *testing.T) {
	peer := domaintest.ID("compact-longpress-peer")
	h := newContactRowHarness(t, peer)
	h.w.paneCompact = true
	// The state the long-press timer leaves behind on the frame before.
	h.w.contextMenuPeer = peer

	h.frame()
	h.tap(image.Pt(24, 30))

	if h.w.contextMenuPeer != peer {
		t.Fatalf("contextMenuPeer = %q, want the menu still open for %q", h.w.contextMenuPeer, peer)
	}
	if got := h.w.router.ActivePeer(); !got.IsZero() {
		t.Fatalf("active peer = %q, want none: the long-press tail must not select", got)
	}
}

// paneCompact is what the row handlers read, and only layoutMain can decide it:
// a row's own constraints are the sidebar's, which is narrower than the
// breakpoint in BOTH layouts.
func TestLayoutMainRecordsThePaneDecision(t *testing.T) {
	for _, tc := range []struct {
		name  string
		width int
		want  bool
	}{
		{"phone width is single-pane", compactLayoutMaxDp - 1, true},
		{"desktop width is two-pane", compactLayoutMaxDp + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIdentityLayoutTestWindow(t)
			w.router = newClosedTestRouter(t)
			w.contactsList = widget.List{List: layout.List{Axis: layout.Vertical}}
			w.paneCompact = !tc.want // must be overwritten either way

			var rt input.Router
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Source:      rt.Source(),
				Now:         time.Date(2026, time.August, 29, 9, 0, 0, 0, time.UTC),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(tc.width, 720)),
			}
			w.layoutMain(gtx)
			rt.Frame(gtx.Ops)

			if w.paneCompact != tc.want {
				t.Fatalf("paneCompact at %ddp = %v, want %v", tc.width, w.paneCompact, tc.want)
			}
		})
	}
}
