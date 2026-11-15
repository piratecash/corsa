package desktop

import (
	"image"
	"strings"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestChatHeaderKeepsIDAtRightEdgeAndOpensContactModal(t *testing.T) {
	const address = "ae47201ce461599cbb1562eb159fcee0075fdcdd"
	peer := domain.PeerIdentityFromWire(address)
	for _, tc := range []struct {
		name    string
		width   int
		compact bool
		id      string
	}{
		{"phone", 320, true, "ae47201c…5fdcdd"},
		{"narrow desktop pane", 420, false, "ae47201c…5fdcdd"},
		{"wide desktop pane", 660, false, address},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIdentityLayoutTestWindow(t)
			w.prefs = &Preferences{}
			w.snap.ActivePeer = peer
			var router input.Router
			now := time.Now()
			frame := func() layout.Dimensions {
				gtx := layout.Context{
					Ops:         new(op.Ops),
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Constraints{Max: image.Pt(tc.width, 720)},
					Now:         now,
				}
				now = now.Add(time.Millisecond * 16)
				w.resetConversationStateOnPeerChange()
				w.handleActions(gtx)
				dims := w.layoutChatHeader(gtx, tc.compact)
				if w.identityPanelVisible {
					w.layoutIdentityPanelOverlay(gtx)
				}
				router.Frame(gtx.Ops)
				return dims
			}
			idButton := func() input.SemanticDesc {
				t.Helper()
				want := w.t("chat.show_contact")
				for _, node := range router.AppendSemantics(nil) {
					if node.Desc.Description == want {
						return node.Desc
					}
				}
				t.Fatalf("cannot find %q control", want)
				return input.SemanticDesc{}
			}
			w.prefs.SetAlias(peer, "PC")
			short := frame()
			button := idButton()
			wantLabel := "ID: " + tc.id
			if button.Bounds.Max.X != tc.width || button.Label != wantLabel {
				t.Fatalf("ID button = %q at %v, want %q at the right edge (%d)", button.Label, button.Bounds, wantLabel, tc.width)
			}
			if tc.compact {
				var backBounds image.Rectangle
				for _, node := range router.AppendSemantics(nil) {
					if node.Desc.Description == w.t("chat.back") {
						backBounds = node.Desc.Bounds
						break
					}
				}
				height := button.Bounds.Dy()
				if backBounds.Size() != image.Pt(height, height) || backBounds.Min.Y != button.Bounds.Min.Y {
					t.Fatalf("Back %v is not square and aligned with ID %v", backBounds, button.Bounds)
				}
			}

			w.prefs.SetAlias(peer, strings.Repeat("VeryLongContactName", 12))
			long := frame()
			if long.Size != short.Size || idButton().Bounds != button.Bounds {
				t.Fatalf("long alias changed the header size or ID position")
			}

			tap := func() {
				center := button.Bounds.Min.Add(button.Bounds.Size().Div(2))
				pos := f32.Pt(float32(center.X), float32(center.Y))
				source := pointer.Mouse
				if tc.compact {
					source = pointer.Touch
				}
				router.Queue(pointer.Event{Kind: pointer.Press, Source: source, Buttons: pointer.ButtonPrimary, Position: pos})
				frame()
				router.Queue(pointer.Event{Kind: pointer.Release, Source: source, Position: pos})
				frame()
			}
			tap()
			if !w.identityPanelVisible || w.identityPanelAddress() != peer || w.identityPanelContactLink != "" {
				t.Fatal("click did not open the peer's address-only card")
			}
			if !router.Source().Focused(&w.identityPanelClose) {
				t.Fatal("the contact modal did not take keyboard focus")
			}
			router.Queue(key.Event{Name: key.NameEscape, State: key.Press})
			frame()
			if w.identityPanelVisible {
				t.Fatal("Escape did not close the contact modal")
			}
			frame()
			tap()
			if !w.identityPanelVisible {
				t.Fatal("click did not reopen the contact modal")
			}
			w.snap.ActivePeer = domain.PeerIdentityFromWire("df47201ce461599cbb1562eb159fcee0075fdcdd")
			frame()
			if w.identityPanelVisible || !w.identityPanelPeer.IsZero() {
				t.Fatal("the previous peer's card remained open after switching chats")
			}
			if idButton().Bounds != button.Bounds {
				t.Fatal("changing the address moved the ID button")
			}
		})
	}
}

func TestPeerContactModalCopiesTheCredentialItShows(t *testing.T) {
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer := domain.PeerIdentityFromWire(owner.Address)
	link, err := contactlink.Build(owner, domain.NetworkID("testnet"))
	if err != nil {
		t.Fatal(err)
	}
	for _, withKeys := range []bool{false, true} {
		w := newIdentityLayoutTestWindow(t)
		w.snap.ActivePeer = peer
		candidate := ""
		if withKeys {
			candidate = link
		}
		payload, qr, err := peerIdentityPanelPayload(peer, candidate)
		if err != nil || payload != candidate || qr.Src.Size().X == 0 {
			t.Fatalf("credential and QR did not match the selected state (keys=%t): %v", withKeys, err)
		}
		w.showIdentityPanel(&w.chatIdentityButton, peer, payload, qr)
		var router input.Router
		frame := func() {
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(390, 720)),
			}
			w.handleActions(gtx)
			w.layoutIdentityPanelOverlay(gtx)
			router.Frame(gtx.Ops)
		}
		frame()
		var shareRendered bool
		var copyIconRendered bool
		for _, node := range router.AppendSemantics(nil) {
			if node.Desc.Label == w.t("chat.copy_contact_link") {
				shareRendered = true
			}
			if node.Desc.Description == w.t("chat.copy_id") && node.Desc.Bounds.Size() == image.Pt(44, 44) {
				copyIconRendered = true
			}
		}
		if shareRendered != withKeys || copyIconRendered != withKeys {
			t.Fatalf("keys=%t: copy-link action visible=%t, inline copy button visible=%t", withKeys, shareRendered, copyIconRendered)
		}

		w.copyIdentityButton.Click()
		frame()
		if _, got, ok := router.WriteClipboard(); !ok || string(got) != owner.Address {
			t.Fatal("copy ID did not write the peer's address")
		}
		if withKeys {
			w.shareContactButton.Click()
			frame()
			if _, got, ok := router.WriteClipboard(); !ok || string(got) != link {
				t.Fatal("copy link did not write the verified contact link")
			}
		}
	}
}
