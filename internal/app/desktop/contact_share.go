package desktop

import (
	"errors"
	"image"
	"image/color"
	"io"
	"strings"

	"gioui.org/io/clipboard"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/qrcode"
)

// contact_share.go is the §4.8 UI: the identity panel renders the node's own
// corsa: link as a QR and the share action puts that link on the clipboard; a
// link pasted into identity search or the composer imports the contact — keys
// verified — with no network involved.

func (w *Window) handleIdentityPanelOpen(gtx layout.Context) {
	peer := w.snap.ActivePeer
	if !peer.IsZero() && w.chatIdentityButton.Clicked(gtx) && w.chatIdentityPeer == peer {
		link, qr, err := peerIdentityPanelPayload(peer, w.client.PeerContactLink(peer))
		if err != nil {
			w.router.SetSendStatus(w.t("status.contact_share_failed", err.Error()))
			return
		}
		w.showIdentityPanel(&w.chatIdentityButton, peer, link, qr)
		w.touchKbd.cancelPendingShow()
		gtx.Execute(key.FocusCmd{})
		gtx.Execute(key.SoftKeyboardCmd{Show: false})
		return
	}

	for w.myIdentityButton.Clicked(gtx) {
		link, err := w.router.BuildContactLink()
		if err != nil {
			w.router.SetSendStatus(w.t("status.contact_share_failed", err.Error()))
			continue
		}
		qrImage, err := contactLinkQRImage(link)
		if err != nil {
			w.router.SetSendStatus(w.t("status.contact_share_failed", err.Error()))
			continue
		}
		w.openIdentityPanel(link, qrImage)
	}
}

func (w *Window) handleIdentityPanelActions(gtx layout.Context) {
	if w.identityPanelClose.Clicked(gtx) {
		w.closeIdentityPanel()
		return
	}
	if w.copyIdentityButton.Clicked(gtx) {
		w.copyIdentityPanelValue(gtx, w.identityPanelAddress().String(), "status.identity_copied")
	}
	if w.shareContactButton.Clicked(gtx) {
		if link, ok := contactLinkForClipboard(w.identityPanelContactLink); ok {
			w.copyIdentityPanelValue(gtx, link, "status.contact_link_copied")
		} else if w.identityPanelPeer.IsZero() {
			w.router.SetSendStatus(w.t("status.contact_share_unavailable"))
		}
	}
	w.refreshPeerIdentityPanel()
}

func (w *Window) openIdentityPanel(link string, qrImage widget.Image) {
	w.showIdentityPanel(&w.myIdentityButton, domain.PeerIdentity{}, link, qrImage)
}

func (w *Window) showIdentityPanel(trigger event.Tag, peer domain.PeerIdentity, link string, qrImage widget.Image) {
	w.identityPanelPeer = peer
	w.identityPanelContactLink = link
	w.identityPanelQRImage = qrImage
	w.identityPanelFeedback = ""
	w.identityPanelGeneration = w.snap.Generation
	w.identityPanelList.Position = layout.Position{}
	w.identityPanelVisible = true
	w.identityPanelFocus.open(trigger)
	w.contextMenuPeer = domain.PeerIdentity{}
	w.showDeleteConfirm = false
	w.showClearChatConfirm = false
	w.showAliasEditor = false
	w.msgContextMsg = nil
	w.peerMenuFocus.abandonRestore()
	w.msgMenuFocus.abandonRestore()
}

func (w *Window) closeIdentityPanel() {
	w.identityPanelVisible = false
	w.identityPanelPeer = domain.PeerIdentity{}
	w.identityPanelContactLink = ""
	w.identityPanelQRImage = widget.Image{}
	w.identityPanelFeedback = ""
	if w.window != nil {
		w.window.Invalidate()
	}
}

func (w *Window) identityPanelAddress() domain.PeerIdentity {
	if !w.identityPanelPeer.IsZero() {
		return w.identityPanelPeer
	}
	return w.snap.MyAddress
}

func (w *Window) copyIdentityPanelValue(gtx layout.Context, value, status string) {
	if value == "" {
		return
	}
	gtx.Execute(clipboard.WriteCmd{Type: "text/plain", Data: io.NopCloser(strings.NewReader(value))})
	if w.identityPanelPeer.IsZero() {
		w.router.SetSendStatus(w.t(status))
	}
	w.identityPanelFeedback = w.t(status)
}

func peerIdentityPanelPayload(peer domain.PeerIdentity, link string) (string, widget.Image, error) {
	if link != "" {
		if qr, err := contactLinkQRImage(link); err == nil {
			return link, qr, nil
		}
	}
	qr, err := contactLinkQRImage(peer.String())
	return "", qr, err
}

// Keys can arrive while the card is open. Update its QR and copy action
// together on a new snapshot so both describe the same credential.
func (w *Window) refreshPeerIdentityPanel() {
	peer := w.identityPanelPeer
	if peer.IsZero() || w.identityPanelGeneration == w.snap.Generation {
		return
	}
	w.identityPanelGeneration = w.snap.Generation
	link := w.client.PeerContactLink(peer)
	if link == w.identityPanelContactLink {
		return
	}
	link, qr, err := peerIdentityPanelPayload(peer, link)
	if err == nil {
		w.identityPanelContactLink = link
		w.identityPanelQRImage = qr
		w.identityPanelFeedback = ""
	}
}

func contactLinkForClipboard(link string) (string, bool) {
	link = strings.TrimSpace(link)
	return link, link != ""
}

// handleContactLinkPaste watches the identity-search editor for a pasted
// corsa: link and imports it immediately. Edge-triggered on the text value
// so one bad link is reported once, not every frame.
func (w *Window) handleContactLinkPaste() {
	if w.identityPanelVisible {
		return
	}
	text := strings.TrimSpace(w.identitySearchEditor.Text())
	if !contactlink.IsContactLink(text) {
		w.lastContactLinkTried = ""
		return
	}
	if text == w.lastContactLinkTried {
		return
	}
	w.lastContactLinkTried = text
	if w.importContactLink(text) {
		w.identitySearchEditor.SetText("")
		w.lastContactLinkTried = ""
	}
}

// importContactLink runs the shared verify-then-import path and reports
// the outcome in the status line. Returns true on success.
func (w *Window) importContactLink(link string) bool {
	result, err := w.router.ImportContactLink(link)
	if result.Contact == domain.ContactAppliedLive {
		key := "status.contact_imported_k_pending"
		if result.K == domain.LocalContactKConfirmed {
			key = "status.contact_imported_k_confirmed"
		}
		if err != nil || result.Fact != domain.LocalContactFactDurableConfirmed {
			key = "status.contact_imported_k_unconfirmed"
		}
		w.router.SetSendStatus(w.t(key, shortFingerprint(result.Identity.String())))
		return true
	}
	if err != nil {
		if errors.Is(err, contactlink.ErrLinkNetworkMismatch) {
			w.router.SetSendStatus(w.t("status.contact_link_other_network"))
			return false
		}
		w.router.SetSendStatus(w.t("status.contact_import_failed", err.Error()))
		return false
	}
	w.router.SetSendStatus(w.t("status.contact_imported", shortFingerprint(result.Identity.String())))
	return true
}

// contactLinkQRImage renders the link as a QR module image with the
// mandatory 4-module quiet zone at 4 px per module; the widget scales it.
func contactLinkQRImage(link string) (widget.Image, error) {
	matrix, err := qrcode.Encode([]byte(link))
	if err != nil {
		return widget.Image{}, err
	}
	const scale, quiet = 4, 4
	side := (matrix.Size() + 2*quiet) * scale
	img := image.NewRGBA(image.Rect(0, 0, side, side))
	for y := 0; y < side; y++ {
		for x := 0; x < side; x++ {
			img.Set(x, y, color.White)
		}
	}
	dark := color.RGBA{A: 255}
	for my := 0; my < matrix.Size(); my++ {
		for mx := 0; mx < matrix.Size(); mx++ {
			if !matrix.Dark(mx, my) {
				continue
			}
			for dy := 0; dy < scale; dy++ {
				for dx := 0; dx < scale; dx++ {
					img.Set((quiet+mx)*scale+dx, (quiet+my)*scale+dy, dark)
				}
			}
		}
	}
	return widget.Image{Src: paint.NewImageOp(img), Fit: widget.Contain}, nil
}

func (w *Window) layoutIdentityQR(gtx layout.Context, sideDp unit.Dp) layout.Dimensions {
	side := min(gtx.Dp(sideDp), gtx.Constraints.Max.X, gtx.Constraints.Max.Y)
	gtx.Constraints.Min = image.Pt(side, side)
	gtx.Constraints.Max = image.Pt(side, side)
	return w.identityPanelQRImage.Layout(gtx)
}
