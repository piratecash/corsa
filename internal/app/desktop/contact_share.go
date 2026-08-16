package desktop

import (
	"errors"
	"image"
	"image/color"
	"io"
	"strings"

	"gioui.org/io/clipboard"
	"gioui.org/layout"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/qrcode"
)

// contact_share.go is the §4.8 UI: "share contact" renders the node's own
// corsa: link as a QR and puts the link on the clipboard; a link pasted
// into the identity search or the composer imports the contact — keys
// verified — with no network involved.

// handleShareContact serves the share button: first click builds the link,
// copies it and shows the QR; the next click hides the QR again.
func (w *Window) handleShareContact(gtx layout.Context) {
	for w.shareContactButton.Clicked(gtx) {
		if w.shareQRVisible {
			w.shareQRVisible = false
			continue
		}
		link, err := w.router.BuildContactLink()
		if err != nil {
			w.router.SetSendStatus(w.t("status.contact_import_failed", err.Error()))
			continue
		}
		qrImage, err := contactLinkQRImage(link)
		if err != nil {
			w.router.SetSendStatus(w.t("status.contact_import_failed", err.Error()))
			continue
		}
		w.shareQRImage = qrImage
		w.shareQRVisible = true
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(link)),
		})
		w.router.SetSendStatus(w.t("status.contact_link_copied"))
	}
}

// handleContactLinkPaste watches the identity-search editor for a pasted
// corsa: link and imports it immediately. Edge-triggered on the text value
// so one bad link is reported once, not every frame.
func (w *Window) handleContactLinkPaste() {
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
	peer, err := w.router.ImportContactLink(link)
	if err != nil {
		if errors.Is(err, contactlink.ErrLinkNetworkMismatch) {
			w.router.SetSendStatus(w.t("status.contact_link_other_network"))
			return false
		}
		w.router.SetSendStatus(w.t("status.contact_import_failed", err.Error()))
		return false
	}
	w.router.SetSendStatus(w.t("status.contact_imported", shortFingerprint(peer.String())))
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

// layoutShareContactQR draws the QR panel under the profile header.
func (w *Window) layoutShareContactQR(gtx layout.Context) layout.Dimensions {
	side := gtx.Dp(unit.Dp(220))
	gtx.Constraints.Min = image.Pt(side, side)
	gtx.Constraints.Max = image.Pt(side, side)
	return w.shareQRImage.Layout(gtx)
}
