package desktop

import (
	"image"
	"image/color"

	"gioui.org/font"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget/material"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

const fullChatHeaderIDMinWidth = 500
const chatHeaderInset = unit.Dp(4)

func (w *Window) layoutDesktopChatHeader(gtx layout.Context) layout.Dimensions {
	if w.snap.ActivePeer.IsZero() {
		return layout.Dimensions{}
	}
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Top: chatHeaderInset, Left: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.layoutChatHeader(gtx, false)
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			size := image.Pt(gtx.Constraints.Max.X, min(gtx.Dp(1), gtx.Constraints.Max.Y))
			paint.FillShape(gtx.Ops, color.NRGBA{R: 41, G: 53, B: 68, A: 255}, clip.Rect{Max: size}.Op())
			return layout.Dimensions{Size: size}
		}),
	)
}

func (w *Window) layoutChatHeader(gtx layout.Context, compact bool) layout.Dimensions {
	peer := w.snap.ActivePeer
	if peer.IsZero() {
		return layout.Dimensions{}
	}
	w.chatIdentityPeer = peer
	name := w.prefs.Alias(peer)
	if name == "" {
		name = w.t("chat.title")
	}
	address := peer.String()
	id := address
	if compact || gtx.Constraints.Max.X < max(gtx.Dp(fullChatHeaderIDMinWidth), gtx.Sp(fullChatHeaderIDMinWidth)) {
		id = address[:8] + "…" + address[len(address)-6:]
	}

	return layout.Inset{Bottom: chatHeaderInset}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				children := make([]layout.FlexChild, 0, 4)
				if compact {
					children = append(children, layout.Rigid(w.layoutChatBackButton))
				}
				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutReachableIndicator(gtx, w.snap.NodeStatus, peer)
					}),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						label := material.Label(w.theme, unit.Sp(15), name)
						label.Color = color.NRGBA{R: 200, G: 212, B: 228, A: 255}
						label.Font.Weight = font.Bold
						label.MaxLines = 1
						label.WrapPolicy = text.WrapGraphemes
						return label.Layout(directedByContent(gtx, name))
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutChatIDButton(gtx, id)
					}),
				)
				return layout.Flex{Alignment: layout.Middle, Gap: gtx.Dp(unit.Dp(6))}.Layout(gtx, children...)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutPendingDeletesCaption(gtx, peer)
			}),
		)
	})
}

func (w *Window) layoutChatBackButton(gtx layout.Context) layout.Dimensions {
	return w.kit().SquareIconButton(gtx, &w.compactBackBtn, w.chevronLeftIcon, w.t("chat.back"))
}

func (w *Window) layoutChatIDButton(gtx layout.Context, id string) layout.Dimensions {
	prefix := w.t("chat.identity_label")
	height := min(gtx.Dp(unit.Dp(ui.SquareIconButtonSideDp)), gtx.Constraints.Max.Y)
	gtx.Constraints.Min.Y = max(gtx.Constraints.Min.Y, height)
	return w.kit().ButtonFrame(gtx, &w.chatIdentityButton, ui.ButtonPrimary, unit.Dp(ui.ModalButtonRadiusDp), func(gtx layout.Context) layout.Dimensions {
		dims := layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Left: unit.Dp(8), Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Alignment: layout.Baseline, Gap: gtx.Dp(unit.Dp(5))}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Label(w.theme, unit.Sp(12), prefix)
						label.Color = color.NRGBA{R: 183, G: 197, B: 213, A: 255}
						label.MaxLines = 1
						return label.Layout(gtx)
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := w.chatAddressLabel(id)
						label.MaxLines = 1
						return label.Layout(leftToRight(gtx))
					}),
				)
			})
		})
		semantic.LabelOp(prefix + " " + id).Add(gtx.Ops)
		semantic.DescriptionOp(w.t("chat.show_contact")).Add(gtx.Ops)
		return dims
	})
}

func (w *Window) chatAddressLabel(address string) material.LabelStyle {
	label := material.Label(w.theme, unit.Sp(12), address)
	label.Font.Typeface = "Go Mono"
	label.Color = color.NRGBA{R: 154, G: 196, B: 235, A: 255}
	return label
}
