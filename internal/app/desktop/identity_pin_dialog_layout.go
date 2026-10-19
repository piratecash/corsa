package desktop

import (
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

func (c *consoleModal) identityPinOpen() bool {
	return c.identityPinRemoval != nil && c.identityPinRemoval.visible
}
func (c *consoleModal) openIdentityPinDialog() {
	if c.identityPinRemoval == nil {
		c.identityPinRemoval = newIdentityPinDialog()
	}
	c.identityPinRemoval.open()
	c.languageMenuOpen, c.tabMenuOpen, c.focusPending = false, false, false
	c.parent.invalidate()
}
func (c *consoleModal) closeIdentityPinDialog() {
	if c.identityPinRemoval != nil {
		c.identityPinRemoval.close()
	}
	c.focusPending, c.pinReturnFocus = true, true
	c.parent.invalidate()
}
func (c *consoleModal) layoutIdentityPinSettings(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.settingsHeading(gtx, c.parent.t("settings.advanced"))
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutSettingsAction(gtx, &c.identityPinRemovalButton, c.parent.t("settings.identity_pin_action"))
		}),
	)
}
func (c *consoleModal) layoutIdentityPinOverlay(gtx layout.Context) layout.Dimensions {
	d := c.identityPinRemoval
	return c.parent.kit().Modal(gtx, ui.Modal{
		Title: c.parent.t("pin.title"), CloseHint: c.parent.t("pin.cancel"), Close: &d.closeButton,
		DismissTag: &d.dismissTag, Dismiss: c.closeIdentityPinDialog, CornerRadius: unit.Dp(ui.ModalCardRadiusDp),
		Sizing: ui.ModalSizingInset, Compact: c.parent.isCompactLayout(gtx), Content: keyboardTailOwner(&c.parent.touchKbd, c.layoutIdentityPinContent),
	})
}
func (c *consoleModal) layoutIdentityPinContent(gtx layout.Context) layout.Dimensions {
	d := c.identityPinRemoval
	c.handleEscape(gtx)
	if !d.visible {
		return layout.Dimensions{}
	}
	if d.focusPending {
		d.focusPending = false
		c.parent.touchKbd.noteExplicitEditorFocus()
		gtx.Execute(key.FocusCmd{Tag: &d.editor})
	}
	for {
		event, ok := d.editor.Update(gtx)
		if !ok {
			break
		}
		if _, submit := event.(widget.SubmitEvent); submit {
			d.requestPreview(c.parent)
		}
	}
	d.poll()
	for d.cancelButton.Clicked(gtx) {
		c.closeIdentityPinDialog()
	}
	if !d.visible {
		return layout.Dimensions{}
	}
	for d.previewButton.Clicked(gtx) {
		if d.canPreview() {
			d.requestPreview(c.parent)
		}
	}
	for d.acknowledgeButton.Clicked(gtx) {
		if d.canAcknowledge() {
			d.acknowledged = !d.acknowledged
		}
	}
	for d.confirmButton.Clicked(gtx) {
		d.requestConfirm(c.parent)
	}
	c.parent.touchKbd.trackEditorFocus(gtx, gtx.Focused(&d.editor))
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			list := material.List(c.theme(), &d.list)
			return list.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions { return c.layoutIdentityPinBody(gtx) })
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutIdentityPinAction(gtx, &d.previewButton, "pin.preview", d.canPreview())
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutIdentityPinAction(gtx, &d.confirmButton, "pin.confirm", d.canConfirm())
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutSettingsAction(gtx, &d.cancelButton, c.parent.t("pin.cancel"))
				}),
			)
		}),
	)
}
func (c *consoleModal) pinText(gtx layout.Context, text string) layout.Dimensions {
	label := material.Label(c.theme(), unit.Sp(13), text)
	label.Color = settingsHeadingColor()
	return label.Layout(gtx)
}
func (c *consoleModal) layoutIdentityPinBody(gtx layout.Context) layout.Dimensions {
	d := c.identityPinRemoval
	children := []layout.FlexChild{
		layout.Rigid(func(gtx layout.Context) layout.Dimensions { return c.pinText(gtx, c.parent.t("pin.intro")) }),
		layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions { return c.settingsHeading(gtx, c.parent.t("pin.identity")) }),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			control := gtx
			if d.phase == identityPinConfirming || d.phase == identityPinLocalPending || d.phase == identityPinRecoveryRequired {
				control = gtx.Disabled()
			}
			editor := material.Editor(c.theme(), &d.editor, c.parent.t("pin.identity_hint"))
			editor.TextSize = unit.Sp(14)
			return editor.Layout(control)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
	}
	if d.preview != nil {
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.pinText(gtx, c.parent.t("pin.exact", d.preview.identity.String()))
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.pinText(gtx, c.parent.t("pin.pinned_at", d.preview.pinnedAt.UTC().Format("2006-01-02 15:04:05 UTC")))
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				dims := c.pinText(gtx, c.parent.t("pin.warning_v2"))
				d.warningShown = true
				return dims
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				control := gtx
				if !d.canAcknowledge() {
					control = gtx.Disabled()
				}
				return c.parent.kit().Checkbox(control, &d.acknowledgeButton, ui.Checkbox{Label: c.parent.t("pin.ack"), Checked: d.acknowledged})
			}),
		)
	}
	children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
		return c.pinText(gtx, c.parent.t(identityPinStatusKey(d.phase)))
	}))
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}
func identityPinStatusKey(phase identityPinDialogPhase) string {
	switch phase {
	case identityPinDraft:
		return "pin.status_draft"
	case identityPinLoading:
		return "pin.status_loading"
	case identityPinPreviewReady:
		return "pin.status_ready"
	case identityPinUnavailable:
		return "pin.status_unavailable"
	case identityPinInvalidWarning:
		return "pin.status_warning"
	case identityPinConfirming:
		return "pin.status_confirming"
	case identityPinLocalPending:
		return "pin.status_pending"
	case identityPinProtectionRetained:
		return "pin.status_retained"
	case identityPinRecoveryRequired:
		return "pin.status_recovery"
	case identityPinInvalidIdentity:
		return "pin.status_identity"
	case identityPinStalePreview:
		return "pin.status_stale"
	case identityPinNotPinned:
		return "pin.status_not_pinned"
	default:
		return "pin.status_failed"
	}
}

func (c *consoleModal) layoutIdentityPinAction(gtx layout.Context, button *widget.Clickable, key string, enabled bool) layout.Dimensions {
	if !enabled {
		gtx = gtx.Disabled()
		opacity := paint.PushOpacity(gtx.Ops, ui.DimmedAlpha)
		defer opacity.Pop()
	}
	return c.layoutSettingsAction(gtx, button, c.parent.t(key))
}
