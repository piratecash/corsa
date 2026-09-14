package desktop

import (
	"image"
	"image/color"
	"time"

	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/updatecheck"
)

// console_modal_settings.go is the console's Settings tab: the choices that
// belong to this installation rather than to a peer or a message.
//
// The language selector lives here rather than in the window header. It is the
// same lookup it always was — a button naming the language in use, a card of
// options under it — moved off a header that was paying a permanent slot, on a
// phone the widest one, for a control a user touches about twice in the life of
// an installation. What did not move is the window-level machinery: the card
// now hangs inside the console modal, on the same Back/Escape ladder as the tab
// menu beside it.

const (
	settingsTitleSp   = 20
	settingsHeadingSp = 14
	settingsStatusSp  = 12
	// settingsSectionGapDp separates two sections; settingsBlockGapDp a
	// heading from what it introduces, and one control from the next.
	settingsSectionGapDp = 18
	settingsBlockGapDp   = 10
	// settingsTitleGapDp separates the tab title from the first section. It is
	// a constant rather than a literal because languageAnchor measures against
	// it — a gap changed in one place and not the other would hang the
	// language card away from the button it belongs to.
	settingsTitleGapDp   = 12
	settingsActionPadXDp = 12
	settingsActionPadYDp = 8
	settingsActionRadius = 5
)

func settingsHeadingColor() color.NRGBA {
	return color.NRGBA{R: 0xb0, G: 0xbb, B: 0xcd, A: 255}
}

func settingsStatusColor() color.NRGBA {
	return color.NRGBA{R: 0x9a, G: 0xa8, B: 0xbe, A: 255}
}

// handleSettingsActions drains the Settings tab's controls.
//
// It runs for every tab, not only the visible one, for the reason the tab strip
// does: a Clickable whose clicks nobody drains keeps them queued, and the
// language rows would then fire on whatever frame next asked.
func (c *consoleModal) handleSettingsActions(gtx layout.Context) {
	// Picking a language is the lookup's job done, so it closes with the pick.
	if c.parent.handleLanguageSelection(gtx) {
		c.languageMenuOpen = false
	}
	for c.languageMenuButton.Clicked(gtx) {
		c.languageMenuOpen = !c.languageMenuOpen
	}

	for c.updateCheckToggle.Clicked(gtx) {
		c.parent.setReleaseCheckEnabled(!c.parent.releaseCheckEnabled())
	}
	for c.updateCheckNow.Clicked(gtx) {
		c.parent.releaseCheckNow()
	}
}

func (c *consoleModal) layoutSettingsTab(gtx layout.Context) layout.Dimensions {
	ui.Fill(gtx, ui.PanelFill())

	// 8dp panel padding matching the main window cards, like every other tab.
	return layout.UniformInset(unit.Dp(8)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		// The column is recorded and replayed so the open lookup can be drawn
		// AFTER it: later ops paint on top and win the press, which is what
		// keeps the card over the sections instead of pushing them down. The
		// alternative — drawing it inside the scrolling list — is worse than it
		// sounds: a list clips its children, so the card would be cut off at
		// the bottom of the viewport.
		macro := op.Record(gtx.Ops)
		dims := c.layoutSettingsColumn(gtx)
		column := macro.Stop()
		column.Add(gtx.Ops)

		if c.languageMenuOpen {
			c.layoutLanguageMenu(gtx)
		}
		return dims
	})
}

func (c *consoleModal) layoutSettingsColumn(gtx layout.Context) layout.Dimensions {
	title := material.Label(c.theme(), unit.Sp(settingsTitleSp), c.parent.t("console.settings_title"))
	title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(title.Layout),
		layout.Rigid(layout.Spacer{Height: unit.Dp(settingsTitleGapDp)}.Layout),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			// One scrolling item holding every section, the shape the Donate
			// tab uses. The sections must not each be a list row: a list inside
			// a list scrolls two things under one finger.
			list := material.List(c.theme(), &c.settingsList)
			return list.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
				return c.layoutSettingsSections(gtx)
			})
		}),
	)
}

func (c *consoleModal) layoutSettingsSections(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(c.layoutLanguageSection),
		layout.Rigid(layout.Spacer{Height: unit.Dp(settingsSectionGapDp)}.Layout),
		layout.Rigid(c.layoutReleaseCheckSection),
	)
}

func (c *consoleModal) settingsHeading(gtx layout.Context, text string) layout.Dimensions {
	label := material.Label(c.theme(), unit.Sp(settingsHeadingSp), text)
	label.Color = settingsHeadingColor()
	label.Font.Weight = 600
	return label.Layout(gtx)
}

func (c *consoleModal) layoutLanguageSection(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.settingsHeading(gtx, c.parent.t("settings.language"))
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(settingsBlockGapDp)}.Layout),
		layout.Rigid(c.layoutLanguageButton),
	)
}

// layoutLanguageButton draws the lookup's closed state: the language in use,
// with a chevron. It is the same toolbar button the window header carried
// before the choice moved here — the control the user already knows — and it is
// sized to its content rather than stretched across the tab, because a row of
// six full-width options was what this replaced.
func (c *consoleModal) layoutLanguageButton(gtx layout.Context) layout.Dimensions {
	return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.X = 0
		dims := c.parent.kit().ToolbarButton(gtx, &c.languageMenuButton, ui.ToolbarButtonOpts{
			Label:    c.parent.currentLanguageRowLabel(),
			Icon:     c.parent.chevronDownIcon,
			IconSide: ui.IconTrailing,
			Active:   c.languageMenuOpen,
		})
		// Half of where the card hangs; languageAnchor computes the other half.
		c.languageButtonSize = dims.Size
		return dims
	})
}

// languageAnchor is the language button's rectangle in the Settings tab's own
// coordinates, which is where its card hangs from.
//
// Gio exposes no way to read a widget's absolute position, so the rectangle is
// reconstructed — but only from things this file lays out and only from things
// that are LABELS: the tab title, the section heading and the two gaps between
// them. Measuring a label costs a shaping pass and disturbs nothing, while
// measuring the button itself would drain its click queue (the trap
// ui.MenuPopupFitWidth documents). The button's size comes from the layout that
// has already happened this frame.
//
// The scroll offset is subtracted because the section rides inside the tab's
// list: without it the card would stay put while the button it belongs to
// scrolled away.
func (c *consoleModal) languageAnchor(gtx layout.Context) image.Rectangle {
	measure := gtx
	measure.Ops = new(op.Ops)
	measure.Constraints.Min = image.Point{}

	title := material.Label(c.theme(), unit.Sp(settingsTitleSp), c.parent.t("console.settings_title"))
	heading := material.Label(c.theme(), unit.Sp(settingsHeadingSp), c.parent.t("settings.language"))
	heading.Font.Weight = 600

	top := title.Layout(measure).Size.Y +
		gtx.Dp(unit.Dp(settingsTitleGapDp)) +
		heading.Layout(measure).Size.Y +
		gtx.Dp(unit.Dp(settingsBlockGapDp)) -
		c.settingsList.Position.Offset

	size := c.languageButtonSize
	return image.Rect(0, top, size.X, top+size.Y)
}

// layoutLanguageMenu draws the open lookup: a backdrop that catches the press
// meant to dismiss it, and the shared popup card hanging under the button.
//
// LEFT-aligned with the button, unlike the tab menu: this one sits at the left
// of its section rather than at the right end of a strip.
func (c *consoleModal) layoutLanguageMenu(gtx layout.Context) {
	kit := c.parent.kit()
	kit.MenuPopupBackdrop(gtx, &c.languageMenuDismissTag, ui.MenuPopupScrimDim, func() {
		c.languageMenuOpen = false
	})

	popup := ui.MenuPopup{
		Items:  c.parent.languageOptionRows(),
		Scroll: &c.languageMenuList,
	}
	anchor := c.languageAnchor(gtx)
	// At least as wide as the button it drops from — a card narrower than its
	// own trigger reads as belonging to something else.
	width := min(max(kit.MenuPopupFitWidth(gtx, popup), anchor.Dx()), gtx.Constraints.Max.X)

	x := ui.MenuPopupAnchorX(anchor.Min.X, width, gtx.Constraints.Max.X)
	y, height := languageMenuPlacement(gtx, anchor,
		kit.MenuPopupFitHeight(gtx, popup),
		gtx.Dp(unit.Dp(ui.MenuPopupAnchorGapDp)))

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	defer stack.Pop()

	cardGTX := gtx
	cardGTX.Constraints.Min.X = width
	cardGTX.Constraints.Max.X = width
	cardGTX.Constraints.Min.Y = 0
	cardGTX.Constraints.Max.Y = height
	dims := kit.MenuPopupCard(cardGTX, popup)

	// Where the card actually landed, for the test that holds it inside the
	// tab. It cannot be read back from the frame's semantics: a list reports
	// the UNCLIPPED bounds of rows it has scrolled out of view, so a button
	// below the fold looks like an overflowing one.
	c.languageMenuRect = image.Rectangle{Min: image.Pt(x, y), Max: image.Pt(x+dims.Size.X, y+dims.Size.Y)}
}

// languageMenuPlacement decides where the card goes and how tall it may be:
// under the button when there is room, above it when there is more room there,
// and clamped into the tab either way.
//
// wanted is the height the card would take if nothing stopped it; the returned
// height is a CAP, so a card that cannot have all of it hugs what it gets and
// scrolls the rest.
//
// The clamp is the point. The first cut placed the card under the button
// always, and when less than a usable row was left below it forced the height
// up to a floor WITHOUT moving the card — so the last options sat past the
// bottom edge of the tab, where scrolling INSIDE the card could not reach them.
// A menu that does not fit has to move, not grow: at 390x130 the button ends at
// y=120 and the card now flips above it instead of running to y=155.
func languageMenuPlacement(gtx layout.Context, anchor image.Rectangle, wanted, gap int) (y, height int) {
	area := gtx.Constraints.Max.Y
	below := area - (anchor.Max.Y + gap)
	above := anchor.Min.Y - gap

	// Below is the natural side and keeps it when it can hold the whole card,
	// or when it is simply the roomier of the two. Flipping for a few pixels
	// would make the card jump sides as the section scrolls.
	if below >= wanted || below >= above {
		height = below
		y = anchor.Max.Y + gap
	} else {
		height = above
		y = anchor.Min.Y - gap - min(wanted, above)
	}

	// Neither side can show even one row. Take the whole tab and scroll: a
	// sliver a few pixels tall shows nothing, and every option has to stay
	// reachable. menuMinUsableDp is a DECISION here, not a forced height —
	// forcing the height was the bug, because a card made taller without being
	// moved simply grows past the bottom edge.
	if height < gtx.Dp(unit.Dp(menuMinUsableDp)) {
		height = area
		y = 0
	}
	height = min(height, area)

	card := min(wanted, height)
	if y+card > area {
		y = area - card
	}
	if y < 0 {
		y = 0
	}
	return y, height
}

func (c *consoleModal) layoutReleaseCheckSection(gtx layout.Context) layout.Dimensions {
	enabled := c.parent.releaseCheckEnabled()

	children := []layout.FlexChild{
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.settingsHeading(gtx, c.parent.t("settings.updates"))
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(settingsBlockGapDp)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.parent.kit().Checkbox(gtx, &c.updateCheckToggle, ui.Checkbox{
				Label: c.parent.t("settings.check_github"),
				// The note is not decoration. This is the one request the
				// application makes outside its own transport, and the address
				// it comes from identifies the operator to a third party — the
				// user cannot weigh that against knowing about a release
				// unless it is written next to the switch.
				Note:    c.parent.t("settings.check_github_note"),
				Checked: enabled,
				Check:   c.parent.checkIcon,
			})
		}),
	}

	// The button and the status line belong to a check that is allowed to
	// happen AND can happen. Off, there is nothing to run and nothing to
	// report; with no checker (an unparsable running version — see
	// newReleaseChecker) a "Check now" button would do nothing and explain
	// nothing, because there would be no status line to explain it in.
	if enabled && c.parent.releaseChecker != nil {
		children = append(children,
			layout.Rigid(layout.Spacer{Height: unit.Dp(settingsBlockGapDp)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutSettingsAction(gtx, &c.updateCheckNow, c.parent.t("settings.check_now"))
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(settingsBlockGapDp)}.Layout),
			layout.Rigid(c.layoutReleaseCheckStatus),
		)
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

// layoutSettingsAction draws a chip that does something, sized to its label.
// A Rigid child of a vertical Flex is handed the container's cross-axis
// minimum, and a chip that took it would be as wide as the tab.
func (c *consoleModal) layoutSettingsAction(gtx layout.Context, button *widget.Clickable, text string) layout.Dimensions {
	return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.X = 0
		return c.parent.kit().Chip(gtx, button, ui.ChipFill(false), unit.Dp(settingsActionRadius), func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Top: unit.Dp(settingsActionPadYDp), Bottom: unit.Dp(settingsActionPadYDp),
				Left: unit.Dp(settingsActionPadXDp), Right: unit.Dp(settingsActionPadXDp),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				label := material.Label(c.theme(), unit.Sp(13), text)
				label.Color = color.NRGBA{R: 0xdc, G: 0xe4, B: 0xf0, A: 255}
				label.Font.Weight = 600
				label.MaxLines = 1
				return label.Layout(gtx)
			})
		})
	})
}

// layoutReleaseCheckStatus reports what the last check found.
//
// The header badge answers only "is there an update". A check that never
// completed and a check that failed both leave that badge dark, and without
// this line the two are indistinguishable from "you are up to date" — which is
// the one conclusion a failed check does not support.
func (c *consoleModal) layoutReleaseCheckStatus(gtx layout.Context) layout.Dimensions {
	lines := c.parent.releaseCheckStatusLines()
	children := make([]layout.FlexChild, 0, 2*len(lines))
	for index, line := range lines {
		if index > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout))
		}
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Label(c.theme(), unit.Sp(settingsStatusSp), line)
			label.Color = settingsStatusColor()
			return label.Layout(gtx)
		}))
	}
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

// releaseCheckStatusLines is the text of the status block, as data. Kept off
// the layout so a test can read what the tab says without a GPU.
func (w *Window) releaseCheckStatusLines() []string {
	if w.releaseChecker == nil {
		return nil
	}
	result, ok := w.releaseChecker.Latest()
	if !ok {
		return []string{w.t("settings.release_pending")}
	}

	lines := []string{releaseOutcomeLine(w, result)}
	if !result.CheckedAt.IsZero() {
		lines = append(lines, w.t("settings.release_checked_at", result.CheckedAt.Format(time.RFC3339)))
	}
	return lines
}

// releaseOutcomeLine maps the closed Outcome enum onto its sentence. A switch
// rather than a chain of conditions, so a new outcome is a compile-time
// omission here rather than a silently blank line.
func releaseOutcomeLine(w *Window, result updatecheck.Result) string {
	switch {
	case result.Outcome == updatecheck.OutcomeUpdateAvailable:
		return w.t("settings.release_available", result.Latest.String())
	case result.Outcome == updatecheck.OutcomeUpToDate:
		return w.t("settings.release_current", result.Latest.String())
	case result.Outcome == updatecheck.OutcomeFailed && result.Err != nil:
		// The reason is shown, not swallowed: "rate limit exceeded" and "no
		// route to host" ask the user for different things.
		return w.t("settings.release_failed", result.Err.Error())
	default:
		// Also catches a failed result carrying no error, which Result
		// forbids: there is nothing true to say about it beyond "no completed
		// answer", and an English "unknown" in a localised sentence would be
		// worse than saying that.
		return w.t("settings.release_pending")
	}
}
