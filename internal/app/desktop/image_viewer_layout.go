package desktop

import (
	"image"
	"image/color"
	"runtime"
	"strconv"

	"gioui.org/f32"
	"gioui.org/font"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

// image_viewer_layout.go draws the viewer: screens 8a (desktop), 8b (phone),
// 8c (states) and 8d (delete confirmation) of the design.
//
// The two layouts are not variants of one arrangement, which is why they are
// two functions. A desktop window puts the arrows beside the picture and the
// zoom controls in the header; a phone has room for neither, steps with a
// swipe, and zooms only by pinch — so every row differs in what it holds,
// not merely in how wide it is.

const (
	// viewerInsetDp is the desktop gap between the window edge and the
	// viewer's own content, the same 6dp the console card keeps.
	viewerInsetDp = ui.ModalCardInsetDp
	viewerPadDp   = 10
	// viewerCompactPadDp and viewerCompactBottomDp are the phone layout's
	// padding. The bottom is deeper: it holds the row away from the system
	// navigation bar.
	viewerCompactPadDp    = 12
	viewerCompactBottomDp = 34

	// viewerButtonDp is the standard circle. The shared component
	// (ui.RoundIconButton) uses the same 44dp when it is not told a size, so
	// this is named only where a size has to be passed alongside another.
	viewerButtonDp    = 44
	viewerArrowIconDp = 24
	// viewerStripButtonDp is the smaller circle that steps the carousel on
	// the phone layout, where the arrows are not beside the picture.
	viewerStripButtonDp     = 36
	viewerStripButtonIconDp = 20

	viewerArrowGapDp = 12
	viewerHeaderGap  = 8
	// viewerCloseGapDp separates the close button from the zoom group, so
	// "one more step of zoom" and "put this away" are not neighbours.
	viewerCloseGapDp = 16

	viewerThumbWidthDp         = 48
	viewerThumbHeightDp        = 36
	viewerCompactThumbWidthDp  = 46
	viewerCompactThumbHeightDp = 34
	viewerThumbGapDp           = 6
	viewerThumbRadiusDp        = 5
	viewerImageRadiusDp        = 4

	viewerNameSizeSp  = 14
	viewerMetaSizeSp  = 12
	viewerStateSizeSp = 12

	// viewerConfirmWidthDp is the delete confirmation's width, and
	// viewerConfirmPadDp its inner padding — the same card the chat and
	// identity confirmations use (screen 3d), centred over the picture.
	viewerConfirmWidthDp  = 300
	viewerConfirmPadDp    = 7
	viewerConfirmRadiusDp = 8
)

// viewerMonoTypeface is the family the counter, the size and the zoom
// percentage are set in. Digits that change as the user steps or zooms jump
// around in a proportional face; the bundled Go collection carries Go Mono,
// and a name it did not carry would simply fall back to the theme's face.
const viewerMonoTypeface = font.Typeface("Go Mono")

// viewerBackdropColor is what the viewer puts between the picture and the
// application.
//
// Opaque, unlike every other modal backdrop here. The design's .88/.92 was
// tried and is wrong for this one surface: at a tenth of the way through, a
// bright message bubble and its white text stay legible behind the picture —
// the conversation reads as still being there, and a viewer whose whole job
// is to show one image ends up showing the chat through it. Every other
// modal keeps its translucency; they cover a card, not a photograph.
func viewerBackdropColor() color.NRGBA {
	return color.NRGBA{R: 0x06, G: 0x08, B: 0x0c, A: 255}
}

// viewerDeletePalettes are the destructive circle's two looks. The design
// (screen 8a) names the idle one — #2a1c1e on #4a3336 with an #e6746c
// glyph — and says the button is otherwise the same control as every other
// 44dp circle, so it lights up under the pointer the same way, along the same
// step the shared pair takes.
var viewerDeletePalettes = [2]ui.ModalCloseButtonPalette{
	{
		Fill:   color.NRGBA{R: 0x2a, G: 0x1c, B: 0x1e, A: 255},
		Border: color.NRGBA{R: 0x4a, G: 0x33, B: 0x36, A: 255},
		Icon:   color.NRGBA{R: 0xe6, G: 0x74, B: 0x6c, A: 255},
	},
	{
		Fill:   color.NRGBA{R: 0x43, G: 0x2a, B: 0x2d, A: 255},
		Border: color.NRGBA{R: 0x6b, G: 0x49, B: 0x4d, A: 255},
		Icon:   color.NRGBA{R: 0xff, G: 0x9a, B: 0x92, A: 255},
	},
}

// viewerButtonPalettes is the standard circle, shared with every modal's
// close button — the design draws them as one component (screen 7c).
func viewerButtonPalettes() (idle, hovered ui.ModalCloseButtonPalette) {
	return ui.ModalCloseButtonColors(ui.ModalCloseButtonIdle),
		ui.ModalCloseButtonColors(ui.ModalCloseButtonHighlighted)
}

func viewerNameColor() color.NRGBA  { return color.NRGBA{R: 0xf6, G: 0xf8, B: 0xfb, A: 255} }
func viewerMetaColor() color.NRGBA  { return color.NRGBA{R: 0x8b, G: 0x9e, B: 0xb7, A: 255} }
func viewerStateColor() color.NRGBA { return color.NRGBA{R: 0xc4, G: 0xcd, B: 0xda, A: 255} }
func viewerBrokenColor() color.NRGBA {
	return color.NRGBA{R: 0xc2, G: 0x56, B: 0x4f, A: 255}
}
func viewerLinkColor() color.NRGBA { return color.NRGBA{R: 0x56, G: 0x9c, B: 0xe7, A: 255} }

func viewerThumbFill() color.NRGBA   { return color.NRGBA{R: 0x18, G: 0x20, B: 0x2b, A: 255} }
func viewerThumbBorder() color.NRGBA { return color.NRGBA{R: 0x2c, G: 0x3a, B: 0x4c, A: 255} }
func viewerThumbActive() color.NRGBA { return color.NRGBA{R: 0x39, G: 0x62, B: 0xaa, A: 255} }

// layout draws the whole viewer over the window.
func (v *imageViewer) layout(gtx layout.Context) layout.Dimensions {
	// The list first: a message deleted here or at the peer can empty it, and
	// nothing below should draw an item that is gone.
	v.syncItems(gtx.Now)
	if !v.visible {
		return layout.Dimensions{}
	}
	if v.awaitingFile {
		// Something in the strip is still downloading. Nothing else on this
		// frame will ask for the next one, so the poll asks for it here.
		v.parent.scheduleTransferInvalidate(viewerItemsPollInterval)
	}
	v.claimFocus(gtx)
	v.readKeys(gtx)
	v.handleActions(gtx)

	compact := v.parent.isCompactLayout(gtx)
	// Recorded for the picture area, which is laid out with the constraints
	// of its own box and could only measure that box's width.
	v.compact = compact
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return v.layoutBackdrop(gtx)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min = gtx.Constraints.Max
			// Everything under an open confirmation is inert: it is still
			// drawn, because the question is about the picture behind it, but
			// it can neither be clicked nor reached with Tab.
			if v.confirmDelete {
				gtx = gtx.Disabled()
			}
			if compact {
				return v.layoutCompact(gtx)
			}
			return v.layoutWide(gtx)
		}),
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			if !v.confirmDelete {
				return layout.Dimensions{}
			}
			return v.layoutDeleteConfirm(gtx)
		}),
	)
}

// claimFocus moves the keyboard onto the viewer's close button on the frame
// after it opens.
//
// It has to move somewhere inside the viewer: Gio leaves focus where it was,
// which is the composer the viewer now covers — and that composer is laid
// out with input disabled, so what the user typed would go nowhere while
// Enter still reached a Send button they cannot see. The close button is the
// one control every state of the viewer draws.
func (v *imageViewer) claimFocus(gtx layout.Context) {
	if !v.focusPending {
		return
	}
	v.focusPending = false
	gtx.Execute(key.FocusCmd{Tag: &v.closeBtn})
}

// handleActions drains the viewer's controls. The confirmation takes the
// whole surface while it is up: nothing else may act on an image the user
// has been asked a destructive question about.
func (v *imageViewer) handleActions(gtx layout.Context) {
	if v.confirmDelete {
		// ONE answer per question, and the rest of the queue thrown away.
		// A double click delivers two clicks to the same frame: the first
		// deletes and moves the viewer to the next image, and the second
		// would delete THAT one — a picture nobody was asked about. Left in
		// the queue instead, it would fire the moment the confirmation is
		// opened again.
		if v.deleteYesBtn.Clicked(gtx) {
			drainClicks(gtx, &v.deleteYesBtn, &v.deleteNoBtn)
			v.confirmDeleteCurrent()
			return
		}
		if v.deleteNoBtn.Clicked(gtx) {
			drainClicks(gtx, &v.deleteYesBtn, &v.deleteNoBtn)
			v.confirmDelete = false
			v.parent.invalidate()
		}
		return
	}
	for v.closeBtn.Clicked(gtx) {
		v.parent.closeImageViewer()
	}
	for v.prevBtn.Clicked(gtx) {
		v.step(-1)
	}
	for v.nextBtn.Clicked(gtx) {
		v.step(1)
	}
	for v.carouselPrevBtn.Clicked(gtx) {
		v.step(-1)
	}
	for v.carouselNextBtn.Clicked(gtx) {
		v.step(1)
	}
	for v.zoomInBtn.Clicked(gtx) {
		v.zoomBy(1)
	}
	for v.zoomOutBtn.Clicked(gtx) {
		v.zoomBy(-1)
	}
	for v.downloadBtn.Clicked(gtx) {
		v.downloadCurrent()
	}
	for v.deleteBtn.Clicked(gtx) {
		v.requestDelete()
	}
	for v.externalBtn.Clicked(gtx) {
		if item, ok := v.current(); ok {
			go openFile(item.path)
		}
	}
	for i := range v.thumbBtns {
		if i >= len(v.items) {
			break
		}
		for v.thumbBtns[i].Clicked(gtx) {
			v.show(i)
		}
	}
}

// layoutBackdrop dims the window and swallows every press that is not on the
// viewer's own controls; a press beside the picture closes the viewer.
//
// The picture registers its own pointer area on top of this one, so a drag
// that pans a magnified image is not also a dismissal.
func (v *imageViewer) layoutBackdrop(gtx layout.Context) layout.Dimensions {
	ui.Fill(gtx, viewerBackdropColor())

	area := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, &v.dismissTag)
	area.Pop()

	for {
		ev, ok := gtx.Event(pointer.Filter{Target: &v.dismissTag, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			v.parent.closeImageViewer()
		}
	}
	return layout.Dimensions{Size: gtx.Constraints.Max}
}

// layoutWide is screen 8a: the header with the zoom group, the picture
// between two arrows, and the save / strip / delete row under it.
func (v *imageViewer) layoutWide(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(viewerInsetDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(viewerPadDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(v.layoutWideHeader),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return v.layoutStepButton(gtx, &v.prevBtn, -1, viewerButtonDp, viewerArrowIconDp)
						}),
						layout.Rigid(v.arrowGap),
						layout.Flexed(1, v.layoutPicture),
						layout.Rigid(v.arrowGap),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return v.layoutStepButton(gtx, &v.nextBtn, 1, viewerButtonDp, viewerArrowIconDp)
						}),
					)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Inset{Top: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return v.layoutActionRow(gtx, false)
					})
				}),
			)
		})
	})
}

// arrowGap is the air between an arrow and the picture. It stays even when
// the arrow is dimmed, because the arrow does.
func (v *imageViewer) arrowGap(gtx layout.Context) layout.Dimensions {
	if len(v.items) < 2 {
		return layout.Dimensions{}
	}
	return layout.Spacer{Width: unit.Dp(viewerArrowGapDp)}.Layout(gtx)
}

// layoutCompact is screen 8b: a close button, the picture, the strip with
// its own stepping buttons, the caption, and the save / delete row.
func (v *imageViewer) layoutCompact(gtx layout.Context) layout.Dimensions {
	inset := layout.Inset{
		Top:    unit.Dp(viewerCompactPadDp),
		Left:   unit.Dp(viewerCompactPadDp),
		Right:  unit.Dp(viewerCompactPadDp),
		Bottom: unit.Dp(viewerCompactBottomDp),
	}
	return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.E.Layout(gtx, v.layoutCloseButton)
			}),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{Bottom: unit.Dp(14)}.Layout(gtx, v.layoutPicture)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				if len(v.items) < 2 {
					return layout.Dimensions{}
				}
				return layout.Inset{Top: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return v.layoutStepButton(gtx, &v.carouselPrevBtn, -1, viewerStripButtonDp, viewerStripButtonIconDp)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(viewerThumbGapDp)}.Layout),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							// Centred, so a strip that fits sits between its
							// two buttons rather than packing against one of
							// them.
							return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return v.layoutCarousel(gtx, true)
							})
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(viewerThumbGapDp)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return v.layoutStepButton(gtx, &v.carouselNextBtn, 1, viewerStripButtonDp, viewerStripButtonIconDp)
						}),
					)
				})
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{Top: unit.Dp(16)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return v.layoutCaption(gtx, layout.Center, true)
				})
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{Top: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return v.layoutActionRow(gtx, true)
				})
			}),
		)
	})
}

// layoutWideHeader is the file name and its counter on the left, the zoom
// group and the close button on the right.
func (v *imageViewer) layoutWideHeader(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return v.layoutCaption(gtx, layout.W, false)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(viewerHeaderGap)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.layoutZoomButton(gtx, &v.zoomOutBtn, -1)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Left: unit.Dp(viewerHeaderGap), Right: unit.Dp(viewerHeaderGap)}.Layout(gtx,
				func(gtx layout.Context) layout.Dimensions {
					gtx.Constraints.Min.X = gtx.Dp(unit.Dp(38))
					label := v.monoLabel(viewerMetaSizeSp, v.zoomLabel(), viewerMetaColor())
					label.Alignment = text.Middle
					return label.Layout(gtx)
				})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.layoutZoomButton(gtx, &v.zoomInBtn, 1)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(viewerCloseGapDp)}.Layout),
		layout.Rigid(v.layoutCloseButton),
	)
}

// layoutCaption is the file name over its "2 / 5 · 840 kB · 1280×960" line.
// The dimensions are the file's own, and only the desktop layout has room
// for them.
func (v *imageViewer) layoutCaption(gtx layout.Context, align layout.Direction, compact bool) layout.Dimensions {
	item, ok := v.current()
	if !ok {
		return layout.Dimensions{}
	}
	return align.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Alignment: alignmentFor(align)}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Label(v.parent.theme, unit.Sp(viewerNameSizeSp), item.name)
				label.Color = viewerNameColor()
				label.MaxLines = 1
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := v.monoLabel(viewerMetaSizeSp, v.metaLine(item, compact), viewerMetaColor())
				label.MaxLines = 1
				return label.Layout(gtx)
			}),
		)
	})
}

func alignmentFor(align layout.Direction) layout.Alignment {
	if align == layout.Center {
		return layout.Middle
	}
	return layout.Start
}

// metaLine is the counter, the file size and — where there is room — the
// picture's own dimensions. The counter is left out for a single image:
// "1 / 1" tells the user nothing they cannot see.
func (v *imageViewer) metaLine(item viewerItem, compact bool) string {
	line := ""
	if len(v.items) > 1 {
		line = strconv.Itoa(v.index+1) + " / " + strconv.Itoa(len(v.items)) + " · "
	}
	line += formatFileSize(item.size)
	if compact {
		return line
	}
	if natural := v.naturalSize(item); natural.X > 0 && natural.Y > 0 {
		line += " · " + strconv.Itoa(natural.X) + "×" + strconv.Itoa(natural.Y)
	}
	return line
}

// naturalSize is the file's own pixel dimensions, from whichever cache has
// already decoded it. Zero until one of them has: the header is a caption,
// not a reason to read a file header on the layout path.
func (v *imageViewer) naturalSize(item viewerItem) image.Point {
	if entry := v.cache.lookup(item.path, v.parent.window).Entry; entry != nil {
		return entry.natural
	}
	if entry := v.parent.thumbCache.get(item.path, v.parent.window); entry != nil {
		return entry.natural
	}
	return image.Point{}
}

func (v *imageViewer) monoLabel(size unit.Sp, value string, fill color.NRGBA) material.LabelStyle {
	label := material.Label(v.parent.theme, size, value)
	label.Font.Typeface = viewerMonoTypeface
	label.Color = fill
	return label
}

// layoutPicture draws the image, or what stands in for it: the stretched
// thumbnail while the full bitmap is still being decoded, and the "cannot
// display" fallback when it never will be.
func (v *imageViewer) layoutPicture(gtx layout.Context) layout.Dimensions {
	gtx.Constraints.Min = gtx.Constraints.Max
	v.viewport = gtx.Constraints.Max
	size := gtx.Constraints.Max

	item, ok := v.current()
	if !ok {
		return layout.Dimensions{Size: size}
	}

	// Drawn first, because what the gestures may be aimed at is the picture
	// itself and its rectangle is only known once it has been placed. The
	// area beside a portrait picture is backdrop and behaves like it: a
	// press there closes the viewer instead of being swallowed by a drag
	// target with nothing under it.
	drawn, painted := v.paintCurrent(gtx, item)
	switch {
	case painted:
		// Exactly the picture. What is beside it is backdrop and closes the
		// viewer, which is what a press on empty space looks like it should
		// do — before this the whole box swallowed those presses, and a
		// portrait picture left two columns of dead space.
	case v.compact:
		// Nothing is drawn (a file still arriving, a picture that cannot be
		// decoded), and on a phone the swipe across this box is the only way
		// to leave it: there are no arrows over the picture there. So the
		// box keeps the gestures even with nothing in it.
		drawn = image.Rectangle{Max: size}
	default:
		return layout.Dimensions{Size: size}
	}
	area := clip.Rect(drawn).Push(gtx.Ops)
	event.Op(gtx.Ops, &v.imageTag)
	area.Pop()
	v.readImageGestures(gtx)
	return layout.Dimensions{Size: size}
}

// paintCurrent draws the current item — the picture, or the state that
// stands in for it — and reports the rectangle the picture occupies, if any.
//
// The rectangle is clipped to the box: a magnified picture runs past every
// edge, and a drag that leaves the box keeps panning through Gio's implicit
// grab rather than through an area nobody can see.
func (v *imageViewer) paintCurrent(gtx layout.Context, item viewerItem) (image.Rectangle, bool) {
	if item.path == "" {
		// The file is still on its way; there is nothing to decode yet and
		// the list re-resolves this item until there is.
		v.fit = image.Point{}
		v.layoutLoading(gtx)
		return image.Rectangle{}, false
	}

	full := v.cache.lookup(item.path, v.parent.window)
	if full.Entry != nil {
		return v.paintPicture(gtx, full.Entry), true
	}
	if !full.Pending {
		// Nothing is drawn, so nothing can be panned: a fit left over from
		// the previous image would let a drag move a picture that is not
		// there.
		v.fit = image.Point{}
		v.layoutUndisplayable(gtx)
		return image.Rectangle{}, false
	}
	// Decoding. The thumbnail the chat already holds is the same picture at
	// a lower resolution, so it stands in — a soft image is closer to the
	// truth than an empty rectangle, and it appears in the right place and
	// at the right aspect ratio, so nothing moves when the full one lands.
	if thumb := v.parent.thumbCache.get(item.path, v.parent.window); thumb != nil {
		return v.paintPicture(gtx, thumb), true
	}
	v.fit = image.Point{}
	v.layoutLoading(gtx)
	return image.Rectangle{}, false
}

// paintPicture draws one decoded bitmap at the current zoom and pan, and
// returns the rectangle it covers inside the viewport.
func (v *imageViewer) paintPicture(gtx layout.Context, entry *thumbnailEntry) image.Rectangle {
	natural := entry.natural
	if natural.X <= 0 || natural.Y <= 0 {
		natural = entry.bounds
	}
	v.fit = scaledSize(natural, v.viewport)
	display := v.displaySize()
	if display.X <= 0 || display.Y <= 0 {
		return image.Rectangle{}
	}
	// Re-clamped every frame, not only when the pan changes: a resized
	// window changes what "as far as it goes" means, and a pan left over
	// from the larger one would hold the picture off centre.
	v.offset = clampViewerOffset(v.offset, display, v.viewport)

	origin := image.Pt(
		(v.viewport.X-display.X)/2+v.offset.X,
		(v.viewport.Y-display.Y)/2+v.offset.Y,
	)
	destination := image.Rectangle{Min: origin, Max: origin.Add(display)}

	defer clip.Rect(image.Rectangle{Max: v.viewport}).Push(gtx.Ops).Pop()
	defer clip.UniformRRect(destination, gtx.Dp(unit.Dp(viewerImageRadiusDp))).Push(gtx.Ops).Pop()
	defer op.Offset(destination.Min).Push(gtx.Ops).Pop()
	defer op.Affine(f32.AffineId().Scale(f32.Point{}, f32.Pt(
		float32(display.X)/float32(entry.bounds.X),
		float32(display.Y)/float32(entry.bounds.Y),
	))).Push(gtx.Ops).Pop()
	entry.op.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	return destination.Intersect(image.Rectangle{Max: v.viewport})
}

// layoutLoading is the state where nothing has been decoded yet — a file
// still arriving, or a picture large enough that the decode is queued behind
// others.
func (v *imageViewer) layoutLoading(gtx layout.Context) layout.Dimensions {
	return v.layoutCentredState(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Alignment: layout.Middle}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.Icon(gtx, v.parent.hourglassIcon, unit.Dp(26), viewerMetaColor())
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return v.monoLabel(unit.Sp(11), v.parent.t("viewer.loading"), viewerMetaColor()).Layout(gtx)
			}),
		)
	})
}

// layoutUndisplayable is the fallback: a file this application cannot draw —
// not an image after all, or one whose decode failed. The external
// application, which used to be what a click on a preview did, is what is
// left here.
func (v *imageViewer) layoutUndisplayable(gtx layout.Context) layout.Dimensions {
	return v.layoutCentredState(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Alignment: layout.Middle}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return ui.Icon(gtx, v.parent.brokenImageIcon, unit.Dp(26), viewerBrokenColor())
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Label(v.parent.theme, unit.Sp(viewerStateSizeSp), v.parent.t("viewer.cannot_display"))
				label.Color = viewerStateColor()
				label.Alignment = text.Middle
				return label.Layout(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				// Android has no external viewer to hand the file to (gogio
				// ships no FileProvider — see open_android.go), so the link
				// there would be exactly the dead control this whole feature
				// replaced. The save button in the row below still works.
				if runtime.GOOS == "android" {
					return layout.Dimensions{}
				}
				return layout.Inset{Top: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return material.Clickable(gtx, &v.externalBtn, func(gtx layout.Context) layout.Dimensions {
						pointer.CursorPointer.Add(gtx.Ops)
						label := material.Label(v.parent.theme, unit.Sp(viewerStateSizeSp), v.parent.t("file.open_file"))
						label.Color = viewerLinkColor()
						return label.Layout(gtx)
					})
				})
			}),
		)
	})
}

func (v *imageViewer) layoutCentredState(gtx layout.Context, content layout.Widget) layout.Dimensions {
	size := gtx.Constraints.Max
	layout.Center.Layout(gtx, content)
	return layout.Dimensions{Size: size}
}

// layoutActionRow is the bottom row both layouts share: save on the left,
// delete on the right, and — on a desktop window — the thumbnail strip
// between them. The phone puts its strip higher, beside its own stepping
// buttons, so the row there is the two buttons at opposite ends.
func (v *imageViewer) layoutActionRow(gtx layout.Context, compact bool) layout.Dimensions {
	// Neither button has anything to work on while the file is still
	// arriving: there is nothing to save and nothing to delete. They dim
	// rather than disappear, like the arrows at the ends of the strip.
	item, ok := v.current()
	hasFile := ok && item.path != ""
	// Deleting is for files that arrived here. What this node holds for a
	// picture it SENT is the transmit blob the recipient is still served
	// from — shared between messages carrying the same content and
	// impossible to get back — so the button is inert on an outgoing image,
	// and the message menu's Delete is the way to take one back.
	canDelete := hasFile && !item.mine
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.layoutCircleButton(gtx, &v.downloadBtn, ui.RoundIconButton{
				Icon:    v.parent.downloadIcon,
				Hint:    v.parent.t("file.download"),
				Enabled: hasFile,
			})
		}),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			if compact {
				return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, 0)}
			}
			return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return v.layoutCarousel(gtx, false)
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.layoutCircleButton(gtx, &v.deleteBtn, ui.RoundIconButton{
				Icon:    v.parent.deleteIcon,
				Hint:    v.parent.t("viewer.delete"),
				Idle:    viewerDeletePalettes[0],
				Hovered: viewerDeletePalettes[1],
				Enabled: canDelete,
			})
		}),
	)
}

// layoutCarousel is the thumbnail strip. It appears from two images: a strip
// of one is a decoration.
//
// The tiles come from the thumbnail cache the chat has already filled, so
// opening the viewer decodes exactly one picture — the one being shown — and
// not the whole conversation.
func (v *imageViewer) layoutCarousel(gtx layout.Context, compact bool) layout.Dimensions {
	if len(v.items) < 2 {
		return layout.Dimensions{}
	}
	if v.carouselTarget >= 0 {
		// Applied here, before the list lays out: layout.List computes its
		// own position while it runs, and a scroll asked for from inside it
		// is overwritten by that computation.
		v.carousel.ScrollTo(v.carouselTarget)
		v.carouselTarget = -1
	}
	width := viewerThumbWidthDp
	height := viewerThumbHeightDp
	if compact {
		width = viewerCompactThumbWidthDp
		height = viewerCompactThumbHeightDp
	}
	stripWidth := len(v.items)*gtx.Dp(unit.Dp(width+viewerThumbGapDp)) - gtx.Dp(unit.Dp(viewerThumbGapDp))
	gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, stripWidth)
	gtx.Constraints.Min.X = gtx.Constraints.Max.X
	gtx.Constraints.Min.Y = gtx.Dp(unit.Dp(height))
	gtx.Constraints.Max.Y = gtx.Constraints.Min.Y
	return v.carousel.Layout(gtx, len(v.items), func(gtx layout.Context, index int) layout.Dimensions {
		tile := func(gtx layout.Context) layout.Dimensions {
			return v.layoutThumbTile(gtx, index, width, height)
		}
		if index == len(v.items)-1 {
			return tile(gtx)
		}
		return layout.Inset{Right: unit.Dp(viewerThumbGapDp)}.Layout(gtx, tile)
	})
}

func (v *imageViewer) layoutThumbTile(gtx layout.Context, index, width, height int) layout.Dimensions {
	size := image.Pt(gtx.Dp(unit.Dp(width)), gtx.Dp(unit.Dp(height)))
	gtx.Constraints = layout.Exact(size)
	active := index == v.index
	borderColor := viewerThumbBorder()
	borderWidth := 1
	if active {
		borderColor = viewerThumbActive()
		borderWidth = 2
	}
	item := v.items[index]
	return v.thumbBtns[index].Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		pointer.CursorPointer.Add(gtx.Ops)
		semantic.Button.Add(gtx.Ops)
		radius := gtx.Dp(unit.Dp(viewerThumbRadiusDp))
		bounds := image.Rectangle{Max: size}
		paint.FillShape(gtx.Ops, borderColor, clip.UniformRRect(bounds, radius).Op(gtx.Ops))
		inner := image.Rect(borderWidth, borderWidth, size.X-borderWidth, size.Y-borderWidth)
		paint.FillShape(gtx.Ops, viewerThumbFill(), clip.UniformRRect(inner, max(0, radius-borderWidth)).Op(gtx.Ops))

		entry := v.parent.thumbCache.get(item.path, v.parent.window)
		if entry == nil {
			return layout.Dimensions{Size: size}
		}
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			fitW, fitH := thumbnailDisplaySize(entry.bounds.X, entry.bounds.Y, inner.Dx(), inner.Dy())
			gtx.Constraints = layout.Exact(image.Pt(fitW, fitH))
			return widget.Image{Src: entry.op, Fit: widget.Contain, Position: layout.Center}.Layout(gtx)
		})
	})
}

// layoutCircleButton draws one of the viewer's circular controls through the
// shared component every modal close button uses (ui.RoundIconButton): the
// same geometry, the same two palettes, the same hover — which this viewer
// used to redraw by hand and, drawing it by hand, forgot.
func (v *imageViewer) layoutCircleButton(gtx layout.Context, button *widget.Clickable, opts ui.RoundIconButton) layout.Dimensions {
	if opts.Idle == (ui.ModalCloseButtonPalette{}) {
		opts.Idle, opts.Hovered = viewerButtonPalettes()
	}
	return v.parent.kit().RoundIconButton(gtx, button, opts)
}

func (v *imageViewer) layoutCloseButton(gtx layout.Context) layout.Dimensions {
	return v.parent.kit().ModalCloseButton(gtx, &v.closeBtn, v.parent.t("viewer.close"))
}

// layoutStepButton is an arrow. It is drawn only when there is more than one
// image, and dimmed at the end it cannot move past.
func (v *imageViewer) layoutStepButton(gtx layout.Context, button *widget.Clickable, delta, size, iconSize int) layout.Dimensions {
	if len(v.items) < 2 {
		return layout.Dimensions{}
	}
	icon := v.parent.chevronLeftIcon
	hint := v.parent.t("viewer.previous")
	if delta > 0 {
		icon = v.parent.chevronIcon
		hint = v.parent.t("viewer.next")
	}
	return v.layoutCircleButton(gtx, button, ui.RoundIconButton{
		Icon:    icon,
		Hint:    hint,
		SideDp:  size,
		IconDp:  iconSize,
		Enabled: v.canStep(delta),
	})
}

func (v *imageViewer) layoutZoomButton(gtx layout.Context, button *widget.Clickable, delta int) layout.Dimensions {
	icon := v.parent.zoomOutIcon
	hint := v.parent.t("viewer.zoom_out")
	if delta > 0 {
		icon = v.parent.zoomInIcon
		hint = v.parent.t("viewer.zoom_in")
	}
	return v.layoutCircleButton(gtx, button, ui.RoundIconButton{
		Icon:    icon,
		Hint:    hint,
		Enabled: viewerZoomStep(v.zoom, delta) != v.zoom,
	})
}

// layoutDeleteConfirm is screen 8d: a second scrim over the picture and the
// same confirmation card the chat's destructive actions use, centred.
//
// Its own backdrop closes the question and nothing else. Backing out of a
// deletion must not also put away the picture it was about — the user is
// answering "not this one", not "I am done here".
func (v *imageViewer) layoutDeleteConfirm(gtx layout.Context) layout.Dimensions {
	ui.Fill(gtx, color.NRGBA{R: 0x06, G: 0x08, B: 0x0c, A: 153})

	area := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, &v.confirmTag)
	area.Pop()
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: &v.confirmTag, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			v.confirmDelete = false
			v.parent.invalidate()
		}
	}

	item, ok := v.current()
	if !ok {
		return layout.Dimensions{Size: gtx.Constraints.Max}
	}
	layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		width := min(gtx.Dp(unit.Dp(viewerConfirmWidthDp)), gtx.Constraints.Max.X)
		gtx.Constraints.Min.X = width
		gtx.Constraints.Max.X = width
		gtx.Constraints.Min.Y = 0
		border := widget.Border{
			Color:        color.NRGBA{R: 0x48, G: 0x55, B: 0x6a, A: 255},
			CornerRadius: unit.Dp(viewerConfirmRadiusDp),
			Width:        unit.Dp(1),
		}
		// Recorded, so the card's own size is known before its presses are
		// caught: everything the card does not put a widget under — the
		// padding, the separator, the gap between the question and the
		// answers — is card, not backdrop, and pressing it must not dismiss
		// the question.
		macro := op.Record(gtx.Ops)
		dims := border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return ui.Filled(gtx, color.NRGBA{R: 0x1c, G: 0x22, B: 0x2c, A: 255}, unit.Dp(viewerConfirmRadiusDp),
				func(gtx layout.Context) layout.Dimensions {
					return layout.UniformInset(unit.Dp(viewerConfirmPadDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return v.layoutDeleteConfirmRows(gtx, item)
					})
				})
		})
		card := macro.Stop()
		ui.SwallowPresses(gtx, &v.confirmCardTag, dims.Size)
		card.Add(gtx.Ops)
		return dims
	})
	return layout.Dimensions{Size: gtx.Constraints.Max}
}

func (v *imageViewer) layoutDeleteConfirmRows(gtx layout.Context, item viewerItem) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Left: unit.Dp(12), Right: unit.Dp(12),
				Top: unit.Dp(8), Bottom: unit.Dp(4),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				label := material.Caption(v.parent.theme, item.name)
				label.Color = color.NRGBA{R: 140, G: 155, B: 178, A: 255}
				label.MaxLines = 1
				return label.Layout(gtx)
			})
		}),
		layout.Rigid(v.parent.contextMenuSeparator),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Left: unit.Dp(12), Right: unit.Dp(12),
				Top: unit.Dp(2), Bottom: unit.Dp(6),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				label := material.Caption(v.parent.theme, v.parent.t("viewer.delete_confirm"))
				label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
				return label.Layout(gtx)
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.parent.contextMenuItem(gtx, &v.deleteYesBtn, v.parent.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return v.parent.contextMenuItem(gtx, &v.deleteNoBtn, v.parent.t("context.delete_no"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}
