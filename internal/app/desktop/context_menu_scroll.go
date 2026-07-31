package desktop

import (
	"gioui.org/io/event"
	"gioui.org/layout"
)

// menuRow is one focusable context-menu row's vertical span, in the CONTENT
// coordinates of the menu card — the coordinate system the enclosing
// layout.List scrolls with Position.Offset.
type menuRow struct {
	tag      event.Tag
	top, bot int
}

// menuScroll makes keyboard focus scroll a context menu that does not fit.
//
// Both menus put their whole card into a layout.List as a SINGLE element, so
// the List clamps an overflowing menu to the room above the touch keyboard and
// lets the user drag the rest into view (see contextMenuCard). That is right
// for a finger and wrong for a keyboard: menuFocusState walks focus from row to
// row INSIDE that one element, and moving focus inside an element never moves
// the List — Position.First stays 0 and Position.Offset never changes, because
// nothing in Gio relates the two. Tab therefore walks straight off the bottom
// edge of the card and goes on walking rows the user cannot see, with Enter
// still landing on whichever invisible row focus reached. Delete sitting one
// row below the fold is activated blind.
//
// (Gio does NOT drop that focus, contrary to the obvious guess. keyQueue.inputOp
// sets state.visible for any handler the frame MENTIONS, and clipping is not
// consulted; the single element emits every row's event.Op on every frame
// whatever the scroll offset. So the failure is silent, not self-correcting.)
//
// The fix is to measure. Every Flex child of every menu sub-view goes through
// row, which records where each focusable row ended up, and the overlay then
// asks into to move the List so the row that keyboard focus just reached is
// inside the viewport. Nothing here runs for a pointer user: into is driven by
// menuFocusState.want, which is set only where drive issues a FocusCmd.
//
// The measurement cannot be taken from inside the element. List.Layout gives
// its element an unbounded main axis (constraints 0..inf) precisely so the
// element can report its natural size, so the viewport height has to be
// captured at the List.Layout CALL SITE and handed to begin.
type menuScroll struct {
	// rows are this frame's focusable spans, in layout order.
	rows []menuRow
	// view is the List viewport height captured by begin.
	view int
	// y is the running content offset: the top of the next Flex child.
	y int
	// ok records that the spans describe the card that was actually drawn —
	// see flex. Everything below refuses to scroll when it is false, because
	// scrolling by a number that does not describe the screen is worse than
	// not scrolling at all.
	ok bool
}

// begin starts one frame of measurement. Call it at the List.Layout CALL SITE,
// immediately before the call, passing the viewport height from there — both
// because that is the only place the height is knowable, and because a List
// whose viewport has collapsed to nothing lays its element out zero times, so a
// reset from inside the element would be a reset that sometimes does not happen.
//
// Resetting ok here is what makes a half-measured frame safe: a sub-view that
// returns before reaching flex (layoutAliasEditorMenu does exactly that when
// Enter submits the alias) leaves ok false, and last frame's spans — which
// belong to a card that is about to be replaced — can no longer be used.
func (s *menuScroll) begin(view int) {
	s.rows = s.rows[:0]
	s.view = view
	s.y = 0
	s.ok = false
}

// row wraps one Flex child of a menu sub-view, recording its span. tag is the
// focus target the row draws, or nil for a row that cannot hold focus — a
// header, a separator, a caption, a spacer, a disabled label.
//
// EVERY child of the Flex must come through here, including the ones with no
// tag. The spans are cumulative, so a child laid out behind this type's back
// shifts every row after it and the arithmetic silently describes a different
// menu. flex checks for exactly that.
func (s *menuScroll) row(tag event.Tag, w layout.Widget) layout.FlexChild {
	return layout.Rigid(func(gtx layout.Context) layout.Dimensions {
		dims := w(gtx)
		if tag != nil {
			s.rows = append(s.rows, menuRow{tag: tag, top: s.y, bot: s.y + dims.Size.Y})
		}
		s.y += dims.Size.Y
		return dims
	})
}

// flex lays the measured children out as the vertical Flex the menus have
// always been, and decides whether the measurement can be trusted.
//
// A vertical Flex of Rigid children stacks them in call order at cumulative
// offsets and reports their total as its own size — with one exception, a
// Min.Y larger than the content, which the default SpaceEnd spacing would
// absorb as leading space and shift every row. Inside a List element Min.Y is
// 0, so that cannot happen; and if some future caller made it happen, or added
// a Rigid without going through row, the total would stop matching the running
// offset. Comparing the two is therefore both checks at once, and failing it
// costs the keyboard user scrolling rather than correctness.
func (s *menuScroll) flex(gtx layout.Context, children ...layout.FlexChild) layout.Dimensions {
	dims := layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	s.ok = s.y == dims.Size.Y
	return dims
}

// menuScrollOffset returns the scroll offset that brings the span [top, bot)
// into a viewport of height view showing a total of total, starting from the
// current offset cur. All values are content pixels; the result is the List's
// Position.Offset, the distance from the content's leading edge to the
// viewport's.
//
// An already-visible span returns cur unchanged, so walking focus down a menu
// that fits scrolls nothing.
//
// The push-up test runs BEFORE the pull-down test on purpose. A row taller than
// the viewport satisfies neither on its own, and the order decides which end of
// it the user sees: bottom-first then top-second leaves the offset at top, so
// an oversized row shows its beginning. The alias editor grown by a long alias
// is the row that can do this.
func menuScrollOffset(cur, view, total, top, bot int) int {
	if bot > cur+view {
		cur = bot - view
	}
	if top < cur {
		cur = top
	}
	if limit := total - view; cur > limit {
		cur = limit
	}
	if cur < 0 {
		cur = 0
	}
	return cur
}

// into scrolls l so that tag's row is visible, and reports whether it moved the
// List. tag is the row keyboard focus was just placed on, or nil when this
// frame moved no focus.
//
// First is pinned to 0 rather than driven, because the whole menu is element 0
// and there is no other element to make first. That is also why List.ScrollTo
// is no use here: it scrolls to an ELEMENT, and this List has exactly one.
func (s *menuScroll) into(l *layout.List, tag event.Tag) bool {
	if !s.ok || tag == nil || s.view <= 0 || s.y <= s.view {
		return false
	}
	for _, r := range s.rows {
		if r.tag != tag {
			continue
		}
		off := menuScrollOffset(l.Position.Offset, s.view, s.y, r.top, r.bot)
		if off == l.Position.Offset && l.Position.First == 0 {
			return false
		}
		l.Position.First = 0
		l.Position.Offset = off
		return true
	}
	return false
}
