package desktop

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"sort"
	"sync/atomic"
	"time"

	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// fileDeleteTimeout caps how long the desktop waits for the delete
// dispatch round trip before giving up on the goroutine. The wire-side
// retry/ack flow runs independently in the DM router; this bound only
// covers the local SendMessageDelete call.
const fileDeleteTimeout = 10 * time.Second

// fileTabRefreshInterval is the polled redraw cadence used while any
// transfer is mid-flight or while any image thumbnail is still being
// decoded in the background. State-change events drive immediate
// invalidation; this timer fills in the gap between events when
// chunk progress is the only thing changing or when a thumb is
// pending in the shared cache (which pins its invalidate target to
// the window that first requested it — see thumbnailCache.lookup).
const fileTabRefreshInterval = 750 * time.Millisecond

// fileRowSelectables bundles the per-row selectable widgets so the
// user can click-drag and Cmd/Ctrl-C the filename, peer identity,
// and meta line (size + timestamp). Each widget.Selectable holds
// its own selection range; bundling them per FileID keeps the
// state aligned with the row's lifecycle (map entry pruned on
// transfer disappearance).
type fileRowSelectables struct {
	Name widget.Selectable
	Peer widget.Selectable
	Meta widget.Selectable
}

// fileTabThumbWidth is the fixed width of the image preview column.
// Every image row reserves the same horizontal slot so the file
// names align across rows regardless of image aspect ratio.
const fileTabThumbWidth = unit.Dp(56)

// fileTabThumbHeightMax caps the height so a tall portrait image
// does not stretch the row. The real height is the smaller of this
// cap and the aspect-correct scaled height (see thumbnailDisplaySize).
const fileTabThumbHeightMax = unit.Dp(72)

// layoutFileTab renders the File tab as a flat chronological list of
// transfers. Each row carries the peer identity inline (with a small
// reachability dot mirroring the identity-list visual language)
// rather than as a section header — the user requested this layout
// because peer info next to each filename is easier to scan than a
// per-peer header that scrolls off when the list is long.
//
// Per-row layout reuses the chat-thread layoutFileProgressBar
// where the visual is identical (sender progress, receiver
// progress + Cancel, Show-in-Folder + Open click handlers).
// Download / Restart / cancel-near-bar / disk-actions-left-aligned
// are file-tab-specific because the chat-thread variants are
// tuned for the narrower chat-bubble width and would float
// elements off-centre on a full-width row.
//
// The Delete button is direction-aware:
//
//   - Direction == "send": gated on peer online state because
//     SendMessageDelete dispatches a message_delete control DM
//     and waits for the peer's ack (pessimistic ordering). When
//     offline the button renders dimmed and the click is silently
//     drained — issuing a delete to an offline peer would burn the
//     entire retry budget waiting for an ack that cannot come.
//
//   - Direction == "receive": always enabled. The local-only
//     delete path inside DMRouter.SendMessageDelete removes the
//     local row and any backing on-disk file synchronously
//     without a wire round-trip, so peer reachability is
//     irrelevant for inbound rows.
func (c *ConsoleWindow) layoutFileTab(gtx layout.Context) layout.Dimensions {
	transfers := c.collectFileTransfers()
	c.pruneFileTabButtons(transfers)

	if hasActiveFileTransfer(transfers) || c.hasPendingThumbnail(transfers) {
		c.scheduleFileTabInvalidate()
	}

	bg := color.NRGBA{R: 21, G: 26, B: 34, A: 255}
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, bg)
		return layout.UniformInset(unit.Dp(18)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme, unit.Sp(20), c.parent.t("console.file_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			summary := material.Body1(c.theme, c.fileSummaryText(transfers))
			summary.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(summary.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(14)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					if len(transfers) == 0 {
						label := material.Body1(c.theme, c.parent.t("console.file_empty"))
						label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
						return label.Layout(gtx)
					}
					return material.List(c.theme, &c.fileList).Layout(gtx, len(transfers), func(gtx layout.Context, idx int) layout.Dimensions {
						return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							return c.layoutFileTransferRow(gtx, transfers[idx])
						})
					})
				}),
			)
		})
	})
}

// collectFileTransfers fetches the full transfer snapshot and sorts
// it by activity timestamp descending, breaking ties by FileID for
// stability (Go map iteration is non-deterministic; without the
// tie-break two same-second entries would shuffle between paints
// and look like flicker).
func (c *ConsoleWindow) collectFileTransfers() []filetransfer.TransferSnapshot {
	if c.parent == nil || c.parent.router == nil {
		return nil
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return nil
	}
	transfers := bridge.AllTransfers()
	sort.SliceStable(transfers, func(i, j int) bool {
		a, b := transferTimestampForSort(transfers[i]), transferTimestampForSort(transfers[j])
		if a != b {
			return a > b
		}
		return transfers[i].FileID > transfers[j].FileID
	})
	return transfers
}

// transferTimestampForSort returns the timestamp used to order rows.
// CompletedAt takes precedence when set because a completed download
// finished AFTER its CreatedAt; without this, terminal rows would
// freeze at their original announce time and shuffle around as new
// active rows appear.
func transferTimestampForSort(t filetransfer.TransferSnapshot) string {
	if t.CompletedAt != "" {
		return t.CompletedAt
	}
	return t.CreatedAt
}

// fileSummaryText reports total / active counts in the tab header.
// "Active" means anything not in a terminal state.
func (c *ConsoleWindow) fileSummaryText(transfers []filetransfer.TransferSnapshot) string {
	active := 0
	for _, t := range transfers {
		if !isTerminalTransferState(t.State) {
			active++
		}
	}
	return c.parent.t("console.file_summary", len(transfers), active)
}

// hasActiveFileTransfer reports whether any transfer is in a
// non-terminal state. Used to gate the polled redraw timer.
func hasActiveFileTransfer(transfers []filetransfer.TransferSnapshot) bool {
	for _, t := range transfers {
		if !isTerminalTransferState(t.State) {
			return true
		}
	}
	return false
}

// hasPendingThumbnail reports whether any image transfer's thumbnail
// is still being decoded. The cache pings its Invalidate target to
// the window that first requested a path, so when the chat thread
// asked first, our console window misses the wakeup — covered here
// by polling.
//
// Uses thumbnailCache.lookup so the (entry, pending) pair is read
// under one lock — the older get()+isPending() pair had a window
// where Pending→Ready between calls returned (nil, false) and
// dropped the polling gate prematurely (reviewer P3 race).
func (c *ConsoleWindow) hasPendingThumbnail(transfers []filetransfer.TransferSnapshot) bool {
	if c.parent == nil || c.window == nil {
		return false
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return false
	}
	for _, t := range transfers {
		if !isImageContentType(t.ContentType) {
			continue
		}
		path := bridge.FilePath(t.FileID, t.Direction == "send")
		if path == "" {
			continue
		}
		if c.parent.thumbCache.lookup(path, c.window).Pending {
			return true
		}
	}
	return false
}

// isTerminalTransferState mirrors the filetransfer.isSenderTerminal /
// isReceiverTerminal predicates by string state name.
func isTerminalTransferState(state string) bool {
	switch state {
	case "completed", "failed", "tombstone":
		return true
	default:
		return false
	}
}

// pruneFileTabButtons drops Clickable entries from every per-row map
// whose FileID no longer appears in the latest snapshot, so the
// maps cannot grow without bound.
func (c *ConsoleWindow) pruneFileTabButtons(transfers []filetransfer.TransferSnapshot) {
	live := make(map[domain.FileID]struct{}, len(transfers))
	for _, t := range transfers {
		live[t.FileID] = struct{}{}
	}
	for _, m := range []map[domain.FileID]*widget.Clickable{
		c.fileDeleteButtons,
		c.fileDownloadButtons,
		c.fileRestartButtons,
		c.fileThumbButtons,
	} {
		for id := range m {
			if _, ok := live[id]; !ok {
				delete(m, id)
			}
		}
	}
	for id := range c.fileRowSelectables {
		if _, ok := live[id]; !ok {
			delete(c.fileRowSelectables, id)
		}
	}
}

// fileRowSelectableSet looks up (or lazily creates) the selectable
// bundle for a given FileID. Caller MUST be on the UI goroutine —
// the map is not concurrency-safe.
func (c *ConsoleWindow) fileRowSelectableSet(fileID domain.FileID) *fileRowSelectables {
	sel, ok := c.fileRowSelectables[fileID]
	if ok {
		return sel
	}
	sel = &fileRowSelectables{}
	c.fileRowSelectables[fileID] = sel
	return sel
}

// fileRowButton looks up the Clickable for a (map, FileID), lazily
// creating an entry on first access. Single helper used by every
// per-row button accessor; callers MUST be on the UI goroutine.
func fileRowButton(m map[domain.FileID]*widget.Clickable, fileID domain.FileID) *widget.Clickable {
	btn, ok := m[fileID]
	if ok {
		return btn
	}
	btn = &widget.Clickable{}
	m[fileID] = btn
	return btn
}

func (c *ConsoleWindow) fileDeleteButton(id domain.FileID) *widget.Clickable {
	return fileRowButton(c.fileDeleteButtons, id)
}

func (c *ConsoleWindow) fileDownloadButton(id domain.FileID) *widget.Clickable {
	return fileRowButton(c.fileDownloadButtons, id)
}

func (c *ConsoleWindow) fileRestartButton(id domain.FileID) *widget.Clickable {
	return fileRowButton(c.fileRestartButtons, id)
}

func (c *ConsoleWindow) fileThumbButton(id domain.FileID) *widget.Clickable {
	return fileRowButton(c.fileThumbButtons, id)
}

// scheduleFileTabInvalidate coalesces redraw requests so the polled
// 750ms timer cannot stack up multiple goroutines.
func (c *ConsoleWindow) scheduleFileTabInvalidate() {
	if !atomic.CompareAndSwapInt32(&c.fileTabInvalidating, 0, 1) {
		return
	}
	go func() {
		time.Sleep(fileTabRefreshInterval)
		atomic.StoreInt32(&c.fileTabInvalidating, 0)
		c.invalidateWindow()
	}()
}

// layoutFileTransferRow renders one transfer card. Layout:
//
//	┌─ thumb ─┬─ content (Flexed) ───────────┬─ actions (rigid) ─┐
//	│         │ ↓ filename                    │ [ state badge ]   │
//	│         │ ●  peer-id-truncated          │ [   Delete    ]   │
//	│         │ 1.2 MB / 4.8 MB  (50%) · …    │                   │
//	│         │ [ progress / Download / … ]    │                   │
//	│         │ downloaded                    │                   │
//	│         │ [Show in Folder] [Open]       │                   │
//	└─────────┴───────────────────────────────┴───────────────────┘
//
// Delete sits directly under the state badge in a dedicated right
// column rather than on its own row at the bottom — saves a line
// of vertical space and keeps the destructive action visually
// anchored to the row's primary status indicator.
//
// The body builders are shared with the chat-thread file card
// (layoutFileProgressBar, layoutReceiverProgress, layoutReachableIndicator,
// the Open / Show in folder click handlers) so the two surfaces
// stay visually identical for the same transfer state.
func (c *ConsoleWindow) layoutFileTransferRow(gtx layout.Context, t filetransfer.TransferSnapshot) layout.Dimensions {
	cardBg := color.NRGBA{R: 28, G: 35, B: 47, A: 255}
	border := color.NRGBA{R: 60, G: 76, B: 100, A: 255}
	textPrimary := color.NRGBA{R: 235, G: 240, B: 250, A: 255}
	textSecondary := color.NRGBA{R: 168, G: 182, B: 204, A: 255}

	peerOnline := c.parent.peerOnline(t.Peer)

	contentColumn := func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowHeader(gtx, t, textPrimary)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowPeerLine(gtx, t, textSecondary)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				sel := c.fileRowSelectableSet(t.FileID)
				return c.layoutSelectableText(gtx, &sel.Meta, c.fileRowMetaLine(t), textSecondary)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowProgressOrButton(gtx, t, peerOnline)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowStateLabel(gtx, t)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowDiskActions(gtx, t)
			}),
		)
	}

	// actionColumnInner expects its parent to feed in finite Min.Y =
	// Max.Y constraints (see body below), so the Flexed(1) spacers
	// distribute real space and the Delete button lands at the
	// vertical centre between the badge and the column bottom.
	//
	// Without that finite-Y precondition (e.g. inside a default
	// Horizontal Flex which leaves Max.Y = parent's max), Flexed
	// vertical slots expand to the LIST's full height — which is
	// how an earlier version stretched each row to the bottom of
	// the tab.
	actionColumnInner := func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical, Alignment: layout.End}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileStateBadge(gtx, t)
			}),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Dimensions{Size: image.Pt(gtx.Constraints.Min.X, gtx.Constraints.Min.Y)}
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.layoutFileRowDeleteAction(gtx, t, peerOnline)
			}),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Dimensions{Size: image.Pt(gtx.Constraints.Min.X, gtx.Constraints.Min.Y)}
			}),
		)
	}

	// fileRowNarrowWideThreshold is the minimum row width below
	// which the side-by-side layout breaks down — the action
	// column's badge + Delete pill need at least ~140dp, the
	// content column needs at least ~80dp for legible filenames,
	// plus a 10dp spacer between. Below this we stack vertically.
	const fileRowNarrowWideThreshold = unit.Dp(230)

	// body lays out content + action either side-by-side (the
	// common case) or vertically stacked (narrow rows / very narrow
	// console windows). The side-by-side path uses a two-pass
	// op.Record measurement so the action column's Y constraints
	// match the content column's actual height — that's what makes
	// the Flexed(1) spacers inside actionColumnInner correctly
	// vertical-centre the Delete button. Without matched Y
	// constraints the spacers expand to the list's full height
	// (rows would stretch to the bottom of the tab).
	//
	// The narrow path is a plain Vertical Flex: content first, a
	// small gap, then the action column (without centring) sized
	// naturally. This keeps the badge + Delete inside the row's
	// horizontal bounds when the tab is too tight to host both
	// columns side by side.
	body := func(gtx layout.Context) layout.Dimensions {
		spacerW := gtx.Dp(unit.Dp(10))
		actionMaxW := gtx.Dp(unit.Dp(140))
		threshold := gtx.Dp(fileRowNarrowWideThreshold)

		if gtx.Constraints.Max.X < threshold {
			// Narrow row: stack vertically. action column inner
			// uses Flexed spacers, which would expand to the full
			// list height in a vertical context — substitute the
			// rigid badge + gap + delete arrangement instead.
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(contentColumn),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Spacing: layout.SpaceStart}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return c.layoutFileStateBadge(gtx, t)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return c.layoutFileRowDeleteAction(gtx, t, peerOnline)
						}),
					)
				}),
			)
		}

		// Wide path: side-by-side with vertical-centred Delete.
		contentMaxX := gtx.Constraints.Max.X - spacerW - actionMaxW
		// Pass 1: lay out (and record) content with the bounded
		// width. Reserve action's full width unconditionally — we
		// already gated on threshold above, so contentMaxX is at
		// least 80dp.
		contentGtx := gtx
		contentGtx.Constraints.Max.X = contentMaxX
		contentGtx.Constraints.Min.X = 0
		contentGtx.Constraints.Min.Y = 0
		contentMacro := op.Record(gtx.Ops)
		contentDims := contentColumn(contentGtx)
		contentCall := contentMacro.Stop()

		// Pass 2: lay out (and record) action with Min.Y = Max.Y =
		// content height. Flexed(1) spacers inside actionColumnInner
		// then split the gap between badge and column-bottom
		// evenly, vertical-centring the Delete button.
		actionGtx := gtx
		actionGtx.Constraints.Min.X = 0
		actionGtx.Constraints.Max.X = actionMaxW
		actionGtx.Constraints.Min.Y = contentDims.Size.Y
		actionGtx.Constraints.Max.Y = contentDims.Size.Y
		actionMacro := op.Record(gtx.Ops)
		actionDims := actionColumnInner(actionGtx)
		actionCall := actionMacro.Stop()

		// Render: content at origin, action shifted right.
		contentCall.Add(gtx.Ops)
		actionX := contentDims.Size.X + spacerW
		// Right-align the action column when there's slack so the
		// badge / Delete sit at the row's right edge regardless of
		// how short the content column ended up being.
		if rightEdge := gtx.Constraints.Max.X - actionDims.Size.X; rightEdge > actionX {
			actionX = rightEdge
		}
		trans := op.Offset(image.Pt(actionX, 0)).Push(gtx.Ops)
		actionCall.Add(gtx.Ops)
		trans.Pop()

		totalW := actionX + actionDims.Size.X
		if totalW < gtx.Constraints.Min.X {
			totalW = gtx.Constraints.Min.X
		}
		return layout.Dimensions{Size: image.Pt(totalW, contentDims.Size.Y)}
	}

	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			fill(gtx, border)
			return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				fill(gtx, cardBg)
				return layout.Dimensions{Size: gtx.Constraints.Min}
			})
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				thumb := c.layoutFileRowThumbnail(gtx, t)
				if thumb == nil {
					return body(gtx)
				}
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Start}.Layout(gtx,
					layout.Rigid(thumb),
					layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
					layout.Flexed(1, body),
				)
			})
		}),
	)
}

// layoutFileRowThumbnail returns a layout function rendering the
// fixed-width image preview, or nil when the transfer is not an
// image / has no on-disk path / decode is not yet ready.
//
// Uses thumbnailCache.lookup so the cache state read happens under
// one lock — fixes the get()+isPending() race where a Pending→Ready
// transition between the two calls dropped the polling gate.
func (c *ConsoleWindow) layoutFileRowThumbnail(gtx layout.Context, t filetransfer.TransferSnapshot) layout.Widget {
	if !isImageContentType(t.ContentType) || c.parent == nil {
		return nil
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return nil
	}
	path := bridge.FilePath(t.FileID, t.Direction == "send")
	if path == "" {
		return nil
	}
	res := c.parent.thumbCache.lookup(path, c.window)
	if res.Entry == nil {
		return nil
	}
	imgOp := res.Entry.op
	imgBounds := res.Entry.bounds
	btn := c.fileThumbButton(t.FileID)
	for btn.Clicked(gtx) {
		go openFile(path)
	}
	return func(gtx layout.Context) layout.Dimensions {
		w := gtx.Dp(fileTabThumbWidth)
		hMax := gtx.Dp(fileTabThumbHeightMax)
		dispW, dispH := thumbnailDisplaySize(imgBounds.X, imgBounds.Y, w, hMax)
		if dispW < w && dispW > 0 {
			dispH = dispH * w / dispW
			if dispH > hMax {
				dispH = hMax
			}
			dispW = w
		}
		size := image.Pt(dispW, dispH)
		return btn.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			pointer.CursorPointer.Add(gtx.Ops)
			defer clip.UniformRRect(image.Rectangle{Max: size}, gtx.Dp(unit.Dp(4))).Push(gtx.Ops).Pop()
			imgWidget := widget.Image{
				Src:      imgOp,
				Fit:      widget.ScaleDown,
				Position: layout.Center,
			}
			gtx.Constraints = layout.Exact(size)
			return imgWidget.Layout(gtx)
		})
	}
}

// layoutFileRowHeader: [arrow] [filename ........]
//
// The state pill used to live on this line (right-aligned), but has
// moved to the row's dedicated action column where it sits directly
// above the Delete button — see layoutFileTransferRow's
// actionColumn. Keeping the header to "arrow + filename" only lets
// the filename grow into the freed horizontal space.
//
// The filename is rendered via widget.Selectable so the user can
// click-drag to highlight and Cmd/Ctrl-C the text. The peer ID
// and meta line are similarly selectable — see layoutFileRowPeerLine
// and the meta-line builder in layoutFileTransferRow.
func (c *ConsoleWindow) layoutFileRowHeader(gtx layout.Context, t filetransfer.TransferSnapshot, fg color.NRGBA) layout.Dimensions {
	sel := c.fileRowSelectableSet(t.FileID)
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			arrow := material.Body2(c.theme, c.directionIcon(t.Direction))
			arrow.Color = c.directionColor(t.Direction)
			arrow.Font.Weight = 700
			return arrow.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return c.layoutSelectableText(gtx, &sel.Name, fileDisplayName(t), fg)
		}),
	)
}

// layoutFileRowPeerLine renders the per-row peer info: a small
// reachability dot (same widget the chat sidebar uses for
// identities) followed by the truncated peer identity. The dot
// gives at-a-glance online/offline status without needing a group
// header — the user-requested layout.
//
// The peer identity text is selectable so the user can copy it.
// We render the truncated form in the row but selection returns
// the full string to the clipboard via SetText below — actually
// SetText copies what it's given, so we feed the truncated form
// (matching what the user sees). For copying the full ID the user
// can right-click the chat sidebar identity row instead.
func (c *ConsoleWindow) layoutFileRowPeerLine(gtx layout.Context, t filetransfer.TransferSnapshot, fg color.NRGBA) layout.Dimensions {
	status := c.parent.router.Snapshot().NodeStatus
	sel := c.fileRowSelectableSet(t.FileID)
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.parent.layoutReachableIndicator(gtx, status, t.Peer)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutSelectableText(gtx, &sel.Peer, string(t.Peer), fg)
		}),
	)
}

// layoutFileStateBadge renders a rounded pill via op.Record so the
// background paint exactly matches the label dimensions instead of
// stretching to fill the parent's max constraint (the previous
// Stack.Expanded approach did the latter and looked like broken
// layout when the pill landed in a Flexed slot).
//
// "completed" is split by direction so the user reads "Uploaded"
// on a sent file and "Downloaded" on a received one — direction
// disambiguates a single-state-per-side concept.
func (c *ConsoleWindow) layoutFileStateBadge(gtx layout.Context, t filetransfer.TransferSnapshot) layout.Dimensions {
	bg, fg := fileStateBadgeColors(t.State)
	inset := layout.Inset{Top: unit.Dp(3), Bottom: unit.Dp(3), Left: unit.Dp(8), Right: unit.Dp(8)}
	macro := op.Record(gtx.Ops)
	dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		lbl := material.Caption(c.theme, c.parent.t(stateBadgeKey(t)))
		lbl.Color = fg
		lbl.Font.Weight = 600
		return lbl.Layout(gtx)
	})
	call := macro.Stop()
	defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(8))).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: bg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	call.Add(gtx.Ops)
	return dims
}

// stateBadgeKey selects the i18n key for the state pill. "completed"
// splits by direction (Uploaded vs Downloaded); other states pass
// through.
func stateBadgeKey(t filetransfer.TransferSnapshot) string {
	if t.State == "completed" {
		if t.Direction == "send" {
			return "console.file.state.uploaded"
		}
		return "console.file.state.downloaded"
	}
	return "console.file.state." + t.State
}

// layoutFileRowProgressOrButton dispatches to the right widget
// based on (Direction, State, peerOnline). The progress-bar widgets
// are reused from the chat thread; Download / Restart use file-tab
// owned Clickables so the click handler can defer
// c.invalidateWindow() — without that, a Restart click on a terminal
// "failed" row leaves the file tab showing the old state until some
// unrelated event triggers a redraw.
func (c *ConsoleWindow) layoutFileRowProgressOrButton(gtx layout.Context, t filetransfer.TransferSnapshot, peerOnline bool) layout.Dimensions {
	progressBg := color.NRGBA{R: 50, G: 60, B: 80, A: 255}
	progressFg := color.NRGBA{R: 72, G: 150, B: 255, A: 255}
	percent := transferProgressPercent(t)

	switch t.Direction {
	case "send":
		switch t.State {
		case "announced", "serving":
			return c.parent.layoutFileProgressBar(gtx, progressBg, progressFg, percent)
		}
		return layout.Dimensions{}
	case "receive":
		switch t.State {
		case "downloading", "verifying":
			// Active download: progress bar + cancel ✕ packed
			// tight on the left. Cancel works regardless of
			// peerOnline — the receiver is just resetting its own
			// local mapping; no wire round-trip.
			return c.layoutFileTabReceiverProgress(gtx, progressBg, progressFg, percent, t.FileID)
		case "failed":
			if !peerOnline {
				return layout.Dimensions{}
			}
			return c.layoutFileTabRestartButton(gtx, t.FileID)
		case "available", "waiting_route":
			if !peerOnline {
				return layout.Dimensions{}
			}
			return c.layoutFileTabDownloadButton(gtx, t.FileID)
		}
	}
	return layout.Dimensions{}
}

// layoutFileTabReceiverProgress is the file-tab variant of the
// chat-thread layoutReceiverProgress.
//
// Why a separate variant: the chat-thread version uses
// `Spacing: layout.SpaceBetween` with `Flexed(1)` for the bar.
// `layoutFileProgressBar` internally clamps at 260dp max width, so
// inside a Flexed slot wider than 260dp the bar fills the LEFT
// 260dp of its slot and the slot still consumes the rest, pushing
// the cancel button to the far right of the row — visually
// disconnected from the bar. (Inside a chat bubble the row is
// roughly 260dp wide, so this never showed there.)
//
// Here we use Rigid for both children so the bar takes its natural
// 260dp and the cancel button sits 8dp to its right, visually
// anchored to the progress.
//
// The cancel-button click handler is reused from the chat-thread
// w.fileCancelDownloadBtns map so a click in either surface
// resolves to the same FileBridge.CancelDownload call. The map is
// lazily initialised here defensively in case the file tab paints
// before the chat thread.
func (c *ConsoleWindow) layoutFileTabReceiverProgress(gtx layout.Context, bg, fg color.NRGBA, percent int, fileID domain.FileID) layout.Dimensions {
	messageID := string(fileID)
	if c.parent.fileCancelDownloadBtns == nil {
		c.parent.fileCancelDownloadBtns = make(map[string]*widget.Clickable)
	}
	cancelBtn, ok := c.parent.fileCancelDownloadBtns[messageID]
	if !ok {
		cancelBtn = new(widget.Clickable)
		c.parent.fileCancelDownloadBtns[messageID] = cancelBtn
	}
	for cancelBtn.Clicked(gtx) {
		go func() {
			defer c.invalidateWindow()
			if err := c.parent.router.FileBridge().CancelDownload(fileID); err != nil {
				_ = err
			}
		}()
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.parent.layoutFileProgressBar(gtx, bg, fg, percent)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			matBtn := material.Button(c.theme, cancelBtn, "✕")
			matBtn.Background = color.NRGBA{R: 65, G: 70, B: 85, A: 255}
			matBtn.Color = color.NRGBA{R: 220, G: 220, B: 220, A: 255}
			matBtn.Inset = layout.UniformInset(unit.Dp(2))
			matBtn.CornerRadius = unit.Dp(10)
			matBtn.TextSize = unit.Sp(12)
			gtx.Constraints.Min = image.Pt(gtx.Dp(unit.Dp(24)), gtx.Dp(unit.Dp(24)))
			gtx.Constraints.Max = gtx.Constraints.Min
			return matBtn.Layout(gtx)
		}),
	)
}

// layoutFileTabDownloadButton mirrors the chat-thread
// layoutFileDownloadButton visual but routes the click through a
// file-tab-owned Clickable so the goroutine can defer
// c.invalidateWindow(). Without the invalidate, the click triggers
// one frame, the goroutine completes asynchronously, and no further
// frame fires until something unrelated happens — leaving the row
// stuck on "Available + Download".
func (c *ConsoleWindow) layoutFileTabDownloadButton(gtx layout.Context, fileID domain.FileID) layout.Dimensions {
	btn := c.fileDownloadButton(fileID)
	for btn.Clicked(gtx) {
		c.dispatchFileTabDownloadAsync(fileID)
	}
	matBtn := material.Button(c.theme, btn, c.parent.t("file.download"))
	matBtn.Background = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	matBtn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	matBtn.Inset = layout.Inset{
		Top: unit.Dp(4), Bottom: unit.Dp(4),
		Left: unit.Dp(12), Right: unit.Dp(12),
	}
	matBtn.CornerRadius = unit.Dp(6)
	matBtn.TextSize = unit.Sp(13)
	return layout.Flex{}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.X = gtx.Dp(unit.Dp(100))
			return matBtn.Layout(gtx)
		}),
	)
}

// layoutFileTabRestartButton is the file-tab counterpart to
// chat-thread layoutFileRestartButton — same visual, but with
// invalidate-after-click so the row updates from "Failed + Restart"
// → "Available" / "Downloading" without waiting for an unrelated
// repaint event.
func (c *ConsoleWindow) layoutFileTabRestartButton(gtx layout.Context, fileID domain.FileID) layout.Dimensions {
	btn := c.fileRestartButton(fileID)
	for btn.Clicked(gtx) {
		c.dispatchFileTabRestartAsync(fileID)
	}
	matBtn := material.Button(c.theme, btn, c.parent.t("file.restart"))
	matBtn.Background = color.NRGBA{R: 70, G: 90, B: 130, A: 255}
	matBtn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	matBtn.Inset = layout.Inset{
		Top: unit.Dp(4), Bottom: unit.Dp(4),
		Left: unit.Dp(12), Right: unit.Dp(12),
	}
	matBtn.CornerRadius = unit.Dp(6)
	matBtn.TextSize = unit.Sp(13)
	return layout.Flex{}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.X = gtx.Dp(unit.Dp(100))
			return matBtn.Layout(gtx)
		}),
	)
}

// dispatchFileTabDownloadAsync runs StartDownload off the UI
// goroutine and invalidates the console window when it settles so
// the row picks up the new state on the next frame.
func (c *ConsoleWindow) dispatchFileTabDownloadAsync(fileID domain.FileID) {
	if c.parent == nil || c.parent.router == nil {
		return
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return
	}
	go func() {
		defer c.invalidateWindow()
		if err := bridge.StartDownload(fileID); err != nil {
			// Errors are intentionally swallowed: the row will stay
			// in the original state and the user can retry. We do
			// not surface a status string because the chat-thread
			// counterpart doesn't either, and we want parity.
			_ = err
		}
	}()
}

// dispatchFileTabRestartAsync runs RestartDownload + StartDownload
// off the UI goroutine and invalidates after the pair settles.
// Mirrors the chat-thread layoutFileRestartButton goroutine.
func (c *ConsoleWindow) dispatchFileTabRestartAsync(fileID domain.FileID) {
	if c.parent == nil || c.parent.router == nil {
		return
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return
	}
	go func() {
		defer c.invalidateWindow()
		if err := bridge.RestartDownload(fileID); err != nil {
			_ = err
			return
		}
		if err := bridge.StartDownload(fileID); err != nil {
			_ = err
		}
	}()
}

// layoutFileRowStateLabel renders the chat-thread style status line
// ("downloaded" / "sender offline" / "confirming…" / "failed" /
// "unavailable"). Returns zero dims when the state has no
// informational caption.
func (c *ConsoleWindow) layoutFileRowStateLabel(gtx layout.Context, t filetransfer.TransferSnapshot) layout.Dimensions {
	labelKey, fg := fileRowStateLabel(t)
	if labelKey == "" {
		return layout.Dimensions{}
	}
	return layout.Inset{Top: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		lbl := material.Caption(c.theme, c.parent.t(labelKey))
		lbl.Color = fg
		return lbl.Layout(gtx)
	})
}

// fileRowStateLabel returns the (i18n key, color) pair for the
// status caption shown below the progress bar. Mirrors the chat-thread
// switch at layoutFileCard's stateLabel section so the two surfaces
// surface the same wording for the same transfer state.
func fileRowStateLabel(t filetransfer.TransferSnapshot) (string, color.NRGBA) {
	green := color.NRGBA{R: 100, G: 220, B: 130, A: 255}
	gray := color.NRGBA{R: 180, G: 180, B: 180, A: 255}
	red := color.NRGBA{R: 255, G: 100, B: 100, A: 255}
	amber := color.NRGBA{R: 255, G: 200, B: 80, A: 255}

	switch t.Direction {
	case "send":
		if t.State == "completed" {
			return "console.file.label.downloaded_by_peer", green
		}
	case "receive":
		switch t.State {
		case "completed":
			return "console.file.label.completed", green
		case "waiting_ack":
			return "console.file.label.confirming", gray
		case "failed":
			return "console.file.label.failed", red
		case "waiting_route":
			return "console.file.label.sender_offline", amber
		}
	}
	return "", color.NRGBA{}
}

// layoutFileRowDiskActions renders Show in Folder + Open buttons,
// LEFT-aligned and packed tight on the left side of the row.
//
// Visibility is gated on a "fully downloaded" state predicate
// (showDiskActionsForRow): the buttons appear directly under the
// "downloaded" / "completed" state caption to reinforce that they
// are the natural follow-up to a finished transfer.
//
// Why we don't reuse the chat-thread's layoutFileActionButtons:
// that method uses Spacing: layout.SpaceStart which RIGHT-aligns
// the buttons (chat bubbles are mostly right-aligned for outgoing
// messages, so the chat-thread layout is correct in context). On
// the file tab the row spans the whole tab width, so right-aligned
// buttons read as "wasted line + buttons floating off in the
// distance". Left alignment packs the action affordance under the
// status caption it semantically belongs to.
func (c *ConsoleWindow) layoutFileRowDiskActions(gtx layout.Context, t filetransfer.TransferSnapshot) layout.Dimensions {
	if !showDiskActionsForRow(t) {
		return layout.Dimensions{}
	}
	if c.parent == nil || c.parent.router == nil {
		return layout.Dimensions{}
	}
	bridge := c.parent.router.FileBridge()
	if bridge == nil {
		return layout.Dimensions{}
	}
	path := bridge.FilePath(t.FileID, t.Direction == "send")
	if path == "" {
		return layout.Dimensions{}
	}
	messageID := string(t.FileID)
	// Reuse the chat-thread Clickable maps (same package) so the
	// click handlers stay in sync — the buttons control the same
	// underlying file regardless of which surface the user clicks
	// from. Maps are lazily initialised by the chat-thread on its
	// first paint; we initialise here defensively because the file
	// tab could be the first surface to render this row.
	if c.parent.fileRevealBtns == nil {
		c.parent.fileRevealBtns = make(map[string]*widget.Clickable)
	}
	if c.parent.fileOpenBtns == nil {
		c.parent.fileOpenBtns = make(map[string]*widget.Clickable)
	}
	revealBtn, ok := c.parent.fileRevealBtns[messageID]
	if !ok {
		revealBtn = new(widget.Clickable)
		c.parent.fileRevealBtns[messageID] = revealBtn
	}
	openBtn, ok := c.parent.fileOpenBtns[messageID]
	if !ok {
		openBtn = new(widget.Clickable)
		c.parent.fileOpenBtns[messageID] = openBtn
	}

	for revealBtn.Clicked(gtx) {
		go revealFileInDir(path)
	}
	for openBtn.Clicked(gtx) {
		go openFile(path)
	}

	btnBg := color.NRGBA{R: 50, G: 60, B: 80, A: 255}
	btnFg := color.NRGBA{R: 180, G: 200, B: 230, A: 255}

	// Left-aligned: no Spacing flag, no Flexed spacer. Rigid items
	// pack on the start (left) edge in their natural order.
	return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				matBtn := material.Button(c.theme, revealBtn, c.parent.t("file.show_in_folder"))
				matBtn.Background = btnBg
				matBtn.Color = btnFg
				matBtn.Inset = layout.Inset{
					Top: unit.Dp(3), Bottom: unit.Dp(3),
					Left: unit.Dp(8), Right: unit.Dp(8),
				}
				matBtn.CornerRadius = unit.Dp(5)
				matBtn.TextSize = unit.Sp(11)
				return matBtn.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				matBtn := material.Button(c.theme, openBtn, c.parent.t("file.open_file"))
				matBtn.Background = btnBg
				matBtn.Color = btnFg
				matBtn.Inset = layout.Inset{
					Top: unit.Dp(3), Bottom: unit.Dp(3),
					Left: unit.Dp(8), Right: unit.Dp(8),
				}
				matBtn.CornerRadius = unit.Dp(5)
				matBtn.TextSize = unit.Sp(11)
				return matBtn.Layout(gtx)
			}),
		)
	})
}

// showDiskActionsForRow gates the Show in Folder + Open buttons on
// the transfer's "fully downloaded" status. The buttons appear
// directly under the "downloaded" / "completed" state caption to
// reinforce the user's mental model that those actions are the
// natural follow-up to a finished transfer.
//
// Sender rows: only "completed" qualifies — the recipient acked,
// the local transmit blob is still on disk, opening it reads as
// "look at what I sent". Earlier states (announced, serving) are
// in-flight; the chat thread is the place to peek at an outgoing
// file mid-flight.
//
// Receiver rows: "waiting_ack" AND "completed" both qualify — the
// verifier has already renamed the partial to CompletedPath in
// both cases, so the file is truly on disk. Earlier states have
// no completed file. "tombstone" is the post-cleanup record where
// the file has been deleted.
func showDiskActionsForRow(t filetransfer.TransferSnapshot) bool {
	switch t.Direction {
	case "send":
		return t.State == "completed"
	case "receive":
		return t.State == "completed" || t.State == "waiting_ack"
	}
	return false
}

// layoutFileRowDeleteAction renders the Delete button. The button
// lives in the row's right action column (see actionColumn in
// layoutFileTransferRow), directly under the state badge.
//
// Tombstones get nothing (the file is already gone).
//
// Direction-aware offline gate, matching DMRouter.SendMessageDelete:
//
//   - Direction == "receive": local-only delete path. The router
//     removes the local row and the on-disk file synchronously
//     without a wire round-trip; peer reachability is irrelevant.
//     The button is always actionable for incoming rows.
//
//   - Direction == "send": needs the peer to ack. Issuing a delete
//     to an offline peer would burn the entire retry budget waiting
//     for an ack that cannot come. Render as a static gray
//     "disabled" box (no Clickable, no hover ripple) so the
//     control is unmistakably non-interactive.
func (c *ConsoleWindow) layoutFileRowDeleteAction(gtx layout.Context, t filetransfer.TransferSnapshot, peerOnline bool) layout.Dimensions {
	if t.State == "tombstone" {
		return layout.Dimensions{}
	}
	// Receive-direction deletes are local-only; ignore peerOnline.
	deleteEnabled := t.Direction != "send" || peerOnline
	if !deleteEnabled {
		return c.layoutFileRowDeleteDisabled(gtx)
	}
	btn := c.fileDeleteButton(t.FileID)
	for btn.Clicked(gtx) {
		c.dispatchFileDeleteAsync(t)
	}
	label := material.Button(c.theme, btn, c.parent.t("console.file.delete"))
	label.Background = color.NRGBA{R: 120, G: 50, B: 60, A: 255}
	label.Color = color.NRGBA{R: 250, G: 240, B: 240, A: 255}
	label.Inset = layout.Inset{
		Top: unit.Dp(3), Bottom: unit.Dp(3),
		Left: unit.Dp(10), Right: unit.Dp(10),
	}
	label.CornerRadius = unit.Dp(5)
	label.TextSize = unit.Sp(11)
	return label.Layout(gtx)
}

// layoutFileRowDeleteDisabled renders the offline-state Delete pill
// via the shared layoutDisabledDeletePill helper (see window.go).
// Both surfaces — the chat-thread file card and this file tab —
// must show the same visual for "Delete is unavailable because the
// peer is offline", so the visual is owned in one place.
func (c *ConsoleWindow) layoutFileRowDeleteDisabled(gtx layout.Context) layout.Dimensions {
	return layoutDisabledDeletePill(gtx, c.theme, c.parent.t("console.file.delete"))
}

// dispatchFileDeleteAsync runs SendMessageDelete on a background
// goroutine. Mirrors the pattern in window.go's context-menu
// delete handler. Local row stays alive until the peer's ack
// confirms the deletion; the terminal status arrives via
// TopicMessageDeleteCompleted and the existing subscription
// invalidates the file tab.
func (c *ConsoleWindow) dispatchFileDeleteAsync(t filetransfer.TransferSnapshot) {
	if c.parent == nil || c.parent.router == nil {
		return
	}
	router := c.parent.router
	router.SetSendStatus(c.parent.t("status.message_deleting"))

	peer := t.Peer
	target := domain.MessageID(t.FileID)
	outgoing := t.Direction == "send"
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), fileDeleteTimeout)
		defer cancel()
		if err := router.SendMessageDelete(ctx, peer, target); err != nil {
			router.SetSendStatus(c.parent.t("status.message_delete_failed", err.Error()))
			return
		}
		if !outgoing {
			// Receive direction = local-only path inside
			// SendMessageDelete. The router has already removed
			// the local row, run the file-transfer cleanup hook,
			// and published TopicMessageDeleteCompleted with the
			// terminal "deleted" outcome. The Window's async
			// handleMessageDeleteOutcome subscriber may have
			// already written the final status before this
			// goroutine resumes; writing "Delete request sent;
			// waiting for peer…" here would race-overwrite that
			// terminal status with a misleading caption (no peer
			// ack is coming because no wire round-trip happened).
			return
		}
		// Outgoing: pessimistic ordering — local row stays alive
		// until the peer's message_delete_ack arrives. The
		// terminal UI status surfaces via TopicMessageDeleteCompleted
		// and handleMessageDeleteOutcome; this caption is the
		// transient "in flight" hint.
		router.SetSendStatus(c.parent.t("status.message_delete_dispatched"))
	}()
}

// fileRowMetaLine: "1.2 MB / 4.8 MB · 14:33" (active) or "4.8 MB · 14:33" (terminal).
func (c *ConsoleWindow) fileRowMetaLine(t filetransfer.TransferSnapshot) string {
	timestamp := t.CreatedAt
	if t.CompletedAt != "" {
		timestamp = t.CompletedAt
	}
	return fmt.Sprintf("%s · %s",
		formatTransferBytes(t),
		formatTransferTimestamp(timestamp),
	)
}

// formatTransferBytes returns the size + progress legend shown on
// the meta line. Format matches the chat-thread file card so the
// two surfaces read identically:
//
//   - Non-terminal (announced / serving / available / downloading /
//     verifying / waiting_route / waiting_ack), even at zero bytes:
//     "0 B / 4.8 MB  (0%)"
//     "1.2 MB / 4.8 MB  (25%)"
//   - Terminal (completed / failed / tombstone):
//     "4.8 MB"
//
// The double-space before the parenthesised percent is intentional
// — it visually separates the size pair from the progress hint and
// matches the chat-thread spacing exactly.
//
// We deliberately do NOT short-circuit on Bytes == 0: the user
// wants to see "0 B / total (0%)" while a download is starting up
// (between StartDownload firing the chunk_request and the first
// chunk_response arriving), and parity with the chat thread is the
// reference behaviour for that gap.
func formatTransferBytes(t filetransfer.TransferSnapshot) string {
	if isTerminalTransferState(t.State) {
		return formatBytesShort(t.FileSize)
	}
	return fmt.Sprintf("%s / %s  (%d%%)",
		formatBytesShort(t.Bytes),
		formatBytesShort(t.FileSize),
		transferProgressPercent(t),
	)
}

// formatBytesShort renders a uint64 byte count with binary-prefix
// units (KB, MB, GB, TB).
func formatBytesShort(n uint64) string {
	const (
		kb = 1024
		mb = 1024 * 1024
		gb = 1024 * 1024 * 1024
		tb = 1024 * 1024 * 1024 * 1024
	)
	switch {
	case n >= tb:
		return fmt.Sprintf("%.1f TB", float64(n)/float64(tb))
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// formatTransferTimestamp pretty-prints an RFC3339 string. Falls
// back to the raw input if parsing fails so we never silently hide
// an unexpected value.
func formatTransferTimestamp(rfc3339 string) string {
	if rfc3339 == "" {
		return ""
	}
	ts, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return rfc3339
	}
	return ts.Local().Format("2006-01-02 15:04")
}

// transferProgressFraction returns the [0,1] fraction transferred,
// clamped. Terminal-state special cases avoid showing a stale Bytes
// counter as still-progressing.
func transferProgressFraction(t filetransfer.TransferSnapshot) float64 {
	if t.FileSize == 0 {
		return 0
	}
	if isTerminalTransferState(t.State) {
		if t.State == "completed" {
			return 1
		}
		return 0
	}
	frac := float64(t.Bytes) / float64(t.FileSize)
	if frac < 0 {
		return 0
	}
	if frac > 1 {
		return 1
	}
	return frac
}

// transferProgressPercent is the integer-clamp the chat-thread
// progress bar consumes.
func transferProgressPercent(t filetransfer.TransferSnapshot) int {
	pct := int(transferProgressFraction(t)*100 + 0.5)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// fileDisplayName returns the filename, falling back to the FileID
// prefix for legacy entries with stripped metadata.
func fileDisplayName(t filetransfer.TransferSnapshot) string {
	if t.FileName != "" {
		return t.FileName
	}
	return string(t.FileID)
}

// directionIcon: ↑ outbound, ↓ inbound.
func (c *ConsoleWindow) directionIcon(direction string) string {
	switch direction {
	case "send":
		return "↑"
	case "receive":
		return "↓"
	default:
		return ""
	}
}

// directionColor pairs the arrow with a state colour: cool blue for
// outbound, green-ish for inbound.
func (c *ConsoleWindow) directionColor(direction string) color.NRGBA {
	switch direction {
	case "send":
		return color.NRGBA{R: 92, G: 156, B: 220, A: 255}
	case "receive":
		return color.NRGBA{R: 102, G: 196, B: 156, A: 255}
	default:
		return color.NRGBA{R: 168, G: 182, B: 204, A: 255}
	}
}

// fileStateBadgeColors maps a transfer state to badge background +
// foreground.
func fileStateBadgeColors(state string) (bg, fg color.NRGBA) {
	switch state {
	case "completed":
		return color.NRGBA{R: 36, G: 92, B: 60, A: 255}, color.NRGBA{R: 220, G: 250, B: 230, A: 255}
	case "failed":
		return color.NRGBA{R: 120, G: 50, B: 60, A: 255}, color.NRGBA{R: 250, G: 230, B: 230, A: 255}
	case "tombstone":
		return color.NRGBA{R: 70, G: 78, B: 92, A: 255}, color.NRGBA{R: 200, G: 210, B: 220, A: 255}
	case "downloading", "verifying", "serving":
		return color.NRGBA{R: 40, G: 76, B: 130, A: 255}, color.NRGBA{R: 220, G: 232, B: 250, A: 255}
	case "available", "announced":
		return color.NRGBA{R: 60, G: 70, B: 96, A: 255}, color.NRGBA{R: 220, G: 232, B: 250, A: 255}
	case "waiting_route", "waiting_ack":
		return color.NRGBA{R: 96, G: 78, B: 36, A: 255}, color.NRGBA{R: 250, G: 240, B: 220, A: 255}
	default:
		return color.NRGBA{R: 60, G: 70, B: 96, A: 255}, color.NRGBA{R: 220, G: 232, B: 250, A: 255}
	}
}

// truncatePeerIdentity shortens a peer identity for the row layout.
func truncatePeerIdentity(p domain.PeerIdentity) string {
	s := string(p)
	if len(s) <= 24 {
		return s
	}
	return s[:12] + "…" + s[len(s)-8:]
}
