package desktop

import (
	"image"
	"image/color"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"corsa/internal/core/config"
	"corsa/internal/core/crashlog"
	"corsa/internal/core/domain"
	"corsa/internal/core/rpc"
	"corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

type Window struct {
	router   *service.DMRouter
	client   *service.DesktopClient
	cmdTable *rpc.CommandTable
	runtime  *NodeRuntime
	prefs    *Preferences
	theme    *material.Theme
	ops      op.Ops

	recipientEditor      widget.Editor
	identitySearchEditor widget.Editor
	messageEditor        widget.Editor
	contactsList         widget.List
	chatList             widget.List
	consoleButton        widget.Clickable
	updateButton         widget.Clickable
	sendButton           widget.Clickable
	copyIdentityButton   widget.Clickable
	languageToggle       widget.Clickable
	languageOptions      map[string]*widget.Clickable
	recipientButtons     map[domain.PeerIdentity]*widget.Clickable
	recipientRightClick  map[domain.PeerIdentity]*rightClickState
	messageSelectables   map[string]*widget.Selectable
	lastChatPeer         domain.PeerIdentity
	language             string
	showLanguageMenu     bool
	consoleOpen          bool

	// Global cursor tracking for context menu positioning.
	cursorTracker int // tag for window-level pointer events
	lastCursorPos image.Point

	// Context menu state for right-click on recipient buttons.
	contextMenuPeer      domain.PeerIdentity // fingerprint of the peer whose context menu is open
	contextMenuPos       image.Point
	ctxMenuCopy          widget.Clickable
	ctxMenuDelete        widget.Clickable
	ctxMenuDeleteConfirm widget.Clickable
	ctxMenuDeleteCancel  widget.Clickable
	ctxMenuAlias         widget.Clickable
	ctxMenuAliasSave     widget.Clickable
	ctxMenuAliasCancel   widget.Clickable
	showDeleteConfirm    bool // true when confirmation step is shown
	showAliasEditor      bool // true when alias input is shown
	aliasEditor          widget.Editor

	snap service.RouterSnapshot

	consoleMu sync.Mutex

	window *app.Window
}

const (
	languageButtonWidth = 76
	languageMenuWidth   = 220
	languageOverlayTop  = 58
	windowInset         = 24
	languageMenuHeight  = 316
)

// newAppTheme creates a fresh material.Theme with the application colour scheme.
// Each window must own its own Theme because the embedded text.Shaper uses an
// unsynchronised map cache and is therefore not safe for concurrent use.
func newAppTheme() *material.Theme {
	theme := material.NewTheme()
	theme.Bg = color.NRGBA{R: 18, G: 21, B: 26, A: 255}
	theme.Fg = color.NRGBA{R: 235, G: 239, B: 244, A: 255}
	theme.ContrastBg = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	theme.ContrastFg = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	return theme
}

func NewWindow(client *service.DesktopClient, router *service.DMRouter, cmdTable *rpc.CommandTable, runtime *NodeRuntime, prefs *Preferences) *Window {
	theme := newAppTheme()

	language := normalizeLanguage(client.Language())
	if prefs != nil && prefs.Language != "" {
		language = normalizeLanguage(prefs.Language)
	}

	w := &Window{
		router:              router,
		client:              client,
		cmdTable:            cmdTable,
		runtime:             runtime,
		prefs:               prefs,
		theme:               theme,
		language:            language,
		languageOptions:     make(map[string]*widget.Clickable),
		recipientButtons:    make(map[domain.PeerIdentity]*widget.Clickable),
		recipientRightClick: make(map[domain.PeerIdentity]*rightClickState),
		messageSelectables:  make(map[string]*widget.Selectable),
		contactsList:        widget.List{List: layout.List{Axis: layout.Vertical}},
		chatList:            widget.List{List: layout.List{Axis: layout.Vertical, ScrollToEnd: true}},
	}
	w.aliasEditor.SingleLine = true
	w.aliasEditor.Submit = true
	return w
}

func (w *Window) Run() error {
	go func() {
		defer crashlog.DeferRecover()

		window := new(app.Window)
		w.window = window
		window.Option(
			app.Title(w.t("app.title")+" — "+w.t("app.subtitle")),
			app.Size(unit.Dp(1100), unit.Dp(1100)),
		)

		w.startPolling(window)

		if err := w.loop(window); err != nil {
			panic(err)
		}
		os.Exit(0)
	}()

	app.Main()
	return nil
}

func (w *Window) startPolling(window *app.Window) {
	w.router.Start()

	go func() {
		for ev := range w.router.Subscribe() {
			if ev.Type == service.UIEventBeep {
				go systemBeep()
			}
			if w.window != nil {
				w.window.Invalidate()
			}
		}
	}()
}

func (w *Window) loop(window *app.Window) error {
	for {
		switch e := window.Event().(type) {
		case app.DestroyEvent:
			return e.Err
		case app.FrameEvent:
			gtx := app.NewContext(&w.ops, e)
			w.layout(gtx)
			e.Frame(gtx.Ops)
		}
	}
}

func (w *Window) layout(gtx layout.Context) layout.Dimensions {
	w.snap = w.router.Snapshot()
	w.handlePendingActions()
	w.handleActions(gtx)
	fill(gtx, color.NRGBA{R: 12, G: 15, B: 20, A: 255})

	// Track cursor position at window level for accurate context menu placement.
	defer clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, &w.cursorTracker)
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: &w.cursorTracker,
			Kinds:  pointer.Move | pointer.Press | pointer.Drag,
		})
		if !ok {
			break
		}
		if pe, ok := ev.(pointer.Event); ok {
			w.lastCursorPos = pe.Position.Round()
		}
	}

	inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(6), Right: unit.Dp(6)}
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(w.layoutHeader),
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Flexed(1, w.layoutMain),
				)
			})
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if !w.showLanguageMenu {
				return layout.Dimensions{}
			}
			return w.layoutLanguageOverlay(gtx)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if w.contextMenuPeer == "" {
				return layout.Dimensions{}
			}
			return w.layoutContextMenuOverlay(gtx)
		}),
	)
}

// Gio widgets are single-threaded — mutations MUST happen on the UI goroutine.
func (w *Window) handlePendingActions() {
	pa := w.router.ConsumePendingActions()

	if pa.ClearEditor {
		w.messageEditor.SetText("")
	}
	if pa.ScrollToEnd {
		w.chatList.Position.BeforeEnd = false
	}
	if pa.RecipientText != "" {
		w.recipientEditor.SetText(string(pa.RecipientText))
	}
}

func (w *Window) handleActions(gtx layout.Context) {
	for w.languageToggle.Clicked(gtx) {
		w.showLanguageMenu = !w.showLanguageMenu
	}

	for w.consoleButton.Clicked(gtx) {
		w.openConsoleWindow()
	}

	for w.updateButton.Clicked(gtx) {
		openBrowser("https://github.com/piratecash/corsa/releases")
	}

	for w.sendButton.Clicked(gtx) {
		w.triggerSend()
	}

	w.handleMessageSubmitShortcut(gtx)

	for w.copyIdentityButton.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(w.snap.MyAddress)),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	w.handleContextMenuActions(gtx)
}

func (w *Window) handleContextMenuActions(gtx layout.Context) {
	if w.contextMenuPeer == "" {
		return
	}

	if w.ctxMenuCopy.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(string(w.contextMenuPeer))),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		w.contextMenuPeer = ""
		w.showDeleteConfirm = false
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuAlias.Clicked(gtx) {
		w.showAliasEditor = true
		existing := ""
		if w.prefs != nil {
			existing = w.prefs.Alias(w.contextMenuPeer)
		}
		w.aliasEditor.SetText(existing)
		gtx.Execute(key.FocusCmd{Tag: &w.aliasEditor})
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuAliasSave.Clicked(gtx) {
		alias := strings.TrimSpace(w.aliasEditor.Text())
		if w.prefs != nil {
			w.prefs.SetAlias(w.contextMenuPeer, alias)
			_ = w.prefs.Save()
		}
		w.contextMenuPeer = ""
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuAliasCancel.Clicked(gtx) {
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	for w.ctxMenuDelete.Clicked(gtx) {
		w.showDeleteConfirm = true
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuDeleteConfirm.Clicked(gtx) {
		peer := w.contextMenuPeer
		w.contextMenuPeer = ""
		w.showDeleteConfirm = false

		// Capture the ordered recipient list before deletion so we can
		// pick the nearest neighbor for auto-selection in the UI layer.
		recipients := w.snapRecipients()
		removedIdx := -1
		for i, r := range recipients {
			if r == peer {
				removedIdx = i
				break
			}
		}

		wasActive, err := w.router.RemovePeer(peer)
		if err != nil {
			w.router.SetSendStatus(w.t("status.delete_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		// Remove saved alias together with the identity.
		if w.prefs != nil {
			w.prefs.SetAlias(peer, "")
			_ = w.prefs.Save()
		}

		if wasActive && removedIdx >= 0 && len(recipients) > 1 {
			remaining := make([]domain.PeerIdentity, 0, len(recipients)-1)
			remaining = append(remaining, recipients[:removedIdx]...)
			remaining = append(remaining, recipients[removedIdx+1:]...)
			nextIdx := removedIdx
			if nextIdx >= len(remaining) {
				nextIdx = len(remaining) - 1
			}
			w.router.SelectPeer(remaining[nextIdx])
			w.recipientEditor.SetText(string(remaining[nextIdx]))
		}

		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuDeleteCancel.Clicked(gtx) {
		w.showDeleteConfirm = false
		if w.window != nil {
			w.window.Invalidate()
		}
	}
}

func (w *Window) handleMessageSubmitShortcut(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(
			key.Filter{Focus: &w.messageEditor, Name: key.NameEnter, Optional: key.ModShift},
			key.Filter{Focus: &w.messageEditor, Name: key.NameReturn, Optional: key.ModShift},
		)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		if ke.Modifiers.Contain(key.ModShift) {
			continue
		}
		w.triggerSend()
	}
}

func (w *Window) triggerSend() {
	to := domain.PeerIdentity(strings.TrimSpace(string(w.snap.ActivePeer)))
	if to == "" {
		to = domain.PeerIdentity(strings.TrimSpace(w.recipientEditor.Text()))
	}
	body := strings.TrimSpace(w.messageEditor.Text())
	if body == "" {
		return
	}
	w.router.SendMessage(to, body)
}

func (w *Window) layoutHeader(gtx layout.Context) layout.Dimensions {
	title := material.Label(w.theme, unit.Sp(24), w.t("app.title"))
	title.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	title.Font.Weight = 700

	return layout.Flex{
		Axis:      layout.Horizontal,
		Spacing:   layout.SpaceBetween,
		Alignment: layout.Middle,
	}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return title.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis:      layout.Horizontal,
				Alignment: layout.Middle,
			}.Layout(gtx,
				layout.Rigid(w.layoutUpdateBadge),
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
				layout.Rigid(w.layoutConsoleButton),
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
				layout.Rigid(w.layoutLanguageSelectorInline),
			)
		}),
	)
}

func (w *Window) layoutMain(gtx layout.Context) layout.Dimensions {
	status := w.snap.NodeStatus
	recipients := w.snapRecipients()
	w.ensureSelectedRecipient(recipients)

	return layout.Flex{
		Axis:    layout.Horizontal,
		Spacing: layout.SpaceBetween,
	}.Layout(gtx,
		layout.Flexed(0.3, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					lbl := material.Label(w.theme, unit.Sp(15), w.t("app.subtitle"))
					lbl.Color = color.NRGBA{R: 144, G: 156, B: 173, A: 255}
					return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, lbl.Layout)
				}),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return w.layoutContactsCard(gtx, status, recipients)
				}),
			)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Flexed(0.7, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					recipient := w.snap.ActivePeer
					if recipient == "" {
						return layout.Dimensions{}
					}
					lbl := material.Label(w.theme, unit.Sp(15), w.t("chat.with", w.peerDisplayName(recipient)))
					lbl.Color = color.NRGBA{R: 200, G: 212, B: 228, A: 255}
					lbl.Font.Weight = 600
					return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, lbl.Layout)
				}),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return w.layoutChatCard(gtx, status)
				}),
				layout.Rigid(w.layoutComposerCard),
			)
		}),
	)
}

func (w *Window) layoutContactsCard(gtx layout.Context, status service.NodeStatus, recipients []domain.PeerIdentity) layout.Dimensions {
	rows := []string{
		w.t("clients.you", w.snap.MyAddress),
		w.t("clients.known", len(recipients)),
	}

	return w.card(gtx, w.t("clients.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(220)))
			btn := material.Button(w.theme, &w.copyIdentityButton, w.t("clients.copy_identity"))
			return btn.Layout(gtx)
		})
	}, func(gtx layout.Context) layout.Dimensions {
		searchResults := searchKnownIdentities(status.KnownIDs, recipients, w.snap.MyAddress, w.identitySearchEditor.Text())

		children := []layout.FlexChild{
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.identitySearchCard(gtx, status, searchResults)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		}

		if len(recipients) == 0 {
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body1(w.theme, w.t("clients.empty"))
				label.Color = color.NRGBA{R: 165, G: 177, B: 194, A: 255}
				return label.Layout(gtx)
			}))
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
		}

		list := material.List(w.theme, &w.contactsList)
		children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return list.Layout(gtx, len(recipients), func(gtx layout.Context, index int) layout.Dimensions {
				return w.layoutRecipientButton(gtx, status, recipients[index], true)
			})
		}))
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	})
}

func (w *Window) identitySearchCard(gtx layout.Context, status service.NodeStatus, results []domain.PeerIdentity) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Left: unit.Dp(4), Right: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
				backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
				cardHeight := gtx.Dp(unit.Dp(78))
				return layout.Stack{}.Layout(gtx,
					layout.Expanded(func(gtx layout.Context) layout.Dimensions {
						gtx.Constraints.Min.Y = cardHeight
						gtx.Constraints.Max.Y = cardHeight
						fill(gtx, borderColor)
						return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							fill(gtx, backgroundColor)
							return layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										label := material.Body2(w.theme, w.t("clients.search_label"))
										label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
										return label.Layout(gtx)
									}),
									layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										w.identitySearchEditor.SingleLine = true
										editor := material.Editor(w.theme, &w.identitySearchEditor, w.t("clients.search_placeholder"))
										editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
										editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
										return editor.Layout(gtx)
									}),
								)
							})
						})
					}),
				)
			})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			if len(results) == 0 {
				return layout.Dimensions{}
			}
			if len(results) > 4 {
				results = results[:4]
			}
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, recipientsToChildren(results, func(gtx layout.Context, identity domain.PeerIdentity) layout.Dimensions {
				return layout.Inset{Left: unit.Dp(4), Right: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return w.layoutRecipientButton(gtx, status, identity, false)
				})
			})...)
		}),
	)
}

func (w *Window) layoutRecipientButton(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity, showUnread bool) layout.Dimensions {
	btn := w.recipientButton(fingerprint)
	rc := w.recipientRightClickState(fingerprint)

	return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fpStr := string(fingerprint)
		for btn.Clicked(gtx) {
			w.recipientEditor.SetText(fpStr)
			w.router.SetSendStatus(w.t("status.chat_selected"))
			w.router.SelectPeer(fingerprint)
		}

		// Detect right-click (secondary button) for context menu.
		// Position comes from the window-level cursor tracker (lastCursorPos)
		// because card-local coordinates don't account for scroll offset
		// and nested layout transforms.
		for {
			ev, ok := gtx.Event(pointer.Filter{
				Target: rc,
				Kinds:  pointer.Press | pointer.Release,
			})
			if !ok {
				break
			}
			pe, ok := ev.(pointer.Event)
			if !ok {
				continue
			}
			if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonSecondary) {
				rc.pressed = true
			}
			if pe.Kind == pointer.Release && rc.pressed {
				rc.pressed = false
				w.contextMenuPeer = fingerprint
				w.contextMenuPos = w.lastCursorPos
				w.showDeleteConfirm = false
				w.showAliasEditor = false
				// Auto-select this identity so the user sees the chat.
				w.recipientEditor.SetText(fpStr)
				w.router.SelectPeer(fingerprint)
			}
		}

		bg := color.NRGBA{R: 34, G: 46, B: 62, A: 255}
		if fingerprint == w.snap.ActivePeer {
			bg = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
		}

		return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
			// Register right-click tag inside the Clickable's clip area.
			// Clickable only handles ButtonPrimary, so ButtonSecondary
			// events propagate to our tag without conflict.
			event.Op(gtx.Ops, rc)

			fill(gtx, bg)
			return layout.UniformInset(unit.Dp(14)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{
							Axis:      layout.Horizontal,
							Spacing:   layout.SpaceBetween,
							Alignment: layout.Middle,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return layout.Inset{Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
									return w.layoutReachableIndicator(gtx, status, fingerprint)
								})
							}),
							layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
								title := material.Body1(w.theme, w.peerDisplayName(fingerprint))
								title.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
								title.Font.Weight = 600
								return title.Layout(gtx)
							}),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								if !showUnread {
									return layout.Dimensions{}
								}
								ps := w.snap.Peers[fingerprint]
								if ps == nil || ps.Unread == 0 {
									return layout.Dimensions{}
								}
								return w.layoutUnreadBadge(gtx, ps.Unread)
							}),
						)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						preview := w.snapPreview(fingerprint)
						if strings.TrimSpace(preview) == "" {
							preview = fpStr
						}
						label := material.Body2(w.theme, ellipsize(preview, 44))
						label.Color = color.NRGBA{R: 187, G: 197, B: 212, A: 255}
						label.MaxLines = 1
						return label.Layout(gtx)
					}),
				)
			})
		})
	})
}

func (w *Window) layoutChatCard(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	recipient := w.snap.ActivePeer
	var rows []string

	if recipient == "" {
		rows = append(rows, w.t("chat.choose"))
		return w.card(gtx, w.t("chat.title"), rows)
	}

	conversation := w.snap.ActiveMessages
	if len(conversation) == 0 {
		if !w.snap.CacheReady {
			return w.layoutLoadingCard(gtx, "")
		}
		rows = append(rows, w.t("chat.empty"))
		return w.card(gtx, "", rows)
	}

	return w.card(gtx, "", rows, func(gtx layout.Context) layout.Dimensions {
		return w.layoutConversation(gtx, recipient, conversation)
	})
}

func (w *Window) layoutLoadingCard(gtx layout.Context, title string) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

		inset := layout.UniformInset(unit.Dp(8))
		return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(w.theme, unit.Sp(20), title)
					label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{
							Axis:      layout.Vertical,
							Alignment: layout.Middle,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Label(w.theme, unit.Sp(32), ". . .")
								label.Color = color.NRGBA{R: 120, G: 144, B: 176, A: 255}
								label.Alignment = text.Middle
								return label.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Label(w.theme, unit.Sp(16), w.t("chat.loading"))
								label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
								label.Alignment = text.Middle
								return label.Layout(gtx)
							}),
						)
					})
				}),
			)
		})
	})
}

func (w *Window) layoutComposerCard(gtx layout.Context) layout.Dimensions {
	recipient := w.snap.ActivePeer
	status := w.snap.NodeStatus

	var rows []string
	if sendStatus := w.snap.SendStatus; sendStatus != "" {
		rows = append(rows, sendStatus)
	}

	return w.card(gtx, "", rows, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.messageInputCard(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis:      layout.Horizontal,
					Spacing:   layout.SpaceBetween,
					Alignment: layout.Middle,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutNetworkStatus(gtx, status)
					}),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Dimensions{}
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := w.t("compose.send")
						if recipient == "" {
							label = w.t("compose.select_first")
						}
						return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(260)))
							btn := material.Button(w.theme, &w.sendButton, label)
							return btn.Layout(gtx)
						})
					}),
				)
			}),
		)
	})
}

func (w *Window) layoutNetworkStatus(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	state, connected, total, pending := networkStatusSummary(status)
	labelText := w.t("compose.network_status", strings.ToUpper(state), connected, total, pending)
	if labelText == "compose.network_status" {
		labelText = "NET " + strings.ToUpper(state) + " | " + strconv.Itoa(connected) + "/" + strconv.Itoa(total) + " peers | " + strconv.Itoa(pending) + " pending"
	}
	breakdownText := w.networkBreakdownText(status)
	bg, fg := networkStateColors(state)

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(8), Right: unit.Dp(8)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Caption(w.theme, labelText)
					label.Color = fg
					return label.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(breakdownText) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Top: unit.Dp(2)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						label := material.Caption(w.theme, breakdownText)
						label.Color = color.NRGBA{R: 214, G: 221, B: 232, A: 220}
						return label.Layout(gtx)
					})
				}),
			)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(12))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: bg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

// networkStatusSummary computes the aggregate network status based on the
// number of usable peers. Stalled peers have a live TCP session but are
// excluded from message routing (routingTargets skips peerStateStalled),
// so they do not count as usable. Per-peer health details (degraded,
// stalled) are shown in the breakdown line instead.
func networkStatusSummary(status service.NodeStatus) (string, int, int, int) {
	usable := 0  // healthy + degraded — can route messages
	stalled := 0 // connected at TCP level but not routing
	reconnecting := 0
	pending := 0

	for _, item := range status.PeerHealth {
		switch item.State {
		case "healthy", "degraded":
			usable++
		case "stalled":
			stalled++
		case "reconnecting":
			reconnecting++
		}
		pending += item.PendingCount
	}

	connected := usable + stalled
	total := connected + reconnecting

	switch {
	case total == 0:
		return "offline", 0, 0, pending
	case connected == 0:
		return "reconnecting", 0, total, pending
	case usable == 0:
		// Peers exist but none can route — functionally limited.
		return "limited", connected, total, pending
	case usable == 1:
		return "limited", connected, total, pending
	case usable*2 < total:
		// Less than half of known peers are usable.
		return "warning", connected, total, pending
	default:
		return "healthy", connected, total, pending
	}
}

func (w *Window) networkBreakdownText(status service.NodeStatus) string {
	healthy := 0
	degraded := 0
	stalled := 0
	reconnecting := 0
	outbound := 0
	inbound := 0

	for _, item := range status.PeerHealth {
		switch item.State {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "stalled":
			stalled++
		case "reconnecting":
			reconnecting++
		}
		switch item.Direction {
		case "outbound":
			outbound++
		case "inbound":
			inbound++
		}
	}

	if healthy == 0 && degraded == 0 && stalled == 0 && reconnecting == 0 {
		return ""
	}

	text := w.t("compose.network_breakdown", healthy, degraded, stalled, reconnecting)
	if text == "compose.network_breakdown" {
		return "H " + strconv.Itoa(healthy) + " | D " + strconv.Itoa(degraded) + " | S " + strconv.Itoa(stalled) + " | R " + strconv.Itoa(reconnecting) + " | ↑" + strconv.Itoa(outbound) + " ↓" + strconv.Itoa(inbound)
	}
	return text
}

func networkStateColors(state string) (color.NRGBA, color.NRGBA) {
	switch state {
	case "healthy":
		return color.NRGBA{R: 36, G: 92, B: 63, A: 255}, color.NRGBA{R: 231, G: 255, B: 239, A: 255}
	case "limited", "warning":
		return color.NRGBA{R: 140, G: 110, B: 20, A: 255}, color.NRGBA{R: 255, G: 240, B: 180, A: 255}
	case "reconnecting":
		return color.NRGBA{R: 57, G: 67, B: 84, A: 255}, color.NRGBA{R: 231, G: 237, B: 246, A: 255}
	default: // offline, unknown
		return color.NRGBA{R: 51, G: 56, B: 66, A: 255}, color.NRGBA{R: 214, G: 221, B: 232, A: 255}
	}
}

func (w *Window) messageInputCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
	backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
	cardHeight := gtx.Dp(unit.Dp(96))

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.Y = cardHeight
		gtx.Constraints.Max.Y = cardHeight
		fill(gtx, borderColor)

		return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			fill(gtx, backgroundColor)

			inset := layout.UniformInset(unit.Dp(8))
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Body2(w.theme, w.t("compose.body"))
						label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
						return label.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						w.messageEditor.SingleLine = false
						editor := material.Editor(w.theme, &w.messageEditor, w.t("compose.placeholder"))
						editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
						editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}

						height := gtx.Dp(unit.Dp(36))
						gtx.Constraints.Min.Y = height
						gtx.Constraints.Max.Y = height
						return editor.Layout(gtx)
					}),
				)
			})
		})
	})
}

func (w *Window) localNodeErrorRow() string {
	if err := w.runtime.Error(); err != "" {
		return w.t("node.error", err)
	}
	return w.t("node.error", w.t("node.error.none"))
}

func (w *Window) snapPreview(recipient domain.PeerIdentity) string {
	me := w.snap.MyAddress
	ps, ok := w.snap.Peers[recipient]
	if ok && ps.Preview.Body != "" {
		if ps.Preview.Sender == me {
			return "You: " + ps.Preview.Body
		}
		return ps.Preview.Body
	}
	return ""
}

// snapRecipients builds the sidebar recipient list from the router's peer
// state. Contacts are managed exclusively by the router: loaded from
// chatlog at startup, added on incoming messages, removed via RemovePeer.
// No polling or external contact source is involved.
func (w *Window) snapRecipients() []domain.PeerIdentity {
	recipients := make([]domain.PeerIdentity, 0, len(w.snap.Peers))
	me := domain.PeerIdentity(w.snap.MyAddress)
	for id := range w.snap.Peers {
		if id != me && id != "" {
			recipients = append(recipients, id)
		}
	}
	sort.Slice(recipients, func(i, j int) bool { return recipients[i] < recipients[j] })
	merged := mergeRecipientOrder(recipients, w.snap.PeerOrder)
	sortSidebarPeers(merged, w.snap)
	return merged
}

func (w *Window) recipientButton(id domain.PeerIdentity) *widget.Clickable {
	if btn, ok := w.recipientButtons[id]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.recipientButtons[id] = btn
	return btn
}

// rightClickState is a tag for receiving secondary-button pointer events
// on a recipient card. Each recipient gets its own tag so Gio routes
// events correctly.
type rightClickState struct {
	pressed bool
}

func (w *Window) recipientRightClickState(id domain.PeerIdentity) *rightClickState {
	if s, ok := w.recipientRightClick[id]; ok {
		return s
	}
	s := new(rightClickState)
	w.recipientRightClick[id] = s
	return s
}

func (w *Window) ensureSelectedRecipient(recipients []domain.PeerIdentity) {
	selected := w.snap.ActivePeer

	if len(recipients) == 0 {
		if strings.TrimSpace(string(selected)) != "" {
			w.recipientEditor.SetText(string(selected))
		} else {
			w.recipientEditor.SetText("")
		}
		return
	}

	for _, recipient := range recipients {
		if recipient == selected {
			w.recipientEditor.SetText(string(recipient))
			return
		}
	}

	if strings.TrimSpace(string(selected)) != "" {
		w.recipientEditor.SetText(string(selected))
		return
	}

	// Auto-select first recipient. AutoSelectPeer sends seen receipts
	// the same way SelectPeer does — the chat is on screen.
	w.router.AutoSelectPeer(recipients[0])
	w.recipientEditor.SetText(string(recipients[0]))
}

func mergeRecipientOrder(recipients, order []domain.PeerIdentity) []domain.PeerIdentity {
	if len(recipients) == 0 {
		return nil
	}
	known := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range recipients {
		known[recipient] = struct{}{}
	}
	out := make([]domain.PeerIdentity, 0, len(recipients))
	used := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range order {
		if _, ok := known[recipient]; !ok {
			continue
		}
		if _, ok := used[recipient]; ok {
			continue
		}
		used[recipient] = struct{}{}
		out = append(out, recipient)
	}
	for _, recipient := range recipients {
		if _, ok := used[recipient]; ok {
			continue
		}
		used[recipient] = struct{}{}
		out = append(out, recipient)
	}
	return out
}

func (w *Window) layoutUnreadBadge(gtx layout.Context, count int) layout.Dimensions {
	height := gtx.Dp(unit.Dp(24))
	width := gtx.Dp(unit.Dp(28))
	labelText := intToString(count)
	if count > 9 {
		labelText = "9+"
		width = gtx.Dp(unit.Dp(34))
	}
	gtx.Constraints.Min = image.Pt(width, height)
	gtx.Constraints.Max = image.Pt(width, height)
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			rr := clip.UniformRRect(image.Rectangle{Max: image.Pt(width, height)}, height/2)
			stack := clip.Stroke{
				Path:  rr.Path(gtx.Ops),
				Width: float32(gtx.Dp(unit.Dp(1))),
			}.Op().Push(gtx.Ops)
			defer stack.Pop()
			paint.ColorOp{Color: color.NRGBA{R: 221, G: 228, B: 240, A: 255}}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			return layout.Dimensions{Size: image.Pt(width, height)}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, labelText)
			label.Color = color.NRGBA{R: 232, G: 237, B: 247, A: 255}
			return layout.Inset{Left: unit.Dp(10), Top: unit.Dp(3)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Center.Layout(gtx, label.Layout)
			})
		}),
	)
}

// layoutReachableIndicator draws a small circle reflecting the routing table
// reachability of the identity. Three visual states:
//
//   - ReachableIDs == nil         → gray outline only (probe failed / data unavailable)
//   - ReachableIDs[id] == true    → green filled (at least one live route)
//   - ReachableIDs[id] == false   → gray filled  (no live route)
func (w *Window) layoutReachableIndicator(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity) layout.Dimensions {
	sz := gtx.Dp(unit.Dp(10))
	bounds := image.Pt(sz, sz)

	if status.ReachableIDs == nil {
		// Stroke-only circle: no reachability data available.
		strokeWidth := float32(gtx.Dp(unit.Dp(1.5)))
		stk := clip.Stroke{
			Path:  clip.Ellipse{Max: bounds}.Path(gtx.Ops),
			Width: strokeWidth,
		}.Op().Push(gtx.Ops)
		paint.ColorOp{Color: color.NRGBA{R: 96, G: 110, B: 130, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		stk.Pop()
		return layout.Dimensions{Size: bounds}
	}

	indicatorColor := color.NRGBA{R: 96, G: 110, B: 130, A: 255} // gray — unreachable
	if status.ReachableIDs[fingerprint] {
		indicatorColor = color.NRGBA{R: 72, G: 199, B: 142, A: 255} // green — reachable
	}
	defer clip.Ellipse{Max: bounds}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: indicatorColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	return layout.Dimensions{Size: bounds}
}

func ellipsize(s string, limit int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if limit <= 0 || len([]rune(s)) <= limit {
		return s
	}
	runes := []rune(s)
	if limit <= 1 {
		return string(runes[:limit])
	}
	return string(runes[:limit-1]) + "…"
}

func intToString(v int) string {
	return strconv.Itoa(v)
}

func (w *Window) layoutConversation(gtx layout.Context, recipient domain.PeerIdentity, conversation []service.DirectMessage) layout.Dimensions {
	list := material.List(w.theme, &w.chatList)
	return list.Layout(gtx, len(conversation), func(gtx layout.Context, index int) layout.Dimensions {
		message := conversation[index]
		return layout.Inset{Bottom: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.layoutChatBubble(gtx, recipient, message)
		})
	})
}

func (w *Window) layoutChatBubble(gtx layout.Context, recipient domain.PeerIdentity, message service.DirectMessage) layout.Dimensions {
	me := w.snap.MyAddress
	isMine := message.Sender == me

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		if isMine {
			return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.chatBubbleCard(gtx, message, true, w.t("chat.you_label"))
			})
		}
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.chatBubbleCard(gtx, message, false, w.peerDisplayName(recipient))
		})
	})
}

func (w *Window) chatBubbleCard(gtx layout.Context, message service.DirectMessage, isMine bool, author string) layout.Dimensions {
	maxWidth := gtx.Dp(unit.Dp(520))
	if gtx.Constraints.Max.X < maxWidth {
		maxWidth = gtx.Constraints.Max.X
	}
	gtx.Constraints.Max.X = maxWidth

	borderColor := color.NRGBA{R: 55, G: 68, B: 86, A: 255}
	authorColor := color.NRGBA{R: 162, G: 176, B: 196, A: 255}
	statusColor := color.NRGBA{R: 110, G: 130, B: 160, A: 180}
	if isMine {
		borderColor = color.NRGBA{R: 74, G: 109, B: 176, A: 255}
		authorColor = color.NRGBA{R: 173, G: 205, B: 255, A: 255}
	}

	border := widget.Border{Color: borderColor, CornerRadius: unit.Dp(8), Width: unit.Dp(1)}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			children := []layout.FlexChild{
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{
						Axis:      layout.Horizontal,
						Alignment: layout.Middle,
					}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, author)
							label.Color = authorColor
							return label.Layout(gtx)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, message.Timestamp.Local().Format("15:04"))
							label.Color = color.NRGBA{R: 160, G: 185, B: 220, A: 255}
							return label.Layout(gtx)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					sel := w.messageSelectable(message.ID)
					sel.SetText(message.Body)
					textColor := color.NRGBA{R: 245, G: 247, B: 250, A: 255}
					selColor := color.NRGBA{R: 72, G: 96, B: 140, A: 180}

					textMacro := op.Record(gtx.Ops)
					paint.ColorOp{Color: textColor}.Add(gtx.Ops)
					textMaterial := textMacro.Stop()

					selMacro := op.Record(gtx.Ops)
					paint.ColorOp{Color: selColor}.Add(gtx.Ops)
					selMaterial := selMacro.Stop()

					return sel.Layout(gtx, w.theme.Shaper, font.Font{Typeface: w.theme.Face}, w.theme.TextSize, textMaterial, selMaterial)
				}),
			}

			if isMine && (message.DeliveredAt != nil || message.ReceiptStatus == "queued" || message.ReceiptStatus == "retrying" || message.ReceiptStatus == "failed" || message.ReceiptStatus == "expired" || message.ReceiptStatus == "sent" || message.ReceiptStatus == "delivered" || message.ReceiptStatus == "seen") {
				children = append(children,
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						statusText := ""
						switch {
						case message.ReceiptStatus == "seen" && message.DeliveredAt != nil:
							statusText = "✓✓ " + message.DeliveredAt.Local().Format("15:04")
						case message.ReceiptStatus == "seen":
							statusText = "✓✓"
						case message.ReceiptStatus == "delivered" && message.DeliveredAt != nil:
							statusText = "✓ " + message.DeliveredAt.Local().Format("15:04")
						case message.ReceiptStatus == "delivered":
							statusText = "✓"
						case message.DeliveredAt != nil:
							statusText = "✓ " + message.DeliveredAt.Local().Format("15:04")
						case message.ReceiptStatus == "queued":
							statusText = w.t("chat.status.queued")
						case message.ReceiptStatus == "retrying":
							statusText = w.t("chat.status.retrying")
						case message.ReceiptStatus == "failed":
							statusText = w.t("chat.status.failed")
						case message.ReceiptStatus == "expired":
							statusText = w.t("chat.status.expired")
						case message.ReceiptStatus == "sent":
							statusText = w.t("chat.status.sent")
						}
						return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, statusText)
							label.Color = statusColor
							return label.Layout(gtx)
						})
					}),
				)
			}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
		})
	})
}

// messageSelectable returns a reusable Selectable widget for the given
// message ID, creating one on first access. This allows users to select
// and copy message text in the chat view. The cache is cleared when the
// active conversation changes to prevent unbounded growth across
// different chat peers.
func (w *Window) messageSelectable(id string) *widget.Selectable {
	if peer := w.snap.ActivePeer; peer != w.lastChatPeer {
		w.messageSelectables = make(map[string]*widget.Selectable)
		w.lastChatPeer = peer
	}
	sel := w.messageSelectables[id]
	if sel == nil {
		sel = &widget.Selectable{}
		w.messageSelectables[id] = sel
	}
	return sel
}

func searchKnownIdentities(knownIDs []string, recipients []domain.PeerIdentity, self, query string) []domain.PeerIdentity {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return nil
	}

	alreadyListed := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range recipients {
		alreadyListed[recipient] = struct{}{}
	}

	results := make([]domain.PeerIdentity, 0, len(knownIDs))
	seen := make(map[domain.PeerIdentity]struct{}, len(knownIDs))
	for _, raw := range knownIDs {
		raw = strings.TrimSpace(raw)
		if raw == "" || raw == self {
			continue
		}
		id := domain.PeerIdentity(raw)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := alreadyListed[id]; ok {
			continue
		}
		if !strings.Contains(strings.ToLower(raw), query) {
			continue
		}
		results = append(results, id)
	}

	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	return results
}

func recipientsToChildren(values []domain.PeerIdentity, render func(layout.Context, domain.PeerIdentity) layout.Dimensions) []layout.FlexChild {
	children := make([]layout.FlexChild, 0, len(values))
	for _, value := range values {
		value := value
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return render(gtx, value)
		}))
	}
	return children
}

func shortFingerprint(value string) string {
	if len(value) <= 14 {
		return value
	}
	return value[:8] + "..." + value[len(value)-6:]
}

func joinOrNone(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ", ")
}

func fallback(value, alt string) string {
	if strings.TrimSpace(value) == "" {
		return alt
	}
	return value
}

// peerDisplayName returns the user-assigned alias for the identity,
// falling back to shortFingerprint when no alias is set.
func (w *Window) peerDisplayName(identity domain.PeerIdentity) string {
	if w.prefs != nil {
		if alias := w.prefs.Alias(identity); alias != "" {
			return alias
		}
	}
	return shortFingerprint(string(identity))
}

func (w *Window) t(key string, args ...any) string {
	return translate(w.language, key, args...)
}

func (w *Window) layoutConsoleButton(gtx layout.Context) layout.Dimensions {
	btn := material.Button(w.theme, &w.consoleButton, w.t("header.console"))
	btn.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
	btn.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
	return btn.Layout(gtx)
}

func (w *Window) layoutUpdateBadge(gtx layout.Context) layout.Dimensions {
	if !w.hasNewerPeerBuild() {
		return layout.Dimensions{}
	}
	btn := material.Button(w.theme, &w.updateButton, w.t("header.update"))
	btn.Background = color.NRGBA{R: 230, G: 126, B: 34, A: 255}
	btn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	return btn.Layout(gtx)
}

// hasNewerPeerBuild returns true when at least 2 distinct peer identities
// report a ClientBuild higher than ours. A single peer is not enough
// because someone could build a custom version with an inflated build
// number to trigger false upgrade prompts across the network.
// Deduplication uses PeerID (the peer's mesh identity) so that the same
// node appearing under multiple addresses counts only once.
func (w *Window) hasNewerPeerBuild() bool {
	myBuild := config.ClientBuild
	seen := make(map[string]struct{})
	count := 0
	for _, ph := range w.snap.NodeStatus.PeerHealth {
		if ph.ClientBuild <= myBuild {
			continue
		}
		// Deduplicate by peer identity. Fall back to Address when the
		// identity is not yet known (pre-handshake health entries).
		key := ph.PeerID
		if key == "" {
			key = ph.Address
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		count++
		if count >= 2 {
			return true
		}
	}
	return false
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func (w *Window) openConsoleWindow() {
	w.consoleMu.Lock()
	if w.consoleOpen {
		w.consoleMu.Unlock()
		return
	}
	w.consoleOpen = true
	w.consoleMu.Unlock()

	console := NewConsoleWindow(w, func() {
		w.consoleMu.Lock()
		w.consoleOpen = false
		w.consoleMu.Unlock()
	})
	console.Open()
}

func (w *Window) languageButton(code string) *widget.Clickable {
	if btn, ok := w.languageOptions[code]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.languageOptions[code] = btn
	return btn
}

func (w *Window) layoutLanguageSelectorInline(gtx layout.Context) layout.Dimensions {
	return layout.Flex{
		Axis:      layout.Horizontal,
		Alignment: layout.Middle,
	}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(w.theme, w.t("header.language"))
			label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(w.layoutLanguageDropdown),
	)
}

func (w *Window) layoutLanguageDropdown(gtx layout.Context) layout.Dimensions {
	label := currentLanguageLabel(w.language)
	btn := material.Button(w.theme, &w.languageToggle, label+"  v")
	btn.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
	btn.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
	return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		width := gtx.Dp(unit.Dp(languageButtonWidth))
		if gtx.Constraints.Max.X < width {
			width = gtx.Constraints.Max.X
		}
		gtx.Constraints.Min.X = width
		gtx.Constraints.Max.X = width
		return btn.Layout(gtx)
	})
}

func (w *Window) layoutLanguageOverlay(gtx layout.Context) layout.Dimensions {
	x := gtx.Constraints.Max.X - gtx.Dp(unit.Dp(windowInset)) - gtx.Dp(unit.Dp(languageMenuWidth))
	if x < 0 {
		x = 0
	}
	y := gtx.Dp(unit.Dp(windowInset + languageOverlayTop))

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	defer stack.Pop()

	menuGTX := gtx
	menuGTX.Constraints.Min.X = gtx.Dp(unit.Dp(languageMenuWidth))
	menuGTX.Constraints.Max.X = gtx.Dp(unit.Dp(languageMenuWidth))
	menuGTX.Constraints.Min.Y = gtx.Dp(unit.Dp(languageMenuHeight))
	menuGTX.Constraints.Max.Y = gtx.Dp(unit.Dp(languageMenuHeight))
	_ = w.languageMenuCard(menuGTX)

	return layout.Dimensions{}
}

// layoutContextMenuOverlay renders the right-click context menu for a recipient identity.
// It shows "Copy identity" and "Delete identity" options. Delete requires a confirmation step.
func (w *Window) layoutContextMenuOverlay(gtx layout.Context) layout.Dimensions {
	// Dismiss context menu on click outside.
	// We draw a full-screen transparent clickable area behind the menu.
	dismissArea := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, &w.contextMenuPeer)
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: &w.contextMenuPeer, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			w.contextMenuPeer = ""
			w.showDeleteConfirm = false
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
		}
	}
	dismissArea.Pop()

	menuWidth := gtx.Dp(unit.Dp(220))
	windowW := gtx.Constraints.Max.X
	windowH := gtx.Constraints.Max.Y

	// Measure menu height first so we can flip upward when near the bottom.
	measureGTX := gtx
	measureGTX.Constraints.Min.X = menuWidth
	measureGTX.Constraints.Max.X = menuWidth
	measureGTX.Constraints.Min.Y = 0
	measureGTX.Constraints.Max.Y = windowH
	macro := op.Record(measureGTX.Ops)
	dims := w.contextMenuCard(measureGTX)
	menuCall := macro.Stop()

	x := w.contextMenuPos.X
	y := w.contextMenuPos.Y

	// Flip horizontally if overflows right edge.
	if x+menuWidth > windowW {
		x = windowW - menuWidth
	}
	if x < 0 {
		x = 0
	}

	// Flip vertically if overflows bottom edge.
	if y+dims.Size.Y > windowH {
		y = y - dims.Size.Y
		if y < 0 {
			y = 0
		}
	}

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	menuCall.Add(gtx.Ops)
	stack.Pop()

	return layout.Dimensions{}
}

func (w *Window) contextMenuCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 72, G: 85, B: 106, A: 255}
	bgColor := color.NRGBA{R: 28, G: 34, B: 44, A: 255}
	rr := gtx.Dp(unit.Dp(8))
	borderWidth := gtx.Dp(unit.Dp(1))

	// Measure content to know the total size.
	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(6)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			if w.showDeleteConfirm {
				return w.layoutDeleteConfirmMenu(gtx)
			}
			if w.showAliasEditor {
				return w.layoutAliasEditorMenu(gtx)
			}
			return w.layoutContextMenuItems(gtx)
		})
	})
	contentCall := macro.Stop()

	bounds := image.Rectangle{Max: dims.Size}

	// Clip everything to the rounded rectangle so corners are clean.
	defer clip.UniformRRect(bounds, rr).Push(gtx.Ops).Pop()

	// 1. Border fill (covers the full rounded rect).
	paint.ColorOp{Color: borderColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	// 2. Background fill inset by border width.
	innerBounds := image.Rectangle{
		Min: image.Pt(borderWidth, borderWidth),
		Max: image.Pt(dims.Size.X-borderWidth, dims.Size.Y-borderWidth),
	}
	innerRR := rr - borderWidth
	if innerRR < 0 {
		innerRR = 0
	}
	defer clip.UniformRRect(innerBounds, innerRR).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: bgColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	// 3. Replay content on top.
	contentCall.Add(gtx.Ops)

	return dims
}

func (w *Window) layoutContextMenuItems(gtx layout.Context) layout.Dimensions {
	aliasLabel := w.t("context.set_alias")
	if w.prefs != nil && w.prefs.Alias(w.contextMenuPeer) != "" {
		aliasLabel = w.t("context.edit_alias")
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAlias, aliasLabel,
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuCopy, w.t("context.copy_identity"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDelete, w.t("context.delete_identity"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
	)
}

func (w *Window) contextMenuHeader(gtx layout.Context) layout.Dimensions {
	return layout.Inset{
		Left: unit.Dp(12), Right: unit.Dp(12),
		Top: unit.Dp(8), Bottom: unit.Dp(4),
	}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		label := material.Caption(w.theme, w.peerDisplayName(w.contextMenuPeer))
		label.Color = color.NRGBA{R: 140, G: 155, B: 178, A: 255}
		return label.Layout(gtx)
	})
}

func (w *Window) contextMenuSeparator(gtx layout.Context) layout.Dimensions {
	return layout.Inset{
		Left: unit.Dp(8), Right: unit.Dp(8),
		Top: unit.Dp(2), Bottom: unit.Dp(4),
	}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		height := gtx.Dp(unit.Dp(1))
		sz := image.Pt(gtx.Constraints.Max.X, height)
		defer clip.Rect(image.Rectangle{Max: sz}).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: color.NRGBA{R: 55, G: 65, B: 82, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		return layout.Dimensions{Size: sz}
	})
}

func (w *Window) layoutDeleteConfirmMenu(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("context.delete_confirm"))
			label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
			return layout.Inset{Left: unit.Dp(12), Right: unit.Dp(12), Top: unit.Dp(2), Bottom: unit.Dp(6)}.Layout(gtx, label.Layout)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteConfirm, w.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteCancel, w.t("context.delete_no"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

func (w *Window) layoutAliasEditorMenu(gtx layout.Context) layout.Dimensions {
	// Handle Enter key as save shortcut.
	for {
		ev, ok := w.aliasEditor.Update(gtx)
		if !ok {
			break
		}
		if submit, ok := ev.(widget.SubmitEvent); ok {
			alias := strings.TrimSpace(submit.Text)
			if w.prefs != nil {
				w.prefs.SetAlias(w.contextMenuPeer, alias)
				_ = w.prefs.Save()
			}
			w.contextMenuPeer = ""
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
			return layout.Dimensions{}
		}
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Left: unit.Dp(12), Right: unit.Dp(12),
				Top: unit.Dp(4), Bottom: unit.Dp(6),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				ed := material.Editor(w.theme, &w.aliasEditor, w.t("context.alias_placeholder"))
				ed.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				ed.HintColor = color.NRGBA{R: 120, G: 135, B: 158, A: 255}
				gtx.Constraints.Min.X = gtx.Dp(unit.Dp(160))
				return ed.Layout(gtx)
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasSave, w.t("context.alias_save"),
				color.NRGBA{R: 130, G: 200, B: 130, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasCancel, w.t("context.alias_cancel"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

func (w *Window) contextMenuItem(gtx layout.Context, btn *widget.Clickable, label string, fg color.NRGBA) layout.Dimensions {
	hoverBg := color.NRGBA{R: 42, G: 52, B: 68, A: 255}

	return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
		if btn.Hovered() {
			fill(gtx, hoverBg)
		}
		return layout.Inset{
			Top: unit.Dp(8), Bottom: unit.Dp(8),
			Left: unit.Dp(12), Right: unit.Dp(12),
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			lbl := material.Body2(w.theme, label)
			lbl.Color = fg
			return lbl.Layout(gtx)
		})
	})
}

func (w *Window) languageMenuCard(gtx layout.Context) layout.Dimensions {
	fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

	return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		items := make([]layout.FlexChild, 0, len(supportedLanguages)*2)
		for i, option := range supportedLanguages {
			opt := option
			btn := w.languageButton(opt.Code)
			items = append(items, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				for btn.Clicked(gtx) {
					w.language = normalizeLanguage(opt.Code)
					w.showLanguageMenu = false
					if w.prefs != nil {
						w.prefs.Language = w.language
						_ = w.prefs.Save()
					}
					if w.window != nil {
						w.window.Invalidate()
					}
				}

				style := material.Button(w.theme, btn, opt.Label+" - "+localizedLanguageName(opt.Code))
				if opt.Code == w.language {
					style.Background = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
				} else {
					style.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
				}
				style.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				return style.Layout(gtx)
			}))
			if i < len(supportedLanguages)-1 {
				items = append(items, layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout))
			}
		}
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, items...)
	})
}

func (w *Window) card(gtx layout.Context, titleText string, rows []string, extras ...func(layout.Context) layout.Dimensions) layout.Dimensions {
	// Record the content layout first so we know the actual height,
	// then draw the background to match. Without this, fill() would
	// use gtx.Constraints.Max.Y which stretches Rigid cards (like the
	// composer) to the bottom of the window.
	macro := op.Record(gtx.Ops)

	inset := layout.UniformInset(unit.Dp(8))
	dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := make([]layout.FlexChild, 0, len(rows)+len(extras)+2)
		if strings.TrimSpace(titleText) != "" {
			children = append(children,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(w.theme, unit.Sp(16), titleText)
					label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
			)
		}

		for _, row := range rows {
			text := row
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(w.theme, text)
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}))
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout))
		}

		for _, extra := range extras {
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout))
			children = append(children, layout.Rigid(extra))
		}

		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx, children...)
	})

	contentOps := macro.Stop()

	// Draw the background sized to the actual content (or Max.Y for
	// Flexed cards like the chat area that should fill available space).
	bgHeight := dims.Size.Y
	if gtx.Constraints.Min.Y > bgHeight {
		bgHeight = gtx.Constraints.Min.Y
	}
	bgStack := clip.Rect{Max: image.Pt(gtx.Constraints.Max.X, bgHeight)}.Push(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 21, G: 26, B: 34, A: 255}}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	bgStack.Pop()

	contentOps.Add(gtx.Ops)
	return dims
}

func fill(gtx layout.Context, c color.NRGBA) {
	stack := clip.Rect{Max: image.Pt(gtx.Constraints.Max.X, gtx.Constraints.Max.Y)}.Push(gtx.Ops)
	defer stack.Pop()
	paint.ColorOp{Color: c}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}
