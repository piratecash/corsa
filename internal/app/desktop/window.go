package desktop

import (
	"context"
	"image"
	"image/color"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/io/clipboard"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

type Window struct {
	client  *service.DesktopClient
	runtime *NodeRuntime
	prefs   *Preferences
	theme   *material.Theme
	ops     op.Ops

	recipientEditor    widget.Editor
	messageEditor      widget.Editor
	contactsList       widget.List
	chatList           widget.List
	sendButton         widget.Clickable
	syncButton         widget.Clickable
	copyIdentityButton widget.Clickable
	rebuildTrustButton widget.Clickable
	languageToggle     widget.Clickable
	languageOptions    map[string]*widget.Clickable
	recipientButtons   map[string]*widget.Clickable
	selectedRecipient  string
	language           string
	showLanguageMenu   bool
	sendStatus         string

	mu         sync.RWMutex
	nodeStatus service.NodeStatus
	window     *app.Window
}

const (
	languageButtonWidth = 76
	languageMenuWidth   = 220
	languageOverlayTop  = 58
	windowInset         = 24
	languageMenuHeight  = 316
)

func NewWindow(client *service.DesktopClient, runtime *NodeRuntime, prefs *Preferences) *Window {
	theme := material.NewTheme()
	theme.Bg = color.NRGBA{R: 18, G: 21, B: 26, A: 255}
	theme.Fg = color.NRGBA{R: 235, G: 239, B: 244, A: 255}
	theme.ContrastBg = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	theme.ContrastFg = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

	language := normalizeLanguage(client.Language())
	if prefs != nil && prefs.Language != "" {
		language = normalizeLanguage(prefs.Language)
	}

	return &Window{
		client:           client,
		runtime:          runtime,
		prefs:            prefs,
		theme:            theme,
		language:         language,
		languageOptions:  make(map[string]*widget.Clickable),
		recipientButtons: make(map[string]*widget.Clickable),
		sendStatus:       translate(language, "status.compose_default"),
		contactsList:     widget.List{List: layout.List{Axis: layout.Vertical}},
		chatList:         widget.List{List: layout.List{Axis: layout.Vertical}},
	}
}

func (w *Window) Run() error {
	go func() {
		window := new(app.Window)
		w.window = window
		window.Option(
			app.Title(w.t("app.title")),
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
	events, cancel := w.client.SubscribeLocalChanges()

	go func() {
		defer cancel()
		for range events {
			w.refreshStatus()
		}
	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			w.refreshStatus()
			<-ticker.C
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
	w.handleActions(gtx)
	fill(gtx, color.NRGBA{R: 12, G: 15, B: 20, A: 255})

	inset := layout.UniformInset(unit.Dp(24))
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(w.layoutHeader),
					layout.Rigid(layout.Spacer{Height: unit.Dp(18)}.Layout),
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
	)
}

func (w *Window) handleActions(gtx layout.Context) {
	for w.languageToggle.Clicked(gtx) {
		w.showLanguageMenu = !w.showLanguageMenu
	}

	for w.sendButton.Clicked(gtx) {
		w.triggerSend()
	}

	w.messageEditor.Submit = true
	for {
		event, ok := w.messageEditor.Update(gtx)
		if !ok {
			break
		}
		if _, ok := event.(widget.SubmitEvent); ok {
			w.triggerSend()
		}
	}

	for w.copyIdentityButton.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(w.client.Address())),
		})
		w.sendStatus = w.t("status.identity_copied")
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	for w.syncButton.Clicked(gtx) {
		recipient := strings.TrimSpace(w.selectedRecipient)
		if recipient == "" {
			w.sendStatus = w.t("compose.select_first")
			continue
		}

		peers := append([]string(nil), w.currentStatus().Peers...)
		if len(peers) == 0 {
			w.sendStatus = w.t("chat.sync_disabled")
			continue
		}

		w.sendStatus = w.t("status.syncing")
		go func(recipient string, peers []string) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			imported, err := w.client.SyncDirectMessagesFromPeers(ctx, peers, recipient)
			cancel()

			w.mu.Lock()
			if err != nil {
				w.sendStatus = w.t("status.sync_failed", err.Error())
			} else {
				w.sendStatus = w.t("status.sync_done", imported)
			}
			w.mu.Unlock()

			w.refreshStatus()
			if w.window != nil {
				w.window.Invalidate()
			}
		}(recipient, peers)
	}

	for w.rebuildTrustButton.Clicked(gtx) {
		w.sendStatus = w.t("status.trust_rebuilding")

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			count, err := w.client.RebuildTrust(ctx)
			cancel()

			w.mu.Lock()
			if err != nil {
				w.sendStatus = w.t("status.trust_failed", err.Error())
			} else {
				w.sendStatus = w.t("status.trust_rebuilt", count)
			}
			w.mu.Unlock()

			w.refreshStatus()
			if w.window != nil {
				w.window.Invalidate()
			}
		}()
	}
}

func (w *Window) triggerSend() {
	to := strings.TrimSpace(w.selectedRecipient)
	if to == "" {
		to = strings.TrimSpace(w.recipientEditor.Text())
	}
	body := strings.TrimSpace(w.messageEditor.Text())
	if body == "" {
		return
	}
	w.sendStatus = w.t("status.sending")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := w.client.SendDirectMessage(ctx, to, body)
		cancel()

		w.mu.Lock()
		if err != nil {
			w.sendStatus = w.t("status.send_failed", err.Error())
		} else {
			w.sendStatus = w.t("status.message_sent")
			w.messageEditor.SetText("")
		}
		w.mu.Unlock()

		if err == nil {
			w.refreshStatus()
		}

		if w.window != nil {
			w.window.Invalidate()
		}
	}()
}

func (w *Window) layoutHeader(gtx layout.Context) layout.Dimensions {
	title := material.H3(w.theme, w.t("app.title"))
	title.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}

	subtitle := material.Body1(w.theme, w.t("app.subtitle"))
	subtitle.Color = color.NRGBA{R: 144, G: 156, B: 173, A: 255}

	return layout.Flex{
		Axis:      layout.Horizontal,
		Spacing:   layout.SpaceBetween,
		Alignment: layout.Start,
	}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				layout.Rigid(subtitle.Layout),
			)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(16)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.E.Layout(gtx, w.layoutLanguageSelector)
		}),
	)
}

func (w *Window) layoutMain(gtx layout.Context) layout.Dimensions {
	status := w.currentStatus()
	recipients := knownRecipients(status.KnownIDs, status.Contacts, w.client.Address())
	w.ensureSelectedRecipient(recipients)

	return layout.Flex{
		Axis: layout.Vertical,
	}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis:    layout.Horizontal,
				Spacing: layout.SpaceBetween,
			}.Layout(gtx,
				layout.Flexed(0.3, func(gtx layout.Context) layout.Dimensions {
					return w.layoutContactsCard(gtx, recipients)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(18)}.Layout),
				layout.Flexed(0.7, func(gtx layout.Context) layout.Dimensions {
					return w.layoutChatCard(gtx, status)
				}),
			)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(18)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis:    layout.Horizontal,
				Spacing: layout.SpaceBetween,
			}.Layout(gtx,
				layout.Flexed(0.3, func(gtx layout.Context) layout.Dimensions {
					return w.layoutNodeSummary(gtx, status)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(18)}.Layout),
				layout.Flexed(0.7, func(gtx layout.Context) layout.Dimensions {
					return w.layoutComposerCard(gtx)
				}),
			)
		}),
	)
}

func (w *Window) layoutContactsCard(gtx layout.Context, recipients []string) layout.Dimensions {
	rows := []string{
		w.t("clients.you", w.client.Address()),
		w.t("clients.known", len(recipients)),
	}

	return w.card(gtx, w.t("clients.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(220)))
			btn := material.Button(w.theme, &w.copyIdentityButton, w.t("clients.copy_identity"))
			return btn.Layout(gtx)
		})
	}, func(gtx layout.Context) layout.Dimensions {
		if len(recipients) == 0 {
			label := material.Body1(w.theme, w.t("clients.empty"))
			label.Color = color.NRGBA{R: 165, G: 177, B: 194, A: 255}
			return label.Layout(gtx)
		}

		maxY := gtx.Dp(unit.Dp(420))
		if gtx.Constraints.Max.Y > maxY {
			gtx.Constraints.Max.Y = maxY
		}
		list := material.List(w.theme, &w.contactsList)
		return list.Layout(gtx, len(recipients), func(gtx layout.Context, index int) layout.Dimensions {
			fingerprint := recipients[index]
			btn := w.recipientButton(fingerprint)
			return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				for btn.Clicked(gtx) {
					w.selectedRecipient = fingerprint
					w.recipientEditor.SetText(fingerprint)
					w.sendStatus = w.t("status.chat_selected")
				}

				style := material.Button(w.theme, btn, fingerprint)
				if fingerprint == w.selectedRecipient {
					style.Background = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
				} else {
					style.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
				}
				style.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				return style.Layout(gtx)
			})
		})
	})
}

func (w *Window) layoutNodeSummary(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	rows := []string{
		w.t("node.client_version", w.client.Version()),
		w.t("node.peer_version", fallback(status.ClientVersion, strings.ReplaceAll(w.client.Version(), " ", "-"))),
		w.t("node.listen", w.runtime.ListenAddress()),
		w.t("node.type", fallback(status.NodeType, "full")),
		w.t("node.services", fallback(joinOrNone(status.Services), "identity,contacts,messages,gazeta,relay")),
		w.t("node.connected", status.Connected),
		w.t("node.peers", len(status.Peers)),
		w.localNodeErrorRow(),
	}

	if !status.CheckedAt.IsZero() {
		rows = append(rows, w.t("node.checked", status.CheckedAt.Format(time.RFC3339)))
	}

	return w.card(gtx, w.t("node.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(240)))
			btn := material.Button(w.theme, &w.rebuildTrustButton, w.t("node.trust_rebuild"))
			return btn.Layout(gtx)
		})
	})
}

func (w *Window) layoutChatCard(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	recipient := w.selectedRecipient
	title := w.t("chat.title")
	rows := []string{}

	if recipient == "" {
		rows = append(rows, w.t("chat.choose"))
		return w.card(gtx, title, rows)
	}

	title = w.t("chat.with", shortFingerprint(recipient))
	rows = append(rows,
		w.t("chat.fingerprint", recipient),
		w.t("chat.peers", len(status.Peers)),
	)

	conversation := w.conversationEntries(status, recipient)
	if len(conversation) == 0 {
		rows = append(rows, w.t("chat.empty"))
		return w.card(gtx, title, rows)
	}

	return w.card(gtx, title, rows, func(gtx layout.Context) layout.Dimensions {
		return w.layoutConversation(gtx, recipient, conversation)
	})
}

func (w *Window) layoutComposerCard(gtx layout.Context) layout.Dimensions {
	recipient := w.selectedRecipient
	status := w.currentStatus()
	rows := []string{
		w.sendStatusLine(),
	}

	if recipient == "" {
		rows = append(rows, w.t("compose.recipient.select"))
	} else {
		rows = append(rows, w.t("compose.recipient.value", recipient))
	}

	return w.card(gtx, w.t("compose.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.messageInputCard(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(16)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis:      layout.Horizontal,
					Spacing:   layout.SpaceBetween,
					Alignment: layout.Middle,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						canSync := recipient != "" && len(status.Peers) > 0
						return w.layoutChatActions(gtx, canSync)
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

func (w *Window) messageInputCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
	backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
	cardHeight := gtx.Dp(unit.Dp(230))

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.Y = cardHeight
		gtx.Constraints.Max.Y = cardHeight
		fill(gtx, borderColor)

		return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			fill(gtx, backgroundColor)

			inset := layout.UniformInset(unit.Dp(14))
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Body2(w.theme, w.t("compose.body"))
						label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
						return label.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						w.messageEditor.SingleLine = false
						editor := material.Editor(w.theme, &w.messageEditor, w.t("compose.placeholder"))
						editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
						editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}

						height := gtx.Dp(unit.Dp(160))
						gtx.Constraints.Min.Y = height
						gtx.Constraints.Max.Y = height
						return editor.Layout(gtx)
					}),
				)
			})
		})
	})
}

func (w *Window) layoutChatActions(gtx layout.Context, canSync bool) layout.Dimensions {
	label := w.t("chat.sync")
	if !canSync {
		label = w.t("chat.sync_disabled")
	}

	return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(280)))
		btn := material.Button(w.theme, &w.syncButton, label)
		if !canSync {
			btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
		}
		return btn.Layout(gtx)
	})
}

func (w *Window) localNodeErrorRow() string {
	if err := w.runtime.Error(); err != "" {
		return w.t("node.error", err)
	}
	return w.t("node.error", w.t("node.error.none"))
}

func (w *Window) refreshStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	status := w.client.ProbeNode(ctx)
	cancel()

	recipient := strings.TrimSpace(w.selectedRecipient)
	if recipient != "" {
		go func(recipient string, messages []service.DirectMessage) {
			seenCtx, seenCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = w.client.MarkConversationSeen(seenCtx, recipient, messages)
			seenCancel()
		}(recipient, append([]service.DirectMessage(nil), status.DirectMessages...))
	}

	w.mu.Lock()
	w.nodeStatus = status
	w.mu.Unlock()

	if w.window != nil {
		w.window.Invalidate()
	}
}

func (w *Window) currentStatus() service.NodeStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.nodeStatus
}

func (w *Window) sendStatusLine() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.sendStatus
}

func (w *Window) recipientButton(id string) *widget.Clickable {
	if btn, ok := w.recipientButtons[id]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.recipientButtons[id] = btn
	return btn
}

func (w *Window) ensureSelectedRecipient(recipients []string) {
	if len(recipients) == 0 {
		if strings.TrimSpace(w.selectedRecipient) != "" {
			w.recipientEditor.SetText(w.selectedRecipient)
		} else {
			w.recipientEditor.SetText("")
		}
		return
	}

	for _, recipient := range recipients {
		if recipient == w.selectedRecipient {
			w.recipientEditor.SetText(recipient)
			return
		}
	}

	if strings.TrimSpace(w.selectedRecipient) != "" {
		w.recipientEditor.SetText(w.selectedRecipient)
		return
	}

	w.selectedRecipient = recipients[0]
	w.recipientEditor.SetText(recipients[0])
}

func (w *Window) conversationEntries(status service.NodeStatus, recipient string) []service.DirectMessage {
	me := w.client.Address()
	rows := make([]service.DirectMessage, 0, len(status.DirectMessages))

	for _, raw := range status.DirectMessages {
		sender := raw.Sender
		target := raw.Recipient

		switch {
		case sender == me && target == recipient:
			rows = append(rows, raw)
		case sender == recipient && target == me:
			rows = append(rows, raw)
		}
	}

	return rows
}

func (w *Window) layoutConversation(gtx layout.Context, recipient string, conversation []service.DirectMessage) layout.Dimensions {
	list := material.List(w.theme, &w.chatList)
	return list.Layout(gtx, len(conversation), func(gtx layout.Context, index int) layout.Dimensions {
		message := conversation[index]
		return layout.Inset{Bottom: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.layoutChatBubble(gtx, recipient, message)
		})
	})
}

func (w *Window) layoutChatBubble(gtx layout.Context, recipient string, message service.DirectMessage) layout.Dimensions {
	me := w.client.Address()
	isMine := message.Sender == me

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		if isMine {
			return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.chatBubbleCard(gtx, message, true, w.t("chat.you_label"))
			})
		}
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.chatBubbleCard(gtx, message, false, shortFingerprint(recipient))
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
	statusColor := color.NRGBA{R: 142, G: 178, B: 230, A: 255}
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
						Spacing:   layout.SpaceBetween,
						Alignment: layout.Middle,
					}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, author)
							label.Color = authorColor
							return label.Layout(gtx)
						}),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return layout.Dimensions{}
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, message.Timestamp.Format("15:04"))
							label.Color = color.NRGBA{R: 132, G: 144, B: 160, A: 255}
							return label.Layout(gtx)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body1(w.theme, message.Body)
					label.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
					return label.Layout(gtx)
				}),
			}

			if isMine && message.DeliveredAt != nil {
				children = append(children,
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						statusText := "✓ " + message.DeliveredAt.Format("15:04")
						if message.ReceiptStatus == "seen" {
							statusText = "✓✓ " + message.DeliveredAt.Format("15:04")
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

func knownRecipients(ids []string, contacts map[string]service.Contact, self string) []string {
	known := make(map[string]struct{}, len(ids)+len(contacts))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || id == self {
			continue
		}
		known[id] = struct{}{}
	}
	for id := range contacts {
		id = strings.TrimSpace(id)
		if id == "" || id == self {
			continue
		}
		known[id] = struct{}{}
	}

	recipients := make([]string, 0, len(known))
	for id := range known {
		recipients = append(recipients, id)
	}
	sort.Strings(recipients)
	return recipients
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

func (w *Window) t(key string, args ...any) string {
	return translate(w.language, key, args...)
}

func (w *Window) languageButton(code string) *widget.Clickable {
	if btn, ok := w.languageOptions[code]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.languageOptions[code] = btn
	return btn
}

func (w *Window) layoutLanguageSelector(gtx layout.Context) layout.Dimensions {
	return layout.Flex{
		Axis:      layout.Vertical,
		Alignment: layout.End,
	}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(w.theme, w.t("header.language"))
			label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
			return layout.E.Layout(gtx, label.Layout)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
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
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

		inset := layout.UniformInset(unit.Dp(18))
		return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			children := make([]layout.FlexChild, 0, len(rows)+len(extras)+2)
			if strings.TrimSpace(titleText) != "" {
				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Label(w.theme, unit.Sp(20), titleText)
						label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
						return label.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				)
			}

			for _, row := range rows {
				text := row
				children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body1(w.theme, text)
					label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
					return label.Layout(gtx)
				}))
				children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout))
			}

			for _, extra := range extras {
				children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout))
				children = append(children, layout.Rigid(extra))
			}

			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx, children...)
		})
	})
}

func fill(gtx layout.Context, c color.NRGBA) {
	stack := clip.Rect{Max: image.Pt(gtx.Constraints.Max.X, gtx.Constraints.Max.Y)}.Push(gtx.Ops)
	defer stack.Pop()
	paint.ColorOp{Color: c}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}
