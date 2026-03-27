package desktop

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"io"
	"strings"
	"sync"
	"time"

	"corsa/internal/core/rpc"
	"corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

const (
	defaultConsoleWindowWidth  = unit.Dp(1140)
	defaultConsoleWindowHeight = unit.Dp(1020)
	maxVisibleSuggestions      = 5
)

type consoleTab int

const (
	consoleTabConsole consoleTab = iota
	consoleTabPeers
	consoleTabInfo
)

type consoleEntry struct {
	Command    string
	Output     string
	Failed     bool
	CreatedAt  time.Time
	OutputText widget.Selectable
	CopyButton widget.Clickable
}

type consoleSuggestion struct {
	Label  string
	Insert string
}

type ConsoleWindow struct {
	parent           *Window
	theme            *material.Theme
	ops              op.Ops
	onClose          func()
	window           *app.Window
	closed           chan struct{} // closed when DestroyEvent is received; used as a hard gate for cross-goroutine Invalidate
	peerList         widget.List
	peerSectionList  widget.List
	historyList      widget.List
	suggestList      widget.List
	consoleEditor    widget.Editor
	runButton        widget.Clickable
	consoleTabButton widget.Clickable
	peersTabButton   widget.Clickable
	infoTabButton    widget.Clickable
	activeTab        consoleTab
	mu               sync.RWMutex
	consoleEntries   []consoleEntry
	consoleBusy      bool
	suggestButtons   map[string]*widget.Clickable
	lastSuggestQuery string
	hideSuggestions  bool
	selectedSuggest  int
	suggestBaseQuery string
	suggestSnapshot  []consoleSuggestion
	cachedCommands   []consoleSuggestion // loaded from CommandTable at init
}

func NewConsoleWindow(parent *Window, onClose func()) *ConsoleWindow {
	window := &ConsoleWindow{
		parent:  parent,
		theme:   newAppTheme(),
		onClose: onClose,
		closed:  make(chan struct{}),
		peerList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		peerSectionList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		historyList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		suggestList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		suggestButtons:  make(map[string]*widget.Clickable),
		selectedSuggest: -1,
	}
	window.consoleEditor.SingleLine = true
	window.consoleEntries = []consoleEntry{
		newConsoleEntry(consoleEntry{
			Command:   "help",
			Output:    parent.t("console.welcome"),
			CreatedAt: time.Now(),
		}),
	}

	// Load available commands directly from CommandTable — no HTTP, always available.
	if parent.cmdTable != nil {
		window.loadCommands()
	}

	return window
}

func (c *ConsoleWindow) Open() {
	go func() {
		window := new(app.Window)

		c.mu.Lock()
		c.window = window
		c.mu.Unlock()

		window.Option(
			app.Title(c.parent.t("console.title")),
			app.Size(defaultConsoleWindowWidth, defaultConsoleWindowHeight),
		)

		// Periodic refresh goroutine.  Uses c.closed as a hard stop so it
		// never calls Invalidate after DestroyEvent begins processing.
		// The actual Invalidate goes through invalidateWindow() which holds
		// RLock during the call, preventing DestroyEvent from nilling out
		// the window handle until Invalidate returns.
		ticker := time.NewTicker(2 * time.Second)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-c.closed:
					return
				case <-ticker.C:
					c.invalidateWindow()
				}
			}
		}()

		for {
			switch e := window.Event().(type) {
			case app.DestroyEvent:
				// Signal all cross-goroutine callers (ticker, command goroutine)
				// to stop touching the window BEFORE the native handle is freed.
				close(c.closed)

				c.mu.Lock()
				c.window = nil
				c.mu.Unlock()

				if c.onClose != nil {
					c.onClose()
				}
				return
			case app.FrameEvent:
				gtx := app.NewContext(&c.ops, e)
				c.layout(gtx)
				e.Frame(gtx.Ops)
			}
		}
	}()
}

func (c *ConsoleWindow) layout(gtx layout.Context) layout.Dimensions {
	c.handleActions(gtx)
	fill(gtx, colorBackground())
	inset := layout.UniformInset(unit.Dp(24))
	return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(c.layoutTabs),
			layout.Rigid(layout.Spacer{Height: unit.Dp(16)}.Layout),
			layout.Flexed(1, c.layoutActiveTab),
		)
	})
}

func colorBackground() color.NRGBA {
	return color.NRGBA{R: 12, G: 15, B: 20, A: 255}
}

func (c *ConsoleWindow) handleActions(gtx layout.Context) {
	c.syncSuggestionVisibility()
	suggestions := c.consoleSuggestions()

	for c.consoleTabButton.Clicked(gtx) {
		c.activeTab = consoleTabConsole
	}
	for c.peersTabButton.Clicked(gtx) {
		c.activeTab = consoleTabPeers
	}
	for c.infoTabButton.Clicked(gtx) {
		c.activeTab = consoleTabInfo
	}
	for c.runButton.Clicked(gtx) {
		c.submitConsoleCommand()
	}
	for _, item := range suggestions {
		btn := c.suggestionButton(item.Label)
		for btn.Clicked(gtx) {
			c.applySuggestion(gtx, item.Insert)
		}
	}

	for {
		ev, ok := gtx.Event(
			key.Filter{Focus: &c.consoleEditor, Name: key.NameDownArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameUpArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameRightArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameSpace},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameEscape},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameTab},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameEnter},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameReturn},
		)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		switch ke.Name {
		case key.NameDownArrow:
			c.moveSuggestionSelection(1, suggestions)
			continue
		case key.NameUpArrow:
			c.moveSuggestionSelection(-1, suggestions)
			continue
		case key.NameRightArrow, key.NameSpace:
			if c.commitSuggestionForArguments(gtx, suggestions) {
				continue
			}
		case key.NameEscape:
			if c.cancelSuggestions(gtx) {
				continue
			}
		case key.NameTab:
			if c.applySelectedSuggestion(gtx, suggestions, true) {
				continue
			}
		case key.NameEnter, key.NameReturn:
			if c.selectedSuggest >= 0 && len(suggestions) > 0 {
				c.submitConsoleCommand()
				continue
			}
			if c.applySelectedSuggestion(gtx, suggestions, false) {
				continue
			}
		}
		c.submitConsoleCommand()
	}
}

func (c *ConsoleWindow) layoutTabs(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.consoleTabButton, c.activeTab == consoleTabConsole, c.parent.t("console.tab.console"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.peersTabButton, c.activeTab == consoleTabPeers, c.parent.t("console.tab.peers"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.infoTabButton, c.activeTab == consoleTabInfo, c.parent.t("console.tab.info"))
		}),
	)
}

func (c *ConsoleWindow) layoutTabButton(gtx layout.Context, clickable *widget.Clickable, active bool, labelText string) layout.Dimensions {
	return material.Clickable(gtx, clickable, func(gtx layout.Context) layout.Dimensions {
		bg := color.NRGBA{R: 34, G: 46, B: 62, A: 255}
		fg := color.NRGBA{R: 220, G: 228, B: 240, A: 255}
		if active {
			bg = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
			fg = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
		}
		fill(gtx, bg)
		return layout.Inset{Top: unit.Dp(10), Bottom: unit.Dp(10), Left: unit.Dp(16), Right: unit.Dp(16)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Body1(c.theme, labelText)
			label.Color = fg
			label.Font.Weight = 600
			return label.Layout(gtx)
		})
	})
}

func (c *ConsoleWindow) layoutActiveTab(gtx layout.Context) layout.Dimensions {
	status := c.parent.router.Snapshot().NodeStatus
	switch c.activeTab {
	case consoleTabPeers:
		return c.layoutPeersTab(gtx, status)
	case consoleTabInfo:
		return c.layoutInfoTab(gtx, status)
	default:
		return c.layoutConsoleTab(gtx)
	}
}

func (c *ConsoleWindow) infoRows(status service.NodeStatus) []string {
	connectedPeers := countConnectedPeers(status.PeerHealth)
	rows := []string{
		c.parent.t("node.client_version", c.parent.client.Version()),
		c.parent.t("node.peer_version", fallback(status.ClientVersion, strings.ReplaceAll(c.parent.client.Version(), " ", "-"))),
		c.parent.t("node.listener", status.ListenerEnabled),
		c.parent.t("node.listen", fallback(status.ListenerAddress, c.parent.runtime.ListenAddress())),
		c.parent.t("node.type", fallback(status.NodeType, "full")),
		c.parent.t("node.services", fallback(joinOrNone(status.Services), "identity,contacts,messages,gazeta,relay")),
		c.parent.t("node.connected", status.Connected),
		c.parent.t("node.known_peers", len(status.Peers)),
		c.parent.t("node.connected_peers", connectedPeers),
		c.parent.localNodeErrorRow(),
	}

	if !status.CheckedAt.IsZero() {
		rows = append(rows, c.parent.t("node.checked", status.CheckedAt.Format(time.RFC3339)))
	}

	return rows
}

func (c *ConsoleWindow) layoutInfoTab(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	return c.parent.card(gtx, c.parent.t("console.info_title"), c.infoRows(status))
}

func (c *ConsoleWindow) layoutPeersTab(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	connectedPeers := connectedPeerHealth(status.PeerHealth)
	pendingPeers := nonConnectedPeerHealth(status.PeerHealth)
	knownOnlyPeers := knownOnlyPeers(status.Peers, status.PeerHealth)
	rows := []string{
		c.parent.t("node.connected_peers", len(connectedPeers)),
	}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		return layout.UniformInset(unit.Dp(18)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme, unit.Sp(20), c.parent.t("console.peers_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			summary := material.Body1(c.theme, peerHealthSummary(c.parent, status))
			summary.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutInfoRows(gtx, rows)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
				layout.Rigid(summary.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					if len(connectedPeers) == 0 && len(pendingPeers) == 0 && len(knownOnlyPeers) == 0 {
						label := material.Body1(c.theme, c.parent.t("console.peers_empty"))
						label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
						return label.Layout(gtx)
					}
					return c.layoutPeersContent(gtx, connectedPeers, pendingPeers, knownOnlyPeers)
				}),
			)
		})
	})
}

func (c *ConsoleWindow) layoutConsoleTab(gtx layout.Context) layout.Dimensions {
	rows := []string{
		c.parent.t("console.help"),
	}

	return c.parent.card(gtx, c.parent.t("console.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.Stack{}.Layout(gtx,
			layout.Expanded(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
					layout.Rigid(c.layoutConsoleInput),
					layout.Rigid(layout.Spacer{Height: unit.Dp(16)}.Layout),
					layout.Flexed(1, c.layoutConsoleHistory),
				)
			}),
			layout.Stacked(func(gtx layout.Context) layout.Dimensions {
				suggestions := c.consoleSuggestions()
				if len(suggestions) == 0 {
					return layout.Dimensions{}
				}
				return layout.Inset{Top: unit.Dp(66)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(560)))
					return c.layoutConsoleSuggestions(gtx, suggestions)
				})
			}),
		)
	})
}

func (c *ConsoleWindow) layoutConsoleInput(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(c.theme, c.parent.t("console.input_label"))
			label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
					backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
					height := gtx.Dp(unit.Dp(54))
					return layout.Stack{}.Layout(gtx,
						layout.Expanded(func(gtx layout.Context) layout.Dimensions {
							gtx.Constraints.Min.Y = height
							gtx.Constraints.Max.Y = height
							fill(gtx, borderColor)
							return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								fill(gtx, backgroundColor)
								return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
									editor := material.Editor(c.theme, &c.consoleEditor, c.parent.t("console.placeholder"))
									editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
									editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
									return editor.Layout(gtx)
								})
							})
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := c.parent.t("console.run")
					if c.isConsoleBusy() {
						label = c.parent.t("console.running")
					}
					btn := material.Button(c.theme, &c.runButton, label)
					if c.isConsoleBusy() {
						btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
					}
					return btn.Layout(gtx)
				}),
			)
		}),
	)
}

func (c *ConsoleWindow) layoutConsoleSuggestions(gtx layout.Context, suggestions []consoleSuggestion) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		headerHeight := gtx.Dp(unit.Dp(28))
		headerGap := gtx.Dp(unit.Dp(8))
		itemHeight := gtx.Dp(unit.Dp(62))
		visibleItems := len(suggestions)
		if visibleItems > maxVisibleSuggestions {
			visibleItems = maxVisibleSuggestions
		}
		listHeight := visibleItems * itemHeight
		totalHeight := headerHeight + headerGap + listHeight + gtx.Dp(unit.Dp(20))

		macro := op.Record(gtx.Ops)
		dims := layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.Y = totalHeight
			gtx.Constraints.Max.Y = totalHeight
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Caption(c.theme, c.parent.t("console.suggestions_hint"))
					label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					gtx.Constraints.Min.Y = listHeight
					gtx.Constraints.Max.Y = listHeight
					return c.suggestList.Layout(gtx, len(suggestions), func(gtx layout.Context, index int) layout.Dimensions {
						item := suggestions[index]
						top := unit.Dp(0)
						if index > 0 {
							top = unit.Dp(6)
						}
						return layout.Inset{Top: top}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							return c.layoutConsoleSuggestionItem(gtx, item.Label, index == c.selectedSuggest)
						})
					})
				}),
			)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: color.NRGBA{R: 24, G: 30, B: 39, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

func (c *ConsoleWindow) layoutConsoleSuggestionItem(gtx layout.Context, command string, selected bool) layout.Dimensions {
	btn := c.suggestionButton(command)
	return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
		bg := color.NRGBA{R: 34, G: 46, B: 62, A: 255}
		if selected {
			bg = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
		}
		fill(gtx, bg)
		return layout.Inset{Top: unit.Dp(10), Bottom: unit.Dp(10), Left: unit.Dp(12), Right: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(c.theme, command)
			label.Color = color.NRGBA{R: 231, G: 237, B: 246, A: 255}
			return label.Layout(gtx)
		})
	})
}

func (c *ConsoleWindow) layoutConsoleHistory(gtx layout.Context) layout.Dimensions {
	entries := c.consoleHistory()
	return c.historyList.Layout(gtx, len(entries), func(gtx layout.Context, index int) layout.Dimensions {
		entry := entries[index]
		top := unit.Dp(0)
		if index > 0 {
			top = unit.Dp(10)
		}
		return layout.Inset{Top: top}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return c.layoutConsoleHistoryCard(gtx, entry)
		})
	})
}

func (c *ConsoleWindow) layoutConsoleHistoryCard(gtx layout.Context, entry *consoleEntry) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
		return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			for entry.CopyButton.Clicked(gtx) {
				gtx.Execute(clipboard.WriteCmd{
					Type: "text/plain",
					Data: io.NopCloser(strings.NewReader(entry.Output)),
				})
			}
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							command := material.Body1(c.theme, "> "+entry.Command)
							command.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
							command.Font.Weight = 600
							return command.Layout(gtx)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							btn := material.Button(c.theme, &entry.CopyButton, c.parent.t("console.copy"))
							btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
							return btn.Layout(gtx)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body2(c.theme, entry.CreatedAt.Format("2006-01-02 15:04:05"))
					label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutSelectableOutput(gtx, entry)
				}),
			)
		})
	})
}

func (c *ConsoleWindow) submitConsoleCommand() {
	if c.isConsoleBusy() {
		return
	}

	command := strings.TrimSpace(c.consoleEditor.Text())
	if command == "" {
		return
	}

	c.setConsoleBusy(true)
	c.consoleEditor.SetText("")
	c.hideSuggestions = false
	c.lastSuggestQuery = ""
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil

	go func(command string) {
		output, err := c.executeCommand(command)

		entry := newConsoleEntry(consoleEntry{
			Command:   command,
			CreatedAt: time.Now(),
		})
		if err != nil {
			entry.Output = err.Error()
			entry.Failed = true
		} else {
			entry.Output = output
		}
		entry.OutputText.SetText(entry.Output)

		c.mu.Lock()
		c.consoleEntries = append([]consoleEntry{entry}, c.consoleEntries...)
		c.consoleBusy = false
		c.mu.Unlock()

		c.invalidateWindow()
	}(command)
}

// invalidateWindow safely invalidates the console window from any goroutine.
// It checks the closed channel first (non-blocking) — once closed, the native
// window handle is about to be freed, so no Invalidate is attempted.  Then it
// reads c.window under the mutex and calls Invalidate while still holding the
// lock so that DestroyEvent cannot nil-out the pointer and free the handle
// between the read and the call.
func (c *ConsoleWindow) invalidateWindow() {
	select {
	case <-c.closed:
		return
	default:
	}
	c.mu.RLock()
	w := c.window
	if w != nil {
		w.Invalidate()
	}
	c.mu.RUnlock()
}

func (c *ConsoleWindow) isConsoleBusy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.consoleBusy
}

func (c *ConsoleWindow) setConsoleBusy(value bool) {
	c.mu.Lock()
	c.consoleBusy = value
	c.mu.Unlock()
}

func (c *ConsoleWindow) consoleHistory() []*consoleEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*consoleEntry, 0, len(c.consoleEntries))
	for i := range c.consoleEntries {
		out = append(out, &c.consoleEntries[i])
	}
	return out
}

func (c *ConsoleWindow) suggestionButton(command string) *widget.Clickable {
	if btn, ok := c.suggestButtons[command]; ok {
		return btn
	}
	btn := new(widget.Clickable)
	c.suggestButtons[command] = btn
	return btn
}

func (c *ConsoleWindow) consoleSuggestions() []consoleSuggestion {
	if len(c.suggestSnapshot) > 0 {
		return append([]consoleSuggestion(nil), c.suggestSnapshot...)
	}
	query := strings.TrimSpace(strings.ToLower(c.consoleEditor.Text()))
	if c.hideSuggestions {
		return nil
	}
	all := c.getCommands()
	if query == "" {
		return nil
	}

	matches := make([]consoleSuggestion, 0, len(all))
	for _, item := range all {
		if strings.HasPrefix(strings.ToLower(item.Label), query) || strings.Contains(strings.ToLower(item.Label), query) {
			matches = append(matches, item)
		}
	}
	if len(matches) > 6 {
		return matches[:6]
	}
	return matches
}

func (c *ConsoleWindow) syncSuggestionVisibility() {
	query := strings.TrimSpace(c.consoleEditor.Text())
	if query != c.lastSuggestQuery {
		if len(c.suggestSnapshot) > 0 && query == c.currentSuggestionText() {
			c.lastSuggestQuery = query
			return
		}
		c.hideSuggestions = false
		c.lastSuggestQuery = query
		c.selectedSuggest = -1
		if len(c.suggestSnapshot) > 0 {
			base := strings.TrimSpace(c.suggestBaseQuery)
			current := strings.TrimSpace(query)
			// If the user resumed typing after arrow-navigation, drop the frozen
			// snapshot and return to live filtering from the current input.
			if current != base {
				c.suggestBaseQuery = ""
				c.suggestSnapshot = nil
			}
		}
	}
}

func (c *ConsoleWindow) moveSuggestionSelection(delta int, suggestions []consoleSuggestion) {
	if len(c.suggestSnapshot) == 0 {
		c.suggestBaseQuery = strings.TrimSpace(c.consoleEditor.Text())
		c.suggestSnapshot = c.computeConsoleSuggestions(c.suggestBaseQuery)
		suggestions = append([]consoleSuggestion(nil), c.suggestSnapshot...)
	}
	if len(suggestions) == 0 {
		c.selectedSuggest = -1
		return
	}
	if c.selectedSuggest < 0 {
		if delta > 0 {
			c.selectedSuggest = 0
		} else {
			c.selectedSuggest = len(suggestions) - 1
		}
	} else {
		c.selectedSuggest += delta
		if c.selectedSuggest < 0 {
			c.selectedSuggest = len(suggestions) - 1
		}
		if c.selectedSuggest >= len(suggestions) {
			c.selectedSuggest = 0
		}
	}
	c.consoleEditor.SetText(suggestions[c.selectedSuggest].Insert)
	pos := len([]rune(suggestions[c.selectedSuggest].Insert))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestions = false
	c.lastSuggestQuery = strings.TrimSpace(c.consoleEditor.Text())
}

func (c *ConsoleWindow) applySelectedSuggestion(gtx layout.Context, suggestions []consoleSuggestion, chooseFirst bool) bool {
	if len(suggestions) == 0 {
		return false
	}
	if c.selectedSuggest < 0 && chooseFirst {
		c.selectedSuggest = 0
	}
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(suggestions) {
		return false
	}
	c.applySuggestion(gtx, suggestions[c.selectedSuggest].Insert)
	return true
}

func (c *ConsoleWindow) commitSuggestionForArguments(gtx layout.Context, suggestions []consoleSuggestion) bool {
	if len(suggestions) == 0 {
		return false
	}
	if c.selectedSuggest < 0 {
		c.selectedSuggest = 0
	}
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(suggestions) {
		return false
	}

	item := suggestions[c.selectedSuggest].Insert
	if !strings.HasSuffix(item, " ") {
		item += " "
	}
	c.consoleEditor.SetText(item)
	pos := len([]rune(item))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestions = true
	c.lastSuggestQuery = strings.TrimSpace(c.consoleEditor.Text())
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
	return true
}

func (c *ConsoleWindow) cancelSuggestions(gtx layout.Context) bool {
	if len(c.suggestSnapshot) == 0 && !c.hideSuggestions {
		return false
	}

	base := strings.TrimSpace(c.suggestBaseQuery)
	c.consoleEditor.SetText(base)
	pos := len([]rune(base))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestions = true
	c.lastSuggestQuery = base
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
	return true
}

func (c *ConsoleWindow) applySuggestion(gtx layout.Context, item string) {
	c.consoleEditor.SetText(item)
	pos := len([]rune(item))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestions = true
	c.lastSuggestQuery = strings.TrimSpace(c.consoleEditor.Text())
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
}

func (c *ConsoleWindow) currentSuggestionText() string {
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(c.suggestSnapshot) {
		return ""
	}
	return c.suggestSnapshot[c.selectedSuggest].Label
}

func (c *ConsoleWindow) computeConsoleSuggestions(query string) []consoleSuggestion {
	query = strings.TrimSpace(strings.ToLower(query))
	if c.hideSuggestions || query == "" {
		return nil
	}
	all := c.getCommands()
	matches := make([]consoleSuggestion, 0, len(all))
	for _, item := range all {
		if strings.HasPrefix(strings.ToLower(item.Label), query) || strings.Contains(strings.ToLower(item.Label), query) {
			matches = append(matches, item)
		}
	}
	if len(matches) > 6 {
		return matches[:6]
	}
	return matches
}

func newConsoleEntry(entry consoleEntry) consoleEntry {
	entry.OutputText.SetText(entry.Output)
	return entry
}

func (c *ConsoleWindow) layoutSelectableOutput(gtx layout.Context, entry *consoleEntry) layout.Dimensions {
	textColor := color.NRGBA{R: 208, G: 216, B: 228, A: 255}
	if entry.Failed {
		textColor = color.NRGBA{R: 255, G: 168, B: 168, A: 255}
	}

	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: textColor}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()

	selectionMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 72, G: 96, B: 140, A: 180}}.Add(gtx.Ops)
	selectionMaterial := selectionMacro.Stop()

	entry.OutputText.SetText(entry.Output)
	return entry.OutputText.Layout(gtx, c.theme.Shaper, font.Font{Typeface: c.theme.Face}, c.theme.TextSize, textMaterial, selectionMaterial)
}

func (c *ConsoleWindow) layoutInfoRows(gtx layout.Context, rows []string) layout.Dimensions {
	children := make([]layout.FlexChild, 0, len(rows)*2)
	for _, row := range rows {
		text := row
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body1(c.theme, text)
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
		)
	}
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

func (c *ConsoleWindow) layoutPeersContent(gtx layout.Context, connectedPeers, pendingPeers []service.PeerHealth, knownOnlyPeers []string) layout.Dimensions {
	type section struct {
		top    unit.Dp
		render func(layout.Context) layout.Dimensions
	}

	sections := make([]section, 0, 3)
	if len(connectedPeers) > 0 {
		sections = append(sections, section{
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutPeerSection(gtx, c.parent.t("console.peers.connected", len(connectedPeers)), connectedPeers)
			},
		})
	}
	if len(pendingPeers) > 0 {
		sections = append(sections, section{
			top: unit.Dp(14),
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutPeerSection(gtx, c.parent.t("console.peers.pending", len(pendingPeers)), pendingPeers)
			},
		})
	}
	if len(knownOnlyPeers) > 0 {
		sections = append(sections, section{
			top: unit.Dp(14),
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutKnownPeersSection(gtx, knownOnlyPeers)
			},
		})
	}

	return c.peerSectionList.Layout(gtx, len(sections), func(gtx layout.Context, index int) layout.Dimensions {
		item := sections[index]
		return layout.Inset{Top: item.top}.Layout(gtx, item.render)
	})
}

// executeCommand parses console input and dispatches it through CommandTable.
// Falls back to DesktopClient.ExecuteConsoleCommand if CommandTable is unavailable.
func (c *ConsoleWindow) executeCommand(input string) (string, error) {
	if c.parent.cmdTable == nil {
		return c.parent.client.ExecuteConsoleCommand(input)
	}

	trimmed := strings.TrimSpace(input)

	// Raw JSON frames are routed directly through ExecuteConsoleCommand,
	// which preserves all wire fields via protocol.ParseFrameLine. Going
	// through CommandTable would lose caller-supplied fields (e.g. a hello
	// frame's Client/ClientVersion) because handlers rebuild frames from args.
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		if c.parent.client != nil {
			return c.parent.client.ExecuteConsoleCommand(input)
		}
	}

	req, err := rpc.ParseConsoleInput(input)
	if err != nil {
		return "", err
	}

	// Console-specific "help" — human-readable text with categories,
	// defaults, and self-address. The CommandTable help handler returns
	// machine JSON for API consumers; the console needs a different format.
	if req.Name == "help" {
		addr := ""
		if c.parent.client != nil {
			addr = c.parent.client.Address()
		}
		return consoleHelpText(c.parent.cmdTable, addr), nil
	}

	resp := c.parent.cmdTable.Execute(req)

	// Unknown command fallback: forward raw input to the old
	// ExecuteConsoleCommand path, which can pass arbitrary JSON frames
	// through to HandleLocalFrame. This preserves passthrough for
	// named command types not registered in CommandTable.
	if resp.ErrorKind == rpc.ErrNotFound && c.parent.client != nil {
		return c.parent.client.ExecuteConsoleCommand(input)
	}

	if resp.Error != nil {
		return "", resp.Error
	}

	// Pretty-print JSON response for console display
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, resp.Data, "", "  "); err != nil {
		return string(resp.Data), nil
	}
	return prettyJSON.String(), nil
}

// loadCommands populates command suggestions from CommandTable — synchronous, no HTTP.
func (c *ConsoleWindow) loadCommands() {
	commands := c.parent.cmdTable.Commands()
	suggestions := commandInfoToSuggestions(commands)
	c.mu.Lock()
	c.cachedCommands = suggestions
	c.mu.Unlock()
}

// getCommands returns the current command suggestions.
func (c *ConsoleWindow) getCommands() []consoleSuggestion {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cachedCommands
}

// defaultPrefills maps command names to default argument templates
// that should be inserted on autocomplete selection. This preserves
// the old desktop console UX where fetch_chatlog prefilled "dm" as topic.
var defaultPrefills = map[string]string{
	"fetch_chatlog": "fetch_chatlog dm",
}

func commandInfoToSuggestions(commands []rpc.CommandInfo) []consoleSuggestion {
	suggestions := make([]consoleSuggestion, 0, len(commands))
	for _, cmd := range commands {
		label := cmd.Name
		if cmd.Usage != "" {
			label = cmd.Name + " " + cmd.Usage
		}
		insert := cmd.Name
		if prefill, ok := defaultPrefills[cmd.Name]; ok {
			insert = prefill
		}
		suggestions = append(suggestions, consoleSuggestion{
			Label:  label,
			Insert: insert,
		})
	}
	return suggestions
}

// consoleHelpText formats CommandTable metadata into a human-readable help
// screen for the desktop console. Grouped by category with usage hints,
// defaults, and self-address — matching the legacy consoleHelpText from
// DesktopClient but generated dynamically from CommandTable.
func consoleHelpText(table *rpc.CommandTable, selfAddress string) string {
	commands := table.Commands()

	// Group by category, preserving display order.
	categoryOrder := []string{"system", "network", "identity", "message", "chatlog", "notice"}
	categoryLabels := map[string]string{
		"system":   "Control",
		"network":  "Network",
		"identity": "Identity & Contacts",
		"message":  "Messages",
		"chatlog":  "Chat History",
		"notice":   "Notices",
	}
	grouped := make(map[string][]rpc.CommandInfo)
	for _, cmd := range commands {
		grouped[cmd.Category] = append(grouped[cmd.Category], cmd)
	}

	var lines []string
	for _, cat := range categoryOrder {
		cmds := grouped[cat]
		if len(cmds) == 0 {
			continue
		}
		label := categoryLabels[cat]
		if label == "" {
			label = cat
		}
		lines = append(lines, fmt.Sprintf("== %s ==", label))
		for _, cmd := range cmds {
			if cmd.Usage != "" {
				lines = append(lines, cmd.Name+" "+cmd.Usage)
			} else {
				lines = append(lines, cmd.Name)
			}
		}
		lines = append(lines, "")
	}

	lines = append(lines,
		"Defaults:",
		"  topic for fetch_messages/fetch_message_ids: global",
		"  topic for fetch_pending_messages/fetch_inbox: dm",
		"  recipient: "+selfAddress,
		"",
		"You can also paste a raw JSON protocol frame.",
	)

	return strings.Join(lines, "\n")
}

func stringItemsToChildren(values []string, render func(layout.Context, string) layout.Dimensions) []layout.FlexChild {
	children := make([]layout.FlexChild, 0, len(values))
	for _, value := range values {
		value := value
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return render(gtx, value)
		}))
	}
	return children
}

func countConnectedPeers(peers []service.PeerHealth) int {
	count := 0
	for _, item := range peers {
		if item.Connected {
			count++
		}
	}
	return count
}

func connectedPeerHealth(peers []service.PeerHealth) []service.PeerHealth {
	connected := make([]service.PeerHealth, 0, len(peers))
	for _, item := range peers {
		if item.Connected {
			connected = append(connected, item)
		}
	}
	return connected
}

func nonConnectedPeerHealth(peers []service.PeerHealth) []service.PeerHealth {
	pending := make([]service.PeerHealth, 0, len(peers))
	for _, item := range peers {
		if item.Connected {
			continue
		}
		pending = append(pending, item)
	}
	return pending
}

func knownOnlyPeers(peers []string, health []service.PeerHealth) []string {
	seen := make(map[string]struct{}, len(health))
	for _, item := range health {
		seen[strings.TrimSpace(item.Address)] = struct{}{}
	}

	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		out = append(out, peer)
	}
	return out
}

func peerHealthSummary(parent *Window, status service.NodeStatus) string {
	healthy := 0
	degraded := 0
	stalled := 0
	reconnecting := 0
	pending := 0

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
		pending += item.PendingCount
	}

	summary := parent.t("node.peer_health.summary", healthy, degraded, stalled, reconnecting, pending)
	if summary == "node.peer_health.summary" {
		return fmt.Sprintf("Healthy: %d, Degraded: %d, Stalled: %d, Reconnecting: %d, Pending: %d", healthy, degraded, stalled, reconnecting, pending)
	}
	return summary
}

func (c *ConsoleWindow) layoutPeerSection(gtx layout.Context, title string, peers []service.PeerHealth) layout.Dimensions {
	if len(peers) == 0 {
		return layout.Dimensions{}
	}

	children := []layout.FlexChild{
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body1(c.theme, title)
			label.Color = color.NRGBA{R: 232, G: 237, B: 247, A: 255}
			label.Font.Weight = 600
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
	}

	for i, peer := range peers {
		if i > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout))
		}
		peer := peer
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutPeerHealthCard(gtx, peer)
		}))
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

func (c *ConsoleWindow) layoutKnownPeersSection(gtx layout.Context, peers []string) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body1(c.theme, c.parent.t("console.peers.known_only", len(peers)))
			label.Color = color.NRGBA{R: 232, G: 237, B: 247, A: 255}
			label.Font.Weight = 600
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, stringItemsToChildren(peers, func(gtx layout.Context, peer string) layout.Dimensions {
				return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
					return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						label := material.Body2(c.theme, peer)
						label.Color = color.NRGBA{R: 208, G: 216, B: 228, A: 255}
						return label.Layout(gtx)
					})
				})
			})...)
		}),
	)
}

func (c *ConsoleWindow) layoutPeerHealthCard(gtx layout.Context, item service.PeerHealth) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
		return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Alignment: layout.Middle}.Layout(gtx,
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							label := material.Body1(c.theme, item.Address)
							label.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
							return label.Layout(gtx)
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return c.layoutStateBadge(gtx, item.State)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.ClientVersion) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Bottom: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						label := material.Caption(c.theme, item.ClientVersion)
						label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
						return label.Layout(gtx)
					})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Caption(c.theme, c.peerHealthMeta(item))
					label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.LastError) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						label := material.Caption(c.theme, item.LastError)
						label.Color = color.NRGBA{R: 255, G: 168, B: 168, A: 255}
						return label.Layout(gtx)
					})
				}),
			)
		})
	})
}

func (c *ConsoleWindow) layoutStateBadge(gtx layout.Context, state string) layout.Dimensions {
	bg, fg := peerStateColors(state)
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(10), Right: unit.Dp(10)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(c.theme, strings.ToUpper(c.parent.t("node.peer_state."+state)))
			label.Color = fg
			return label.Layout(gtx)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: bg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

func (c *ConsoleWindow) peerHealthMeta(item service.PeerHealth) string {
	lastRecv := "-"
	if item.LastUsefulReceiveAt != nil {
		lastRecv = item.LastUsefulReceiveAt.Format("15:04:05")
	}
	lastPong := "-"
	if item.LastPongAt != nil {
		lastPong = item.LastPongAt.Format("15:04:05")
	}
	connected := c.parent.t("node.link.down")
	if item.Connected {
		connected = c.parent.t("node.link.up")
	}
	text := c.parent.t("node.peer_health.meta", connected, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score)
	if text == "node.peer_health.meta" {
		return fmt.Sprintf("%s | pending %d | recv %s | pong %s | fails %d | score %d", connected, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score)
	}
	return text
}

func peerStateColors(state string) (color.NRGBA, color.NRGBA) {
	switch state {
	case "healthy":
		return color.NRGBA{R: 36, G: 92, B: 63, A: 255}, color.NRGBA{R: 231, G: 255, B: 239, A: 255}
	case "degraded":
		return color.NRGBA{R: 110, G: 82, B: 25, A: 255}, color.NRGBA{R: 255, G: 244, B: 210, A: 255}
	case "stalled":
		return color.NRGBA{R: 118, G: 50, B: 37, A: 255}, color.NRGBA{R: 255, G: 225, B: 220, A: 255}
	default:
		return color.NRGBA{R: 57, G: 67, B: 84, A: 255}, color.NRGBA{R: 231, G: 237, B: 246, A: 255}
	}
}
