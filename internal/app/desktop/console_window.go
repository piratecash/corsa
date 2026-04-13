package desktop

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
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

type consoleTab int32

func (c *ConsoleWindow) currentTab() consoleTab {
	return consoleTab(atomic.LoadInt32(&c.activeTab))
}

const (
	consoleTabConsole consoleTab = iota
	consoleTabPeers
	consoleTabTraffic
	consoleTabInfo
	consoleTabDonate
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

// peerCardSelectables holds widget.Selectable instances for each text field
// in a peer health card, enabling mouse text selection and copy.
type peerCardSelectables struct {
	Address widget.Selectable
	Version widget.Selectable
	Meta    widget.Selectable
	Error   widget.Selectable
}

type consoleDonateEntry struct {
	Label      string
	Address    string
	Text       widget.Selectable
	Scroll     widget.List
	CopyButton widget.Clickable
}

type ConsoleWindow struct {
	parent            *Window
	theme             *material.Theme
	ops               op.Ops
	onClose           func()
	window            *app.Window
	closed            chan struct{} // closed when DestroyEvent is received; used as a hard gate for cross-goroutine Invalidate
	peerList          widget.List
	peerSectionList   widget.List
	peerSelectables   map[string]*peerCardSelectables // keyed by peer address; lazily created
	historyList       widget.List
	suggestList       widget.List
	donateList        widget.List
	consoleEditor     widget.Editor
	runButton         widget.Clickable
	consoleTabButton  widget.Clickable
	peersTabButton    widget.Clickable
	trafficTabButton  widget.Clickable
	infoTabButton     widget.Clickable
	donateTabButton   widget.Clickable
	activeTab         int32     // consoleTab value; accessed atomically (UI writes, ticker reads)
	trafficSamplesIn  []float32 // per-second received bytes/s (newest last)
	trafficSamplesOut []float32 // per-second sent bytes/s (newest last)
	trafficTotalSent  int64     // cumulative sent (for totals display)
	trafficTotalRecv  int64     // cumulative received (for totals display)
	trafficLoaded     bool      // true after initial history load
	trafficTicker     *time.Ticker
	mu                sync.RWMutex
	consoleEntries    []consoleEntry
	consoleBusy       bool
	suggestButtons    map[string]*widget.Clickable
	lastSuggestQuery  string
	hideSuggestions   bool
	selectedSuggest   int
	suggestBaseQuery  string
	suggestSnapshot   []consoleSuggestion
	cachedCommands    []consoleSuggestion // loaded from CommandTable at init
	donateEntries     []consoleDonateEntry
	donateLink        widget.Selectable
	donateLinkButton  widget.Clickable
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
		donateList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		peerSelectables: make(map[string]*peerCardSelectables),
		suggestButtons:  make(map[string]*widget.Clickable),
		selectedSuggest: -1,
		donateEntries:   newConsoleDonateEntries(),
	}
	window.consoleEditor.SingleLine = true
	window.donateLink.SetText(consoleDonateURL)
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
		atomic.StoreInt32(&c.activeTab, int32(consoleTabConsole))
	}
	for c.peersTabButton.Clicked(gtx) {
		atomic.StoreInt32(&c.activeTab, int32(consoleTabPeers))
	}
	for c.trafficTabButton.Clicked(gtx) {
		atomic.StoreInt32(&c.activeTab, int32(consoleTabTraffic))
		c.startTrafficTicker()
	}
	for c.infoTabButton.Clicked(gtx) {
		atomic.StoreInt32(&c.activeTab, int32(consoleTabInfo))
	}
	for c.donateTabButton.Clicked(gtx) {
		atomic.StoreInt32(&c.activeTab, int32(consoleTabDonate))
	}
	for c.donateLinkButton.Clicked(gtx) {
		go func() {
			_ = openExternalURL(consoleDonateURL)
		}()
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
		case key.NameRightArrow:
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
			return c.layoutTabButton(gtx, &c.consoleTabButton, c.currentTab() == consoleTabConsole, c.parent.t("console.tab.console"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.peersTabButton, c.currentTab() == consoleTabPeers, c.parent.t("console.tab.peers"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.trafficTabButton, c.currentTab() == consoleTabTraffic, c.parent.t("console.tab.traffic"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.infoTabButton, c.currentTab() == consoleTabInfo, c.parent.t("console.tab.info"))
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, &c.donateTabButton, c.currentTab() == consoleTabDonate, c.parent.t("console.tab.donate"))
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
	switch c.currentTab() {
	case consoleTabPeers:
		return c.layoutPeersTab(gtx, status)
	case consoleTabTraffic:
		return c.layoutTrafficTab(gtx)
	case consoleTabInfo:
		return c.layoutInfoTab(gtx, status)
	case consoleTabDonate:
		return c.layoutDonateTab(gtx)
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
		c.parent.t("node.capabilities", fallback(joinOrNone(status.Capabilities), "none")),
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
	return c.card(gtx, c.parent.t("console.info_title"), c.infoRows(status))
}

func (c *ConsoleWindow) layoutDonateTab(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		return layout.UniformInset(unit.Dp(18)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme, unit.Sp(20), c.parent.t("console.donate_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					list := material.List(c.theme, &c.donateList)
					return list.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
						return c.layoutDonateSection(gtx)
					})
				}),
			)
		})
	})
}

func (c *ConsoleWindow) layoutPeersTab(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	activePeers := activePeerHealth(status.PeerHealth)
	rows := []string{
		c.parent.t("node.connected_peers", len(activePeers)),
	}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		return layout.UniformInset(unit.Dp(18)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme, unit.Sp(20), c.parent.t("console.peers_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			summary := material.Body1(c.theme, activePeerSummary(c.parent, activePeers))
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
					if len(activePeers) == 0 {
						label := material.Body1(c.theme, c.parent.t("console.peers_empty"))
						label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
						return label.Layout(gtx)
					}
					return c.layoutActivePeersContent(gtx, activePeers)
				}),
			)
		})
	})
}

func (c *ConsoleWindow) layoutConsoleTab(gtx layout.Context) layout.Dimensions {
	rows := []string{
		c.parent.t("console.help"),
	}

	return c.card(gtx, c.parent.t("console.title"), rows, func(gtx layout.Context) layout.Dimensions {
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

const consoleDonateURL = "https://pirate.cash/donate/"

func newConsoleDonateEntries() []consoleDonateEntry {
	entries := []consoleDonateEntry{
		{Label: "PirateCash", Address: "PB2vfGqfagNb12DyYTZBYWGnreyt7E4Pug"},
		{Label: "Cosanta", Address: "Cbbp3meofT1ESU5p4d9ucXpXw9pxKCMEyi"},
		{Label: "PIRATE / COSANTA (BEP-20)", Address: "0x52be29951B0D10d5eFa48D58363a25fE5Cc097e9"},
		{Label: "Bitcoin", Address: "bc1q2ph64sryt6skegze6726fp98u44kjsc5exktap"},
		{Label: "Dash", Address: "Xv7U37XKp5d4fjvbeuganwhqXN7Sm4JJkt"},
		{Label: "Zcash", Address: "zs1hwyqs4mfrynq0ysjmhv8wuau5zam0gwpx8ujfv8epgyufkmmsp6t7cfk9y0th7qyx7fsc5azm08"},
		{Label: "Monero", Address: "4AzdEoZxeGMFkdtAxaNLAZakqEVsWpVb2at4u6966WGDiXkS7ZPyi7haeThTGUAWXVKDTmQ9DYTWRHMjGVSBW82xRQqPxkg"},
	}
	for i := range entries {
		entries[i].Text.SetText(entries[i].Address)
		entries[i].Scroll = widget.List{List: layout.List{Axis: layout.Horizontal}}
	}
	return entries
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

func (c *ConsoleWindow) layoutSelectableText(gtx layout.Context, sel *widget.Selectable, text string, textColor color.NRGBA) layout.Dimensions {
	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: textColor}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()

	selectionMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 72, G: 96, B: 140, A: 180}}.Add(gtx.Ops)
	selectionMaterial := selectionMacro.Stop()

	sel.SetText(text)
	return sel.Layout(gtx, c.theme.Shaper, font.Font{Typeface: c.theme.Face}, c.theme.TextSize, textMaterial, selectionMaterial)
}

func (c *ConsoleWindow) layoutDonateSection(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := []layout.FlexChild{
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(c.theme, c.parent.t("console.donate_description"))
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(c.theme, c.parent.t("console.donate_source"))
				label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.donateLinkButton.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					pointer.CursorPointer.Add(gtx.Ops)
					label := material.Body2(c.theme, consoleDonateURL)
					label.Color = color.NRGBA{R: 124, G: 177, B: 255, A: 255}
					label.Font.Weight = 600
					return label.Layout(gtx)
				})
			}),
		}

		for i := range c.donateEntries {
			entry := &c.donateEntries[i]
			children = append(children,
				layout.Rigid(layout.Spacer{Height: unit.Dp(14)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					for entry.CopyButton.Clicked(gtx) {
						gtx.Execute(clipboard.WriteCmd{
							Type: "text/plain",
							Data: io.NopCloser(strings.NewReader(entry.Address)),
						})
					}
					return c.layoutDonateAddressCard(gtx, entry)
				}),
			)
		}

		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	})
}

func (c *ConsoleWindow) layoutDonateAddressCard(gtx layout.Context, entry *consoleDonateEntry) layout.Dimensions {
	border := color.NRGBA{R: 56, G: 68, B: 86, A: 255}
	bg := color.NRGBA{R: 28, G: 35, B: 46, A: 255}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		borderMacro := op.Record(gtx.Ops)
		dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			contentMacro := op.Record(gtx.Ops)
			contentDims := layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{Axis: layout.Horizontal}.Layout(gtx,
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										return c.layoutDonateBadge(gtx, entry.Label)
									}),
								)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return entry.Scroll.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
									return c.layoutSelectableText(gtx, &entry.Text, entry.Address, color.NRGBA{R: 245, G: 247, B: 250, A: 255})
								})
							}),
						)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						btn := material.Button(c.theme, &entry.CopyButton, c.parent.t("console.copy"))
						btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
						return btn.Layout(gtx)
					}),
				)
			})
			contentCall := contentMacro.Stop()

			defer clip.UniformRRect(image.Rectangle{Max: contentDims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
			paint.ColorOp{Color: bg}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			contentCall.Add(gtx.Ops)
			return contentDims
		})
		borderCall := borderMacro.Stop()

		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(11))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: border}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		borderCall.Add(gtx.Ops)
		return dims
	})
}

func (c *ConsoleWindow) layoutDonateBadge(gtx layout.Context, text string) layout.Dimensions {
	bg := color.NRGBA{R: 42, G: 51, B: 64, A: 255}
	fg := color.NRGBA{R: 198, G: 210, B: 226, A: 255}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.X = 0
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(10), Right: unit.Dp(10)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(c.theme, strings.ToUpper(text))
			label.Color = fg
			label.Font.Weight = 600
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

func openExternalURL(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

// card renders a styled card using the console window's own theme to avoid
// a data race with the parent window's text shaper (not thread-safe on Linux).
func (c *ConsoleWindow) card(gtx layout.Context, titleText string, rows []string, extras ...func(layout.Context) layout.Dimensions) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

		inset := layout.UniformInset(unit.Dp(18))
		return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			children := make([]layout.FlexChild, 0, len(rows)+len(extras)+2)
			if strings.TrimSpace(titleText) != "" {
				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Label(c.theme, unit.Sp(20), titleText)
						label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
						return label.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				)
			}

			for _, row := range rows {
				text := row
				children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body1(c.theme, text)
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

// peerSelectablesFor returns or creates the set of Selectable widgets for a peer address.
func (c *ConsoleWindow) peerSelectablesFor(address string) *peerCardSelectables {
	sel, ok := c.peerSelectables[address]
	if !ok {
		sel = &peerCardSelectables{}
		c.peerSelectables[address] = sel
	}
	return sel
}

// peerSlotGroup defines the display order and label for a CM slot group.
// Peers are sorted into groups by effectiveSlotState and rendered top-to-bottom
// in the order defined here: active first, then dialing, reconnecting, etc.
var peerSlotGroups = []struct {
	state string
	label string
}{
	{"active", "Active"},
	{"dialing", "Dialing"},
	{"reconnecting", "Reconnecting"},
	{"queued", "Queued"},
	{"retry_wait", "Retry Wait"},
	{"", "Inbound"},
}

// effectiveSlotState returns the grouping key for a peer.
// CM-managed peers use SlotState; inbound-only peers (Connected, no slot)
// return "" which maps to the "Inbound" group.
func effectiveSlotState(p service.PeerHealth) string {
	if p.SlotState != "" {
		return p.SlotState
	}
	return ""
}

// layoutActivePeersContent groups peers by CM slot state and renders each
// group as a titled section. Groups appear in a fixed priority order:
// Active → Dialing → Reconnecting → Queued → Retry Wait → Inbound.
func (c *ConsoleWindow) layoutActivePeersContent(gtx layout.Context, peers []service.PeerHealth) layout.Dimensions {
	grouped := make(map[string][]service.PeerHealth, len(peerSlotGroups))
	for _, p := range peers {
		key := effectiveSlotState(p)
		grouped[key] = append(grouped[key], p)
	}

	type section struct {
		top    unit.Dp
		render func(layout.Context) layout.Dimensions
	}
	sections := make([]section, 0, len(peerSlotGroups))
	for _, g := range peerSlotGroups {
		items := grouped[g.state]
		if len(items) == 0 {
			continue
		}
		top := unit.Dp(14)
		if len(sections) == 0 {
			top = 0
		}
		label := fmt.Sprintf("%s (%d)", g.label, len(items))
		groupItems := items // capture for closure
		sections = append(sections, section{
			top: top,
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutPeerSection(gtx, label, groupItems)
			},
		})
	}

	list := material.List(c.theme, &c.peerSectionList)
	return list.Layout(gtx, len(sections), func(gtx layout.Context, index int) layout.Dimensions {
		item := sections[index]
		return layout.Inset{Top: item.top}.Layout(gtx, item.render)
	})
}

// activePeerSummary builds a one-line status summary for the active peer list.
// Counts both health states (healthy/degraded/stalled) and CM slot states
// (dialing/queued/retry_wait) so the user sees the full connection picture.
func activePeerSummary(parent *Window, peers []service.PeerHealth) string {
	healthy := 0
	degraded := 0
	stalled := 0
	dialing := 0
	queued := 0
	retryWait := 0
	var totalIn, totalOut int64
	for _, item := range peers {
		switch item.State {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "stalled":
			stalled++
		}
		switch item.SlotState {
		case "dialing":
			dialing++
		case "queued":
			queued++
		case "retry_wait":
			retryWait++
		}
		totalIn += item.BytesReceived
		totalOut += item.BytesSent
	}
	summary := parent.t("node.active_peer.summary", healthy, degraded, stalled)
	if summary == "node.active_peer.summary" {
		base := fmt.Sprintf("Healthy: %d, Degraded: %d, Stalled: %d", healthy, degraded, stalled)
		if dialing > 0 || queued > 0 || retryWait > 0 {
			base += fmt.Sprintf(" | Dialing: %d, Queued: %d, RetryWait: %d", dialing, queued, retryWait)
		}
		base += fmt.Sprintf(" | In: %s, Out: %s", formatBytes(totalIn), formatBytes(totalOut))
		return base
	}
	return summary
}

// executeCommand parses console input and dispatches it through CommandTable.
func (c *ConsoleWindow) executeCommand(input string) (string, error) {
	if c.parent.cmdTable == nil {
		return "", fmt.Errorf("command table not initialized")
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
			addr = string(c.parent.client.Address())
		}
		return consoleHelpText(c.parent.cmdTable, addr), nil
	}

	resp := c.parent.cmdTable.Execute(req)

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
// the old desktop console UX where fetchChatlog prefilled "dm" as topic.
var defaultPrefills = map[string]string{
	"fetchChatlog": "fetchChatlog dm",
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
	categoryOrder := []string{"system", "network", "routing", "metrics", "identity", "message", "file", "chatlog", "notice", "view"}
	categoryLabels := map[string]string{
		"system":   "Control",
		"network":  "Network",
		"routing":  "Routing",
		"metrics":  "Metrics",
		"identity": "Identity & Contacts",
		"message":  "Messages",
		"file":     "File Transfer",
		"chatlog":  "Chat History",
		"notice":   "Notices",
		"view":     "Desktop Views",
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
		"You can also paste a raw JSON frame for any registered command.",
	)

	return strings.Join(lines, "\n")
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

// activePeerHealth returns peers that the ConnectionManager is actively
// managing (any SlotState: queued, dialing, active, reconnecting, retry_wait)
// plus inbound peers that are Connected but have no CM slot.
// This matches the scope of the getActivePeers RPC command — all CM slots
// plus live inbound connections — and excludes "known-only" peers that
// have a health entry but no slot and no active TCP connection.
func activePeerHealth(peers []service.PeerHealth) []service.PeerHealth {
	active := make([]service.PeerHealth, 0, len(peers))
	for _, item := range peers {
		if item.SlotState != "" || item.Connected {
			active = append(active, item)
		}
	}
	return active
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

func (c *ConsoleWindow) layoutPeerHealthCard(gtx layout.Context, item service.PeerHealth) layout.Dimensions {
	sel := c.peerSelectablesFor(item.Address)
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
		return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							arrow := peerDirectionArrow(item.Direction)
							if arrow == "" {
								return layout.Dimensions{}
							}
							return layout.Inset{Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								label := material.Body1(c.theme, arrow)
								label.Color = peerDirectionColor(item.Direction)
								return label.Layout(gtx)
							})
						}),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return c.layoutSelectableText(gtx, &sel.Address, item.Address, color.NRGBA{R: 245, G: 247, B: 250, A: 255})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							// Prefer CM slot state for badge when available;
							// fall back to health-derived state for inbound-only peers.
							badgeState := item.State
							if item.SlotState != "" {
								badgeState = item.SlotState
							}
							return c.layoutStateBadge(gtx, badgeState)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.ClientVersion) == "" {
						return layout.Dimensions{}
					}
					versionText := item.ClientVersion
					if item.ProtocolVersion > 0 {
						versionText = fmt.Sprintf("%s (proto v%d)", item.ClientVersion, item.ProtocolVersion)
					}
					return layout.Inset{Bottom: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return c.layoutSelectableText(gtx, &sel.Version, versionText, color.NRGBA{R: 167, G: 179, B: 196, A: 255})
					})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutSelectableText(gtx, &sel.Meta, c.peerHealthMeta(item), color.NRGBA{R: 196, G: 205, B: 218, A: 255})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.LastError) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return c.layoutSelectableText(gtx, &sel.Error, item.LastError, color.NRGBA{R: 255, G: 168, B: 168, A: 255})
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
	dirLabel := ""
	if item.Direction != "" {
		dirLabel = " " + item.Direction
	}
	uptime := "-"
	if item.Connected && item.LastConnectedAt != nil {
		uptime = formatUptime(time.Since(*item.LastConnectedAt))
	}

	// Build slot suffix for CM-managed outbound peers.
	slotSuffix := ""
	if item.SlotState != "" {
		slotSuffix = fmt.Sprintf(" | slot %s", item.SlotState)
		if item.SlotRetryCount > 0 {
			slotSuffix += fmt.Sprintf(" retry %d", item.SlotRetryCount)
		}
		if item.SlotConnectedAddr != "" && item.SlotConnectedAddr != item.Address {
			slotSuffix += fmt.Sprintf(" via %s", item.SlotConnectedAddr)
		}
	}

	text := c.parent.t("node.peer_health.meta", connected+dirLabel, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score, formatBytes(item.BytesReceived), formatBytes(item.BytesSent))
	if text == "node.peer_health.meta" {
		return fmt.Sprintf("%s%s | uptime %s | pending %d | recv %s | pong %s | fails %d | score %d | in %s | out %s%s", connected, dirLabel, uptime, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score, formatBytes(item.BytesReceived), formatBytes(item.BytesSent), slotSuffix)
	}
	return text + " | uptime " + uptime + slotSuffix
}

// formatBytes formats a byte count into a human-readable string (B, KB, MB, GB, TB).
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTP"[exp])
}

// formatUptime formats a duration as a human-readable uptime string.
// Uses the largest applicable unit: "42s", "15m32s", "3h10m", "2d5h".
func formatUptime(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	totalSeconds := int(d.Seconds())
	if totalSeconds < 60 {
		return fmt.Sprintf("%ds", totalSeconds)
	}
	totalMinutes := totalSeconds / 60
	seconds := totalSeconds % 60
	if totalMinutes < 60 {
		return fmt.Sprintf("%dm%ds", totalMinutes, seconds)
	}
	hours := totalMinutes / 60
	minutes := totalMinutes % 60
	if hours < 24 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	days := hours / 24
	hours = hours % 24
	return fmt.Sprintf("%dd%dh", days, hours)
}

// --- Traffic tab ---

const (
	trafficGraphVisiblePoints = 600  // visible data points (10 min at 1 sample/sec)
	trafficMaxSamples         = 3600 // hard cap matching metrics.Collector ring buffer
)

// Shared palette for traffic visualization (bars, line, legend, badges).
var (
	trafficInColor    = color.NRGBA{R: 59, G: 186, B: 130, A: 200} // green bars
	trafficOutColor   = color.NRGBA{R: 86, G: 156, B: 231, A: 200} // blue bars
	trafficTotalColor = color.NRGBA{R: 230, G: 180, B: 60, A: 255} // yellow line
	trafficInSolid    = color.NRGBA{R: 59, G: 186, B: 130, A: 255} // green solid (legend/badges)
	trafficOutSolid   = color.NRGBA{R: 86, G: 156, B: 231, A: 255} // blue solid (legend/badges)
)

// loadTrafficHistory fetches the full history from the metrics collector
// and populates the local sample slices. Called when the tab opens and on
// every ticker restart so reopening shows accurate data.
// On any failure (RPC error, unmarshal error, nil collector) the cached
// graph state is cleared to prevent rendering stale data from a previous
// session or a pre-restart collector.
func (c *ConsoleWindow) loadTrafficHistory() {
	if c.parent.cmdTable == nil {
		c.resetTrafficState()
		return
	}
	resp := c.parent.cmdTable.Execute(rpc.CommandRequest{Name: "fetchTrafficHistory"})
	if resp.Error != nil {
		c.resetTrafficState()
		return
	}
	var frame struct {
		TrafficHistory *struct {
			Samples []struct {
				BytesSentPS   int64 `json:"bytes_sent_ps"`
				BytesRecvPS   int64 `json:"bytes_recv_ps"`
				TotalSent     int64 `json:"total_sent"`
				TotalReceived int64 `json:"total_received"`
			} `json:"samples"`
		} `json:"traffic_history"`
	}
	if err := json.Unmarshal(resp.Data, &frame); err != nil || frame.TrafficHistory == nil {
		c.resetTrafficState()
		return
	}

	samples := frame.TrafficHistory.Samples

	c.mu.Lock()
	c.trafficSamplesIn = make([]float32, 0, len(samples))
	c.trafficSamplesOut = make([]float32, 0, len(samples))
	for _, s := range samples {
		c.trafficSamplesIn = append(c.trafficSamplesIn, float32(s.BytesRecvPS))
		c.trafficSamplesOut = append(c.trafficSamplesOut, float32(s.BytesSentPS))
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		c.trafficTotalSent = last.TotalSent
		c.trafficTotalRecv = last.TotalReceived
		c.trafficLoaded = true
	} else {
		// Empty history (e.g. collector just restarted). Do NOT set trafficLoaded
		// so the next sampleTraffic call records the current counters as baseline
		// instead of computing a bogus delta against stale values.
		c.trafficTotalSent = 0
		c.trafficTotalRecv = 0
		c.trafficLoaded = false
	}
	c.mu.Unlock()
}

// resetTrafficState clears all cached traffic graph data so the UI does not
// render stale samples from a previous session or failed reload.
func (c *ConsoleWindow) resetTrafficState() {
	c.mu.Lock()
	c.trafficSamplesIn = nil
	c.trafficSamplesOut = nil
	c.trafficTotalSent = 0
	c.trafficTotalRecv = 0
	c.trafficLoaded = false
	c.mu.Unlock()
}

// startTrafficTicker launches a 1-second ticker that samples traffic stats
// and invalidates the window. Reloads the full history from the collector
// on every call so that reopening the tab shows accurate per-second data
// instead of compressing the missed interval into a single spike.
// Called from the UI goroutine (handleActions); the ticker goroutine
// accesses trafficTicker under c.mu to avoid races.
func (c *ConsoleWindow) startTrafficTicker() {
	c.mu.Lock()
	if c.trafficTicker != nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// Always reload full history from the collector — the collector kept
	// sampling while the tab was inactive, so we get accurate per-second data.
	c.loadTrafficHistory()

	c.mu.Lock()
	ticker := time.NewTicker(1 * time.Second)
	c.trafficTicker = ticker
	c.mu.Unlock()

	go func() {
		for {
			select {
			case <-c.closed:
				ticker.Stop()
				return
			case <-ticker.C:
				if c.currentTab() != consoleTabTraffic {
					ticker.Stop()
					c.mu.Lock()
					c.trafficTicker = nil
					c.mu.Unlock()
					return
				}
				c.sampleTraffic()
				c.invalidateWindow()
			}
		}
	}()
}

func (c *ConsoleWindow) sampleTraffic() {
	if c.parent.cmdTable == nil {
		return
	}
	resp := c.parent.cmdTable.Execute(rpc.CommandRequest{Name: "fetchNetworkStats"})
	if resp.Error != nil {
		return
	}
	var frame struct {
		NetworkStats *struct {
			TotalBytesSent     int64 `json:"total_bytes_sent"`
			TotalBytesReceived int64 `json:"total_bytes_received"`
		} `json:"network_stats"`
	}
	if err := json.Unmarshal(resp.Data, &frame); err != nil || frame.NetworkStats == nil {
		return
	}

	sent := frame.NetworkStats.TotalBytesSent
	recv := frame.NetworkStats.TotalBytesReceived

	c.mu.Lock()
	if c.trafficLoaded {
		deltaSent := sent - c.trafficTotalSent
		deltaRecv := recv - c.trafficTotalRecv
		if deltaSent < 0 {
			deltaSent = 0
		}
		if deltaRecv < 0 {
			deltaRecv = 0
		}
		c.trafficSamplesOut = append(c.trafficSamplesOut, float32(deltaSent))
		c.trafficSamplesIn = append(c.trafficSamplesIn, float32(deltaRecv))

		// Trim to trafficMaxSamples to prevent unbounded growth.
		if len(c.trafficSamplesIn) > trafficMaxSamples {
			c.trafficSamplesIn = c.trafficSamplesIn[len(c.trafficSamplesIn)-trafficMaxSamples:]
		}
		if len(c.trafficSamplesOut) > trafficMaxSamples {
			c.trafficSamplesOut = c.trafficSamplesOut[len(c.trafficSamplesOut)-trafficMaxSamples:]
		}
	}
	c.trafficTotalSent = sent
	c.trafficTotalRecv = recv
	c.trafficLoaded = true
	c.mu.Unlock()
}

func (c *ConsoleWindow) layoutTrafficTab(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		return layout.UniformInset(unit.Dp(18)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				// Title
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.H6(c.theme, c.parent.t("console.traffic_title"))
					label.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				// Graph area
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return c.layoutTrafficGraph(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				// Legend (below graph)
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutTrafficLegend(gtx)
				}),
			)
		})
	})
}

func (c *ConsoleWindow) layoutTrafficLegend(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficInSolid)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme, c.parent.t("console.traffic_in"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(20)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficOutSolid)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme, c.parent.t("console.traffic_out"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(20)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficTotalColor)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme, c.parent.t("console.traffic_total"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
	)
}

func (c *ConsoleWindow) layoutColorDot(gtx layout.Context, clr color.NRGBA) layout.Dimensions {
	sz := gtx.Dp(unit.Dp(10))
	defer clip.UniformRRect(image.Rectangle{Max: image.Pt(sz, sz)}, sz/2).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	return layout.Dimensions{Size: image.Pt(sz, sz)}
}

// trafficVisibleSlice returns a copy of the tail portion of data visible on the graph.
// A copy is required because the caller reads under RLock while the ticker goroutine
// may append to the original slice — sharing the backing array would be a data race.
func trafficVisibleSlice(data []float32, maxVisible int) []float32 {
	src := data
	if len(src) > maxVisible {
		src = src[len(src)-maxVisible:]
	}
	out := make([]float32, len(src))
	copy(out, src)
	return out
}

func (c *ConsoleWindow) layoutTrafficGraph(gtx layout.Context) layout.Dimensions {
	// snapshot traffic data under read-lock to avoid races with ticker goroutine
	c.mu.RLock()
	visIn := trafficVisibleSlice(c.trafficSamplesIn, trafficGraphVisiblePoints)
	visOut := trafficVisibleSlice(c.trafficSamplesOut, trafficGraphVisiblePoints)
	totalSent := c.trafficTotalSent
	totalRecv := c.trafficTotalRecv
	c.mu.RUnlock()

	// reserve left margin for Y-axis labels
	yAxisWidth := gtx.Dp(unit.Dp(60))
	totalWidth := gtx.Constraints.Max.X
	height := gtx.Constraints.Max.Y
	if height <= 0 {
		height = gtx.Dp(unit.Dp(200))
	}
	graphWidth := totalWidth - yAxisWidth
	if graphWidth < 10 {
		graphWidth = 10
	}

	// dark background for entire area
	graphBg := color.NRGBA{R: 15, G: 19, B: 27, A: 255}
	defer clip.Rect{Max: image.Pt(totalWidth, height)}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: graphBg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	count := len(visIn)
	if len(visOut) > count {
		count = len(visOut)
	}

	// find max across in, out, and total for unified scale
	var maxVal float32
	for i := 0; i < count; i++ {
		var inVal, outVal float32
		if i < len(visIn) {
			inVal = visIn[i]
		}
		if i < len(visOut) {
			outVal = visOut[i]
		}
		total := inVal + outVal
		if total > maxVal {
			maxVal = total
		}
	}
	if maxVal < 1 {
		maxVal = 1
	}
	maxVal *= 1.1 // 10% headroom

	// draw Y-axis labels and horizontal grid lines (4 lines)
	gridColor := color.NRGBA{R: 40, G: 48, B: 60, A: 255}
	labelColor := color.NRGBA{R: 100, G: 110, B: 125, A: 255}
	for i := 1; i <= 4; i++ {
		y := height * i / 5
		drawHLine(gtx, yAxisWidth, totalWidth, y, gridColor)
		gridVal := maxVal * float32(5-i) / 5.0
		lbl := material.Caption(c.theme, formatBytes(int64(gridVal))+"/s")
		lbl.Color = labelColor
		stack := op.Offset(image.Pt(0, y-gtx.Dp(unit.Dp(7)))).Push(gtx.Ops)
		lbl.Layout(gtx)
		stack.Pop()
	}

	if count == 0 {
		return layout.Dimensions{Size: image.Pt(totalWidth, height)}
	}

	fHeight := float32(height)
	fGraphW := float32(graphWidth)

	// all samples are spread across the full graph width
	visibleCount := count
	if visibleCount > trafficGraphVisiblePoints {
		visibleCount = trafficGraphVisiblePoints
	}

	// pixels per sample (fractional for smooth distribution)
	pxPerSample := fGraphW / float32(visibleCount)

	// bar width: half of step, minimum 1px; gap = 1px between in and out
	barW := int(pxPerSample/2) - 1
	if barW < 1 {
		barW = 1
	}

	// draw IN and OUT bars side by side across full width
	for i := 0; i < visibleCount; i++ {
		dataIdx := count - visibleCount + i
		var inVal, outVal float32
		if dataIdx < len(visIn) {
			inVal = visIn[dataIdx]
		}
		if dataIdx < len(visOut) {
			outVal = visOut[dataIdx]
		}

		// center of this sample on X axis
		centerX := yAxisWidth + int(float32(i)*pxPerSample+pxPerSample/2)

		// IN bar (green) — left of center
		if inVal > 0 {
			inH := int((inVal / maxVal) * fHeight)
			if inH < 1 {
				inH = 1
			}
			x0 := centerX - barW - 1
			drawRect(gtx, image.Rect(x0, height-inH, x0+barW, height), trafficInColor)
		}

		// OUT bar (blue) — right of center
		if outVal > 0 {
			outH := int((outVal / maxVal) * fHeight)
			if outH < 1 {
				outH = 1
			}
			x0 := centerX + 1
			drawRect(gtx, image.Rect(x0, height-outH, x0+barW, height), trafficOutColor)
		}
	}

	// draw Total line (in+out) across full width on top of bars
	if visibleCount >= 2 {
		drawTrafficLine(gtx, visIn, visOut, count, visibleCount, yAxisWidth, pxPerSample, fHeight, maxVal, trafficTotalColor)
	}

	// draw badges (Total In / Total Out) in the top-right corner of the graph
	c.drawTrafficBadges(gtx, totalWidth, height, totalSent, totalRecv)

	return layout.Dimensions{Size: image.Pt(totalWidth, height)}
}

// drawTrafficBadges renders Total In / Total Out stacked vertically
// inside a single rounded rectangle in the top-right corner of the graph.
func (c *ConsoleWindow) drawTrafficBadges(gtx layout.Context, totalWidth, height int, totalSent, totalRecv int64) {
	inText := c.parent.t("console.traffic_total_in", formatBytes(totalRecv))
	outText := c.parent.t("console.traffic_total_out", formatBytes(totalSent))

	padH := gtx.Dp(unit.Dp(10))
	padV := gtx.Dp(unit.Dp(8))
	lineGap := gtx.Dp(unit.Dp(4))
	margin := gtx.Dp(unit.Dp(10))
	radius := gtx.Dp(unit.Dp(6))

	badgeBg := color.NRGBA{R: 30, G: 36, B: 48, A: 220}

	// measure text with unconstrained width
	measureGtx := gtx
	measureGtx.Constraints.Min = image.Point{}
	measureGtx.Constraints.Max = image.Pt(totalWidth, height)

	inMacro := op.Record(gtx.Ops)
	inLbl := material.Caption(c.theme, inText)
	inLbl.Color = trafficInSolid
	inDims := inLbl.Layout(measureGtx)
	inCall := inMacro.Stop()

	outMacro := op.Record(gtx.Ops)
	outLbl := material.Caption(c.theme, outText)
	outLbl.Color = trafficOutSolid
	outDims := outLbl.Layout(measureGtx)
	outCall := outMacro.Stop()

	// box size: widest text + padding, both lines stacked
	textW := inDims.Size.X
	if outDims.Size.X > textW {
		textW = outDims.Size.X
	}
	boxW := textW + padH*2
	boxH := inDims.Size.Y + lineGap + outDims.Size.Y + padV*2

	// position: top-right corner
	boxX := totalWidth - boxW - margin
	boxY := margin

	// draw single rounded background
	drawRoundedRect(gtx, image.Rect(boxX, boxY, boxX+boxW, boxY+boxH), radius, badgeBg)

	// draw In text
	s1 := op.Offset(image.Pt(boxX+padH, boxY+padV)).Push(gtx.Ops)
	inCall.Add(gtx.Ops)
	s1.Pop()

	// draw Out text below In
	s2 := op.Offset(image.Pt(boxX+padH, boxY+padV+inDims.Size.Y+lineGap)).Push(gtx.Ops)
	outCall.Add(gtx.Ops)
	s2.Pop()
}

func drawRoundedRect(gtx layout.Context, r image.Rectangle, radius int, clr color.NRGBA) {
	defer clip.UniformRRect(r, radius).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

// drawTrafficLine draws the total (in+out) as a continuous line
// connecting all non-zero sample points across zero gaps between clusters.
func drawTrafficLine(gtx layout.Context, visIn, visOut []float32, totalCount, visibleCount, yAxisWidth int, pxPerSample, height, maxVal float32, clr color.NRGBA) {
	const lineW = 2

	startIdx := totalCount - visibleCount
	prevX, prevY := -1, -1
	for i := 0; i < visibleCount; i++ {
		dataIdx := startIdx + i
		var inVal, outVal float32
		if dataIdx < len(visIn) {
			inVal = visIn[dataIdx]
		}
		if dataIdx < len(visOut) {
			outVal = visOut[dataIdx]
		}
		total := inVal + outVal
		if total <= 0 {
			continue // skip zeros but keep prev point for continuity
		}

		curX := yAxisWidth + int(float32(i)*pxPerSample+pxPerSample/2)
		curY := int(height - (total/maxVal)*height)

		if prevX >= 0 {
			drawLineBresenham(gtx, prevX, prevY, curX, curY, lineW, clr)
		} else {
			drawRect(gtx, image.Rect(curX, curY, curX+lineW, curY+lineW), clr)
		}

		prevX = curX
		prevY = curY
	}
}

// drawLineBresenham draws a line from (x0,y0) to (x1,y1) using
// Bresenham's algorithm, rendering each point as a [w x w] square.
func drawLineBresenham(gtx layout.Context, x0, y0, x1, y1, w int, clr color.NRGBA) {
	dx := x1 - x0
	dy := y1 - y0
	if dx < 0 {
		dx = -dx
	}
	if dy < 0 {
		dy = -dy
	}

	sx := 1
	if x0 > x1 {
		sx = -1
	}
	sy := 1
	if y0 > y1 {
		sy = -1
	}

	err := dx - dy
	for {
		drawRect(gtx, image.Rect(x0, y0, x0+w, y0+w), clr)
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x0 += sx
		}
		if e2 < dx {
			err += dx
			y0 += sy
		}
	}
}

func drawRect(gtx layout.Context, r image.Rectangle, clr color.NRGBA) {
	defer clip.Rect(r).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

func drawHLine(gtx layout.Context, x0, x1, y int, clr color.NRGBA) {
	defer clip.Rect{Min: image.Pt(x0, y), Max: image.Pt(x1, y+1)}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

// peerDirectionArrow returns a Unicode arrow for the connection direction:
// ↑ for outbound (we initiated), ↓ for inbound (they connected to us).
func peerDirectionArrow(direction string) string {
	switch direction {
	case "outbound":
		return "↑"
	case "inbound":
		return "↓"
	default:
		return ""
	}
}

// peerDirectionColor returns the color for the direction arrow.
func peerDirectionColor(direction string) color.NRGBA {
	switch direction {
	case "outbound":
		return color.NRGBA{R: 100, G: 200, B: 255, A: 255} // light blue
	case "inbound":
		return color.NRGBA{R: 180, G: 130, B: 255, A: 255} // light purple
	default:
		return color.NRGBA{R: 196, G: 205, B: 218, A: 255} // gray
	}
}

func peerStateColors(state string) (color.NRGBA, color.NRGBA) {
	switch state {
	case "healthy", "active":
		return color.NRGBA{R: 36, G: 92, B: 63, A: 255}, color.NRGBA{R: 231, G: 255, B: 239, A: 255}
	case "degraded", "reconnecting":
		return color.NRGBA{R: 110, G: 82, B: 25, A: 255}, color.NRGBA{R: 255, G: 244, B: 210, A: 255}
	case "stalled", "retry_wait":
		return color.NRGBA{R: 118, G: 50, B: 37, A: 255}, color.NRGBA{R: 255, G: 225, B: 220, A: 255}
	case "dialing", "queued":
		return color.NRGBA{R: 40, G: 70, B: 110, A: 255}, color.NRGBA{R: 210, G: 230, B: 255, A: 255}
	default:
		return color.NRGBA{R: 57, G: 67, B: 84, A: 255}, color.NRGBA{R: 231, G: 237, B: 246, A: 255}
	}
}
