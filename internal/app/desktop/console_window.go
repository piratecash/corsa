package desktop

import (
	"fmt"
	"image"
	"image/color"
	"strings"
	"time"

	"corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

type ConsoleWindow struct {
	parent   *Window
	theme    *material.Theme
	ops      op.Ops
	onClose  func()
	peerList widget.List
}

func NewConsoleWindow(parent *Window, onClose func()) *ConsoleWindow {
	return &ConsoleWindow{
		parent:  parent,
		theme:   parent.theme,
		onClose: onClose,
		peerList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
	}
}

func (c *ConsoleWindow) Open() {
	go func() {
		window := new(app.Window)
		window.Option(
			app.Title(c.parent.t("console.title")),
			app.Size(unit.Dp(760), unit.Dp(680)),
		)

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		go func() {
			for range ticker.C {
				window.Invalidate()
			}
		}()

		for {
			switch e := window.Event().(type) {
			case app.DestroyEvent:
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
	fill(gtx, colorBackground())
	inset := layout.UniformInset(unit.Dp(24))
	return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		status := c.parent.currentStatus()
		return c.parent.card(gtx, c.parent.t("console.title"), c.consoleRows(status), func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if len(status.PeerHealth) == 0 {
						return layout.Dimensions{}
					}
					label := material.Body1(c.theme, peerHealthSummary(c.parent, status))
					label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if len(status.PeerHealth) == 0 {
						return layout.Dimensions{}
					}
					return layout.Spacer{Height: unit.Dp(12)}.Layout(gtx)
				}),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					if len(status.PeerHealth) == 0 {
						return layout.Dimensions{}
					}
					return c.layoutPeerHealthList(gtx, status.PeerHealth)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Spacer{Height: unit.Dp(12)}.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(240)))
						btn := material.Button(c.theme, &c.parent.rebuildTrustButton, c.parent.t("node.trust_rebuild"))
						return btn.Layout(gtx)
					})
				}),
			)
		})
	})
}

func colorBackground() color.NRGBA {
	return color.NRGBA{R: 12, G: 15, B: 20, A: 255}
}

func (c *ConsoleWindow) consoleRows(status service.NodeStatus) []string {
	connectedPeers := countConnectedPeers(status.PeerHealth)
	rows := []string{
		c.parent.t("node.client_version", c.parent.client.Version()),
		c.parent.t("node.peer_version", fallback(status.ClientVersion, strings.ReplaceAll(c.parent.client.Version(), " ", "-"))),
		c.parent.t("node.listen", c.parent.runtime.ListenAddress()),
		c.parent.t("node.type", fallback(status.NodeType, "full")),
		c.parent.t("node.services", fallback(joinOrNone(status.Services), "identity,contacts,messages,gazeta,relay")),
		c.parent.t("node.connected", status.Connected),
		c.parent.t("node.known_peers", len(status.Peers)),
		c.parent.t("node.connected_peers", connectedPeers),
		c.parent.localNodeErrorRow(),
	}

	if len(status.PeerHealth) == 0 {
		rows = append(rows, c.parent.t("node.peer_health.none"))
	} else {
		rows = append(rows, c.parent.t("node.peer_health.title"))
	}

	if !status.CheckedAt.IsZero() {
		rows = append(rows, c.parent.t("node.checked", status.CheckedAt.Format(time.RFC3339)))
	}

	return rows
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

func (c *ConsoleWindow) layoutPeerHealthList(gtx layout.Context, peers []service.PeerHealth) layout.Dimensions {
	return c.peerList.Layout(gtx, len(peers), func(gtx layout.Context, index int) layout.Dimensions {
		top := unit.Dp(0)
		if index > 0 {
			top = unit.Dp(10)
		}
		return layout.Inset{Top: top}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return c.layoutPeerHealthCard(gtx, peers[index])
		})
	})
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
					label := material.Caption(c.theme, peerHealthMeta(item))
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
			label := material.Caption(c.theme, strings.ToUpper(state))
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

func peerHealthMeta(item service.PeerHealth) string {
	lastRecv := "-"
	if item.LastUsefulReceiveAt != nil {
		lastRecv = item.LastUsefulReceiveAt.Format("15:04:05")
	}
	lastPong := "-"
	if item.LastPongAt != nil {
		lastPong = item.LastPongAt.Format("15:04:05")
	}
	connected := "down"
	if item.Connected {
		connected = "up"
	}
	return fmt.Sprintf("%s | pending %d | recv %s | pong %s | fails %d", connected, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures)
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
