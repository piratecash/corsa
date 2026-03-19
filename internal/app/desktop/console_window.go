package desktop

import (
	"image/color"
	"strings"
	"time"

	"corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget/material"
)

type ConsoleWindow struct {
	parent  *Window
	theme   *material.Theme
	ops     op.Ops
	onClose func()
}

func NewConsoleWindow(parent *Window, onClose func()) *ConsoleWindow {
	return &ConsoleWindow{
		parent:  parent,
		theme:   parent.theme,
		onClose: onClose,
	}
}

func (c *ConsoleWindow) Open() {
	go func() {
		window := new(app.Window)
		window.Option(
			app.Title(c.parent.t("console.title")),
			app.Size(unit.Dp(520), unit.Dp(460)),
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
			return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(240)))
				btn := material.Button(c.theme, &c.parent.rebuildTrustButton, c.parent.t("node.trust_rebuild"))
				return btn.Layout(gtx)
			})
		})
	})
}

func colorBackground() color.NRGBA {
	return color.NRGBA{R: 12, G: 15, B: 20, A: 255}
}

func (c *ConsoleWindow) consoleRows(status service.NodeStatus) []string {
	rows := []string{
		c.parent.t("node.client_version", c.parent.client.Version()),
		c.parent.t("node.peer_version", fallback(status.ClientVersion, strings.ReplaceAll(c.parent.client.Version(), " ", "-"))),
		c.parent.t("node.listen", c.parent.runtime.ListenAddress()),
		c.parent.t("node.type", fallback(status.NodeType, "full")),
		c.parent.t("node.services", fallback(joinOrNone(status.Services), "identity,contacts,messages,gazeta,relay")),
		c.parent.t("node.connected", status.Connected),
		c.parent.t("node.peers", len(status.Peers)),
		c.parent.localNodeErrorRow(),
	}

	if !status.CheckedAt.IsZero() {
		rows = append(rows, c.parent.t("node.checked", status.CheckedAt.Format(time.RFC3339)))
	}

	return rows
}
