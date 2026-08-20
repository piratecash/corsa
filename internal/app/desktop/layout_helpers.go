package desktop

import "gioui.org/layout"

func layoutVerticallyCentered(gtx layout.Context, content layout.Widget) layout.Dimensions {
	return layout.Flex{
		Axis:      layout.Vertical,
		Alignment: layout.Start,
		Spacing:   layout.SpaceSides,
	}.Layout(gtx, layout.Rigid(content))
}
