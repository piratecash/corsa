package desktop

import (
	"gioui.org/layout"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

// layoutVerticallyCentered is ui.VerticallyCentered under the name the screens
// in this package already call it by. The rule itself moved to the component
// package, where the emoji panel's search field needs it too.
func layoutVerticallyCentered(gtx layout.Context, content layout.Widget) layout.Dimensions {
	return ui.VerticallyCentered(gtx, content)
}
