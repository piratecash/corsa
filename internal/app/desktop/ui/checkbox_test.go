package ui

import (
	"image"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

func checkboxKit() Kit {
	return Kit{Theme: material.NewTheme()}
}

func layoutCheckbox(t *testing.T, router *input.Router, button *widget.Clickable, box Checkbox, width int) layout.Dimensions {
	t.Helper()
	gtx := layout.Context{
		Ops:    new(op.Ops),
		Source: router.Source(),
		Metric: unit.Metric{PxPerDp: 1, PxPerSp: 1},
		// A maximum with no minimum, so the row comes out at its own height —
		// an Exact constraint would make every variant exactly as tall as the
		// space offered and hide the difference this measures.
		Constraints: layout.Constraints{Max: image.Pt(width, 200)},
	}
	dims := checkboxKit().Checkbox(gtx, button, box)
	router.Frame(gtx.Ops)
	return dims
}

// A screen reader has to find a checkbox here, and it has to find the state.
// The control paints its own box rather than using material.CheckBox, so the
// semantics are ours to get right.
func TestCheckboxAnnouncesItselfAndItsState(t *testing.T) {
	for _, checked := range []bool{false, true} {
		var router input.Router
		var button widget.Clickable
		layoutCheckbox(t, &router, &button, Checkbox{Label: "Consent", Checked: checked}, 300)

		found := false
		for _, node := range router.AppendSemantics(nil) {
			if node.Desc.Class != semantic.CheckBox {
				continue
			}
			found = true
			if node.Desc.Selected != checked {
				t.Errorf("checked=%v: semantics report Selected=%v", checked, node.Desc.Selected)
			}
			if node.Desc.Label != "Consent" {
				t.Errorf("checked=%v: semantics label = %q, want the checkbox label", checked, node.Desc.Label)
			}
		}
		if !found {
			t.Fatalf("checked=%v: nothing announced itself as a checkbox", checked)
		}
	}
}

// The whole row is the target, label included: an 18dp box is a hard thing to
// hit with a finger, and the label beside it is the part the user is looking
// at.
func TestCheckboxTakesAPressOnItsLabel(t *testing.T) {
	var router input.Router
	var button widget.Clickable
	box := Checkbox{Label: "Check GitHub for new versions", Note: "Off by default."}

	const width = 300
	frame := func() bool {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(width, 200)),
		}
		clicked := button.Clicked(gtx)
		checkboxKit().Checkbox(gtx, &button, box)
		router.Frame(gtx.Ops)
		return clicked
	}

	frame()

	// Well to the right of the 18dp box, in the label column.
	at := f32.Pt(width-40, 10)
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)

	if !frame() {
		t.Fatal("a press on the label did not reach the checkbox")
	}
}

// The note is a second line, not a longer first one: it carries the
// consequence of ticking the box, and the row has to grow to hold it or the
// consequence is the part that gets clipped.
func TestCheckboxWithANoteIsTallerThanWithout(t *testing.T) {
	var routerPlain, routerNoted input.Router
	var plainButton, notedButton widget.Clickable

	plain := layoutCheckbox(t, &routerPlain, &plainButton, Checkbox{Label: "Consent"}, 300)
	noted := layoutCheckbox(t, &routerNoted, &notedButton,
		Checkbox{Label: "Consent", Note: "Off by default. The request goes straight to a third party."}, 300)

	if noted.Size.Y <= plain.Size.Y {
		t.Fatalf("with a note the row is %ddp tall, without it %ddp: the note is not on a line of its own",
			noted.Size.Y, plain.Size.Y)
	}
}

// The box keeps its own size. It is a Rigid child of a Flex, which hands every
// child the row's cross-axis minimum — a box that took it would be as tall as
// the note beside it.
func TestCheckboxBoxDoesNotStretchToTheRow(t *testing.T) {
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(300, 200)),
	}
	dims := checkboxKit().checkboxMark(gtx, Checkbox{Checked: true})

	if dims.Size.X != checkboxBoxDp || dims.Size.Y != checkboxBoxDp {
		t.Fatalf("the box laid out %v, want %dx%d", dims.Size, checkboxBoxDp, checkboxBoxDp)
	}
}
