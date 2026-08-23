package desktop

import (
	"image"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/service"
)

// console_modal_shell_test.go covers the console AS A MODAL: opening it,
// closing it the three ways a modal closes, and the state it does and does not
// keep across a close. The console's own behaviour — commands, suggestions,
// tabs — is in console_modal_test.go.

// openConsoleForTest opens the console modal outside a frame. The real opener
// runs inside one and hands its context on so the emoji picker can close
// through its own path; a test that has no frame yet supplies a bare one.
func openConsoleForTest(w *Window) {
	w.openConsoleModal(layout.Context{Ops: new(op.Ops)})
}

// newConsoleModalTestWindow builds a window whose console modal can be laid
// out for real. No router is needed: the tab content reads the frame's
// snapshot off the parent (see consoleModal.layoutActiveTab), and the zero
// snapshot is what an idle node looks like anyway.
func newConsoleModalTestWindow(t *testing.T) *Window {
	t.Helper()
	w := newIdentityLayoutTestWindow(t)
	w.consoleModal = newConsoleModal(w)
	return w
}

func consoleModalFrame(w *Window, router *input.Router, size image.Point) layout.Context {
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(size),
	}
	w.handleActions(gtx)
	if w.consoleModalVisible() {
		w.layoutConsoleOverlay(gtx)
	}
	router.Frame(gtx.Ops)
	return gtx
}

func TestConsoleModalOpensClosedAndReusesOneInstance(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	if w.consoleModalVisible() {
		t.Fatal("console modal is visible before anything opened it")
	}

	openConsoleForTest(w)
	if !w.consoleModalVisible() {
		t.Fatal("openConsoleModal did not show the modal")
	}
	first := w.consoleModal

	w.closeConsoleModal()
	if w.consoleModalVisible() {
		t.Fatal("closeConsoleModal did not hide the modal")
	}

	openConsoleForTest(w)
	if w.consoleModal != first {
		t.Fatal("reopening the console built a second instance; command history would be lost")
	}
}

// Closing must not throw away what the user typed: the modal is reused, so the
// command history survives it.
func TestConsoleModalKeepsHistoryAcrossClose(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.commandHistory = []string{"getPeers"}
	w.closeConsoleModal()
	openConsoleForTest(w)

	if got := w.consoleModal.commandHistory; len(got) != 1 || got[0] != "getPeers" {
		t.Fatalf("command history after close/open = %v, want [getPeers]", got)
	}
}

// Opening the console must clear the identity panel: two stacked modals leave
// the user with a dismissal order nothing on screen explains.
func TestConsoleModalClosesTheIdentityPanel(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	w.identityPanelVisible = true

	openConsoleForTest(w)

	if w.identityPanelVisible {
		t.Fatal("identity panel stayed open under the console modal")
	}
}

// Escape closes the console the same way it closes identity details. Before
// this it fell through to submitConsoleCommand and RAN whatever was typed.
func TestConsoleModalEscapeClosesInsteadOfSubmitting(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.consoleEditor.SetText("getPeers")

	consoleModalFrame(w, &router, image.Pt(1000, 700))
	router.Queue(key.Event{Name: key.NameEscape, State: key.Press})
	consoleModalFrame(w, &router, image.Pt(1000, 700))

	if w.consoleModalVisible() {
		t.Fatal("Escape did not close the console modal")
	}
	if got := len(w.consoleModal.consoleHistory()); got != 1 {
		t.Fatalf("Escape submitted the command: console history has %d entries, want the welcome entry only", got)
	}
}

// A press beside the card closes; a press on the card does not.
func TestConsoleModalBackdropDismissesOnlyOutsideTheCard(t *testing.T) {
	press := func(t *testing.T, position f32.Point) bool {
		t.Helper()
		var router input.Router
		w := newConsoleModalTestWindow(t)
		openConsoleForTest(w)

		consoleModalFrame(w, &router, image.Pt(1000, 700))
		router.Queue(pointer.Event{
			Source:   pointer.Mouse,
			Kind:     pointer.Press,
			Buttons:  pointer.ButtonPrimary,
			Position: position,
		})
		consoleModalFrame(w, &router, image.Pt(1000, 700))
		return w.consoleModalVisible()
	}

	// The card is inset by ui.ModalCardInsetDp, so only that thin margin is
	// outside it.
	if press(t, f32.Pt(2, 2)) {
		t.Fatal("press on the backdrop did not close the console modal")
	}
	if !press(t, f32.Pt(500, 350)) {
		t.Fatal("press inside the console card closed the modal")
	}
}

// The Console button in the main window is what opens the modal.
func TestConsoleButtonOpensTheModal(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(200, 60)),
	}
	w.handleActions(gtx)
	w.layoutConsoleButton(gtx)
	router.Frame(gtx.Ops)

	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: f32.Pt(40, 20)},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: f32.Pt(40, 20)},
	)
	gtx = layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(200, 60)),
	}
	w.handleActions(gtx)

	if !w.consoleModalVisible() {
		t.Fatal("clicking the Console button did not open the console modal")
	}
}

// The console button is laid out on every platform. Android used to get the
// network bar alone, because the console was a second app.Window there — which
// Android has no way to show.
func TestComposerFooterCarriesTheConsoleButton(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(450, 200)},
	}
	w.layoutComposerFooter(gtx, service.NodeStatus{})
	router.Frame(gtx.Ops)

	buttons := 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button {
			buttons++
		}
	}
	if buttons < 1 {
		t.Fatal("footer exposes no buttons, want the console button")
	}
}

// Back (Android hardware/gesture Back) closes the topmost modal, and the
// console sits above identity details.
func TestConsoleModalIsTheTopBackTarget(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	gtx := layout.Context{
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}

	w.identityPanelVisible = true
	openConsoleForTest(w)
	// openConsoleModal clears the identity panel, so put it back to prove the
	// ordering rather than the clearing.
	w.identityPanelVisible = true

	if got := w.topNavigationDismissTarget(gtx); got != dismissConsoleModal {
		t.Fatalf("top dismiss target = %v, want the console modal", got)
	}
}

// The traffic ticker samples only while the graph has an audience.
func TestTrafficTickerFollowsTheVisibleGraph(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	console := w.console()

	if console.trafficViewVisible() {
		t.Fatal("traffic considered visible with the console closed")
	}

	openConsoleForTest(w)
	if console.trafficViewVisible() {
		t.Fatal("traffic considered visible on the Console tab")
	}

	console.selectTab(consoleTabTraffic)
	if !console.trafficViewVisible() {
		t.Fatal("traffic not considered visible on the Traffic tab")
	}

	w.closeConsoleModal()
	if console.trafficViewVisible() {
		t.Fatal("traffic still considered visible after the console closed")
	}
}
