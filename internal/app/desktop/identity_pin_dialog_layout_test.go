package desktop

import (
	"image"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gioui.org/gpu/headless"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/stretchr/testify/mock"
)

func TestIdentityPinAdvancedSettingsActionExists(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)
	router := new(input.Router)
	settingsTabFrame(t, w, router, image.Pt(390, 900))
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button && node.Desc.Label == w.t("settings.identity_pin_action") {
			return
		}
	}
	t.Fatal("advanced settings offers no dedicated identity pin action")
}

func pinButtonBounds(t *testing.T, router *input.Router, label string) image.Rectangle {
	t.Helper()
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button && node.Desc.Label == label {
			return node.Desc.Bounds
		}
	}
	t.Fatalf("missing button %q", label)
	return image.Rectangle{}
}
func TestIdentityPinModalRealGesturesAndNoConsoleHistory(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)
	w.consoleModal.identityPinRemoval = d
	router := new(input.Router)
	size := image.Pt(900, 900)
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
	consoleModalFrame(w, router, size)
	press(router, centreOf(pinButtonBounds(t, router, w.t("pin.preview"))))
	consoleModalFrame(w, router, size)
	w.sendWG.Wait()
	consoleModalFrame(w, router, size)
	if d.preview == nil || !d.warningShown || d.canConfirm() {
		t.Fatal("preview did not require a separate acknowledgement")
	}
	var ack image.Rectangle
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.CheckBox && node.Desc.Label == w.t("pin.ack") {
			ack = node.Desc.Bounds
		}
	}
	if ack.Empty() || !ack.In(image.Rectangle{Max: size}) {
		t.Fatalf("acknowledgement unreachable: %v", ack)
	}
	press(router, centreOf(ack))
	consoleModalFrame(w, router, size)
	if !d.canConfirm() {
		t.Fatal("explicit visible acknowledgement did not enable confirm")
	}
	provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, preview.Identity, preview.Token).Return(domain.IdentityPinRemovalResult{Identity: preview.Identity, OperationID: "one", Outcome: domain.IdentityPinRemovalCommitted}, nil).Once()
	press(router, centreOf(pinButtonBounds(t, router, w.t("pin.confirm"))))
	consoleModalFrame(w, router, size)
	w.sendWG.Wait()
	consoleModalFrame(w, router, size)
	if d.phase != identityPinLocalPending || d.canConfirm() {
		t.Fatal("local commit did not remain pending")
	}
	if len(w.consoleModal.commandHistory) != 0 {
		t.Fatal("dedicated pin operation entered console history")
	}
	for _, node := range router.AppendSemantics(nil) {
		if strings.Contains(node.Desc.Label, string(preview.Token)) || strings.Contains(node.Desc.Description, string(preview.Token)) {
			t.Fatal("semantic tree exposed confirmation token")
		}
	}
	press(router, centreOf(pinButtonBounds(t, router, w.t("pin.cancel"))))
	consoleModalFrame(w, router, size)
	if d.visible || d.preview != nil || d.editor.Text() != "" || !w.consoleModalVisible() {
		t.Fatal("cancel did not wipe only the inner dialog")
	}
}
func TestIdentityPinModalUnavailableAndCompactFooter(t *testing.T) {
	for _, language := range []string{"en", "ru"} {
		t.Run(language, func(t *testing.T) {
			d, w, provider, preview := pinDialogFixture(t)
			w.language = language
			openConsoleForTest(w)
			w.consoleModal.selectTab(consoleTabSettings)
			w.consoleModal.identityPinRemoval = d
			preview.RemovalAvailable = false
			provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
			d.requestPreview(w)
			w.sendWG.Wait()
			router := new(input.Router)
			size := image.Pt(390, 640)
			consoleModalFrame(w, router, size)
			if d.phase != identityPinUnavailable || d.preview == nil || d.preview.token != "" {
				t.Fatal("unavailable preview retained a usable credential")
			}
			for _, label := range []string{w.t("pin.preview"), w.t("pin.confirm"), w.t("pin.cancel")} {
				bounds := pinButtonBounds(t, router, label)
				if bounds.Empty() || !bounds.In(image.Rectangle{Max: size}) {
					t.Fatalf("compact footer is unreachable: %q %v", label, bounds)
				}
			}
			d.acknowledgeButton.Click()
			d.confirmButton.Click()
			consoleModalFrame(w, router, size)
			provider.AssertNotCalled(t, "ConfirmIdentityPinRemoval", mock.Anything, mock.Anything, mock.Anything)
			w.escapeConsoleModal(layout.Context{Ops: new(op.Ops)})
			consoleModalFrame(w, router, size)
			if d.visible || !w.consoleModalVisible() {
				t.Fatal("Back did not dismiss just the identity dialog")
			}
		})
	}
}
func TestIdentityPinAdvancedActionOpensEmptyDedicatedSurface(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)
	router := new(input.Router)
	size := image.Pt(900, 900)
	consoleModalFrame(w, router, size)
	press(router, centreOf(pinButtonBounds(t, router, w.t("settings.identity_pin_action"))))
	consoleModalFrame(w, router, size)
	d := w.consoleModal.identityPinRemoval
	if !w.consoleModal.identityPinOpen() || d.editor.Text() != "" || d.preview != nil {
		t.Fatal("advanced action did not open an empty exact-identity dialog")
	}
	consoleModalFrame(w, router, size)
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Label == w.t("settings.check_github") {
			t.Fatal("settings controls remain active under dedicated modal")
		}
	}
	d.closeButton.Click()
	consoleModalFrame(w, router, size)
	if d.visible || d.preview != nil {
		t.Fatal("header close did not discard dialog")
	}
}

func TestIdentityPinModalEnterOnlyPreviewsAndEscapeWipes(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	openConsoleForTest(w)
	w.consoleModal.identityPinRemoval = d
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
	router := new(input.Router)
	size := image.Pt(900, 900)
	consoleModalFrame(w, router, size)
	router.Queue(key.Event{Name: key.NameReturn, State: key.Press})
	consoleModalFrame(w, router, size)
	w.sendWG.Wait()
	consoleModalFrame(w, router, size)
	if d.preview == nil || d.canConfirm() {
		t.Fatal("Enter failed to preview safely")
	}
	provider.AssertNotCalled(t, "ConfirmIdentityPinRemoval", mock.Anything, mock.Anything, mock.Anything)
	router.Queue(key.Event{Name: key.NameEscape, State: key.Press})
	consoleModalFrame(w, router, size)
	if d.visible || d.preview != nil || d.editor.Text() != "" || !w.consoleModalVisible() {
		t.Fatal("Escape did not wipe inner dialog")
	}
}
func TestIdentityPinModalHeadlessRender(t *testing.T) {
	for _, language := range []string{"en", "ru"} {
		t.Run(language, func(t *testing.T) {
			const width, height = 390, 760
			surface, err := headless.NewWindow(width, height)
			if err != nil {
				t.Skipf("headless GPU unavailable: %v", err)
			}
			defer surface.Release()
			d, w, provider, preview := pinDialogFixture(t)
			w.language = language
			openConsoleForTest(w)
			w.consoleModal.identityPinRemoval = d
			preview.RemovalAvailable = false
			provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
			d.requestPreview(w)
			w.sendWG.Wait()
			d.poll()
			ops := new(op.Ops)
			w.layoutConsoleOverlay(layout.Context{Ops: ops, Metric: unit.Metric{PxPerDp: 1, PxPerSp: 1}, Constraints: layout.Exact(image.Pt(width, height))})
			if err := surface.Frame(ops); err != nil {
				t.Fatal(err)
			}
			shot := image.NewRGBA(image.Rect(0, 0, width, height))
			if err := surface.Screenshot(shot); err != nil {
				t.Fatal(err)
			}
			if brightPixelBounds(shot).Empty() {
				t.Fatal("dialog rendered no text/control pixels")
			}
			if directory := os.Getenv("CORSA_UI_PIN_RENDER_DIR"); directory != "" {
				file, err := os.Create(filepath.Join(directory, "identity-pin-"+language+".png"))
				if err != nil {
					t.Fatal(err)
				}
				encodeErr := png.Encode(file, shot)
				closeErr := file.Close()
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				if closeErr != nil {
					t.Fatal(closeErr)
				}
			}
		})
	}
}
