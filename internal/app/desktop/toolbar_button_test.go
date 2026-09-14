package desktop

import (
	"testing"
)

// The component itself is covered in internal/app/desktop/ui. What belongs
// here is how this application drives it.

// The Console button reads as active while the modal it opens is on screen,
// the same way the selected console tab does.
func TestConsoleToolbarButtonIsActiveWhileTheModalIsOpen(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)

	if w.consoleToolbarButton().Active {
		t.Fatal("console button is active with the modal closed")
	}

	w.consoleModal = newConsoleModal(w)
	w.consoleModal.visible.Store(true)

	if !w.consoleToolbarButton().Active {
		t.Fatal("console button is not active with the modal open")
	}
}
