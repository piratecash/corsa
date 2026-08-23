package desktop

import (
	"testing"
)

// The component itself is covered in internal/app/desktop/ui. What belongs
// here is how this application drives it.

// The language button reads as active while its menu is open, the same way the
// selected console tab does.
func TestLanguageToolbarButtonIsActiveWhileTheMenuIsOpen(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)

	if w.languageToolbarButton().Active {
		t.Fatal("language button is active with the menu closed")
	}
	w.showLanguageMenu = true
	if !w.languageToolbarButton().Active {
		t.Fatal("language button is not active with the menu open")
	}
}
