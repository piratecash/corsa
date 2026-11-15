package desktop

import (
	"fmt"
	"image"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"gioui.org/io/input"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestCompactContactsKeepsNamedSearchResultAfterRestart(t *testing.T) {
	const alias = "PC"
	peer := domaintest.ID("remote identity")
	self := domaintest.ID("local identity")
	path := filepath.Join(t.TempDir(), "identity.desktop.json")
	startWindow := func() *Window {
		t.Helper()
		prefs, err := LoadPreferences(path)
		if err != nil {
			t.Fatal(err)
		}
		w := newIdentityLayoutTestWindow(t)
		w.prefs = prefs
		w.snap.MyAddress = self
		w.contactsList = widget.List{List: layout.List{Axis: layout.Vertical}}
		return w
	}
	var router input.Router
	context := func() layout.Context {
		return layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(360, 720)),
		}
	}
	frame := func(w *Window) {
		gtx := context()
		w.layoutMain(gtx)
		router.Frame(gtx.Ops)
	}

	w := startWindow()
	query := peer.String()[:4]
	w.identitySearchEditor.SetText(query)
	seedIdentitySearch(w, query, peer.String())
	frame(w)
	if w.searchItems != 1 || w.contactsItems != 0 {
		t.Fatalf("fresh client has %d search rows and %d known identity rows, want 1 and 0", w.searchItems, w.contactsItems)
	}
	w.recipientMenuButton(peer).Click()
	frame(w)
	w.ctxMenuAlias.Click()
	w.handleContextMenuActions(context())
	if !w.showAliasEditor || w.contextMenuPeer != peer {
		t.Fatal("search result did not open its alias editor")
	}
	w.aliasEditor.SetText(alias)
	w.ctxMenuAliasSave.Click()
	w.handleContextMenuActions(context())

	// The new window only has the preferences on disk, with no message
	// history, discovery state, or active search carried over.
	restarted := startWindow()
	if got := restarted.prefs.Alias(peer); got != alias {
		t.Fatalf("saved name = %q after reloading preferences, want %q", got, alias)
	}
	frame(restarted)
	if restarted.searchItems != 0 || restarted.contactsItems != 1 {
		t.Fatalf("restarted client has %d search rows and %d known identity rows, want 0 and 1", restarted.searchItems, restarted.contactsItems)
	}
	if got := restarted.snapRecipients(); !slices.Equal(got, []domain.PeerIdentity{peer}) {
		t.Fatalf("known identities = %v, want [%s]", got, peer)
	}
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Label == alias {
			return
		}
	}
	t.Fatalf("compact contacts pane does not show the saved name %q", alias)
}

func TestKnownIdentitiesMergeExistingAliasPreferencesWithConversations(t *testing.T) {
	self := domaintest.ID("self")
	aliasOnly := domaintest.ID("alias without conversation")
	conversation := domaintest.ID("alias with conversation")
	unnamed := domaintest.ID("empty alias")
	path := filepath.Join(t.TempDir(), "identity.desktop.json")
	payload := fmt.Sprintf(`{"language":"en","aliases":{"%s":"PC","%s":"Laptop","%s":"Self","%s":"","invalid":"Invalid"}}`,
		aliasOnly, conversation, self, unnamed)
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	prefs, err := LoadPreferences(path)
	if err != nil {
		t.Fatal(err)
	}
	w := &Window{
		prefs: prefs,
		snap: service.RouterSnapshot{
			MyAddress: self,
			Peers:     map[domain.PeerIdentity]*service.RouterPeerState{conversation: {}},
			PeerOrder: []domain.PeerIdentity{conversation},
		},
	}
	want := []domain.PeerIdentity{conversation, aliasOnly}
	if got := w.snapRecipients(); !slices.Equal(got, want) {
		t.Fatalf("known identities from existing preferences and conversations = %v, want %v", got, want)
	}

	prefs.SetAlias(aliasOnly, "")
	prefs.SetAlias(conversation, "")
	if got := w.snapRecipients(); !slices.Equal(got, []domain.PeerIdentity{conversation}) {
		t.Fatalf("known identities after clearing names = %v, want [%s]", got, conversation)
	}
}
