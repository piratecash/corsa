package desktop

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestK1DesktopPasteRetainsLiveContactWhenDurableIntentUnavailable(t *testing.T) {
	for _, language := range []string{"en", "ru"} {
		t.Run(language, func(t *testing.T) {
			owner, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			peer, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0"})
			// No trust path: ordinary contacts remain usable, explicit durable authority
			// is unavailable. No application, network or node Run is started.
			svc := node.NewService(cfg, owner, nil)
			t.Cleanup(svc.WaitBackground)
			client := service.NewDesktopClient(config.App{Network: "gazeta-devnet"}, cfg, owner, svc, nil)
			router := service.NewDMRouter(client, nil, nil, nil)
			t.Cleanup(func() {
				if !router.ShutdownDrain(time.Second) {
					t.Error("router did not drain")
				}
			})
			w := newIdentityLayoutTestWindow(t)
			w.language = language
			w.router = router
			link, err := contactlink.Build(peer, domain.NetworkID("gazeta-devnet"))
			if err != nil {
				t.Fatal(err)
			}
			w.identitySearchEditor.SetText(link)
			w.handleContactLinkPaste()
			if w.identitySearchEditor.Text() != "" || w.lastContactLinkTried != "" {
				t.Fatal("partial import was discarded/reported as a total failure")
			}
			if got, want := router.Snapshot().SendStatus, w.t("status.contact_imported_k_unconfirmed", shortFingerprint(peer.Address)); got != want {
				t.Fatalf("dishonest partial status %q, want %q", got, want)
			}
			contacts := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
			for _, contact := range contacts.Contacts {
				if contact.Address == peer.Address {
					return
				}
			}
			t.Fatal("ordinary live contact was lost on strict-backend refusal")
		})
	}
}
