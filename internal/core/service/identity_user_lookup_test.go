package service

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestWindowLookupTypedRouterAndDesktopPort(t *testing.T) {
	client, _ := newTestDesktopClientWithNode(t)
	router := NewDMRouter(client, nil, nil, nil)
	target := domaintest.ID("window-lookup")
	generic, err := router.ResolveIdentity(target)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := router.ResolveUserIdentity(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if actual == "" || actual != generic {
		t.Fatal("typed Window join changed normal single-flight resolution")
	}
	first, err := client.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("genuine typed Window/desktop path failed idempotence")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := router.ResolveUserIdentity(ctx, domaintest.ID("cancelled-window")); err == nil {
		t.Fatal("cancelled Window admission accepted")
	}
	if !router.ShutdownDrain(time.Second) {
		t.Fatal("typed lookup operation did not drain")
	}
	if _, err := router.ResolveUserIdentity(t.Context(), target); err == nil {
		t.Fatal("closed router admitted new Window action")
	}
}
