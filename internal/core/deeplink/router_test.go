package deeplink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// TestRouterDispatchesByKind: the contact handler sees contact links and
// nothing else; another member's link reaches its own handler.
func TestRouterDispatchesByKind(t *testing.T) {
	var contactSeen, groupSeen []string

	router, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		deeplink.KindContact: deeplink.HandlerFunc(func(_ context.Context, link deeplink.Link) error {
			contactSeen = append(contactSeen, link.URI)
			return nil
		}),
		"group": deeplink.HandlerFunc(func(_ context.Context, link deeplink.Link) error {
			groupSeen = append(groupSeen, link.URI)
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	contact := newContactLink(t)
	if _, err := router.Handle(context.Background(), contact); err != nil {
		t.Fatalf("handle contact: %v", err)
	}
	if _, err := router.Handle(context.Background(), "corsa:group/abc?v=1"); err != nil {
		t.Fatalf("handle group: %v", err)
	}

	if len(contactSeen) != 1 || contactSeen[0] != contact {
		t.Errorf("contact handler saw %v, want [%s]", contactSeen, contact)
	}
	if len(groupSeen) != 1 || groupSeen[0] != "corsa:group/abc?v=1" {
		t.Errorf("group handler saw %v", groupSeen)
	}
}

// TestRouterUnsupportedKind: a link minted by a newer build names its
// kind in a distinguishable error, so the UI can say "this version does
// not support that link" instead of "malformed".
func TestRouterUnsupportedKind(t *testing.T) {
	router, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		deeplink.KindContact: deeplink.HandlerFunc(func(context.Context, deeplink.Link) error { return nil }),
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	link, err := router.Handle(context.Background(), "corsa:group/abc?v=1")
	if !errors.Is(err, deeplink.ErrUnsupportedKind) {
		t.Fatalf("err = %v, want %v", err, deeplink.ErrUnsupportedKind)
	}
	if link.Kind != "group" {
		t.Errorf("kind = %q, want group — the caller cannot name it otherwise", link.Kind)
	}
}

// TestRouterReportsHandlerFailure: a member that refuses its own link
// (bad keys, wrong network) surfaces unchanged, together with the kind.
func TestRouterReportsHandlerFailure(t *testing.T) {
	refused := errors.New("keys do not verify")
	router, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		deeplink.KindContact: deeplink.HandlerFunc(func(context.Context, deeplink.Link) error { return refused }),
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	link, err := router.Handle(context.Background(), newContactLink(t))
	if !errors.Is(err, refused) {
		t.Fatalf("err = %v, want %v", err, refused)
	}
	if link.Kind != deeplink.KindContact {
		t.Errorf("kind = %q, want %q", link.Kind, deeplink.KindContact)
	}
}

// TestRouterMalformedNeverReachesAHandler: classification fails first,
// so no handler is asked to defend itself against junk.
func TestRouterMalformedNeverReachesAHandler(t *testing.T) {
	called := false
	router, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		deeplink.KindContact: deeplink.HandlerFunc(func(context.Context, deeplink.Link) error {
			called = true
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	if _, err := router.Handle(context.Background(), "https://example.org"); !errors.Is(err, deeplink.ErrMalformed) {
		t.Fatalf("err = %v, want %v", err, deeplink.ErrMalformed)
	}
	if called {
		t.Error("a handler was asked to parse a non-corsa URI")
	}
}

// TestNewRouterRejectsBrokenTables: an unroutable table is a build
// mistake and must fail at construction, not at the user's click.
func TestNewRouterRejectsBrokenTables(t *testing.T) {
	if _, err := deeplink.NewRouter(nil); err == nil {
		t.Error("empty table accepted")
	}
	if _, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{deeplink.KindContact: nil}); err == nil {
		t.Error("nil handler accepted")
	}
	if _, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		"Not A Kind": deeplink.HandlerFunc(func(context.Context, deeplink.Link) error { return nil }),
	}); err == nil {
		t.Error("kind outside the name charset accepted")
	}
}
