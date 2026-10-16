package node

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// dial_intent_test.go pins what a dial with the intent "to X" does that an
// address dial does not (docs/protocol/session_v2.md, "Dial intent"): the
// handshake expects X, so another identity proven on the endpoint closes the
// dial with errEndpointOwnerMismatch, which is not charged to the address and
// leaves the endpoint's binding as it was; X proven there is a success that
// takes the endpoint over from a previous owner; and only such a dial, to an
// identity that is this node's contact, protects the endpoint — and raises an
// ordinary binding only when X already owned it. An address dial keeps
// creating and renewing ordinary bindings alone.

// dialToIdentity is the intent "to x"; it fails the test for the zero
// identity.
func dialToIdentity(t *testing.T, x domain.PeerIdentity) domain.DialIntent {
	t.Helper()
	intent, err := domain.DialToIdentity(x)
	if err != nil {
		t.Fatalf("intent to %s: %v", x, err)
	}
	return intent
}

// dialWithIntent makes one dial from svc to address with intent, over a plain
// TCP socket, and returns how it ended. An established session is closed at
// once: nothing owns it. Only a dial that could not open its socket at all is
// repeated — it never reached the peer.
func dialWithIntent(t *testing.T, svc *Service, address domain.PeerAddress, intent domain.DialIntent) error {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		transport, err := svc.dialPeerTransport(ctx, address, intent, func(ctx context.Context) (net.Conn, error) {
			var dialer net.Dialer
			raw, err := dialer.DialContext(ctx, "tcp", string(address))
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errPeerDialTransport, err)
			}
			return raw, nil
		})
		cancel()
		if err == nil {
			_ = transport.wire.Close()
			return nil
		}
		if !errors.Is(err, errPeerDialTransport) {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatalf("no dial to %s opened its socket (waited 15s): %v", address, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// trustAsContact makes id one of svc's contacts, as the user adding it would.
func trustAsContact(t *testing.T, svc *Service, id domain.PeerIdentity) {
	t.Helper()
	if stored, err := svc.trust.remember(trustedContact{Address: id.String(), PubKey: "pk-contact", Source: "test"}); err != nil || !stored {
		t.Fatalf("trust %s: stored %v, err %v", id, stored, err)
	}
}

// sameBinding reports whether after is before untouched: the same owner, the
// same protection, the same last v2 session.
func sameBinding(before, after endpointBinding) bool {
	sameProtection := (before.protection == nil) == (after.protection == nil) &&
		(before.protection == nil || *before.protection == *after.protection)
	return before.identity == after.identity && sameProtection && before.lastV2.Equal(after.lastV2)
}

// A dial "to X" that meets another identity Y on an endpoint bound to X is
// closed with errEndpointOwnerMismatch: Y's session is not a session with X.
// The binding stays X's, with its tier and its term, Y is not pinned for it,
// no change of owner is journalled, and the mismatch is journalled with the
// endpoint and the identity that was expected.
func TestADialToXThatMeetsAnotherIdentityIsClosedAndLeavesTheBinding(t *testing.T) {
	t.Parallel()
	for name, tc := range ownerChangeCases() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			listener, address, stopListener := listenerNode(t, nil)
			defer stopListener()
			endpoint := domain.PeerAddress(normalizeAddress(address))
			expected, other := domain.PeerIdentity{0x95, 0x01}, peerIdentityOf(t, listener)

			var journal *storeJournal
			dialer, stopDialer := startQuietNode(t, func(svc *Service) {
				journal = journalledStore(svc.secureSessions.store)
				if result, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), expected, tc.protection); err != nil || result.tier != tc.tier {
					t.Fatalf("bind the expected owner: tier %v, err %v", result.tier, err)
				}
			})
			defer stopDialer()
			store := dialer.secureSessions.store
			before, _ := bindingOf(store, endpoint)

			err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, expected))
			if !errors.Is(err, errEndpointOwnerMismatch) {
				t.Fatalf("dial to %s met %s: err = %v, want errEndpointOwnerMismatch", expected, other, err)
			}
			if !isUnchargedDialRefusal(err) {
				t.Fatalf("an owner mismatch is charged to the address: %v", err)
			}

			after, bound := bindingOf(store, endpoint)
			if !bound || !sameBinding(before, after) {
				t.Fatalf("the mismatch moved the binding from %+v to %+v (bound %v)", before, after, bound)
			}
			if store.identityPinned(other) {
				t.Fatalf("the identity met instead of %s was pinned by a dial that was not meant for it", expected)
			}
			if got := len(journal.entries(t, "endpoint_owner_changed")); got != 0 {
				t.Fatalf("a mismatch journalled %d changes of owner, want none", got)
			}
			mismatches := journal.entries(t, "endpoint_owner_mismatch")
			if len(mismatches) != 1 || mismatches[0]["peer"] != string(endpoint) || mismatches[0]["expected_identity"] != expected.String() || mismatches[0]["identity"] != other.String() {
				t.Fatalf("endpoint_owner_mismatch = %v, want one line naming %s, expected %s and proven %s", mismatches, endpoint, expected, other)
			}
			if connID, ok := mismatches[0]["conn_id"].(float64); !ok || connID == 0 {
				t.Fatalf("endpoint_owner_mismatch conn_id = %v, want the dial's connection", mismatches[0]["conn_id"])
			}
		})
	}
}

// A dial "to X" that meets Y on an endpoint with no binding is closed the same
// way, and leaves no binding behind: only a session with the identity the dial
// meant earns one.
func TestADialToXThatMeetsAnotherIdentityOnAFreeEndpointBindsNothing(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	store := dialer.secureSessions.store

	if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, domain.PeerIdentity{0x95, 0x02})); !errors.Is(err, errEndpointOwnerMismatch) {
		t.Fatalf("err = %v, want errEndpointOwnerMismatch", err)
	}
	if binding, bound := bindingOf(store, endpoint); bound {
		t.Fatalf("a dial that met the wrong identity bound the endpoint: %+v", binding)
	}
	if store.identityPinned(peerIdentityOf(t, listener)) {
		t.Fatal("a dial that met the wrong identity pinned it")
	}
}

// On an endpoint last owned by Y, a dial "to X" that proves
// X succeeds and takes the endpoint over for X with the tier it had: the
// change of owner is journalled, and a contact X does not raise an ordinary
// binding it did not own yet.
func TestADialToXThatProvesXTakesTheEndpointOverFromY(t *testing.T) {
	t.Parallel()
	for name, tc := range ownerChangeCases() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			listener, address, stopListener := listenerNode(t, nil)
			defer stopListener()
			endpoint := domain.PeerAddress(normalizeAddress(address))
			previous, x := domain.PeerIdentity{0x95, 0x03}, peerIdentityOf(t, listener)

			var journal *storeJournal
			dialer, stopDialer := startQuietNode(t, func(svc *Service) {
				journal = journalledStore(svc.secureSessions.store)
				trustAsContact(t, svc, x)
				if result, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), previous, tc.protection); err != nil || result.tier != tc.tier {
					t.Fatalf("bind the previous owner: tier %v, err %v", result.tier, err)
				}
			})
			defer stopDialer()
			store := dialer.secureSessions.store
			before, _ := bindingOf(store, endpoint)

			if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
				t.Fatalf("dial to %s that proved it: %v", x, err)
			}

			after, bound := bindingOf(store, endpoint)
			if !bound || after.identity != x {
				t.Fatalf("binding after the dial: bound %v, owner %s; want %s", bound, after.identity, x)
			}
			if after.tier() != tc.tier {
				t.Fatalf("the change of owner moved the tier from %v to %v", tc.tier, after.tier())
			}
			if (before.protection == nil) != (after.protection == nil) || (after.protection != nil && *after.protection != *before.protection) {
				t.Fatalf("the change of owner moved the protection from %+v to %+v", before.protection, after.protection)
			}
			changes := journal.entries(t, "endpoint_owner_changed")
			if len(changes) != 1 || changes[0]["previous_identity"] != previous.String() || changes[0]["identity"] != x.String() {
				t.Fatalf("endpoint_owner_changed = %v, want one line from %s to %s", changes, previous, x)
			}
		})
	}
}

// A contact X and its endpoint: an address dial that proves X again
// leaves the binding ordinary; a dial "to X" that proves X, which already
// owned the endpoint, raises it to protected in pool K with X as its giver and
// journals it; a dial "to X" on an endpoint Y owned takes it over without
// raising it, and the next dial "to X" raises it. A dial "to X" to a free
// endpoint creates it protected.
func TestOnlyADialToAContactThatAlreadyOwnedTheEndpointRaisesIt(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	x := peerIdentityOf(t, listener)

	var journal *storeJournal
	dialer, stopDialer := startQuietNode(t, func(svc *Service) {
		journal = journalledStore(svc.secureSessions.store)
		trustAsContact(t, svc, x)
	})
	defer stopDialer()
	store := dialer.secureSessions.store
	wantGeneral := func(what string) {
		t.Helper()
		if binding, bound := bindingOf(store, endpoint); !bound || binding.identity != x || binding.tier() != bindingTierGeneral {
			t.Fatalf("%s: bound %v, owner %s, tier %v; want an ordinary binding of %s", what, bound, binding.identity, binding.tier(), x)
		}
	}
	wantProtectedByX := func(what string) {
		t.Helper()
		binding, bound := bindingOf(store, endpoint)
		if !bound || binding.protection == nil || binding.protection.by != x || binding.protection.pool != domain.ProtectedBindingPoolContact {
			t.Fatalf("%s: bound %v, protection %+v; want protected by %s in pool K", what, bound, binding.protection, x)
		}
	}

	if err := dialWithIntent(t, dialer, endpoint, domain.DialToAddress()); err != nil {
		t.Fatalf("address dial: %v", err)
	}
	if err := dialWithIntent(t, dialer, endpoint, domain.DialToAddress()); err != nil {
		t.Fatalf("second address dial: %v", err)
	}
	wantGeneral("an address dial that proved the contact again")

	if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
		t.Fatalf("dial to the contact: %v", err)
	}
	wantProtectedByX("a dial to the contact that already owned the endpoint")
	protections := journal.entries(t, "endpoint_binding_protected")
	if len(protections) != 1 || protections[0]["peer"] != string(endpoint) || protections[0]["identity"] != x.String() || protections[0]["pool"] != string(domain.ProtectedBindingPoolContact) {
		t.Fatalf("endpoint_binding_protected = %v, want one line naming %s, %s and pool K", protections, endpoint, x)
	}
	if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
		t.Fatalf("second dial to the contact: %v", err)
	}
	if got := len(journal.entries(t, "endpoint_binding_protected")); got != 1 {
		t.Fatalf("a renewal of a protected binding journalled a protection again: %d lines", got)
	}

	t.Run("taken over from Y, then raised", func(t *testing.T) {
		listener, address, stopListener := listenerNode(t, nil)
		defer stopListener()
		endpoint := domain.PeerAddress(normalizeAddress(address))
		x := peerIdentityOf(t, listener)
		dialer, stopDialer := startQuietNode(t, func(svc *Service) {
			trustAsContact(t, svc, x)
			if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{0x95, 0x04}, endpointProtectionNone()); err != nil {
				t.Fatalf("bind Y: %v", err)
			}
		})
		defer stopDialer()
		store := dialer.secureSessions.store

		if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
			t.Fatalf("dial to the contact on Y's endpoint: %v", err)
		}
		if binding, _ := bindingOf(store, endpoint); binding.identity != x || binding.tier() != bindingTierGeneral {
			t.Fatalf("after the change of owner: owner %s, tier %v; want %s, ordinary", binding.identity, binding.tier(), x)
		}
		if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
			t.Fatalf("next dial to the contact: %v", err)
		}
		if binding, _ := bindingOf(store, endpoint); binding.protection == nil || binding.protection.by != x {
			t.Fatalf("the next dial to the contact left the protection %+v, want given by %s", binding.protection, x)
		}
	})

	t.Run("a free endpoint", func(t *testing.T) {
		listener, address, stopListener := listenerNode(t, nil)
		defer stopListener()
		endpoint := domain.PeerAddress(normalizeAddress(address))
		x := peerIdentityOf(t, listener)
		dialer, stopDialer := startQuietNode(t, func(svc *Service) { trustAsContact(t, svc, x) })
		defer stopDialer()

		if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
			t.Fatalf("dial to the contact: %v", err)
		}
		if binding, _ := bindingOf(dialer.secureSessions.store, endpoint); binding.protection == nil || binding.protection.by != x || binding.protection.pool != domain.ProtectedBindingPoolContact {
			t.Fatalf("a dial to the contact on a free endpoint left the protection %+v, want given by %s in pool K", binding.protection, x)
		}
	})
}

// P2-R11. A dial "to X" for an X that is not this node's contact is a
// success that binds the endpoint as an ordinary binding: protection is for
// contacts, and a relation is not derived by any path yet.
func TestADialToANonContactCreatesAnOrdinaryBinding(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	x := peerIdentityOf(t, listener)
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	store := dialer.secureSessions.store

	for range 2 {
		if err := dialWithIntent(t, dialer, endpoint, dialToIdentity(t, x)); err != nil {
			t.Fatalf("dial to a non-contact: %v", err)
		}
	}
	if binding, bound := bindingOf(store, endpoint); !bound || binding.identity != x || binding.tier() != bindingTierGeneral {
		t.Fatalf("bound %v, owner %s, tier %v; want an ordinary binding of %s", bound, binding.identity, binding.tier(), x)
	}
}

// A dial path that states no intent is refused before any socket: the zero
// DialIntent is not taken for an address dial.
func TestADialWithoutAnIntentIsRefusedBeforeAnySocket(t *testing.T) {
	t.Parallel()
	svc := newLegacyRefusalService(t)
	sockets := 0
	_, err := svc.dialPeerTransport(context.Background(), "198.51.100.7:64646", domain.DialIntent{}, func(context.Context) (net.Conn, error) {
		sockets++
		return nil, errors.New("no socket expected")
	})
	if !errors.Is(err, domain.ErrDialIntentUnset) {
		t.Fatalf("err = %v, want domain.ErrDialIntentUnset", err)
	}
	if sockets != 0 {
		t.Fatalf("a dial without an intent opened %d sockets", sockets)
	}
}

// No production path dials "to X" yet: which paths carry the intent is a
// network-visible decision (an endpoint protected for good), taken path by
// path. The guard turns red on the first production call of
// domain.DialToIdentity anywhere in the module, so that call comes with its
// decision, its tests and docs/protocol/session_v2.md.
func TestNoProductionPathDialsToAnIdentityYet(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	if calls := callsOfDialToIdentity(t, root); len(calls) > 0 {
		t.Fatalf("a production path dials to an identity: %s — decide it in docs/protocol/session_v2.md (\"Dial intent\") and update this guard", strings.Join(calls, ", "))
	}
}

// The guard reads the whole module, not only this package: a call from
// another package — the SDK, the desktop app — is found too, and so is one
// spelled without the package name inside domain itself. Test files are not
// production paths.
func TestTheDialToIdentityGuardReadsTheWholeModule(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeProbeFile(t, root, "go.mod", "module probe\n")
	writeProbeFile(t, root, "sdk/contact.go", "package sdk\nfunc open() { _, _ = domain.DialToIdentity(x) }\n")
	writeProbeFile(t, root, "internal/core/domain/inside.go", "package domain\nfunc inside() { _, _ = DialToIdentity(x) }\n")
	writeProbeFile(t, root, "internal/core/node/caller_test.go", "package node\nfunc probe() { _, _ = domain.DialToIdentity(x) }\n")
	writeProbeFile(t, root, ".gomodcache/dep/dep.go", "package dep\nfunc dep() { _, _ = domain.DialToIdentity(x) }\n")
	calls := callsOfDialToIdentity(t, root)
	if len(calls) != 2 {
		t.Fatalf("calls found = %v, want the SDK one and the one inside domain", calls)
	}
}

func writeProbeFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// moduleRoot is the directory of the module's go.mod, above this package.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the package")
		}
		dir = parent
	}
}

// callsOfDialToIdentity lists the calls of DialToIdentity in the non-test Go
// files under root, skipping hidden directories (the module and build caches
// live there), vendor and testdata.
func callsOfDialToIdentity(t *testing.T, root string) []string {
	t.Helper()
	fset := token.NewFileSet()
	var calls []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if ok && calledName(call.Fun) == "DialToIdentity" {
				calls = append(calls, fset.Position(call.Pos()).String())
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return calls
}

// calledName is the name a call is made by: the selector's name, or the
// identifier itself.
func calledName(fun ast.Expr) string {
	switch callee := fun.(type) {
	case *ast.SelectorExpr:
		return callee.Sel.Name
	case *ast.Ident:
		return callee.Name
	}
	return ""
}
