package node

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// protected_binding_limit_test.go pins the bound of protected endpoint
// bindings (docs/protocol/session_v2.md, "Bounds of the endpoint bindings"):
// 32 768 in all, of which 8 192 are a reserve only the contact pool (K) may
// use — the relation pool (R) never takes it, not even for a while, so R
// holds at most 24 576 — and at most 4 per identity that gave the
// protection. The pool of a binding is fixed when the
// binding is created or raised. At a bound nothing existing is removed or
// lowered: the new binding is refused with its reason and may be recorded as
// an ordinary binding with its 30 days instead, and an operation that needs
// the protection fails.

// boundIdentity is the identity number i of a fill; distinct from the
// identities the tests name directly.
func boundIdentity(i int) domain.PeerIdentity {
	return domain.PeerIdentity{0xb0, byte(i), byte(i >> 8), byte(i >> 16)}
}

// boundEndpoint is the endpoint number i of a fill.
func boundEndpoint(i int) domain.PeerAddress {
	return domain.PeerAddress(fmt.Sprintf("10.%d.%d.%d:64646", byte(i>>16), byte(i>>8), byte(i)))
}

// fillProtected creates count protected bindings of pool, four per identity,
// numbering endpoints and identities from first.
func fillProtected(t *testing.T, store *secureSessionStore, pool domain.ProtectedBindingPool, first, count int) {
	t.Helper()
	for i := first; i < first+count; i++ {
		result, err := store.noteProvenOutbound(dialledAt(boundEndpoint(i)), boundIdentity(i/maxProtectedPerIdentity), endpointProtectionWanted(pool))
		if err != nil || result.tier != bindingTierProtected {
			t.Fatalf("fill %s #%d: tier %v, refused %v, err %v", pool, i, result.tier, result.refused, err)
		}
	}
}

// refusalReason is the reason of a refused protection, "" when none.
func refusalReason(result endpointBindingResult) domain.ProtectedBindingRefusalReason {
	if result.refused == nil {
		return ""
	}
	return result.refused.Reason
}

// bindingOf is the binding of address as the store holds it, under its
// canonical key; an address without one has none.
func bindingOf(store *secureSessionStore, address domain.PeerAddress) (endpointBinding, bool) {
	endpoint, err := canonicalEndpoint(address)
	if err != nil {
		return endpointBinding{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	binding, bound := store.endpoints[endpoint]
	return binding, bound
}

// insertSeededLocked puts binding under the canonical key of address, in the
// source group the store gives that address. Caller holds store.mu.
func insertSeededLocked(t *testing.T, store *secureSessionStore, address domain.PeerAddress, binding endpointBinding) {
	t.Helper()
	endpoint := mustCanonical(t, address)
	binding.group = endpointSourceGroup(endpoint, nil)
	store.insertBindingLocked(endpoint, binding)
}

// protectedRecords is every protected binding of the store with its owner,
// giver and pool — what must survive every path untouched.
func protectedRecords(store *secureSessionStore) map[domain.PeerAddress]bindingProtection {
	store.mu.Lock()
	defer store.mu.Unlock()
	records := make(map[domain.PeerAddress]bindingProtection)
	for endpoint, binding := range store.endpoints {
		if binding.protection != nil {
			records[endpoint.Address()] = *binding.protection
		}
	}
	return records
}

// assertTallies recounts the bindings and compares the result with the
// counters the store keeps, and checks the bounds hold.
func assertTallies(t *testing.T, store *secureSessionStore) {
	t.Helper()
	assertCounts(t, store, true)
}

// assertCounts recounts the bindings and compares the result with the
// counters the store keeps; withBounds also checks the bounds hold.
func assertCounts(t *testing.T, store *secureSessionStore, withBounds bool) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	pools := make(map[domain.ProtectedBindingPool]int)
	givers := make(map[domain.PeerIdentity]int)
	general := 0
	for _, binding := range store.endpoints {
		if binding.protection == nil {
			general++
			continue
		}
		pools[binding.protection.pool]++
		givers[binding.protection.by]++
	}
	if general != store.generalBindings {
		t.Fatalf("general bindings: counted %d, the store says %d", general, store.generalBindings)
	}
	for _, pool := range []domain.ProtectedBindingPool{domain.ProtectedBindingPoolContact, domain.ProtectedBindingPoolRelation} {
		if pools[pool] != store.protectedByPool[pool] {
			t.Fatalf("pool %s: counted %d, the store says %d", pool, pools[pool], store.protectedByPool[pool])
		}
	}
	for giver, count := range givers {
		if count != store.protectedByGiver[giver] {
			t.Fatalf("protected_by %s: counted %d, the store says %d", giver, count, store.protectedByGiver[giver])
		}
	}
	if len(givers) != len(store.protectedByGiver) {
		t.Fatalf("protected_by entries: counted %d, the store keeps %d", len(givers), len(store.protectedByGiver))
	}
	if !withBounds {
		return
	}
	if r := pools[domain.ProtectedBindingPoolRelation]; r > maxProtectedBindings-protectedReserveContact {
		t.Fatalf("the relation pool holds %d, more than %d", r, maxProtectedBindings-protectedReserveContact)
	}
	if total := pools[domain.ProtectedBindingPoolContact] + pools[domain.ProtectedBindingPoolRelation]; total > maxProtectedBindings {
		t.Fatalf("protected bindings %d, more than %d", total, maxProtectedBindings)
	}
	if general > maxGeneralBindings {
		t.Fatalf("general bindings %d, more than %d", general, maxGeneralBindings)
	}
}

// The bounds are the numbers the protocol documentation promises operators;
// a change to them is a documented change, not a tuning detail.
func TestTheProtectedBindingBoundsMatchTheDocumentedLimits(t *testing.T) {
	t.Parallel()
	if maxProtectedBindings != 32768 || protectedReserveContact != 8192 || maxProtectedPerIdentity != 4 {
		t.Fatalf("bounds = %d total, %d reserve for K, %d per identity; want 32768, 8192, 4",
			maxProtectedBindings, protectedReserveContact, maxProtectedPerIdentity)
	}
	if maxGeneralBindings != 16384 || maxPinnedIdentities != 20000 {
		t.Fatalf("general bindings %d, pinned identities %d; want 16384, 20000", maxGeneralBindings, maxPinnedIdentities)
	}
}

// With the relation pool full, a new relation is refused protection —
// pool_r_full, recorded as an ordinary binding, counted — while a new contact
// still gets protection out of the reserve.
func TestTheContactReserveHoldsWhenTheRelationPoolIsFull(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	fillProtected(t, store, domain.ProtectedBindingPoolRelation, 0, maxProtectedBindings-protectedReserveContact)

	relation, contact := domain.PeerIdentity{0xc1}, domain.PeerIdentity{0xc2}
	result, err := store.noteProvenOutbound(dialledAt("198.51.100.1:64646"), relation, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	if err != nil {
		t.Fatalf("a refused protection failed the session: %v", err)
	}
	if refusalReason(result) != domain.ProtectedBindingRefusedPoolRFull || result.tier != bindingTierGeneral {
		t.Fatalf("new relation: tier %v, refusal %q; want general, pool_r_full", result.tier, refusalReason(result))
	}
	if got := store.stats().ProtectedRefusalsPoolRFull; got != 1 {
		t.Fatalf("pool_r_full refusals counted = %d, want 1", got)
	}

	result, err = store.noteProvenOutbound(dialledAt("198.51.100.2:64646"), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if err != nil || result.tier != bindingTierProtected || result.refused != nil {
		t.Fatalf("new contact: tier %v, refusal %q, err %v; want protected from the reserve", result.tier, refusalReason(result), err)
	}
	stats := store.stats()
	if stats.ProtectedBindingsRelation != maxProtectedBindings-protectedReserveContact || stats.ProtectedBindingsContact != 1 {
		t.Fatalf("pools = K %d, R %d", stats.ProtectedBindingsContact, stats.ProtectedBindingsRelation)
	}
	assertTallies(t, store)
}

// At the total bound a new contact is refused with store_full and a new
// relation with pool_r_full; at four bindings of one identity the fifth is
// refused with per_identity. Nothing existing changes.
func TestTheTotalAndThePerIdentityBoundsRefuseNewProtection(t *testing.T) {
	t.Parallel()

	t.Run("total", func(t *testing.T) {
		t.Parallel()
		store := loadSecureSessionStore("", newStoreClock().trusted())
		fillProtected(t, store, domain.ProtectedBindingPoolRelation, 0, maxProtectedBindings/2)
		fillProtected(t, store, domain.ProtectedBindingPoolContact, maxProtectedBindings/2, maxProtectedBindings/2)
		before := protectedRecords(store)

		result, _ := store.noteProvenOutbound(dialledAt("198.51.100.1:64646"), domain.PeerIdentity{0xc1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
		if refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
			t.Fatalf("new contact at the total bound: refusal %q, want store_full", refusalReason(result))
		}
		result, _ = store.noteProvenOutbound(dialledAt("198.51.100.2:64646"), domain.PeerIdentity{0xc2}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
		if refusalReason(result) != domain.ProtectedBindingRefusedPoolRFull {
			t.Fatalf("new relation at the total bound: refusal %q, want pool_r_full", refusalReason(result))
		}
		if after := protectedRecords(store); !reflect.DeepEqual(before, after) {
			t.Fatal("a refusal at the total bound changed the existing protected bindings")
		}
		assertTallies(t, store)
	})

	t.Run("per identity", func(t *testing.T) {
		t.Parallel()
		store := loadSecureSessionStore("", newStoreClock().trusted())
		giver := domain.PeerIdentity{0xc3}
		for i := range maxProtectedPerIdentity {
			result, _ := store.noteProvenOutbound(dialledAt(boundEndpoint(i)), giver, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
			if result.tier != bindingTierProtected {
				t.Fatalf("endpoint %d of the identity: tier %v, want protected", i, result.tier)
			}
		}
		before := protectedRecords(store)
		result, _ := store.noteProvenOutbound(dialledAt(boundEndpoint(maxProtectedPerIdentity)), giver, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
		if refusalReason(result) != domain.ProtectedBindingRefusedPerIdentity || result.tier != bindingTierGeneral {
			t.Fatalf("fifth endpoint: tier %v, refusal %q; want general, per_identity", result.tier, refusalReason(result))
		}
		if after := protectedRecords(store); !reflect.DeepEqual(before, after) {
			t.Fatal("the fifth endpoint changed the four protected ones")
		}
		if got := store.stats().ProtectedRefusalsPerIdentity; got != 1 {
			t.Fatalf("per_identity refusals counted = %d, want 1", got)
		}
		assertTallies(t, store)
	})
}

// The pool is fixed when the binding is created: a contact that is
// removed, or becomes a relation, keeps its bindings in K; a relation that
// becomes a contact keeps its bindings in R. No order of events takes R past
// its pool.
func TestThePoolOfAProtectedBindingIsFixedAtCreation(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	contactEndpoint, relationEndpoint := domain.PeerAddress("198.51.100.1:64646"), domain.PeerAddress("198.51.100.2:64646")
	contact, relation := domain.PeerIdentity{0xc1}, domain.PeerIdentity{0xc2}
	if result, _ := store.noteProvenOutbound(dialledAt(contactEndpoint), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
		t.Fatal("precondition: the contact binding is not protected")
	}
	if result, _ := store.noteProvenOutbound(dialledAt(relationEndpoint), relation, endpointProtectionWanted(domain.ProtectedBindingPoolRelation)); result.tier != bindingTierProtected {
		t.Fatal("precondition: the relation binding is not protected")
	}

	for name, protection := range map[string]endpointProtection{
		"the contact removed":       endpointProtectionNone(),
		"the contact now in R":      endpointProtectionWanted(domain.ProtectedBindingPoolRelation),
		"the relation now in K":     endpointProtectionWanted(domain.ProtectedBindingPoolContact),
		"the relation no longer R":  endpointProtectionNone(),
		"the contact required as R": endpointProtectionRequired(domain.ProtectedBindingPoolRelation),
	} {
		_, _ = store.noteProvenOutbound(dialledAt(contactEndpoint), contact, protection)
		_, _ = store.noteProvenOutbound(dialledAt(relationEndpoint), relation, protection)
		got := protectedRecords(store)
		if got[contactEndpoint].pool != domain.ProtectedBindingPoolContact || got[relationEndpoint].pool != domain.ProtectedBindingPoolRelation {
			t.Fatalf("after %s: pools K→%q, R→%q; want them unchanged", name, got[contactEndpoint].pool, got[relationEndpoint].pool)
		}
		stats := store.stats()
		if stats.ProtectedBindingsContact != 1 || stats.ProtectedBindingsRelation != 1 {
			t.Fatalf("after %s: counted K %d, R %d; want 1, 1", name, stats.ProtectedBindingsContact, stats.ProtectedBindingsRelation)
		}
	}

	// R full, then every relation binding re-noted as a contact and a fresh
	// relation tried: R stays at its pool.
	fillProtected(t, store, domain.ProtectedBindingPoolRelation, 0, maxProtectedBindings-protectedReserveContact-1)
	for i := range maxProtectedBindings - protectedReserveContact - 1 {
		_, _ = store.noteProvenOutbound(dialledAt(boundEndpoint(i)), boundIdentity(i/maxProtectedPerIdentity), endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	}
	result, _ := store.noteProvenOutbound(dialledAt("198.51.100.3:64646"), domain.PeerIdentity{0xc4}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	if refusalReason(result) != domain.ProtectedBindingRefusedPoolRFull {
		t.Fatalf("a relation past the pool: refusal %q, want pool_r_full", refusalReason(result))
	}
	if got := store.stats().ProtectedBindingsRelation; got != maxProtectedBindings-protectedReserveContact {
		t.Fatalf("relation pool = %d, want %d", got, maxProtectedBindings-protectedReserveContact)
	}
	assertTallies(t, store)
}

// A new owner of an endpoint never raises its tier: only an identity that
// already was the endpoint's last identity raises it, so whoever takes an
// address over cannot turn an ordinary binding into one that never expires.
func TestAChangeOfOwnerNeverRaisesTheTier(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	endpoint := domain.PeerAddress("198.51.100.1:64646")
	stranger, contact := domain.PeerIdentity{0xc1}, domain.PeerIdentity{0xc2}
	if result, _ := store.noteProvenOutbound(dialledAt(endpoint), stranger, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("precondition: tier %v, want general", result.tier)
	}
	if result, _ := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierGeneral {
		t.Fatalf("a new owner raised the tier: %v", result.tier)
	}
	if binding, _ := bindingOf(store, endpoint); binding.identity != contact {
		t.Fatalf("the owner was not updated: %s", binding.identity)
	}
	if result, _ := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
		t.Fatalf("the same owner proven again did not raise the tier: %v", result.tier)
	}
	assertTallies(t, store)
}

// Concurrent creations at one free place — in R, in the total, at one
// identity — produce exactly one protected binding; every other one is
// refused with the right reason. Run under -race.
func TestConcurrentProtectedBindingsTakeTheLastPlaceOnce(t *testing.T) {
	t.Parallel()
	const contenders = 64

	cases := map[string]struct {
		prepare func(t *testing.T, store *secureSessionStore)
		// contender is what goroutine i asks for.
		contender func(i int) (domain.PeerAddress, domain.PeerIdentity, endpointProtection)
		reason    domain.ProtectedBindingRefusalReason
	}{
		"the relation pool": {
			prepare: func(t *testing.T, store *secureSessionStore) {
				fillProtected(t, store, domain.ProtectedBindingPoolRelation, 0, maxProtectedBindings-protectedReserveContact-1)
			},
			contender: func(i int) (domain.PeerAddress, domain.PeerIdentity, endpointProtection) {
				return domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i)), domain.PeerIdentity{0xd0, byte(i)}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation)
			},
			reason: domain.ProtectedBindingRefusedPoolRFull,
		},
		"the total": {
			prepare: func(t *testing.T, store *secureSessionStore) {
				fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, maxProtectedBindings-1)
			},
			contender: func(i int) (domain.PeerAddress, domain.PeerIdentity, endpointProtection) {
				return domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i)), domain.PeerIdentity{0xd0, byte(i)}, endpointProtectionWanted(domain.ProtectedBindingPoolContact)
			},
			reason: domain.ProtectedBindingRefusedStoreFull,
		},
		"one identity": {
			prepare: func(t *testing.T, store *secureSessionStore) {
				for i := range maxProtectedPerIdentity - 1 {
					_, _ = store.noteProvenOutbound(dialledAt(boundEndpoint(i)), domain.PeerIdentity{0xd1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
				}
			},
			contender: func(i int) (domain.PeerAddress, domain.PeerIdentity, endpointProtection) {
				return domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i)), domain.PeerIdentity{0xd1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact)
			},
			reason: domain.ProtectedBindingRefusedPerIdentity,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := loadSecureSessionStore("", newStoreClock().trusted())
			tc.prepare(t, store)

			var protected, refusedRightly, other atomic.Int32
			var start, done sync.WaitGroup
			start.Add(1)
			for i := range contenders {
				done.Add(1)
				go func() {
					defer done.Done()
					start.Wait()
					address, id, protection := tc.contender(i)
					result, err := store.noteProvenOutbound(dialledAt(address), id, protection)
					switch {
					case err == nil && result.tier == bindingTierProtected:
						protected.Add(1)
					case err == nil && refusalReason(result) == tc.reason && result.tier != bindingTierProtected:
						refusedRightly.Add(1)
					default:
						other.Add(1)
					}
				}()
			}
			start.Done()
			done.Wait()
			if protected.Load() != 1 || refusedRightly.Load() != contenders-1 || other.Load() != 0 {
				t.Fatalf("protected %d, refused with %s %d, other %d; want 1, %d, 0",
					protected.Load(), tc.reason, refusedRightly.Load(), other.Load(), contenders-1)
			}
			assertTallies(t, store)
		})
	}
}

// diskBacked points a store that was filled in memory at path and writes it
// there, so a large fill does not rewrite the whole file once per binding.
func diskBacked(t *testing.T, store *secureSessionStore, path string) {
	t.Helper()
	store.mu.Lock()
	store.dir, store.name = filepath.Dir(path), filepath.Base(path)
	store.writeFile = store.writeToDisk
	store.changes++
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
}

// A store full to the bound comes back from a restart with the same
// bindings, tiers, givers and pools — the pool read from the file, not
// derived again — and keeps refusing at the bound.
func TestAProtectedStoreAtItsBoundSurvivesARestart(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore("", clock.trusted())
	fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, protectedReserveContact)
	fillProtected(t, store, domain.ProtectedBindingPoolRelation, protectedReserveContact, maxProtectedBindings-protectedReserveContact)
	diskBacked(t, store, path)
	before := protectedRecords(store)

	restarted := loadSecureSessionStore(path, clock.trusted())
	if restarted.unreadable {
		t.Fatal("the full store could not be read back")
	}
	if after := protectedRecords(restarted); !reflect.DeepEqual(before, after) {
		t.Fatalf("the restart changed the protected bindings: %d before, %d after", len(before), len(after))
	}
	assertTallies(t, restarted)
	result, _ := restarted.noteProvenOutbound(dialledAt("198.51.100.1:64646"), domain.PeerIdentity{0xc1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("after the restart a new contact: refusal %q, want store_full", refusalReason(result))
	}
	result, _ = restarted.noteProvenOutbound(dialledAt("198.51.100.2:64646"), domain.PeerIdentity{0xc2}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	if refusalReason(result) != domain.ProtectedBindingRefusedPoolRFull {
		t.Fatalf("after the restart a new relation: refusal %q, want pool_r_full", refusalReason(result))
	}
}

// A protected binding written before pools existed — "protected": true and
// nothing else — was a contact's: it loads into K, given by its identity.
func TestAProtectedBindingWithoutAPoolLoadsAsAContacts(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	id := domain.PeerIdentity{7}
	raw := fmt.Sprintf(`{"format":1,"identities":{"%s":"2026-05-28T21:46:40Z"},"endpoints":{"192.0.2.1:64646":{"identity":"%s","last_v2":"2026-05-28T21:46:40Z","protected":true}}}`, id, id)
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	store := loadSecureSessionStore(path, newStoreClock().trusted())
	if store.unreadable {
		t.Fatal("a protected binding without a pool made the store unreadable")
	}
	binding, bound := bindingOf(store, "192.0.2.1:64646")
	if !bound || binding.protection == nil || binding.protection.pool != domain.ProtectedBindingPoolContact || binding.protection.by != id {
		t.Fatalf("binding = %+v, want protected in K, given by %s", binding, id)
	}
	assertTallies(t, store)
}

// A record that contradicts itself is a damaged file: the store fails closed.
func TestAProtectedBindingRecordThatContradictsItselfFailsClosed(t *testing.T) {
	t.Parallel()
	id := domain.PeerIdentity{7}
	for name, record := range map[string]string{
		"a pool on an ordinary binding": fmt.Sprintf(`{"identity":"%s","last_v2":"2026-05-28T21:46:40Z","pool":"K"}`, id),
		"an unknown pool":               fmt.Sprintf(`{"identity":"%s","last_v2":"2026-05-28T21:46:40Z","protected":true,"protected_by":"%s","pool":"X"}`, id, id),
		"a zero giver":                  fmt.Sprintf(`{"identity":"%s","last_v2":"2026-05-28T21:46:40Z","protected":true,"protected_by":"","pool":"K"}`, id),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
			raw := `{"format":1,"identities":{},"endpoints":{"192.0.2.1:64646":` + record + `}}`
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			if store := loadSecureSessionStore(path, newStoreClock().trusted()); !store.unreadable {
				t.Fatal("a damaged binding record was accepted")
			}
		})
	}
}

// No path lowers or removes a protected binding — the bound reached,
// the contact removed, the giver's pool changed, a fifth endpoint, a new
// owner, a full pin store, 60 days — and the restart that follows reads every
// one of them back.
func TestNoPathLowersOrRemovesAProtectedBinding(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore("", clock.trusted())
	contact := domain.PeerIdentity{0xc1}
	var endpoints []domain.PeerAddress
	for i := range maxProtectedPerIdentity {
		endpoint := domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i))
		endpoints = append(endpoints, endpoint)
		if result, _ := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
			t.Fatalf("precondition: endpoint %d not protected", i)
		}
	}
	fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, maxProtectedBindings-maxProtectedPerIdentity)
	diskBacked(t, store, path)
	before := protectedRecords(store)

	// The bound reached: more contacts and relations.
	_, _ = store.noteProvenOutbound(dialledAt("203.0.113.1:64646"), domain.PeerIdentity{0xc2}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	_, _ = store.noteProvenOutbound(dialledAt("203.0.113.2:64646"), domain.PeerIdentity{0xc3}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	// The contact removed, then moved to R.
	_, _ = store.noteProvenOutbound(dialledAt(endpoints[0]), contact, endpointProtectionNone())
	_, _ = store.noteProvenOutbound(dialledAt(endpoints[1]), contact, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	// A fifth endpoint of the contact.
	_, _ = store.noteProvenOutbound(dialledAt("203.0.113.3:64646"), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	// A new owner of a protected endpoint.
	newOwner := domain.PeerIdentity{0xc4}
	_, _ = store.noteProvenOutbound(dialledAt(endpoints[2]), newOwner, endpointProtectionNone())
	// A full pin store, and a new identity on a protected endpoint.
	fillPinStore(t, store)
	if _, err := store.noteProvenOutbound(dialledAt(endpoints[3]), domain.PeerIdentity{0xc5}, endpointProtectionNone()); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("precondition: a new identity at the pin bound: %v", err)
	}
	// 60 days.
	clock.now = clock.now.Add(2 * endpointBindingTTL)
	_, _ = store.noteProvenOutbound(dialledAt("203.0.113.4:64646"), domain.PeerIdentity{0xc6}, endpointProtectionNone())

	want := before
	owned := want[endpoints[2]]
	if after := protectedRecords(store); !reflect.DeepEqual(want, after) {
		t.Fatalf("a path changed the protected bindings: %d before, %d after", len(want), len(after))
	}
	if binding, _ := bindingOf(store, endpoints[2]); binding.identity != newOwner || binding.protection == nil || *binding.protection != owned {
		t.Fatalf("the new owner's endpoint = %+v; want owner %s, protection %+v unchanged", binding, newOwner, owned)
	}
	for _, endpoint := range endpoints {
		if !store.endpointRequiresV2(endpoint) {
			t.Fatalf("%s no longer requires v2 after 60 days", endpoint)
		}
	}
	if err := store.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	written, err := os.ReadFile(path) //nolint:gosec // the file this test wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	if after := protectedRecords(restarted); !reflect.DeepEqual(want, after) {
		t.Fatalf("the restart changed the protected bindings: %d before, %d after", len(want), len(after))
	}
	assertTallies(t, restarted)
	// The restarted store writes back exactly the file it read.
	restarted.mu.Lock()
	restarted.changes++
	restarted.mu.Unlock()
	if err := restarted.persist(); err != nil {
		t.Fatalf("persist after the restart: %v", err)
	}
	rewritten, err := os.ReadFile(path) //nolint:gosec // the file this test wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(written, rewritten) {
		t.Fatalf("the store read back and written again differs from the file: %d bytes before, %d after", len(written), len(rewritten))
	}
}

// The three outcomes — protected; protection refused and an ordinary
// binding recorded; no binding — are told apart in the result and the
// counters, a refusal never reads as protected, and an operation that needs
// the protection fails with the typed refusal.
func TestTheOutcomesOfABindingAreReportedApart(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())

	result, err := store.noteProvenOutbound(dialledAt("198.51.100.1:64646"), domain.PeerIdentity{0xc1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if err != nil || result.tier != bindingTierProtected || result.refused != nil {
		t.Fatalf("protection with room: tier %v, refusal %q, err %v", result.tier, refusalReason(result), err)
	}

	fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, maxProtectedBindings-1)
	result, err = store.noteProvenOutbound(dialledAt("198.51.100.2:64646"), domain.PeerIdentity{0xc2}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if err != nil || result.tier != bindingTierGeneral || refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("protection refused, general recorded: tier %v, refusal %q, err %v", result.tier, refusalReason(result), err)
	}

	required := domain.PeerAddress("198.51.100.3:64646")
	result, err = store.noteProvenOutbound(dialledAt(required), domain.PeerIdentity{0xc3}, endpointProtectionRequired(domain.ProtectedBindingPoolContact))
	var refused *domain.ProtectedBindingRefused
	if !errors.As(err, &refused) || refused.Reason != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("an operation that needs the protection: err %v, want the typed refusal store_full", err)
	}
	if result.tier == bindingTierProtected {
		t.Fatal("a refused protection was reported as protected")
	}

	// The ordinary tier full too: no binding at all.
	store.mu.Lock()
	for i := 0; store.generalBindings < maxGeneralBindings; i++ {
		insertSeededLocked(t, store, domain.PeerAddress(fmt.Sprintf("203.0.%d.%d:64646", byte(i>>8), byte(i))), endpointBinding{identity: domain.PeerIdentity{0xc4}, lastV2: store.now()})
	}
	store.mu.Unlock()
	nowhere := domain.PeerAddress("198.51.100.4:64646")
	result, err = store.noteProvenOutbound(dialledAt(nowhere), domain.PeerIdentity{0xc5}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if err != nil || result.tier != bindingTierNone || refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("no binding: tier %v, refusal %q, err %v", result.tier, refusalReason(result), err)
	}
	if _, bound := bindingOf(store, nowhere); bound {
		t.Fatal("a binding was recorded past both bounds")
	}

	stats := store.stats()
	if stats.ProtectedRefusalsStoreFull != 3 || stats.ProtectedRefusalsPoolRFull != 0 || stats.ProtectedRefusalsPerIdentity != 0 {
		t.Fatalf("refusals counted: store_full %d, pool_r_full %d, per_identity %d; want 3, 0, 0",
			stats.ProtectedRefusalsStoreFull, stats.ProtectedRefusalsPoolRFull, stats.ProtectedRefusalsPerIdentity)
	}
	if stats.ProtectedCapacity != maxProtectedBindings || stats.ProtectedReserveContact != protectedReserveContact ||
		stats.ProtectedPerIdentityCapacity != maxProtectedPerIdentity || stats.GeneralCapacity != maxGeneralBindings {
		t.Fatalf("capacities reported: %+v", stats)
	}
	if stats.ProtectedBindingsContact != maxProtectedBindings || stats.GeneralBindings != maxGeneralBindings {
		t.Fatalf("bindings reported: K %d, general %d", stats.ProtectedBindingsContact, stats.GeneralBindings)
	}
	assertTallies(t, store)
}

// worstHost is a 253-character DNS name, distinct for every i: the longest
// host a binding can hold — the worst case of "Size at the bounds" in
// docs/protocol/session_v2.md.
func worstHost(i int) string {
	name := fmt.Sprintf("n%06d", i)
	for len(name) < 253 {
		name += "." + strings.Repeat("a", min(63, 253-len(name)-1))
	}
	return name
}

// storeFileSize is the exact size of the store file for the given content,
// all times whole seconds in UTC and every endpoint host:65535 with a host of
// hostLen bytes. It follows the JSON shape field by field.
func storeFileSize(identities, protected, general, hostLen int) int {
	const (
		idLen   = 40 // hex
		timeLen = len("2026-05-28T21:46:40Z")
	)
	identity := len(`"`) + idLen + len(`":"`) + timeLen + len(`"`)
	key := len(`"`) + hostLen + len(`:65535`) + len(`":`)
	// The longest group is the shared one of names; a general record
	// carries derived_from — the longest name key — as a derived key would.
	keyFields := len(`,"transport":"tcp","source_group":"`) + len(domain.SourceGroupUnresolvedName) +
		len(`","first_at":"`) + timeLen + len(`"`)
	general1 := key + len(`{"identity":"`) + idLen + len(`","last_v2":"`) + timeLen + len(`"`) +
		keyFields + len(`,"derived_from":"`) + hostLen + len(`:65535"}`)
	protected1 := key + len(`{"identity":"`) + idLen + len(`","last_v2":"`) + timeLen +
		len(`","protected":true,"protected_by":"`) + idLen + len(`","pool":"K"`) + keyFields + len(`}`)
	endpoints := protected + general
	metadata := 0
	if identities > 0 {
		metadata = len(`,"identity_metadata":{`) + identities*(len(`"`)+idLen+len(`":{"version":1,"classification":"unclassified"}`)) + max(identities-1, 0) + len(`}`)
	}
	return metadata + len(`{"format":2,"identities":{`) +
		identities*identity + max(identities-1, 0) +
		len(`},"endpoints":{`) +
		protected*protected1 + general*general1 + max(endpoints-1, 0) +
		len(`}}`)
}

// heapInUse is the live heap after a collection.
func heapInUse() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

const mib = 1 << 20

// liveStoreBudget is the memory bound for the live structure in the worst
// case — every host a 253-character name, every ordinary binding a derived
// key naming another 253-character name, every binding with its creation
// time and a group string of its own, the daily quota full. Measured ≈ 32.8
// MiB: above the ≈ 30 MiB the design of endpoint keys and groups estimated,
// a deviation recorded here and in docs/protocol/session_v2.md, and the
// bound is the measurement with ≈ 10 % to spare.
const liveStoreBudget = 36 * mib

// savingStoreBudget is the heap during a save in the worst case, derived from
// this implementation: the live structure, a snapshot and the encoded file
// twice, because encoding/json builds the document in its own scratch buffer
// and returns a copy of it; the scratch buffer stays pooled until the next
// collections. A format written from one buffer would hold the file once; the
// store is JSON, and a save measured ≈ 92 MiB. The budget below (live budget
// + 8 MiB + 2 × file ≈ 101 MiB) only keeps that measurement from growing;
// changing the file format is out of this test's scope.
func savingStoreBudget(file int64) uint64 {
	return liveStoreBudget + 8*mib + 2*uint64(file)
}

// The store filled to every bound with the longest records. The file
// is exactly the size its shape gives; the live structure, and the structure
// plus the encoded file at the moment of a save, stay within the memory
// budget; a save never holds more than the old file and one temp copy on
// disk; temps older than an hour are swept at start, younger ones are left.
//
// Not parallel: it measures the heap.
func TestTheStoreAtItsBoundsStaysWithinItsSizeBudget(t *testing.T) {
	clock := &storeClock{now: time.Unix(1780000000, 0).UTC()}
	dir := t.TempDir()
	path := filepath.Join(dir, "secure-sessions-64646.json")

	base := heapInUse()
	store := loadSecureSessionStore("", clock.trusted())
	store.mu.Lock()
	now := store.now()
	for i := range maxPinnedIdentities {
		store.identities[boundIdentity(i)] = identityPinEntry{pinnedAt: now}
	}
	for i := range maxProtectedBindings {
		giver := boundIdentity(i / maxProtectedPerIdentity)
		insertSeededLocked(t, store, domain.PeerAddress(worstHost(i)+":65535"), endpointBinding{
			identity:   giver,
			lastV2:     now,
			protection: &bindingProtection{by: giver, pool: domain.ProtectedBindingPoolContact},
			firstAt:    &now,
		})
	}
	for i := range maxGeneralBindings {
		name := mustCanonical(t, domain.PeerAddress(worstHost(maxProtectedBindings+maxGeneralBindings+i)+":65535"))
		insertSeededLocked(t, store, domain.PeerAddress(worstHost(maxProtectedBindings+i)+":65535"), endpointBinding{
			identity:    boundIdentity(i),
			lastV2:      now,
			firstAt:     &now,
			derivedFrom: &name,
		})
	}
	// As on the real path, every binding holds a group string of its own
	// (sourceGroup formats one per creation, and a load decodes one per
	// record), of the longest length a group has; and the daily quota holds
	// as many creations as it ever may for the ordinary tier — 64 in each of
	// the groups that 16 384 creations fill.
	for endpoint, binding := range store.endpoints {
		binding.group = domain.SourceGroup(strings.Clone(string(domain.SourceGroupUnresolvedName)))
		store.endpoints[endpoint] = binding
	}
	for i := range maxGeneralBindings {
		group := domain.SourceGroup(fmt.Sprintf("%04x:%04x::/32x", i/generalCreationsPerGroupPerDay, 0))
		store.recordGeneralCreationLocked(group, now)
	}
	store.mu.Unlock()
	live := heapInUse() - base
	assertTallies(t, store)

	// The heap at the moment of the save: the live store and the encoded
	// file handed to the writer.
	var atSave uint64
	var inFlight, maxInFlight atomic.Int32
	store.mu.Lock()
	store.dir, store.name = dir, filepath.Base(path)
	store.writeFile = func(raw []byte) error {
		if n := inFlight.Add(1); n > maxInFlight.Load() {
			maxInFlight.Store(n)
		}
		defer inFlight.Add(-1)
		if atSave == 0 {
			atSave = heapInUse() - base
		}
		err := store.writeToDisk(raw)
		runtime.KeepAlive(raw)
		return err
	}
	store.changes++
	store.mu.Unlock()
	var saves sync.WaitGroup
	for range 4 {
		saves.Add(1)
		go func() {
			defer saves.Done()
			if err := store.persist(); err != nil {
				t.Errorf("persist: %v", err)
			}
		}()
	}
	saves.Wait()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if want := storeFileSize(maxPinnedIdentities, maxProtectedBindings, maxGeneralBindings, 253); info.Size() != int64(want) {
		t.Fatalf("file = %d bytes, the shape gives %d", info.Size(), want)
	}
	t.Logf("at the bounds, 253-byte hosts: file %.1f MiB, live structure %.1f MiB, at save %.1f MiB",
		float64(info.Size())/mib, float64(live)/mib, float64(atSave)/mib)
	if live > liveStoreBudget {
		t.Fatalf("live structure %.1f MiB, budget %.1f MiB", float64(live)/mib, float64(liveStoreBudget)/mib)
	}
	if budget := savingStoreBudget(info.Size()); atSave > budget {
		t.Fatalf("heap at save %.1f MiB, budget %.1f MiB", float64(atSave)/mib, float64(budget)/mib)
	}
	if maxInFlight.Load() != 1 {
		t.Fatalf("%d saves wrote at once: the disk would hold more than the old file and one temp", maxInFlight.Load())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("the directory holds %d entries after the saves, want only the store file", len(entries))
	}

	// A crash between the temp and the rename leaves temps behind: an old
	// one is swept at start, a young one may be a neighbour's write in
	// progress and is left.
	stale, young := filepath.Join(dir, secretfile.TempPrefix+"stale"), filepath.Join(dir, secretfile.TempPrefix+"young")
	for _, name := range []string{stale, young} {
		if err := os.WriteFile(name, []byte("partial"), 0o600); err != nil {
			t.Fatalf("write temp: %v", err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("age temp: %v", err)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	if restarted.unreadable {
		t.Fatal("the full store could not be read back")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("the temp older than an hour was not swept at start: %v", err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Fatalf("a young temp was swept: %v", err)
	}
	runtime.KeepAlive(store)
}

// A file holding more protected bindings than the bound — written by a build
// with another bound, or by hand — is loaded whole: nothing is dropped or
// lowered, and every new protection is refused until the count falls under
// the bound, which only an operator can make it do.
func TestAStoreLoadedPastTheProtectedBoundKeepsEveryBindingAndRefusesNewOnes(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	const past = maxProtectedBindings + 100
	store := loadSecureSessionStore("", clock.trusted())
	store.mu.Lock()
	now := store.now()
	for i := range past {
		giver := boundIdentity(i)
		insertSeededLocked(t, store, boundEndpoint(i), endpointBinding{
			identity:   giver,
			lastV2:     now,
			protection: &bindingProtection{by: giver, pool: domain.ProtectedBindingPoolContact},
		})
	}
	store.mu.Unlock()
	diskBacked(t, store, path)
	before := protectedRecords(store)

	restarted := loadSecureSessionStore(path, clock.trusted())
	if restarted.unreadable {
		t.Fatal("a store past the protected bound was refused as unreadable")
	}
	if after := protectedRecords(restarted); !reflect.DeepEqual(before, after) {
		t.Fatalf("loading changed the protected bindings: %d before, %d after", len(before), len(after))
	}
	assertCounts(t, restarted, false)
	if got := restarted.stats().ProtectedBindingsContact; got != past {
		t.Fatalf("contact pool = %d, want %d", got, past)
	}
	result, _ := restarted.noteProvenOutbound(dialledAt("198.51.100.1:64646"), domain.PeerIdentity{0xc1}, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("a new contact: refusal %q, want store_full", refusalReason(result))
	}
	result, _ = restarted.noteProvenOutbound(dialledAt("198.51.100.2:64646"), domain.PeerIdentity{0xc2}, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	if refusalReason(result) != domain.ProtectedBindingRefusedPoolRFull {
		t.Fatalf("a new relation: refusal %q, want pool_r_full", refusalReason(result))
	}
}

// An identity at its own bound in a full relation pool is refused for its own
// bound: per_identity is checked first, and only it is counted.
func TestTheIdentityBoundIsReportedBeforeThePoolBound(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	fillProtected(t, store, domain.ProtectedBindingPoolRelation, 0, maxProtectedBindings-protectedReserveContact)
	giver := domain.PeerIdentity{0xc1}
	for i := range maxProtectedPerIdentity {
		endpoint := domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i))
		if result, _ := store.noteProvenOutbound(dialledAt(endpoint), giver, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
			t.Fatalf("precondition: endpoint %d of the identity not protected", i)
		}
	}
	result, _ := store.noteProvenOutbound(dialledAt("203.0.113.1:64646"), giver, endpointProtectionWanted(domain.ProtectedBindingPoolRelation))
	if refusalReason(result) != domain.ProtectedBindingRefusedPerIdentity {
		t.Fatalf("refusal %q, want per_identity", refusalReason(result))
	}
	stats := store.stats()
	if stats.ProtectedRefusalsPerIdentity != 1 || stats.ProtectedRefusalsPoolRFull != 0 {
		t.Fatalf("counted per_identity %d, pool_r_full %d; want 1, 0", stats.ProtectedRefusalsPerIdentity, stats.ProtectedRefusalsPoolRFull)
	}
}

// A raise refused at the bound is refused at every renewal — the binding
// stays ordinary with store_full each time — but counted and logged once per
// binding, with its reason.
func TestARaiseRefusedAtTheBoundIsCountedAndLoggedOnce(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	var journal bytes.Buffer
	store.logger = zerolog.New(&journal)
	endpoint, contact := domain.PeerAddress("198.51.100.1:64646"), domain.PeerIdentity{0xc1}
	if result, _ := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatal("precondition: the binding is not ordinary")
	}
	fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, maxProtectedBindings)

	for renewal := range 3 {
		result, err := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
		if err != nil || result.tier != bindingTierGeneral || refusalReason(result) != domain.ProtectedBindingRefusedStoreFull {
			t.Fatalf("renewal %d: tier %v, refusal %q, err %v; want general, store_full", renewal, result.tier, refusalReason(result), err)
		}
	}
	if got := store.stats().ProtectedRefusalsStoreFull; got != 1 {
		t.Fatalf("store_full refusals counted = %d over three renewals, want 1", got)
	}
	lines := strings.Count(journal.String(), `"message":"protected_binding_limit_reached"`)
	if lines != 1 || !strings.Contains(journal.String(), `"reason":"store_full"`) || !strings.Contains(journal.String(), `"recorded":"general"`) {
		t.Fatalf("journal has %d protected_binding_limit_reached lines, want 1 with reason store_full, recorded general:\n%s", lines, journal.String())
	}
	assertTallies(t, store)
}

// A new owner never raises the tier, and an operation that requires the
// protection on such an endpoint is refused with owner_changed — not passed
// as done. The binding still records the new owner's v2 session as ordinary;
// owner_changed is not a bound and is not counted as one.
func TestARequiredProtectionOnAChangedOwnerIsRefused(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	endpoint := domain.PeerAddress("198.51.100.1:64646")
	stranger, contact := domain.PeerIdentity{0xc1}, domain.PeerIdentity{0xc2}
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), stranger, endpointProtectionNone()); err != nil {
		t.Fatalf("precondition: %v", err)
	}
	result, err := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionRequired(domain.ProtectedBindingPoolContact))
	var refused *domain.ProtectedBindingRefused
	if !errors.As(err, &refused) || refused.Reason != domain.ProtectedBindingRefusedOwnerChanged {
		t.Fatalf("err = %v, want the typed refusal owner_changed", err)
	}
	if result.tier != bindingTierGeneral {
		t.Fatalf("tier %v, want general", result.tier)
	}
	if binding, _ := bindingOf(store, endpoint); binding.identity != contact || binding.protection != nil {
		t.Fatalf("binding = %+v, want ordinary, owned by the new owner", binding)
	}
	stats := store.stats()
	if stats.ProtectedRefusalsStoreFull+stats.ProtectedRefusalsPoolRFull+stats.ProtectedRefusalsPerIdentity != 0 {
		t.Fatal("owner_changed was counted as a bound")
	}
	// The same owner proven again, still required: now raised.
	if result, err := store.noteProvenOutbound(dialledAt(endpoint), contact, endpointProtectionRequired(domain.ProtectedBindingPoolContact)); err != nil || result.tier != bindingTierProtected {
		t.Fatalf("the owner proven again: tier %v, err %v; want protected", result.tier, err)
	}
}

// A required protection refused at a bound leaves the ordinary binding the v2
// session earned: the endpoint is protected for its 30 days rather than not
// at all; it is the operation that fails.
func TestARequiredProtectionRefusedAtABoundKeepsTheOrdinaryBinding(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	fillProtected(t, store, domain.ProtectedBindingPoolContact, 0, maxProtectedBindings)
	endpoint := domain.PeerAddress("198.51.100.1:64646")
	_, err := store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{0xc1}, endpointProtectionRequired(domain.ProtectedBindingPoolContact))
	var refused *domain.ProtectedBindingRefused
	if !errors.As(err, &refused) || refused.Reason != domain.ProtectedBindingRefusedStoreFull {
		t.Fatalf("err = %v, want the typed refusal store_full", err)
	}
	binding, bound := bindingOf(store, endpoint)
	if !bound || binding.protection != nil || !store.endpointRequiresV2(endpoint) {
		t.Fatalf("binding = %+v (bound %v), want an ordinary binding in force", binding, bound)
	}
}

// Renewals are written in batches, at most once a minute for the whole store:
// a renewal only marks the store as holding unwritten renewals, and the
// periodic flush writes them once a minute has passed since the last flush —
// a crash loses at most that minute (plus one flush tick). Creating a binding
// is written at once.
func TestRenewalsAreWrittenInBatchesAtMostOnceAMinute(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	writes := 0
	store.writeFile = func(raw []byte) error {
		writes++
		return store.writeToDisk(raw)
	}
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if writes != 1 {
		t.Fatalf("creating a binding wrote %d times, want at once", writes)
	}
	onDisk := func() time.Time {
		binding, _ := bindingOf(loadSecureSessionStore(path, clock.trusted()), endpoint)
		return binding.lastV2
	}

	clock.now = clock.now.Add(90 * time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	renewed := clock.now.UTC()
	if writes != 1 {
		t.Fatalf("a renewal wrote the store itself (%d writes): renewals are batched", writes)
	}
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if writes != 2 || !onDisk().Equal(renewed) {
		t.Fatalf("the first flush after a minute did not write the renewal: %d writes, on disk %v", writes, onDisk())
	}

	clock.now = clock.now.Add(10 * time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if writes != 2 {
		t.Fatalf("a flush 10 s after the last one wrote (%d writes): at most one batch a minute", writes)
	}
	clock.now = clock.now.Add(50 * time.Second)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if writes != 3 || !onDisk().Equal(clock.now.Add(-50*time.Second).UTC()) {
		t.Fatalf("the flush a minute after the last one did not write: %d writes, on disk %v", writes, onDisk())
	}
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if writes != 3 {
		t.Fatalf("a flush with nothing unwritten wrote (%d writes)", writes)
	}

	// The last flush, when the node stops, writes whatever is unwritten.
	clock.now = clock.now.Add(5 * time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	if err := store.flushRenewalsNow(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if writes != 4 || !onDisk().Equal(clock.now.UTC()) {
		t.Fatalf("the final flush did not write the last renewal: %d writes, on disk %v", writes, onDisk())
	}
}

// Renewing a protected binding changes nothing that matters — it never
// expires — so it is not written, not even by the next flush.
func TestRenewingAProtectedBindingWritesNothing(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore(filepath.Join(t.TempDir(), "secure-sessions-64646.json"), clock.trusted())
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}
	if result, _ := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
		t.Fatal("precondition: the binding is not protected")
	}
	writes := 0
	store.writeFile = func([]byte) error { writes++; return nil }
	clock.now = clock.now.Add(2 * time.Hour)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	clock.now = clock.now.Add(2 * time.Minute)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if writes != 0 {
		t.Fatalf("renewing a protected binding wrote the store %d times", writes)
	}
}

// Concurrent saves of one state write it once: a save that finds the state
// it was called for already written returns without writing again.
func TestConcurrentSavesOfOneStateWriteOnce(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore(filepath.Join(t.TempDir(), "secure-sessions-64646.json"), newStoreClock().trusted())
	var writes atomic.Int32
	store.writeFile = func([]byte) error {
		writes.Add(1)
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	store.mu.Lock()
	store.identities[domain.PeerIdentity{7}] = identityPinEntry{pinnedAt: store.now()}
	store.changes++
	store.mu.Unlock()

	var saves sync.WaitGroup
	for range 16 {
		saves.Add(1)
		go func() {
			defer saves.Done()
			if err := store.persist(); err != nil {
				t.Errorf("persist: %v", err)
			}
		}()
	}
	saves.Wait()
	if got := writes.Load(); got != 1 {
		t.Fatalf("16 concurrent saves of one state wrote %d times, want 1", got)
	}
}

// sharedStoreClock is a test clock a running node's goroutines read while the
// test moves it.
type sharedStoreClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSharedStoreClock() *sharedStoreClock {
	return &sharedStoreClock{now: newStoreClock().now}
}

func (c *sharedStoreClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sharedStoreClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// sinceBoot moves with now: the test only lets time go by and never sets the
// wall clock.
func (c *sharedStoreClock) sinceBoot() (time.Duration, error) {
	return c.read().Sub(storeClockBoot), nil
}

// trusted is the trusted time of a process started now on these clocks.
func (c *sharedStoreClock) trusted() *trustedclock.Clock {
	return trustedclock.New(trustedclock.Sources{Wall: c.read, SinceBoot: c.sinceBoot})
}

// bootstrapFlushNode is a running node whose bootstrap ticks the test sends
// and whose secure-session store lives at path on the test's clock.
func bootstrapFlushNode(t *testing.T, clock *sharedStoreClock, path string) (*Service, chan<- time.Time, func()) {
	t.Helper()
	ticks := make(chan time.Time)
	svc, stop := startQuietNode(t, func(svc *Service) {
		svc.bootstrapTickOverride = ticks
		svc.secureSessions.store = loadSecureSessionStore(path, clock.trusted())
	})
	return svc, ticks, stop
}

// tick runs one bootstrap tick to its end: the loop is one goroutine, so the
// second send is accepted only once the first tick has finished.
func tick(ticks chan<- time.Time) {
	ticks <- time.Time{}
	ticks <- time.Time{}
}

func lastV2OnDisk(t *testing.T, path string, clock *sharedStoreClock, endpoint domain.PeerAddress) time.Time {
	t.Helper()
	binding, _ := bindingOf(loadSecureSessionStore(path, clock.trusted()), endpoint)
	return binding.lastV2
}

// The running node writes its batched renewals from the bootstrap tick: a
// renewal reaches the disk on the first tick a minute after the last flush,
// and a flush whose write failed is retried a minute later.
func TestTheBootstrapTickWritesBatchedRenewalsAndRetriesAFailedOne(t *testing.T) {
	t.Parallel()
	clock := newSharedStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	svc, ticks, stop := bootstrapFlushNode(t, clock, path)
	defer stop()
	store := svc.secureSessions.store
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}

	clock.advance(renewalFlushEvery + time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	renewed := clock.read().UTC()
	tick(ticks)
	if got := lastV2OnDisk(t, path, clock, endpoint); !got.Equal(renewed) {
		t.Fatalf("the tick a minute on did not write the renewal: on disk %v, want %v", got, renewed)
	}

	// A failing disk: the flush fails, and the next one a minute later
	// writes it.
	diskFull := errors.New("disk full")
	store.writeMu.Lock()
	store.writeFile = func([]byte) error { return diskFull }
	store.writeMu.Unlock()
	clock.advance(renewalFlushEvery)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	failed := clock.read().UTC()
	tick(ticks)
	store.writeMu.Lock()
	store.writeFile = store.writeToDisk
	store.writeMu.Unlock()
	tick(ticks)
	if got := lastV2OnDisk(t, path, clock, endpoint); got.Equal(failed) {
		t.Fatal("the failed flush was retried before a minute had passed")
	}
	clock.advance(renewalFlushEvery)
	tick(ticks)
	if got := lastV2OnDisk(t, path, clock, endpoint); !got.Equal(failed) {
		t.Fatalf("the failed flush was not retried a minute later: on disk %v, want %v", got, failed)
	}
}

// Stopping the node writes the renewals the last tick left unwritten.
func TestStoppingTheNodeWritesTheUnwrittenRenewals(t *testing.T) {
	t.Parallel()
	clock := newSharedStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	svc, _, stop := bootstrapFlushNode(t, clock, path)
	store := svc.secureSessions.store
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clock.advance(5 * time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone())
	stop()
	if got := lastV2OnDisk(t, path, clock, endpoint); !got.Equal(clock.read().UTC()) {
		t.Fatalf("stopping the node lost the last renewal: on disk %v, want %v", got, clock.read().UTC())
	}
}
