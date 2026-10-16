package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// endpoint_binding_journal_test.go pins what happens to an endpoint binding
// when its endpoint changes hands or its 30 days run out
// (docs/protocol/session_v2.md, "Endpoint bindings"): an address dial that
// proves a new identity on a bound endpoint moves the binding to it with the
// same tier and a term no shorter, journals endpoint_owner_changed with the
// connection it happened on, and hands the new owner nothing of the old one;
// an ordinary binding past its term is removed on the periodic flush — not
// only when the ordinary tier is full — and every removal is journalled as
// endpoint_binding_expired.

// storeJournal is a store's log, safe for the goroutines of a running node.
type storeJournal struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (j *storeJournal) Write(p []byte) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.buf.Write(p)
}

// entries are the journal lines whose message is message, decoded.
func (j *storeJournal) entries(t *testing.T, message string) []map[string]any {
	t.Helper()
	j.mu.Lock()
	raw := append([]byte(nil), j.buf.Bytes()...)
	j.mu.Unlock()
	var found []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		var entry map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("journal line is not JSON: %v: %s", err, scanner.Text())
		}
		if entry["message"] == message {
			found = append(found, entry)
		}
	}
	return found
}

func journalledStore(store *secureSessionStore) *storeJournal {
	journal := &storeJournal{}
	store.logger = zerolog.New(journal)
	return journal
}

// ownerChangeCase is a binding a dialler holds for an endpoint before the
// endpoint is found under a new owner.
type ownerChangeCase struct {
	protection endpointProtection
	tier       endpointBindingTier
}

func ownerChangeCases() map[string]ownerChangeCase {
	return map[string]ownerChangeCase{
		"general":   {protection: endpointProtectionNone(), tier: bindingTierGeneral},
		"protected": {protection: endpointProtectionWanted(domain.ProtectedBindingPoolContact), tier: bindingTierProtected},
	}
}

// An address dial that proves Y on an endpoint bound to X takes the binding
// over for Y: the tier stays — a protected binding keeps its giver and pool
// — an ordinary one's term is no shorter than before, and one
// endpoint_owner_changed line names the endpoint, both identities and the
// connection. Proving Y again changes no owner and journals nothing.
func TestAnAddressDialProvingANewOwnerMovesTheBindingAndJournalsIt(t *testing.T) {
	t.Parallel()
	for name, tc := range ownerChangeCases() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			listener, address, stopListener := listenerNode(t, nil)
			defer stopListener()
			endpoint := domain.PeerAddress(normalizeAddress(address))
			previous, owner := domain.PeerIdentity{0x80, 0x01}, peerIdentityOf(t, listener)

			var journal *storeJournal
			dialer, stopDialer := startQuietNode(t, func(svc *Service) {
				journal = journalledStore(svc.secureSessions.store)
				if result, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), previous, tc.protection); err != nil || result.tier != tc.tier {
					t.Fatalf("bind the previous owner: tier %v, err %v", result.tier, err)
				}
			})
			defer stopDialer()
			store := dialer.secureSessions.store
			before, _ := bindingOf(store, endpoint)

			connID, established, err := dialSessionPastTransport(t, dialer, endpoint)
			if !established {
				t.Fatalf("no v2 session with the new owner: %v", err)
			}

			after, bound := bindingOf(store, endpoint)
			if !bound || after.identity != owner {
				t.Fatalf("binding after the dial: bound %v, last identity %s; want %s", bound, after.identity, owner)
			}
			if after.tier() != tc.tier {
				t.Fatalf("the change of owner moved the tier from %v to %v", tc.tier, after.tier())
			}
			if (before.protection == nil) != (after.protection == nil) || (after.protection != nil && *after.protection != *before.protection) {
				t.Fatalf("the change of owner moved the protection from %+v to %+v", before.protection, after.protection)
			}
			if after.lastV2.Before(before.lastV2) {
				t.Fatalf("the change of owner shortened the term: last v2 %v, before %v", after.lastV2, before.lastV2)
			}
			if !store.endpointRequiresV2(endpoint) {
				t.Fatal("the endpoint no longer requires v2 after the change of owner")
			}

			changes := journal.entries(t, "endpoint_owner_changed")
			if len(changes) != 1 {
				t.Fatalf("endpoint_owner_changed logged %d times, want once", len(changes))
			}
			change := changes[0]
			if change["peer"] != string(endpoint) || change["previous_identity"] != previous.String() || change["identity"] != owner.String() {
				t.Fatalf("endpoint_owner_changed = %v; want peer %s, previous_identity %s, identity %s", change, endpoint, previous, owner)
			}
			if got, ok := change["conn_id"].(float64); !ok || domain.ConnID(got) != connID {
				t.Fatalf("endpoint_owner_changed conn_id = %v, want the session's %d", change["conn_id"], connID)
			}

			if established, err := dialPastTransport(t, dialer, endpoint); !established {
				t.Fatalf("no second session with the same owner: %v", err)
			}
			if got := len(journal.entries(t, "endpoint_owner_changed")); got != 1 {
				t.Fatalf("proving the same owner again logged endpoint_owner_changed: %d lines", got)
			}
		})
	}
}

// The new owner of an endpoint inherits nothing from the previous one: the
// previous owner keeps its pin, the protection still counts for the identity
// that gave it, and the new owner has no protection of its own — its quota
// is untouched and it is not the giver of anything.
func TestANewOwnerOfAnEndpointInheritsNothingFromThePrevious(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	previous, owner := domain.PeerIdentity{0x82, 0x01}, peerIdentityOf(t, listener)

	dialer, stopDialer := startQuietNode(t, func(svc *Service) {
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), previous, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); err != nil {
			t.Fatalf("bind the previous owner: %v", err)
		}
	})
	defer stopDialer()
	store := dialer.secureSessions.store

	if established, err := dialPastTransport(t, dialer, endpoint); !established {
		t.Fatalf("no v2 session with the new owner: %v", err)
	}
	if binding, _ := bindingOf(store, endpoint); binding.identity != owner {
		t.Fatalf("precondition: the endpoint is owned by %s, want %s", binding.identity, owner)
	}

	if !store.identityPinned(previous) {
		t.Fatal("the previous owner lost its pin when the endpoint changed hands")
	}
	binding, _ := bindingOf(store, endpoint)
	if binding.protection == nil || binding.protection.by != previous || binding.protection.pool != domain.ProtectedBindingPoolContact {
		t.Fatalf("the protection is %+v, want still given by %s in pool K", binding.protection, previous)
	}
	store.mu.Lock()
	byPrevious, byOwner := store.protectedByGiver[previous], store.protectedByGiver[owner]
	store.mu.Unlock()
	if byPrevious != 1 || byOwner != 0 {
		t.Fatalf("protected bindings counted for the previous owner %d, for the new one %d; want 1 and 0", byPrevious, byOwner)
	}
	if got := store.stats().ProtectedBindingsContact; got != 1 {
		t.Fatalf("protected bindings in pool K = %d, want the one the previous owner gave", got)
	}
	if routes := dialer.routingTable.Lookup(previous); len(routes) != 0 {
		t.Fatalf("the new owner's session gave the previous owner routes: %+v", routes)
	}
}

// A change of owner that crashed between its temporary file and the rename
// leaves the previous owner on disk, and one that got through leaves the new
// one: both require v2, so after a restart a v1 answer on the endpoint is a
// downgrade either way and the dialler does not follow it.
func TestAnOwnerChangeInterruptedByACrashKeepsTheEndpointOnV2AfterARestart(t *testing.T) {
	t.Parallel()
	old, address, stopOld := listenerNode(t, withoutOwnDials(asOldNode))
	defer stopOld()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	previous, owner := domain.PeerIdentity{0x83, 0x01}, domain.PeerIdentity{0x83, 0x02}
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")

	first := loadSecureSessionStore(path, trustedclock.NewSystem())
	if _, err := first.noteProvenOutbound(dialledAt(endpoint), previous, endpointProtectionNone()); err != nil {
		t.Fatalf("bind the previous owner: %v", err)
	}
	errCrash := errors.New("crashed before the rename")
	first.writeFile = func(raw []byte) error {
		temp := filepath.Join(filepath.Dir(path), secretfile.TempPrefix+"owner-change")
		if err := os.WriteFile(temp, raw, 0o600); err != nil {
			return err
		}
		return errCrash
	}
	if _, err := first.noteProvenOutbound(dialledAt(endpoint), owner, endpointProtectionNone()); !errors.Is(err, errCrash) {
		t.Fatalf("the change of owner = %v, want the crash", err)
	}

	crashed := loadSecureSessionStore(path, trustedclock.NewSystem())
	if binding, bound := bindingOf(crashed, endpoint); !bound || binding.identity != previous || !crashed.endpointRequiresV2(endpoint) {
		t.Fatalf("after a crash before the rename: bound %v, owner %s, requires v2 %v; want the previous owner, v2", bound, binding.identity, crashed.endpointRequiresV2(endpoint))
	}

	first.writeFile = first.writeToDisk
	if err := first.flushRenewalsNow(); err != nil {
		t.Fatalf("write the change of owner: %v", err)
	}
	renamed := loadSecureSessionStore(path, trustedclock.NewSystem())
	if binding, bound := bindingOf(renamed, endpoint); !bound || binding.identity != owner || !renamed.endpointRequiresV2(endpoint) {
		t.Fatalf("after the rename: bound %v, owner %s, requires v2 %v; want the new owner, v2", bound, binding.identity, renamed.endpointRequiresV2(endpoint))
	}

	for name, restarted := range map[string]*secureSessionStore{"crashed before the rename": crashed, "renamed": renamed} {
		dialer, stopDialer := dialerNode(t, address, func(svc *Service) {
			svc.secureSessions.store = restarted
		})
		assertDowngradeRefused(t, dialer, old, endpoint, "an endpoint whose change of owner "+name)
		stopDialer()
	}
}

// An ordinary binding past its 30 days is removed on the periodic flush, even
// with the ordinary tier far from full, and the removal is journalled with
// the endpoint and its last identity. A protected binding stays, and so does
// an ordinary one still within its term. The removal reaches the disk.
func TestAnExpiredBindingIsSweptOnTheFlushAndJournalled(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	journal := journalledStore(store)
	expiring, fresh, protected := domain.PeerAddress("192.0.2.1:64646"), domain.PeerAddress("192.0.2.2:64646"), domain.PeerAddress("192.0.2.3:64646")
	expiringOwner := domain.PeerIdentity{0xe1}
	if _, err := store.noteProvenOutbound(dialledAt(expiring), expiringOwner, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := store.noteProvenOutbound(dialledAt(protected), domain.PeerIdentity{0xe3}, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clock.now = clock.now.Add(endpointBindingTTL / 2)
	if _, err := store.noteProvenOutbound(dialledAt(fresh), domain.PeerIdentity{0xe2}, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}

	clock.now = clock.now.Add(endpointBindingTTL/2 + time.Second)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if _, bound := bindingOf(store, expiring); bound {
		t.Fatal("an ordinary binding past its 30 days is still held after the flush")
	}
	for _, kept := range []domain.PeerAddress{fresh, protected} {
		if _, bound := bindingOf(store, kept); !bound {
			t.Fatalf("the flush removed %s, which had not expired", kept)
		}
	}
	if got := store.stats().GeneralBindings; got != 1 {
		t.Fatalf("ordinary bindings counted = %d after the sweep, want 1", got)
	}
	expired := journal.entries(t, "endpoint_binding_expired")
	if len(expired) != 1 || expired[0]["peer"] != string(expiring) || expired[0]["identity"] != expiringOwner.String() {
		t.Fatalf("endpoint_binding_expired = %v; want one line for %s owned by %s", expired, expiring, expiringOwner)
	}
	if _, bound := bindingOf(loadSecureSessionStore(path, clock.trusted()), expiring); bound {
		t.Fatal("the swept binding is still on disk")
	}
	assertTallies(t, store)
}

// Every other way an expired binding leaves the store is journalled too: a
// new binding on the same endpoint replaces it, and a full ordinary tier
// prunes it to make room.
func TestEveryRemovalOfAnExpiredBindingIsJournalled(t *testing.T) {
	t.Parallel()
	t.Run("replaced by a new binding", func(t *testing.T) {
		t.Parallel()
		clock := newStoreClock()
		store := loadSecureSessionStore("", clock.trusted())
		journal := journalledStore(store)
		endpoint, previous := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{0xe1}
		_, _ = store.noteProvenOutbound(dialledAt(endpoint), previous, endpointProtectionNone())
		clock.now = clock.now.Add(endpointBindingTTL + time.Second)
		_, _ = store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{0xe2}, endpointProtectionNone())

		expired := journal.entries(t, "endpoint_binding_expired")
		if len(expired) != 1 || expired[0]["peer"] != string(endpoint) || expired[0]["identity"] != previous.String() {
			t.Fatalf("endpoint_binding_expired = %v; want one line for %s owned by %s", expired, endpoint, previous)
		}
		if got := len(journal.entries(t, "endpoint_owner_changed")); got != 0 {
			t.Fatalf("a binding created after the expiry was journalled as a change of owner %d times", got)
		}
	})
	t.Run("pruned by a full ordinary tier", func(t *testing.T) {
		t.Parallel()
		clock := newStoreClock()
		store := loadSecureSessionStore("", clock.trusted())
		journal := journalledStore(store)
		store.mu.Lock()
		for i := 0; store.generalBindings < maxGeneralBindings; i++ {
			insertSeededLocked(t, store, boundEndpoint(i), endpointBinding{identity: boundIdentity(i), lastV2: store.now()})
		}
		store.mu.Unlock()
		clock.now = clock.now.Add(endpointBindingTTL + time.Second)
		if result, _ := store.noteProvenOutbound(dialledAt("198.51.100.1:64646"), domain.PeerIdentity{0xe2}, endpointProtectionNone()); result.tier != bindingTierGeneral {
			t.Fatalf("no room made in a tier of expired bindings: tier %v", result.tier)
		}
		if got := len(journal.entries(t, "endpoint_binding_expired")); got != maxGeneralBindings {
			t.Fatalf("endpoint_binding_expired logged %d times, want once for each of the %d pruned bindings", got, maxGeneralBindings)
		}
	})
}

// The sweep of expired bindings runs at most once a minute: a binding that
// expires right after a sweep stays in the store through the ticks of the
// next minute — it already requires nothing — and is removed by the first
// flush a minute after the last sweep.
func TestTheExpirySweepRunsAtMostOnceAMinute(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	endpoint := domain.PeerAddress("192.0.2.1:64646")
	_, _ = store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{0xe1}, endpointProtectionNone())

	clock.now = clock.now.Add(endpointBindingTTL - time.Second)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	clock.now = clock.now.Add(renewalFlushEvery / 2)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if store.endpointRequiresV2(endpoint) {
		t.Fatal("the binding still requires v2 past its 30 days")
	}
	if _, held := bindingOf(store, endpoint); !held {
		t.Fatal("a flush 30 s after the last sweep swept again")
	}
	clock.now = clock.now.Add(renewalFlushEvery / 2)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, held := bindingOf(store, endpoint); held {
		t.Fatal("the flush a minute after the last sweep did not remove the expired binding")
	}
}

// A sweep due while the batch of writes is not removes the binding in memory
// at once, but the file keeps the record until the next batch writes, like a
// renewal; the next batch removes it from the disk too.
func TestASweptBindingLeavesTheDiskWithTheNextBatch(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	expiring, renewed := domain.PeerAddress("192.0.2.1:64646"), domain.PeerAddress("192.0.2.2:64646")
	_, _ = store.noteProvenOutbound(dialledAt(expiring), domain.PeerIdentity{0xe1}, endpointProtectionNone())
	onDisk := func() bool {
		_, held := bindingOf(loadSecureSessionStore(path, clock.trusted()), expiring)
		return held
	}

	// The sweep runs at T, ten seconds before the binding expires; nothing
	// is due for the disk. The second binding is created then, so it is
	// renewed, not replaced, half a minute later.
	clock.now = clock.now.Add(endpointBindingTTL - 10*time.Second)
	_, _ = store.noteProvenOutbound(dialledAt(renewed), domain.PeerIdentity{0xe2}, endpointProtectionNone())
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// A renewal written at T+30 s puts the batch half a minute behind the
	// sweep.
	clock.now = clock.now.Add(renewalFlushEvery / 2)
	_, _ = store.noteProvenOutbound(dialledAt(renewed), domain.PeerIdentity{0xe2}, endpointProtectionNone())
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// T+60 s: the sweep is due, the batch is not.
	clock.now = clock.now.Add(renewalFlushEvery / 2)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, held := bindingOf(store, expiring); held {
		t.Fatal("the due sweep left the expired binding in memory")
	}
	if !onDisk() {
		t.Fatal("the removal was written before the next batch")
	}
	// T+90 s: the batch is due and writes the removal.
	clock.now = clock.now.Add(renewalFlushEvery / 2)
	if err := store.flushRenewals(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if onDisk() {
		t.Fatal("the next batch did not write the removal of the expired binding")
	}
}
