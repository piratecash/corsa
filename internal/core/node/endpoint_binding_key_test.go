package node

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_binding_key_test.go pins how the store keys, groups and bounds the
// endpoint bindings (docs/protocol/session_v2.md, "Endpoint keys and source
// groups"): one binding per canonical endpoint, a source group fixed at
// creation, at most generalCreationsPerGroupPerDay new ordinary bindings a
// day per group, the derived key of a name dialled direct, and a store file
// that the build before these fields wrote — and that build itself — read
// without loss.

// dialledAt is an outbound dial to address whose connected address is not
// known, as on a socket without an IP peer.
func dialledAt(address domain.PeerAddress) outboundDial {
	return outboundDial{address: address}
}

// dialledReaching is a direct dial to address that reached connected.
func dialledReaching(address domain.PeerAddress, connected string) outboundDial {
	reached := netip.MustParseAddrPort(connected)
	return outboundDial{address: address, connected: &reached}
}

// bindingAt is the binding the store holds for address, by its canonical key.
func bindingAt(t *testing.T, store *secureSessionStore, address domain.PeerAddress) (endpointBinding, bool) {
	t.Helper()
	return bindingOf(store, address)
}

// At the store: two spellings of one endpoint are one binding.
func TestTwoSpellingsOfOneEndpointAreOneBinding(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{0x73}
	for _, spelling := range []domain.PeerAddress{"[::ffff:198.51.100.1]:64646", "198.51.100.1:64646", "[2001:DB8::1]:64646", "[2001:db8:0::1]:64646", "Node.Example.COM.:64646", "node.example.com:64646"} {
		if _, err := store.noteProvenOutbound(dialledAt(spelling), id, endpointProtectionNone()); err != nil {
			t.Fatalf("note %q: %v", spelling, err)
		}
	}
	if got := store.stats().GeneralBindings; got != 3 {
		t.Fatalf("six spellings of three endpoints made %d bindings, want 3", got)
	}
	for _, spelling := range []domain.PeerAddress{"198.51.100.1:64646", "[::FFFF:198.51.100.1]:64646", "[2001:db8::0:1]:64646", "NODE.example.com:64646"} {
		if !store.endpointRequiresV2(spelling) {
			t.Errorf("%q is not covered by the binding of its endpoint", spelling)
		}
	}
}

// At the store: an address with no canonical form gets no binding,
// with the reason journalled and counted; the session's identity is pinned
// and the session stands. Punycode is accepted.
func TestAnAddressWithoutACanonicalFormGetsNoBinding(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	journal := journalledStore(store)
	id := domain.PeerIdentity{0x86}
	for _, address := range []domain.PeerAddress{"127.1:64646", "0x7f000001:64646", "2130706433:64646", "пример.рф:64646"} {
		result, err := store.noteProvenOutbound(dialledAt(address), id, endpointProtectionNone())
		if err != nil || result.tier != bindingTierNone {
			t.Fatalf("%q: tier %v, err %v; want no binding and no error", address, result.tier, err)
		}
	}
	// The aliases among them always require v2
	// (TestAnAliasOfABoundableHostAlwaysRequiresV2); the rest have nothing
	// that would.
	if store.endpointRequiresV2("пример.рф:64646") {
		t.Fatal("a non-ASCII name requires v2 without a binding")
	}
	if !store.identityPinned(id) {
		t.Fatal("the identity proven on an address without a canonical form was not pinned")
	}
	stats := store.stats()
	if stats.GeneralBindings != 0 || stats.EndpointBindingsSkippedNotCanonical != 4 {
		t.Fatalf("bindings %d, skipped %d; want 0 and 4", stats.GeneralBindings, stats.EndpointBindingsSkippedNotCanonical)
	}
	if lines := journal.entries(t, "endpoint_binding_skipped_not_canonical"); len(lines) != 4 || lines[0]["reason"] == nil {
		t.Fatalf("endpoint_binding_skipped_not_canonical = %v, want four lines with a reason", lines)
	}
	if result, _ := store.noteProvenOutbound(dialledAt("xn--80ak6aa92e.com:64646"), id, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("a punycode name: tier %v, want general", result.tier)
	}
}

// A stream of v2 sessions with fresh identities in one group
// creates at most 64 ordinary bindings a day there. The refusal is journalled
// once per group and window and counted every time; another group, the
// renewals and changes of owner of the bindings that exist, and protection
// are not touched; a day later the group may grow again.
func TestNewOrdinaryBindingsAreBoundedPerSourceGroupPerDay(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	journal := journalledStore(store)
	inGroup := func(i int) domain.PeerAddress {
		return domain.PeerAddress(fmt.Sprintf("198.51.%d.%d:64646", i/250, i%250+1))
	}
	for i := range generalCreationsPerGroupPerDay {
		if result, _ := store.noteProvenOutbound(dialledAt(inGroup(i)), boundIdentity(i), endpointProtectionNone()); result.tier != bindingTierGeneral {
			t.Fatalf("binding #%d of the group: tier %v, want general", i, result.tier)
		}
	}
	over := inGroup(generalCreationsPerGroupPerDay)
	for range 2 {
		result, err := store.noteProvenOutbound(dialledAt(over), domain.PeerIdentity{0x74}, endpointProtectionNone())
		if err != nil || result.tier != bindingTierNone {
			t.Fatalf("past the quota: tier %v, err %v; want no binding and no error", result.tier, err)
		}
	}
	if store.endpointRequiresV2(over) {
		t.Fatal("a binding was created past the group's quota")
	}
	stats := store.stats()
	if stats.GeneralRefusalsSourceGroupQuota != 2 || stats.GeneralCreationsPerGroupPerDay != generalCreationsPerGroupPerDay {
		t.Fatalf("refusals %d, quota %d; want 2 and %d", stats.GeneralRefusalsSourceGroupQuota, stats.GeneralCreationsPerGroupPerDay, generalCreationsPerGroupPerDay)
	}
	lines := journal.entries(t, "endpoint_binding_refused_source_group_quota")
	if len(lines) != 1 || lines[0]["source_group"] != "198.51.0.0/16" {
		t.Fatalf("endpoint_binding_refused_source_group_quota = %v, want one line naming 198.51.0.0/16", lines)
	}

	if result, _ := store.noteProvenOutbound(dialledAt("203.0.113.1:64646"), domain.PeerIdentity{0x75}, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("another group was refused by a full one: tier %v", result.tier)
	}
	clock.now = clock.now.Add(time.Hour)
	if result, err := store.noteProvenOutbound(dialledAt(inGroup(0)), domain.PeerIdentity{0x76}, endpointProtectionNone()); err != nil || result.tier != bindingTierGeneral || result.ownerChange == nil {
		t.Fatalf("a change of owner in a full group: tier %v, owner change %v, err %v", result.tier, result.ownerChange, err)
	}
	if binding, _ := bindingAt(t, store, inGroup(0)); !binding.lastV2.Equal(clock.now) {
		t.Fatal("the renewal of an existing binding in a full group was refused")
	}
	if result, _ := store.noteProvenOutbound(dialledAt(over), domain.PeerIdentity{0x77}, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
		t.Fatalf("a protection in a full group: tier %v, want protected — the quota bounds ordinary bindings only", result.tier)
	}

	clock.now = clock.now.Add(24 * time.Hour)
	if result, _ := store.noteProvenOutbound(dialledAt(inGroup(generalCreationsPerGroupPerDay+1)), domain.PeerIdentity{0x78}, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("a day later the group is still refused: tier %v", result.tier)
	}
}

// The quota counts creations, not live bindings, and the creation times are
// on disk: a restart does not hand the group a fresh day.
func TestTheSourceGroupQuotaSurvivesARestart(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	for i := range generalCreationsPerGroupPerDay {
		if _, err := store.noteProvenOutbound(dialledAt(domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i+1))), boundIdentity(i), endpointProtectionNone()); err != nil {
			t.Fatalf("note #%d: %v", i, err)
		}
	}
	clock.now = clock.now.Add(time.Hour)
	restarted := loadSecureSessionStore(path, clock.trusted())
	if result, _ := restarted.noteProvenOutbound(dialledAt("198.51.200.1:64646"), domain.PeerIdentity{0x74}, endpointProtectionNone()); result.tier != bindingTierNone {
		t.Fatalf("a restart reopened the group's quota: tier %v", result.tier)
	}
	clock.now = clock.now.Add(24 * time.Hour)
	if result, _ := restarted.noteProvenOutbound(dialledAt("198.51.200.1:64646"), domain.PeerIdentity{0x74}, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("a day after the creations the group is still refused: tier %v", result.tier)
	}
}

// 10^4 onion endpoints proving v2 in one day make at most 64
// bindings in the tor group; the clearnet bindings and the onion bindings
// made before the stream are untouched.
func TestAStreamOfOnionEndpointsStaysInsideTheTorQuota(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	onion := func(i int) domain.PeerAddress {
		name := fmt.Sprintf("%056d", i)
		name = strings.Map(func(r rune) rune { return 'a' + (r - '0') }, name)
		return domain.PeerAddress(name + ".onion:64646")
	}
	settled := []domain.PeerAddress{onion(1_000_001), onion(1_000_002), "203.0.113.1:64646", "[2001:db8::1]:64646"}
	for i, address := range settled {
		if result, _ := store.noteProvenOutbound(dialledAt(address), domain.PeerIdentity{0x85, byte(i)}, endpointProtectionNone()); result.tier != bindingTierGeneral {
			t.Fatalf("%q: tier %v, want general", address, result.tier)
		}
	}
	clock.now = clock.now.Add(2 * 24 * time.Hour)
	created := 0
	for i := range 10_000 {
		if result, _ := store.noteProvenOutbound(dialledAt(onion(i)), boundIdentity(i), endpointProtectionNone()); result.tier == bindingTierGeneral {
			created++
		}
	}
	if created != generalCreationsPerGroupPerDay {
		t.Fatalf("the onion stream created %d bindings in a day, want %d", created, generalCreationsPerGroupPerDay)
	}
	for _, address := range settled {
		if !store.endpointRequiresV2(address) {
			t.Errorf("%q lost its binding to the onion stream", address)
		}
	}
	if result, _ := store.noteProvenOutbound(dialledAt("198.51.100.9:64646"), domain.PeerIdentity{0x86}, endpointProtectionNone()); result.tier != bindingTierGeneral {
		t.Fatalf("a clearnet endpoint was refused after the onion stream: tier %v", result.tier)
	}
	if binding, _ := bindingAt(t, store, onion(0)); binding.group != domain.SourceGroupTor {
		t.Fatalf("an onion binding is in group %q, want tor", binding.group)
	}
}

// A name dialled direct binds the address the dial reached too —
// a general derived key with derived_from, in the group of that address — so
// the IP form of the same host cannot be downgraded after a restart. An
// overlay dial reaches the proxy, never the peer, and derives nothing; nor
// does an IP dial or a dial whose connected address is unknown.
func TestANameDialledDirectBindsTheAddressItReached(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	contact := domain.PeerIdentity{0x84}
	name, reached := domain.PeerAddress("Node.Example.com:64646"), domain.PeerAddress("198.51.100.20:64646")
	result, err := store.noteProvenOutbound(dialledReaching(name, "[::ffff:198.51.100.20]:64646"), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if err != nil || result.tier != bindingTierProtected {
		t.Fatalf("the name: tier %v, err %v; want protected", result.tier, err)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	nameBinding, _ := bindingAt(t, restarted, name)
	derived, bound := bindingAt(t, restarted, reached)
	switch {
	case !bound || derived.protection != nil:
		t.Fatalf("the reached address: bound %v, protected %v; want a general binding", bound, bound && derived.protection != nil)
	case derived.derivedFrom == nil || derived.derivedFrom.Address() != "node.example.com:64646":
		t.Fatalf("the reached address derives from %v, want node.example.com:64646", derived.derivedFrom)
	case derived.group != "198.51.0.0/16" || nameBinding.group != "198.51.0.0/16":
		t.Fatalf("groups: name %q, derived %q; want both 198.51.0.0/16", nameBinding.group, derived.group)
	case derived.identity != contact:
		t.Fatalf("the derived key names %v, want %v", derived.identity, contact)
	}

	// A direct dial "to X" on the IP form never raises the derived key to
	// protected; it only renews its term.
	clock.now = clock.now.Add(time.Hour)
	result, _ = restarted.noteProvenOutbound(dialledAt(reached), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact))
	if result.tier != bindingTierGeneral || refusalReason(result) != domain.ProtectedBindingRefusedDerivedKey {
		t.Fatalf("a dial to X on the derived key: tier %v, refusal %q; want general, derived_key", result.tier, refusalReason(result))
	}
	if renewed, _ := bindingAt(t, restarted, reached); !renewed.lastV2.Equal(clock.now) || renewed.derivedFrom == nil {
		t.Fatal("the derived key was not renewed, or lost its derived_from")
	}

	for _, dial := range []outboundDial{
		dialledReaching(domain.PeerAddress(testOnionV3+".onion:64646"), "127.0.0.1:9050"),
		dialledReaching("203.0.113.5:64646", "203.0.113.5:64646"),
		dialledAt("other.example.com:64646"),
	} {
		before := restarted.stats().GeneralBindings
		if _, err := restarted.noteProvenOutbound(dial, domain.PeerIdentity{0x85}, endpointProtectionNone()); err != nil {
			t.Fatalf("%q: %v", dial.address, err)
		}
		if got := restarted.stats().GeneralBindings - before; got != 1 {
			t.Errorf("%q made %d bindings, want only its own", dial.address, got)
		}
	}
	if _, proxied := bindingAt(t, restarted, "127.0.0.1:9050"); proxied {
		t.Fatal("an onion dial bound the proxy's address")
	}
	if other, _ := bindingAt(t, restarted, "other.example.com:64646"); other.group != domain.SourceGroupUnresolvedName {
		t.Fatalf("a name whose reached address is unknown is in %q, want %q", other.group, domain.SourceGroupUnresolvedName)
	}
}

// On a running node: the connection manager dialling a name that
// resolves to the listener binds the name and, derived from it, the address
// the dial reached.
func TestADialByNameBindsTheReachedAddressOnANode(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	listenerID := peerIdentityOf(t, listener)
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	_, port, _ := splitHostPort(address)
	byName := domain.PeerAddress("localhost:" + port)

	if established, err := dialPastTransport(t, dialer, byName); !established {
		t.Fatalf("the dial by name was not established: %v", err)
	}
	store := dialer.secureSessions.store
	nameBinding, bound := bindingAt(t, store, byName)
	if !bound || nameBinding.identity != listenerID || nameBinding.group != domain.SourceGroupLoopback {
		t.Fatalf("the name: bound %v, identity %v, group %q; want the listener in loopback", bound, nameBinding.identity, nameBinding.group)
	}
	store.mu.Lock()
	var derived []endpointBinding
	for endpoint, binding := range store.endpoints {
		if binding.derivedFrom != nil && binding.derivedFrom.Address() == byName {
			derived = append(derived, binding)
			if host, _, _ := splitHostPort(string(endpoint.Address())); !netip.MustParseAddr(host).IsLoopback() {
				t.Errorf("the derived key %s is not the loopback address the dial reached", endpoint)
			}
		}
	}
	store.mu.Unlock()
	if len(derived) != 1 || derived[0].protection != nil || derived[0].identity != listenerID {
		t.Fatalf("derived keys of %s: %d, want one general binding to the listener", byName, len(derived))
	}
}

// headSecureSessionFile is the store file as the build before endpoint keys
// and groups wrote and read it: format 1, the raw dialled address as the
// key, no transport, source group, creation time or derived_from.
type headSecureSessionFile struct {
	Format     int                                              `json:"format"`
	Identities map[domain.PeerIdentity]time.Time                `json:"identities"`
	Endpoints  map[domain.PeerAddress]headEndpointBindingRecord `json:"endpoints"`
}

type headEndpointBindingRecord struct {
	Identity    domain.PeerIdentity          `json:"identity"`
	LastV2      time.Time                    `json:"last_v2"`
	Protected   bool                         `json:"protected,omitempty"`
	ProtectedBy *domain.PeerIdentity         `json:"protected_by,omitempty"`
	Pool        *domain.ProtectedBindingPool `json:"pool,omitempty"`
}

func writeHeadStoreFile(t *testing.T, path string, file headSecureSessionFile) {
	t.Helper()
	raw, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// A file the previous build wrote — no new fields — is read without loss:
// every binding keeps its identity, tier, giver, pool and last v2 time under
// its canonical key, and gets the source group of its address.
func TestAStoreFileWithoutKeysAndGroupsIsReadWithoutLoss(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	at := clock.now.Add(-time.Hour).UTC()
	giver, pool := domain.PeerIdentity{0xa2}, domain.ProtectedBindingPoolRelation
	writeHeadStoreFile(t, path, headSecureSessionFile{
		Format:     secureSessionStoreFormat,
		Identities: map[domain.PeerIdentity]time.Time{{0xa1}: at, giver: at},
		Endpoints: map[domain.PeerAddress]headEndpointBindingRecord{
			"198.51.100.1:64646":          {Identity: domain.PeerIdentity{0xa1}, LastV2: at},
			"[::ffff:198.51.100.2]:64646": {Identity: giver, LastV2: at, Protected: true, ProtectedBy: &giver, Pool: &pool},
			"[2001:DB8::1]:64646":         {Identity: domain.PeerIdentity{0xa1}, LastV2: at, Protected: true},
			"Node.Example.com:64646":      {Identity: domain.PeerIdentity{0xa1}, LastV2: at},
			testOnionV3 + ".onion:64646":  {Identity: domain.PeerIdentity{0xa1}, LastV2: at},
		},
	})
	store := loadSecureSessionStore(path, clock.trusted())
	if store.unreadable {
		t.Fatal("a file of the previous build was refused as unreadable")
	}
	want := map[domain.PeerAddress]struct {
		group      domain.SourceGroup
		protection *bindingProtection
	}{
		"198.51.100.1:64646":         {"198.51.0.0/16", nil},
		"198.51.100.2:64646":         {"198.51.0.0/16", &bindingProtection{by: giver, pool: pool}},
		"[2001:db8::1]:64646":        {"2001:db8::/32", &bindingProtection{by: domain.PeerIdentity{0xa1}, pool: domain.ProtectedBindingPoolContact}},
		"node.example.com:64646":     {domain.SourceGroupUnresolvedName, nil},
		testOnionV3 + ".onion:64646": {domain.SourceGroupTor, nil},
	}
	for address, expect := range want {
		binding, bound := bindingAt(t, store, address)
		switch {
		case !bound:
			t.Errorf("%q lost its binding", address)
		case binding.group != expect.group:
			t.Errorf("%q is in group %q, want %q", address, binding.group, expect.group)
		case !binding.lastV2.Equal(at):
			t.Errorf("%q last v2 %v, want %v", address, binding.lastV2, at)
		case (binding.protection == nil) != (expect.protection == nil) || (expect.protection != nil && *binding.protection != *expect.protection):
			t.Errorf("%q protection %v, want %v", address, binding.protection, expect.protection)
		case binding.firstAt != nil:
			t.Errorf("%q has a creation time the file never held", address)
		}
	}
	if stats := store.stats(); stats.GeneralBindings != 3 || stats.ProtectedBindingsContact != 1 || stats.ProtectedBindingsRelation != 1 {
		t.Fatalf("counts: general %d, contact %d, relation %d; want 3, 1, 1", stats.GeneralBindings, stats.ProtectedBindingsContact, stats.ProtectedBindingsRelation)
	}
}

// Two records of the previous build that are spellings of one endpoint merge
// into one binding field by field, whatever order the file is read in: the
// strongest protection — any protection over none, the contact pool over the
// relation pool, its giver with it — so a merge never lowers protection or
// moves a contact's place to the relation pool; the identity and last v2 of
// the fresher record, the endpoint's last proven owner; the earliest creation
// time. The bounds' counts follow the merged binding, and the merge is
// journalled.
func TestRecordsOfOneEndpointMergeWithoutLoweringProtection(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	older, fresher := clock.now.Add(-2*time.Hour).UTC(), clock.now.Add(-time.Hour).UTC()
	contactGiver, relationGiver := domain.PeerIdentity{0xb8}, domain.PeerIdentity{0xb9}
	contactPool, relationPool := domain.ProtectedBindingPoolContact, domain.ProtectedBindingPoolRelation
	cases := map[string]struct {
		records    map[domain.PeerAddress]headEndpointBindingRecord
		key        domain.PeerAddress
		identity   domain.PeerIdentity
		lastV2     time.Time
		protection *bindingProtection
	}{
		"protection survives a fresher ordinary record": {
			records: map[domain.PeerAddress]headEndpointBindingRecord{
				"[::ffff:198.51.100.1]:64646": {Identity: domain.PeerIdentity{0xb1}, LastV2: older, Protected: true},
				"198.51.100.1:64646":          {Identity: domain.PeerIdentity{0xb2}, LastV2: fresher},
			},
			key: "198.51.100.1:64646", identity: domain.PeerIdentity{0xb2}, lastV2: fresher,
			protection: &bindingProtection{by: domain.PeerIdentity{0xb1}, pool: contactPool},
		},
		"the contact pool over a fresher relation": {
			records: map[domain.PeerAddress]headEndpointBindingRecord{
				"[::ffff:198.51.100.3]:64646": {Identity: contactGiver, LastV2: older, Protected: true, ProtectedBy: &contactGiver, Pool: &contactPool},
				"198.51.100.3:64646":          {Identity: relationGiver, LastV2: fresher, Protected: true, ProtectedBy: &relationGiver, Pool: &relationPool},
			},
			key: "198.51.100.3:64646", identity: relationGiver, lastV2: fresher,
			protection: &bindingProtection{by: contactGiver, pool: contactPool},
		},
		"the fresher of two ordinary": {
			records: map[domain.PeerAddress]headEndpointBindingRecord{
				"Node.Example.com:64646":  {Identity: domain.PeerIdentity{0xb3}, LastV2: older},
				"node.example.com.:64646": {Identity: domain.PeerIdentity{0xb4}, LastV2: fresher},
				"NODE.example.com:64646":  {Identity: domain.PeerIdentity{0xb5}, LastV2: older},
			},
			key: "node.example.com:64646", identity: domain.PeerIdentity{0xb4}, lastV2: fresher,
		},
		"equal records: the smaller address": {
			records: map[domain.PeerAddress]headEndpointBindingRecord{
				"[2001:db8::1]:64646": {Identity: domain.PeerIdentity{0xb6}, LastV2: older},
				"[2001:DB8::1]:64646": {Identity: domain.PeerIdentity{0xb7}, LastV2: older},
			},
			key: "[2001:db8::1]:64646", identity: domain.PeerIdentity{0xb7}, lastV2: older,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
			writeHeadStoreFile(t, path, headSecureSessionFile{Format: secureSessionStoreFormat, Endpoints: tc.records})
			// Map order differs between reads: the outcome must not.
			for range 20 {
				store := loadSecureSessionStoreWith(path, clock.trusted(), zerolog.Nop())
				binding, bound := bindingAt(t, store, tc.key)
				switch {
				case !bound:
					t.Fatal("the merged binding is missing")
				case binding.identity != tc.identity || !binding.lastV2.Equal(tc.lastV2):
					t.Fatalf("merged owner %v at %v, want %v at %v", binding.identity, binding.lastV2, tc.identity, tc.lastV2)
				case (binding.protection == nil) != (tc.protection == nil) || (tc.protection != nil && *binding.protection != *tc.protection):
					t.Fatalf("merged protection %v, want %v", binding.protection, tc.protection)
				}
				stats := store.stats()
				if total := stats.GeneralBindings + stats.ProtectedBindingsContact + stats.ProtectedBindingsRelation; total != 1 {
					t.Fatalf("%d bindings after the merge, want 1", total)
				}
				assertCounts(t, store, true)
			}
			journal := &storeJournal{}
			loadSecureSessionStoreWith(path, clock.trusted(), zerolog.New(journal))
			if merged := journal.entries(t, "endpoint_binding_merged"); len(merged) != len(tc.records)-1 {
				t.Fatalf("endpoint_binding_merged logged %d times, want %d", len(merged), len(tc.records)-1)
			}
		})
	}
}

// A record of the previous build whose address has no canonical form cannot
// be keyed, and it is not dropped either — a protected one is never removed
// automatically. It is kept under the exact stored address, for comparison
// only: a lookup of that exact address finds it, it is never renewed, raised
// or created again, a protected one never expires and counts against the
// protected bounds, an ordinary one expires 30 days after its last v2, and
// every write carries it back to the file as it was.
func TestARecordWithoutACanonicalAddressIsKeptAsItWas(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	at := clock.now.Add(-time.Hour).UTC()
	giver, pool := domain.PeerIdentity{0xc1}, domain.ProtectedBindingPoolContact
	protectedRaw, generalRaw := domain.PeerAddress("bücher.de:64646"), domain.PeerAddress("foo_bar.example:64646")
	writeHeadStoreFile(t, path, headSecureSessionFile{Format: secureSessionStoreFormat, Endpoints: map[domain.PeerAddress]headEndpointBindingRecord{
		protectedRaw:         {Identity: giver, LastV2: at, Protected: true, ProtectedBy: &giver, Pool: &pool},
		generalRaw:           {Identity: domain.PeerIdentity{0xc2}, LastV2: at},
		"198.51.100.1:64646": {Identity: domain.PeerIdentity{0xc3}, LastV2: at},
	}})
	store := loadSecureSessionStore(path, clock.trusted())
	if store.unreadable || !store.endpointRequiresV2("198.51.100.1:64646") {
		t.Fatal("records without a canonical address cost the rest of the file")
	}
	if !store.endpointRequiresV2(protectedRaw) || !store.endpointRequiresV2(generalRaw) {
		t.Fatal("a record without a canonical address no longer protects its exact address")
	}
	stats := store.stats()
	if stats.NonCanonicalBindings != 2 || stats.ProtectedBindingsContact != 1 || stats.GeneralBindings != 2 {
		t.Fatalf("non-canonical %d, contact %d, general %d; want 2, 1, 2", stats.NonCanonicalBindings, stats.ProtectedBindingsContact, stats.GeneralBindings)
	}

	clock.now = clock.now.Add(time.Hour)
	if _, err := store.noteProvenOutbound(dialledAt(generalRaw), domain.PeerIdentity{0xc4}, endpointProtectionWanted(pool)); err != nil {
		t.Fatalf("note: %v", err)
	}
	store.mu.Lock()
	kept := store.nonCanonical[generalRaw]
	store.mu.Unlock()
	if kept.identity != (domain.PeerIdentity{0xc2}) || !kept.lastV2.Equal(at) || kept.protection != nil {
		t.Fatalf("a dial renewed, re-owned or raised a non-canonical record: %+v", kept)
	}
	var head headSecureSessionFile
	raw, err := os.ReadFile(path) //nolint:gosec // the file this test's store wrote
	if err != nil || json.Unmarshal(raw, &head) != nil {
		t.Fatalf("read back: %v", err)
	}
	if record, present := head.Endpoints[protectedRaw]; !present || !record.Protected || *record.ProtectedBy != giver {
		t.Fatalf("a write lost the protected non-canonical record: %+v", record)
	}
	if _, present := head.Endpoints[generalRaw]; !present {
		t.Fatal("a write lost the ordinary non-canonical record")
	}

	clock.now = at.Add(endpointBindingTTL + time.Minute)
	if store.endpointRequiresV2(generalRaw) {
		t.Fatal("an ordinary non-canonical record outlived its 30 days")
	}
	if !store.endpointRequiresV2(protectedRaw) {
		t.Fatal("a protected non-canonical record expired")
	}
	if err := store.flushRenewalsNow(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := store.stats().NonCanonicalBindings; got != 1 {
		t.Fatalf("after the sweep %d non-canonical records, want only the protected one", got)
	}
}

// An address that is a numeric alias of an IP address, or an alias of an
// onion service (a subdomain of a v3 name, a v2 name), cannot be keyed, and a
// binding on the host it names would not be found under it — so v1 is never
// followed there: the endpoint always requires v2. Other addresses without a
// canonical form require v2 only where a kept record says so.
func TestAnAliasOfABoundableHostAlwaysRequiresV2(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	for _, alias := range []domain.PeerAddress{
		"127.1:64646", "127.0.1:64646", "0x7f000001:64646", "2130706433:64646", "198.51.100.1.5:64646", "01.2.3.4:64646", "example.0x1f:64646",
		domain.PeerAddress("www." + testOnionV3 + ".onion:64646"),
		"abcdefghijklmnop.onion:64646",
	} {
		if !store.endpointRequiresV2(alias) {
			t.Errorf("%q, an alias of another host, lets v1 through", alias)
		}
	}
	for _, other := range []domain.PeerAddress{"bücher.de:64646", "foo_bar.example:64646", "198.51.100.1:64646", "node.0x.org:64646", "0xnode.example.com:64646"} {
		if store.endpointRequiresV2(other) {
			t.Errorf("%q requires v2 with nothing bound", other)
		}
	}
}

// One name dial is one creation against its group's quota, whether it binds
// the name alone or the name and its derived key — across a restart too —
// and a refusal of the pair is counted once.
func TestADialByNameSpendsOnePlaceOfTheQuota(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	for i := range generalCreationsPerGroupPerDay {
		if i == generalCreationsPerGroupPerDay/2 {
			store = loadSecureSessionStore(path, clock.trusted())
		}
		dial := dialledReaching(domain.PeerAddress(fmt.Sprintf("n%d.example.com:64646", i)), fmt.Sprintf("198.51.100.%d:64646", i+1))
		result, _ := store.noteProvenOutbound(dial, boundIdentity(i), endpointProtectionNone())
		if result.tier != bindingTierGeneral || result.derived == nil || result.derived.result.tier != bindingTierGeneral {
			t.Fatalf("name dial #%d: tier %v, derived %+v; want both bound", i, result.tier, result.derived)
		}
	}
	over := dialledReaching("over.example.com:64646", "198.51.200.1:64646")
	if result, _ := store.noteProvenOutbound(over, domain.PeerIdentity{0x74}, endpointProtectionNone()); result.tier != bindingTierNone {
		t.Fatalf("a name dial past the quota: tier %v", result.tier)
	}
	if got := store.stats().GeneralRefusalsSourceGroupQuota; got != 1 {
		t.Fatalf("one refused name dial counted %d refusals, want 1", got)
	}
}

// A binding created ordinary and raised to protected within the day keeps the
// place of the quota it took, across a restart too.
func TestABindingRaisedWithinTheDayKeepsItsQuotaPlaceAcrossARestart(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	for i := range generalCreationsPerGroupPerDay {
		if _, err := store.noteProvenOutbound(dialledAt(domain.PeerAddress(fmt.Sprintf("198.51.100.%d:64646", i+1))), boundIdentity(i), endpointProtectionNone()); err != nil {
			t.Fatalf("note #%d: %v", i, err)
		}
	}
	if result, _ := store.noteProvenOutbound(dialledAt("198.51.100.1:64646"), boundIdentity(0), endpointProtectionWanted(domain.ProtectedBindingPoolContact)); result.tier != bindingTierProtected {
		t.Fatalf("the raise: tier %v", result.tier)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	if result, _ := restarted.noteProvenOutbound(dialledAt("198.51.200.1:64646"), domain.PeerIdentity{0x74}, endpointProtectionNone()); result.tier != bindingTierNone {
		t.Fatalf("a restart gave back the quota place of a raised binding: tier %v", result.tier)
	}
}

// The file this build writes keeps format 1 and adds only optional fields:
// the previous build's decoder reads every binding back.
func TestTheNewStoreFileIsReadByThePreviousBuild(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	contact := domain.PeerIdentity{0xd1}
	if _, err := store.noteProvenOutbound(dialledReaching("node.example.com:64646", "198.51.100.20:64646"), contact, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); err != nil {
		t.Fatalf("note: %v", err)
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the file this test's store wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var head headSecureSessionFile
	if err := json.Unmarshal(raw, &head); err != nil {
		t.Fatalf("the previous build cannot decode the new file: %v", err)
	}
	if head.Format != 1 || len(head.Endpoints) != 2 {
		t.Fatalf("format %d, %d endpoints; want 1 and 2", head.Format, len(head.Endpoints))
	}
	named, derived := head.Endpoints["node.example.com:64646"], head.Endpoints["198.51.100.20:64646"]
	if named.Identity != contact || !named.Protected || derived.Identity != contact || derived.Protected {
		t.Fatalf("as the previous build reads it: name %+v, derived %+v", named, derived)
	}
	var fields struct {
		Endpoints map[string]map[string]any `json:"endpoints"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode: %v", err)
	}
	record := fields.Endpoints["198.51.100.20:64646"]
	if record["transport"] != "tcp" || record["source_group"] != "198.51.0.0/16" || record["first_at"] == nil || record["derived_from"] != "node.example.com:64646" {
		t.Fatalf("the derived record = %v; want transport, source_group, first_at and derived_from", record)
	}
}

// A record whose transport contradicts its address cannot be trusted: the
// file is unreadable and every peer is protected.
func TestARecordWhoseTransportContradictsItsAddressFailsClosed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	raw := `{"format":1,"identities":{},"endpoints":{"198.51.100.1:64646":{"identity":"` + domain.PeerIdentity{0xe1}.String() +
		`","last_v2":"2026-05-28T21:46:40Z","transport":"tor","source_group":"198.51.0.0/16"}}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if store := loadSecureSessionStore(path, newStoreClock().trusted()); !store.unreadable {
		t.Fatal("a record with a contradictory transport was accepted")
	}
}

// writeStoreRecords writes a store file of this build's records.
func writeStoreRecords(t *testing.T, path string, records map[domain.PeerAddress]endpointBindingRecord) {
	t.Helper()
	raw, err := json.Marshal(secureSessionFile{Format: secureSessionStoreFormat, Endpoints: records})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// A merge keeps the earliest creation time of the records, and none when any
// record has none: such a record was written before creation times were kept,
// so it is older than every known one.
func TestAMergeKeepsTheEarliestCreationTime(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	earlier, later := clock.now.Add(-3*time.Hour).UTC(), clock.now.Add(-2*time.Hour).UTC()
	lastV2 := clock.now.Add(-time.Hour).UTC()
	cases := map[string]struct {
		first, second *time.Time
		want          *time.Time
	}{
		"both known":  {first: &later, second: &earlier, want: &earlier},
		"one unknown": {first: &earlier, second: nil, want: nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
			writeStoreRecords(t, path, map[domain.PeerAddress]endpointBindingRecord{
				"198.51.100.1:64646":          {Identity: domain.PeerIdentity{0xd1}, LastV2: lastV2, FirstAt: tc.first},
				"[::ffff:198.51.100.1]:64646": {Identity: domain.PeerIdentity{0xd2}, LastV2: lastV2, FirstAt: tc.second},
			})
			for range 10 {
				binding, bound := bindingAt(t, loadSecureSessionStore(path, clock.trusted()), "198.51.100.1:64646")
				if !bound || (binding.firstAt == nil) != (tc.want == nil) || (tc.want != nil && !binding.firstAt.Equal(*tc.want)) {
					t.Fatalf("merged creation time %v, want %v", binding.firstAt, tc.want)
				}
			}
		})
	}
}

// A record kept under an address without a canonical form is checked like any
// other, except for what cannot be compared with its address: an unknown
// transport, or a derived_from that is not an endpoint or sits on a
// protected binding, makes the file unreadable; a valid derived_from is kept
// and written back.
func TestARecordWithoutACanonicalAddressIsCheckedLikeAnyOther(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	at := clock.now.Add(-time.Hour).UTC()
	stored := domain.PeerAddress("bücher.de:64646")
	udp, tcp := domain.EndpointTransport("udp"), domain.EndpointTransportTCP
	name, notAnEndpoint := domain.PeerAddress("node.example.com:64646"), domain.PeerAddress("node.example.com")
	giver, pool := domain.PeerIdentity{0xe1}, domain.ProtectedBindingPoolContact
	for label, record := range map[string]endpointBindingRecord{
		"unknown transport":            {Identity: giver, LastV2: at, Transport: &udp},
		"derived_from not an endpoint": {Identity: giver, LastV2: at, DerivedFrom: &notAnEndpoint},
		"derived_from on protected":    {Identity: giver, LastV2: at, Protected: true, ProtectedBy: &giver, Pool: &pool, DerivedFrom: &name},
	} {
		path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
		writeStoreRecords(t, path, map[domain.PeerAddress]endpointBindingRecord{stored: record})
		if !loadSecureSessionStore(path, clock.trusted()).unreadable {
			t.Errorf("%s: the record was accepted", label)
		}
	}
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	writeStoreRecords(t, path, map[domain.PeerAddress]endpointBindingRecord{stored: {Identity: giver, LastV2: at, Transport: &tcp, DerivedFrom: &name}})
	store := loadSecureSessionStore(path, clock.trusted())
	if store.unreadable {
		t.Fatal("a valid record without a canonical address was refused")
	}
	store.mu.Lock()
	store.changes++
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the file this test's store wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var file secureSessionFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back := file.Endpoints[stored]; back.DerivedFrom == nil || *back.DerivedFrom != name {
		t.Fatalf("derived_from was not written back: %+v", back)
	}
}
