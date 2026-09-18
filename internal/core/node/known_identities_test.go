package node

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// known_identities_test.go pins the bounded-set contract that replaced the
// unbounded s.known map: dedup-aware Add, membership, snapshot, and LRU
// eviction at capacity (the memory bound) with recency promotion on re-Add.

func TestBoundedKnownIdentities_AddReportsNovelty(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	if !b.Add("a") {
		t.Fatal("first Add must report novel (true)")
	}
	if b.Add("a") {
		t.Fatal("duplicate Add must report not-novel (false)")
	}
	if !b.Has("a") {
		t.Fatal("Has must report a present member")
	}
	if b.Has("missing") {
		t.Fatal("Has must report absent for an unknown key")
	}
}

func TestBoundedKnownIdentities_Snapshot(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	b.Add("a")
	b.Add("b")
	b.Add("a") // duplicate, no effect
	snap := b.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot must contain the 2 distinct members, got %d", len(snap))
	}
	seen := map[string]bool{}
	for _, s := range snap {
		seen[s] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("snapshot must contain both a and b, got %v", snap)
	}
}

func TestBoundedKnownIdentities_EvictsLeastRecentlyUsedAtCapacity(t *testing.T) {
	b := newBoundedKnownIdentities(3)
	b.Add("1")
	b.Add("2")
	b.Add("3")
	// At capacity. Adding a 4th evicts the least-recently-used ("1").
	b.Add("4")
	if b.Len() != 3 {
		t.Fatalf("membership must never exceed capacity, got %d", b.Len())
	}
	if b.Has("1") {
		t.Fatal("least-recently-used entry must be evicted at capacity")
	}
	for _, k := range []string{"2", "3", "4"} {
		if !b.Has(k) {
			t.Fatalf("newer entry %q must survive eviction", k)
		}
	}
}

func TestBoundedKnownIdentities_ReAddPromotesRecency(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Add("1")
	b.Add("2")
	// Touch "1": re-Add promotes it to most-recently-used, so "2" is now the
	// least-recently-used and must be the one evicted next.
	if b.Add("1") {
		t.Fatal("re-Add of a present key must report not-novel (false)")
	}
	b.Add("3") // capacity 2 → evict LRU, which is now "2", not "1"
	if !b.Has("1") {
		t.Fatal("a recently re-observed identity must NOT be evicted")
	}
	if b.Has("2") {
		t.Fatal("the least-recently-used identity (2) must be evicted")
	}
	if !b.Has("3") {
		t.Fatal("the newest identity must be present")
	}
}

func TestBoundedKnownIdentities_ReAddAfterEviction(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Add("1")
	b.Add("2")
	b.Add("3") // evicts "1"
	if b.Has("1") {
		t.Fatal("1 should have been evicted")
	}
	if !b.Add("1") {
		t.Fatal("re-adding an evicted key must report novel again")
	}
	if b.Len() != 2 {
		t.Fatalf("membership must stay bounded after re-add, got %d", b.Len())
	}
}

func TestBoundedKnownIdentities_StaysBoundedUnderChurn(t *testing.T) {
	const cap = 100
	b := newBoundedKnownIdentities(cap)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("id-%d", i))
		if b.Len() > cap {
			t.Fatalf("membership exceeded capacity during churn: %d", b.Len())
		}
	}
	if b.Len() != cap {
		t.Fatalf("after sustained churn the set must sit at capacity, got %d", b.Len())
	}
	// Oldest must be gone, newest must be present.
	if b.Has("id-0") {
		t.Fatal("oldest churned key must have been evicted")
	}
	if !b.Has("id-9999") {
		t.Fatal("most-recent key must be present")
	}
}

func TestBoundedKnownIdentities_PinnedNeverEvicted(t *testing.T) {
	b := newBoundedKnownIdentities(3)
	b.Pin("contact")
	for i := 0; i < 100; i++ {
		b.Add(fmt.Sprintf("transit-%d", i))
	}
	if !b.Has("contact") {
		t.Fatal("pinned member must survive sustained churn")
	}
	if b.Len() != 3 {
		t.Fatalf("membership must stay at capacity, got %d", b.Len())
	}
}

func TestBoundedKnownIdentities_OnEvictCascades(t *testing.T) {
	keys := map[string]string{}
	b := newBoundedKnownIdentities(2)
	b.onEvict = func(address string) { delete(keys, address) }

	b.Add("a")
	keys["a"] = "key-a"
	b.Add("b")
	keys["b"] = "key-b"
	b.Add("c") // evicts "a"
	keys["c"] = "key-c"

	if _, ok := keys["a"]; ok {
		t.Fatal("onEvict must remove the evicted member's key-map entry")
	}
	if _, ok := keys["b"]; !ok {
		t.Fatal("surviving member's key-map entry must remain")
	}
}

func TestBoundedKnownIdentities_PinToleratesOverCapacity(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Pin("p1")
	b.Pin("p2")
	b.Pin("p3") // pinned membership may exceed capacity
	if !b.Has("p1") || !b.Has("p2") || !b.Has("p3") {
		t.Fatal("all pinned members must be present even above capacity")
	}
}

func TestBoundedKnownIdentities_UnpinRestoresEvictability(t *testing.T) {
	b := newBoundedKnownIdentities(3)
	b.Pin("contact")
	b.Unpin("contact")
	for i := 0; i < 100; i++ {
		b.Add(fmt.Sprintf("transit-%d", i))
	}
	if b.Has("contact") {
		t.Fatal("unpinned member must become evictable again")
	}
	if b.Len() != 3 {
		t.Fatalf("membership must stay at capacity after unpin, got %d", b.Len())
	}
	// Re-pinning after unpin must protect again (trust re-granted).
	b.Pin("contact2")
	for i := 0; i < 100; i++ {
		b.Add(fmt.Sprintf("churn-%d", i))
	}
	if !b.Has("contact2") {
		t.Fatal("re-pinned member must survive churn")
	}
}

func TestBoundedKnownIdentities_UnpinDrainsOverCapacity(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Pin("p1")
	b.Pin("p2")
	b.Pin("p3") // membership 3 > capacity 2, all pinned
	b.Unpin("p1")
	if b.Len() != 2 {
		t.Fatalf("unpin must drain membership back to capacity, got %d", b.Len())
	}
	if b.Has("p1") {
		t.Fatal("the only unpinned member must be the one drained")
	}
	if !b.Has("p2") || !b.Has("p3") {
		t.Fatal("pinned members must survive the drain")
	}

	// Unpin with everything else still pinned above capacity must not spin:
	// p2/p3 pinned, capacity 1.
	b2 := newBoundedKnownIdentities(1)
	b2.Pin("q1")
	b2.Pin("q2")
	b2.Pin("q3")
	b2.Unpin("q2")
	if b2.Len() != 2 {
		t.Fatalf("drain must stop once only pinned members remain over capacity, got %d", b2.Len())
	}
	if b2.Has("q2") {
		t.Fatal("q2 must have been drained")
	}
}

// SearchByFragment is what replaced handing the whole set to the UI: it
// answers a fragment with the smallest matching addresses, capped, and
// allocates nothing proportional to the set.
func TestBoundedKnownIdentities_SearchByFragment(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	for _, address := range []string{"aa11", "bb22", "ab33", "CC44", "ac55"} {
		b.Add(address)
	}

	got := b.SearchByFragment("a", nil, 10)
	want := []string{"aa11", "ab33", "ac55"}
	if len(got) != len(want) {
		t.Fatalf("matches = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("matches = %v, want %v (smallest address first)", got, want)
		}
	}

	// Case-insensitive against what the operator typed, both ways round.
	if matches := b.SearchByFragment("cc", nil, 10); len(matches) != 1 || matches[0] != "CC44" {
		t.Fatalf("lowercase fragment against an uppercase address = %v", matches)
	}
	if matches := b.SearchByFragment("B2", nil, 10); len(matches) != 1 || matches[0] != "bb22" {
		t.Fatalf("uppercase fragment against a lowercase address = %v", matches)
	}

	// A fragment nobody matches, and a fragment everybody matches.
	if matches := b.SearchByFragment("zz", nil, 10); len(matches) != 0 {
		t.Fatalf("no-match fragment = %v, want empty", matches)
	}
	if matches := b.SearchByFragment("", nil, 3); len(matches) != 3 || matches[0] != "CC44" {
		t.Fatalf("empty fragment = %v, want the first 3 by address", matches)
	}
	if matches := b.SearchByFragment("a", nil, 0); matches != nil {
		t.Fatalf("limit 0 = %v, want nil", matches)
	}
}

// The cap is the point: a fragment that matches most of a large set must
// still answer in a fixed amount of memory, and must answer with the same
// entries a full sort would have put first.
func TestBoundedKnownIdentities_SearchIsCappedAndDeterministic(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	for i := range 5000 {
		b.Add(fmt.Sprintf("%06x-aa", i))
	}

	const limit = 8
	got := b.SearchByFragment("aa", nil, limit)
	if len(got) != limit {
		t.Fatalf("matches = %d, want the cap %d", len(got), limit)
	}
	for i := range limit {
		want := fmt.Sprintf("%06x-aa", i)
		if got[i] != want {
			t.Fatalf("match %d = %q, want %q — the cap must keep the smallest, not an arbitrary subset", i, got[i], want)
		}
	}

	// Repeating the query gives the same answer: map iteration order must
	// not reach the reader.
	for range 20 {
		again := b.SearchByFragment("aa", nil, limit)
		for i := range limit {
			if again[i] != got[i] {
				t.Fatalf("repeat query differs at %d: %q vs %q", i, again[i], got[i])
			}
		}
	}
}

// Eviction is the other half of the promise: what the node forgot, the
// search no longer offers — the failure the UI's own copy of the list had.
func TestBoundedKnownIdentities_SearchForgetsEvicted(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Add("aa01")
	b.Add("aa02")
	b.Add("aa03")

	matches := b.SearchByFragment("aa", nil, 10)
	if len(matches) != 2 {
		t.Fatalf("matches = %v, want the two survivors", matches)
	}
	for _, match := range matches {
		if match == "aa01" {
			t.Fatal("the evicted identity is still offered by the search")
		}
	}
}

// Every path that grows the known set must announce the discovery: the UI's
// address search is keyed by the counter those announcements drive, so a
// path that grows the set in silence leaves the window answering from an
// answer taken before it. The key-map chokepoints are such a path — they
// insert because a key must be reachable by the eviction hook.
func TestKeyMaterialImportAnnouncesTheDiscovery(t *testing.T) {
	t.Parallel()
	bus := ebus.New()
	discovered := make(chan domain.PeerIdentity, 8)
	bus.Subscribe(ebus.TopicIdentityAdded, func(identity domain.PeerIdentity) { discovered <- identity })

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, bus)
	t.Cleanup(svc.WaitBackground)

	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	for name, grow := range map[string]func(){
		"pub_key":  func() { svc.addKnownPubKey(owner.Address, "pk") },
		"box_key":  func() { svc.addKnownBoxKey(owner.Address, "bk") },
		"box_sig":  func() { svc.addKnownBoxSig(owner.Address, "bs") },
		"identity": func() { svc.addKnownIdentity(domain.PeerIdentityFromWire(owner.Address)) },
	} {
		// Each subtest starts from a node that has never seen the owner, so
		// every chokepoint gets to be the one that discovers it.
		svc.knowledgeMu.Lock()
		svc.known = newBoundedKnownIdentities(maxKnownIdentities)
		svc.knowledgeMu.Unlock()
		drain(discovered)

		grow()
		select {
		case got := <-discovered:
			if got != domain.PeerIdentityFromWire(owner.Address) {
				t.Fatalf("%s announced %s, want %s", name, got, owner.Address)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s grew the known set without announcing it", name)
		}

		// Already known: no second announcement, or every key refresh would
		// retire the window's search answer for nothing.
		grow()
		select {
		case got := <-discovered:
			t.Fatalf("%s announced %s again for an identity already in the set", name, got)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func drain(ch chan domain.PeerIdentity) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// The exclusions are applied inside the walk, so the limit bounds the
// ANSWER and not the search. A caller that already shows the smallest
// matches must still be told about the next one — however many of them
// there are, and whatever the limit is.
func TestBoundedKnownIdentities_SearchExcludesBeforeTheLimit(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	exclude := make(map[string]struct{}, 1000)
	for i := range 1000 {
		address := fmt.Sprintf("aa%04d", i)
		b.Add(address)
		exclude[address] = struct{}{}
	}
	wanted := "aa9999"
	b.Add(wanted)

	matches := b.SearchByFragment("aa", exclude, 4)
	if len(matches) != 1 || matches[0] != wanted {
		t.Fatalf("matches = %v, want only %q — a thousand excluded matches must not hide it", matches, wanted)
	}

	// Without the exclusions the same query fills the limit from the front.
	if matches := b.SearchByFragment("aa", nil, 4); len(matches) != 4 || matches[0] != "aa0000" {
		t.Fatalf("unfiltered matches = %v", matches)
	}
}
