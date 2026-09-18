package service

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
)

// identity_search_test.go covers what replaced the identity list in the
// status snapshot: the node answers a fragment on demand, the router caches
// one answer, and the discovery counter is what makes a stale answer visible.

// newSearchRouter builds a router over a real embedded node, because the
// search is a question asked THROUGH the node — a fake would only assert
// that the router can talk to itself.
// testSearchAddress builds a 40-hex address with a known prefix, so a
// fragment in the test means what it says — domaintest.ID hashes its label
// and would make "aa" match nothing.
func testSearchAddress(prefix string, n int) string {
	return prefix + fmt.Sprintf("%0*x", 40-len(prefix), n)
}

func newSearchRouter(t *testing.T) (*DMRouter, *node.Service, *testStatusProvider) {
	t.Helper()
	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := node.NewService(config.Node{
		ListenAddress:  ":0",
		TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
	}, id, ebus.New())
	t.Cleanup(func() { svc.WaitBackground() })

	info := NewAppInfo(config.App{Version: "test"}, config.Node{}, id)
	client := &DesktopClient{id: id, appCfg: config.App{Version: "test"}, localNode: svc, info: info}
	client.rpc = NewLocalRPCClient(info, svc)
	provider := &testStatusProvider{}
	router := &DMRouter{
		client:        client,
		statusMonitor: provider,
		peers:         make(map[domain.PeerIdentity]*RouterPeerState),
	}
	return router, svc, provider
}

func TestSearchIdentitiesAsksTheNode(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)

	wanted := testSearchAddress("aa", 1)
	other := testSearchAddress("bb", 2)
	svc.SeedKnownIdentitiesForTest(wanted, other)

	results, err := router.SearchIdentities("aa")
	if err != nil {
		t.Fatalf("SearchIdentities: %v", err)
	}
	if len(results) != 1 || results[0] != domain.PeerIdentityFromWire(wanted) {
		t.Fatalf("results = %v, want exactly the matching identity", results)
	}

	// An empty fragment is not a request to enumerate everything.
	if results, err := router.SearchIdentities("   "); err != nil || results != nil {
		t.Fatalf("empty fragment: results=%v err=%v, want nil/nil", results, err)
	}

	// Nothing matches: an answer, not an error.
	if results, err := router.SearchIdentities("zzzzzz"); err != nil || len(results) != 0 {
		t.Fatalf("no-match fragment: results=%v err=%v", results, err)
	}
}

// Nothing between the window and the node caches an answer, so an identity
// the node learns through ANY path is found by the next question — the
// failure this replaced was a cached empty answer that only a discovery
// event could retire, and one of the node's insert paths did not publish
// one.
func TestSearchIdentitiesHoldsNoStaleAnswer(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)

	first := testSearchAddress("aa", 1)
	svc.SeedKnownIdentitiesForTest(first)
	if results, err := router.SearchIdentities("aa"); err != nil || len(results) != 1 {
		t.Fatalf("first search: results=%v err=%v", results, err)
	}

	// The node learns a second match. No event, no version bump, no new
	// query: asking the same question again must still see it.
	second := testSearchAddress("aa", 2)
	svc.SeedKnownIdentitiesForTest(second)
	results, err := router.SearchIdentities("aa")
	if err != nil {
		t.Fatalf("second search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %v, want both — an answer may not outlive what the node knows", results)
	}
}

// The node owns the ceiling: a router asking for its own limit still gets
// no more than the node is willing to enumerate.
func TestSearchIdentitiesIsCappedByTheNode(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)

	addresses := make([]string, 0, identitySearchRows*8)
	for i := range identitySearchRows * 8 {
		addresses = append(addresses, testSearchAddress("cc", i))
	}
	svc.SeedKnownIdentitiesForTest(addresses...)

	results, err := router.SearchIdentities("")
	if err != nil || results != nil {
		t.Fatalf("empty fragment must not enumerate: results=%d err=%v", len(results), err)
	}
	results, err = router.SearchIdentities("cc")
	if err != nil {
		t.Fatalf("SearchIdentities: %v", err)
	}
	if len(results) > identitySearchRows {
		t.Fatalf("results = %d, want at most %d", len(results), identitySearchRows)
	}
}

// The monitor no longer carries a list, so an identity burst costs a
// counter increment per event and nothing proportional to what the node
// knows.
func TestIdentityAddedAdvancesTheVersionOnly(t *testing.T) {
	t.Parallel()
	bus := ebus.New()
	domains := make(chan NodeStatusDomain, 64)
	monitor := NewNodeStatusMonitor(NodeStatusMonitorOpts{
		EventBus:         bus,
		OnChanged:        func() {},
		OnPartialChanged: func(d NodeStatusDomain) { domains <- d },
	})
	monitor.Start()

	const discoveries = 16
	for i := range discoveries {
		ebus.PublishIdentityAdded(bus, domaintest.ID("burst-"+string(rune('a'+i))))
	}

	seen := 0
	for seen < discoveries {
		if d := <-domains; d == NodeStatusDomainKnownIDs {
			seen++
		}
	}
	if version := monitor.KnownIDsVersion(); version != discoveries {
		t.Fatalf("KnownIDsVersion = %d after %d discoveries", version, discoveries)
	}

	status := monitor.NodeStatus()
	if status.KnownIDsVersion != discoveries {
		t.Fatalf("status version = %d, want %d", status.KnownIDsVersion, discoveries)
	}
}

// The cap must not hide a match behind the rows the caller was going to
// throw away. Existing conversations are excluded by the node, in the same
// walk, so the answer is the smallest matches the caller can USE — not the
// smallest matches full stop, filtered afterwards into nothing.
func TestSearchIdentitiesLooksPastExcludedMatches(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)

	// Far more existing conversations than any limit or page budget: the
	// old shapes (cap, then filter; page, then give up) both reported
	// nothing here.
	const listedCount = 512
	listed := make([]string, 0, listedCount)
	for i := range listedCount {
		address := testSearchAddress("aa", i)
		listed = append(listed, address)
		router.peers[domain.PeerIdentityFromWire(address)] = &RouterPeerState{}
	}
	wanted := testSearchAddress("aa", listedCount+1)
	svc.SeedKnownIdentitiesForTest(append(listed, wanted)...)

	results, err := router.SearchIdentities("aa")
	if err != nil {
		t.Fatalf("SearchIdentities: %v", err)
	}
	if len(results) != 1 || results[0] != domain.PeerIdentityFromWire(wanted) {
		t.Fatalf("results = %v, want the one identity the user is not already talking to", results)
	}
}

// The node's own address is excluded on the same side as the conversations,
// so it never takes one of the rows either.
func TestSearchIdentitiesExcludesSelf(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)

	self := router.client.Address()
	if self.IsZero() {
		t.Fatal("test setup: the client has no address")
	}
	other := testSearchAddress(self.String()[:2], 7)
	svc.SeedKnownIdentitiesForTest(self.String(), other)

	results, err := router.SearchIdentities(self.String()[:2])
	if err != nil {
		t.Fatalf("SearchIdentities: %v", err)
	}
	for _, result := range results {
		if result == self {
			t.Fatalf("the node offered its own address: %v", results)
		}
	}
}

// The ordinary case still answers with everything that matches.
func TestSearchIdentitiesReturnsEveryUsableMatch(t *testing.T) {
	t.Parallel()
	router, svc, _ := newSearchRouter(t)
	svc.SeedKnownIdentitiesForTest(testSearchAddress("aa", 1), testSearchAddress("aa", 2))

	results, err := router.SearchIdentities("aa")
	if err != nil {
		t.Fatalf("SearchIdentities: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %v, want both matches", results)
	}
}
