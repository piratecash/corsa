package desktop

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

func makeSnap(peers map[domain.PeerIdentity]*service.RouterPeerState, reachable map[domain.PeerIdentity]bool) service.RouterSnapshot {
	return service.RouterSnapshot{
		Peers: peers,
		NodeStatus: service.NodeStatus{
			ReachableIDs: reachable,
		},
	}
}

// TestSidebarPeerTier verifies that sidebarPeerTier assigns the correct tier
// based on reachability and unread status.
func TestSidebarPeerTier(t *testing.T) {
	now := time.Now()
	snap := makeSnap(
		map[domain.PeerIdentity]*service.RouterPeerState{
			domaintest.ID("online-unread"):  {Unread: 3, Preview: service.ConversationPreview{Timestamp: now}},
			domaintest.ID("online-read"):    {Unread: 0, Preview: service.ConversationPreview{Timestamp: now}},
			domaintest.ID("offline-unread"): {Unread: 5, Preview: service.ConversationPreview{Timestamp: now}},
			domaintest.ID("offline-read"):   {Unread: 0, Preview: service.ConversationPreview{Timestamp: now}},
		},
		map[domain.PeerIdentity]bool{
			domaintest.ID("online-unread"): true,
			domaintest.ID("online-read"):   true,
		},
	)

	cases := []struct {
		peer domain.PeerIdentity
		tier int
	}{
		{domaintest.ID("online-unread"), 0},
		{domaintest.ID("online-read"), 1},
		{domaintest.ID("offline-unread"), 2},
		{domaintest.ID("offline-read"), 3},
	}
	for _, tc := range cases {
		got := sidebarPeerTier(tc.peer, snap)
		if got != tc.tier {
			t.Errorf("sidebarPeerTier(%q) = %d, want %d", tc.peer, got, tc.tier)
		}
	}
}

// TestSidebarPeerTierNilReachableIDs verifies that when ReachableIDs is nil
// (probe not yet completed), all peers are treated as offline (tiers 2/3).
func TestSidebarPeerTierNilReachableIDs(t *testing.T) {
	snap := makeSnap(
		map[domain.PeerIdentity]*service.RouterPeerState{
			domaintest.ID("peer-unread"): {Unread: 1},
			domaintest.ID("peer-read"):   {Unread: 0},
		},
		nil,
	)

	if tier := sidebarPeerTier(domaintest.ID("peer-unread"), snap); tier != 2 {
		t.Errorf("nil ReachableIDs: unread peer should be tier 2, got %d", tier)
	}
	if tier := sidebarPeerTier(domaintest.ID("peer-read"), snap); tier != 3 {
		t.Errorf("nil ReachableIDs: read peer should be tier 3, got %d", tier)
	}
}

// TestSortSidebarPeers verifies the full 4-tier sorting:
//
//  1. Online + unread (by unread count desc)
//  2. Online, no unread (most recent activity, i.e. input order)
//  3. Offline + unread (by unread count desc)
//  4. Offline, no unread (most recent activity, i.e. input order)
//
// The input arrives in the router's most-recent-activity order, and the sort
// groups it into tiers without disturbing that order inside a tier. The
// preview timestamps below deliberately contradict it: they are what the
// senders' clocks printed, and the sidebar does not rank by them.
func TestSortSidebarPeers(t *testing.T) {
	now := time.Now()
	snap := makeSnap(
		map[domain.PeerIdentity]*service.RouterPeerState{
			domaintest.ID("on-unread-5"):     {Unread: 5, Preview: service.ConversationPreview{Timestamp: now.Add(-1 * time.Hour)}},
			domaintest.ID("on-unread-2"):     {Unread: 2, Preview: service.ConversationPreview{Timestamp: now.Add(-2 * time.Hour)}},
			domaintest.ID("on-read-recent"):  {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-3 * time.Hour)}},
			domaintest.ID("on-read-old"):     {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-10 * time.Minute)}},
			domaintest.ID("off-unread-7"):    {Unread: 7, Preview: service.ConversationPreview{Timestamp: now.Add(-30 * time.Minute)}},
			domaintest.ID("off-unread-1"):    {Unread: 1, Preview: service.ConversationPreview{Timestamp: now.Add(-5 * time.Minute)}},
			domaintest.ID("off-read-recent"): {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-5 * time.Hour)}},
			domaintest.ID("off-read-old"):    {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-20 * time.Minute)}},
		},
		map[domain.PeerIdentity]bool{
			domaintest.ID("on-unread-5"):    true,
			domaintest.ID("on-unread-2"):    true,
			domaintest.ID("on-read-recent"): true,
			domaintest.ID("on-read-old"):    true,
		},
	)

	// Activity order: what happened most recently comes first, tiers mixed.
	peers := []domain.PeerIdentity{
		domaintest.ID("off-unread-1"), domaintest.ID("on-read-recent"), domaintest.ID("off-read-recent"), domaintest.ID("on-unread-5"),
		domaintest.ID("off-unread-7"), domaintest.ID("on-unread-2"), domaintest.ID("on-read-old"), domaintest.ID("off-read-old"),
	}

	sortSidebarPeers(peers, snap)

	expected := []domain.PeerIdentity{
		domaintest.ID("on-unread-5"),     // tier 0, unread 5
		domaintest.ID("on-unread-2"),     // tier 0, unread 2
		domaintest.ID("on-read-recent"),  // tier 1, more recent activity
		domaintest.ID("on-read-old"),     // tier 1, less recent activity
		domaintest.ID("off-unread-7"),    // tier 2, unread 7
		domaintest.ID("off-unread-1"),    // tier 2, unread 1
		domaintest.ID("off-read-recent"), // tier 3, more recent activity
		domaintest.ID("off-read-old"),    // tier 3, less recent activity
	}

	for i, want := range expected {
		if peers[i] != want {
			t.Errorf("peers[%d] = %q, want %q\nfull order: %v", i, peers[i], want, peers)
		}
	}
}

// TestSortSidebarPeersUnreadTiebreakByActivity verifies that peers with equal
// unread counts within the same tier keep the activity order they arrived in,
// and specifically that a larger preview timestamp does not overtake it: that
// timestamp belongs to the peer's own clock.
func TestSortSidebarPeersUnreadTiebreakByActivity(t *testing.T) {
	now := time.Now()
	snap := makeSnap(
		map[domain.PeerIdentity]*service.RouterPeerState{
			// "a" claims the newer time; "b" is the one something actually
			// happened in most recently.
			domaintest.ID("a"): {Unread: 3, Preview: service.ConversationPreview{Timestamp: now.Add(time.Hour)}},
			domaintest.ID("b"): {Unread: 3, Preview: service.ConversationPreview{Timestamp: now.Add(-1 * time.Minute)}},
		},
		map[domain.PeerIdentity]bool{
			domaintest.ID("a"): true,
			domaintest.ID("b"): true,
		},
	)

	peers := []domain.PeerIdentity{domaintest.ID("b"), domaintest.ID("a")}
	sortSidebarPeers(peers, snap)

	if peers[0] != domaintest.ID("b") || peers[1] != domaintest.ID("a") {
		t.Fatalf("expected [b, a] — the order things happened in — got %v", peers)
	}
}

// TestSortSidebarPeersReachabilityChange verifies that when a peer goes
// online, sorting moves it to the correct tier.
func TestSortSidebarPeersReachabilityChange(t *testing.T) {
	now := time.Now()
	peerState := map[domain.PeerIdentity]*service.RouterPeerState{
		domaintest.ID("was-offline"):   {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-1 * time.Minute)}},
		domaintest.ID("always-online"): {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-2 * time.Minute)}},
	}

	// Initially only "always-online" is reachable.
	snap := makeSnap(peerState, map[domain.PeerIdentity]bool{
		domaintest.ID("always-online"): true,
	})

	peers := []domain.PeerIdentity{domaintest.ID("was-offline"), domaintest.ID("always-online")}
	sortSidebarPeers(peers, snap)

	// always-online should be first (tier 1), was-offline second (tier 3).
	if peers[0] != domaintest.ID("always-online") {
		t.Fatalf("before: expected always-online first, got %v", peers)
	}

	// Simulate health poll — was-offline becomes reachable.
	snap.NodeStatus.ReachableIDs[domaintest.ID("was-offline")] = true
	peers = []domain.PeerIdentity{domaintest.ID("was-offline"), domaintest.ID("always-online")}
	sortSidebarPeers(peers, snap)

	// Both are tier 1 now, and was-offline is the one the router lists first
	// as the more recent activity.
	if peers[0] != domaintest.ID("was-offline") {
		t.Fatalf("after: expected was-offline first (more recent activity), got %v", peers)
	}
}

// TestSortSidebarPeersClearUnreadMovesToReadTier verifies that clearing
// unread count (e.g., opening a chat) immediately moves the peer to the
// correct tier on the next sort — no separate re-sort trigger needed.
func TestSortSidebarPeersClearUnreadMovesToReadTier(t *testing.T) {
	now := time.Now()
	peerState := map[domain.PeerIdentity]*service.RouterPeerState{
		domaintest.ID("peer-a"): {Unread: 5, Preview: service.ConversationPreview{Timestamp: now.Add(-1 * time.Hour)}},
		domaintest.ID("peer-b"): {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-30 * time.Minute)}},
	}
	snap := makeSnap(peerState, map[domain.PeerIdentity]bool{
		domaintest.ID("peer-a"): true,
		domaintest.ID("peer-b"): true,
	})

	peers := []domain.PeerIdentity{domaintest.ID("peer-a"), domaintest.ID("peer-b")}
	sortSidebarPeers(peers, snap)

	// peer-a is tier 0 (online+unread), peer-b is tier 1 (online, read).
	if peers[0] != domaintest.ID("peer-a") {
		t.Fatalf("before clear: expected peer-a first, got %v", peers)
	}

	// User opens peer-a's chat → unread cleared.
	peerState[domaintest.ID("peer-a")].Unread = 0

	// The router lists peer-b first: it is the conversation something
	// happened in most recently.
	peers = []domain.PeerIdentity{domaintest.ID("peer-b"), domaintest.ID("peer-a")}
	sortSidebarPeers(peers, snap)

	// Both are now tier 1 (online, read), so that order survives.
	if peers[0] != domaintest.ID("peer-b") {
		t.Fatalf("after clear: expected peer-b first (more recent activity), got %v", peers)
	}
}

// TestSortSidebarPeersNewMessageMovesPeerUp verifies that a new message moves
// its conversation to the top of its tier on the next sort, with no separate
// trigger — and that it does so because the ROUTER moved it to the front of
// the order it publishes, not because of anything in the preview's timestamp.
// The peer's clock is left an hour behind throughout to make that explicit.
func TestSortSidebarPeersNewMessageMovesPeerUp(t *testing.T) {
	now := time.Now()
	peerState := map[domain.PeerIdentity]*service.RouterPeerState{
		domaintest.ID("peer-quiet"): {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-2 * time.Hour)}},
		domaintest.ID("peer-busy"):  {Unread: 0, Preview: service.ConversationPreview{Timestamp: now.Add(-1 * time.Hour)}},
	}
	snap := makeSnap(peerState, nil) // all offline

	peers := []domain.PeerIdentity{domaintest.ID("peer-busy"), domaintest.ID("peer-quiet")}
	sortSidebarPeers(peers, snap)

	if peers[0] != domaintest.ID("peer-busy") {
		t.Fatalf("before the message: expected peer-busy first, got %v", peers)
	}

	// A message lands in peer-quiet's conversation: the router promotes it to
	// the front of PeerOrder, and its stamp — the sender's — stays behind.
	peerState[domaintest.ID("peer-quiet")].Preview.Body = "just arrived"

	peers = []domain.PeerIdentity{domaintest.ID("peer-quiet"), domaintest.ID("peer-busy")}
	sortSidebarPeers(peers, snap)

	if peers[0] != domaintest.ID("peer-quiet") {
		t.Fatalf("after the message: expected peer-quiet first, got %v", peers)
	}
}
