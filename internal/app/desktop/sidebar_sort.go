package desktop

import (
	"sort"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// sidebarPeerTier returns the sort tier for a peer based on reachability
// and unread status. Lower tier = higher priority in the sidebar.
//
//	0 — online + unread messages
//	1 — online, no unread
//	2 — offline + unread messages
//	3 — offline, no unread
//
// "Online" here is the SAME answer the avatar draws, and it has to be: a
// contact sorted into the online group while their avatar is grey is a
// contradiction the user has no way to resolve. It therefore reads the presence
// projection through contactPresence rather than consulting ReachableIDs on its
// own — a stale route used to put a departed contact at the top of the list
// under a grey dot.
//
// Both online states count as online, the proven one and the inferred one: the
// dotted ring says how much the claim is worth, and that is a question about
// this contact, not about where they belong in a list.
//
// When the node has answered nothing yet, every peer is treated as offline
// (tiers 2/3), so the sort degrades to a 2-tier unread/read order.
func sidebarPeerTier(peer domain.PeerIdentity, snap service.RouterSnapshot) int {
	presence := contactPresence(snap.NodeStatus, peer)
	online := presence == contactPresenceOnline || presence == contactPresenceOnlineInferred
	hasUnread := false
	if ps, ok := snap.Peers[peer]; ok {
		hasUnread = ps.Unread > 0
	}

	switch {
	case online && hasUnread:
		return 0
	case online:
		return 1
	case hasUnread:
		return 2
	default:
		return 3
	}
}

// sortSidebarPeers sorts the peer list using 4-tier priority ordering.
// This is a UI/product concern — the router provides data, and the
// presentation layer decides display order.
//
// Tiers:
//  1. Online + unread — by unread count descending
//  2. Online, no unread — by most recent activity
//  3. Offline + unread — by unread count descending
//  4. Offline, no unread — by most recent activity
//
// "Most recent activity" is the router's own PeerOrder, which the input
// already carries (mergeRecipientOrder) and SliceStable preserves: the router
// moves a conversation to the front of that list when a message is applied to
// it, so the order is the order things happened HERE. It deliberately is not
// the preview's timestamp — that is the time the sender's clock printed, and a
// peer whose clock runs fast could otherwise hold the top of the sidebar for
// as long as it kept sending, while one running slow sank below conversations
// nothing had happened in.
func sortSidebarPeers(peers []domain.PeerIdentity, snap service.RouterSnapshot) {
	sort.SliceStable(peers, func(i, j int) bool {
		pi, pj := peers[i], peers[j]
		ti, tj := sidebarPeerTier(pi, snap), sidebarPeerTier(pj, snap)
		if ti != tj {
			return ti < tj
		}

		psi, oki := snap.Peers[pi]
		psj, okj := snap.Peers[pj]
		if !oki || !okj {
			return oki
		}

		// Within unread tiers (0, 2): higher unread count first.
		if ti == 0 || ti == 2 {
			if psi.Unread != psj.Unread {
				return psi.Unread > psj.Unread
			}
		}

		// Same tier, same unread count: keep the input order, which is the
		// router's most-recent-activity order.
		return false
	})
}
