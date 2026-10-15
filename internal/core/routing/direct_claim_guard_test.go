package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// directGuardForeignSeqNo is far above every SeqNo the session lifecycle
// gives a Direct claim in these tests, so the foreign write would win any
// SeqNo comparison against it.
const directGuardForeignSeqNo = 1 << 40

// The (peer, peer) Direct claim belongs to the session lifecycle:
// AddDirectPeer writes it, RemoveDirectPeer withdraws it. A claim through
// the same uplink from any other source — an announcement, a hop_ack-sourced
// update, a bad-hops sample — must not replace it, whatever its SeqNo: the
// replacement would be invalidated silently on disconnect (only Direct
// claims produce our own wire withdrawal) and would age out by TTL while
// the session is still up.
func TestUpdateRoute_NonDirectSourceCannotReplaceDirectClaim(t *testing.T) {
	cases := []struct {
		name   string
		source RouteSource
		hops   int
	}{
		{name: "announcement", source: RouteSourceAnnouncement, hops: 1},
		{name: "announcement over max sane hops", source: RouteSourceAnnouncement, hops: 12},
		{name: "announcement at hops infinity", source: RouteSourceAnnouncement, hops: HopsInfinity},
		{name: "hop_ack", source: RouteSourceHopAck, hops: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, peer := newDirectGuardTable(t)
			want := tbl.InspectUplinkClaim(peer, peer)

			status, err := tbl.UpdateRoute(RouteEntry{
				Identity: peer,
				Origin:   peer,
				NextHop:  peer,
				Hops:     tc.hops,
				SeqNo:    directGuardForeignSeqNo,
				Source:   tc.source,
			})
			if err != nil {
				t.Fatalf("UpdateRoute: %v", err)
			}
			if status != RouteRejected {
				t.Fatalf("UpdateRoute over a live Direct claim = %v, want RouteRejected", status)
			}
			assertDirectClaimUnchanged(t, tbl, peer, *want)
		})
	}
}

// A wire withdrawal (WithdrawRoute) or a poison (InvalidateUplinkClaim)
// through the uplink the Direct claim names would tombstone it, and a
// withdrawn Direct claim is re-emitted to every other neighbour as our own
// withdrawal of a peer that is still connected.
func TestWithdrawalsCannotTombstoneDirectClaim(t *testing.T) {
	cases := []struct {
		name     string
		withdraw func(tbl *Table, peer PeerIdentity) bool
	}{
		{name: "WithdrawRoute", withdraw: func(tbl *Table, peer PeerIdentity) bool {
			return tbl.WithdrawRoute(peer, peer, directGuardForeignSeqNo)
		}},
		{name: "InvalidateUplinkClaim", withdraw: func(tbl *Table, peer PeerIdentity) bool {
			return tbl.InvalidateUplinkClaim(peer, peer)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, peer := newDirectGuardTable(t)
			want := tbl.InspectUplinkClaim(peer, peer)

			if tc.withdraw(tbl, peer) {
				t.Fatal("a withdrawal through the uplink tombstoned the live Direct claim")
			}
			assertDirectClaimUnchanged(t, tbl, peer, *want)
		})
	}
}

// The tombstone RemoveDirectPeer leaves behind is ours too: it carries the
// SeqNo of the withdrawal we emitted, and AnnounceTo keeps re-emitting it as
// our own withdrawal. A foreign write must not move that SeqNo, and the next
// session must still reactivate the slot as a Direct claim.
func TestForeignWritesCannotRewriteDirectTombstone(t *testing.T) {
	tbl, peer := newDirectGuardTable(t)
	if _, err := tbl.RemoveDirectPeer(peer); err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}
	tombstone := *tbl.InspectUplinkClaim(peer, peer)

	if tbl.WithdrawRoute(peer, peer, directGuardForeignSeqNo) {
		t.Fatal("WithdrawRoute rewrote our own Direct tombstone")
	}
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: peer,
		Origin:   peer,
		NextHop:  peer,
		Hops:     1,
		SeqNo:    directGuardForeignSeqNo,
		Source:   RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("UpdateRoute over our own Direct tombstone = %v, want RouteRejected", status)
	}
	got := tbl.InspectUplinkClaim(peer, peer)
	if got.Source != RouteSourceDirect || !got.IsWithdrawn() || got.SeqNo != tombstone.SeqNo {
		t.Fatalf("Direct tombstone = {Source:%s Withdrawn:%v SeqNo:%d}, want {Source:direct Withdrawn:true SeqNo:%d}",
			got.Source, got.IsWithdrawn(), got.SeqNo, tombstone.SeqNo)
	}

	if _, err := tbl.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer after disconnect: %v", err)
	}
	reactivated := tbl.InspectUplinkClaim(peer, peer)
	if reactivated.Source != RouteSourceDirect || reactivated.IsWithdrawn() || reactivated.SeqNo <= tombstone.SeqNo {
		t.Fatalf("reconnect must reactivate the Direct claim above the tombstone SeqNo %d, got %+v", tombstone.SeqNo, *reactivated)
	}
}

// newDirectGuardTable returns a table with one live Direct claim to the
// returned peer and a sane-hops ceiling, so the fast-invalidation branch is
// armed for the bad-hops case.
func newDirectGuardTable(t *testing.T) (*Table, PeerIdentity) {
	t.Helper()
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithMaxSaneHops(8))
	peer := domaintest.ID("peer")
	if _, err := tbl.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	return tbl, peer
}

// assertDirectClaimUnchanged checks the stored (peer, peer) claim, that it is
// still advertised live to another neighbour, and that the disconnect still
// produces our own wire withdrawal for it.
func assertDirectClaimUnchanged(t *testing.T, tbl *Table, peer PeerIdentity, want RouteEntry) {
	t.Helper()
	got := tbl.InspectUplinkClaim(peer, peer)
	if got == nil {
		t.Fatal("the Direct claim disappeared")
	}
	if got.Source != RouteSourceDirect || got.IsWithdrawn() || got.Hops != want.Hops || got.SeqNo != want.SeqNo || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("Direct claim = %+v, want %+v", *got, want)
	}

	advertised := false
	for _, entry := range tbl.AnnounceTo(domaintest.ID("other-neighbour")) {
		if entry.Identity == peer {
			advertised = entry.Hops == 1
		}
	}
	if !advertised {
		t.Fatal("the Direct claim is no longer advertised live to another neighbour")
	}

	removed, err := tbl.RemoveDirectPeer(peer)
	if err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}
	if len(removed.Withdrawals) != 1 || removed.Withdrawals[0].Identity != peer {
		t.Fatalf("disconnect must emit our own withdrawal for the peer, got %+v", removed.Withdrawals)
	}
}
