package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestAnnounceTargetForDirtyContract pins the dirty-marking contract of
// AnnounceTargetFor against the snapshot publisher's coalescing gate.
//
// A normal targeted emit only advances the per-receiver outbound-SeqNo
// watermark, which is internal to routeStore and NOT part of
// routing.Snapshot. Marking the table dirty for it would wake
// rebuildRoutingSnapshot once per second under steady route_query traffic
// and walk the table for a change no consumer can observe. So the normal
// path must leave the dirty flag clean — only the SeqNo flap-cap engage
// (which bumps the published SeqNoFlapHoldowns CapStats counter) may mark
// dirty, mirroring AnnounceTo / AnnounceProjectionFor.
func TestAnnounceTargetForDirtyContract(t *testing.T) {
	t.Run("normal_emit_does_not_mark_dirty", func(t *testing.T) {
		now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
		tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(func() time.Time { return now }))

		mustUpdate(t, tbl, RouteEntry{
			Identity: domaintest.ID("id-target"), Origin: domaintest.ID("self"), NextHop: domaintest.ID("id-uplink"),
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		// Clear the dirty bit the route insert legitimately set, so the
		// assertion below observes only what AnnounceTargetFor does.
		tbl.ConsumeDirty()

		entry, uplink, ok := tbl.AnnounceTargetFor(domaintest.ID("id-target"), domaintest.ID("id-requester"))
		if !ok {
			t.Fatal("AnnounceTargetFor returned ok=false; want a live winner")
		}
		if uplink != domaintest.ID("id-uplink") || entry.Identity != domaintest.ID("id-target") {
			t.Fatalf("AnnounceTargetFor winner = (%q, %q), want (id-target, id-uplink)", entry.Identity, uplink)
		}
		if tbl.IsDirty() {
			t.Fatal("normal AnnounceTargetFor emit marked the table dirty; it only advanced the " +
				"unpublished outbound-SeqNo watermark and must not wake the snapshot publisher")
		}
	})

	t.Run("seqno_flap_cap_engage_marks_dirty", func(t *testing.T) {
		now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
		var (
			localID = domaintest.ID("node-A")
			victim  = domaintest.ID("victim")
			viewer  = domaintest.ID("peer-Z")
		)
		tbl := NewTable(
			WithClock(func() time.Time { return now }),
			WithLocalOrigin(localID),
			WithMaxSeqAdvancePerWindow(3),
			WithSeqAdvanceWindow(1*time.Minute),
			WithSeqHoldDownDuration(2*time.Minute),
		)

		// Three in-threshold advances via AnnounceTo.
		for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")} {
			mustUpdate(t, tbl, RouteEntry{
				Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
				Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
			})
			_ = tbl.AnnounceTo(viewer)
			now = now.Add(time.Second)
		}
		if tbl.CapStats().SeqNoFlapHoldowns != 0 {
			t.Fatalf("setup: SeqNoFlapHoldowns=%d, want 0", tbl.CapStats().SeqNoFlapHoldowns)
		}

		// Land the 4th distinct-content claim and clear the dirty bit it
		// set, so the next assertion isolates AnnounceTargetFor's effect.
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u4"),
			Hops: 2, SeqNo: 14, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		// This call tips velocity over the threshold and engages
		// hold-down INSIDE AnnounceTargetFor — the arming emit is
		// suppressed (ok=false) but the published SeqNoFlapHoldowns
		// counter is bumped, so the table MUST be marked dirty.
		if _, _, ok := tbl.AnnounceTargetFor(victim, viewer); ok {
			t.Fatal("AnnounceTargetFor returned ok=true on the arming call; expected suppression")
		}
		if tbl.CapStats().SeqNoFlapHoldowns == 0 {
			t.Fatal("setup: expected the arming call to bump SeqNoFlapHoldowns")
		}
		if !tbl.IsDirty() {
			t.Fatal("SeqNo flap-cap engage did not mark the table dirty; the published " +
				"SeqNoFlapHoldowns bump would not reach the snapshot until an unrelated writer fires")
		}
	})
}
