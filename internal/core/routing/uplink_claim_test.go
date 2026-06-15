package routing

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// --- IsWithdrawn ---

func TestUplinkClaim_IsWithdrawn(t *testing.T) {
	cases := []struct {
		name     string
		claim    UplinkClaim
		expected bool
	}{
		{"live", UplinkClaim{Hops: 3, Withdrawn: false}, false},
		{"withdrawn flag set", UplinkClaim{Hops: 3, Withdrawn: true}, true},
		{"withdrawn with HopsInfinity", UplinkClaim{Hops: HopsInfinity, Withdrawn: true}, true},
		// Hops=HopsInfinity alone is NOT enough — Withdrawn flag is
		// canonical inside storage. This documents the invariant: the
		// caller (ApplyUpdate / WithdrawTriple) is responsible for
		// setting Withdrawn correctly when ingesting a wire withdrawal.
		{"HopsInfinity without flag is structurally invalid but IsWithdrawn returns false", UplinkClaim{Hops: HopsInfinity, Withdrawn: false}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.claim.IsWithdrawn(); got != tc.expected {
				t.Fatalf("IsWithdrawn() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// --- IsExpired ---

func TestUplinkClaim_IsExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		expires  time.Time
		expected bool
	}{
		{"past", now.Add(-time.Second), true},
		{"now (inclusive)", now, true},
		{"future", now.Add(time.Hour), false},
		{"zero treated as never expires", time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := UplinkClaim{Uplink: domaintest.ID("u"), Hops: 1, Source: RouteSourceDirect, ExpiresAt: tc.expires}
			if got := c.IsExpired(now); got != tc.expected {
				t.Fatalf("IsExpired(now) = %v, want %v", got, tc.expected)
			}
		})
	}
}

// --- Validate ---

func TestUplinkClaim_ValidateRejectsEmptyUplink(t *testing.T) {
	c := UplinkClaim{Hops: 1, Source: RouteSourceDirect}
	if err := c.Validate(); !errors.Is(err, ErrEmptyUplink) {
		t.Fatalf("expected ErrEmptyUplink, got %v", err)
	}
}

func TestUplinkClaim_ValidateRejectsRouteSourceLocal(t *testing.T) {
	// RouteSourceLocal is synthetic — never stored as UplinkClaim.
	// The check protects against callers bypassing the public Table
	// API to inject a zero-hop highest-trust route.
	c := UplinkClaim{Uplink: domaintest.ID("self"), Hops: 0, Source: RouteSourceLocal}
	if err := c.Validate(); !errors.Is(err, ErrLocalSourceReserved) {
		t.Fatalf("expected ErrLocalSourceReserved, got %v", err)
	}
}

func TestUplinkClaim_ValidateAcceptsLiveDirectAtHops1(t *testing.T) {
	c := UplinkClaim{Uplink: domaintest.ID("alice"), Hops: 1, Source: RouteSourceDirect}
	if err := c.Validate(); err != nil {
		t.Fatalf("well-formed direct should pass: %v", err)
	}
}

func TestUplinkClaim_ValidateRejectsDirectWithWrongHops(t *testing.T) {
	c := UplinkClaim{Uplink: domaintest.ID("alice"), Hops: 3, Source: RouteSourceDirect}
	if err := c.Validate(); !errors.Is(err, ErrDirectHopsMust1) {
		t.Fatalf("expected ErrDirectHopsMust1, got %v", err)
	}
}

func TestUplinkClaim_ValidateAcceptsLiveAnnouncement(t *testing.T) {
	for _, hops := range []uint8{1, 5, 15} {
		c := UplinkClaim{Uplink: domaintest.ID("n1"), Hops: hops, Source: RouteSourceAnnouncement}
		if err := c.Validate(); err != nil {
			t.Fatalf("live announcement hops=%d should pass: %v", hops, err)
		}
	}
}

func TestUplinkClaim_ValidateRejectsLiveAnnouncementOutOfRange(t *testing.T) {
	for _, hops := range []uint8{0, HopsInfinity} {
		c := UplinkClaim{Uplink: domaintest.ID("n1"), Hops: hops, Source: RouteSourceAnnouncement}
		if err := c.Validate(); !errors.Is(err, ErrInvalidClaimHops) {
			t.Fatalf("live announcement hops=%d should fail with ErrInvalidClaimHops, got %v", hops, err)
		}
	}
}

func TestUplinkClaim_ValidateAcceptsWithdrawnAtAnyHops(t *testing.T) {
	// Withdrawn tombstones keep the just-withdrawn entry's Hops,
	// which can be anything 0..HopsInfinity. The validate rule
	// relaxes for Withdrawn so InvalidateAllVia (which sets
	// Hops=HopsInfinity on direct routes that originally had Hops=1)
	// and direct-route-cleared-with-Hops=0 paths both pass.
	for _, hops := range []uint8{0, 1, 5, 15, HopsInfinity} {
		c := UplinkClaim{Uplink: domaintest.ID("alice"), Hops: hops, Source: RouteSourceAnnouncement, Withdrawn: true}
		if err := c.Validate(); err != nil {
			t.Fatalf("withdrawn tombstone hops=%d should pass: %v", hops, err)
		}
	}
}

// --- toUplinkClaim ---

func TestToUplinkClaim_DropsOriginPreservesExtra(t *testing.T) {
	// Origin on the incoming RouteEntry was consumed for anti-spoof
	// by the caller; the storage shape has no dedicated Origin field
	// (per-(Identity, Uplink) keying makes it unnecessary). Extra,
	// however, IS preserved per the documented RouteEntry.Extra
	// forward-compat contract — wire-protocol unknown fields must
	// relay unchanged through storage.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	extra := json.RawMessage(`{"onion_box":"deadbeef"}`)
	entry := RouteEntry{
		Identity:  domaintest.ID("alice"),
		Origin:    domaintest.ID("bob"), // foreign, transit lineage; not preserved
		NextHop:   domaintest.ID("charlie"),
		Hops:      3,
		SeqNo:     42,
		Source:    RouteSourceAnnouncement,
		ExpiresAt: now.Add(time.Hour),
		Extra:     extra,
	}

	claim := toUplinkClaim(entry, now)

	if claim.Uplink != domaintest.ID("charlie") {
		t.Fatalf("Uplink should mirror NextHop: got %q, want %q", claim.Uplink, "charlie")
	}
	if claim.Hops != 3 {
		t.Fatalf("Hops should mirror entry.Hops: got %d, want 3", claim.Hops)
	}
	if claim.SeqNo != 42 {
		t.Fatalf("SeqNo should mirror entry.SeqNo: got %d, want 42", claim.SeqNo)
	}
	if claim.Source != RouteSourceAnnouncement {
		t.Fatalf("Source should mirror entry.Source: got %s", claim.Source)
	}
	if !claim.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt should be set to `now`: got %v, want %v", claim.UpdatedAt, now)
	}
	if !claim.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("ExpiresAt should mirror entry.ExpiresAt: got %v", claim.ExpiresAt)
	}
	if claim.Withdrawn {
		t.Fatal("live entry should not be withdrawn")
	}
	if string(claim.Extra) != string(extra) {
		t.Fatalf("Extra should be preserved through storage: got %q, want %q",
			string(claim.Extra), string(extra))
	}
}

func TestToUplinkClaim_DerivesWithdrawnFromHopsInfinity(t *testing.T) {
	// Wire withdrawals arrive as RouteEntry{Hops: HopsInfinity}.
	// toUplinkClaim translates the sentinel into the dedicated
	// Withdrawn flag (Hops on storage keeps the sentinel value as
	// observed; ApplyUpdate / WithdrawTriple are the callers that
	// must set Withdrawn correctly via this helper).
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	entry := RouteEntry{
		Identity:  domaintest.ID("alice"),
		Origin:    domaintest.ID("alice"),
		NextHop:   domaintest.ID("charlie"),
		Hops:      HopsInfinity,
		SeqNo:     99,
		Source:    RouteSourceAnnouncement,
		ExpiresAt: now.Add(time.Minute),
	}

	claim := toUplinkClaim(entry, now)

	if !claim.Withdrawn {
		t.Fatal("HopsInfinity entry should produce a withdrawn claim")
	}
	if claim.Hops != HopsInfinity {
		t.Fatalf("withdrawn claim should keep the wire Hops value: got %d, want %d", claim.Hops, HopsInfinity)
	}
}

func TestToUplinkClaim_DirectRouteHops1(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	entry := RouteEntry{
		Identity: domaintest.ID("alice"),
		Origin:   domaintest.ID("me"),
		NextHop:  domaintest.ID("alice"), // direct: NextHop == Identity
		Hops:     1,
		SeqNo:    7,
		Source:   RouteSourceDirect,
		// ExpiresAt zero — direct routes are socket-bound, not TTL-bound
	}

	claim := toUplinkClaim(entry, now)

	if claim.Source != RouteSourceDirect {
		t.Fatalf("Source should be Direct: got %s", claim.Source)
	}
	if claim.Hops != 1 {
		t.Fatalf("direct Hops should be 1: got %d", claim.Hops)
	}
	if !claim.ExpiresAt.IsZero() {
		t.Fatalf("direct ExpiresAt should remain zero: got %v", claim.ExpiresAt)
	}
}

// --- toRouteEntry ---

func TestToRouteEntry_OriginEqualsLocalOriginWhenSet(t *testing.T) {
	// Phase A migration contract: every wire-emit (live + withdrawal)
	// uses Origin = localOrigin so that pre-A1 receivers' withdrawal
	// anti-spoof (Origin == sender) accepts our frames and live +
	// withdrawal land in the SAME (Identity, Origin, NextHop) slot
	// on the legacy receiver. toRouteEntry mirrors that contract on
	// the boundary so internal RouteEntry consumers see consistent
	// Origin values across Lookup / Snapshot / Announce paths.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	claim := UplinkClaim{
		Uplink:    domaintest.ID("charlie"),
		Hops:      3,
		SeqNo:     42,
		Source:    RouteSourceAnnouncement,
		UpdatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}

	entry := toRouteEntry(domaintest.ID("alice"), domaintest.ID("self-node"), claim)

	if entry.Identity != domaintest.ID("alice") {
		t.Fatalf("Identity: got %q, want %q", entry.Identity, "alice")
	}
	if entry.Origin != domaintest.ID("self-node") {
		t.Fatalf("Origin should be set to localOrigin: got %q, want %q", entry.Origin, "self-node")
	}
	if entry.NextHop != domaintest.ID("charlie") {
		t.Fatalf("NextHop should mirror claim.Uplink: got %q", entry.NextHop)
	}
	if entry.Hops != 3 {
		t.Fatalf("Hops: got %d, want 3", entry.Hops)
	}
	if entry.SeqNo != 42 {
		t.Fatalf("SeqNo: got %d, want 42", entry.SeqNo)
	}
	if entry.Source != RouteSourceAnnouncement {
		t.Fatalf("Source: got %s", entry.Source)
	}
	if !entry.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("ExpiresAt: got %v", entry.ExpiresAt)
	}
}

func TestToRouteEntry_FallsBackToIdentityWhenLocalOriginEmpty(t *testing.T) {
	// Test fixtures that create routing.Table without
	// WithLocalOrigin have localOrigin="" inside routeStore. Emitting
	// Origin="" would later fail RouteEntry.Validate at any
	// downstream consumer. The fallback ensures synthesised entries
	// always have non-empty Origin: when localOrigin is "", we
	// substitute Identity. This matches the AnnounceProjectionFor /
	// InvalidateAllVia wire-emit fallback so the boundary stays
	// consistent across call sites.
	claim := UplinkClaim{
		Uplink: domaintest.ID("charlie"),
		Hops:   1,
		SeqNo:  1,
		Source: RouteSourceDirect,
	}
	entry := toRouteEntry(domaintest.ID("alice"), domain.PeerIdentity{}, claim)
	if entry.Origin != domaintest.ID("alice") {
		t.Fatalf("empty localOrigin should fall back to Identity: got %q, want %q",
			entry.Origin, "alice")
	}
}

func TestToRouteEntry_WithdrawnClaimShowsHopsInfinity(t *testing.T) {
	// Boundary RouteEntry consumers (RPC, tests, AnnounceTo) keep
	// using RouteEntry.IsWithdrawn(), which checks Hops >=
	// HopsInfinity. The synthesised RouteEntry must therefore
	// carry Hops=HopsInfinity for withdrawn claims regardless of
	// the stored claim.Hops value, so the boundary contract
	// stays compatible.
	claim := UplinkClaim{
		Uplink:    domaintest.ID("charlie"),
		Hops:      3, // pre-withdrawal hop count, kept in storage
		SeqNo:     99,
		Source:    RouteSourceAnnouncement,
		Withdrawn: true,
	}

	entry := toRouteEntry(domaintest.ID("alice"), domaintest.ID("self-node"), claim)

	if entry.Hops != HopsInfinity {
		t.Fatalf("withdrawn claim should surface as Hops=HopsInfinity on boundary RouteEntry: got %d", entry.Hops)
	}
	if !entry.IsWithdrawn() {
		t.Fatal("synthesised entry should report IsWithdrawn() = true")
	}
}

func TestToRouteEntry_ExtraPreserved(t *testing.T) {
	// Extra is the wire-protocol forward-compat field; the documented
	// RouteEntry.Extra / frame.go contract says it MUST round-trip
	// unchanged through storage so that re-announced routes carry
	// the wire's unknown fields onward. The Phase A storage swap
	// preserves Extra on UplinkClaim and the conversion helpers
	// thread it both directions.
	extra := json.RawMessage(`{"onion_box":"deadbeef","future":42}`)
	claim := UplinkClaim{
		Uplink: domaintest.ID("charlie"),
		Hops:   2,
		SeqNo:  3,
		Source: RouteSourceAnnouncement,
		Extra:  extra,
	}
	entry := toRouteEntry(domaintest.ID("alice"), domaintest.ID("self-node"), claim)
	if string(entry.Extra) != string(extra) {
		t.Fatalf("Extra should round-trip through toRouteEntry: got %q, want %q",
			string(entry.Extra), string(extra))
	}
}

// --- Round-trip ---

func TestUplinkClaim_RoundTripPreservesRoutingFieldsAndExtra(t *testing.T) {
	// Convert a RouteEntry → UplinkClaim → RouteEntry and verify
	// that the routing-relevant fields (Identity, NextHop, Hops,
	// SeqNo, Source, ExpiresAt, IsWithdrawn) and the forward-compat
	// Extra field round-trip. Origin intentionally does NOT
	// round-trip: it goes in as whatever the wire carried and
	// comes back as localOrigin — that is the Phase A swap's
	// migration contract, pinned here so future readers see it
	// documented in a test.
	localOrigin := domaintest.ID("self-node")
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	extra := json.RawMessage(`{"onion_box":"deadbeef"}`)
	cases := []struct {
		name  string
		entry RouteEntry
	}{
		{
			name: "live announcement via transit",
			entry: RouteEntry{
				Identity: domaintest.ID("alice"), Origin: domaintest.ID("transit-origin"), NextHop: domaintest.ID("charlie"),
				Hops: 5, SeqNo: 17, Source: RouteSourceAnnouncement,
				ExpiresAt: now.Add(time.Hour),
				Extra:     extra,
			},
		},
		{
			name: "live hop_ack",
			entry: RouteEntry{
				Identity: domaintest.ID("alice"), Origin: domaintest.ID("some-origin"), NextHop: domaintest.ID("charlie"),
				Hops: 2, SeqNo: 3, Source: RouteSourceHopAck,
				ExpiresAt: now.Add(time.Minute),
				Extra:     extra,
			},
		},
		{
			name: "direct route",
			entry: RouteEntry{
				Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
				Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
				// ExpiresAt zero — direct
			},
		},
		{
			name: "wire withdrawal",
			entry: RouteEntry{
				Identity: domaintest.ID("alice"), Origin: domaintest.ID("transit-origin"), NextHop: domaintest.ID("charlie"),
				Hops: HopsInfinity, SeqNo: 99, Source: RouteSourceAnnouncement,
				ExpiresAt: now.Add(time.Minute),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim := toUplinkClaim(tc.entry, now)
			back := toRouteEntry(tc.entry.Identity, localOrigin, claim)

			if back.Identity != tc.entry.Identity {
				t.Errorf("Identity not preserved: got %q, want %q", back.Identity, tc.entry.Identity)
			}
			if back.NextHop != tc.entry.NextHop {
				t.Errorf("NextHop not preserved: got %q, want %q", back.NextHop, tc.entry.NextHop)
			}
			if back.Hops != tc.entry.Hops {
				t.Errorf("Hops not preserved: got %d, want %d", back.Hops, tc.entry.Hops)
			}
			if back.SeqNo != tc.entry.SeqNo {
				t.Errorf("SeqNo not preserved: got %d, want %d", back.SeqNo, tc.entry.SeqNo)
			}
			if back.Source != tc.entry.Source {
				t.Errorf("Source not preserved: got %s, want %s", back.Source, tc.entry.Source)
			}
			if !back.ExpiresAt.Equal(tc.entry.ExpiresAt) {
				t.Errorf("ExpiresAt not preserved: got %v, want %v", back.ExpiresAt, tc.entry.ExpiresAt)
			}
			if back.IsWithdrawn() != tc.entry.IsWithdrawn() {
				t.Errorf("IsWithdrawn not preserved: got %v, want %v", back.IsWithdrawn(), tc.entry.IsWithdrawn())
			}
			if string(back.Extra) != string(tc.entry.Extra) {
				t.Errorf("Extra not preserved: got %q, want %q",
					string(back.Extra), string(tc.entry.Extra))
			}

			// Origin documentation: post-swap Origin is always
			// localOrigin, regardless of what came in. This is the
			// migration contract for pre-A1 receivers' withdrawal
			// anti-spoof; see route_store_lookup.go for the wire-
			// emit contract.
			if back.Origin != localOrigin {
				t.Errorf("Origin should be set to localOrigin on round-trip: got %q, want %q (was %q on input)",
					back.Origin, localOrigin, tc.entry.Origin)
			}
		})
	}
}
