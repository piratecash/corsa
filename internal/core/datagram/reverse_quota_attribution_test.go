package datagram

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// reverse_quota_attribution_test.go covers the isolation half of the
// measurement step: a shared quota must say WHO it turned away, not only how
// full it was.
//
// Reference: docs/refactoring/dht/13-measurements.md §2.

// tightReverseCaps is a reverse-limits seam with a quota small enough to make
// it bind in a test.
type tightReverseCaps struct {
	global      int
	perUpstream int
}

func (c tightReverseCaps) ReverseStateCaps() (int, int) { return c.global, c.perUpstream }

// quotaLabel builds a distinct, non-zero exchange label. Reserve refuses a
// zero one, so a fixture that forgot to set a byte would look like a quota
// refusal.
func quotaLabel(discriminator, plane byte) Label {
	var raw domain.PeerIdentity
	raw[0] = discriminator
	raw[1] = plane
	return NewLabel(raw)
}

// reserveLocal fills one local request slot, or reports the refusal.
func reserveLocal(t *testing.T, table *ReverseTable, label byte, dtype domain.DType) ReverseReserveResult {
	t.Helper()
	return table.Reserve(ReverseReserveOpts{
		Label:      quotaLabel(label, 1),
		Dst:        domain.PeerIdentity{9},
		DType:      dtype,
		Upstream:   LocalUpstream(),
		ReceivedAt: time.Now().UTC(),
	})
}

// TestSharedLocalQuotaNamesWhoItRefused is the finding this step measures for.
//
// Every locally originated request exchange shares ONE bucket, and a full
// bucket refuses rather than evicting. So a subsystem that asks a lot can stop
// another from ever asking, without exceeding a limit of its own — and the
// only trace of it used to be a counter that mixes local refusals with transit
// ones. This pins that a refusal now names the dtype it turned away.
//
// The mutation this kills: counting refusals without attributing them, which
// looks identical on a healthy node and answers nothing on a blocked one.
func TestSharedLocalQuotaNamesWhoItRefused(t *testing.T) {
	t.Parallel()

	const quota = 2
	table := NewReverseTable(ReverseTableConfig{
		Limits: tightReverseCaps{global: 64, perUpstream: quota},
	})

	// One noisy subsystem takes the whole bucket.
	for i := range quota {
		if outcome := reserveLocal(t, table, byte('a'+i), "get_identity"); outcome.Outcome() != ReverseSlotReserved {
			t.Fatalf("filling the quota failed at slot %d: %v", i, outcome.Outcome())
		}
	}
	if slots := table.LocalSlots(); slots != quota {
		t.Fatalf("LocalSlots() = %d, want the quota %d", slots, quota)
	}

	// A different subsystem now asks and is refused. Nothing is evicted: the
	// local bucket never gives a slot back before it expires.
	refused := reserveLocal(t, table, 'z', "liveness_probe")
	if refused.Outcome() == ReverseSlotReserved {
		t.Fatal("the quota admitted a request beyond its cap")
	}

	refusals := table.LocalRefusals()
	if got := refusals["liveness_probe"]; got != 1 {
		t.Fatalf("liveness_probe refusals = %d, want 1: a shared quota that cannot name who it blocked answers nothing",
			got)
	}
	if _, blamed := refusals["get_identity"]; blamed {
		t.Fatal("the subsystem that HOLDS the slots was recorded as refused")
	}
}

// TestTransitRefusalsAreNotAttributedByDType pins the deliberate asymmetry.
//
// A transit request's dtype arrives from the wire, so keying an accounting map
// on it would let a stranger grow this node's memory one invented type name at
// a time. Transit refusals already have their own drop reason; only the local
// bucket — whose dtypes come from this build's own senders — is attributed.
func TestTransitRefusalsAreNotAttributedByDType(t *testing.T) {
	t.Parallel()

	const quota = 1
	table := NewReverseTable(ReverseTableConfig{
		Limits: tightReverseCaps{global: 64, perUpstream: quota},
	})
	upstream := ChannelUpstream(NetworkChannel(11), ProvenIdentityKey(domain.PeerIdentity{7}), domain.PeerIdentity{7})

	reserve := func(label byte, dtype domain.DType) ReverseReserveResult {
		return table.Reserve(ReverseReserveOpts{
			Label:      quotaLabel(label, 2),
			Dst:        domain.PeerIdentity{9},
			DType:      dtype,
			Upstream:   upstream,
			ReceivedAt: time.Now().UTC(),
		})
	}
	if outcome := reserve('a', "invented_by_a_stranger"); outcome.Outcome() != ReverseSlotReserved {
		t.Fatalf("filling the transit quota failed: %v", outcome.Outcome())
	}
	if outcome := reserve('b', "another_invention"); outcome.Outcome() == ReverseSlotReserved {
		t.Fatal("the transit quota admitted a request beyond its cap")
	}

	if refusals := table.LocalRefusals(); len(refusals) != 0 {
		t.Fatalf("transit refusals were attributed by dtype (%v): the key space is a stranger's to choose",
			refusals)
	}
}

// TestLocalRefusalAttributionIsBounded keeps the accounting map from becoming
// the growth it exists to detect.
//
// Today its keys are this build's own dtypes — a handful — so the cap cannot
// bind. It is asserted anyway because "the keys are ours" is a property of
// today's callers rather than of the type, and an accounting map a future
// caller could grow is the shape several of this tree's leaks had.
func TestLocalRefusalAttributionIsBounded(t *testing.T) {
	t.Parallel()

	table := NewReverseTable(ReverseTableConfig{
		Limits: tightReverseCaps{global: 4, perUpstream: 1},
	})
	if outcome := reserveLocal(t, table, 'a', "holder"); outcome.Outcome() != ReverseSlotReserved {
		t.Fatalf("filling the quota failed: %v", outcome.Outcome())
	}
	for i := range localRefusalDTypeCap * 2 {
		reserveLocal(t, table, 'z', domain.DType("dtype_"+string(rune('a'+i%26))+string(rune('a'+i/26))))
	}
	if got := len(table.LocalRefusals()); got > localRefusalDTypeCap {
		t.Fatalf("the refusal map holds %d keys, above its own cap of %d", got, localRefusalDTypeCap)
	}
}
