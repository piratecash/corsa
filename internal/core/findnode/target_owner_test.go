package findnode

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func targetOwnerFixture(t *testing.T, limit int, start Instant) (*TargetCoordinator, *identity.Identity) {
	t.Helper()
	self, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	key, err := identity.ParsePublicKey(self.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	o, err := NewTargetCoordinator(key, domain.NetworkID("gazeta-devnet"), SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: limit}, start)
	if err != nil {
		t.Fatal(err)
	}
	return o, self
}

func targetOwnerRequest(o *TargetCoordinator, id domain.FindNodeRequestID, challenge uint32) protocol.FindNodeRequest {
	var nonce domain.FindNodeChallenge
	binary.BigEndian.PutUint32(nonce[:4], challenge)
	return protocol.FindNodeRequest{ID: id, Target: o.cache.self, Challenge: nonce}
}

func targetOwnerReserve(t *testing.T, o *TargetCoordinator, source Session, req protocol.FindNodeRequest, now, signBy Instant) LocalTargetResult {
	t.Helper()
	// This fixture classifies the actual current candidates before owner entry.
	// The coordinator neither calls this callback nor manufactures pin classes.
	sources := append(o.cache.Sessions(), source)
	result, err := o.ReserveLocalTarget(source, req, viewFor(t, o.cache.self, now, sources...), now, signBy)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func targetOwnerSignature(t *testing.T, self *identity.Identity, input PublicSigningInput) domain.FindNodeSignature {
	t.Helper()
	transcript, err := input.Transcript()
	if err != nil {
		t.Fatal(err)
	}
	signed := identity.SignPayload(self, transcript)
	raw, err := base64.RawURLEncoding.DecodeString(signed)
	if err != nil || len(raw) != len(domain.FindNodeSignature{}) {
		t.Fatalf("actual RawURL signature: len=%d err=%v", len(raw), err)
	}
	var signature domain.FindNodeSignature
	copy(signature[:], raw)
	return signature
}

func targetOwnerTake(t *testing.T, o *TargetCoordinator, res LocalReservation, now Instant) (ConsumedSigningPermit, PublicSigningInput) {
	t.Helper()
	permit, input, err := o.TakeSignWork(res, now)
	if err != nil {
		t.Fatal(err)
	}
	return permit, input
}

func TestTargetCoordinatorConsumedAuthorityAndActualSignature(t *testing.T) {
	a, _ := provenFixture(t, 10001, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10002, "192.0.0.0/16", 0)
	c, _ := provenFixture(t, 10003, "198.51.0.0/16", 0)
	o, self := targetOwnerFixture(t, 2, 0)
	deadline := Instant(time.Second)
	req := targetOwnerRequest(o, 11, 1)
	result := targetOwnerReserve(t, o, a, req, 0, deadline)
	if result.Kind != LocalReserved || !result.Answer.Target.IsZero() {
		t.Fatalf("new reservation result: %+v", result)
	}
	// A real valid signature alone is insufficient: no Consume has occurred.
	input := PublicSigningInput{network: o.network, target: req.Target, challenge: req.Challenge}
	sig := targetOwnerSignature(t, self, input)
	forged := ConsumedSigningPermit{owner: result.Reservation.owner, entry: result.Reservation.entry}
	if _, err := o.CompleteSigned(forged, sig, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("completed without actual Consume: %v", err)
	}
	wait := targetOwnerReserve(t, o, b, targetOwnerRequest(o, 22, 1), 0, deadline)
	if wait.Kind != LocalWaitingExisting || wait.Reservation != (LocalReservation{}) || o.Snapshot().CalendarPending != 1 {
		t.Fatalf("waiter acquired authority: %+v %+v", wait, o.Snapshot())
	}
	permit, input := targetOwnerTake(t, o, result.Reservation, 0)
	if input.Network() != o.network || input.Target() != req.Target || input.Challenge() != req.Challenge {
		t.Fatal("public input binding changed")
	}
	first, err := input.Transcript()
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(first)
	first[0] ^= 255
	second, err := input.Transcript()
	if err != nil || !bytes.Equal(second, before) {
		t.Fatalf("transcript slice retained: %v", err)
	}
	if _, _, err := o.TakeSignWork(result.Reservation, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("second Take: %v", err)
	}
	deliveries, err := o.CompleteSigned(permit, sig, 0)
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("completion %d %v", len(deliveries), err)
	}
	for i, delivery := range deliveries {
		original := req
		if i == 1 {
			original.ID = 22
		}
		if delivery.Answer.ID != original.ID || protocol.VerifyFindNodeAnswer(delivery.Answer, original, o.network) != nil {
			t.Fatal("actual signature or per-session envelope binding failed")
		}
	}
	if _, err := o.CompleteSigned(permit, sig, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("repeated completion: %v", err)
	}
	copyResult := targetOwnerReserve(t, o, c, targetOwnerRequest(o, 33, 1), 0, deadline)
	if copyResult.Kind != LocalReadyCopy || copyResult.Answer.ID != 33 || copyResult.Reservation != (LocalReservation{}) {
		t.Fatalf("ready copy %+v", copyResult)
	}
	if copyResult.Answer.Signature != deliveries[0].Answer.Signature || !bytes.Equal(copyResult.Answer.PublicKey.Bytes(), deliveries[0].Answer.PublicKey.Bytes()) || copyResult.Answer.Target != req.Target {
		t.Fatal("cached signed body changed")
	}
	snapshot := o.Snapshot()
	if snapshot.Local.Consumed != 1 || snapshot.Local.Completed != 1 || snapshot.CalendarPending != 0 || snapshot.Cache.Ready != 1 {
		t.Fatalf("unexpected composed snapshot %+v", snapshot)
	}
	if _, err := o.Cancel(result.Reservation, 0); err != nil {
		t.Fatal(err)
	}
	if snapshot != o.Snapshot() { // Diagnostic sequence alone changes.
		next := o.Snapshot()
		next.Sequence = snapshot.Sequence
		if next != snapshot {
			t.Fatal("Ready was cancelled")
		}
	}
}

func TestTargetCoordinatorConstructorAndCrossOwner(t *testing.T) {
	o, _ := targetOwnerFixture(t, 1, 0)
	profile := SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 1}
	for _, tc := range []struct {
		key     identity.PublicKey
		network domain.NetworkID
		profile SigningProfile
		start   Instant
	}{
		{identity.PublicKey{}, o.network, profile, 0},
		{o.key, "", profile, 0},
		{o.key, domain.NetworkID(strings.Repeat("x", 65536)), profile, 0},
		{o.key, o.network, SigningProfile{}, 0},
		{o.key, o.network, profile, -1},
	} {
		if _, err := NewTargetCoordinator(tc.key, tc.network, tc.profile, tc.start); err == nil {
			t.Fatal("invalid constructor input accepted")
		}
	}
	a, _ := provenFixture(t, 10101, domain.SourceGroupLoopback, 0)
	result := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
	other, _ := targetOwnerFixture(t, 1, 0)
	if _, _, err := other.TakeSignWork(result.Reservation, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("cross-owner Take: %v", err)
	}
	if _, _, err := o.TakeSignWork(LocalReservation{}, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("zero Take: %v", err)
	}
	permit, _ := targetOwnerTake(t, o, result.Reservation, 0)
	if _, err := other.CompleteSigned(permit, domain.FindNodeSignature{}, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("cross-owner completion: %v", err)
	}
	if _, err := other.CompleteSigned(ConsumedSigningPermit{}, domain.FindNodeSignature{}, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("zero completion: %v", err)
	}
	if _, err := other.Cancel(result.Reservation, 0); err != nil {
		t.Fatal(err)
	}
	// The same Session cannot gain reset credit through another calendar.
	req := targetOwnerRequest(other, 1, 1)
	if _, err := other.ReserveLocalTarget(a, req, viewFor(t, req.Target, 0, a), 0, Instant(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("calendar rebind: %v", err)
	}
	if got := other.Snapshot(); got.CalendarPending != 0 || got.Cache.CancelledWork != 1 {
		t.Fatalf("refusal did not retire generation: %+v", got)
	}
}

func TestTargetCoordinatorClosePendingNeverReassignsRetainedRow(t *testing.T) {
	a, _ := provenFixture(t, 10201, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10202, domain.SourceGroupLoopback, 0)
	c, _ := provenFixture(t, 10203, domain.SourceGroupLoopback, 0)
	d, _ := provenFixture(t, 10204, domain.SourceGroupLoopback, 0)
	o, _ := targetOwnerFixture(t, 2, 0)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 7), 0, Instant(time.Second))
	targetOwnerReserve(t, o, b, targetOwnerRequest(o, 2, 7), 0, Instant(time.Second))
	// A close by another existing state owner is reconciled, not only direct
	// coordinator.CloseSession. No production close integration is installed.
	NewPendingTable().CloseSession(a)
	if err := o.Expire(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := o.TakeSignWork(res.Reservation, 0); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("closed original still signable: %v", err)
	}
	wait := targetOwnerReserve(t, o, c, targetOwnerRequest(o, 3, 7), 0, Instant(time.Second))
	if wait.Kind != LocalWaitingExisting || wait.Reservation != (LocalReservation{}) {
		t.Fatal("retained generation was re-attributed")
	}
	if err := o.CloseSession(b, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ReserveLocalTarget(d, targetOwnerRequest(o, 4, 7), viewFor(t, o.cache.self, 0, c, d), 0, Instant(time.Second)); !errors.Is(err, ErrDuplicateExcess) {
		t.Fatalf("close reset three-session history: %v", err)
	}
	if got := o.Snapshot(); got.Unsignable != 1 || got.CalendarPending != 0 || got.Cache.SessionReferences != 1 || got.Cache.HistoryReferences != 3 {
		t.Fatalf("close inventory %+v", got)
	}
	if res.Reservation.entry.coordinated.alias != (SigningReservation{}) {
		t.Fatal("retained row still references original TLS session")
	}
}

func TestTargetCoordinatorConsumedCloseAndReadyExternalClose(t *testing.T) {
	a, _ := provenFixture(t, 10301, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10302, domain.SourceGroupLoopback, 0)
	c, _ := provenFixture(t, 10303, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 1, 0)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 9), 0, Instant(time.Second))
	targetOwnerReserve(t, o, b, targetOwnerRequest(o, 2, 9), 0, Instant(time.Second))
	permit, input := targetOwnerTake(t, o, res.Reservation, 0)
	NewPendingTable().CloseSession(b)
	if err := o.CloseSession(a, -1); !errors.Is(err, ErrClock) {
		t.Fatalf("invalid close time: %v", err)
	}
	if got := o.Snapshot(); got.Consumed != 1 || got.Cache.SessionReferences != 0 || got.Cache.HistoryReferences != 2 || got.SampleAt != 0 {
		t.Fatalf("consumed close %+v", got)
	}
	deliveries, err := o.CompleteSigned(permit, targetOwnerSignature(t, self, input), 0)
	if err != nil || len(deliveries) != 0 {
		t.Fatalf("closed consumed source lost completion: %d %v", len(deliveries), err)
	}
	copyResult := targetOwnerReserve(t, o, c, targetOwnerRequest(o, 3, 9), 0, Instant(time.Second))
	if copyResult.Kind != LocalReadyCopy || copyResult.Answer.ID != 3 {
		t.Fatalf("zero-recipient cached body lost: %+v", copyResult)
	}
	NewPendingTable().CloseSession(c)
	if err := o.Expire(-1); !errors.Is(err, ErrClock) {
		t.Fatalf("invalid expiry time: %v", err)
	}
	if got := o.Snapshot(); got.Cache.Ready != 1 || got.Cache.SessionReferences != 0 || got.Cache.HistoryReferences != 3 {
		t.Fatalf("Ready close erased dedup or retained TLS: %+v", got)
	}
}

func TestTargetCoordinatorDuplicateCloseDoesNotRetireOriginalAlias(t *testing.T) {
	a, _ := provenFixture(t, 10401, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10402, domain.SourceGroupLoopback, 0)
	o, _ := targetOwnerFixture(t, 1, 0)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
	targetOwnerReserve(t, o, b, targetOwnerRequest(o, 2, 1), 0, Instant(time.Second))
	if err := o.CloseSession(b, 0); err != nil {
		t.Fatal(err)
	}
	targetOwnerTake(t, o, res.Reservation, 0)
}

func TestTargetCoordinatorExpiryAndPressureReusePhysicalSlot(t *testing.T) {
	t.Run("expiry", func(t *testing.T) {
		a, _ := provenFixture(t, 10501, domain.SourceGroupLoopback, 0)
		o, _ := targetOwnerFixture(t, 1, 0)
		old := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
		if err := o.Expire(Instant(time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := o.Snapshot(); got.CalendarPending != 0 || got.Cache.Entries != 0 || got.Cache.Expired != 1 {
			t.Fatalf("expiry orphaned physical alias: %+v", got)
		}
		fresh := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 2, 1), Instant(time.Second), Instant(2*time.Second))
		if fresh.Kind != LocalReserved || fresh.Reservation.entry == old.Reservation.entry {
			t.Fatal("legitimate expiry did not permit a fresh generation")
		}
		if _, _, err := o.TakeSignWork(old.Reservation, Instant(time.Second)); !errors.Is(err, ErrStaleWork) {
			t.Fatalf("old generation resurrected: %v", err)
		}
		if len(old.Reservation.entry.recipients) != 0 || old.Reservation.entry.coordinated != nil {
			t.Fatal("detached token retained TLS owner state")
		}
	})
	t.Run("eviction_before_reserve", func(t *testing.T) {
		a, _ := provenFixture(t, 10511, domain.SourceGroupLoopback, 0)
		o, _ := targetOwnerFixture(t, 1, 0)
		original := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(6*time.Second))
		// Calendar capacity refuses subsequent rows, leaving existing bounded
		// Cancelled tombstones. Fill the SAME cache, not another job queue.
		for i := uint32(2); i <= MaxTargetEntries; i++ {
			now := Instant(i)
			req := targetOwnerRequest(o, domain.FindNodeRequestID(i), i)
			if _, err := o.ReserveLocalTarget(a, req, viewFor(t, o.cache.self, now, a), now, Instant(6*time.Second)); !errors.Is(err, ErrCapacity) {
				t.Fatalf("calendar refusal %d: %v", i, err)
			}
		}
		if got := o.Snapshot(); got.Cache.Entries != 256 || got.CalendarPending != 1 || got.Cache.CancelledWork != 255 {
			t.Fatalf("full cache fixture %+v", got)
		}
		// Empty Cancelled rows are PriorityNew; make the real pending row the
		// same priority with an earlier receipt, so legitimate policy evicts it.
		fresh := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 300, 300), 300, Instant(6*time.Second))
		if fresh.Kind != LocalReserved || o.Snapshot().CalendarPending != 1 || o.Snapshot().Cache.Evicted != 1 {
			t.Fatal("victim alias was not freed before replacement Reserve")
		}
		if _, _, err := o.TakeSignWork(original.Reservation, 300); !errors.Is(err, ErrStaleWork) {
			t.Fatalf("evicted reservation resurrected: %v", err)
		}
		if fresh.Reservation.entry.coordinated.alias.At() != Instant(250*time.Millisecond) {
			t.Fatal("eviction refunded projected node credit")
		}
		// The same removed challenge may create NEW work; its old token stays
		// stale. Free the physical slot while retaining its tombstone first.
		if ok, err := o.Cancel(fresh.Reservation, 300); !ok || err != nil {
			t.Fatalf("cancel replacement: %t %v", ok, err)
		}
		reused := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 301, 1), 301, Instant(6*time.Second))
		if reused.Reservation.entry == original.Reservation.entry {
			t.Fatal("challenge-only identity reused old generation")
		}
	})
}

func TestTargetCoordinatorStrictOriginalDeadlineAndReadyLifetime(t *testing.T) {
	t.Run("standalone_calendar_still_accepts_latest_equality", func(t *testing.T) {
		source, _ := provenFixture(t, 10600, domain.SourceGroupLoopback, 0)
		calendar, err := NewSigningCalendar(SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 1}, 0)
		if err != nil {
			t.Fatal(err)
		}
		alias, err := calendar.Reserve(source, 0, Instant(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := calendar.Consume(alias, Instant(time.Second)); err != nil {
			t.Fatalf("standalone latest-equality contract changed: %v", err)
		}
	})
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "take", true: "complete"}[complete], func(t *testing.T) {
			a, _ := provenFixture(t, 10601, domain.SourceGroupLoopback, 0)
			o, self := targetOwnerFixture(t, 1, 0)
			deadline := Instant(time.Second)
			res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, deadline)
			if !complete {
				if _, _, err := o.TakeSignWork(res.Reservation, deadline); !errors.Is(err, ErrStaleWork) {
					t.Fatalf("Take at signBy: %v", err)
				}
				return
			}
			permit, input := targetOwnerTake(t, o, res.Reservation, 0)
			if _, err := o.CompleteSigned(permit, targetOwnerSignature(t, self, input), deadline); !errors.Is(err, ErrStaleWork) {
				t.Fatalf("completion at original signBy: %v", err)
			}
		})
	}
	a, _ := provenFixture(t, 10611, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 1, 0)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
	permit, input := targetOwnerTake(t, o, res.Reservation, 0)
	completedAt := Instant(time.Second) - 1
	if _, err := o.CompleteSigned(permit, targetOwnerSignature(t, self, input), completedAt); err != nil {
		t.Fatal(err)
	}
	if err := o.Expire(completedAt + cacheLifetime - 1); err != nil || o.Snapshot().Cache.Ready != 1 {
		t.Fatalf("Ready TTL shortened to signing deadline: %v", err)
	}
	if err := o.Expire(completedAt + cacheLifetime); err != nil || o.Snapshot().Cache.Entries != 0 {
		t.Fatalf("Ready TTL not completion+60s: %v", err)
	}
}

func TestTargetCoordinatorCompletionOverflowPreservesConsumedAndCanCancel(t *testing.T) {
	start := Instant(math.MaxInt64 - int64(2*time.Second))
	a, _ := provenFixture(t, 10701, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 1, start)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), start, start+Instant(time.Second))
	permit, input := targetOwnerTake(t, o, res.Reservation, start)
	if _, err := o.CompleteSigned(permit, targetOwnerSignature(t, self, input), start); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow completion: %v", err)
	}
	if got := o.Snapshot(); got.Consumed != 1 || got.Cache.Ready != 0 {
		t.Fatalf("overflow partially completed %+v", got)
	}
	if ok, err := o.Cancel(res.Reservation, start); !ok || err != nil {
		t.Fatalf("cancel overflow: %t %v", ok, err)
	}
	if _, err := o.CompleteSigned(permit, domain.FindNodeSignature{}, start); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("cancelled consumed permit: %v", err)
	}
}

func TestTargetCoordinatorEvictedConsumedPermitNeverTargetsReplacement(t *testing.T) {
	a, _ := provenFixture(t, 10711, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 8, 0)
	first := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(6*time.Second))
	oldPermit, oldInput := targetOwnerTake(t, o, first.Reservation, 0)
	for i := uint32(2); i <= 8; i++ {
		targetOwnerReserve(t, o, a, targetOwnerRequest(o, domain.FindNodeRequestID(i), i), Instant(i), Instant(6*time.Second))
	}
	// Per-source eight-row pressure legitimately removes the oldest Consumed
	// generation. Its detached token must lose both recipients and authority.
	targetOwnerReserve(t, o, a, targetOwnerRequest(o, 9, 9), 9, Instant(6*time.Second))
	fresh := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 10, 1), 10, Instant(6*time.Second)+10)
	if fresh.Kind != LocalReserved || fresh.Reservation.entry == first.Reservation.entry {
		t.Fatal("legitimate removal reused a consumed generation")
	}
	if len(first.Reservation.entry.recipients) != 0 || first.Reservation.entry.coordinated != nil {
		t.Fatal("evicted consumed generation retained TLS/authority")
	}
	if _, err := o.CompleteSigned(oldPermit, targetOwnerSignature(t, self, oldInput), 10); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("old permit completed replacement challenge: %v", err)
	}
	at := fresh.Reservation.entry.coordinated.alias.At()
	permit, input := targetOwnerTake(t, o, fresh.Reservation, at)
	if _, err := o.CompleteSigned(oldPermit, targetOwnerSignature(t, self, oldInput), at); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("old permit acquired the replacement's Consumed phase: %v", err)
	}
	if ok, err := o.Cancel(first.Reservation, at); ok || err != nil {
		t.Fatalf("old reservation cancelled replacement: %t %v", ok, err)
	}
	deliveries, err := o.CompleteSigned(permit, targetOwnerSignature(t, self, input), at)
	if err != nil || len(deliveries) != 1 || deliveries[0].Answer.ID != 10 {
		t.Fatalf("fresh independently charged generation: %d %v", len(deliveries), err)
	}
}

func TestTargetCoordinatorRefusalsAndBadClockGrantNoAuthority(t *testing.T) {
	a, _ := provenFixture(t, 10721, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10722, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 2, 0)
	req := targetOwnerRequest(o, 1, 1)
	if _, err := o.ReserveLocalTarget(Session{}, req, PrioritySnapshot{}, 0, Instant(time.Second)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unproven source: %v", err)
	}
	if _, err := o.ReserveLocalTarget(a, req, PrioritySnapshot{}, 0, Instant(time.Second)); !errors.Is(err, ErrPrioritySnapshot) {
		t.Fatalf("absent authoritative snapshot: %v", err)
	}
	res := targetOwnerReserve(t, o, a, req, 0, Instant(time.Second))
	permit, input := targetOwnerTake(t, o, res.Reservation, 0)
	sig := targetOwnerSignature(t, self, input)
	if _, err := o.CompleteSigned(permit, sig, -1); !errors.Is(err, ErrClock) {
		t.Fatalf("bad clock completed: %v", err)
	}
	if _, err := o.ReserveLocalTarget(b, targetOwnerRequest(o, 2, 2), PrioritySnapshot{}, -1, Instant(time.Second)); !errors.Is(err, ErrClock) {
		t.Fatalf("bad clock reserved: %v", err)
	}
	if got := o.Snapshot(); got.Consumed != 1 || got.Cache.Entries != 1 || got.SampleAt != 0 || got.CalendarPending != 0 {
		t.Fatalf("bad clock mutated authority: %+v", got)
	}
	if _, err := o.CompleteSigned(permit, sig, 0); err != nil {
		t.Fatalf("bad clock destroyed previously consumed authority: %v", err)
	}
	second := targetOwnerReserve(t, o, b, targetOwnerRequest(o, 2, 2), 0, Instant(time.Second))
	if ok, err := o.Cancel(second.Reservation, 0); !ok || err != nil {
		t.Fatalf("cancel pending %t %v", ok, err)
	}
	c, _ := provenFixture(t, 10723, domain.SourceGroupLoopback, 0)
	if _, err := o.ReserveLocalTarget(c, targetOwnerRequest(o, 3, 2), viewFor(t, o.cache.self, 0, a, c), 0, Instant(time.Second)); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("retained tombstone silently re-signed: %v", err)
	}
}

func TestTargetCoordinatorBadClockCleanupAndSequenceSaturation(t *testing.T) {
	a, _ := provenFixture(t, 10801, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10802, domain.SourceGroupLoopback, 0)
	o, _ := targetOwnerFixture(t, 3, 0)
	first := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
	second := targetOwnerReserve(t, o, b, targetOwnerRequest(o, 2, 2), 0, Instant(2*time.Second))
	o.mu.Lock()
	o.sequence = math.MaxUint64 - 1
	o.mu.Unlock()
	NewPendingTable().CloseSession(a)
	if err := o.Expire(-1); !errors.Is(err, ErrClock) {
		t.Fatalf("bad time expiry: %v", err)
	}
	if got := o.Snapshot(); !got.SequenceSaturated || got.Sequence != math.MaxUint64 || got.SampleAt != 0 || got.Cache.Entries != 2 || got.CalendarPending != 1 || got.Unsignable != 1 || got.Cache.SessionReferences != 1 {
		t.Fatalf("bad time close reconciliation/sequence: %+v", got)
	}
	if first.Reservation.entry.coordinated.alias != (SigningReservation{}) {
		t.Fatal("closed original alias retained TLS")
	}
	if _, _, err := o.TakeSignWork(second.Reservation, -1); !errors.Is(err, ErrClock) {
		t.Fatalf("bad time Take: %v", err)
	}
	permit, _ := targetOwnerTake(t, o, second.Reservation, Instant(250*time.Millisecond))
	if ok, err := o.Cancel(second.Reservation, -1); !ok || !errors.Is(err, ErrClock) {
		t.Fatalf("bad time must cancel exact consumed row: %t %v", ok, err)
	}
	if _, err := o.CompleteSigned(permit, domain.FindNodeSignature{}, Instant(250*time.Millisecond)); !errors.Is(err, ErrStaleWork) {
		t.Fatalf("cancelled consumed permit accepted: %v", err)
	}
	if err := o.CloseSession(b, -1); !errors.Is(err, ErrClock) {
		t.Fatal(err)
	}
	if err := o.Expire(Instant(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := o.Snapshot(); got.Sequence != math.MaxUint64 || !got.SequenceSaturated || got.Cache.Entries != 0 || got.CalendarPending != 0 {
		t.Fatalf("sequence saturation gated cleanup %+v", got)
	}
}

func TestTargetCoordinatorActualNodeAndSessionDebit(t *testing.T) {
	a, _ := provenFixture(t, 10901, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 10902, domain.SourceGroupLoopback, 0)
	o, _ := targetOwnerFixture(t, 8, 0)
	var reservations [6]LocalReservation
	for i := range reservations {
		source := a
		if i == 1 {
			source = b
		}
		reservations[i] = targetOwnerReserve(t, o, source, targetOwnerRequest(o, domain.FindNodeRequestID(i), uint32(i+1)), 0, Instant(6*time.Second)).Reservation
	}
	// Delayed Consume must enforce actual node spacing, not just slot times.
	targetOwnerTake(t, o, reservations[0], Instant(time.Second))
	if _, _, err := o.TakeSignWork(reservations[1], Instant(time.Second)); !errors.Is(err, ErrNotReady) {
		t.Fatalf("actual node spacing bypassed: %v", err)
	}
	targetOwnerTake(t, o, reservations[1], Instant(1250*time.Millisecond))
	for i := 2; i < 5; i++ {
		targetOwnerTake(t, o, reservations[i], Instant(time.Duration(1000+i*250)*time.Millisecond))
	}
	// A used its four burst tokens at 1s,1.5s,1.75s,2s with refill at 2s;
	// inspect actual debit independently of the projected calendar promises.
	a.state.mu.Lock()
	actual := a.state.signingActual.tokens
	a.state.mu.Unlock()
	if actual != forwardToken {
		t.Fatalf("actual source budget not debited: %d", actual)
	}
	targetOwnerTake(t, o, reservations[5], Instant(2250*time.Millisecond))
	if ok, err := o.Cancel(reservations[5], Instant(2250*time.Millisecond)); !ok || err != nil {
		t.Fatalf("cancel consumed budget %t %v", ok, err)
	}
	// Cancellation never refills source actual credits or node spacing.
	a.state.mu.Lock()
	actual = a.state.signingActual.tokens
	a.state.mu.Unlock()
	if actual != forwardToken/4 {
		t.Fatalf("cancellation refunded actual credit: %d", actual)
	}
}

func TestTargetCoordinatorConcurrentTransitions(t *testing.T) {
	a, _ := provenFixture(t, 11001, domain.SourceGroupLoopback, 0)
	b, _ := provenFixture(t, 11002, domain.SourceGroupLoopback, 0)
	o, self := targetOwnerFixture(t, 1, 0)
	res := targetOwnerReserve(t, o, a, targetOwnerRequest(o, 1, 1), 0, Instant(time.Second))
	input := PublicSigningInput{network: o.network, target: o.cache.self, challenge: res.Reservation.entry.challenge}
	sig := targetOwnerSignature(t, self, input)
	var permits atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			permit, _, err := o.TakeSignWork(res.Reservation, 0)
			if err == nil {
				permits.Add(1)
				_, _ = o.CompleteSigned(permit, sig, 0)
			}
		})
	}
	wg.Go(func() { _, _ = o.Cancel(res.Reservation, 0) })
	wg.Go(func() { _ = o.CloseSession(a, 0) })
	wg.Go(func() { _ = o.Expire(0) })
	wg.Go(func() {
		_, _ = o.ReserveLocalTarget(b, targetOwnerRequest(o, 2, 1), viewFor(t, o.cache.self, 0, b), 0, Instant(time.Second))
	})
	wg.Wait()
	if permits.Load() > 1 {
		t.Fatal("multiple consumed permits issued")
	}
	got := o.Snapshot()
	if got.CalendarPending != got.AliasPending || got.Cache.SessionReferences > 1 || got.Cache.HistoryReferences > 2 {
		t.Fatalf("concurrent owner invariant %+v", got)
	}
}

func TestTargetCleanupBatchBoundAndZeroing(t *testing.T) {
	// White-box bound test of the fixed temporary batch, not a claim that 256
	// reservations fit the current explicit spacing/deadline profile.
	calendar, err := NewSigningCalendar(SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 256}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var changes targetChanges
	for range MaxTargetEntries {
		slot := &signingSlot{}
		calendar.slots[slot] = struct{}{}
		state := &coordinatedSigning{alias: SigningReservation{calendar: calendar, slot: slot}}
		changes.retireAlias(state)
		if state.alias != (SigningReservation{}) {
			t.Fatal("cleanup source not cleared")
		}
	}
	changes.drain(calendar)
	if calendar.Pending() != 0 || changes != (targetChanges{}) {
		t.Fatal("cleanup did not drain and reset every physical reference")
	}
	changes.count = MaxTargetEntries
	defer func() {
		message, ok := recover().(string)
		if !ok || message != "find node: target cleanup phase exceeded cache bound" {
			t.Fatal("cleanup must assert its bound before append, not silently truncate or reach a runtime bounds panic")
		}
	}()
	changes.retireAlias(&coordinatedSigning{alias: SigningReservation{slot: &signingSlot{}}})
}
