package datagram

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// deadlines_test.go proves the timing rule of §3.3 is CONSTRUCTIVE and is the
// SAME on every node: it is a pure function of the signed header, so two nodes
// on one path cannot come to different conclusions about one frame.
//
// The rule used to be split between a profile that asked and a layer that
// clamped, and most of this file tested the clamps. There is nothing left to
// clamp against: the one policy IS the layer's.

// deadlineTestInstant is the signed time every case here measures from. It is a
// whole second because the wire carries auth.time as a unix second.
func deadlineTestInstant() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }

// deadlineTestHeader builds the header view of an ordinary signed frame.
func deadlineTestHeader(t *testing.T, reshape func(*protocol.DatagramFrame)) Header {
	t.Helper()
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         domaintest.ID("deadline-src"),
		Dst:         domaintest.ID("deadline-dst"),
		TTL:         OriginTTL(),
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DType("push_identity"),
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        make([]byte, domain.DatagramSaltBytes),
			PubKey:      make([]byte, 32),
			Sig:         make([]byte, 64),
			MaxTTL:      OriginTTL(),
			Time:        deadlineTestInstant().Unix(),
		},
	}
	if reshape != nil {
		reshape(&frame)
	}
	header, err := NewHeader(frame)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	return header
}

// TestDeadlinesRaiseReplayUntilToValidUntil pins the one relation §2.2 rules
// out by name: a key that dies before the frame it identifies would let an
// exact copy through once per window, forever.
func TestDeadlinesRaiseReplayUntilToValidUntil(t *testing.T) {
	header := deadlineTestHeader(t, nil)

	decision := ComputeDeadlines(header, deadlineTestInstant())
	deadlines, ok := decision.Deadlines()
	if !ok || decision.Outcome() != DeadlinesComputed {
		t.Fatalf("outcome = %s, want computed", decision.Outcome())
	}
	if deadlines.ReplayUntil().Before(deadlines.ValidUntil()) {
		t.Fatalf("replay_until = %s is below valid_until %s", deadlines.ReplayUntil(), deadlines.ValidUntil())
	}
}

// TestSendUntilIsTheEarliestOfItsThreeBounds pins ALL THREE terms of the send
// deadline, one case per term, by choosing inputs at which each is the binding
// one.
//
// Every expectation below is written out in wall-clock terms instead of being
// recomputed from the constant under test: an expectation phrased as
// `signed.Add(freshness).Add(-sendGrace)` agrees with whatever sendGrace
// happens to be, which makes the margin unpinned in the one file that is
// supposed to pin it.
//
// The per-class queue residence is what makes the two classes differ at all —
// 5 s for control, 30 s for bulk (§4.2) — and a flat value for both would let a
// control frame sit in a lane six times longer than the class allows anywhere
// else in the spec.
func TestSendUntilIsTheEarliestOfItsThreeBounds(t *testing.T) {
	// The margins, spelled out. A change to any of them is a protocol change
	// every node on a path has to make at once, so it belongs in a diff that
	// touches this line too.
	const (
		freshnessWindow = 5 * time.Minute
		wantSendGrace   = time.Minute
	)
	if sendGrace != wantSendGrace {
		t.Fatalf("sendGrace = %s, want %s: the send window moved for every node on every path, "+
			"and the expectations below have to be re-derived rather than silently absorbed",
			sendGrace, wantSendGrace)
	}

	cases := map[string]struct {
		class      domain.DatagramClass
		residence  time.Duration
		writeGrace time.Duration
	}{
		"control": {class: domain.DatagramClassControl, residence: 5 * time.Second, writeGrace: 5 * time.Second},
		"bulk":    {class: domain.DatagramClassBulk, residence: 30 * time.Second, writeGrace: 30 * time.Second},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			header := deadlineTestHeader(t, func(frame *protocol.DatagramFrame) {
				frame.Class = tc.class
			})
			signed := deadlineTestInstant()

			// Placed at the instant it was signed, the frame's whole budget is
			// the queue residence: the validity window is minutes away.
			early, ok := ComputeDeadlines(header, signed).Deadlines()
			if !ok {
				t.Fatal("an ordinary frame produced no deadlines")
			}
			if want := signed.Add(tc.residence); !early.SendUntil().Equal(want) {
				t.Fatalf("send_until = %s, want the queue residence bound %s", early.SendUntil(), want)
			}

			// Late in the frame's life the window is what binds, and the margin
			// is subtracted so no write is started at the very edge.
			late := signed.Add(4 * time.Minute)
			bounded, ok := ComputeDeadlines(header, late).Deadlines()
			if !ok {
				t.Fatal("a late frame produced no deadlines")
			}
			if want := signed.Add(4 * time.Minute); !bounded.SendUntil().Equal(want) {
				t.Fatalf("send_until = %s, want the window bound %s", bounded.SendUntil(), want)
			}

			// THE THIRD TERM: the room reserved for the socket write itself.
			//
			// It binds nothing on today's constants — the base replay window
			// and the freshness window hold the same value, so valid_until is
			// the later bound and send_grace the larger margin — and that is
			// precisely why the whole term can be deleted without any of the
			// assertions above noticing. domain.DatagramFreshnessWindow says
			// in as many words that the two windows are separate knobs, and on
			// a build where the base window is the shorter one this is the
			// ONLY term that keeps the write inside valid_until: the other two
			// are measured from the UNCLAMPED freshness end and from now.
			freshnessEnd := signed.Add(freshnessWindow)
			clampedValidUntil := signed.Add(2 * time.Minute)
			// Chosen so the queue residence lands after the write-grace bound
			// in BOTH classes, and the case cannot be satisfied by the first
			// two terms.
			now := signed.Add(115 * time.Second)

			want := clampedValidUntil.Add(-tc.writeGrace)
			got := sendWindowEnd(freshnessEnd, clampedValidUntil, now, tc.class)
			if !got.Equal(want) {
				t.Fatalf("send_until = %s with valid_until clamped to %s, want the write-grace "+
					"bound %s — a write may not run past valid_until", got, clampedValidUntil, want)
			}
			if !want.Before(freshnessEnd.Add(-wantSendGrace)) || !want.Before(now.Add(tc.residence)) {
				t.Fatalf("the case does not isolate the write grace: bound %s is not strictly "+
					"below the queue bound %s and the window bound %s",
					want, now.Add(tc.residence), freshnessEnd.Add(-wantSendGrace))
			}
		})
	}
}

// TestDeadlinesExpireWhenTheClampLandsBehindNow pins the `expired` outcome: the
// frame is alive, but there is no time left to write it, so it is not enqueued
// at all.
func TestDeadlinesExpireWhenTheClampLandsBehindNow(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	signed := deadlineTestInstant()
	now := signed.Add(5 * time.Minute).Add(-time.Second)

	decision := ComputeDeadlines(header, now)
	if decision.Outcome() != DeadlinesExpired {
		t.Fatalf("outcome = %s, want expired", decision.Outcome())
	}
	deadlines, ok := decision.Deadlines()
	if !ok {
		t.Fatal("an expired frame must still report its deadlines: it is alive, only unsendable")
	}
	if !deadlines.SendUntil().Before(now) {
		t.Fatalf("send_until = %s is not behind now = %s", deadlines.SendUntil(), now)
	}
	if now.After(deadlines.ValidUntil()) {
		t.Fatal("expired must not be reported for a frame that is actually stale")
	}
}

// TestDeadlinesRejectFrameFromTheFuture pins that valid_from is a refusal of
// its own: "too far in the future" cannot be expressed by a single deadline,
// which is why §2.2 uses an interval.
func TestDeadlinesRejectFrameFromTheFuture(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	now := deadlineTestInstant().Add(-6 * time.Minute)

	decision := ComputeDeadlines(header, now)
	if decision.Outcome() != DeadlinesNotYetValid {
		t.Fatalf("outcome = %s, want not_yet_valid", decision.Outcome())
	}
	if _, ok := decision.Deadlines(); ok {
		t.Fatal("a refused frame must not carry deadlines")
	}
}

// TestDeadlinesAreBoundedByTheBaseReplayWindow is the transit trade stated
// once, where it now lives: the node's only anti-replay state is the bounded
// in-memory cache, so nothing it carries may outlive what that cache can still
// recognise as a repeat.
func TestDeadlinesAreBoundedByTheBaseReplayWindow(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	signed := deadlineTestInstant()
	baseWindowEnd := signed.Add(domain.DatagramBaseReplayWindow)

	deadlines, ok := ComputeDeadlines(header, signed).Deadlines()
	if !ok {
		t.Fatal("an ordinary frame produced no deadlines")
	}
	if deadlines.ValidUntil().After(baseWindowEnd) {
		t.Fatalf("valid_until = %s outlives the base window %s", deadlines.ValidUntil(), baseWindowEnd)
	}
	if deadlines.ReplayUntil().After(baseWindowEnd) {
		t.Fatalf("replay_until = %s outlives the base window %s", deadlines.ReplayUntil(), baseWindowEnd)
	}
	// The invariant a lowered retention alone would break: below valid_until
	// there is a window in which a copy is both admissible and unrecognisable.
	if deadlines.ReplayUntil().Before(deadlines.ValidUntil()) {
		t.Fatalf("replay_until %s is below valid_until %s: the repeat window is back",
			deadlines.ReplayUntil(), deadlines.ValidUntil())
	}

	// Past the window the same frame is STALE rather than forwardable.
	late := ComputeDeadlines(header, baseWindowEnd.Add(time.Nanosecond))
	if late.Outcome() != DeadlinesStale {
		t.Fatalf("past the base window: outcome = %s, want stale", late.Outcome())
	}
	if _, forwardable := late.Deadlines(); forwardable {
		t.Fatal("a stale frame must carry no deadlines to forward it by")
	}
}

// TestDeadlinesCountFromTheSignedTime pins that every window is measured from
// auth.time and not from arrival, so a frame delayed in transit does not occupy
// a replay slot longer than one that arrived promptly.
func TestDeadlinesCountFromTheSignedTime(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	signed := deadlineTestInstant()

	prompt, ok := ComputeDeadlines(header, signed).Deadlines()
	if !ok {
		t.Fatal("prompt frame produced no deadlines")
	}
	delayed, ok := ComputeDeadlines(header, signed.Add(3*time.Minute)).Deadlines()
	if !ok {
		t.Fatal("delayed frame produced no deadlines")
	}
	if !prompt.ReplayUntil().Equal(delayed.ReplayUntil()) {
		t.Fatalf("replay_until moved with the arrival time: %s vs %s",
			prompt.ReplayUntil(), delayed.ReplayUntil())
	}
	if !prompt.ValidUntil().Equal(delayed.ValidUntil()) {
		t.Fatalf("valid_until moved with the arrival time: %s vs %s",
			prompt.ValidUntil(), delayed.ValidUntil())
	}
}

// TestDeadlineBoundariesAreInclusiveForLife is the single invariant of §2.2
// stated as a test: at now == replay_until == valid_until the frame is ALIVE
// and its key is ALIVE. A mismatch here would open a whole second of
// re-delivery at second-granularity time.
func TestDeadlineBoundariesAreInclusiveForLife(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	boundary := deadlineTestInstant().Add(5 * time.Minute)

	// At the boundary the frame is ALIVE — `expired` says exactly that: it is
	// unsendable, because the mandatory write_grace margin means the last write
	// had to START earlier. What it must NOT be is stale: local delivery and the
	// replay key still run off these deadlines.
	alive := ComputeDeadlines(header, boundary)
	if alive.Outcome() != DeadlinesExpired {
		t.Fatalf("at now == valid_until: outcome = %s, want expired (alive but unsendable)", alive.Outcome())
	}
	deadlines, _ := alive.Deadlines()
	if !deadlines.ValidUntil().Equal(boundary) || !deadlines.ReplayUntil().Equal(boundary) {
		t.Fatalf("boundary case must satisfy now == valid_until == replay_until, got %s and %s",
			deadlines.ValidUntil(), deadlines.ReplayUntil())
	}

	// Death is strictly past the bound.
	dead := ComputeDeadlines(header, boundary.Add(time.Nanosecond))
	if dead.Outcome() != DeadlinesStale {
		t.Fatalf("one nanosecond past valid_until: outcome = %s, want stale", dead.Outcome())
	}
}

// TestDeadlinesForAnOrdinaryFrame walks the whole arithmetic of §3.3 in one
// place, so the four instants are pinned together rather than each in isolation.
func TestDeadlinesForAnOrdinaryFrame(t *testing.T) {
	header := deadlineTestHeader(t, nil)
	signed := deadlineTestInstant()

	decision := ComputeDeadlines(header, signed)
	if decision.Outcome() != DeadlinesComputed {
		t.Fatalf("outcome = %s, want computed", decision.Outcome())
	}
	deadlines, _ := decision.Deadlines()
	checks := map[string]struct{ got, want time.Time }{
		"valid_from":   {deadlines.ValidFrom(), signed.Add(-5 * time.Minute)},
		"valid_until":  {deadlines.ValidUntil(), signed.Add(5 * time.Minute)},
		"replay_until": {deadlines.ReplayUntil(), signed.Add(5 * time.Minute)},
		// queue_residence(control) from `now` is the binding term for a frame
		// placed at the instant it was signed.
		"send_until": {deadlines.SendUntil(), signed.Add(5 * time.Second)},
	}
	for name, check := range checks {
		if !check.got.Equal(check.want) {
			t.Fatalf("%s = %s, want %s", name, check.got, check.want)
		}
	}
}
