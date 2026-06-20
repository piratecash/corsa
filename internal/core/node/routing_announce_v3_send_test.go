package node

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_announce_v3_send_test.go covers the Phase 4 compact-announce
// send primitives (phase-4 §3.1, overview §7.1): build round-trip via
// RawLine, hops int→uint8 narrowing with the HopsInfinity-aware clamp,
// chunking that respects MaxFrameLine, and SendRouteAnnounceV3 dispatch
// through the capability-gated path.

func TestBuildRouteAnnounceV3Frame_RoundTripPreservesEntries(t *testing.T) {
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 2, SeqNo: 7, Extra: json.RawMessage(`{"k":"v"}`)},
		{Identity: idPeerB, Origin: idOriginC, Hops: int(routing.HopsInfinity), SeqNo: 8},
	}

	frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 42, entries)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if frame.Type != protocol.RouteAnnounceV3FrameType {
		t.Fatalf("Type: got %q want %q", frame.Type, protocol.RouteAnnounceV3FrameType)
	}
	if !strings.HasSuffix(frame.RawLine, "\n") {
		t.Fatalf("RawLine must end with newline (line-framed transport)")
	}

	raw := strings.TrimRight(frame.RawLine, "\n")
	got, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(raw))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Kind != protocol.RouteAnnounceV3KindFull || got.Epoch != 42 {
		t.Fatalf("header: got kind=%q epoch=%d want full/42", got.Kind, got.Epoch)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("entries: got %d want 2", len(got.Entries))
	}
	if got.Entries[0].Identity != idTargetX.String() || got.Entries[0].Hops != 2 || got.Entries[0].SeqNo != 7 {
		t.Fatalf("entry 0: %+v", got.Entries[0])
	}
	if string(got.Entries[0].Extra) != `{"k":"v"}` {
		t.Fatalf("entry 0 Extra forwarded wrong: got %q", got.Entries[0].Extra)
	}
	// HopsInfinity withdrawal marker survives int→uint8.
	if got.Entries[1].Hops != uint8(routing.HopsInfinity) {
		t.Fatalf("withdrawal hops: got %d want %d", got.Entries[1].Hops, routing.HopsInfinity)
	}
	// Compact-format guarantee: no Origin on the wire.
	if strings.Contains(frame.RawLine, "origin") {
		t.Fatalf("v3 RawLine leaked origin: %s", frame.RawLine)
	}
}

func TestBuildRouteAnnounceV3Frame_HopsClampedAtHopsInfinity(t *testing.T) {
	// A pathological upstream hop count above HopsInfinity must clamp to
	// HopsInfinity so it still reads as a withdrawal after type narrowing
	// (uint8 wrap would otherwise turn 257 into 1 — a live route).
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 257},
	}
	frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindDelta, 1, entries)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(strings.TrimRight(frame.RawLine, "\n")))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Entries[0].Hops != uint8(routing.HopsInfinity) {
		t.Fatalf("out-of-range hops clamp: got %d want %d", got.Entries[0].Hops, routing.HopsInfinity)
	}
}

func TestBuildRouteAnnounceV3Frame_RejectsInvalidKind(t *testing.T) {
	if _, err := buildRouteAnnounceV3Frame("snapshot", 1, nil); err == nil {
		t.Fatal("expected invalid-kind error")
	}
}

func TestChunkRouteAnnounceV3_NilAndEmpty(t *testing.T) {
	if chunks, skipped := chunkRouteAnnounceV3EntriesBySize(nil, protocol.RouteAnnounceV3KindFull, 1, protocol.MaxFrameLine); chunks != nil || skipped != nil {
		t.Fatalf("nil input: got chunks=%v skipped=%v want nil/nil", chunks, skipped)
	}
	if chunks, skipped := chunkRouteAnnounceV3EntriesBySize([]routing.AnnounceEntry{}, protocol.RouteAnnounceV3KindFull, 1, protocol.MaxFrameLine); chunks != nil || skipped != nil {
		t.Fatalf("empty input: got chunks=%v skipped=%v want nil/nil", chunks, skipped)
	}
}

func TestChunkRouteAnnounceV3_SmallSetSingleChunk(t *testing.T) {
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		{Identity: idPeerB, Origin: idOriginC, Hops: 2, SeqNo: 2},
	}
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, protocol.MaxFrameLine)
	if len(chunks) != 1 || len(chunks[0]) != 2 {
		t.Fatalf("small set must fit one chunk; got %d chunk(s) sizes=%v", len(chunks), chunkSizes(chunks))
	}
	if len(skipped) != 0 {
		t.Fatalf("no entry should be skipped; got %v", skipped)
	}
}

func TestChunkRouteAnnounceV3_SplitsWhenBudgetTight(t *testing.T) {
	// Force a split with a tight per-frame budget. Two small entries with a
	// budget that fits exactly one entry per chunk → two chunks.
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		{Identity: idPeerB, Origin: idOriginC, Hops: 1, SeqNo: 1},
	}
	// Measure a single-entry frame to size the budget at exactly that.
	frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 1, entries[:1])
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	budget := len(frame.RawLine)
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, budget)
	if len(chunks) != 2 || len(chunks[0]) != 1 || len(chunks[1]) != 1 {
		t.Fatalf("budget=%d (single-entry size) must split; got %d chunks sizes=%v", budget, len(chunks), chunkSizes(chunks))
	}
	if len(skipped) != 0 {
		t.Fatalf("no entry should be skipped on a split; got %v", skipped)
	}
}

func TestChunkRouteAnnounceV3_SkipsSingleEntryOverBudget(t *testing.T) {
	// A single entry whose own VALID one-entry frame exceeds the budget cannot
	// be chunked further — it hits the soloLen > maxBytes branch, is reported
	// via skipped and dropped from chunks. Extra must be valid JSON so the size
	// probe succeeds and the size guard (not the marshal-error guard) fires —
	// invalid Extra is a different path, pinned in the test below.
	bigExtra := json.RawMessage(`{"blob":"` + strings.Repeat("a", 1024) + `"}`)
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1, Extra: bigExtra},
	}
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, 128)
	if len(chunks) != 0 {
		t.Fatalf("oversize single entry must not produce chunks; got %d", len(chunks))
	}
	if len(skipped) != 1 || skipped[0] != 0 {
		t.Fatalf("expected skipped=[0], got %v", skipped)
	}
}

func TestChunkRouteAnnounceV3_SkipsEntryWithInvalidExtra(t *testing.T) {
	// An entry whose Extra is not valid JSON can never be marshalled into a
	// frame, so the size probe fails and the entry is skipped — while the
	// well-formed entries around it still ship. This pins the side effect of
	// the exact-sizing rewrite: every entry is now encoded during sizing, so a
	// poison entry is reported individually instead of being fast-accepted and
	// later failing the whole chunk on the send path.
	badExtra := json.RawMessage(strings.Repeat("a", 64)) // not valid JSON.
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		{Identity: idPeerB, Origin: idOriginC, Hops: 1, SeqNo: 2, Extra: badExtra},
		{Identity: idOriginC, Origin: idOriginC, Hops: 1, SeqNo: 3},
	}
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, protocol.MaxFrameLine)
	if len(skipped) != 1 || skipped[0] != 1 {
		t.Fatalf("expected the invalid-Extra entry at index 1 to be skipped; got %v", skipped)
	}
	// The two valid entries still ship and every produced chunk is a real,
	// marshallable frame (the poison entry never reaches one).
	shipped := 0
	for ci, chunk := range chunks {
		if _, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 1, chunk); err != nil {
			t.Fatalf("chunk %d must be a marshallable frame, got %v", ci, err)
		}
		shipped += len(chunk)
	}
	if shipped != 2 {
		t.Fatalf("expected 2 valid entries to ship around the poison one, got %d", shipped)
	}
}

// TestChunkRouteAnnounceV3_ExactSizingIsPreciseAndSafe pins the O(N)
// incremental size decomposition (baseLen + Σ entryLen + commas) against
// ground truth — the actually marshalled frame for every produced chunk —
// guarding both failure directions of the rewrite that dropped the old
// O(N²) "re-marshal the whole growing candidate" probe:
//
//   - UNDER-sizing (predicted < real) would overflow the wire limit, so every
//     produced chunk's real RawLine must be ≤ budget;
//   - OVER-sizing (predicted > real) would split too eagerly and waste frames,
//     so each non-final chunk must be greedily maximal: appending the first
//     entry of the next chunk to it must actually exceed the budget.
//
// Entries are deliberately heterogeneous — SeqNo spans 1..~20 JSON digits,
// a third carry an Extra object, a third carry a (base64-encoded) signature —
// so any per-entry JSON length subtlety the standalone-marshal probe might
// miss surfaces here as a real-frame overflow or a non-maximal chunk.
func TestChunkRouteAnnounceV3_ExactSizingIsPreciseAndSafe(t *testing.T) {
	const n = 600
	entries := make([]routing.AnnounceEntry, n)
	for i := 0; i < n; i++ {
		e := routing.AnnounceEntry{
			Identity: domaintest.ID("tgt" + intToHex4(i)),
			Origin:   domaintest.ID("org" + intToHex4(i%7)),
			Hops:     (i % 15) + 1,
			// Widen the seq_no JSON across 1..20 digits so the per-entry size
			// is not constant — a fixed-width estimate would pass trivially.
			SeqNo: uint64(i)*0x0123456789abc + 1,
		}
		switch i % 3 {
		case 0:
			// Include HTML-escapable chars (< > &) so the size probe's
			// json.Encoder and the frame's json.Marshal must agree on the
			// \u00xx expansion — a mismatch would surface as overflow below.
			e.Extra = json.RawMessage(`{"x":"<` + intToHex4(i) + `>&"}`)
		case 1:
			e.AttestedSig = []byte(strings.Repeat("s", (i%40)+1))
		}
		entries[i] = e
	}

	// Budget fits a handful of entries per chunk → many size-driven splits,
	// all well under the maxRoutesPerAnnounceFrame=100 count cap.
	const budget = 600
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 9, budget)
	if len(skipped) != 0 {
		t.Fatalf("no entry individually exceeds the budget; skipped must be empty, got %v", skipped)
	}
	if len(chunks) < 2 {
		t.Fatalf("budget=%d must force multiple chunks, got %d", budget, len(chunks))
	}

	rejoined := 0
	for ci, chunk := range chunks {
		frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 9, chunk)
		if err != nil {
			t.Fatalf("chunk %d build: %v", ci, err)
		}
		if got := len(frame.RawLine); got > budget {
			t.Fatalf("chunk %d real frame is %d bytes, exceeds budget %d (under-sizing — wire overflow)", ci, got, budget)
		}
		for j, e := range chunk {
			want := entries[rejoined+j]
			if e.SeqNo != want.SeqNo || e.Identity != want.Identity {
				t.Fatalf("chunk %d entry %d out of order", ci, j)
			}
		}
		rejoined += len(chunk)
	}
	if rejoined != n {
		t.Fatalf("rejoined %d != input %d (lost or duplicated entries)", rejoined, n)
	}

	// Greedy maximality: each non-final chunk plus the first entry of the next
	// chunk MUST overflow — otherwise the sizing over-estimated and split early.
	for ci := 0; ci+1 < len(chunks); ci++ {
		if len(chunks[ci]) >= maxRoutesPerAnnounceFrame {
			continue // split forced by the entry-count cap, not by size.
		}
		probe := append(append([]routing.AnnounceEntry{}, chunks[ci]...), chunks[ci+1][0])
		frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 9, probe)
		if err != nil {
			t.Fatalf("probe build at chunk %d: %v", ci, err)
		}
		if got := len(frame.RawLine); got <= budget {
			t.Fatalf("chunk %d not greedily maximal: + next chunk's first entry = %d ≤ budget %d (over-sizing — premature split)", ci, got, budget)
		}
	}
}

func TestSendRouteAnnounceV3_EmitsRawLineFrameOverCapableSession(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	senderAddr := domain.PeerAddress("addr-peerB-v3")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
	}
	ok := svc.SendRouteAnnounceV3(context.Background(), senderAddr, protocol.RouteAnnounceV3KindFull, 7, entries)
	if !ok {
		t.Fatalf("SendRouteAnnounceV3 returned false against a capable session")
	}

	select {
	case got := <-sendCh:
		if got.Type != protocol.RouteAnnounceV3FrameType {
			t.Fatalf("frame Type: got %q want %q", got.Type, protocol.RouteAnnounceV3FrameType)
		}
		parsed, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(strings.TrimRight(got.RawLine, "\n")))
		if err != nil {
			t.Fatalf("RawLine did not parse as v3: %v (raw=%q)", err, got.RawLine)
		}
		if parsed.Kind != protocol.RouteAnnounceV3KindFull || parsed.Epoch != 7 || len(parsed.Entries) != 1 {
			t.Fatalf("parsed header/entries wrong: %+v", parsed)
		}
		if parsed.Entries[0].Identity != idTargetX.String() || parsed.Entries[0].SeqNo != 1 {
			t.Fatalf("entry payload wrong: %+v", parsed.Entries[0])
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("no frame was emitted on the session within 100ms")
	}
}

func TestSendRouteAnnounceV3_DropsAndReturnsFalseWhenPeerLacksV3Cap(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	senderAddr := domain.PeerAddress("addr-peerB-novthree")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: idPeerB,
		// v1+v2+relay — no v3 cap. The v3-gated send must refuse to ship
		// to a session that did not negotiate the compact frame.
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
	}
	if svc.SendRouteAnnounceV3(context.Background(), senderAddr, protocol.RouteAnnounceV3KindFull, 1, entries) {
		t.Fatalf("SendRouteAnnounceV3 must return false when peer lacks v3 cap")
	}
	select {
	case got := <-sendCh:
		t.Fatalf("no frame must be enqueued without v3 cap; got %q", got.Type)
	default:
	}
}

func TestSendRouteAnnounceV3_EmptyEntriesReturnsTrueWithoutWire(t *testing.T) {
	// Empty-entry contract mirrors SendAnnounceRoutes / SendRoutesUpdate:
	// nothing to ship → success without touching the transport.
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	if !svc.SendRouteAnnounceV3(context.Background(), domain.PeerAddress("noop"), protocol.RouteAnnounceV3KindDelta, 1, nil) {
		t.Fatal("empty entries must short-circuit to success")
	}
}

// chunkSizes is a tiny helper for fatal messages.
func chunkSizes(chunks [][]routing.AnnounceEntry) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}
