package node

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
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
	// A single entry whose own marshal exceeds the budget cannot be
	// chunked further — reported via skipped and dropped from chunks.
	huge := json.RawMessage(strings.Repeat("a", 1024))
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1, Extra: huge},
	}
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, 128)
	if len(chunks) != 0 {
		t.Fatalf("oversize single entry must not produce chunks; got %d", len(chunks))
	}
	if len(skipped) != 1 || skipped[0] != 0 {
		t.Fatalf("expected skipped=[0], got %v", skipped)
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
