package node

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestChunkAnnounceEntriesBySize_EmptyReturnsNil documents the
// contract that callers depend on: an empty input MUST return nil
// chunks (not an empty slice) so that SendAnnounceRoutes /
// SendRoutesUpdate short-circuit to "success" instead of dispatching
// an empty announce frame, which would be wire noise and is
// forbidden by the protocol.
func TestChunkAnnounceEntriesBySize_EmptyReturnsNil(t *testing.T) {
	chunks, skipped := chunkAnnounceEntriesBySize(nil, announceWireLegacy, protocol.MaxFrameLine)
	if chunks != nil {
		t.Fatalf("nil input must return nil chunks, got %v", chunks)
	}
	if skipped != nil {
		t.Fatalf("nil input must return nil skipped, got %v", skipped)
	}
	chunks, skipped = chunkAnnounceEntriesBySize([]routing.AnnounceEntry{}, announceWireLegacy, protocol.MaxFrameLine)
	if chunks != nil {
		t.Fatalf("empty slice must return nil chunks, got %v", chunks)
	}
	if skipped != nil {
		t.Fatalf("empty slice must return nil skipped, got %v", skipped)
	}
}

// TestChunkAnnounceEntriesBySize_UnderLimitNoSplit pins the fast-path:
// an input that already fits inside a single frame returns exactly
// one chunk equal to the input. Without this fast-path, every cycle
// would dispatch multiple frames on the common single-frame case.
func TestChunkAnnounceEntriesBySize_UnderLimitNoSplit(t *testing.T) {
	entries := makeAnnounceEntries(50)
	chunks, skipped := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine)
	if len(chunks) != 1 {
		t.Fatalf("under-limit input must return 1 chunk, got %d", len(chunks))
	}
	if len(chunks[0]) != len(entries) {
		t.Fatalf("under-limit chunk must equal input length, got %d want %d", len(chunks[0]), len(entries))
	}
	if len(skipped) != 0 {
		t.Fatalf("under-limit input must skip nothing, got %v", skipped)
	}
}

// TestChunkAnnounceEntriesBySize_OversizeSplitsBySize exercises the
// size-aware split: 2000 routes at production-shape Identity/Origin
// payloads must be sliced into multiple chunks where every chunk's
// serialized announce frame is under MaxFrameLine and the
// concatenation of chunks equals the input in order.
func TestChunkAnnounceEntriesBySize_OversizeSplitsBySize(t *testing.T) {
	entries := makeAnnounceEntries(2000)
	chunks, skipped := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine)
	if len(chunks) < 2 {
		t.Fatalf("expected oversize input to split into multiple chunks, got %d", len(chunks))
	}
	if len(skipped) != 0 {
		t.Fatalf("normal-shaped entries must not be skipped, got %v", skipped)
	}
	rejoined := 0
	for i, chunk := range chunks {
		frame := buildAnnounceFrame(announceWireLegacy, chunk)
		line, err := protocol.MarshalFrameLineWithLimit(frame, protocol.MaxFrameLine)
		if err != nil {
			t.Fatalf("chunk %d failed MarshalFrameLineWithLimit: %v (size guard mistuned)", i, err)
		}
		if len(line) > protocol.MaxFrameLine {
			t.Fatalf("chunk %d serialized to %d bytes, exceeds MaxFrameLine %d",
				i, len(line), protocol.MaxFrameLine)
		}
		// Verify chunk content is contiguous slice of input.
		for j, e := range chunk {
			want := entries[rejoined+j]
			if e.Identity != want.Identity || e.Origin != want.Origin {
				t.Fatalf("chunk %d entry %d out of order: got id=%s want id=%s", i, j, e.Identity, want.Identity)
			}
		}
		rejoined += len(chunk)
	}
	if rejoined != len(entries) {
		t.Fatalf("rejoined chunk lengths %d != input %d (lost or duplicated entries)", rejoined, len(entries))
	}
}

// TestChunkAnnounceEntriesBySize_LargeExtraTriggersSplit proves that
// the chunker reacts to byte size, not entry count. Entries with
// large Extra payloads consume more wire space per route, so a
// modest count of "fat" entries must split into multiple chunks.
func TestChunkAnnounceEntriesBySize_LargeExtraTriggersSplit(t *testing.T) {
	// Each entry carries ~10 KiB Extra — 20 of them encode well
	// above MaxFrameLine. This forces the chunker to split even
	// though the entry count is small.
	const fatExtraSize = 10 * 1024
	const count = 20
	entries := make([]routing.AnnounceEntry, count)
	extraBlob, err := json.Marshal(map[string]string{"blob": strings.Repeat("z", fatExtraSize)})
	if err != nil {
		t.Fatalf("extra blob marshal: %v", err)
	}
	for i := 0; i < count; i++ {
		entries[i] = routing.AnnounceEntry{
			Identity: domaintest.ID("id" + intToHex4(i)),
			Origin:   domaintest.ID("og" + intToHex4(i)),
			Hops:     1,
			SeqNo:    uint64(i + 1),
			Extra:    extraBlob,
		}
	}
	chunks, skipped := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine)
	if len(skipped) != 0 {
		t.Fatalf("each fat entry alone fits under MaxFrameLine, skipped should be empty, got %v", skipped)
	}
	if len(chunks) < 2 {
		t.Fatalf("fat-Extra entries must trigger a multi-chunk split, got %d chunks", len(chunks))
	}
	for i, chunk := range chunks {
		frame := buildAnnounceFrame(announceWireLegacy, chunk)
		if _, err := protocol.MarshalFrameLineWithLimit(frame, protocol.MaxFrameLine); err != nil {
			t.Fatalf("chunk %d failed size guard: %v", i, err)
		}
	}
}

// TestChunkAnnounceEntriesBySize_SingleEntryTooLargeSkipped pins the
// fault-isolation contract: when one entry's own Extra blob alone
// exceeds the budget, the entry is reported via skipped and the
// remaining entries are still chunked normally. Without this, a
// single broken entry would silently take down the entire announce
// frame and stall convergence for unrelated routes.
func TestChunkAnnounceEntriesBySize_SingleEntryTooLargeSkipped(t *testing.T) {
	// Build a giant Extra that, even alone, blows past MaxFrameLine.
	giantExtra, err := json.Marshal(map[string]string{"blob": strings.Repeat("z", protocol.MaxFrameLine+1024)})
	if err != nil {
		t.Fatalf("giant extra marshal: %v", err)
	}
	entries := []routing.AnnounceEntry{
		{
			Identity: domaintest.ID("id" + intToHex4(0)),
			Origin:   domaintest.ID("og" + intToHex4(0)),
			Hops:     1,
			SeqNo:    1,
		},
		{
			Identity: domaintest.ID("id" + intToHex4(1)),
			Origin:   domaintest.ID("og" + intToHex4(1)),
			Hops:     1,
			SeqNo:    2,
			Extra:    giantExtra,
		},
		{
			Identity: domaintest.ID("id" + intToHex4(2)),
			Origin:   domaintest.ID("og" + intToHex4(2)),
			Hops:     1,
			SeqNo:    3,
		},
	}
	chunks, skipped := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine)
	if len(skipped) != 1 || skipped[0] != 1 {
		t.Fatalf("expected exactly one skipped index [1], got %v", skipped)
	}
	// Remaining two entries must still produce at least one chunk.
	if len(chunks) == 0 {
		t.Fatal("non-oversize entries must still be chunked, got 0 chunks")
	}
	totalRouted := 0
	for _, chunk := range chunks {
		totalRouted += len(chunk)
	}
	if totalRouted != 2 {
		t.Fatalf("expected 2 entries to be routed (skipping the oversize one), got %d", totalRouted)
	}
}

// TestAnnounceEntryWireEstimate_IsOverEstimate pins the safety invariant the
// legacy/v2 chunker's (chunkAnnounceEntriesBySize) fast-accept gate depends
// on: announceEntryWireEstimate must never under-estimate the bytes an entry
// actually adds to a frame. (The v3 chunker sizes exactly and does not use
// this estimate — see TestChunkRouteAnnounceV3_ExactSizingIsPreciseAndSafe.)
// If it did, a chunk accepted on the cheap estimate (curEst <= maxBytes/2)
// could marshal past maxBytes and be rejected on the wire.  We measure the
// real contribution as the marshalled-size delta of adding the entry to a
// one-entry base frame (the delta includes the array comma) and require the
// estimate to be >= that for a spread of entry shapes.
func TestAnnounceEntryWireEstimate_IsOverEstimate(t *testing.T) {
	base := routing.AnnounceEntry{
		Identity: domaintest.ID("a"),
		Origin:   domaintest.ID("b"),
		Hops:     1,
		SeqNo:    1,
	}
	baseLine, err := protocol.MarshalFrameLineWithLimit(
		buildAnnounceFrame(announceWireLegacy, []routing.AnnounceEntry{base}),
		protocol.MaxFrameLine,
	)
	if err != nil {
		t.Fatalf("base frame marshal: %v", err)
	}

	smallExtra, _ := json.Marshal(map[string]string{"k": strings.Repeat("z", 200)})
	bigExtra, _ := json.Marshal(map[string]string{"k": strings.Repeat("z", 4096)})
	// escapeExtra carries LITERAL '<' bytes (a valid JSON string char). On
	// re-encode each expands to "<" (1→6 bytes), so the entry's real
	// wire contribution is ~6× its raw length.  This is the exact shape that
	// can balloon past MaxFrameLine after a small raw Extra slips the gate —
	// the case fails if announceEntryWireEstimate counts Extra at raw length
	// instead of worst-case escape expansion.
	escapeExtra := json.RawMessage(`{"k":"` + strings.Repeat("<", 500) + `"}`)
	cases := []routing.AnnounceEntry{
		{Identity: domaintest.ID("c"), Origin: domaintest.ID("d"), Hops: 15, SeqNo: 1 << 40},
		{Identity: domaintest.ID("short"), Origin: domaintest.ID("x"), Hops: 0, SeqNo: 0},
		{Identity: domaintest.ID("e"), Origin: domaintest.ID("f"), Hops: 3, SeqNo: 42, Extra: smallExtra},
		{Identity: domaintest.ID("g"), Origin: domaintest.ID("h"), Hops: 7, SeqNo: 99, Extra: bigExtra},
		{Identity: domaintest.ID("i"), Origin: domaintest.ID("j"), Hops: 2, SeqNo: 7, Extra: escapeExtra},
		// Identity/Origin are now fixed-width [20]byte fingerprints, so they
		// serialize to a constant 40-hex string regardless of input — the
		// per-field escape-overhead concern that the original variable-length
		// string identities exercised no longer applies to these fields. The
		// escape-expansion coverage now lives entirely in the Extra cases above.
		{Identity: domaintest.ID("idlt"), Origin: domaintest.ID("ogamp"), Hops: 1, SeqNo: 1},
	}
	for i, e := range cases {
		twoLine, err := protocol.MarshalFrameLineWithLimit(
			buildAnnounceFrame(announceWireLegacy, []routing.AnnounceEntry{base, e}),
			protocol.MaxFrameLine,
		)
		if err != nil {
			t.Fatalf("case %d two-entry frame marshal: %v", i, err)
		}
		delta := len(twoLine) - len(baseLine)
		if est := announceEntryWireEstimate(e); est < delta {
			t.Fatalf("case %d: estimate %d UNDER real per-entry delta %d — fast-accept gate is unsafe", i, est, delta)
		}
	}
}

// makeAnnounceEntries builds n distinct AnnounceEntry rows with stable
// non-trivial Identity / Origin / Extra payloads so that the encoded
// JSON has roughly the same shape as production routes — this keeps
// the per-chunk size estimate honest.
func makeAnnounceEntries(n int) []routing.AnnounceEntry {
	out := make([]routing.AnnounceEntry, n)
	// Distinct fixed-width identities per index so the encoded entries are
	// not identical. Each serializes to a constant 40-hex string on the wire,
	// matching the real identity footprint.
	for i := 0; i < n; i++ {
		out[i] = routing.AnnounceEntry{
			Identity: domaintest.ID("id" + intToHex4(i)),
			Origin:   domaintest.ID("og" + intToHex4(i)),
			Hops:     i % 16,
			SeqNo:    uint64(i + 1),
		}
	}
	return out
}

// intToHex4 returns a 4-character hex representation of n, padded with
// '0'. Avoids an fmt dependency in the test helper hot path.
func intToHex4(n int) string {
	const hex = "0123456789abcdef"
	b := []byte{
		hex[(n>>12)&0xf],
		hex[(n>>8)&0xf],
		hex[(n>>4)&0xf],
		hex[n&0xf],
	}
	return string(b)
}
