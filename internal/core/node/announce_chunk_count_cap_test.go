package node

import (
	"fmt"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_chunk_count_cap_test.go pins the Phase 4 13.5 hard cap on
// per-frame entry count (maxRoutesPerAnnounceFrame=100), independent
// of the byte-size budget (MaxFrameLine). The cap applies to both
// the legacy chunker (chunkAnnounceEntriesBySize) and the v3
// chunker (chunkRouteAnnounceV3EntriesBySize) so the receive-side
// per-frame work budget stays bounded across wire generations.

// buildSmallEntries materialises n tiny AnnounceEntry values whose
// per-entry wire size is well under MaxFrameLine — so we exercise
// the entry-count cap WITHOUT the byte-size budget firing first.
func buildSmallEntries(n int) []routing.AnnounceEntry {
	entries := make([]routing.AnnounceEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = routing.AnnounceEntry{
			// 40-hex fingerprint shape so identity.IsValidAddress
			// passes upstream; varying the last few bytes keeps
			// entries distinct.
			Identity: domain.PeerIdentity(fmt.Sprintf("aa%038x", i)),
			Origin:   domain.PeerIdentity(fmt.Sprintf("bb%038x", i)),
			Hops:     2,
			SeqNo:    uint64(i + 1),
		}
	}
	return entries
}

func TestChunkAnnounceEntriesBySize_CapsAt100PerFrame(t *testing.T) {
	entries := buildSmallEntries(250) // 2.5x the cap
	chunks, skipped := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine)
	if len(skipped) != 0 {
		t.Fatalf("no entry should be skipped on small-payload chunking; got %d skipped", len(skipped))
	}
	if len(chunks) < 3 {
		t.Fatalf("250 entries / 100-cap must produce >=3 chunks; got %d", len(chunks))
	}
	for i, c := range chunks {
		if len(c) > maxRoutesPerAnnounceFrame {
			t.Fatalf("chunk %d has %d entries > cap %d", i, len(c), maxRoutesPerAnnounceFrame)
		}
	}
	// Total entries preserved across chunks.
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	if total != 250 {
		t.Fatalf("chunked total %d != input 250", total)
	}
}

func TestChunkAnnounceEntriesBySize_V2WireSameCap(t *testing.T) {
	// v2 wire uses the same shape and same chunker — cap applies
	// identically.
	entries := buildSmallEntries(150)
	chunks, _ := chunkAnnounceEntriesBySize(entries, announceWireV2, protocol.MaxFrameLine)
	for _, c := range chunks {
		if len(c) > maxRoutesPerAnnounceFrame {
			t.Fatalf("v2 chunk exceeded cap: %d > %d", len(c), maxRoutesPerAnnounceFrame)
		}
	}
}

func TestChunkRouteAnnounceV3EntriesBySize_CapsAt100PerFrame(t *testing.T) {
	entries := buildSmallEntries(250)
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, 1, protocol.MaxFrameLine)
	if len(skipped) != 0 {
		t.Fatalf("no entry should be skipped on small-payload v3 chunking; got %d skipped", len(skipped))
	}
	if len(chunks) < 3 {
		t.Fatalf("v3: 250 entries / 100-cap must produce >=3 chunks; got %d", len(chunks))
	}
	for i, c := range chunks {
		if len(c) > maxRoutesPerAnnounceFrame {
			t.Fatalf("v3 chunk %d has %d entries > cap %d", i, len(c), maxRoutesPerAnnounceFrame)
		}
	}
}

func TestChunkCount_UnderCapStaysSingleChunk(t *testing.T) {
	// Small input (well under cap) must NOT introduce extra chunk
	// splits — the cap is a ceiling, not a quota.
	entries := buildSmallEntries(10)
	if chunks, _ := chunkAnnounceEntriesBySize(entries, announceWireLegacy, protocol.MaxFrameLine); len(chunks) != 1 || len(chunks[0]) != 10 {
		t.Fatalf("under-cap input must fit in one chunk; got %d chunks", len(chunks))
	}
	if chunks, _ := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindDelta, 1, protocol.MaxFrameLine); len(chunks) != 1 || len(chunks[0]) != 10 {
		t.Fatalf("v3 under-cap input must fit in one chunk; got %d chunks", len(chunks))
	}
}
