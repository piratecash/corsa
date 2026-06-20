package node

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestRouteSyncEntries_WireRoundTrip pins the Phase-0 version-vector
// wire conversion: routing.DigestEntry -> wire (Identity hex, SeqNo) ->
// routing.DigestEntry survives unchanged, so the receiver reconciles
// against the same (Identity, SeqNo) pairs the sender announced.
func TestRouteSyncEntries_WireRoundTrip(t *testing.T) {
	idA := domaintest.ID("a")
	idB := domaintest.ID("b")
	in := []routing.DigestEntry{
		{Identity: idA, MaxSeqNo: 7},
		{Identity: idB, MaxSeqNo: 9},
	}

	wire := routeSyncEntriesToWire(in)
	if len(wire) != 2 {
		t.Fatalf("wire len = %d, want 2", len(wire))
	}
	if wire[0].Identity != idA.String() || wire[0].SeqNo != 7 {
		t.Fatalf("wire[0] = %+v, want (%s, 7)", wire[0], idA.String())
	}

	out := routeSyncEntriesFromWire(wire)
	if len(out) != 2 {
		t.Fatalf("decoded len = %d, want 2", len(out))
	}
	if out[0].Identity != idA || out[0].MaxSeqNo != 7 ||
		out[1].Identity != idB || out[1].MaxSeqNo != 9 {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

// TestRouteSyncEntries_EmptyIsNil — an empty vector marshals to nil so
// the frame omits the entries field (omitempty) and a legacy peer's
// vector-less frame decodes to nil, keeping the hash fallback path.
func TestRouteSyncEntries_EmptyIsNil(t *testing.T) {
	if got := routeSyncEntriesToWire(nil); got != nil {
		t.Fatalf("toWire(nil) = %+v, want nil", got)
	}
	if got := routeSyncEntriesFromWire(nil); got != nil {
		t.Fatalf("fromWire(nil) = %+v, want nil", got)
	}
}

// TestBuildRouteSyncDigestWire_DropsVectorOverFrameLimit — a version
// vector large enough to push the wire line past protocol.MaxFrameLine
// (128 KiB) must be dropped so the command-plane reader does not close the
// frame; the receiver then degrades to the hash-only compare. ~2500 entries
// at worst-case SeqNo is ~150 KiB > 128 KiB, so the guard must fire.
func TestBuildRouteSyncDigestWire_DropsVectorOverFrameLimit(t *testing.T) {
	entries := make([]routing.DigestEntry, 2500)
	for i := range entries {
		entries[i] = routing.DigestEntry{Identity: domaintest.ID(fmt.Sprintf("id-%d", i)), MaxSeqNo: math.MaxUint64}
	}

	wire, err := buildRouteSyncDigestWire("deadbeef", entries, uint32(len(entries)), time.Now())
	if err != nil {
		t.Fatalf("buildRouteSyncDigestWire: %v", err)
	}
	if len(wire.RawLine) > protocol.MaxFrameLine {
		t.Fatalf("wire line = %d bytes, exceeds MaxFrameLine %d — the reader would close it", len(wire.RawLine), protocol.MaxFrameLine)
	}

	// The vector must have been dropped (hash-only fallback), not merely
	// trimmed, so the receiver falls back to the whole-table hash compare.
	frame, err := protocol.UnmarshalRouteSyncDigestFrame([]byte(strings.TrimSuffix(wire.RawLine, "\n")))
	if err != nil {
		t.Fatalf("unmarshal capped frame: %v", err)
	}
	if frame.Entries != nil {
		t.Fatalf("oversized vector not dropped: %d entries still present", len(frame.Entries))
	}
	if frame.Digest != "deadbeef" {
		t.Fatalf("hash-only fallback lost the digest: %q", frame.Digest)
	}
}

// TestBuildRouteSyncDigestWire_KeepsVectorUnderLimit — a vector that fits
// is carried verbatim (no spurious drop).
func TestBuildRouteSyncDigestWire_KeepsVectorUnderLimit(t *testing.T) {
	entries := []routing.DigestEntry{
		{Identity: domaintest.ID("a"), MaxSeqNo: 7},
		{Identity: domaintest.ID("b"), MaxSeqNo: 9},
	}
	wire, err := buildRouteSyncDigestWire("cafe", entries, 2, time.Now())
	if err != nil {
		t.Fatalf("buildRouteSyncDigestWire: %v", err)
	}
	if len(wire.RawLine) > protocol.MaxFrameLine {
		t.Fatalf("small frame unexpectedly over MaxFrameLine: %d", len(wire.RawLine))
	}
	frame, err := protocol.UnmarshalRouteSyncDigestFrame([]byte(strings.TrimSuffix(wire.RawLine, "\n")))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(frame.Entries) != 2 {
		t.Fatalf("vector under the limit was dropped/trimmed: %d entries, want 2", len(frame.Entries))
	}
}

// TestRouteSyncEntries_FromWireDropsZeroIdentity — a malformed/empty
// identity hex cannot key any stored claim's uplink bucket, so it is
// dropped on decode rather than producing a zero-identity entry that
// would never match.
func TestRouteSyncEntries_FromWireDropsZeroIdentity(t *testing.T) {
	wire := []protocol.RouteSyncDigestEntry{
		{Identity: "", SeqNo: 1},
		{Identity: domaintest.ID("a").String(), SeqNo: 2},
	}
	out := routeSyncEntriesFromWire(wire)
	if len(out) != 1 {
		t.Fatalf("decoded len = %d, want 1 (zero identity dropped)", len(out))
	}
	if out[0].MaxSeqNo != 2 {
		t.Fatalf("kept entry SeqNo = %d, want 2", out[0].MaxSeqNo)
	}
}
