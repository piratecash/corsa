package protocol

import (
	"strconv"
	"strings"
	"testing"
)

// buildLegacyAnnounceRoutesLine produces a representative LEGACY announce_routes
// wire line carrying n route entries — the "routes" array shape that
// ParseFrameLine fully materialises into Frame.AnnounceRoutes (each via
// AnnounceRouteFrame.UnmarshalJSON). This is the heaviest ParseFrameLine path
// and the one whose scratch-buffer copy the frameLineBufPool change removes.
// Returned without the trailing newline (ParseFrameLine operates on the
// already-split line).
func buildLegacyAnnounceRoutesLine(n int) string {
	var b strings.Builder
	b.WriteString(`{"type":"announce_routes","version":11,"routes":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"identity":"`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`f1445dd70a9c7372b69047b7122150e98911ce1","origin":"2c3c2b9b99927ae6c6783726ea0250685029d107","hops":`)
		b.WriteString(strconv.Itoa((i % 9) + 1))
		b.WriteString(`,"seq":`)
		b.WriteString(strconv.FormatUint(uint64(1000+i), 10))
		b.WriteByte('}')
	}
	b.WriteString(`]}`)
	return b.String()
}

// buildV3AnnounceLine produces a route_announce_v3 wire line — the production
// churn shape (route_announce_v3_delta). NOTE: ParseFrameLine only parses the
// envelope of this line; the "entries" array is not a Frame field, so it is
// ignored here and decoded later by UnmarshalRouteAnnounceV3Frame off
// Frame.RawLine. This case exists to show the pooled-buffer win still applies
// to the v3 line that actually dominates production traffic, even though the
// per-entry decode happens elsewhere.
func buildV3AnnounceLine(n int) string {
	var b strings.Builder
	b.WriteString(`{"type":"route_announce_v3","kind":"delta","epoch":7,"entries":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"identity":"`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`f1445dd70a9c7372b69047b7122150e98911ce1","hops":`)
		b.WriteString(strconv.Itoa((i % 9) + 1))
		b.WriteString(`,"seq_no":`)
		b.WriteString(strconv.FormatUint(uint64(1000+i), 10))
		b.WriteByte('}')
	}
	b.WriteString(`]}`)
	return b.String()
}

func BenchmarkParseFrameLine(b *testing.B) {
	shapes := []struct {
		name  string
		build func(int) string
	}{
		{"legacy_announce_routes", buildLegacyAnnounceRoutesLine},
		{"v3_envelope", buildV3AnnounceLine},
	}
	for _, shape := range shapes {
		for _, n := range []int{1, 17} {
			line := shape.build(n)
			b.Run(shape.name+"/routes="+strconv.Itoa(n), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(line)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := ParseFrameLine(line); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
