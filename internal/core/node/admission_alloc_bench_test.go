package node

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

// BenchmarkReadFrameLine measures the per-line allocation of readFrameLine on
// the common single-chunk path (line fits within bufio's buffer, which is the
// overwhelming majority of inbound frames). buildBenchLine includes the
// trailing newline so ReadSlice terminates on the first call.
func BenchmarkReadFrameLine(b *testing.B) {
	for _, size := range []int{256, 4096} {
		line := strings.Repeat("a", size-1) + "\n"
		b.Run("bytes="+strconv.Itoa(size), func(b *testing.B) {
			src := strings.NewReader("")
			// Buffer comfortably larger than the line so ReadSlice returns
			// the whole line in one shot (single-chunk fast path).
			reader := bufio.NewReaderSize(src, 64*1024)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.Reset(line)
				reader.Reset(src)
				out, err := readFrameLine(reader, maxResponseLineBytes)
				if err != nil && !errors.Is(err, io.EOF) {
					b.Fatal(err)
				}
				if len(out) != size {
					b.Fatalf("got %d bytes, want %d", len(out), size)
				}
			}
		})
	}
}
