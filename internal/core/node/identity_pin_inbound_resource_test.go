package node

import (
	"runtime"
	"testing"
	"unsafe"
)

func TestInboundN0LazyAndSaturatedStorageCostIsBounded(t *testing.T) {
	runtime.GC()
	before := heapInUse()
	owners := make([]*ninJournal, 1000)
	for k := range owners {
		owners[k] = ninJournalFromImage(ninCodecImage())
		if owners[k].arena != nil {
			t.Fatal("empty attached journal allocated arena")
		}
	}
	runtime.GC()
	after := heapInUse()
	var added uint64
	if after > before {
		added = after - before
	}
	if added > 1024*1024 {
		t.Fatal("empty owner inventory exceeded1MiB/1000 owners", added)
	}
	j := owners[0]
	j.arena = new([ninGroupCapacity]ninRow)
	j.rows = ninGroupCapacity
	for k := 1; k < j.rows; k++ {
		j.arena[k] = ninRow{key: ninKey{1, byte(k >> 8), byte(k)}, count: 64}
	}
	snapshot := j.image()
	if len(snapshot.rows) != 4096 || unsafe.Sizeof(*j.arena) != 2129920 {
		t.Fatal("arena bound changed")
	}
	t.Logf("owner header %dB; empty1000 measured live delta %dB; arena %dB; one snapshot row payload %dB; encoding/full-main/GC overhead additional; no D1 resource admission", unsafe.Sizeof(*j), added, unsafe.Sizeof(*j.arena), uintptr(len(snapshot.rows))*unsafe.Sizeof(ninRow{}))
	runtime.KeepAlive(owners)
	runtime.KeepAlive(snapshot)
}
