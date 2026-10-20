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

func TestInboundN1FixedOwnershipCostAndTerminalAliasRelease(t *testing.T) {
	f := n1AcceptedFixture(t)
	if !f.service.publishInboundLifetime(f.owner) {
		t.Fatal("publish failed")
	}
	// Keep the actual existing job/context aliases across End. These aliases
	// cannot preserve a second proof/socket/store ownership chain in the leaf.
	capturedJob := f.owner
	capturedArrival := acceptedTransportArrivalContext(t.Context(), f.service, f.owner.id, f.owner)
	original := capturedJob.life.nin
	capturedJob.end()
	n1AssertTerminal(t, capturedJob)
	if capturedArrival.Value(proofTransportArrivalKey{}) != capturedJob || capturedJob.life.nin != original {
		t.Fatal("termination changed original opaque alias identity")
	}
	leafBytes := unsafe.Sizeof(matureInboundWitness{})
	wireBytes := unsafe.Sizeof(peekedConn{})
	handleBytes := unsafe.Sizeof(proofTransportOwner{})
	producerBytes := unsafe.Sizeof(inboundPinProducer{})
	sendSelectionBytes := unsafe.Sizeof(inboundSendSelection{})
	closeSelectionBytes := unsafe.Sizeof(inboundCloseSelection{})
	pointerBytes := unsafe.Sizeof(uintptr(0))
	if sendSelectionBytes != 2*pointerBytes || closeSelectionBytes != pointerBytes {
		t.Fatal("captured selection retained more than actual core/original owner", sendSelectionBytes, closeSelectionBytes)
	}
	// These are broad structural regression bounds, NOT heap/RSS acceptance.
	// No witness slice/map or per-session worker/timer belongs in these owners.
	if leafBytes > 1024 || wireBytes > 128 || handleBytes > 32 || producerBytes > 128 {
		t.Fatal("fixed original-owner shape grew", leafBytes, wireBytes, handleBytes, producerBytes)
	}
	t.Logf("N1 shallow sizes: original leaf %dB, candidate W %dB, handle %dB, producer %dB; R grows by one pointer; private context allocator/closure/interface/GC costs additional. B live handlers, J existing delayed jobs, existing concurrent send/Close callers and one selection may retain leaves; J<=B is NOT proved. Existing N0 arena/snapshots/full-main/write buffers additional; no D1 resource admission", leafBytes, wireBytes, handleBytes, producerBytes)
	t.Logf("Send selection %dB, close selection %dB; values retain no Service/ConnID/callback. Borrowed strict port is fixture-owned until synchronous common scheduler joins; actual runtime wrappers pass nil and retain no backend field", sendSelectionBytes, closeSelectionBytes)
	runtime.KeepAlive(capturedJob)
	runtime.KeepAlive(capturedArrival)
}
