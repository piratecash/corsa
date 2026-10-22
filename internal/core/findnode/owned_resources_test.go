package findnode

import "testing"

func TestOwnedResourcesSharedRoundEstimateIsNotUniqueGraphAccounting(t *testing.T) {
	f := executingNew(t, 2, 4, false)
	batch := f.admit(t, 2)
	shape := batch.ResourceShape()
	if shape.BatchBytes == 0 || shape.Works != 2 || shape.NamedRoundEstimateBytes == 0 {
		t.Fatal("actual batch named storage absent", shape)
	}
	first, ok := batch.Next()
	if !ok {
		t.Fatal("first actual receipt absent")
	}
	second, ok := batch.Next()
	if !ok {
		t.Fatal("second actual receipt absent")
	}
	if first.work.receipt.execution != second.work.receipt.execution || first.work.cut != second.work.cut {
		t.Fatal("fixture did not retain one shared original graph")
	}
	q1, err := first.QueueForOriginal(f.members[2])
	if err != nil {
		t.Fatal(err)
	}
	q2, err := second.QueueForOriginal(f.members[2])
	if err != nil {
		t.Fatal(err)
	}
	a, b := q1.ResourceShape(), q2.ResourceShape()
	if a.NamedRoundEstimateBytes != b.NamedRoundEstimateBytes || a.NamedRoundEstimateBytes != shape.NamedRoundEstimateBytes || a.CellBytes == 0 || a.PayloadBytes == 0 {
		t.Fatal("same shared graph yielded different named layout", a, b)
	}
	// Directed metric-only holder test. Actual NetCore ownership boundaries are
	// exercised in NetCore/Node tests; these local aliases are not a GC census.
	h1, h2 := q1.HoldResources(), q2.HoldResources()
	held := f.intake.ResourceSnapshot().Writer
	if held.ForwardItems != 2 || held.CellBytes != a.CellBytes+b.CellBytes || held.PayloadBytes != a.PayloadBytes+b.PayloadBytes || held.NamedRoundEstimateBytes != a.NamedRoundEstimateBytes+b.NamedRoundEstimateBytes {
		t.Fatal("original owner lost distinct cells or unshared estimate", held)
	}
	if err := q1.Cancel(); err != nil {
		t.Fatal(err)
	}
	if f.intake.ResourceSnapshot().Writer.ForwardItems != 2 {
		t.Fatal("logical cancellation released caller-held cell")
	}
	h1.Release()
	h2.Release()
	if after := f.intake.ResourceSnapshot().Writer; after.ForwardItems != 0 || after.CellBytes != 0 || after.PayloadBytes != 0 || after.NamedRoundEstimateBytes != 0 {
		t.Fatal("explicit final-use holders did not retire", after)
	}
	_ = q2.Cancel()
}
func TestOwnedResourceSnapshotsAreReadOnlyAndZeroValuesStayInactive(t *testing.T) {
	if (DormantIntake{}).ResourceSnapshot().Enabled {
		t.Fatal("zero owner became enabled")
	}
	if (OwnedRoundBatch{}).ResourceShape() != (BatchResourceShape{}) || (OwnedQueuedForward{}).ResourceShape() != (WorkResourceShape{}) || (OwnedQueuedAnswer{}).ResourceShape() != (WorkResourceShape{}) {
		t.Fatal("zero handles invented named storage")
	}
	f, _, queued := executingOne(t, false)
	before := f.intake.dormantOwner.coordinator.snapshot()
	clock := f.clock.Load()
	shape := queued.ResourceShape()
	snapshot := f.intake.ResourceSnapshot()
	if !snapshot.Enabled || snapshot.FixedBytes == 0 || snapshot.Pending != 1 || snapshot.PendingEntryBytes == 0 || shape.NamedRoundEstimateBytes <= shape.CellBytes {
		t.Fatal("named owner/retained arrays omitted", snapshot, shape)
	}
	if f.intake.dormantOwner.coordinator.snapshot() != before || f.clock.Load() != clock {
		t.Fatal("resource read changed owner or sampled authority clock")
	}
	_ = queued.Cancel()
}
