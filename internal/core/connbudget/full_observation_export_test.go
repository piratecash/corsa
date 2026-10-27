package connbudget

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func exportedTotalKey() TotalObservationKey {
	return TotalObservationKey{Anchor: "export-anchor", Epoch: [16]byte{31}, ClockDomain: [16]byte{32}, Configuration: [16]byte{33}}
}
func exportedTotalTake(t *testing.T, b *Budget, d Direction) *Reservation {
	t.Helper()
	r, err := b.Reserve(d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Release)
	return r
}
func exportedTotalProjection(t *testing.T, b *Budget, o *TotalObservation) TotalObservationSnapshot {
	t.Helper()
	v, raw := o.Snapshot(), b.fullSnapshot()
	if v.Status != raw.status || v.Reason != raw.reason || v.Key != (TotalObservationKey{Anchor: raw.key.anchor, Epoch: raw.key.epoch, ClockDomain: raw.key.clockDomain, Configuration: raw.key.configuration}) || v.Config != raw.cfg || v.CoverageStart != raw.coverageStart || v.Cut != (TotalObservationCut{Available: raw.cut.available, Rows: raw.cut.rows, Sequence: raw.cut.sequence, At: raw.cut.at, Full: raw.cut.full}) || v.PastOnly != raw.pastOnly || len(v.Rows) != len(raw.rows) || (v.Rows == nil) != (raw.rows == nil) {
		t.Fatal("public adapter lost raw snapshot field/nil shape", v, raw)
	}
	for i, row := range raw.rows {
		if v.Rows[i] != (TotalObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}) {
			t.Fatal("public adapter lost raw row", i, v.Rows[i], row)
		}
	}
	return v
}
func TestTotalObservationAdapterActualOwner(t *testing.T) {
	d := &totalTestClock{}
	cfg := Config{Total: 3, OutboundReserve: 1, MaxOutbound: 1, MaxInbound: 2, MaxAuxiliary: 1}
	key := exportedTotalKey()
	b, o, err := NewWithTotalObservation(cfg, key, 8, d.read)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || o == nil || o.budget != b {
		t.Fatal("adapter constructed another budget")
	}
	key.Epoch[0] = 0
	cfg.Total = 99
	if v := exportedTotalProjection(t, b, o); v.Status != "COMPLETE" || v.Config != (Config{Total: 3, OutboundReserve: 1, MaxOutbound: 1, MaxInbound: 2, MaxAuxiliary: 1}) || v.Key != exportedTotalKey() || !v.Cut.Available || v.PastOnly {
		t.Fatal("initial key/config/baseline not frozen", v)
	}
	out := exportedTotalTake(t, b, DirectionOutbound)
	in := exportedTotalTake(t, b, DirectionInbound)
	d.set(2*time.Second, nil)
	aux := exportedTotalTake(t, b, DirectionAuxiliary)
	full := exportedTotalProjection(t, b, o)
	if !full.Cut.Full || full.Cut.Sequence != 1 || len(full.Rows) != 1 || full.Rows[0].Count != 3 || full.Rows[0].Lower != 2*time.Second || full.Rows[0].Upper != 2*time.Second {
		t.Fatal("last real reservation not observed", full)
	}
	if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrTotalExhausted) {
		t.Fatal("total refusal changed", err)
	}
	d.set(4*time.Second, nil)
	in.Release()
	calls := d.count()
	in.Release()
	if d.count() != calls {
		t.Fatal("double release bypassed original CAS")
	}
	after := exportedTotalProjection(t, b, o)
	if after.Cut.Full || after.Cut.Sequence != 2 || after.Rows[1].Count != 2 || after.Rows[1].Lower != 4*time.Second || after.Rows[1].Upper != 4*time.Second {
		t.Fatal("real first release not observed", after)
	}
	full.Rows[0].Sequence = 99
	full.Key.Epoch[0] = 0
	full.Config.Total = 99
	if got := o.Snapshot(); got.Rows[0].Sequence != 1 || got.Key != exportedTotalKey() || got.Config.Total != 3 {
		t.Fatal("public snapshot aliases owner/key/config")
	}
	out.Release()
	aux.Release()
}
func TestTotalObservationAdapterDisabledAndInvalidConfig(t *testing.T) {
	d := &totalTestClock{}
	cfg := Config{OutboundReserve: 7, MaxOutbound: 1, MaxInbound: 2}
	b, o, err := NewWithTotalObservation(cfg, TotalObservationKey{}, int(^uint(0)>>1), d.read)
	if err != nil {
		t.Fatal(err)
	}
	v := exportedTotalProjection(t, b, o)
	if v.Status != "DISABLED" || v.Reason != "total-disabled" || !v.PastOnly || v.Cut.Available || v.Rows != nil || b.fullObservation.rows != nil || d.count() != 0 || v.Config != cfg {
		t.Fatal("disabled became duration evidence or allocated rows/read clock", v, d.count())
	}
	r := exportedTotalTake(t, b, DirectionOutbound)
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
		t.Fatal(err)
	}
	r.Release()
	_ = o.Snapshot()
	if d.count() != 0 {
		t.Fatal("disabled read called provider")
	}
	bad, readout, err := NewWithTotalObservation(Config{Total: -1}, exportedTotalKey(), 8, d.read)
	if !errors.Is(err, ErrInvalidConfig) || bad != nil || readout != nil || d.count() != 0 {
		t.Fatal("invalid config became observation-only error", bad, readout, err)
	}
	var absent *TotalObservation
	if got := absent.Snapshot(); got.Status != "INACTIVE" || got.Rows != nil || got.PastOnly {
		t.Fatal("nil adapter changed original nil raw snapshot", got)
	}
}
func TestTotalObservationAdapterUnknownAndPastOnly(t *testing.T) {
	for _, name := range []string{"tokens", "capacity", "capacity-arithmetic", "initial-clock", "nil-clock", "later-clock", "gap"} {
		t.Run(name, func(t *testing.T) {
			d := &totalTestClock{}
			key := exportedTotalKey()
			capacity := 8
			clock := d.read
			want := "clock-error"
			switch name {
			case "tokens":
				key.Epoch = [16]byte{}
				want = "invalid-source-identity"
			case "capacity":
				capacity = 0
				want = "invalid-capacity"
			case "capacity-arithmetic":
				capacity = int(^uint(0) >> 1)
				want = "invalid-capacity"
			case "initial-clock":
				d.set(0, errors.New("lost duration"))
			case "nil-clock":
				clock = nil
				want = "unsupported-clock"
			case "gap":
				capacity = 1
				want = "transition-capacity-gap"
			}
			b, o, err := NewWithTotalObservation(Config{Total: 1}, key, capacity, clock)
			if err != nil || b == nil || o == nil {
				t.Fatal("observation failure lost actual budget", err)
			}
			if name == "later-clock" {
				d.set(time.Second, errors.New("lost duration"))
			}
			r := exportedTotalTake(t, b, DirectionInbound)
			before := o.Snapshot()
			r.Release()
			d.set(100*time.Second, nil)
			v := exportedTotalProjection(t, b, o)
			if v.Status != "UNKNOWN" || v.Reason != want || !v.PastOnly || b.Snapshot().Used != 0 {
				t.Fatal("unknown became coverage or changed real release", v, want)
			}
			if name == "gap" && (!before.Cut.Full || v.Cut != before.Cut || !reflect.DeepEqual(v.Rows, before.Rows)) {
				t.Fatal("lost prefix extended through gap", before, v)
			}
		})
	}
}
func TestTotalObservationAdapterProviderOutsideLeaf(t *testing.T) {
	var budget *Budget
	var held atomic.Bool
	clock := func() (time.Duration, error) {
		if budget != nil {
			if !budget.mu.TryLock() {
				held.Store(true)
			} else {
				budget.mu.Unlock()
			}
		}
		return time.Second, nil
	}
	b, o, err := NewWithTotalObservation(Config{Total: 1}, exportedTotalKey(), 8, clock)
	if err != nil {
		t.Fatal(err)
	}
	budget = b
	r := exportedTotalTake(t, b, DirectionInbound)
	_ = o.Snapshot()
	r.Release()
	_ = o.Snapshot()
	if held.Load() {
		t.Fatal("adapter moved provider into budget leaf")
	}
}
