package connbudget

import (
	"bytes"
	"errors"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func exportFileLimits() TotalFileObservationLimits {
	p := totalFileTestLimits()
	return TotalFileObservationLimits{RecordBytes: p.recordBytes, StoreBytes: p.storeBytes, OrphanBytes: p.orphanBytes, ScanBytes: p.scanBytes, StoreRecords: p.storeRecords, OrphanRecords: p.orphanRecords, ScanBatch: p.scanBatch, ScanSteps: p.scanSteps}
}

type totalFileExportFixture struct {
	b        *Budget
	o        *TotalFileObservation
	d        *totalTestClock
	dir      *secretfile.Dir
	releases []func()
}

func newTotalFileExportFixture(t *testing.T, cfg Config) *totalFileExportFixture {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("directory lease unsupported")
	}
	dir, err := secretfile.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &totalFileExportFixture{dir: dir, d: &totalTestClock{}}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	f.b, f.o, err = NewWithTotalFileObservation(cfg, TotalObservationKey{Anchor: "file-export", Epoch: [16]byte{1}, ClockDomain: [16]byte{2}, Configuration: [16]byte{3}}, 4, 1, f.d.read, dir, exportFileLimits(), [16]byte{9})
	if f.o != nil {
		t.Cleanup(func() {
			f.releaseAll()
			if err := f.o.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil || f.o == nil || f.o.Err() != nil {
		t.Fatal("real export construction", err)
	}
	return f
}
func (f *totalFileExportFixture) releaseAll() {
	for _, release := range f.releases {
		release()
	}
}
func (f *totalFileExportFixture) take(t *testing.T, d Direction) *Reservation {
	t.Helper()
	r := totalTake(t, f.b, d)
	f.releases = append(f.releases, r.Release)
	return r
}
func exportCapture(t *testing.T, o *TotalFileObservation) TotalFileObservationResult {
	t.Helper()
	r, err := o.Capture()
	if err != nil || r.Status != "COMMITTED_PAST" || r.AckStatus != "RETIRED" || r.CommitOutcome != secretfile.DurableCommitted || !r.PastOnly || r.AttributionBound {
		t.Fatal("file export Capture", r, err)
	}
	return r
}
func TestTotalFileObservationAcquisition(t *testing.T) {
	t.Run("actual-budget-file-capture-and-legacy", func(t *testing.T) {
		cfg := Config{Total: 3, OutboundReserve: 1, MaxOutbound: 3, MaxInbound: 2, MaxAuxiliary: 2}
		f := newTotalFileExportFixture(t, cfg)
		if f.o.budget != f.b || f.o.issuer.gate != f.b || f.o.LegacyObservation().budget != f.b {
			t.Fatal("second owner/legacy")
		}
		a := f.take(t, DirectionOutbound)
		b := f.take(t, DirectionInbound)
		f.d.set(time.Second, nil)
		c := f.take(t, DirectionAuxiliary)
		if _, err := f.b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
			t.Fatal(err)
		}
		first := exportCapture(t, f.o)
		if first.Checkpoint.Config != cfg || first.Checkpoint.ThroughEntry.Count != 3 || !first.Checkpoint.ThroughFull || first.Checkpoint.Rows != 1 {
			t.Fatal(first)
		}
		old := totalFileRead(t, f.dir, totalFinalName(first.Checkpoint.ID), 2048)
		legacy := f.o.LegacyObservation().Snapshot()
		if legacy.Status != "UNKNOWN" || legacy.Cut.Available || len(legacy.Rows) != 0 || !legacy.PastOnly {
			t.Fatal("legacy V2 suffix", legacy)
		}
		f.d.set(2*time.Second, nil)
		c.Release()
		c.Release()
		last := exportCapture(t, f.o)
		if last.Checkpoint.FromEntry != first.Checkpoint.ThroughEntry || last.Checkpoint.ThroughFull || last.Checkpoint.ThroughSequence != 2 || f.b.Snapshot().Used != 2 {
			t.Fatal(last)
		}
		if !bytes.Equal(old, totalFileRead(t, f.dir, totalFinalName(first.Checkpoint.ID), 2048)) {
			t.Fatal("retirement removed evidence")
		}
		if v, err := f.o.Capture(); err != nil || v.Status != "NO_PROGRESS" || v.CommitOutcome != 0 {
			t.Fatal(v, err)
		}
		a.Release()
		b.Release()
	})
	t.Run("verified-pending-no-IO-no-reverify-and-later-gap", func(t *testing.T) {
		f := newTotalFileExportFixture(t, Config{Total: 1})
		f.d.set(time.Second, nil)
		r := f.take(t, DirectionOutbound)
		f.d.set(2*time.Second, nil)
		r.Release()
		f.d.set(3*time.Second, nil)
		if _, status := f.b.sealTotalSegment(); status != "OFFERED" {
			t.Fatal(status)
		}
		counter := &totalFileVerificationCounter{real: f.o.issuer}
		f.b.fullObservation.segment.issuer = counter
		started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		join := totalCleanup(t, unblock, f.releaseAll)
		join(done)
		next := f.d.count() + 1
		f.d.set(4*time.Second, nil)
		f.d.after = func(n int) {
			if n == next {
				close(started)
				<-resume
			}
		}
		workerErr := make(chan error, 1)
		go func() {
			defer close(done)
			r, err := f.b.Reserve(DirectionOutbound)
			if err == nil {
				r.Release()
			}
			workerErr <- err
		}()
		totalWait(t, started)
		first, err := f.o.Capture()
		if err != nil || first.Status != "DEFERRED" || first.AckStatus != "DEFERRED" || first.CommitOutcome != secretfile.DurableCommitted {
			t.Fatal(first, err)
		}
		calls := counter.count()
		if calls != 1 {
			t.Fatal("actual file verifier not reached", calls)
		}
		f.o.issuer.limits.scanBytes = 1 // A re-Commit/readback would now refuse.
		again, err := f.o.Capture()
		if err != nil || again.Status != "DEFERRED" || again.Checkpoint != first.Checkpoint || again.Digest != first.Digest || counter.count() != calls {
			t.Fatal("pending Capture reverified/changed Q", again, err)
		}
		totalSegmentLeafFree(t, f.b)
		unblock()
		totalWait(t, done)
		if err := <-workerErr; err != nil {
			t.Fatal(err)
		}
		// Original mutations may exceed the remaining ring while old Q is pinned.
		// The good verified Q can retire; the later gap must stay visible separately.
		f.d.set(5*time.Second, nil)
		late := f.take(t, DirectionOutbound)
		late.Release()
		retired, err := f.o.Capture()
		if err != nil || retired.Status != "COMMITTED_PAST" || retired.Checkpoint != first.Checkpoint || retired.Digest != first.Digest || counter.count() != calls || retired.SourceStatus != "UNKNOWN" {
			t.Fatal("pending retire hid gap or repeated IO", retired, err)
		}
	})
	t.Run("disabled-original-errors-and-unavailable-backend", func(t *testing.T) {
		calls := 0
		clock := func() (time.Duration, error) { calls++; return 0, nil }
		b, o, err := NewWithTotalFileObservation(Config{Total: 0, MaxOutbound: 1}, TotalObservationKey{}, -1, -1, clock, &secretfile.Dir{}, TotalFileObservationLimits{}, [16]byte{})
		if err != nil || b == nil || o == nil || calls != 0 {
			t.Fatal(err)
		}
		disabledHandle := o
		defer func() {
			if err := disabledHandle.Close(); err != nil {
				t.Error(err)
			}
		}()
		if v, err := o.Capture(); err != nil || v.Status != "DISABLED" || v.Checkpoint.ID != 0 || v.CommitOutcome != 0 || calls != 0 || o.issuer.lease != nil {
			t.Fatal("disabled file work", v, err)
		}
		if _, _, err := NewWithTotalFileObservation(Config{Total: -1}, TotalObservationKey{}, 0, 0, clock, nil, TotalFileObservationLimits{}, [16]byte{}); !errors.Is(err, ErrInvalidConfig) || calls != 0 {
			t.Fatal(err)
		}
		b, o, err = NewWithTotalFileObservation(Config{Total: 1}, TotalObservationKey{Anchor: "missing-dir", Epoch: [16]byte{1}, ClockDomain: [16]byte{2}, Configuration: [16]byte{3}}, 2, 1, clock, nil, exportFileLimits(), [16]byte{9})
		if err != nil || b == nil || o == nil || o.Err() == nil || o.Status().Status != "UNKNOWN" {
			t.Fatal("backend became startup policy", err)
		}
		defer func() {
			if err := o.Close(); err != nil {
				t.Error(err)
			}
		}()
		r := totalTake(t, b, DirectionOutbound)
		r.Release()
		if v, err := o.Capture(); err == nil || v.Status != "UNKNOWN" || v.Status == "NO_PROGRESS" {
			t.Fatal("fake health", v, err)
		}
		var absent *TotalFileObservation
		if absent.LegacyObservation() != nil || absent.Close() != nil || absent.Err() != nil || absent.Status().Status != "INACTIVE" {
			t.Fatal("nil handle")
		}
	})
	t.Run("unconfirmed-no-progress-negative-control", func(t *testing.T) {
		f := newTotalFileExportFixture(t, Config{Total: 1})
		// Explicit negative control after REAL successful initialization. This is
		// not a constructor Sync-fault/powerloss test or a positive evidence writer.
		f.o.issuer.mu.Lock()
		f.o.issuer.identityConfirmed = false
		f.o.issuer.mu.Unlock()
		if v, err := f.o.Capture(); err == nil || v.Status != "UNKNOWN" || v.Reason != "initial_file_confirmation_unknown" {
			t.Fatal(v, err)
		}
		f.d.set(time.Second, nil)
		v := exportCapture(t, f.o)
		if v.Checkpoint.Rows != 0 || !f.o.issuer.identityConfirmed {
			t.Fatal("offered Q could not strictly reconfirm identity", v)
		}
	})
	t.Run("close-fence-joins-real-write-and-borrows-directory", func(t *testing.T) {
		f := newTotalFileExportFixture(t, Config{Total: 1})
		f.d.set(time.Second, nil)
		r := f.take(t, DirectionOutbound)
		f.d.set(2*time.Second, nil)
		r.Release()
		f.d.set(3*time.Second, nil)
		started, resume, captured, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		join := totalCleanup(t, unblock, f.releaseAll)
		join(captured)
		real := f.o.issuer.writeRecord
		f.o.issuer.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			if strings.HasSuffix(name, ".final") {
				close(started)
				<-resume
			}
			return real(name, raw)
		}
		var captureResult TotalFileObservationResult
		var captureErr, closeErr error
		go func() { defer close(captured); captureResult, captureErr = f.o.Capture() }()
		totalWait(t, started)
		join(closed)
		go func() { defer close(closed); closeErr = f.o.Close() }()
		deadline := time.Now().Add(3 * time.Second)
		for !f.o.closing.Load() {
			if time.Now().After(deadline) {
				t.Fatal("Close did not fence")
			}
			runtime.Gosched()
		}
		totalSegmentLeafFree(t, f.b)
		if v, err := f.o.Capture(); err == nil || v.Status != "UNKNOWN" || v.Reason != "observation_closed" {
			t.Fatal(v, err)
		}
		select {
		case <-closed:
			t.Fatal("Close did not join Capture")
		default:
		}
		unblock()
		totalWait(t, captured)
		totalWait(t, closed)
		if captureErr != nil || captureResult.Status != "COMMITTED_PAST" || closeErr != nil || !f.o.Status().Closed {
			t.Fatal(captureResult, captureErr, closeErr)
		}
		if err := f.dir.DurablePreflight(); err != nil {
			t.Fatal("borrowed Dir closed", err)
		}
		lease, err := f.dir.TryDirectoryLease()
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.o.Close(); err != nil {
			t.Fatal(err)
		}
		extra := f.take(t, DirectionOutbound)
		extra.Release()
	})
}
