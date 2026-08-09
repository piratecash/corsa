package datagram

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// metrics_test.go covers §10: every drop reason observable, the snapshot
// internally consistent, concurrent counting safe.

// A drop nobody can see is a drop nobody can debug, and the layer refuses
// SILENTLY on the wire. So every reason in the closed enum must reach the
// snapshot under its own label.
func TestMetricsEveryDropReasonIsObservable(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	reasons := make([]DropReason, 0, len(dropReasonNames))
	for reason := range dropReasonNames {
		if reason == DropReasonUnset {
			continue
		}
		reasons = append(reasons, reason)
		metrics.ObserveInbound(domain.DatagramModeRouted, InboundDropped, reason)
	}
	if len(reasons) == 0 {
		t.Fatal("the drop reason enum is empty")
	}

	snapshot := metrics.Snapshot()
	for _, reason := range reasons {
		label := reason.String()
		if label == "invalid" {
			t.Fatalf("reason %d has no metric label", reason)
		}
		if snapshot.DropsByReason[label] != 1 {
			t.Fatalf("reason %s counted %d times, want 1", label, snapshot.DropsByReason[label])
		}
		if metrics.DropCount(reason) != 1 {
			t.Fatalf("DropCount(%s) = %d, want 1", label, metrics.DropCount(reason))
		}
	}
	if len(snapshot.DropsByReason) != len(reasons) {
		t.Fatalf("snapshot holds %d reasons, want %d", len(snapshot.DropsByReason), len(reasons))
	}
	if snapshot.Dropped != uint64(len(reasons)) {
		t.Fatalf("Dropped = %d, want %d", snapshot.Dropped, len(reasons))
	}
	// A reason nobody hit stays out of the document rather than filling it
	// with zeroes.
	if _, present := snapshot.DropsByReason["none"]; present {
		t.Fatal("DropReasonUnset leaked into the snapshot")
	}
}

// "Refused answers" is a metric §10 asks for by name, and it is a SET of drop
// reasons, not a prefix: a refused request also starts with DropReverse and
// must not be counted here.
func TestMetricsRefusedAnswersCountsTheAnswerPlaneOnly(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	for _, reason := range refusedAnswerReasons {
		metrics.ObserveInbound(domain.DatagramModeResponse, InboundDropped, reason)
	}
	// Refused REQUESTS, which must stay out of the number.
	metrics.ObserveInbound(domain.DatagramModeRequest, InboundDropped, DropReverseSlotBusy)
	metrics.ObserveInbound(domain.DatagramModeRequest, InboundDropped, DropReverseSlotCapped)

	snapshot := metrics.Snapshot()
	if snapshot.RefusedAnswers != uint64(len(refusedAnswerReasons)) {
		t.Fatalf("RefusedAnswers = %d, want %d", snapshot.RefusedAnswers, len(refusedAnswerReasons))
	}
	if snapshot.Dropped != uint64(len(refusedAnswerReasons))+2 {
		t.Fatalf("Dropped = %d, want every refusal counted once", snapshot.Dropped)
	}
}

// The reverse-state events of §4.2 land in the same snapshot: one type serves
// both the pipeline's sink and the table's, so the two views cannot drift.
func TestMetricsCountsReverseStateEvents(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	var sink reverseMetrics = metrics
	sink.ObserveReverseState(ReverseEventReserved)
	sink.ObserveReverseState(ReverseEventReserved)
	sink.ObserveReverseState(ReverseEventEvicted)

	snapshot := metrics.Snapshot()
	if snapshot.ReverseEvents["reserved"] != 2 || snapshot.ReverseEvents["evicted"] != 1 {
		t.Fatalf("reverse events = %+v", snapshot.ReverseEvents)
	}
	if len(snapshot.ReverseEvents) != 2 {
		t.Fatalf("untouched events leaked into the snapshot: %+v", snapshot.ReverseEvents)
	}
	if metrics.ReverseCount(ReverseEventReserved) != 2 {
		t.Fatalf("ReverseCount = %d, want 2", metrics.ReverseCount(ReverseEventReserved))
	}
}

// The counters are written from every receive goroutine at once. Under -race
// this must be clean, and the totals must be exact once the traffic stops.
func TestMetricsConcurrentObservationIsSafe(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	const (
		writers = 16
		each    = 500
	)
	var wait sync.WaitGroup
	wait.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wait.Done()
			mode := domain.DatagramModeRouted
			if w%2 == 1 {
				mode = domain.DatagramModeResponse
			}
			for i := 0; i < each; i++ {
				switch i % 3 {
				case 0:
					metrics.ObserveInbound(mode, InboundForwarded, DropReasonUnset)
				case 1:
					metrics.ObserveInbound(mode, InboundDropped, DropAdmission)
				default:
					metrics.ObserveInbound(mode, InboundDropped, DropCryptoBudget)
				}
				metrics.ObserveReverseState(ReverseEventProbeSpent)
				metrics.ObserveUnknownDType(domain.DType("unknown"))
			}
			// A reader racing the writers must not tear or panic.
			metrics.Snapshot()
		}(w)
	}
	wait.Wait()

	snapshot := metrics.Snapshot()
	const total = writers * each
	if snapshot.Observed != total {
		t.Fatalf("Observed = %d, want %d", snapshot.Observed, total)
	}
	if snapshot.Forwarded+snapshot.Dropped != total {
		t.Fatalf("forwarded %d + dropped %d != %d", snapshot.Forwarded, snapshot.Dropped, total)
	}
	if snapshot.DropsByReason["admission"]+snapshot.DropsByReason["crypto_budget"] != snapshot.Dropped {
		t.Fatalf("drop reasons %+v do not add up to %d", snapshot.DropsByReason, snapshot.Dropped)
	}
	if snapshot.ReverseEvents["probe_spent"] != total || snapshot.UnknownDType != total {
		t.Fatalf("reverse %d / unknown %d, want %d each",
			snapshot.ReverseEvents["probe_spent"], snapshot.UnknownDType, total)
	}
}

// A layer wired without metrics must still run: the seams are optional by
// construction, and a nil sink is the shape a unit test hands them.
func TestMetricsNilSinkIsSafe(t *testing.T) {
	t.Parallel()

	var metrics *Metrics
	metrics.ObserveInbound(domain.DatagramModeRouted, InboundDropped, DropAdmission)
	metrics.ObserveUnknownDType(domain.DType("x"))
	metrics.ObserveReverseState(ReverseEventBusy)
	if snapshot := metrics.Snapshot(); snapshot.Observed != 0 || len(snapshot.DropsByReason) != 0 {
		t.Fatalf("a nil sink produced counts: %+v", snapshot)
	}
	if metrics.DropCount(DropAdmission) != 0 || metrics.ReverseCount(ReverseEventBusy) != 0 {
		t.Fatal("a nil sink returned counts")
	}
}

// The diagnostic M9 will publish is assembled here, not by its caller, and it
// must survive a node that wired only some of the components.
func TestMetricsDiagnosticsAssembleFromWhatIsWired(t *testing.T) {
	t.Parallel()

	limits := Limits{Peer: PeerBudget{FramesPerSecond: 3}}
	metrics := NewMetrics()
	metrics.ObserveInbound(domain.DatagramModeRouted, InboundForwarded, DropReasonUnset)
	admission := NewPeerAdmission(AdmissionConfig{Budget: limits.Peer})
	queue := NewWeightedQueue(WeightedQueueConfig{})

	partial := CollectDiagnostics(limits, metrics, nil, nil, nil)
	if partial.Metrics.Forwarded != 1 {
		t.Fatalf("metrics missing from the diagnostic: %+v", partial.Metrics)
	}
	if partial.Limits.Peer.FramesPerSecond != 3 || partial.Limits.Peer.ByteBurst == 0 {
		t.Fatalf("the diagnostic must publish the NORMALIZED limits: %+v", partial.Limits.Peer)
	}
	if partial.Admission != (AdmissionStats{}) || partial.Queue != (QueueStats{}) ||
		partial.Replay != (ReplayDiagnostics{}) {
		t.Fatalf("absent components reported non-zero: %+v", partial)
	}

	queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 128, time.Time{}))
	admission.Admit(ProvenIdentityKey(domain.PeerIdentity{1}), 64)

	// One settled record, so the replay block reports a live occupancy AND a
	// live counter — the pair the §5 refusals have to be read against.
	replay := NewBaseReplayCache(BaseReplayCacheConfig{})
	key := domain.ReplayKey{9, 8, 7}
	token, held := replay.Reserve(
		context.Background(), key, LocalIngress(), time.Now().UTC().Add(time.Hour),
	).Reservation()
	if !held {
		t.Fatal("the fixture could not reserve a replay key")
	}
	if applied := replay.Commit(context.Background(), token); !applied.IsApplied() {
		t.Fatalf("the fixture could not commit: %v", applied.Err())
	}

	full := CollectDiagnostics(limits, metrics, admission, queue, replay)
	if full.Admission.Admitted != 1 || full.Admission.AdmittedBytes != 64 {
		t.Fatalf("admission stats missing: %+v", full.Admission)
	}
	if full.Queue.Enqueued != 1 || full.Queue.BulkDepth != 1 {
		t.Fatalf("queue stats missing: %+v", full.Queue)
	}
	if full.Replay.Held != 1 || full.Replay.Counters.Committed != 1 {
		t.Fatalf("replay stats missing: %+v", full.Replay)
	}
}
