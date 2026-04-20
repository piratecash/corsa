package metrics

import (
	"sync"
	"time"
)

const trafficHistoryCapacity = 3600 // 1 hour at 1 sample per second

// TrafficSample holds a single point-in-time traffic snapshot.
type TrafficSample struct {
	Timestamp     time.Time `json:"timestamp"`
	BytesSentPS   int64     `json:"bytes_sent_ps"`  // bytes sent delta since previous sample
	BytesRecvPS   int64     `json:"bytes_recv_ps"`  // bytes received delta since previous sample
	TotalSent     int64     `json:"total_sent"`     // cumulative bytes sent at this moment
	TotalReceived int64     `json:"total_received"` // cumulative bytes received at this moment
}

// TrafficHistory maintains a fixed-size ring buffer of traffic samples.
// When full, the oldest sample is evicted to make room for the newest.
// All methods are safe for concurrent use.
//
// Delta computation expects a baseline: prevSent/prevRecv must reflect the
// cumulative counters that were already in place before the first Record
// call. Seed captures that baseline explicitly; callers that wire up a
// Collector before any traffic flowed can skip Seed (prev stays at 0 and the
// first Record produces delta == totalSent/totalReceived correctly). Callers
// that start observing a running system must Seed(current totals) once
// before the first Record, otherwise the first sample would spike with the
// pre-observation cumulative bytes recorded as "one second of traffic".
type TrafficHistory struct {
	mu       sync.RWMutex
	samples  []TrafficSample
	head     int  // next write position
	full     bool // true once the buffer has wrapped around
	prevSent int64
	prevRecv int64
}

// NewTrafficHistory creates an empty ring buffer.
func NewTrafficHistory() *TrafficHistory {
	return &TrafficHistory{
		samples: make([]TrafficSample, trafficHistoryCapacity),
	}
}

// Seed sets the baseline for delta computation to the given cumulative
// counters without appending a sample. Intended to be called exactly once,
// before the first Record, when the producer is attached to an already-running
// system whose counters are non-zero. Without Seed, the first Record would
// treat prevSent == 0 and report the entire pre-observation cumulative as a
// single-second spike (or, in the previous implementation, discard the first
// delta altogether and hide real traffic that happened between producer
// startup and the first tick).
//
// Calling Seed after Record is a no-op on the recorded samples but overwrites
// the baseline — callers should treat Seed as a startup-time operation.
func (th *TrafficHistory) Seed(totalSent, totalReceived int64) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.prevSent = totalSent
	th.prevRecv = totalReceived
}

// Record adds a new sample. Deltas are computed as totalSent - prevSent /
// totalReceived - prevRecv, clamped to zero on counter resets. The baseline
// prevSent/prevRecv starts at zero (suitable when the producer observes the
// system from startup) and is updated on every Record; see Seed for the case
// where the producer attaches to a system with pre-existing cumulative bytes.
func (th *TrafficHistory) Record(totalSent, totalReceived int64) {
	th.mu.Lock()
	defer th.mu.Unlock()

	deltaSent := totalSent - th.prevSent
	deltaRecv := totalReceived - th.prevRecv
	if deltaSent < 0 {
		deltaSent = 0
	}
	if deltaRecv < 0 {
		deltaRecv = 0
	}

	th.samples[th.head] = TrafficSample{
		Timestamp:     time.Now().UTC(),
		BytesSentPS:   deltaSent,
		BytesRecvPS:   deltaRecv,
		TotalSent:     totalSent,
		TotalReceived: totalReceived,
	}

	th.prevSent = totalSent
	th.prevRecv = totalReceived

	th.head++
	if th.head >= trafficHistoryCapacity {
		th.head = 0
		th.full = true
	}
}

// Len returns the number of recorded samples.
func (th *TrafficHistory) Len() int {
	th.mu.RLock()
	defer th.mu.RUnlock()
	if th.full {
		return trafficHistoryCapacity
	}
	return th.head
}

// Snapshot returns a copy of all recorded samples in chronological order
// (oldest first). The returned slice is owned by the caller.
func (th *TrafficHistory) Snapshot() []TrafficSample {
	th.mu.RLock()
	defer th.mu.RUnlock()

	count := th.head
	if th.full {
		count = trafficHistoryCapacity
	}
	result := make([]TrafficSample, count)

	if th.full {
		n := copy(result, th.samples[th.head:])
		copy(result[n:], th.samples[:th.head])
	} else {
		copy(result, th.samples[:th.head])
	}
	return result
}

// Capacity returns the maximum number of samples the buffer can hold.
func (th *TrafficHistory) Capacity() int {
	return trafficHistoryCapacity
}
