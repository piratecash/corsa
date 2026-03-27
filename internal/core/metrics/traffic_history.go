package metrics

import (
	"sync"
	"time"
)

const trafficHistoryCapacity = 3600 // 1 hour at 1 sample per second

// TrafficSample holds a single point-in-time traffic snapshot.
type TrafficSample struct {
	Timestamp     time.Time `json:"timestamp"`
	BytesSentPS   int64     `json:"bytes_sent_ps"`   // bytes sent delta since previous sample
	BytesRecvPS   int64     `json:"bytes_recv_ps"`   // bytes received delta since previous sample
	TotalSent     int64     `json:"total_sent"`      // cumulative bytes sent at this moment
	TotalReceived int64     `json:"total_received"`  // cumulative bytes received at this moment
}

// TrafficHistory maintains a fixed-size ring buffer of traffic samples.
// When full, the oldest sample is evicted to make room for the newest.
// All methods are safe for concurrent use.
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

// Record adds a new sample. Deltas are computed automatically from the
// difference between the current totals and the previous sample.
func (th *TrafficHistory) Record(totalSent, totalReceived int64) {
	th.mu.Lock()
	defer th.mu.Unlock()

	var deltaSent, deltaRecv int64
	if th.full || th.head > 0 {
		deltaSent = totalSent - th.prevSent
		deltaRecv = totalReceived - th.prevRecv
		if deltaSent < 0 {
			deltaSent = 0
		}
		if deltaRecv < 0 {
			deltaRecv = 0
		}
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
