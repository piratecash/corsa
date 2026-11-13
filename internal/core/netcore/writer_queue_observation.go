package netcore

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// WriterQueueSnapshot records one cut of the opt-in queue. Taken counts
// removals by the writer loop, including items the writer cannot send. Drained
// counts removals by terminal cleanup. Neither counter measures socket writes.
type WriterQueueSnapshot struct {
	Instance      [16]byte
	At            time.Duration
	Depth         int
	Enqueued      uint64
	Taken         uint64
	Drained       uint64
	WriterExited  bool
	UnknownReason string
}

type writerQueueObservation struct {
	mu      sync.Mutex
	changed chan struct{}
	state   WriterQueueSnapshot
}

func newWriterQueueObservation() *writerQueueObservation {
	o := new(writerQueueObservation)
	if _, err := rand.Read(o.state.Instance[:]); err != nil {
		o.state.UnknownReason = "source_identity_unavailable"
	}
	return o
}

// Every observed send and receive is nonblocking under mu. Waiters wait
// outside mu on a shared notification created only when a wait is needed.
func (o *writerQueueObservation) waitLocked() <-chan struct{} {
	if o.changed == nil {
		o.changed = make(chan struct{})
	}
	return o.changed
}

func (o *writerQueueObservation) recordLocked(counter *uint64) {
	if *counter == ^uint64(0) {
		o.invalidateLocked("counter_overflow")
	} else {
		(*counter)++
	}
	if o.changed != nil {
		close(o.changed)
		o.changed = nil
	}
}

func (o *writerQueueObservation) invalidateLocked(reason string) {
	if o.state.UnknownReason == "" {
		o.state.UnknownReason = reason
	}
}

func (pc *NetCore) offerObservedWriterItem(item sendItem, wait bool) (SendStatus, <-chan struct{}) {
	o := pc.writerQueueObservation
	o.mu.Lock()
	defer o.mu.Unlock()
	if status := pc.gateStatus(); status != SendOK {
		return status, nil
	}
	select {
	case <-pc.writerDone:
		return pc.teardownStatus(), nil
	default:
	}
	select {
	case pc.sendCh <- item:
		o.recordLocked(&o.state.Enqueued)
		return SendOK, nil
	default:
		if wait {
			return SendBufferFull, o.waitLocked()
		}
		return SendBufferFull, nil
	}
}

func (pc *NetCore) waitObservedWriterOffer(item sendItem, cancel <-chan struct{}, deadline <-chan time.Time) SendStatus {
	for {
		select {
		case <-pc.writerDone:
			return pc.teardownStatus()
		case <-cancel:
			return SendCtxCancelled
		case <-deadline:
			return SendTimeout
		default:
		}
		status, changed := pc.offerObservedWriterItem(item, true)
		if changed == nil {
			return status
		}
		select {
		case <-pc.writerDone:
			return pc.teardownStatus()
		case <-cancel:
			return SendCtxCancelled
		case <-deadline:
			return SendTimeout
		case <-changed:
		}
	}
}

func (pc *NetCore) takeObservedWriterItem(drain bool) (sendItem, bool, <-chan struct{}) {
	o := pc.writerQueueObservation
	o.mu.Lock()
	defer o.mu.Unlock()
	select {
	case item := <-pc.sendCh:
		if drain {
			o.recordLocked(&o.state.Drained)
		} else {
			o.recordLocked(&o.state.Taken)
		}
		return item, true, nil
	default:
		if drain {
			return sendItem{}, false, nil
		}
		return sendItem{}, false, o.waitLocked()
	}
}

func (pc *NetCore) nextWriterItem() (sendItem, bool) {
	if pc.writerQueueObservation == nil {
		select {
		case <-pc.closing:
			return sendItem{}, false
		case item := <-pc.sendCh:
			return item, true
		}
	}
	for {
		select {
		case <-pc.closing:
			return sendItem{}, false
		default:
		}
		item, ok, changed := pc.takeObservedWriterItem(false)
		if ok {
			return item, true
		}
		select {
		case <-pc.closing:
			return sendItem{}, false
		case <-changed:
		}
	}
}

// WriterQueueObservation returns counts and depth from the same serialized
// cut. At is a local since-boot timestamp, without external UTC attribution.
func (pc *NetCore) WriterQueueObservation() WriterQueueSnapshot {
	o := pc.writerQueueObservation
	if o == nil {
		return WriterQueueSnapshot{UnknownReason: "source_unavailable"}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	select {
	case <-pc.writerExited:
		o.state.WriterExited = true
	default:
	}
	o.state.Depth = len(pc.sendCh)
	s := &o.state
	if s.Taken > s.Enqueued || s.Drained > s.Enqueued-s.Taken ||
		s.Enqueued-s.Taken-s.Drained != uint64(s.Depth) || s.WriterExited && s.Depth != 0 {
		o.invalidateLocked("queue_count_mismatch")
	}
	at, err := trustedclock.SinceBoot()
	if err != nil || at < 0 {
		o.invalidateLocked("duration_read_failed")
	} else if at < s.At {
		o.invalidateLocked("duration_regressed")
	} else {
		s.At = at
	}
	return *s
}
