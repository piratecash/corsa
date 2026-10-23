package node

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type s6PendingDurationClock struct {
	mu  sync.Mutex
	now time.Duration
	err error
}

func (c *s6PendingDurationClock) read() (time.Duration, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now, c.err
}

func (c *s6PendingDurationClock) set(now time.Duration, err error) {
	c.mu.Lock()
	c.now, c.err = now, err
	c.mu.Unlock()
}

func newS6PendingObservedService(t *testing.T) (*Service, *s6PendingDurationClock) {
	t.Helper()
	s := newTestService(t, config.NodeTypeFull)
	c := &s6PendingDurationClock{}
	// Private fixture construction before Run or any queue operation. No
	// fabricated queue events: subsequent changes use the real owners.
	s.deliveryMu.Lock()
	o, err := newPendingQueueObservation(0, c.read)
	s.pendingObservation = o
	s.deliveryMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

func s6PendingFrame(s *Service, id string) protocol.Frame {
	return protocol.Frame{Type: "send_message", ID: id, Topic: "dm", Address: s.Address(),
		Recipient: idTargetX.String(), CreatedAt: time.Now().UTC().Format(time.RFC3339), TTLSeconds: 3600}
}

func requireS6Pending(t *testing.T, s *Service, count int64) domain.PendingQueueObservationSnapshot {
	t.Helper()
	v := s.PendingQueueObservation()
	if !v.Available || !v.CoverageValid || !v.CountValid || !v.DurationValid || v.Count != count || v.UnknownReason != "" {
		t.Fatalf("source snapshot = %+v; want covered actual count %d", v, count)
	}
	if v.AttributionBound {
		t.Fatal("local source fabricated external attribution")
	}
	return v
}

func waitS6Pending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("queue-owner barrier did not complete")
	}
}

func TestS6PendingQueueContinuousRun(t *testing.T) {
	for _, sameKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-key-%v", sameKey), func(t *testing.T) {
			s, clock := newS6PendingObservedService(t)
			addr := domain.PeerAddress("10.80.0.1:64646")
			frame := s6PendingFrame(s, "s6-first")
			if !s.queuePeerFrame(addr, frame) {
				t.Fatal("initial enqueue refused")
			}
			before := requireS6Pending(t, s, 1)
			if before.Sequence != 1 || !before.RunHasIncrease {
				t.Fatalf("first actual append not observed: %+v", before)
			}
			// The real admission path blocks AFTER extraction released
			// deliveryMu and BEFORE failed enqueue/requeue. No observer hook.
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			s.sendAdmissionBarrier = func() { close(entered); <-release }
			s.peerMu.Lock()
			s.sessions[addr] = &peerSession{address: addr, sendCh: make(chan peerSendItem)}
			s.health[addr] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy}
			s.peerMu.Unlock()
			clock.set(12*time.Hour, nil)
			go func() { defer close(done); s.flushPendingPeerFrames(addr) }()
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); waitS6Pending(t, done) })
			waitS6Pending(t, entered)
			clock.set(13*time.Hour, nil)
			concurrent := frame
			if !sameKey {
				concurrent.ID = "s6-concurrent"
			}
			if !s.queuePeerFrame(addr, concurrent) {
				t.Fatal("concurrent enqueue refused")
			}
			releaseOnce.Do(func() { close(release) })
			waitS6Pending(t, done)
			// No source query between the original count 1 and the later
			// higher count 2. A refreshed gauge cannot preserve the decrease.
			clock.set(36*time.Hour, nil)
			after := requireS6Pending(t, s, 2)
			if after.Sequence != 4 || after.RunStart.Earliest != 12*time.Hour || after.RunStart.Latest != 12*time.Hour ||
				after.LongestGrowingRun.Lower != 24*time.Hour || after.LongestGrowingRun.Upper != 24*time.Hour {
				t.Fatalf("hidden decrease did not reset exact run: %+v", after)
			}
			clock.set(36*time.Hour+time.Nanosecond, nil)
			if got := requireS6Pending(t, s, 2).LongestGrowingRun.Lower; got != 24*time.Hour+time.Nanosecond {
				t.Fatalf("duration boundary = %v", got)
			}
			s.deliveryMu.RLock()
			slots, keys := len(s.pending[addr]), len(s.pendingKeys)
			s.deliveryMu.RUnlock()
			if slots != 2 || (sameKey && keys != 1) || (!sameKey && keys != 2) {
				t.Fatalf("existing requeue semantics changed: slots=%d keys=%d", slots, keys)
			}
		})
	}
}

func TestS6PendingQueueAtomicRingAndPayload(t *testing.T) {
	s, clock := newS6PendingObservedService(t)
	s.cfg.PendingRingSize = 2
	addr := domain.PeerAddress("10.80.0.2:64646")
	for i := 0; i < 3; i++ {
		clock.set(time.Duration(i)*time.Hour, nil)
		if !s.queuePeerFrame(addr, s6PendingFrame(s, fmt.Sprintf("ring-%d", i))) {
			t.Fatal("ring append refused")
		}
	}
	v := requireS6Pending(t, s, 2)
	if v.RunStart != v.Baseline || v.RunStartCount != 0 || !v.RunHasIncrease || v.Sequence != 3 {
		t.Fatalf("atomic evict+append introduced a false decrease: %+v", v)
	}
	payload := s6PendingFrame(s, "ring-2")
	payload.Body = "updated"
	if !s.queuePeerFrame(addr, payload) {
		t.Fatal("payload replacement refused")
	}
	after := requireS6Pending(t, s, 2)
	if after.RunStart != v.RunStart || after.Sequence != 4 {
		t.Fatalf("replacement changed queue run: %+v", after)
	}
	clock.set(3*time.Hour, nil)
	s.finishAddressEviction(&addressEviction{address: addr})
	after = requireS6Pending(t, s, 0)
	if after.RunHasIncrease || after.RunStart.Earliest != 3*time.Hour || after.LongestGrowingRun.Lower != 3*time.Hour {
		t.Fatalf("actual address eviction not observed: %+v", after)
	}
}

func TestS6PendingQueueDrainMerge(t *testing.T) {
	s, clock := newS6PendingObservedService(t)
	addr := domain.PeerAddress("10.80.0.3:64646")
	frame := s6PendingFrame(s, "drain-no-route")
	if !s.queuePeerFrame(addr, frame) {
		t.Fatal("enqueue refused")
	}
	before := requireS6Pending(t, s, 1)
	clock.set(time.Hour, nil)
	// No route: the actual identity drain extracts, attempts routing, and
	// merges its failed frame back. The two visible commits have net zero
	// across the call but DO reset the continuous run.
	s.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})
	after := requireS6Pending(t, s, 1)
	if after.Sequence != before.Sequence+2 || after.RunStart.Earliest != time.Hour || after.RunStartCount != 0 || !after.RunHasIncrease {
		t.Fatalf("extract/merge boundary lost: %+v", after)
	}
}

func TestS6PendingQueueCancellationAndReceipts(t *testing.T) {
	for _, path := range []string{"single", "batch", "failed", "recipient", "receipt"} {
		t.Run(path, func(t *testing.T) {
			s, clock := newS6PendingObservedService(t)
			addr := domain.PeerAddress("10.80.0.4:64646")
			frame := s6PendingFrame(s, "clear-owner")
			if !s.queuePeerFrame(addr, frame) {
				t.Fatal("enqueue refused")
			}
			wantSequence := uint64(2)
			if path == "receipt" {
				if !s.queuePeerFrame(addr, protocol.Frame{Type: "relay_delivery_receipt", ID: frame.ID,
					Address: frame.Recipient, Recipient: frame.Address, Status: protocol.ReceiptStatusDelivered}) {
					t.Fatal("receipt enqueue refused")
				}
				wantSequence++
			}
			clock.set(time.Hour, nil)
			switch path {
			case "single":
				if _, err := s.CancelOutgoingDelivery(protocol.MessageID(frame.ID), idTargetX); err != nil {
					t.Fatal(err)
				}
			case "batch":
				if _, err := s.CancelOutgoingDeliveriesTo(idTargetX, nil); err != nil {
					t.Fatal(err)
				}
			case "failed":
				s.failDelivery(frameEnvelope(frame), "failed", "source test")
			case "recipient":
				s.dropPendingForRecipient(frame.Recipient)
			case "receipt":
				r := protocol.DeliveryReceipt{MessageID: protocol.MessageID(frame.ID), Sender: frame.Recipient,
					Recipient: frame.Address, Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC()}
				if _, _, _, result := s.finishReceipt(receiptKeyOf(r), r, false); result != committedFirst {
					t.Fatalf("receipt rejected: %v", result)
				}
			}
			v := requireS6Pending(t, s, 0)
			if v.Sequence != wantSequence || v.RunHasIncrease || v.RunStart.Earliest != time.Hour {
				t.Fatalf("nested clears did not form one real owner commit: %+v", v)
			}
		})
	}
}

func TestS6PendingQueueEvictionAndFireAndForget(t *testing.T) {
	t.Run("orphan", func(t *testing.T) {
		s, clock := newS6PendingObservedService(t)
		addr := domain.PeerAddress("10.80.0.5:64646")
		if !s.queuePeerFrame(addr, s6PendingFrame(s, "orphan")) {
			t.Fatal("enqueue refused")
		}
		s.peerMu.Lock()
		s.health[addr] = &peerHealth{Address: addr, LastDisconnectedAt: time.Now().Add(-2 * orphanedHealthEvictWindow)}
		s.peerMu.Unlock()
		clock.set(time.Hour, nil)
		s.evictOrphanedHealthEntries()
		v := requireS6Pending(t, s, 0)
		if v.Sequence != 2 || v.RunStart.Earliest != time.Hour {
			t.Fatalf("orphan removal lost: %+v", v)
		}
	})
	t.Run("fire-and-forget", func(t *testing.T) {
		s, clock := newS6PendingObservedService(t)
		const conn = netcore.ConnID(7842)
		_ = attachPushObserver(t, s, idTargetX.String(), conn)
		addr := domain.PeerAddress("10.80.0.6:64646")
		frame := protocol.Frame{Type: "push_message", Item: &protocol.MessageFrame{ID: "fire", Sender: idPeerB.String(), Recipient: idTargetX.String()}}
		if !s.queuePeerFrame(addr, frame) {
			t.Fatal("enqueue refused")
		}
		clock.set(time.Hour, nil)
		s.flushPendingFireAndForget(conn, addr)
		v := requireS6Pending(t, s, 0)
		if v.Sequence != 2 || v.RunStart.Earliest != time.Hour {
			t.Fatalf("fire-and-forget extraction lost: %+v", v)
		}
	})
}

func TestS6PendingQueueCoverageAndClock(t *testing.T) {
	s, clock := newS6PendingObservedService(t)
	addr := domain.PeerAddress("10.80.0.7:64646")
	first := requireS6Pending(t, s, 0)
	other, _ := newS6PendingObservedService(t)
	if next := requireS6Pending(t, other, 0); first.SourceID == next.SourceID {
		t.Fatal("source instances collided")
	}
	if absent := newTestService(t, config.NodeTypeFull).PendingQueueObservation(); absent.Available || absent.CoverageValid || absent.UnknownReason != "source_unavailable" {
		t.Fatalf("absent source treated as observed zero: %+v", absent)
	}
	clock.set(time.Hour, nil)
	if !s.queuePeerFrame(addr, s6PendingFrame(s, "clock-before")) {
		t.Fatal("enqueue refused")
	}
	clock.set(0, errors.New("unavailable duration"))
	if !s.queuePeerFrame(addr, s6PendingFrame(s, "clock-error")) {
		t.Fatal("observer error changed enqueue outcome")
	}
	clock.set(2*time.Hour, nil)
	v := s.PendingQueueObservation()
	if !v.CountValid || v.Count != 2 || v.DurationValid || v.CoverageValid || v.UnknownReason != "duration_read_failed" {
		t.Fatalf("read gap bridged or actual count lost: %+v", v)
	}
	t.Run("regression", func(t *testing.T) {
		s, c := newS6PendingObservedService(t)
		c.set(time.Hour, nil)
		if !s.queuePeerFrame(addr, s6PendingFrame(s, "later")) {
			t.Fatal("enqueue refused")
		}
		c.set(time.Second, nil)
		v := s.PendingQueueObservation()
		if v.DurationValid || v.CoverageValid || v.UnknownReason != "duration_regressed" {
			t.Fatalf("regression bridged: %+v", v)
		}
	})
	t.Run("sequence-overflow", func(t *testing.T) {
		s, _ := newS6PendingObservedService(t)
		// Fault injection of exhausted metadata, not a fabricated queue event.
		s.deliveryMu.Lock()
		s.pendingObservation.sequence = ^uint64(0)
		s.deliveryMu.Unlock()
		if !s.queuePeerFrame(addr, s6PendingFrame(s, "overflow")) {
			t.Fatal("observer fault changed queue outcome")
		}
		v := s.PendingQueueObservation()
		if v.CountValid || v.CoverageValid || v.Sequence != ^uint64(0) || v.UnknownReason != "sequence_overflow" {
			t.Fatalf("sequence wrapped: %+v", v)
		}
	})
}

func TestS6PendingQueueOwnerRace(t *testing.T) {
	s, _ := newS6PendingObservedService(t)
	addr := domain.PeerAddress("10.80.0.8:64646")
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				frame := s6PendingFrame(s, fmt.Sprintf("race-%d-%d", worker, i))
				s.queuePeerFrame(addr, frame)
				_, _ = s.CancelOutgoingDelivery(protocol.MessageID(frame.ID), idTargetX)
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 24; i++ {
			s.PendingQueueObservation()
		}
	}()
	wg.Wait()
	v := requireS6Pending(t, s, 0)
	if v.Sequence != 72 {
		t.Fatalf("lost owner commit: %+v", v)
	}
}
