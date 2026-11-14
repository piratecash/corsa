package netcore

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func observedWriter(t *testing.T) (*NetCore, chan<- struct{}, <-chan struct{}, func()) {
	t.Helper()
	permit := make(chan struct{})
	entered := make(chan struct{}, 4)
	conn := newMockConnWithWriter(t, func(data []byte) (int, error) {
		entered <- struct{}{}
		<-permit
		return len(data), nil
	})
	core := New(1, conn, Inbound, Options{ObserveWriterQueue: true})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			done := make(chan struct{})
			go func() { defer close(done); core.Close() }()
			<-core.closing
			close(permit)
			<-done
		})
	}
	t.Cleanup(stop)
	return core, permit, entered, stop
}

func waitObservedWrite(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter Write")
	}
}

func sendObservedWriterFrame(t *testing.T, core *NetCore) {
	t.Helper()
	if status := core.SendRaw([]byte("frame\n")); status != SendOK {
		t.Fatalf("SendRaw = %v", status)
	}
}

func TestWriterQueueObservationKeepsDecreaseAcrossCuts(t *testing.T) {
	core, permit, entered, stop := observedWriter(t)
	sendObservedWriterFrame(t, core)
	waitObservedWrite(t, entered)
	sendObservedWriterFrame(t, core)
	before := core.WriterQueueObservation()
	permit <- struct{}{}
	waitObservedWrite(t, entered)
	sendObservedWriterFrame(t, core)
	after := core.WriterQueueObservation()
	if before.Instance == ([16]byte{}) || after.Instance != before.Instance || after.At < before.At ||
		before.UnknownReason != "" || after.UnknownReason != "" || before.Depth != 1 || after.Depth != 1 ||
		before.Enqueued != 2 || before.Taken != 1 || after.Enqueued != 3 || after.Taken != 2 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	stop()
	end := core.WriterQueueObservation()
	if !end.WriterExited || end.UnknownReason != "" || end.Depth != 0 || end.Enqueued != 3 || end.Taken != 2 || end.Drained != 1 {
		t.Fatalf("terminal cut = %+v", end)
	}
	if status := core.SendRaw([]byte("late\n")); status != SendChanClosed {
		t.Fatalf("late SendRaw = %v", status)
	}
}

func TestWriterQueueObservationBlockingCancelAndWake(t *testing.T) {
	core, permit, entered, stop := observedWriter(t)
	sendObservedWriterFrame(t, core)
	waitObservedWrite(t, entered)
	for range sendChBuffer {
		sendObservedWriterFrame(t, core)
	}
	if status := core.SendRaw([]byte("overflow\n")); status != SendBufferFull {
		t.Fatalf("full queue = %v", status)
	}
	item := sendItem{data: []byte("blocked\n")}
	cancel := make(chan struct{})
	close(cancel)
	if status := core.queueFrameBlocking(item, cancel, nil, 0); status != SendCtxCancelled {
		t.Fatalf("cancelled offer = %v", status)
	}
	deadline := make(chan time.Time, 1)
	deadline <- time.Now()
	if status := core.queueFrameBlocking(item, nil, deadline, 0); status != SendTimeout {
		t.Fatalf("timed-out offer = %v", status)
	}
	result := make(chan SendStatus, 1)
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	go func() { result <- core.queueFrameBlocking(item, nil, timeout.C, 0) }()
	// The writer is inside Write and the queue is full. Only the blocked
	// producer can install a wait notification in this interval.
	observation := core.writerQueueObservation
	limit := time.Now().Add(time.Second)
	for {
		observation.mu.Lock()
		waiting := observation.changed != nil
		observation.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("producer did not enter the full-queue wait")
		}
		runtime.Gosched()
	}
	permit <- struct{}{}
	waitObservedWrite(t, entered)
	select {
	case status := <-result:
		if status != SendOK {
			t.Fatalf("unblocked offer = %v", status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking offer did not return")
	}
	stop()
	cut := core.WriterQueueObservation()
	if cut.Depth != 0 || cut.UnknownReason != "" || cut.Enqueued != sendChBuffer+2 || cut.Taken != 2 || cut.Drained != sendChBuffer {
		t.Fatalf("terminal cut = %+v", cut)
	}
}

func TestWriterQueueObservationOffForOrdinaryConnection(t *testing.T) {
	conn, _ := newBufferedMockConn(t)
	core := New(1, conn, Inbound, Options{})
	t.Cleanup(core.Close)
	sendObservedWriterFrame(t, core)
	if snapshot := core.WriterQueueObservation(); snapshot != (WriterQueueSnapshot{UnknownReason: "source_unavailable"}) {
		t.Fatalf("ordinary writer observation = %+v", snapshot)
	}
}

func TestWriterQueueObservationCountsSocketFailureDrain(t *testing.T) {
	entered, fail := make(chan struct{}), make(chan struct{})
	conn := newMockConnWithWriter(t, func([]byte) (int, error) {
		close(entered)
		<-fail
		return 0, errors.New("test socket failure")
	})
	core := New(1, conn, Inbound, Options{ObserveWriterQueue: true})
	var once sync.Once
	release := func() { once.Do(func() { close(fail) }) }
	t.Cleanup(func() { release(); core.Close() })
	sendObservedWriterFrame(t, core)
	waitObservedWrite(t, entered)
	sendObservedWriterFrame(t, core)
	release()
	select {
	case <-core.writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not report socket failure")
	}
	if cut := core.WriterQueueObservation(); cut.UnknownReason != "" || cut.Depth != 0 || cut.Enqueued != 2 ||
		cut.Taken != 1 || cut.Drained != 1 || cut.WriterExited {
		t.Fatalf("failed writer cut = %+v", cut)
	}
	if status := core.SendRaw([]byte("late\n")); status != SendWriterDone {
		t.Fatalf("late offer = %v", status)
	}
}
