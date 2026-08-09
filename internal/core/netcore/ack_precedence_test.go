package netcore

import (
	"testing"
	"time"
)

// ack_precedence_test.go pins the one answer a sync send may never give: a
// frame the writer already handed to the socket must not come back as a
// failure.
//
// `select` picks UNIFORMLY at random among the cases that are ready, so
// writing the ack arm first buys nothing. When the ack closes in the same
// instant the link dies, the caller's ctx is cancelled or the flush deadline
// expires, three answers out of four are a report of a write that provably
// happened — and SendOK is the only one of the four the caller may act on as
// "flushed". Precedence for a proof therefore has to be a second,
// non-blocking read of the ack before any failure is returned, not the order
// of the cases.
//
// Every case below arms the ack AND its competing arm BEFORE the call, so the
// interleaving is the starting state rather than something the test races for.

// armedFlush is one starting state of awaitFlush: the ack is closed and so is
// at least one of the arms that would otherwise report a failure.
type armedFlush struct {
	name       string
	writerGone bool
	cancelled  bool
	expired    bool
}

func TestAwaitFlushAnswersSendOKWhenTheAckIsReadyBesideAFailure(t *testing.T) {
	t.Parallel()

	cases := []armedFlush{
		{name: "writer_done", writerGone: true},
		{name: "ctx_cancelled", cancelled: true},
		{name: "deadline_expired", expired: true},
		{name: "every_arm_at_once", writerGone: true, cancelled: true, expired: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pc := New(31, newAutoConn(), Inbound, Options{})
			defer pc.Close()

			ack := make(chan struct{})
			close(ack)

			if tc.writerGone {
				// The writer's own exit signal, reached the same way the
				// writer reaches it — the gate stays open, so teardownStatus
				// answers SendWriterDone.
				pc.signalWriterDone()
			}

			var cancel chan struct{}
			if tc.cancelled {
				cancel = make(chan struct{})
				close(cancel)
			}

			var deadline chan time.Time
			if tc.expired {
				deadline = make(chan time.Time)
				close(deadline)
			}

			if st := pc.awaitFlush(ack, cancel, deadline); st != SendOK {
				t.Fatalf("awaitFlush with a closed ack beside %s answered %v, want SendOK: "+
					"the writer closes the ack only after the bytes left, so this frame "+
					"was reported lost after it had provably been written", tc.name, st)
			}
		})
	}
}

// TestSettleEnqueuedFrameKeepsTheAckProofOverTheGate is the enqueue-side twin
// of the property above: the same proof, read at the other end of the send
// path. It is here so a future rewrite of the shared non-blocking ack read
// cannot lose one of its two call sites silently.
func TestSettleEnqueuedFrameKeepsTheAckProofOverTheGate(t *testing.T) {
	t.Parallel()
	pc := New(32, newAutoConn(), Inbound, Options{})
	defer pc.Close()

	ack := make(chan struct{})
	close(ack)
	pc.gate.Store(int32(gateSocketFailed))

	if st := pc.settleEnqueuedFrame(sendItem{data: []byte("flushed\n"), ack: ack}); st != SendOK {
		t.Fatalf("settleEnqueuedFrame with a closed ack under a shut gate answered %v, want SendOK", st)
	}
	if st := pc.settleEnqueuedFrame(sendItem{data: []byte("unproven\n")}); st != SendWriterDone {
		t.Fatalf("settleEnqueuedFrame without an ack under a shut gate answered %v, want SendWriterDone", st)
	}
}
