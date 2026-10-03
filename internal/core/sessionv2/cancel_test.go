package sessionv2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// cancel_test.go pins cancellation at the edges of the handshake. The cancel
// callback runs on its own goroutine; each case cancels at a named phase and
// WAITS for that callback to have run, so the interleaving is the one named,
// not whatever the scheduler picked.

// cancelRun is what a cancelled dial left: its result and the listener's
// session, when the listener got one.
type cancelRun struct {
	session  ProvenSession
	err      error
	listener ProvenSession
}

func dialWithCancelAt(t *testing.T, at handshakePhase) cancelRun {
	t.Helper()
	x, b := newNode(t), newNode(t)
	xSide, bSide := connPair(t)
	accepted := acceptAsync(context.Background(), bSide, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan struct{})
	hooks := handshakeHooks{
		phase: func(p handshakePhase) {
			if p != at {
				return
			}
			cancel()
			select {
			case <-ran:
			case <-time.After(5 * time.Second):
				t.Error("the cancel callback never ran")
			}
		},
		cancelRan: func() { close(ran) },
	}
	session, err := Dial(ctx, xSide, x.local, testIntro(), AnyPeer(), testTimeouts, withHooks(hooks))
	return cancelRun{session: session, err: err, listener: (<-accepted).session}
}

// Cancelled between TLS and the proof phase: the proof deadline must not
// overwrite the cancellation and revive the handshake.
func TestACancelBetweenPhasesIsNotOverwrittenByTheNextDeadline(t *testing.T) {
	run := dialWithCancelAt(t, phaseTLSDone)
	if !errors.Is(run.err, context.Canceled) {
		t.Fatalf("Dial cancelled after TLS = %v, want context.Canceled", run.err)
	}
	// A cancelled dialer stops where it was cancelled: it does not go on to
	// sign and send its proof, so the listener never gets a session.
	if _, err := run.listener.Peer(); !errors.Is(err, ErrZeroSession) {
		t.Fatal("the listener established a session with a dialer cancelled before its proof")
	}
}

// Cancelled with the peer proven but before the handshake is settled: the
// handshake fails; it does not hand out a session.
func TestACancelBeforeSettlingFailsTheHandshake(t *testing.T) {
	run := dialWithCancelAt(t, phaseProven)
	if !errors.Is(run.err, context.Canceled) {
		t.Fatalf("Dial cancelled before settling = %v, want context.Canceled", run.err)
	}
	if _, peerErr := run.session.Peer(); !errors.Is(peerErr, ErrZeroSession) {
		t.Fatal("a cancelled handshake returned a session")
	}
}

// Cancelled once the handshake has settled: the session it returned stays
// usable. Before the fix the late callback set an expired deadline on it.
func TestACancelAfterSettlingLeavesTheSessionUsable(t *testing.T) {
	run := dialWithCancelAt(t, phaseSettled)
	if run.err != nil {
		t.Fatalf("Dial cancelled after settling = %v, want the session", run.err)
	}
	conn, _ := run.session.Conn()
	peerConn, _ := run.listener.Conn()
	go func() { _, _ = peerConn.Write([]byte("still alive\n")) }()
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write on the returned session: %v", err)
	}
	line, err := readLine(conn, 64)
	if err != nil || string(line) != "still alive" {
		t.Fatalf("read on the returned session: %q, %v", line, err)
	}
}

// RawLine bypasses the frame's fields when it is marshalled, so an intro
// carrying one would send whatever it holds instead of the identity the
// package stamps. Dial and Accept refuse it before any byte is sent.
func TestAnIntroWithARawLineIsRefused(t *testing.T) {
	x := newNode(t)
	raw := protocol.Frame{RawLine: `{"type":"hello","challenge":"v1"}`}
	xSide, other := connPair(t)
	if _, err := Dial(context.Background(), xSide, x.local, raw, AnyPeer(), testTimeouts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Dial with a RawLine intro = %v, want ErrInvalidArgument", err)
	}
	_ = other
	lSide, _ := connPair(t)
	if _, err := Accept(context.Background(), lSide, x.local, raw, x.certs, testTimeouts); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Accept with a RawLine intro = %v, want ErrInvalidArgument", err)
	}
}
