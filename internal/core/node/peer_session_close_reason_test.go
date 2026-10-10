package node

import (
	"net"
	"sync"
	"testing"
)

// peer_session_close_reason_test.go pins the close-reason contract the CM
// session goroutine relies on: the first close of a peerSession is the one of
// record, and a goroutine that closes the session itself sees that reason as
// soon as its own Close has returned.

func newCloseReasonSession(t *testing.T) *peerSession {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	return &peerSession{address: "10.0.0.1:64646", conn: local}
}

func closerOf(t *testing.T, session *peerSession) peerSessionCloser {
	t.Helper()
	closer := session.closedBy.Load()
	if closer == nil {
		t.Fatal("closedBy is unset on a closed session")
	}
	return *closer
}

func TestPeerSessionCloseReason_UnsetWhileOpen(t *testing.T) {
	session := newCloseReasonSession(t)
	t.Cleanup(func() { _ = session.Close() })

	if session.closedBy.Load() != nil {
		t.Error("closedBy is set on a session nobody closed")
	}
	if session.closedByLocalEviction() {
		t.Error("an open session reads as closed by a local eviction")
	}
}

// The ConnectionManager closes first; the session goroutine's own Close then
// changes nothing and must see the eviction, not overwrite it. Sequential on
// purpose: the cross-goroutine ordering sync.Once provides is what
// TestPeerSessionCloseReason_RacingClosesAgreeOnTheFirstCloser exercises.
func TestPeerSessionCloseReason_LocalEvictionFirstIsVisibleAfterOwnClose(t *testing.T) {
	session := newCloseReasonSession(t)

	_ = session.closeAs(peerSessionClosedByLocalEviction)
	_ = session.Close()

	if got := closerOf(t, session); got != peerSessionClosedByLocalEviction {
		t.Errorf("closedBy = %d after the owner's later Close, want the eviction that closed first", got)
	}
	if !session.closedByLocalEviction() {
		t.Error("closedByLocalEviction = false after a local eviction closed the session first")
	}
}

// The owner closes first, having charged the peer; a later eviction of the
// same session must not turn that into a local decision.
func TestPeerSessionCloseReason_OwnerFirstIsNotRewrittenByALaterEviction(t *testing.T) {
	session := newCloseReasonSession(t)

	_ = session.Close()
	_ = session.closeAs(peerSessionClosedByLocalEviction)

	if got := closerOf(t, session); got != peerSessionClosedByOwner {
		t.Errorf("closedBy = %d after a later eviction, want the owner's close that came first", got)
	}
	if session.closedByLocalEviction() {
		t.Error("closedByLocalEviction = true for a session its owner closed first")
	}
}

// Racing closes: whichever wins, every closer reads the same reason as soon as
// its own call returns, and the reason never changes afterwards.
func TestPeerSessionCloseReason_RacingClosesAgreeOnTheFirstCloser(t *testing.T) {
	for i := 0; i < 100; i++ {
		session := newCloseReasonSession(t)

		var wg sync.WaitGroup
		seen := make([]peerSessionCloser, 2)
		closers := []peerSessionCloser{peerSessionClosedByLocalEviction, peerSessionClosedByOwner}
		for j, closer := range closers {
			wg.Add(1)
			go func(j int, closer peerSessionCloser) {
				defer wg.Done()
				_ = session.closeAs(closer)
				seen[j] = *session.closedBy.Load()
			}(j, closer)
		}
		wg.Wait()

		final := closerOf(t, session)
		if seen[0] != final || seen[1] != final {
			t.Fatalf("closers read %v after their own close, final reason %d: the reason of record moved", seen, final)
		}
	}
}
