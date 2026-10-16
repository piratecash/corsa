package node

import (
	"io"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"
)

// heartbeatMaxJitter is the largest delay added to a heartbeat interval, so
// that the pings of many sessions opened together do not stay in step.
const heartbeatMaxJitter = 14 * time.Second

// diallerHeartbeatInterval is the base delay between the pings of the side
// that dialled a session. It is shorter than the listener's heartbeatInterval
// because the dialler's pings are what the listener's read bound waits for:
// every released build (v28 … v30) closes an inbound connection on which no
// whole frame arrived for 120 s (inboundReadTimeout), and pings at most
// 75 s + 14 s apart reach it with 31 s to spare for the round trip of the
// previous pong, the writer queue and the scheduler. Pinging every 2 min
// (+ up to 14 s) as the listener does left that bound no room at all: an
// idle session was cut every time the two schedules did not interleave.
const diallerHeartbeatInterval = 75 * time.Second

// inboundSessionIdleTimeout is how long an inbound SESSION may go without the
// first byte of a frame before it is closed. It waits for the dialler's
// schedule, not for this node's own: the slowest dialler is a released build,
// which pings at most heartbeatInterval + heartbeatMaxJitter (2 min 14 s)
// apart, and the bound leaves it 31 s for delivery. It is also the inbound
// stall threshold the periodic sweep (evictStaleInboundConns) and the
// connected-hosts view apply, so the read deadline and the sweep never
// disagree about when a session is dead.
const inboundSessionIdleTimeout = heartbeatInterval + pongStallTimeout

// livenessSchedule is the heartbeat schedule of a session together with the
// read bounds that have to agree with it. They are one value because they
// are one contract: the side that reads a connection may only call it dead
// after the peer's own schedule has had its chance to put a frame on it.
type livenessSchedule struct {
	// diallerPingInterval is the base delay between the pings of the side
	// that dialled the session (servePeerSession).
	diallerPingInterval time.Duration
	// listenerPingInterval is the base delay between the pings of the side
	// that accepted it (inboundHeartbeat).
	listenerPingInterval time.Duration
	// maxPingJitter is the largest delay pickJitter adds to either interval.
	maxPingJitter time.Duration
	// pongStall is how long a ping waits for its pong before the session is
	// torn down, on both sides.
	pongStall time.Duration
	// inboundFrameRead bounds an inbound frame from its first byte to its
	// end, and every wait of an inbound connection that has not become a
	// session yet — the first byte, each handshake frame. It is the
	// Slowloris bound: a peer that trickles a frame or stalls its handshake
	// holds the connection slot no longer than this.
	inboundFrameRead time.Duration
	// inboundSessionIdle bounds the wait for the first byte of the next frame
	// on an inbound session (its heartbeat running), and is the stall
	// threshold of the inbound sweep.
	inboundSessionIdle time.Duration
	// pickJitter draws the jitter of one ping from [0, max].
	pickJitter func(max time.Duration) time.Duration
}

// productionLiveness is the schedule every node runs.
func productionLiveness() livenessSchedule {
	return livenessSchedule{
		diallerPingInterval:  diallerHeartbeatInterval,
		listenerPingInterval: heartbeatInterval,
		maxPingJitter:        heartbeatMaxJitter,
		pongStall:            pongStallTimeout,
		inboundFrameRead:     inboundReadTimeout,
		inboundSessionIdle:   inboundSessionIdleTimeout,
		pickJitter:           uniformPingJitter,
	}
}

// uniformPingJitter draws a jitter uniformly from [0, max].
func uniformPingJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}

// diallerPingDelay is the delay before the dialler's next ping.
func (l livenessSchedule) diallerPingDelay() time.Duration {
	return l.diallerPingInterval + l.pickJitter(l.maxPingJitter)
}

// listenerPingDelay is the delay before the listener's next ping.
func (l livenessSchedule) listenerPingDelay() time.Duration {
	return l.listenerPingInterval + l.pickJitter(l.maxPingJitter)
}

// inboundIdle is the bound on the wait for the next frame of an inbound
// connection: the session bound once it is a session, the frame bound until
// then, so a stalled handshake is held no longer than it always was.
func (l livenessSchedule) inboundIdle(session bool) time.Duration {
	if session {
		return l.inboundSessionIdle
	}
	return l.inboundFrameRead
}

// sessionLiveness is the schedule this Service runs. A struct-literal test
// Service without one runs the production schedule, the same way it runs as
// a v1 node without secure-session state (sessionMode).
func (s *Service) sessionLiveness() livenessSchedule {
	if s.liveness == nil {
		return productionLiveness()
	}
	return *s.liveness
}

// inboundFrameDeadline is the socket as the reader of an inbound connection
// sees it — directly for v1, under TLS for v2 — and it is what tells an idle
// wait from a frame in progress. The read loop arms it with the idle bound
// before it waits for a frame; the first byte the socket delivers after that
// replaces the idle bound with the frame bound. Watching the socket rather
// than the line reader is what keeps the frame bound for v2: TLS hands the
// reader nothing until a whole record has arrived, so a record trickled byte
// by byte would otherwise be held to the longer idle bound.
//
// Bytes that are already past the socket when the wait begins — a partial
// TLS record or decrypted plaintext buffered below the line reader — start no
// clock of their own: until the socket delivers another byte, the idle bound
// applies. A frame already begun in the line reader's own buffer is reported
// by the caller (awaitFrame's started) and gets the frame bound at once.
type inboundFrameDeadline struct {
	conn         net.Conn
	frameTimeout time.Duration
	// waiting is set by awaitFrame and cleared by the first byte Read sees
	// after it. Both run on the connection's read goroutine; the atomic keeps
	// the flag sound should a second reader ever appear.
	waiting atomic.Bool
}

var _ io.Reader = (*inboundFrameDeadline)(nil)

func newInboundFrameDeadline(conn net.Conn, frameTimeout time.Duration) *inboundFrameDeadline {
	return &inboundFrameDeadline{conn: conn, frameTimeout: frameTimeout}
}

func (d *inboundFrameDeadline) Read(p []byte) (int, error) {
	n, err := d.conn.Read(p)
	if n > 0 && d.waiting.CompareAndSwap(true, false) {
		if deadlineErr := d.conn.SetReadDeadline(time.Now().Add(d.frameTimeout)); deadlineErr != nil && err == nil {
			err = deadlineErr
		}
	}
	return n, err
}

// awaitFrame sets the read deadline for the next frame: the frame bound now
// when the frame has already started, otherwise idle until the socket
// delivers its first byte.
func (d *inboundFrameDeadline) awaitFrame(idle time.Duration, started bool) error {
	if started {
		d.waiting.Store(false)
		return d.conn.SetReadDeadline(time.Now().Add(d.frameTimeout))
	}
	d.waiting.Store(true)
	return d.conn.SetReadDeadline(time.Now().Add(idle))
}
