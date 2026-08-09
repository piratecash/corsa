package datagram

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// queue.go implements the WEIGHTED class queue of §5.
//
// What it is and what it is NOT. This queue decides the ORDER in which the
// layer hands frames on for queueing; it does not replace peerSession.sendCh
// or NetCore.sendCh, which are M2's and remain the only things that own a
// socket. The boundary is exact: a frame leaves this queue when the layer is
// ready to call FrameEmitter.EmitTo for it, and everything after that — the
// per-peer write queue, the write deadline re-check and the write grace —
// belongs to netcore, and none of it reports back: the ticket travels one way
// (netcore/write_ticket.go). Per-PEER fairness is therefore not
// this queue's job either: each neighbour already has its own write queue.
// The only fairness dimension here is the one §5 names, between CLASSES.
//
// Why weighted and not strict priority. Strict priority means a permanent
// control stream stops file transfer completely, and a permanent control
// stream is cheap to produce. So control is served BEFORE bulk within its own
// share, and bulk keeps a guaranteed minimum share of the dispatched bytes —
// one quarter to start with (§5, §11.3).
//
// Why deficit round robin and why in BYTES. The share §5 promises is a share
// of BUDGET, and §5 measures budget in serialized frame bytes — the same
// quantity admission is charged in. Counting frames instead would hand bulk a
// sixteenth of the bytes it was promised, because a bulk frame is ~16 times a
// control frame on the wire. Deficit round robin gives the byte share exactly
// and needs no timers: each lane accumulates its weight in bytes per round
// and spends it on whole frames, carrying the remainder into the next round,
// so a frame larger than one quantum is never starved and a lane that empties
// banks nothing.
//
// Overflow contract, stated once so callers do not have to infer it:
//
//   - a lane is bounded in BOTH frames and bytes, whichever binds first;
//   - at the bound the NEW frame is refused and the caller is told
//     synchronously. Already-queued frames are never evicted to make room:
//     a queued frame may already have a reservation or a fixed reverse
//     downstream behind it, so silently dropping it would turn a "queued"
//     answer into a loss nobody observed, while refusing the newcomer is a
//     decision its own caller can still act on;
//   - a frame whose send deadline has already passed is refused on the way
//     in and dropped on the way out. That is the interaction with
//     `send_until`: the queue never hands the emitter a frame the writer
//     would only throw away, and the writer checks again anyway (§4.2) —
//     the two checks are deliberately redundant because queue residence is
//     exactly what the deadline bounds;
//   - a frame REMOVED here is COUNTED and nothing more. The layer keeps no
//     per-send record for a removal to close, so DroppedExpired is the whole
//     of what a caller can learn about it (§10).
//
// Reference: docs/protocol/datagram.md §4.2, §5, §9.

// QueuedFrame is one frame waiting for its turn, together with everything the
// emitter needs and nothing it does not.
type QueuedFrame struct {
	// SendUntil is the deadline the writer re-checks before the socket
	// write (§4.2). Zero means "no deadline", which is legal only for
	// frames the layer creates outside the three modes' timing rules; every
	// pipeline path sets it.
	SendUntil time.Time
	// Peer is the neighbour the frame is destined for.
	Peer domain.PeerIdentity
	// Frame is the datagram itself; its Class picks the lane.
	Frame protocol.DatagramFrame
	// Line is the serialized wire line the layer already produced. It travels
	// with the frame so the writer does not serialize it a second time (§2.3,
	// §5); a caller that has no line yet leaves it nil and only Bytes matters.
	Line []byte
	// Channel is the channel constraint of OutboundFrame, carried through the
	// lane unchanged.
	//
	// It travels for the same reason Line does — the frame is handed to the
	// writer on the FAR side of this lane, so anything the queue does not carry
	// is a fact the writer never learns. Dropping it here would silently
	// un-pin every answer the moment a build wired a class queue, which is every
	// production build: the pin would hold in a unit test that publishes straight
	// to the emitter and nowhere else.
	Channel ChannelID
	// Class picks the lane. It is stated rather than derived from the frame so
	// the queue, the write grace and netcore.OutboundWrite all read ONE value.
	Class domain.DatagramClass
	// Bytes is the SERIALIZED size of the frame, the same quantity
	// admission is charged in (§5). Callers that do not have it already can
	// take it from MeasureFrame; the queue refuses to guess, because a
	// guessed size is a wrong share.
	Bytes int
}

// QueueStats is the lock-free counter snapshot of the queue, shaped like
// AdmissionStats and routing.RouteCapStats: monotonic counters plus the
// current depth.
type QueueStats struct {
	// Enqueued counts frames accepted into a lane.
	Enqueued uint64
	// Dequeued counts frames handed to the emitter.
	Dequeued uint64
	// RefusedFull counts frames refused because their lane was at its
	// frame or byte bound.
	RefusedFull uint64
	// RefusedExpired counts frames refused on the way in because their send
	// deadline had already passed.
	RefusedExpired uint64
	// RefusedInvalid counts frames refused for a class the layer does not
	// have a lane for, or for a missing serialized size.
	RefusedInvalid uint64
	// DroppedExpired counts frames dropped on the way OUT: they waited past
	// their send deadline (§4.2).
	DroppedExpired uint64
	// ControlDepth and BulkDepth are the current per-lane frame counts.
	ControlDepth int
	// BulkDepth is the current bulk lane depth; see ControlDepth.
	BulkDepth int
	// ControlBytes and BulkBytes are the current per-lane byte totals.
	ControlBytes int
	// BulkBytes is the current bulk lane byte total; see ControlBytes.
	BulkBytes int
}

// WeightedQueueConfig wires the queue.
type WeightedQueueConfig struct {
	// Clock is the injectable time source, following the package
	// convention. Defaults to time.Now.
	Clock func() time.Time
	// Caps are the §5 queue numbers. Non-positive fields fall back to the
	// starting values.
	Caps QueueCaps
}

// lane is one class's FIFO plus its deficit.
type lane struct {
	items     []QueuedFrame
	bytes     int
	deficit   int
	quantum   int
	maxFrames int
	maxBytes  int
	class     domain.DatagramClass
}

func (l *lane) empty() bool { return len(l.items) == 0 }

func (l *lane) push(item QueuedFrame) {
	l.items = append(l.items, item)
	l.bytes += item.Bytes
}

func (l *lane) pop() QueuedFrame {
	item := l.items[0]
	// Clear the slot before reslicing: the frame holds a payload buffer, and
	// keeping a reference in the backing array would pin it for as long as
	// the lane lives.
	l.items[0] = QueuedFrame{}
	l.items = l.items[1:]
	l.bytes -= item.Bytes
	return item
}

func (l *lane) full(item QueuedFrame) bool {
	return len(l.items) >= l.maxFrames || l.bytes+item.Bytes > l.maxBytes
}

// WeightedQueue is the class-fair dispatch order of §5.
//
// Locking contract: mu guards the lanes and the round-robin cursor. Nothing
// external is called while it is held — the queue only decides who is next — so
// a caller may enqueue from a handler that the pump is at that moment draining
// for (CLAUDE.md: no callbacks under a lock).
type WeightedQueue struct {
	clock func() time.Time
	ready chan struct{}

	lanes []*lane
	turn  int

	enqueued       atomic.Uint64
	dequeued       atomic.Uint64
	refusedFull    atomic.Uint64
	refusedExpired atomic.Uint64
	refusedInvalid atomic.Uint64
	droppedExpired atomic.Uint64

	passLimit int
	credited  bool
	mu        sync.Mutex
}

// NewWeightedQueue builds the queue. Control is lane 0 and bulk is lane 1,
// and the order is the contract: within a round the control lane is offered
// its quantum first, which is what "control is served before bulk WITHIN its
// share" means (§5).
func NewWeightedQueue(cfg WeightedQueueConfig) *WeightedQueue {
	caps := cfg.Caps.normalized(DefaultLimits().Queue)
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	control := &lane{
		class:     domain.DatagramClassControl,
		quantum:   caps.ControlWeight * caps.QuantumBytes,
		maxFrames: caps.ControlFrames,
		maxBytes:  caps.ControlBytes,
	}
	bulk := &lane{
		class:     domain.DatagramClassBulk,
		quantum:   caps.BulkWeight * caps.QuantumBytes,
		maxFrames: caps.BulkFrames,
		maxBytes:  caps.BulkBytes,
	}
	queue := &WeightedQueue{clock: clock, ready: make(chan struct{}, 1), lanes: []*lane{control, bulk}}
	queue.passLimit = dispatchPassLimit(queue.lanes)
	return queue
}

// dispatchPassLimit is how many lane visits one Dequeue may spend before it
// gives up.
//
// A lane's deficit grows by its quantum on every visit, so a frame always
// fits eventually and the walk always terminates; the limit only has to be
// large enough that it never fires in practice. The worst case is the largest
// frame the queue can hold — a frame above protocol.MaxFrameLine is refused
// at Enqueue and cannot be sent at all — against the smallest quantum, so the
// limit is derived from the configuration instead of guessed, and a tiny
// quantum makes the walk longer rather than making the queue stall.
func dispatchPassLimit(lanes []*lane) int {
	smallest := 0
	for _, target := range lanes {
		if smallest == 0 || target.quantum < smallest {
			smallest = target.quantum
		}
	}
	if smallest <= 0 {
		smallest = 1
	}
	return len(lanes) * (protocol.MaxFrameLine/smallest + 2)
}

// Enqueue places a frame in its class lane. false means refused — see
// QueueStats for which of the three refusals it was.
func (q *WeightedQueue) Enqueue(item QueuedFrame) bool {
	// A frame above the wire line limit could never be written, and a size
	// nobody measured would give the lane a wrong share; both are refused
	// here rather than discovered by the writer.
	if item.Bytes <= 0 || item.Bytes > protocol.MaxFrameLine {
		q.refusedInvalid.Add(1)
		return false
	}
	now := q.clock()
	if expired(item.SendUntil, now) {
		q.refusedExpired.Add(1)
		return false
	}

	q.mu.Lock()
	target, known := q.laneOfLocked(item.Class)
	if !known {
		q.mu.Unlock()
		q.refusedInvalid.Add(1)
		return false
	}
	if target.full(item) {
		q.mu.Unlock()
		q.refusedFull.Add(1)
		return false
	}
	target.push(item)
	q.mu.Unlock()

	q.enqueued.Add(1)
	q.signalReady()
	return true
}

// Dequeue returns the next frame to hand to the emitter. false means the
// queue is empty — every frame it skipped on the way was past its send
// deadline and is counted in DroppedExpired, which is all that is owed for it
// (countExpired).
func (q *WeightedQueue) Dequeue() (QueuedFrame, bool) {
	now := q.clock()
	item, served, dead := q.takeNext(now)
	q.countExpired(dead)
	return item, served
}

// takeNext is the locked half of Dequeue: it picks the next frame and collects
// the dead ones it stepped over, so the caller can finalise them with the
// mutex released.
func (q *WeightedQueue) takeNext(now time.Time) (item QueuedFrame, served bool, dead []QueuedFrame) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Dead frames are removed once: nothing inside the walk can expire a
	// frame, because the walk runs on a single reading of the clock.
	dead = q.dropExpiredHeadsLocked(now)
	for pass := 0; pass < q.passLimit; pass++ {
		next, ok, exhausted := q.serveTurnLocked()
		switch {
		case ok:
			q.dequeued.Add(1)
			return next, true, dead
		case exhausted:
			return QueuedFrame{}, false, dead
		}
	}
	return QueuedFrame{}, false, dead
}

// DropExpired sweeps every lane for frames whose send deadline has passed. It
// is the maintenance form of the head check Dequeue already does: a queue
// nobody is draining should not hold dead frames against the lane bound.
func (q *WeightedQueue) DropExpired() int {
	now := q.clock()
	dead := q.takeExpired(now)
	q.countExpired(dead)
	return len(dead)
}

// takeExpired removes every dead frame of every lane and returns them.
func (q *WeightedQueue) takeExpired(now time.Time) []QueuedFrame {
	q.mu.Lock()
	defer q.mu.Unlock()

	var dead []QueuedFrame
	for _, target := range q.lanes {
		live := target.items[:0]
		for _, item := range target.items {
			if expired(item.SendUntil, now) {
				target.bytes -= item.Bytes
				dead = append(dead, item)
				continue
			}
			live = append(live, item)
		}
		for i := len(live); i < len(target.items); i++ {
			target.items[i] = QueuedFrame{}
		}
		target.items = live
	}
	return dead
}

// countExpired counts the frames the queue threw away because their send
// deadline passed while they waited.
//
// Counting is ALL that is owed. Such a frame was removed after the layer had
// already answered `queued` to its caller, and the queue is the only place that
// knows it never reached a socket — but nothing upstream is waiting on it: the
// layer keeps no per-send record, and repeating belongs to the protocol that
// created the frame.
func (q *WeightedQueue) countExpired(dead []QueuedFrame) {
	if len(dead) == 0 {
		return
	}
	q.droppedExpired.Add(uint64(len(dead)))
}

// Ready is the pump's wake-up signal: a single-slot channel poked on every
// accepted enqueue. It is coalescing on purpose — the pump drains until
// Dequeue says empty, so one wake-up per burst is all it needs, and a
// blocking notification would make Enqueue depend on the pump's liveness.
func (q *WeightedQueue) Ready() <-chan struct{} { return q.ready }

// Stats publishes the counters and the current depths.
func (q *WeightedQueue) Stats() QueueStats {
	stats := QueueStats{
		Enqueued:       q.enqueued.Load(),
		Dequeued:       q.dequeued.Load(),
		RefusedFull:    q.refusedFull.Load(),
		RefusedExpired: q.refusedExpired.Load(),
		RefusedInvalid: q.refusedInvalid.Load(),
		DroppedExpired: q.droppedExpired.Load(),
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, target := range q.lanes {
		switch target.class {
		case domain.DatagramClassControl:
			stats.ControlDepth, stats.ControlBytes = len(target.items), target.bytes
		case domain.DatagramClassBulk:
			stats.BulkDepth, stats.BulkBytes = len(target.items), target.bytes
		}
	}
	return stats
}

// serveTurnLocked is one step of deficit round robin.
//
// served reports that item is a frame to dispatch; exhausted reports that
// every lane is empty, which is the only way the walk ends without one.
func (q *WeightedQueue) serveTurnLocked() (item QueuedFrame, served, exhausted bool) {
	if q.emptyLocked() {
		return QueuedFrame{}, false, true
	}
	current := q.lanes[q.turn]
	if current.empty() {
		// A lane banks nothing while it is idle: without this reset a class
		// that went quiet for a minute would come back with a minute's worth
		// of credit and starve the other one.
		current.deficit = 0
		q.advanceLocked()
		return QueuedFrame{}, false, false
	}
	if !q.credited {
		current.deficit += current.quantum
		q.credited = true
	}
	head := current.items[0]
	if head.Bytes > current.deficit {
		q.advanceLocked()
		return QueuedFrame{}, false, false
	}
	dispatched := current.pop()
	current.deficit -= dispatched.Bytes
	if current.empty() {
		current.deficit = 0
		q.advanceLocked()
	}
	return dispatched, true, false
}

func (q *WeightedQueue) advanceLocked() {
	q.turn = (q.turn + 1) % len(q.lanes)
	q.credited = false
}

func (q *WeightedQueue) emptyLocked() bool {
	for _, target := range q.lanes {
		if !target.empty() {
			return false
		}
	}
	return true
}

// dropExpiredHeadsLocked removes dead frames from the front of every lane and
// returns them for the caller to finalise. The caller must hold mu.
//
// An expired frame does NOT charge the lane's deficit: nothing was sent, so
// charging for it would make a class pay for the queue's own latency.
func (q *WeightedQueue) dropExpiredHeadsLocked(now time.Time) []QueuedFrame {
	var dead []QueuedFrame
	for _, target := range q.lanes {
		for !target.empty() && expired(target.items[0].SendUntil, now) {
			dead = append(dead, target.pop())
		}
	}
	return dead
}

func (q *WeightedQueue) laneOfLocked(class domain.DatagramClass) (*lane, bool) {
	for _, target := range q.lanes {
		if target.class == class {
			return target, true
		}
	}
	return nil, false
}

func (q *WeightedQueue) signalReady() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// expired reports whether a send deadline has passed. The deadline itself is
// alive: the boundary belongs to the frame, exactly as it does for validity
// (§2.2).
func expired(sendUntil, now time.Time) bool {
	return !sendUntil.IsZero() && now.After(sendUntil)
}

// MeasureFrame returns the serialized size of a frame — the quantity both the
// budget of §5 and this queue account in. It exists so a caller never has to
// approximate: the number must be the wire size including base64 and the auth
// block, and the only honest way to get it is to serialize.
func MeasureFrame(frame protocol.DatagramFrame) (int, error) {
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		return 0, err
	}
	return len(line), nil
}
