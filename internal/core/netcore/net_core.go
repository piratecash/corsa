package netcore

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// NetCore owns a single network connection and is the single source of
// truth for inbound connection state: identity, address, capabilities,
// networks, auth, and traffic metering.
//
// The type enforces the single-writer invariant: net.Conn is a private
// field — the only way to send data is through Send() or SendSync(),
// which route through the internal write channel to a dedicated writer
// goroutine. This eliminates the class of bugs where code bypasses the
// designated writer goroutine by calling conn.Write() directly.
//
// Mutable fields (identity, address, caps, networks, auth, lastActivity)
// are protected by an internal RWMutex. Callers do not need to hold s.mu
// to read or write NetCore state — the accessors handle locking.
//
// Current scope: wraps inbound connections only. Outbound sessions continue
// to use peerSession until Phase 3, because servePeerSession owns both
// read and write and has request-reply semantics that require a different
// migration path.
//
// Outbound accounting: sendCh is the LOWER of the two queues on the
// outbound session path (the upper one is node.peerSession.sendCh). A frame
// may travel with a *WriteTicket carrying its outbound contract — send
// deadline and per-frame write grace — which the writer reads immediately
// before the socket write. The ticket is read-only and carries nothing back,
// so one ticket may serve every frame its contract describes. Frames without
// a ticket behave exactly as they did before the contract existed. See
// write_ticket.go for the contract itself and docs/locking.md
// ("peerSession.sendMu — the outbound queue fence") for who drains
// which queue on each death path.

type NetCore struct {
	id        ConnID
	direction Direction

	// mu protects mutable peer state below. Getters take RLock,
	// setters take Lock. Immutable fields (id, direction, connIDNum,
	// rawConn, metered, sendCh, writerDone) are not guarded.
	mu sync.RWMutex

	// Peer identity and capabilities — set during handshake.
	identity domain.PeerIdentity
	address  domain.PeerAddress
	caps     []domain.Capability
	networks map[domain.NetGroup]struct{}

	// declarations is the peer's RAW handshake self-description, the part
	// the typed caps slice above cannot express: capability names this
	// build does not know, and the declared dtype set. It lives here,
	// beside caps, because NetCore is the single source of truth for live
	// connection state — a parallel map keyed by the same connection would
	// be free to disagree with caps about one handshake. See
	// HandshakeDeclarations for the two wire contracts it carries.
	declarations HandshakeDeclarations

	// protocolVersion is the negotiated protocol version reported by
	// the peer during handshake. The exact frame depends on direction:
	//
	//   - Inbound conns receive it from hello.Version, applied by
	//     rememberConnPeerAddr in the node layer through ApplyOpts.
	//     The hello frame is the first frame an inbound peer sends, so
	//     this value is populated immediately on hello receipt.
	//   - Outbound sessions receive it from welcome.Version, mirrored
	//     onto the NetCore by applyWelcomeMetadata for symmetry with
	//     the rest of the welcome-derived peer state. Zero before the
	//     welcome frame is processed on this direction.
	//
	// The file router's inbound carve-out
	// (fileTransferPeerRouteMetaLocked) reads this field through
	// snapshotEntryLocked → connInfo.protocolVersion, so without the
	// inbound population path the carve-out collapsed to version 0 and
	// any peer with an outbound link would unfairly win the
	// protocolVersion DESC primary key.
	protocolVersion domain.ProtocolVersion

	// Auth state (inbound only, nil for outbound).
	// The pointer is swapped atomically via SetAuth/ClearAuth;
	// connauth.State is never mutated in place after creation.
	auth *connauth.State

	// Diagnostics — updated on every received frame.
	lastActivity time.Time

	// isLocal is true when the remote end is a loopback address
	// (127.0.0.0/8, ::1). Set once at registration time, immutable after.
	isLocal bool

	// captureSink receives a copy of every outbound write payload together
	// with its write outcome. Set once via SetCaptureSink from the lifecycle
	// layer; nil means capture is not active for this connection.
	// NetCore does not perform rule lookup — decision "this connection is
	// capture-enabled" is made outside, per plan §7.1.
	captureSink CaptureSink

	// Writer goroutine state.
	//
	// sendCh is NEVER closed. It has many producers on arbitrary goroutines
	// and exactly one consumer, so no producer can ever prove it is the last
	// one, and a producer racing a close panics on send. The upper queue on
	// the same path (peerSession.sendCh) settled this the same way — see
	// docs/locking.md, "peerSession.sendMu — the outbound queue fence". The
	// writer is told to stop through `closing` instead, and the buffer is
	// released with the NetCore itself.
	//
	// closing closes exactly once, in Close(), and is the writer's exit
	// signal. It is a separate channel from writerDone/writerExited because
	// those two report what the WRITER did; this one carries the owner's
	// decision to the writer.
	//
	// writerDone closes as soon as the writer stops writing to the socket —
	// on the first write failure or on normal exit. It is the signal the
	// session loop waits on to notice a dead link.
	//
	// writerExited closes only when the writer goroutine actually returns,
	// which happens strictly after every item that ever entered sendCh has
	// been finalised. Close() waits on THIS one: after a write failure the
	// writer keeps owning sendCh in drain-only mode (see writerLoop), so
	// writerDone alone would let Close() return while items were still
	// unaccounted for.
	//
	// gate holds a sendGate and is the single source of truth for "may this
	// queue still accept a frame". It is raised by the writer the instant a
	// socket write fails or a frame's write grace runs out, BEFORE it drains
	// what is left, and by Close() before it signals the writer. From that
	// moment the queue accepts nothing new: the writer keeps consuming sendCh
	// to finalise the residue, which frees slots, and without this gate a
	// producer would get SendOK — "accepted, will be written" — for a frame
	// the writer can only throw away. That lie is not cosmetic: the session
	// loop accounts SendOK as a useful write and would keep a peer with a
	// dead socket looking healthy.
	//
	// Producers read the gate TWICE — once before offering the frame and
	// once when the frame is already in the queue. A single read before the
	// offer is not a gate: a producer that read gateOpen a moment before the
	// gate was raised would still be told SendOK for a frame that lands in a
	// queue the drain has already walked past. See queueFrame and
	// settleEnqueuedFrame for why the second read closes that window instead
	// of narrowing it.
	sendCh        chan sendItem
	closing       chan struct{}
	writerDone    chan struct{}
	writerDoneOne sync.Once
	writerExited  chan struct{}
	gate          atomic.Int32

	// enqueueBarrier, when non-nil, runs inside every producer between the
	// pre-offer gate check and the channel offer. Production never
	// installs one, so the call site is a load and a branch.
	//
	// It exists because the interleaving the two-sided check defends against
	// lives entirely inside one function call and cannot be produced from
	// outside the package; a test that approximated it with sleeps would pin
	// the scheduler instead of the invariant. Per-connection rather than a
	// package-level var because the tests in this package run in parallel and
	// a shared hook would be written by one of them while another one's
	// producers read it.
	//
	// Installed before the first Send* on this connection and never changed
	// afterwards — the same immutability contract that lets NewBootstrap
	// override writeDeadline after construction.
	enqueueBarrier func()

	// Metering.
	metered *MeteredConn

	// Private — never exposed outside NetCore methods.
	rawConn   net.Conn
	closeOnce sync.Once

	// remoteAddrStr is the cached string form of rawConn.RemoteAddr(),
	// computed once at construction. The remote address of an established
	// connection never changes, but net.(*TCPAddr).String() re-runs
	// JoinHostPort + IP.String on every call — RemoteAddr() sits on the
	// snapshotEntryLocked / sendGossipFrameToPeer hot path and showed up as
	// ~21M allocations (net.(*TCPAddr).String) in alloc_objects. Caching the
	// string here makes RemoteAddr() allocation-free. Immutable after
	// construction, so no lock is required.
	remoteAddrStr string

	// Immutable after construction.
	connIDNum uint64

	// writeDeadline is the per-write deadline applied by writerLoop before
	// each socket write. The value is direction-specific: inbound reuses the
	// generic connWriteTimeout, outbound reuses sessionWriteTimeout so that
	// slow-peer eviction for dialled sessions keeps the same back-pressure
	// characteristics it had before outbound writes were routed through the
	// managed send path. Immutable after construction for peerSession-owned
	// NetCores; newBootstrapNetCore overrides the value once, before any
	// Send* call can happen, and the writer goroutine reads the field only
	// on dequeue — see newBootstrapNetCore for why the override is safe.
	writeDeadline time.Duration
}

// sendItem carries serialised frame data through the per-connection send
// channel. When ack is non-nil the writer goroutine closes it after the
// data has been handed to the socket, letting the caller block until the
// write completes (used by SendSync for error-path frames that must reach
// the wire before the connection is torn down).
//
// ticket carries the optional outbound contract (send deadline, write
// grace) and travels WITH the element rather than in a side table — see
// WriteTicket. nil for every legacy send path, which is what keeps those
// paths byte-for-byte unchanged.
type sendItem struct {
	data   []byte
	ack    chan struct{}
	ticket *WriteTicket
}

// ConnID is a monotonic connection identifier. Using a typed integer instead
// of net.Conn as a map key prevents accidental access to the raw connection.
//
// The canonical declaration lives in the domain package; netcore.ConnID is a
// type alias so existing call sites that write netcore.ConnID(x) or refer to
// netcore.ConnID in signatures continue to compile, while session-level and
// registry-level code can name the identifier through domain.ConnID without
// introducing a second parallel type.
type ConnID = domain.ConnID

// Direction indicates whether a connection was initiated by us (outbound)
// or accepted from a remote peer (inbound).
type Direction int

const (
	Inbound Direction = iota
	Outbound
)

// sendChBuffer is the per-connection write queue depth. When the queue
// is full, Send returns SendBufferFull and the CALLER decides what that
// means: the fire-and-forget session path drops the frame (best-effort
// contract, session survives), the blocking control-plane path waits.
// 512 frames absorbs a connect-time burst (pending-backlog flush +
// full table sync chunks + gossip fanout) against a peer that drains
// slowly; the previous 128 overflowed within seconds under that burst,
// and at the time overflow cost a whole session teardown. Genuinely
// dead sockets are detected by the per-write deadline in writerLoop
// (writeDeadlineFor), not by queue depth.
const sendChBuffer = 512

// Options carries the peer state to populate at construction time.
// All fields are optional — omitted fields stay at zero value. Using a
// single struct instead of scattered Set* calls ensures that the peer
// state is configured atomically and nothing is forgotten.
type Options struct {
	Address         domain.PeerAddress
	Identity        domain.PeerIdentity
	Caps            []domain.Capability
	Networks        map[domain.NetGroup]struct{}
	LastActivity    time.Time
	ProtocolVersion domain.ProtocolVersion

	// Declarations carries the raw handshake self-description. It is a
	// POINTER, unlike every other field here, because both of its slices
	// legitimately validate to nil — an out-of-bounds capability list is
	// defined to empty the whole raw set (§2.2), and an absent dtypes
	// field is defined to declare no type (§6.1). With a value field the
	// "only non-zero fields are applied" rule of ApplyOpts would silently
	// turn either of those two verdicts into "keep whatever was there
	// before", which is the one outcome the spec rules out.
	Declarations *HandshakeDeclarations
}

// HandshakeDeclarations is the peer's RAW, self-declared handshake state:
// the part of hello/welcome that the compile-time typed capability set
// cannot represent. Both fields carry a closed wire contract from
// docs/refactoring/datagram-transport.md, and both are ALREADY VALIDATED by
// the time they reach NetCore — this type stores a verdict, it does not
// re-derive one.
type HandshakeDeclarations struct {
	// AdvertisedNames is the validated RAW capability set, kept beside the
	// typed set because intersectCapabilities drops every name this build
	// does not know — a name released next year would then have nothing to
	// match against. It is consulted by the datagram role gate; dispatch and
	// every existing decision keep running on the typed set. nil means "no
	// name survived validation", which is also what a bounds breach produces:
	// the breach empties the WHOLE set, leaves the typed set untouched and
	// never tears the session down.
	AdvertisedNames []domain.CapabilityName

	// DeclaredDTypes is the dtype set of §6.1, and it keeps the wire
	// distinction a bare slice cannot: an ABSENT field — which is also where
	// a bounds breach lands — names no type, an explicitly EMPTY set says the
	// peer speaks the envelope and handles no type at all, and a non-empty
	// one means exactly those names. The first two name the same SET and are
	// kept apart because they are different statements about the peer. Fixed
	// for the lifetime of the session.
	DeclaredDTypes domain.DeclaredDTypeSet
}

// Clone returns a deep copy. Both getters and setters go through it so that
// no caller can reach into NetCore-owned storage through a retained slice.
func (d HandshakeDeclarations) Clone() HandshakeDeclarations {
	return HandshakeDeclarations{
		AdvertisedNames: cloneCapabilityNames(d.AdvertisedNames),
		DeclaredDTypes:  d.DeclaredDTypes.Clone(),
	}
}

// New creates a NetCore, applies opts, and starts the writer
// goroutine. The caller must eventually call Close() to release resources.
//
// Caps and Networks are cloned so the caller cannot mutate NetCore state
// through the original references.
func New(id ConnID, rawConn net.Conn, dir Direction, opts Options) *NetCore {
	metered, _ := rawConn.(*MeteredConn)

	// Resolve the remote address string once. Guard against a nil net.Addr
	// (some conn implementations return nil before the peer is known) so the
	// cached form degrades to "" instead of panicking — the previous lazy
	// pc.rawConn.RemoteAddr().String() would have panicked in that case.
	var remoteAddrStr string
	if addr := rawConn.RemoteAddr(); addr != nil {
		remoteAddrStr = addr.String()
	}

	pc := &NetCore{
		id:              id,
		direction:       dir,
		address:         opts.Address,
		identity:        opts.Identity,
		caps:            cloneCaps(opts.Caps),
		networks:        cloneNetworks(opts.Networks),
		lastActivity:    opts.LastActivity,
		protocolVersion: opts.ProtocolVersion,
		sendCh:          make(chan sendItem, sendChBuffer),
		closing:         make(chan struct{}),
		writerDone:      make(chan struct{}),
		writerExited:    make(chan struct{}),
		rawConn:         rawConn,
		remoteAddrStr:   remoteAddrStr,
		metered:         metered,
		connIDNum:       uint64(id),
		writeDeadline:   writeDeadlineFor(dir),
	}
	if opts.Declarations != nil {
		pc.declarations = opts.Declarations.Clone()
	}
	go pc.writerLoop()
	return pc
}

// NewBootstrap wraps a one-shot outbound dial (e.g., syncPeer and other
// bootstrap/probe paths that run before a session is established) in a
// NetCore so that every write on conn goes through the single-writer
// invariant instead of raw io.WriteString. The NetCore is never registered
// in the Service connection registry — its sole job is to serialise writes
// on conn while the caller continues to read directly from a bufio.Reader
// over the same conn. Caller owns lifecycle via Close(), which closes the
// underlying conn and waits for the writer goroutine to exit.
//
// writeDeadline is taken from the caller's outer overall-operation budget
// (e.g., syncHandshakeTimeout for syncPeer) rather than the generic
// Outbound default (sessionWriteTimeout), because the bootstrap wrapper
// must not relax the caller's existing timing contract: if the outer code
// guarantees "this whole handshake finishes in 1.5s" via SetDeadline, the
// per-write budget has to fit inside that window, not exceed it.
//
// The writeDeadline override after construction is safe because no sender
// can enqueue data until the caller observes the returned *NetCore, and the
// writer goroutine reads pc.writeDeadline only on dequeue — so the field
// write happens-before any read by Go's memory model via the channel send.
func NewBootstrap(conn net.Conn, writeDeadline time.Duration) *NetCore {
	pc := New(ConnID(0), conn, Outbound, Options{})
	pc.writeDeadline = writeDeadline
	return pc
}

// writeDeadlineFor returns the per-write socket deadline for a given
// connection direction. Outbound (dialled) sessions historically used a
// tighter 3s deadline to keep slow-peer eviction responsive; inbound
// (accepted) connections used 30s. Keeping these values split per direction
// preserves the pre-migration back-pressure behaviour when outbound writes
// move off raw io.WriteString onto NetCore's managed send path.
func writeDeadlineFor(dir Direction) time.Duration {
	if dir == Outbound {
		return sessionWriteTimeout
	}
	return connWriteTimeout
}

// writerLoop is the single goroutine that drains sendCh and writes to
// rawConn. It exits ONLY when Close() signals `closing`.
//
// Ownership protocol: the writer is the sole consumer of sendCh for the
// whole lifetime of the connection, including after a write failure. It
// used to return on the first failed write, which left whatever was still
// buffered — and everything a racing producer enqueued afterwards — sitting
// in a channel nobody would ever read again. Instead the writer switches to
// drain-only mode: the socket is closed, no further bytes are written, and
// every remaining and every later item is discarded by this loop. That way
// "the queue has exactly one consumer, always" is a property of the loop
// structure and not of a race between goroutines.
//
// On the closing branch both cases of the select may be ready at once, and
// which one wins is unobservable: the gate is already shut when `closing` is
// signalled, so an item taken from sendCh is discarded by exactly the same
// rule drainQueued applies to it.
func (pc *NetCore) writerLoop() {
	defer crashlog.DeferRecover()
	defer close(pc.writerExited)
	defer pc.signalWriterDone()

	for {
		select {
		case <-pc.closing:
			pc.drainQueued()
			return
		case item := <-pc.sendCh:
			// The gate is the single source of truth for "this queue is
			// finished": producers read it on both sides of their offer, the
			// loop reads it here.
			if pc.gateStatus() != SendOK {
				continue
			}
			if pc.writeItem(item) {
				continue
			}
			// Shut the door BEFORE draining: the drain frees queue slots, and
			// a producer that slipped in between would be told SendOK for a
			// frame this loop can now only discard. This ordering is also what
			// makes the producers' post-offer check sufficient — a producer
			// that still reads gateOpen is provably ahead of the drain that
			// follows. CompareAndSwap rather than Store so a concurrent
			// Close() is not downgraded to a socket failure.
			pc.gate.CompareAndSwap(int32(gateOpen), int32(gateSocketFailed))
			// Everything already queued behind the failed frame is released in
			// the same instant the link breaks, rather than held until Close().
			pc.drainQueued()
			pc.signalWriterDone()
		}
	}
}

// writeItem performs one socket write and reports whether the connection is
// still usable. A frame dropped on its own send deadline is NOT a socket
// failure: the link is fine, this particular frame simply became worthless
// while it waited, so the writer keeps going.
func (pc *NetCore) writeItem(item sendItem) bool {
	now := time.Now()
	if item.ticket.expiredAt(now) {
		return true
	}

	_ = pc.rawConn.SetWriteDeadline(item.ticket.writeDeadlineAt(now, pc.writeDeadline))
	_, err := pc.rawConn.Write(item.data)
	writeOK := err == nil

	// Capture tap: emit event after write attempt so the event
	// carries the correct outcome (plan §7.1). Non-blocking — the
	// sink must not stall the writer goroutine.
	if sink := pc.loadCaptureSink(); sink != nil {
		sink.OnSendAttempt(item.data, writeOK)
	}

	if !writeOK {
		// The write had already started, so an unknown prefix of the frame may
		// have reached the peer. A frame cut in the middle desyncs the line
		// protocol and there is nothing left to resynchronise, so the
		// connection is dead by definition. Closing the socket here makes that
		// explicit and unblocks the reader on this conn instead of leaving it
		// to wait out its own deadline.
		_ = pc.rawConn.Close()
		return false
	}

	if item.ack != nil {
		close(item.ack)
	}
	_ = pc.rawConn.SetWriteDeadline(time.Time{})
	return true
}

// drainQueued discards every item currently buffered in sendCh without writing
// it, so the frames behind a dead socket are released instead of being pinned
// by the channel until the connection is collected. Only the writer goroutine
// calls this, so it does not compete with anyone for the channel.
//
// The sweep is non-blocking, so it never waits for a producer that is still
// on its way to the queue. That producer is already refused: the gate is shut
// before any drain starts, and the frame it may still deposit is answered
// SendWriterDone / SendChanClosed and released with the buffer itself.
func (pc *NetCore) drainQueued() {
	for {
		select {
		case <-pc.sendCh:
		default:
			return
		}
	}
}

// signalWriterDone closes writerDone at most once. Both the write-failure
// path and the normal exit reach it, and a second close would panic.
func (pc *NetCore) signalWriterDone() {
	pc.writerDoneOne.Do(func() {
		close(pc.writerDone)
	})
}

// SendStatus describes why a send operation succeeded or failed.
// Callers use this to choose the correct recovery action: buffer-full
// is a backpressure signal whose handling is the caller's policy
// (best-effort fire-and-forget paths drop the frame and keep the
// session; control-plane paths may wait or escalate), timeout warrants
// closing the connection, while writerDone and channelClosed mean the
// connection is already dying and an extra Close() would interfere
// with orderly teardown.
//
// Zero value is an invalid sentinel — an uninitialised SendStatus
// cannot be confused with success.
type SendStatus int

const (
	SendStatusInvalid SendStatus = iota // zero value — must never appear in correct code
	SendOK                              // data accepted (and flushed, for sync path)
	SendBufferFull                      // send channel is full — peer too slow
	SendWriterDone                      // writer goroutine already exited
	SendTimeout                         // sync flush deadline expired
	SendChanClosed                      // Close() has shut the queue (conn shutting down)
	SendMarshalError                    // frame serialisation failed — caller's data is bad
	SendCtxCancelled                    // caller ctx cancelled or deadline exceeded mid-flight
)

// String returns a human-readable label for diagnostics and logging.
func (s SendStatus) String() string {
	switch s {
	case SendStatusInvalid:
		return "INVALID(zero)"
	case SendOK:
		return "ok"
	case SendBufferFull:
		return "buffer_full"
	case SendWriterDone:
		return "writer_done"
	case SendTimeout:
		return "timeout"
	case SendChanClosed:
		return "chan_closed"
	case SendMarshalError:
		return "marshal_error"
	case SendCtxCancelled:
		return "ctx_cancelled"
	default:
		return "unknown"
	}
}

// sendGate is the lifecycle state of the send queue, stored in NetCore.gate.
// It is monotonic: a queue that stopped accepting frames never reopens.
//
// The two shut states are kept apart because they are different statements
// about the connection, and the SendStatus each one produces is part of the
// public contract: a socket that died under the writer (SendWriterDone) versus
// an orderly teardown owning the socket (SendChanClosed), which the caller must
// not answer with another Close().
type sendGate int32

const (
	gateOpen sendGate = iota
	gateSocketFailed
	gateClosed
)

// gateStatus reports the answer the queue owes a producer: SendOK while it
// still accepts frames, and the reason it stopped otherwise. One reader for
// both sides of the offer and for the writer loop, so "shut" can never mean
// two different things in two places.
func (pc *NetCore) gateStatus() SendStatus {
	switch sendGate(pc.gate.Load()) {
	case gateSocketFailed:
		return SendWriterDone
	case gateClosed:
		return SendChanClosed
	default:
		return SendOK
	}
}

// teardownStatus answers a producer whose wait ended on writerDone, and it
// reads the answer off the GATE rather than off the channel that fired.
//
// writerDone is ONE channel for TWO facts: the writer closes it when a socket
// write failed under it AND when Close() told it to stop. The two owe the
// caller different answers — SendChanClosed says the owner already holds the
// teardown and must not be answered with another Close(), SendWriterDone says
// the link died on its own — so a wait that named the reason after its own
// channel reported the socket as dead for every orderly shutdown. The gate is
// raised BEFORE writerDone is signalled on both paths (Close() shuts it before
// it touches the socket; the writer shuts it before it drains), which is what
// makes it the more specific of the two and always already set here.
//
// The fallback covers the one state where the writer is gone with the gate
// still open: the writer goroutine panicked and its deferred signalWriterDone
// ran with nobody having shut the queue. The link is then dead with no owner
// holding a teardown, which is exactly what SendWriterDone says.
func (pc *NetCore) teardownStatus() SendStatus {
	if st := pc.gateStatus(); st != SendOK {
		return st
	}
	return SendWriterDone
}

// queueFrame is the enqueue half of every non-blocking send path: refuse at
// the door once the queue is shut, offer the frame without waiting, then read
// the door again. It is one helper because four of the five send entry points
// need exactly this protocol and the fifth differs only in how it waits for a
// slot (queueFrameBlocking) — a gate that some entry points implement
// differently is not a gate.
//
// A full queue is reported as SendBufferFull and is deliberately NOT a link
// failure: the frame never entered the queue, and what back-pressure means is
// the caller's policy.
func (pc *NetCore) queueFrame(item sendItem) SendStatus {
	if st := pc.gateStatus(); st != SendOK {
		return st
	}
	pc.runEnqueueBarrier()

	select {
	case pc.sendCh <- item:
	default:
		return SendBufferFull
	}
	return pc.settleEnqueuedFrame(item)
}

// queueFrameBlocking is the enqueue half of the control-plane path that waits
// for a slot instead of dropping the frame. It never reports SendBufferFull:
// teardown and the caller's own deadline are the only ways out of the wait.
//
// The teardown arm answers from teardownStatus rather than from the channel it
// woke on: a producer waiting for a slot when the OWNER closes the connection
// is owed SendChanClosed, and writerDone alone cannot tell that apart from a
// socket that died under the writer.
func (pc *NetCore) queueFrameBlocking(item sendItem, deadline <-chan time.Time) SendStatus {
	if st := pc.gateStatus(); st != SendOK {
		return st
	}
	pc.runEnqueueBarrier()

	select {
	case pc.sendCh <- item:
	case <-pc.writerDone:
		return pc.teardownStatus()
	case <-deadline:
		return SendTimeout
	}
	return pc.settleEnqueuedFrame(item)
}

// settleEnqueuedFrame decides the caller's answer for a frame that is already
// in the queue by reading the gate AFTER the channel send.
//
// The order is the whole point, and it closes the window rather than shrinking
// it. Go's sync/atomic operations are sequentially consistent, so this Load
// and the single raise of the gate sit in one total order, and the channel
// send is sequenced before this Load in the producer's own program order.
// That leaves two cases and no third:
//
//   - the Load reads a shut gate: the frame entered a queue that no longer
//     accepts anything, so the caller must not account the frame as accepted;
//   - the Load reads gateOpen: the raise has not happened yet, so the drain
//     that follows it starts after the item is in the buffer and will discard
//     it — and so will the writer's main loop, which stays the owner of sendCh
//     in drain-only mode until Close(). SendOK is honest.
//
// A frame that WAS written just before a LATER frame killed the link is the
// one case the gate alone cannot place. Where the item carries an `ack`, it
// does not have to: the writer closes `ack` after the successful write and
// before it ever raises the gate, so an already-closed `ack` is PROOF the
// bytes left, and that proof outranks the gate. Reading it here is what keeps
// the sync entry points from reporting a failure for a frame their caller
// watched succeed.
//
// Without such proof the answer stays the gate's refusal even though the bytes
// may have left. That direction is safe — the layer above re-picks a candidate
// for a frame that already arrived — while the opposite (SendOK for a frame
// nobody will ever write) makes a discarded frame look like a useful write in
// the caller's own accounting.
func (pc *NetCore) settleEnqueuedFrame(item sendItem) SendStatus {
	shut := pc.gateStatus()
	if shut == SendOK {
		return SendOK
	}
	return flushedOr(item.ack, shut)
}

// flushedOr answers SendOK for a frame whose `ack` is already closed and
// `failure` for one with no such proof. It is the single non-blocking read of
// the ack, shared by both ends of the send path — the enqueue side settling a
// frame against the gate and the wait side settling it against a failed arm —
// because "a closed ack outranks every other signal" is ONE rule, and a rule
// that lives in two copies is a rule one of them will eventually lose.
//
// A nil ack is a frame nobody asked to be proven: the receive can never become
// ready, so the answer is the failure, which is exactly what the async entry
// points owe their caller.
func flushedOr(ack <-chan struct{}, failure SendStatus) SendStatus {
	select {
	case <-ack:
		// The writer closes the ack only after the bytes left the process, so
		// this frame is on the wire whatever else went wrong afterwards.
		return SendOK
	default:
		return failure
	}
}

// awaitFlush is the wait half of every sync send path: the frame is already in
// the queue and the caller wants to know whether it reached the socket. It is
// one helper because the three sync entry points differ only in which
// cancellation sources they have — `cancel` is the caller's ctx.Done() or nil
// for the entry points that take no ctx, and a nil channel simply never fires.
//
// A closed `ack` is proof the bytes left, and it outranks every other arm: the
// writer closes it after the successful write and before it can raise the gate.
//
// That precedence CANNOT be expressed by the order of the cases. `select`
// picks uniformly at random among the arms that are ready at once, and all of
// them can be: the writer flushes the frame and the link dies behind it, the
// caller's ctx is cancelled in the same instant, the 5s flush deadline expires
// while the ack is already closed. Whoever reads the ack first therefore reads
// it AGAIN, without blocking, before returning any failure — the second read
// is what turns "the ack arm is listed first" into an actual rule.
//
// This is the direction of imprecision the layer must never have: a frame the
// caller watched succeed, reported as SendWriterDone / SendTimeout /
// SendCtxCancelled, is a write counted as a loss by everything above.
//
// The teardown arm answers from teardownStatus for the same reason
// queueFrameBlocking does: writerDone closes on BOTH death paths, so the frame
// of a caller whose own Close() tore the connection down used to come back as
// SendWriterDone — an invitation to close a connection that is already being
// closed.
func (pc *NetCore) awaitFlush(ack <-chan struct{}, cancel <-chan struct{}, deadline <-chan time.Time) SendStatus {
	select {
	case <-ack:
		return SendOK
	case <-pc.writerDone:
		return flushedOr(ack, pc.teardownStatus())
	case <-cancel:
		return flushedOr(ack, SendCtxCancelled)
	case <-deadline:
		return flushedOr(ack, SendTimeout)
	}
}

// runEnqueueBarrier fires the test-only synchronisation point described on
// the enqueueBarrier field.
func (pc *NetCore) runEnqueueBarrier() {
	if pc.enqueueBarrier != nil {
		pc.enqueueBarrier()
	}
}

// Send enqueues a protocol frame for writing. Non-blocking — returns SendOK
// on success, SendBufferFull if the write queue is full, SendWriterDone once
// the socket has failed, SendChanClosed if the connection is shutting down,
// or SendMarshalError if the frame cannot be serialised.
func (pc *NetCore) Send(frame protocol.Frame) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.SendRaw([]byte(line))
}

// SendRaw enqueues pre-serialized bytes for writing. Non-blocking.
//
// Once the socket has failed the queue is shut: the writer is still draining
// it to finalise what is left, so a slot being free says nothing about a new
// frame's chances. Reporting SendWriterDone instead of SendOK is what keeps
// the caller's "accepted" accounting (markPeerWrite and friends) honest —
// including for a frame that reached the queue while the writer was raising
// the gate, which is why queueFrame checks it on both sides of the offer.
func (pc *NetCore) SendRaw(data []byte) SendStatus {
	return pc.queueFrame(sendItem{data: data})
}

// SendTracked is the metadata-carrying twin of Send: the frame is enqueued
// with a WriteTicket, so it is subject to the ticket's send deadline and
// write grace instead of the connection's default write deadline.
//
// A nil ticket makes this behave exactly like Send.
func (pc *NetCore) SendTracked(frame protocol.Frame, ticket *WriteTicket) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.SendRawTracked([]byte(line), ticket)
}

// SendRawTracked enqueues pre-serialized bytes together with their outbound
// contract. Non-blocking, like SendRaw.
//
// The STATUS is the whole answer, and it answers about ADMISSION rather than
// about the wire. There is nothing else to report, because the ticket travels
// one way: it carries the frame's timing down to the writer and brings nothing
// back.
//
// What each answer proves, stated exactly, because the difference is what a
// caller's fallback policy is built on:
//
//   - SendOK — the frame is in a queue that was still live. That is not a
//     write: the writer may still drop it on its own SendUntil, and the link
//     may break before its turn comes.
//   - a refusal AT THE DOOR — SendMarshalError, SendBufferFull, or the gate's
//     SendWriterDone / SendChanClosed read before the offer — proves the frame
//     never entered the queue and therefore never reached the socket. This
//     half is exact, and it is the one the retry paths depend on.
//   - a refusal AFTER the offer — the gate read by settleEnqueuedFrame —
//     proves only that the frame will not be written FROM THE QUEUE. An async
//     frame carries no ack, so the one case the gate cannot place stays
//     unresolved: this frame may have been written already and a LATER frame
//     killed the link. The status then reports a loss for bytes that left.
//
// The imprecision is one-directional by design, and the direction is the safe
// one: over-reporting a possible loss makes the layer above re-pick a route for
// a frame that already arrived, which the receiving side deduplicates, while
// under-reporting one would make a discarded frame look like a useful write in
// the caller's own accounting. The sync entry points close the gap for the
// frames that need it — there a closed ack is proof the bytes left and it
// outranks every other signal (see awaitFlush).
func (pc *NetCore) SendRawTracked(data []byte, ticket *WriteTicket) SendStatus {
	return pc.queueFrame(sendItem{data: data, ticket: ticket})
}

// SendTrackedObserved is SendTracked with a WITNESS: the caller supplies a
// channel the writer closes once this frame's bytes have left the process.
//
// It exists for one caller and one question. The liveness probe turns silence
// into evidence about another person, and its specification is explicit that a
// probe which never reached the network is not evidence at all — so it has to
// tell "they did not answer" from "we never managed to ask". Every step below
// can swallow a frame silently: a class queue drops it on its send deadline, a
// session queue is discarded when the session closes, writeItem skips an
// expired ticket, and drainQueued throws away everything behind a broken link.
// NONE of those closes the ack, which is what makes an unclosed ack the honest
// answer to the question.
//
// This is NOT the terminal machinery write_ticket.go describes as removed. That
// was an observer attached to the TICKET, which is shared across a candidate
// walk and therefore needed a once-guard and a burn-on-refusal rule; this is
// the ack channel the sync entry points have always used, handed over instead
// of waited on. The ticket still carries nothing back.
//
// The caller must never close the channel itself: closing it is the writer's
// statement that the bytes left, and a caller closing it would be asserting a
// write that did not happen. A nil ack makes this identical to SendTracked.
func (pc *NetCore) SendTrackedObserved(frame protocol.Frame, ticket *WriteTicket, ack chan struct{}) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.queueFrame(sendItem{data: []byte(line), ack: ack, ticket: ticket})
}

// SendSync enqueues a frame and blocks until the writer goroutine flushes
// it to the socket.
func (pc *NetCore) SendSync(frame protocol.Frame) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.SendRawSync([]byte(line))
}

// SendRawSync enqueues pre-serialized bytes and waits for write completion.
//
// Fast-fail on full queue: returns SendBufferFull immediately if the write
// channel has no slot. This preserves the pre-PR2 contract relied on by
// inbound error paths (writeJSONFrameSync / enqueueFrameSync), where a
// saturated queue means the peer is unresponsive and must be evicted
// rather than kept alive while the caller blocks. Outbound control-plane
// writes that must not be starved by fire-and-forget traffic use
// SendRawSyncBlocking instead.
func (pc *NetCore) SendRawSync(data []byte) SendStatus {
	ack := make(chan struct{})

	if st := pc.queueFrame(sendItem{data: data, ack: ack}); st != SendOK {
		return st
	}
	return pc.awaitFlush(ack, nil, time.After(syncFlushTimeout))
}

// SendRawSyncCtx is the ctx-aware twin of SendRawSync. It preserves the
// fast-fail-on-full-queue contract (SendBufferFull on saturated sendCh) and
// additionally honours caller ctx cancellation while waiting for the writer
// goroutine to flush. This is the only sync entry point that lets a
// request-scoped timeout interrupt the 5s syncFlushTimeout wait — direct
// callers of SendRawSync wait out the full deadline regardless of ctx.
//
// Ctx is checked both before enqueue (pre-cancelled fast-fail, no sendCh
// slot consumed) and during the flush wait (mid-flight cancel abort). On
// ctx cancellation during the wait, the ack slot is abandoned; the writer
// goroutine will still eventually consume the already-enqueued frame and
// close the ack channel, but no one is listening — this is benign because
// sendItem owns the ack chan and nothing else references it.
//
// Return value is SendStatus only; ctx.Err() is surfaced through
// SendCtxCancelled. The caller (network_bridge.SendFrameSync) preserves
// the original ctx error (context.Canceled vs context.DeadlineExceeded)
// by intercepting this status and returning ctx.Err() directly before the
// SendStatusToError mapping.
func (pc *NetCore) SendRawSyncCtx(ctx context.Context, data []byte) SendStatus {
	if err := ctx.Err(); err != nil {
		return SendCtxCancelled
	}

	ack := make(chan struct{})

	if st := pc.queueFrame(sendItem{data: data, ack: ack}); st != SendOK {
		return st
	}
	return pc.awaitFlush(ack, ctx.Done(), time.After(syncFlushTimeout))
}

// SendRawSyncBlocking enqueues pre-serialized bytes and waits for write
// completion, BLOCKING on enqueue until a slot is available. Used by
// outbound control-plane writes (handshake, heartbeat,
// request-reply) that must not be starved by fire-and-forget relay traffic
// already queued on sendCh. The entire operation (enqueue + flush) is
// bounded by syncFlushTimeout so a stuck writer cannot hang the caller
// indefinitely, and writerDone unblocks immediately if the connection is
// being torn down.
//
// This method never returns SendBufferFull — backpressure from a saturated
// queue is reserved for the fire-and-forget Send / SendRaw path, which
// uses SendBufferFull as the slow-peer eviction signal.
func (pc *NetCore) SendRawSyncBlocking(data []byte) SendStatus {
	ack := make(chan struct{})

	// Bound the entire operation (enqueue + flush) by a single deadline so
	// a stuck writer can never hang the caller longer than syncFlushTimeout.
	// The timer is armed before the socket-failure check inside
	// queueFrameBlocking so that both halves of the operation share one
	// budget; on the already-dead connection it costs one timer that is
	// stopped on the way out.
	deadline := time.NewTimer(syncFlushTimeout)
	defer deadline.Stop()

	if st := pc.queueFrameBlocking(sendItem{data: data, ack: ack}, deadline.C); st != SendOK {
		return st
	}
	return pc.awaitFlush(ack, nil, deadline.C)
}

// WriterDone returns a channel that is closed when the per-connection
// writer goroutine exits. Callers use this to react to local teardown
// (socket closed, Close() called) independently of Send*() return values —
// e.g. servePeerSession breaks out of its read loop as soon as the writer
// dies, instead of waiting for the next heartbeat to notice.
func (pc *NetCore) WriterDone() <-chan struct{} {
	return pc.writerDone
}

// HasCapability returns true if the peer negotiated the given capability.
func (pc *NetCore) HasCapability(cap domain.Capability) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return hasCapability(pc.caps, cap)
}

// hasCapability is the package-local copy of the slice-contains helper.
// The node package keeps its own identical copy (file_integration.go) —
// duplicating the four-line helper is cheaper than pulling it across the
// package boundary through an exported symbol or shared dependency.
func hasCapability(caps []domain.Capability, target domain.Capability) bool {
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

// Identity returns the peer's Ed25519 fingerprint. Empty before handshake.
func (pc *NetCore) Identity() domain.PeerIdentity {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.identity
}

// SetIdentity is called once during handshake when the peer's identity
// is established.
func (pc *NetCore) SetIdentity(id domain.PeerIdentity) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.identity = id
}

// SetCapabilities records the negotiated capability set.
// The slice is cloned — the caller retains no write path into NetCore state.
func (pc *NetCore) SetCapabilities(caps []domain.Capability) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.caps = cloneCaps(caps)
}

// SetNetworks records the peer's reachable network groups.
// The map is cloned — the caller retains no write path into NetCore state.
func (pc *NetCore) SetNetworks(nets map[domain.NetGroup]struct{}) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.networks = cloneNetworks(nets)
}

// ProtocolVersion returns the negotiated protocol version reported by
// the peer during handshake. The source frame depends on direction:
// hello.Version for inbound conns (populated by rememberConnPeerAddr),
// welcome.Version for outbound sessions (populated by
// applyWelcomeMetadata). See the protocolVersion field comment for the
// full handshake mapping.
//
// Returns zero before the relevant handshake frame has been processed.
// Callers that need to distinguish "not yet handshaken" from "peer
// reports version 0" must consult Identity() / Address() first — those
// are populated by the same handshake frame.
func (pc *NetCore) ProtocolVersion() domain.ProtocolVersion {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.protocolVersion
}

// SetProtocolVersion records the negotiated protocol version once the
// handshake frame carrying it has been parsed (hello on inbound,
// welcome on outbound). Idempotent in practice: peers do not
// renegotiate their protocol version mid-session, so a second call
// with the same value is a no-op.
func (pc *NetCore) SetProtocolVersion(v domain.ProtocolVersion) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.protocolVersion = v
}

// Address returns the overlay address declared during the hello handshake.
// Empty before the hello frame is processed.
func (pc *NetCore) Address() domain.PeerAddress {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.address
}

// SetAddress records the peer's overlay address during hello processing.
func (pc *NetCore) SetAddress(addr domain.PeerAddress) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.address = addr
}

// ConnIDNum returns the monotonic connection identifier as a plain uint64,
// suitable for diagnostics and logging. Immutable — no lock needed.
func (pc *NetCore) ConnIDNum() uint64 {
	return pc.connIDNum
}

// ConnID returns the typed connection identifier used as primary key in
// the Service connection registry. Immutable — no lock needed.
func (pc *NetCore) ConnID() ConnID {
	return ConnID(pc.connIDNum)
}

// Conn returns the underlying net.Conn the NetCore owns. Used by iteration
// helpers in the node-level connection registry that need to surface the
// raw conn handle in their callbacks after the registry was rekeyed from
// net.Conn to ConnID in PR 9.7. Immutable — rawConn is set once in New()
// and never reassigned.
func (pc *NetCore) Conn() net.Conn {
	return pc.rawConn
}

// Auth returns the connection's auth state, or nil for unauthenticated
// connections. The returned pointer is a snapshot — connauth.State is
// never mutated in place after creation, so the caller can safely read
// its fields without holding any lock.
func (pc *NetCore) Auth() *connauth.State {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.auth
}

// SetAuth stores the auth state for this connection.
func (pc *NetCore) SetAuth(state *connauth.State) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = state
}

// ClearAuth removes the auth state from this connection.
func (pc *NetCore) ClearAuth() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = nil
}

// LastActivity returns the timestamp of the last received frame.
func (pc *NetCore) LastActivity() time.Time {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.lastActivity
}

// SetLastActivity sets the last-activity timestamp explicitly (e.g. during
// hello processing when UTC is required).
func (pc *NetCore) SetLastActivity(t time.Time) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastActivity = t
}

// Networks returns a snapshot copy of the peer's reachable network groups.
// The returned map is safe to iterate and retain after the call returns.
func (pc *NetCore) Networks() map[domain.NetGroup]struct{} {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneNetworks(pc.networks)
}

// Capabilities returns a snapshot copy of the negotiated capability set.
// The returned slice is safe to iterate and retain after the call returns.
func (pc *NetCore) Capabilities() []domain.Capability {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneCaps(pc.caps)
}

// CapabilitiesRef returns the negotiated capability set WITHOUT copying. The
// returned slice aliases NetCore-owned storage and MUST be treated as
// read-only — callers must not append, sort, or index-assign it. This is safe
// because domain.Capability is an immutable string and pc.caps is only ever
// REPLACED wholesale (a cloneCaps assignment under pc.mu.Lock), never mutated
// in place, so a reader holding the reference sees a stable immutable snapshot
// even across a concurrent ApplyOpts/SetCaps replace (the old backing array is
// untouched). Use Capabilities() instead when an owned, mutable copy is needed.
//
// Added for the snapshotEntryLocked hot path (forEachInboundConnLocked / gossip
// fan-out), where the per-entry cloneCaps copy was a top alloc_space source and
// every consumer of the resulting connInfo.capabilities is read-only
// (capsContain / range); the one consumer that needs its own buffer
// (dm_router) already makes a defensive copy.
func (pc *NetCore) CapabilitiesRef() []domain.Capability {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.caps
}

// ApplyOpts overwrites NetCore state from an opts struct. This is the
// post-handshake counterpart of newNetCore's opts: the NetCore is created
// at accept time with empty opts (identity unknown yet), and ApplyOpts fills
// in the peer state once the hello frame arrives.
//
// Only non-zero fields in opts are applied — zero values are skipped so that
// a partial update (e.g. only Caps) does not blank out existing fields.
func (pc *NetCore) ApplyOpts(opts Options) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if opts.Address != "" {
		pc.address = opts.Address
	}
	if !opts.Identity.IsZero() {
		pc.identity = opts.Identity
	}
	if opts.Caps != nil {
		pc.caps = cloneCaps(opts.Caps)
	}
	if opts.Networks != nil {
		pc.networks = cloneNetworks(opts.Networks)
	}
	if !opts.LastActivity.IsZero() {
		pc.lastActivity = opts.LastActivity
	}
	if opts.ProtocolVersion != 0 {
		pc.protocolVersion = opts.ProtocolVersion
	}
	if opts.Declarations != nil {
		pc.declarations = opts.Declarations.Clone()
	}
}

// Declarations returns a copy of the peer's raw handshake self-description.
// The zero value is the honest answer for a connection whose handshake has
// not been folded in yet: an empty raw capability set and an absent dtypes
// field, which declares no type.
func (pc *NetCore) Declarations() HandshakeDeclarations {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.declarations.Clone()
}

// SetDeclarations records the peer's raw handshake self-description. The
// argument is cloned — the caller retains no write path into NetCore state.
func (pc *NetCore) SetDeclarations(declarations HandshakeDeclarations) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.declarations = declarations.Clone()
}

// cloneCapabilityNames returns a shallow copy of the raw capability-name
// slice, or nil.
func cloneCapabilityNames(src []domain.CapabilityName) []domain.CapabilityName {
	if src == nil {
		return nil
	}
	out := make([]domain.CapabilityName, len(src))
	copy(out, src)
	return out
}

// cloneCaps returns a shallow copy of the capability slice, or nil.
func cloneCaps(src []domain.Capability) []domain.Capability {
	if src == nil {
		return nil
	}
	out := make([]domain.Capability, len(src))
	copy(out, src)
	return out
}

// cloneNetworks returns a shallow copy of the network-group set, or nil.
func cloneNetworks(src map[domain.NetGroup]struct{}) map[domain.NetGroup]struct{} {
	if src == nil {
		return nil
	}
	out := make(map[domain.NetGroup]struct{}, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// RemoteAddr returns the remote address string (for logging only).
// The value is cached at construction (remoteAddrStr) because the remote
// address of an established connection is immutable and net.Addr.String()
// allocates on every call. No lock required — the field is write-once.
func (pc *NetCore) RemoteAddr() string {
	return pc.remoteAddrStr
}

// Dir returns the connection direction.
func (pc *NetCore) Dir() Direction {
	return pc.direction
}

// IsLocal reports whether the connection originates from a loopback address.
// Immutable after registration — no lock required.
func (pc *NetCore) IsLocal() bool {
	return pc.isLocal
}

// SetLocal marks the connection as local (loopback). Called once during
// registration; must not be changed afterwards.
func (pc *NetCore) SetLocal(v bool) {
	pc.isLocal = v
}

// TouchActivity updates the last-activity timestamp to time.Now().
//
// NOTE: uses time.Now() directly — the node package does not yet have a
// clock abstraction. Introducing one here alone would be inconsistent with
// the 40+ other time.Now() call sites in the package. Tracked as tech debt
// for a dedicated clock-migration task.
func (pc *NetCore) TouchActivity() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastActivity = time.Now()
}

// Close shuts down the connection. Order matters:
//  1. Shut the gate — from this instant every producer is answered
//     SendChanClosed, including one that had already passed the door and is
//     about to deposit its frame (settleEnqueuedFrame reads the gate again
//     after the offer). Doing this FIRST is what makes the steps below safe
//     to run while producers are still in flight.
//  2. Close TCP — unblocks a writer goroutine stuck in conn.Write with a 30s
//     deadline. Without this step the wait in step 4 would inherit that
//     deadline.
//  3. Signal `closing` — tells the writer to release what is buffered and
//     return. sendCh itself is NOT closed: it has many producers on arbitrary
//     goroutines, none of which can prove it is the last one, and a producer
//     racing the close panics on send. This mirrors the fence the upper queue
//     already uses (docs/locking.md, "peerSession.sendMu"). A frame a racing
//     producer still deposits is answered by the gate and released together
//     with the buffer when the NetCore is collected.
//  4. Wait for writerExited — ensures every buffered frame has been finalised
//     before the method returns. writerDone is deliberately NOT the wait
//     target: it fires as soon as the writer stops WRITING, which on the
//     failure path happens while the writer is still the owner of sendCh.
//
// Idempotent — safe to call multiple times.
func (pc *NetCore) Close() {
	pc.closeOnce.Do(func() {
		pc.gate.Store(int32(gateClosed))
		_ = pc.rawConn.Close()
		close(pc.closing)
		<-pc.writerExited
	})
}

// ShutSendQueue stops the queue accepting new frames and does nothing else: the
// socket stays open, the writer keeps draining what is already buffered, and no
// caller is woken.
//
// It exists for an owner that has to PUBLISH the connection's death before it
// can run the full Close — the node's inbound teardown resolves identity and
// health bookkeeping off a registry entry that Close's caller removes, so the
// announcement necessarily comes first. Without this the announcement was the
// FIRST of the two facts a producer can read: a sender admitted by a
// higher-level liveness check that had not yet seen the disconnect deposited
// its frame into a queue this teardown then discarded, and read SendOK — the
// one answer the gate exists to prevent (see the `gate` field and
// settleEnqueuedFrame). Shutting the door first makes the pair honest in either
// interleaving: a producer past the door was there before anyone was told, and
// every producer after the announcement is refused.
//
// The raise is a CAS from gateOpen so a writer that already named the socket as
// failed keeps its reason: this method only says "stopped accepting", never why
// the link died. Monotonic and idempotent, like every other raise of the gate,
// and Close() remains the only thing that owns the transport.
func (pc *NetCore) ShutSendQueue() {
	pc.gate.CompareAndSwap(int32(gateOpen), int32(gateClosed))
}

// Metered returns the MeteredConn wrapper, or nil if not metered.
func (pc *NetCore) Metered() *MeteredConn {
	return pc.metered
}
