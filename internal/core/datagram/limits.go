package datagram

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// limits.go is the ONE place where the numbers of §5 live, and the one place
// where they are justified. §11.3 leaves them to the implementation, which
// means the values below are starting points chosen from what the layer's own
// geometry allows — frame sizes, class caps, the 240 s reverse window, the
// five-minute replay window — and NOT from telemetry that does not exist yet.
// Every field says what it is derived from, so retuning one number is an
// argument about that derivation instead of a guess.
//
// The four groups map one-to-one onto the four components of §5:
//
//   - Peer     → admission.go: the two-stage per-neighbour budget;
//   - Queue    → queue.go: the weighted class queue;
//   - Reverse  → reverse_state.go: the caps of the request/response table;
//   - Replay   → replay_base.go: the bound of the anti-replay cache.
//
// Reference: docs/refactoring/datagram-transport.md §4.2, §5, §11.3.

// Limits is the complete §5 configuration. It is a value, copied into the
// components that need it: a shared mutable knob object would let one
// component's retuning silently change another's behaviour mid-flight.
type Limits struct {
	// Peer is the per-neighbour admission budget (§4.1 steps 1 and 8).
	Peer PeerBudget
	// Queue is the weighted class queue of §5.
	Queue QueueCaps
	// Reverse is the reverse-state table cap set of §4.2 / §5.
	Reverse ReverseCaps
	// Replay is the bound of the base anti-replay cache.
	Replay ReplayCaps
}

// PeerBudget is the two-stage admission budget of one AUTHENTICATED
// neighbour (§5). The identity is the session's, never `src` from the
// header: `src` exists only in routed frames and only becomes meaningful
// AFTER the signature check, which is itself paid for out of this budget.
//
// The three dimensions are ONE budget with three units, not three budgets to
// pick from: every inbound frame charges bytes and frames regardless of its
// class, and every signature check charges a verification on top. A neighbour
// cannot widen any of them by choosing a class — that is the whole point of
// the rule "the budget is per neighbour, classes divide it" (§5).
type PeerBudget struct {
	// BytesPerSecond is the sustained inbound byte rate of one neighbour,
	// counted on the SERIALIZED frame including base64 and the auth block
	// (§5). One legitimate bulk frame is at most
	// DatagramBulkPayloadCap (64 KiB) of payload ≈ 88 KiB on the wire, so
	// 1 MiB/s is roughly twelve full bulk frames per second — comfortably
	// above a single file transfer's chunk rate and far below what a link
	// can carry, because this is a cap on ONE neighbour, and a node with
	// twenty of them would still admit 20 MiB/s in aggregate.
	BytesPerSecond int
	// ByteBurst is the bucket depth in bytes: four seconds of the sustained
	// rate. Deep enough to absorb a chunk burst that arrives back-to-back
	// after a stalled link recovers, shallow enough that a neighbour cannot
	// bank a minute of silence into a multi-megabyte spike.
	ByteBurst int

	// FramesPerSecond is the sustained inbound frame rate of one neighbour.
	// The legacy command limiter (node.cmdRefillRate) allows 30 frames/s per
	// connection with a burst of 100; the datagram plane carries control
	// exchanges AND bulk chunks over the same session, so it gets twice the
	// sustained legacy rate.
	FramesPerSecond int
	// FrameBurst is the frame-bucket depth: four seconds of the sustained
	// rate, the same horizon as ByteBurst so the two dimensions cannot
	// disagree about how long a burst may last.
	FrameBurst int

	// VerifiesPerSecond is the sustained Ed25519 verification rate of one
	// neighbour, charged immediately before the check (§4.1 step 8). One
	// verification costs ~50 µs, so 32/s is ~0.2% of one core per
	// neighbour and ~4% across twenty of them: a hostile peer cannot make
	// signature checking the node's dominant cost, and an honest one never
	// approaches it, because routed frames are the minority of a session's
	// traffic and each one is checked once.
	VerifiesPerSecond int
	// VerifyBurst is the verification-bucket depth. It is deliberately HALF
	// the frame burst, which is what makes the crypto budget — not the byte
	// or frame budget — the binding constraint on a flood of small SIGNED
	// frames (§9): a stream of minimum-size routed frames runs out of
	// verifications long before it runs out of either of the other two.
	VerifyBurst int

	// TrackedPeers bounds the number of per-neighbour buckets kept in
	// memory. Sessions are already bounded by the node's own peer limits,
	// so this is a backstop against a leak, not a policy: 4096 buckets of
	// ~80 bytes is ~320 KiB.
	TrackedPeers int
	// IdleRetention is how long a silent neighbour's bucket is kept. Every
	// bucket refills completely within ByteBurst/BytesPerSecond = 4 s, so
	// after this long a bucket is indistinguishable from a fresh one and
	// dropping it forgives nothing.
	IdleRetention time.Duration
}

// QueueCaps configures the weighted class queue of §5.
//
// The weights are the normative part: control is served BEFORE bulk within
// its own share, and bulk keeps a guaranteed minimum share of the byte budget
// so a permanent control stream — which is cheap to produce — cannot stop a
// file transfer entirely.
type QueueCaps struct {
	// ControlWeight and BulkWeight are the byte shares of one scheduling
	// round. 3:1 is the starting split of §5: bulk is guaranteed one
	// quarter of the dispatched bytes under saturation of both classes.
	ControlWeight int
	// BulkWeight is bulk's share of a round; see ControlWeight.
	BulkWeight int
	// QuantumBytes is one weight unit, in bytes. It does NOT change the
	// long-run share — that is fixed by the weights — only the burstiness:
	// bulk's quantum (1 × 16 KiB) is a quarter of one maximum bulk frame,
	// so a full-size bulk frame leaves after four rounds while control
	// never waits behind more than ControlWeight × QuantumBytes = 48 KiB
	// of its own traffic.
	QuantumBytes int

	// ControlFrames and ControlBytes bound the control lane. A control
	// frame is at most DatagramControlPayloadCap (4 KiB) of payload
	// ≈ 5.6 KiB on the wire, so 256 frames is ~1.4 MiB worst case and the
	// byte cap binds first at 2 MiB.
	//
	// The depth is a MEMORY bound, not a latency one: latency is already
	// bounded by the send deadline, which is queue_residence of the class
	// (5 s for control, 30 s for bulk, §4.2). A queue deeper than what the
	// link drains inside that window only holds frames that will be dropped
	// on their deadline anyway — which the queue does itself, rather than
	// handing the writer a frame it would refuse.
	ControlFrames int
	// ControlBytes bounds the control lane in bytes; see ControlFrames.
	ControlBytes int
	// BulkFrames and BulkBytes bound the bulk lane. 64 full-size bulk
	// frames is ~5.6 MiB, so the frame cap binds first and the byte cap is
	// the memory backstop. Both lanes together are bounded by ~7.6 MiB,
	// which is the same order as the single 8 MiB frame line the legacy
	// peer-session reader already tolerates.
	BulkFrames int
	// BulkBytes bounds the bulk lane in bytes; see BulkFrames.
	BulkBytes int
}

// ReverseCaps configures the reverse-state table (§4.2, §5).
type ReverseCaps struct {
	// GlobalCap is the total number of live reverse records. A record is
	// ~200 bytes, so 4096 of them is ~800 KiB, and with PerUpstreamCap = 64
	// it takes 64 distinct neighbours to fill — a node with more neighbours
	// than that is already past the point where reverse state is its
	// scarcest resource.
	GlobalCap int
	// PerUpstreamCap is the cap of one upstream. A record lives
	// ReverseStateTTL (240 s), so 64 slots means one neighbour may have 64
	// request round-trips in flight through this node at once. The node's
	// relay subsystem allows 500 per peer over a 180 s window; the
	// datagram reverse table is stricter because its plane is
	// unauthenticated by construction (§2.1) and a slot here also carries a
	// probe budget.
	PerUpstreamCap int
	// ProbeBudget is how many REFUSED answers one record tolerates before
	// it stops looking at answers at all (§4.2). Four lets a genuine answer
	// survive three forged ones racing it, and bounds what a hostile
	// downstream can make the node spend per outstanding request.
	ProbeBudget int
}

// ReplayCaps bounds the base anti-replay cache (§5).
type ReplayCaps struct {
	// Capacity is the live entry count of the base cache. It is
	// deliberately SMALLER than what one neighbour could produce inside the
	// five-minute base window — at FramesPerSecond = 64 that is 19 200
	// frames — because the overflow rule is what protects the cache, not
	// its size: at capacity a routed frame from the NOISIEST neighbour is
	// refused while a quiet neighbour is still admitted by evicting one of
	// the noisy one's records (§5, replay_base.go). Sizing the cache for
	// the worst case would trade ~2 MiB of memory for nothing, since the
	// rule would still be the thing doing the work.
	Capacity int
}

const (
	// defaultPeerBytesPerSecond … defaultPeerIdleRetention: see the field
	// comments on PeerBudget for the derivation of each value.
	defaultPeerBytesPerSecond = 1 << 20
	defaultPeerByteBurst      = 4 << 20
	defaultPeerFramesPerSec   = 64
	defaultPeerFrameBurst     = 256
	defaultPeerVerifiesPerSec = 32
	defaultPeerVerifyBurst    = 128
	defaultTrackedPeers       = 4096
	defaultPeerIdleRetention  = time.Minute

	// defaultControlWeight … defaultBulkQueueBytes: see QueueCaps.
	defaultControlWeight     = 3
	defaultBulkWeight        = 1
	defaultQueueQuantumBytes = 16 << 10
	defaultControlQueueDepth = 256
	defaultControlQueueBytes = 2 << 20
	defaultBulkQueueDepth    = 64
	defaultBulkQueueBytes    = 8 << 20

	// defaultReverseProbes mirrors DefaultReverseProbeBudget; see
	// ReverseCaps.ProbeBudget.
	defaultReverseProbes = DefaultReverseProbeBudget
)

// DefaultLimits returns the starting values of §5. They are a function, not a
// package-level variable, so no caller can mutate the defaults for everybody
// else (CLAUDE.md: no global singletons).
func DefaultLimits() Limits {
	return Limits{
		Peer: PeerBudget{
			BytesPerSecond:    defaultPeerBytesPerSecond,
			ByteBurst:         defaultPeerByteBurst,
			FramesPerSecond:   defaultPeerFramesPerSec,
			FrameBurst:        defaultPeerFrameBurst,
			VerifiesPerSecond: defaultPeerVerifiesPerSec,
			VerifyBurst:       defaultPeerVerifyBurst,
			TrackedPeers:      defaultTrackedPeers,
			IdleRetention:     defaultPeerIdleRetention,
		},
		Queue: QueueCaps{
			ControlWeight: defaultControlWeight,
			BulkWeight:    defaultBulkWeight,
			QuantumBytes:  defaultQueueQuantumBytes,
			ControlFrames: defaultControlQueueDepth,
			ControlBytes:  defaultControlQueueBytes,
			BulkFrames:    defaultBulkQueueDepth,
			BulkBytes:     defaultBulkQueueBytes,
		},
		Reverse: ReverseCaps{
			GlobalCap:      defaultReverseGlobalCap,
			PerUpstreamCap: defaultReversePerUpstreamCap,
			ProbeBudget:    defaultReverseProbes,
		},
		Replay: ReplayCaps{Capacity: DefaultBaseReplayCapacity},
	}
}

// Normalized fills every non-positive field from DefaultLimits. Zero is not a
// business signal here: a zero byte budget would mean "admit nothing" and a
// zero cap would mean "store nothing", and both are configuration mistakes
// rather than intentions (CLAUDE.md: zero values are not implicit signals).
func (l Limits) Normalized() Limits {
	defaults := DefaultLimits()
	l.Peer = l.Peer.normalized(defaults.Peer)
	l.Queue = l.Queue.normalized(defaults.Queue)
	l.Reverse = l.Reverse.normalized(defaults.Reverse)
	if l.Replay.Capacity <= 0 {
		l.Replay.Capacity = defaults.Replay.Capacity
	}
	return l
}

func (b PeerBudget) normalized(defaults PeerBudget) PeerBudget {
	b.BytesPerSecond = positiveOr(b.BytesPerSecond, defaults.BytesPerSecond)
	b.ByteBurst = positiveOr(b.ByteBurst, defaults.ByteBurst)
	b.FramesPerSecond = positiveOr(b.FramesPerSecond, defaults.FramesPerSecond)
	b.FrameBurst = positiveOr(b.FrameBurst, defaults.FrameBurst)
	b.VerifiesPerSecond = positiveOr(b.VerifiesPerSecond, defaults.VerifiesPerSecond)
	b.VerifyBurst = positiveOr(b.VerifyBurst, defaults.VerifyBurst)
	b.TrackedPeers = positiveOr(b.TrackedPeers, defaults.TrackedPeers)
	if b.IdleRetention <= 0 {
		b.IdleRetention = defaults.IdleRetention
	}
	return b
}

func (c QueueCaps) normalized(defaults QueueCaps) QueueCaps {
	c.ControlWeight = positiveOr(c.ControlWeight, defaults.ControlWeight)
	c.BulkWeight = positiveOr(c.BulkWeight, defaults.BulkWeight)
	c.QuantumBytes = positiveOr(c.QuantumBytes, defaults.QuantumBytes)
	c.ControlFrames = positiveOr(c.ControlFrames, defaults.ControlFrames)
	c.ControlBytes = positiveOr(c.ControlBytes, defaults.ControlBytes)
	c.BulkFrames = positiveOr(c.BulkFrames, defaults.BulkFrames)
	c.BulkBytes = positiveOr(c.BulkBytes, defaults.BulkBytes)
	return c
}

func (c ReverseCaps) normalized(defaults ReverseCaps) ReverseCaps {
	c.GlobalCap = positiveOr(c.GlobalCap, defaults.GlobalCap)
	c.PerUpstreamCap = positiveOr(c.PerUpstreamCap, defaults.PerUpstreamCap)
	c.ProbeBudget = positiveOr(c.ProbeBudget, defaults.ProbeBudget)
	return c
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// ReverseStateCaps implements the reverseLimits seam of reverse_state.go: the
// whole view the table has of §5 is these two numbers.
func (l Limits) ReverseStateCaps() (global, perUpstream int) {
	caps := l.Reverse.normalized(DefaultLimits().Reverse)
	return caps.GlobalCap, caps.PerUpstreamCap
}

// ReverseTableConfig builds the table's config from the limits, so a caller
// cannot wire the caps and forget the probe budget — they are one decision
// (§4.2, §5).
func (l Limits) ReverseTableConfig(clock func() time.Time, metrics reverseMetrics) ReverseTableConfig {
	normalized := l.Normalized()
	return ReverseTableConfig{
		Clock:   clock,
		Limits:  normalized,
		Metrics: metrics,
		Probes:  normalized.Reverse.ProbeBudget,
	}
}

// BaseReplayCacheConfig builds the anti-replay cache's config from the
// limits. The window is deliberately NOT exposed here: it is wire-normative
// (domain.DatagramBaseReplayWindow, §2.2) and is not a limit to tune.
func (l Limits) BaseReplayCacheConfig(clock func() time.Time) BaseReplayCacheConfig {
	return BaseReplayCacheConfig{Clock: clock, Capacity: l.Normalized().Replay.Capacity}
}

// MaxFrameBytes is the largest serialized frame of a class that the layer can
// legitimately see, used to sanity-check the budgets against reality: a
// budget smaller than one frame of the class would refuse every frame of it.
//
// It is an upper bound, not a measurement: the payload cap plus base64
// expansion plus the fixed header and auth block, rounded up.
func MaxFrameBytes(class domain.DatagramClass) int {
	payloadCap, err := domain.DatagramPayloadCap(class)
	if err != nil {
		return protocol.MaxFrameLine
	}
	const (
		base64Numerator   = 4
		base64Denominator = 3
		// headerAllowance covers the JSON envelope: the two identities, the
		// salt, the signature, the dtype and the fixed keys. Measured
		// frames sit near 400 bytes; 1 KiB is a deliberate over-estimate,
		// because this bound is used to prove budgets are big enough.
		headerAllowance = 1 << 10
	)
	size := payloadCap*base64Numerator/base64Denominator + headerAllowance
	if size > protocol.MaxFrameLine {
		return protocol.MaxFrameLine
	}
	return size
}

// Compile-time proof that the limits satisfy the seam the reverse table
// declared. If the interface moves, this is where it breaks.
var _ reverseLimits = Limits{}
