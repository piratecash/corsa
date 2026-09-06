package domain

import "time"

// Rollout telemetry: what the capability rollout looks like from THIS node.
//
// Two questions, deliberately answered by two different shapes:
//
//   - "what is out there right now" — NeighbourComposition, a GAUGE. It is a
//     snapshot of live neighbours, not a running total, because a cumulative
//     count of "peers that once advertised X" answers nothing about whether an
//     upgrade has landed.
//   - "how did our attempts end" — SessionOutcomeStats, a CUMULATIVE counter.
//     Outcomes are events; a gauge of them would forget the failure the moment
//     the next attempt succeeded.
//
// Neither is an estimate of the network. A node sees its own neighbours and
// nothing else, and the types say so rather than letting a reader mistake a
// local sample for a population statistic (docs/refactoring/dht/05-rollout-metrics.md).

// CapabilityUsage counts how many live connections advertise one capability
// and how many distinct peers those connections belong to.
//
// BOTH numbers, always, because they answer different questions and diverge
// exactly when it matters. Two sockets to one peer are two connections and ONE
// peer; reporting only connections would let a single reconnecting neighbour
// look like a rollout wave, and reporting only peers would hide that half our
// sockets still speak the old format.
type CapabilityUsage struct {
	// Capability is the advertised name. Always one of the constants this
	// build knows — never a string taken from the wire.
	Capability Capability
	// Connections is how many live connections advertised it.
	Connections int
	// Peers is how many distinct identities own at least one such connection.
	Peers int
}

// NeighbourComposition is a point-in-time census of live neighbours by
// advertised capability.
//
// READINESS IS A FIELD, not an inference from zeros. Before the first refresh
// every count is legitimately zero, and so is the count on a node with no
// neighbours — the two states look identical and mean opposite things ("we
// have not looked yet" vs "we looked and there is nobody"). A reader that
// cannot tell them apart will eventually report the first as the second.
//
// UpdatedAt belongs to the snapshot rather than to the reader: the refresh runs
// on a background cadence, so the moment an RPC answers is not the moment the
// numbers were true.
type NeighbourComposition struct {
	// Ready is false until the first refresh has completed.
	Ready bool
	// UpdatedAt is when this census was taken. Zero while !Ready.
	UpdatedAt time.Time

	// Connections is every live neighbour connection counted, in both
	// directions, BEFORE any capability filter. Neighbours that advertise
	// nothing we recognise are part of the population and are the ones a
	// rollout is waiting on — filtering them out would answer "how many of the
	// upgraded are upgraded".
	Connections int
	// Peers is how many distinct identities those connections resolve to.
	Peers int

	// IdentityUnproven counts connections whose remote identity is claimed but
	// not proven to us. It is reported separately rather than folded into
	// Peers because an advertised capability on such a connection is a claim by
	// somebody we have not authenticated: usable as a rollout hint, never as
	// authority.
	IdentityUnproven int
	// IdentityUnknown counts live connections with no identity yet — the
	// handshake has not produced one. They are counted so the parts add up:
	// dropping them would make Connections and the per-capability rows
	// disagree with no visible reason.
	IdentityUnknown int

	// Capabilities holds one row per capability this build knows about,
	// including the ones nobody advertised (a zero row is the answer "nobody
	// has it", which is exactly the interesting one early in a rollout).
	Capabilities []CapabilityUsage

	// RoutingV3Triplet counts connections advertising the COMPLETE v3 triplet
	// on that one connection.
	//
	// It is a separate row and not something a reader may assemble from three
	// Capabilities rows: those rows are per-capability populations, and
	// intersecting them would claim a combination no single connection
	// necessarily offered. One peer with two sockets — one advertising routing
	// v3, another advertising relay — would satisfy the intersection and
	// support the triplet on neither.
	RoutingV3Triplet CapabilityUsage
}

// SessionOutcomeStats splits outbound session attempts by how they ended.
//
// The split follows the SMP-relay statistics of SimpleX (errorsConnect /
// errorsCompat / errorsOther) for one reason: "could not reach them" and
// "reached them and could not agree" call for opposite actions — one is a
// network or availability problem, the other is a rollout problem. A single
// failure counter mixes the two and hides precisely the signal a rollout
// needs.
type SessionOutcomeStats struct {
	// StartedAt is when accumulation began. The counters are in-memory and
	// reset on restart, so a bare cumulative number is unreadable without it.
	StartedAt time.Time
	// ReadAt is when the counters were loaded — the closing edge of the period
	// they describe.
	//
	// Separate from the response's `snapshot_at`, which belongs to a cached
	// routing snapshot and stands still while the routing table is unchanged:
	// two answers with one `snapshot_at` and different counts would make every
	// rate computed between them wrong.
	ReadAt time.Time

	// Attempts is every completed outbound attempt, successful or not. It is
	// the denominator; without it a rise in failures cannot be told from a
	// rise in traffic.
	//
	// DERIVED from the four outcomes rather than counted separately: a
	// standalone counter cannot be advanced atomically together with the
	// outcome it belongs to, so a reader between the two increments would see
	// a total that does not equal its parts. Here it always does.
	Attempts uint64
	// Succeeded is attempts that produced a live session.
	Succeeded uint64
	// ErrorsConnect is attempts that never established transport.
	ErrorsConnect uint64
	// ErrorsCompat is attempts that established transport and were refused on
	// protocol compatibility.
	ErrorsCompat uint64
	// ErrorsOther is every other failure. Named "other" rather than left
	// implicit so the four numbers sum to Attempts and a gap is visible
	// instead of silently absorbed.
	ErrorsOther uint64
}
