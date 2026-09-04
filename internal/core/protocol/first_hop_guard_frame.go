package protocol

// first_hop_guard_frame.go carries the first-hop guard set across the LOCAL RPC
// boundary — an operator asking their own node which neighbours carry its
// privacy-sensitive traffic.
//
// # Why this is published at all
//
// The guard policy's promises are statistical, not per-call: "the first hop
// stopped changing", "the set stopped growing", "traffic is not leaving through
// neighbours outside the set". None of those can be established by a unit test
// on a fixture — they are claims about a live network over time — and every one
// of them fails SILENTLY. A primary that rotates hourly, a sampled set walking
// up to its cap, every probe leaving through a fallback neighbour: all three
// look exactly like the policy working, from the outside and from the logs.
//
// So the counters are part of the feature rather than a debugging afterthought.
//
// # Why it is safe to publish
//
// This is a LOCAL frame. It never crosses to a peer, and what it names —
// this node's own neighbours — is already visible to the operator in the peer
// list. It says nothing about which CONTACT any traffic was about.
//
// Strings rather than enums for the same reason as PresenceFrame: a frame is
// JSON, and a number would make an older reader mis-decode silently.

// FirstHopGuardFrame is one sampled neighbour as it crosses local RPC.
type FirstHopGuardFrame struct {
	// Identity is the neighbour's 40-hex fingerprint.
	Identity string `json:"identity"`
	// SampledAt is when it entered the set, RFC3339. Fuzzed at the source —
	// the exact moment is metadata — so it is a period, not a timestamp.
	SampledAt string `json:"sampled_at,omitempty"`
	// ConfirmedAt is when it first carried a frame; empty means it never has.
	ConfirmedAt string `json:"confirmed_at,omitempty"`
	// ConfirmedSeq is the confirmation ORDER, which is what actually ranks the
	// set. Published next to ConfirmedAt precisely because the two can
	// disagree: the date is fuzzed, the sequence is exact.
	ConfirmedSeq uint64 `json:"confirmed_seq,omitempty"`
	// RetryAt is when a guard in back-off may be offered again; empty when it
	// is not in back-off.
	RetryAt string `json:"retry_at,omitempty"`
	// Failures is the consecutive-failure count behind RetryAt.
	Failures int `json:"failures,omitempty"`
	// Inbound records that this neighbour dialled US, the direction in which
	// our identity was never proven to them.
	Inbound bool `json:"inbound,omitempty"`
	// Primary marks the guards currently eligible to carry traffic, in order.
	Primary bool `json:"primary,omitempty"`
}

// FirstHopGuardStatsFrame is the counter set. Together with the entries it
// answers "is the policy doing what it claims".
type FirstHopGuardStatsFrame struct {
	// Admitted counts neighbours ever taken into the set.
	Admitted uint64 `json:"admitted"`
	// Confirmed counts guards that have carried a frame.
	Confirmed uint64 `json:"confirmed"`
	// PrimaryChanges counts how often the LEADING hop changed. On a healthy
	// node this settles at a small number; one that keeps climbing is the
	// rotation the guard model exists to prevent.
	PrimaryChanges uint64 `json:"primary_changes"`
	// BackOffs counts failures that armed a retry delay.
	BackOffs uint64 `json:"back_offs"`
	// OutsideSetUses counts frames carried by a neighbour that was not in the
	// set — the policy's own miss rate. A node where this dominates has a
	// stated bound on exposure that is not holding.
	OutsideSetUses uint64 `json:"outside_set_uses"`
	// Retired counts entries dropped at the end of their lifetime.
	Retired uint64 `json:"retired"`
	// Cap and PrimaryTarget publish the constants the numbers above are read
	// against, so a reader does not have to know the build to interpret them.
	Cap           int `json:"cap"`
	PrimaryTarget int `json:"primary_target"`
}
