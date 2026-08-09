package datagram

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// ttl.go holds the WHOLE ttl life cycle of §4.1.1 as pure functions, in one
// place. Each of the five rules looks obvious on its own; together they
// admit exactly one correct order, and splitting them across the three mode
// branches is how implementations drift apart:
//
//  1. the zero check runs on the RAW incoming value, before anything else —
//     clamping first would resurrect a dead datagram;
//  2. `ttl <= auth.max_ttl` also runs on the RAW value, before the clamp;
//  3. the clamp to defaultMaxHops runs after both checks on the routed
//     plane, and as an EXPLICIT pipeline step immediately before the
//     decrement on the request/response planes, where auth.max_ttl does not
//     exist and the common part checks only `ttl == 0`;
//  4. the decrement happens exactly once, and only when forwarding somebody
//     else's frame: the origin node does not decrement, and local delivery
//     (dst == self, or a response whose upstream is local) does not either;
//  5. a response starts at defaultMaxHops, and the only thing that produces
//     one is the target's handler — a transit never answers (§4.4).
//
// Reference: docs/refactoring/datagram-transport.md §4.1, §4.1.1, §4.2.

// TTLExhausted reports rule 1: a RAW incoming ttl of zero is dropped before
// any clamp. The argument is named raw to make the call site say out loud
// which value it is judging — the clamped one would be a bug that no test
// of the clamp itself could catch.
func TTLExhausted(raw uint8) bool { return raw == 0 }

// TTLWithinBudget reports rule 2: the RAW ttl must not exceed the sender's
// signed hop budget. Checking the clamped value instead would silently
// admit `ttl = 255` on a frame whose signer allowed ten hops, because the
// clamp would have already rewritten it to ten.
//
// It exists only on the routed plane: request and response carry no auth,
// hence no max_ttl, which is exactly why rule 3 needs an explicit clamp
// step there (§4.1 request step 6, response step 6).
func TTLWithinBudget(raw, maxTTL uint8) bool { return raw <= maxTTL }

// ClampTTL implements rule 3. defaultMaxHops is a WIRE constant, not a
// local knob: the clamp, the initial ttl of a response and the sizing of
// the reverse-state window all read it, and two nodes disagreeing about it
// would produce frames one considers lawful and the other inflated.
func ClampTTL(raw uint8) uint8 {
	if raw > domain.DatagramDefaultMaxHops {
		return domain.DatagramDefaultMaxHops
	}
	return raw
}

// DecrementTTL implements rule 4 for ONE hop of somebody else's frame. The
// bool is false when there is no budget left to spend, so a caller cannot
// underflow a uint8 into 255 and hand a dead frame a full new life.
//
// It refuses `ttl = 1` as well, and that is the point rather than an
// off-by-one. The decrement pays for the hop that is ABOUT to be made, so a
// frame leaving with `ttl = 0` arrives at a neighbour who is obliged by step 3
// of §4.1 to drop it on the raw value. Serializing and writing it anyway buys
// nothing and costs a socket write plus one frame of that neighbour's inbound
// budget for every frame that reaches the end of its hop budget — and the
// budget is exhausted precisely where loops and long paths concentrate.
//
// It is deliberately not called anywhere on a local-delivery path: there is
// no hop to pay for, and §4.1.1 lists the three call sites exhaustively —
// routed transit forwarding, request forwarding, and handing a response to
// a network upstream.
func DecrementTTL(ttl uint8) (uint8, bool) {
	if ttl <= 1 {
		return 0, false
	}
	return ttl - 1, true
}

// OriginTTL is the hop budget a locally created frame leaves with (rule 4).
// The origin does NOT decrement: "decrement before sending" without that
// carve-out would spend one hop at the source, defaultMaxHops would cover
// nine hops, and a reply travelling the maximum path back would arrive with
// ttl = 0 and be dropped.
func OriginTTL() uint8 { return domain.DatagramDefaultMaxHops }

// ResponseTTL is the initial ttl of a response (rule 5). The only thing that
// answers is the target's handler, so the
// return path is by construction no longer than the forward path, and the
// forward path was already bounded by defaultMaxHops, so the budget is
// provably sufficient and at the same time does not let a reply roam longer
// than the request that caused it.
func ResponseTTL() uint8 { return domain.DatagramDefaultMaxHops }

// ClampAndDecrement is rule 3 followed by rule 4 — the pair the unsigned
// planes perform as one explicit pipeline step (§4.1 request step 6,
// response step 6). They are exposed together because on those planes there
// is no budget check between them, and a caller that clamped without
// decrementing (or the reverse) would be the exact defect §4.1.1 warns
// about: an unsigned request with `ttl = 255` living twenty-five times
// longer than the reverse state that has to survive it.
func ClampAndDecrement(raw uint8) (uint8, bool) {
	return DecrementTTL(ClampTTL(raw))
}
