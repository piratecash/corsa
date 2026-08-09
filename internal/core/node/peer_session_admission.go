package node

// peer_session_admission.go is the per-neighbour admission of the RESPONSE
// plane: the budget, the entitlement gates and the violation ledger the
// outbound peer-session reader (readPeerSession) applies to every line BEFORE
// it is parsed.
//
// # Why this plane needed its own admission at all
//
// The peer-session reader is the widest reader on the node. It accepts up to
// maxResponseLineBytes (8 MiB) per line because two reply types legitimately
// need it (hasWideFrameLineBudget), and until this file existed that width was
// unmetered in three separate ways:
//
//  1. the line was READ AND COPIED in full before anything classified it, so
//     "admission before any decoding" (§4.1 step 1, spec line 417) held against
//     the parser but not against the read: a peer could make the node allocate
//     eight megabytes per frame just to be told the frame was inadmissible;
//  2. a size violation was a silent per-frame drop with no cost to the sender,
//     so it could be repeated forever;
//  3. of the two types that DO buy the wide budget, `contacts` is a REPLY and
//     was accepted — and fully decoded — with no request outstanding, while
//     `push_message` is unsolicited by design. Either could be repeated into
//     the 256-slot session inbox, which is where multi-megabyte frames stop
//     being CPU and start being resident memory.
//
// # Why a budget of its own and not datagram.PeerAdmission
//
// The datagram plane already owns the §5 two-stage per-neighbour budget, and
// reusing it here was considered and rejected on four counts:
//
//   - AVAILABILITY. PeerAdmission exists only when the datagram layer is built
//     (capability plus feature flag). The response plane carries DM delivery on
//     every node, including every node with the layer off; a core DoS defence
//     must not be conditional on an experimental flag;
//   - INDEPENDENCE. §5 sizes the datagram budget for datagram traffic. Sharing
//     one bucket would let an announce full-sync or a DM backlog starve the
//     datagram plane and vice versa — the response plane would either have to
//     inherit a budget sized for 88 KiB bulk frames, or force the datagram
//     budget up to accommodate a 176 KiB DM, and each answer breaks the other
//     plane's sizing argument;
//   - SHAPE. PeerAdmission charges a KNOWN frame size after the frame is in
//     hand. The hole above is precisely that the size is not known until the
//     bytes are read, so what this plane needs is a gate consulted DURING the
//     read — a different operation, not a different caller of the same one;
//   - COST. PeerAdmission is a map keyed by identity with an eviction policy,
//     because datagrams arrive from neighbours that may have no session. This
//     reader always has a session, which is exactly one neighbour with exactly
//     the right lifetime, so a map lookup and a shared mutex on the hottest
//     receive path in the node would buy nothing.
//
// What IS reused is the design: token buckets refilled in continuous time, one
// budget per neighbour whose types DIVIDE it rather than extend it (§5), and a
// cheap probe before the expensive work rather than an accounting pass after
// it.
//
// # Punishment follows the model the project already has
//
// The outbound direction has no ConnID-keyed ban surface — addBanScore keys on
// the IP of an ACCEPTED connection, and dispatchSessionDatagramLine records
// that fact rather than working around it. So repetition is punished the way
// this direction already punishes it: a violation ledger on the session, and a
// teardown that returns an error wrapping protocol.ErrRateLimited, which
// markPeerDisconnected turns into a peer-health penalty and a machine-readable
// disconnect code, and which sessionCloseCauseFromError attributes to the PEER
// so the disconnect_storm quarantine sees it.
//
// TWO frame types are outside this budget altogether, and both are DATA PLANES
// this reader only carries — it is a rule rather than a concession:
//
//   - a `datagram` line arriving while the datagram layer is built does not
//     charge these buckets at all — not its bytes, not its frame — so it can
//     neither be scored by this ledger nor empty the bucket for anybody else. It
//     pays the §5 per-neighbour budget of its own plane instead, charged on the
//     same raw wire line by the ingress the read loop diverts it to;
//   - an ADMISSIBLE `file_command` — one this session negotiated and this node
//     has a router for — does not charge them either, which is what the accepted
//     direction has done since the plane existed. The full argument, including
//     the exposure this leaves and why a budget invented here would not close
//     it, is on sessionFileCommandIsAdmissible.
//
// Exempting only its REFUSAL was not enough, and the shape of what was left is
// the reason the rule is now stated on the meter: the bucket is SHARED. A stream
// of valid datagrams emptied it, and the next line of any other plane — a
// `ping`, a file chunk, an announce — met an empty bucket, took the violation
// and, repeated, ended the session with `rate-limited`. The punishment was not
// removed but moved onto the neighbour's other traffic, which is what §5's
// "types divide a budget rather than extend it" forbids ACROSS planes: two
// budgets that are meant to be independent cannot share one bucket.
//
// The SAME division holds for an oversize line that claims that plane: it pays
// the plane's §5 budget for the bytes it made this node read and nothing else
// (refuseOverBudgetSessionLine, refuseOversizeDatagramClaim). A relay does not
// author the frame it forwards, so §2.3's size verdict is a silent drop rather
// than a violation — the ledger below would otherwise end the session over a
// frame the neighbour only carried.
//
// Every other refusal on this reader keeps both its charge and its score.

import (
	"bufio"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Budget constants
// ---------------------------------------------------------------------------

// peerSessionBytesPerSecond is the sustained inbound byte rate ONE neighbour
// may impose on the peer-session reader, counted on the raw wire line before
// anything is parsed.
//
// 4 MiB/s is sized off the widest legitimate producer on this reader, the file
// transfer chunk stream: a chunk is domain.DefaultChunkSize (16 KiB) raw and
// about 29 KiB on the wire, and the receiver is stop-and-wait per transfer
// (requestNextChunk runs after each response), so one transfer costs at most
// one chunk per round trip — roughly 1.5 MB/s on a 20 ms link. 4 MiB/s leaves
// room for several concurrent transfers plus DM delivery and the announce
// plane on the same session, while capping what a hostile neighbour can make
// this node read, hold and decode at a rate a single core absorbs.
const peerSessionBytesPerSecond = 4 << 20

// peerSessionByteBurst is the byte-bucket depth: four seconds of the sustained
// rate, the same horizon the datagram plane uses for the same reason — deep
// enough to absorb a backlog that arrives back-to-back after a stalled link
// recovers, shallow enough that a neighbour cannot bank a minute of silence
// into one enormous spike.
const peerSessionByteBurst = 4 * peerSessionBytesPerSecond

// peerSessionFramesPerSecond is the sustained inbound frame rate of one
// neighbour on this reader.
//
// The inbound TCP plane allows cmdRefillRate (30/s, burst 100) per connection
// and EXEMPTS the chunked announce plane from it; the datagram plane allows 60.
// This reader carries all of that plus file chunks and DM pushes over a single
// session and has no exemptions, so it gets an order of magnitude more than the
// legacy command rate. 256/s is far above anything the protocol produces —
// every bulk producer on this path is either round-trip bound or chunked — and
// far below what an unmetered socket delivers.
const peerSessionFramesPerSecond = 256

// peerSessionFrameBurst is the frame-bucket depth: four seconds of the
// sustained rate, matched to peerSessionByteBurst so the two dimensions cannot
// disagree about how long a burst may last. It also has to swallow the largest
// legitimate single burst on this reader — a connect-time full sync, which
// chunks at maxRoutesPerAnnounceFrame (100 routes) per frame — with room left
// for the traffic that continues underneath it.
const peerSessionFrameBurst = 4 * peerSessionFramesPerSecond

// peerSessionPushBytesPerSecond is the SEPARATE, narrower byte budget of
// unsolicited `push_message` — the only member of hasWideFrameLineBudget that
// arrives without a request behind it, and therefore the only one whose volume
// the receiver cannot bound by simply not asking.
//
// It is a sub-budget of the shared byte bucket, never an addition to it: a push
// charges both, so the type divides the neighbour's budget rather than widening
// it (§5). 512 KiB/s is ~3 maximum-size DMs per second (a body at
// maxPeerCommandBodyBytes is ~176 KiB on the wire) or ~128 ordinary ones, while
// the sender's own push loop is ack-gated and ships one DM at a time per
// session — so no legitimate delivery approaches it.
const peerSessionPushBytesPerSecond = 512 << 10

// peerSessionPushByteBurst is the push bucket depth, again four seconds. It is
// the number that bounds RESIDENT memory: it is the most a neighbour can have
// in flight in unsolicited wide pushes, against the peerSessionInboxBuffer
// (256) slots that used to admit 8 MiB each.
const peerSessionPushByteBurst = 4 * peerSessionPushBytesPerSecond

// ---------------------------------------------------------------------------
// The contact verification budget: stage two, on the one batched reply
// ---------------------------------------------------------------------------

// maxContactsPerResponse is how many entries ONE `contacts` reply may carry.
//
// # Why a count and not only bytes
//
// The budgets above meter what a neighbour makes this node READ. `contacts` is
// the reply they were widened for, and what its bytes BUY is one
// identity.VerifyBoxKeyBinding — an Ed25519 verification — per array element,
// run in a loop over the whole array. At roughly approximateContactWireBytes on
// the wire, protocol.MaxResponseLine (8 MiB) admits about thirty thousand
// elements, so one legitimate-looking reply to a fetch_contacts this node itself
// asked for bought ~1.5 s of signature checking, and the byte burst admitted two
// of them back to back. A byte budget cannot express that: the price of a byte
// is not constant across frame types, and sizing the byte budget for the most
// expensive one would starve the file-transfer stream it was derived from.
//
// # Why 4096
//
// Sized from what the SENDER can legitimately produce and from what the reply
// may cost, in that order:
//
//   - the LOCAL contacts answer has no count cap of its own — it serialises
//     every address in s.boxKeys, bounded only by maxKnownIdentities (50 000)
//     plus the pinned trust store — so the honest ceiling was set by the wire,
//     not by the protocol. contactsFrameForNetwork builds the outgoing reply
//     bounded to exactly this number — the cap is spent during the walk, not on
//     its result — which is what keeps the receiver's cap from cutting a
//     legitimate exchange: both ends read the same constant;
//   - 4096 entries is about 1 MiB on the wire, comfortably inside one response
//     line, so the COUNT is what binds and not the byte budget — which is the
//     whole point of the second stage;
//   - 4096 verifications is ~0.2 s of one core (a verification costs ~50 µs,
//     the same figure the datagram plane sizes its crypto budget with). That is
//     the most one reply may cost, and it is paid once per fetch_contacts this
//     node CHOSE to send;
//   - it is 64× maxAnnouncePeers (64), the cap the sibling reply `peers` already
//     carries. `peers` lists addresses this node might dial; `contacts` lists
//     identities it holds key material for, which is legitimately a much larger
//     set — but not an unbounded one.
//
// A reply past the cap is refused whole rather than trimmed: s.boxKeys is a map,
// so the entries a trim would keep are chosen by Go's randomised iteration
// order, and importing an attacker-influenced random subset of a reply that
// already broke the contract is worse than importing none of it.
const maxContactsPerResponse = 4096

// approximateContactWireBytes is one serialised protocol.ContactFrame: a
// 40-character address, two 44-character base64 keys, an 86-character
// base64url signature and the JSON around them. It is documentation for the
// sizing above and the arithmetic a test asserts the cap against, never a
// budget that is charged.
const approximateContactWireBytes = 265

// The SUSTAINED rate of contact-binding verifications lives in
// contact_verify_budget.go (contactVerifiesPerSecond / contactVerifyBurst) and
// NOT here, because it is not a property of a session: a bucket that lived on
// this controller was born full with every reconnect, and the second importer —
// the fresh recovery dial of syncPeer — has no session at all. Both charge one
// node-scoped bucket keyed on the remote endpoint instead.

// peerSessionViolationBudget is how many admission violations a session
// TOLERATES in the recent past; the one after that ends it.
//
// One violation is a dropped frame and nothing more: the reader has to survive
// a peer that legitimately sends a large reply of a type hasWideFrameLineBudget
// does not know about — that trade is named in its doc comment and dropping one
// frame must stay recoverable. A SERIES is not an accident; it is a neighbour
// discovering that violations are free. Four forgiven is high enough that no
// single misunderstanding reaches it and low enough that a flood is cut off in
// milliseconds.
//
// It is a "tolerated" count rather than a "trip at" count because the ledger
// decays in continuous time: with a trip-at comparison the fifth violation of
// five would score 4.9997 and be forgiven by the microseconds that elapsed
// between the frames, which is a limit that depends on how fast the attacker
// sends.
const peerSessionViolationBudget = 4

// peerSessionViolationDecayPerSecond forgives the ledger over time, so the
// count means "violations in the recent past" and not "violations since the
// session opened". At 0.1/s a peer is fully forgiven after 50 s of good
// behaviour, and a peer that repeats a violation once per ten seconds keeps its
// session forever while still losing every offending frame.
const peerSessionViolationDecayPerSecond = 0.1

// ---------------------------------------------------------------------------
// The per-session controller
// ---------------------------------------------------------------------------

// peerSessionAdmission is the response-plane admission state of ONE outbound
// session, and therefore of one authenticated neighbour: this node opens at
// most one session per peer address, so "per session" and "per neighbour" are
// the same set here.
//
// # Locking
//
// mu is the state's OWN lock and is deliberately not one of the seven domain
// mutexes (docs/locking.md). It is taken only by the session's reader goroutine
// and by the request helper on the serve loop, is never held across I/O or a
// callback, and guards nothing but arithmetic. Routing it through peerMu would
// put the node's busiest lock on the per-frame receive path to protect four
// floats.
//
// # Zero value
//
// The zero value is a live controller with FULL buckets: lastRefill is stamped
// and the buckets are filled on first use. That is what lets peerSession keep
// it by value and every existing construction site — production and test alike
// — stay unchanged, without a nil check that would silently mean "no budget".
type peerSessionAdmission struct {
	mu sync.Mutex

	// clock is the injectable time source; nil selects the wall clock. Tests
	// set it through setClock before the reader starts.
	clock func() time.Time

	// started distinguishes "never used" from "used and empty", which a zero
	// lastRefill cannot: a zero time would refill the buckets by six decades
	// on first use, which is the same answer by accident and the wrong one on
	// purpose.
	started    bool
	lastRefill time.Time

	// bytes and frames are the SHARED raw budget every line charges.
	bytes  float64
	frames float64

	// pushBytes is the narrower unsolicited-push sub-budget.
	pushBytes float64

	// violations is the leaky ledger that turns repetition into a teardown.
	violations float64

	// awaitingReply is the reply type peerSessionRequest currently has
	// outstanding on this session, or "" when nothing is in flight. It is the
	// state that makes "a big `contacts` is only admissible when we asked for
	// one" expressible at all: the reader goroutine and the requester are
	// different goroutines, so the request cannot be a local variable.
	awaitingReply string
}

// setClock installs the time source. It exists for tests; production leaves the
// field nil and gets time.Now.
func (a *peerSessionAdmission) setClock(clock func() time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.clock = clock
}

// now reads the clock. Caller holds mu.
func (a *peerSessionAdmission) nowLocked() time.Time {
	if a.clock == nil {
		return time.Now()
	}
	return a.clock()
}

// refillLocked advances every bucket to now. Caller holds mu.
func (a *peerSessionAdmission) refillLocked(now time.Time) {
	if !a.started {
		a.started = true
		a.lastRefill = now
		a.bytes = peerSessionByteBurst
		a.frames = peerSessionFrameBurst
		a.pushBytes = peerSessionPushByteBurst
		return
	}
	elapsed := now.Sub(a.lastRefill).Seconds()
	if elapsed <= 0 {
		// A backwards or stalled clock refills nothing. Advancing lastRefill
		// anyway would let a repeated backwards step erase the elapsed time
		// the buckets are owed.
		return
	}
	a.lastRefill = now
	a.bytes = capBucket(a.bytes+elapsed*peerSessionBytesPerSecond, peerSessionByteBurst)
	a.frames = capBucket(a.frames+elapsed*peerSessionFramesPerSecond, peerSessionFrameBurst)
	a.pushBytes = capBucket(a.pushBytes+elapsed*peerSessionPushBytesPerSecond, peerSessionPushByteBurst)
	a.violations -= elapsed * peerSessionViolationDecayPerSecond
	if a.violations < 0 {
		a.violations = 0
	}
}

// capBucket clamps a refilled bucket to its depth.
func capBucket(tokens, burst float64) float64 {
	if tokens > burst {
		return burst
	}
	return tokens
}

// chargeLine charges ONE fully-read line against the shared raw budget and,
// when the line claims to be a push, against the unsolicited-push sub-budget.
// It reports whether the line may go on to be parsed.
//
// The rule is "spend while non-empty, then owe": a bucket admits as long as it
// holds anything at all, and the charge may drive it negative. That is what
// keeps one legitimate oversize frame from being cut in half by a bucket that
// happens to be a few bytes short — the frame is admitted whole and the
// neighbour pays for it out of the next second of its allowance. The debt is
// bounded by one line, because the very next line meets an empty bucket.
func (a *peerSessionAdmission) chargeLine(size int, push bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(a.nowLocked())

	if a.bytes <= 0 || a.frames <= 0 {
		return false
	}
	if push && a.pushBytes <= 0 {
		return false
	}
	a.bytes -= float64(size)
	a.frames--
	if push {
		a.pushBytes -= float64(size)
	}
	return true
}

// canExtend answers the pre-read half: does this neighbour still have raw
// budget to spend on a line that has already reached the strict limit, and — for
// an unsolicited push — does its own sub-budget still hold anything.
//
// It PROBES and does not charge, because the size is not known yet; the charge
// lands in chargeLine once the line is in hand. A probe that refuses stops the
// read before the remainder is ever pulled off the socket, which is the whole
// point of the staging.
func (a *peerSessionAdmission) canExtend(push bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(a.nowLocked())
	if a.bytes <= 0 || a.frames <= 0 {
		return false
	}
	return !push || a.pushBytes > 0
}

// recordViolation adds one to the leaky ledger and reports the new score plus
// whether the session has to go.
func (a *peerSessionAdmission) recordViolation() (float64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillLocked(a.nowLocked())
	a.violations++
	return a.violations, a.violations > peerSessionViolationBudget
}

// expectReply records that a request is in flight and its reply type. It
// returns the release function, so the caller cannot forget the clear: the one
// caller is peerSessionRequest, whose every exit is a defer away.
func (a *peerSessionAdmission) expectReply(replyType string) func() {
	a.mu.Lock()
	a.awaitingReply = replyType
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.awaitingReply = ""
		a.mu.Unlock()
	}
}

// awaitsReply reports whether replyType is the reply this session is currently
// waiting for.
func (a *peerSessionAdmission) awaitsReply(replyType string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.awaitingReply != "" && a.awaitingReply == replyType
}

// ---------------------------------------------------------------------------
// Entitlement
// ---------------------------------------------------------------------------

// isSolicitedOnlyFrameType names the frame types that are REPLIES on the
// peer-session reader: they exist only as the answer to a request this node
// sent, so one arriving with nothing outstanding is either a stale reply to a
// request that already timed out or a peer feeding the reader work it never
// asked for.
//
// `contacts` is the whole set today, and it is the expensive one: a remote's
// contact store is bounded only by maxKnownIdentities plus its trust store, so
// it is the reply protocol.MaxResponseLine was raised for. Its
// SIZE is metered here; what those bytes buy — one signature verification per
// array element — is metered separately by maxContactsPerResponse and the
// per-remote bucket of contact_verify_budget.go, because a byte budget cannot
// price crypto. `peers` is
// NOT here even though it is equally a reply, because it is bounded to
// maxAnnouncePeers (64) entries on ingest and cannot buy the wide budget in the
// first place — listing it would add a rejection path without removing any work.
func isSolicitedOnlyFrameType(frameType string) bool {
	return frameType == "contacts"
}

// grantFrameLineExtension is the pre-read gate of the peer-session reader: a
// line that has reached protocol.MaxFrameLine asks here how much further it may
// grow, and the answer is taken from the CLAIM in its first bytes, the type's
// entitlement, and the neighbour's remaining budget — before the remainder is
// read or copied.
//
// Zero means "stop now": the reader refuses the line without pulling the rest
// of it into memory, and the caller discards the remainder and charges the
// neighbour a violation.
//
// The claim is not trusted as a classification and does not need to be. It can
// only ever BUY the wide budget for a type entitled to it; the authoritative
// verdict is taken from the complete line by admitFrameLinePreParse, which
// refuses every line whose type the scan and the parser could read differently.
// A decoy that claims `push_message` in its first bytes and parses as something
// else therefore ends up refused anyway, having paid the full byte charge for
// the bytes it made this node read.
func (s *Service) grantFrameLineExtension(session *peerSession, claimed string, named bool) int {
	if !named || !hasWideFrameLineBudget(claimed) {
		return 0
	}
	if isSolicitedOnlyFrameType(claimed) && !session.admission.awaitsReply(claimed) {
		// A multi-megabyte reply to a request nobody made. This is the branch
		// that closes the hole: before it, `contacts` bought 8 MiB of read,
		// copy and decode from any authenticated peer at any moment.
		return 0
	}
	if !session.admission.canExtend(claimed == "push_message") {
		return 0
	}
	// The line may grow to the response budget the two entitled types were
	// given (maxResponseLineBytes); the charge for whatever it actually uses
	// lands in chargeLine when the line is complete.
	return maxResponseLineBytes - protocol.MaxFrameLine
}

// claimedFrameTypeFromPrefix reads the top-level `type` out of a line PREFIX.
//
// It is classifyFrameLine's scanner with one difference that follows from
// having only a prefix: it answers as soon as the first literal, unescaped,
// plain-string top-level `type` is found, because a prefix cannot see whether a
// second candidate follows. That is sound HERE and only here — the answer buys
// nothing but the right to keep reading, and the complete line still has to
// pass admitFrameLinePreParse, which refuses duplicates outright. A prefix that
// names nothing, names something escaped, or gives `type` a non-string value
// reports false and buys nothing.
func claimedFrameTypeFromPrefix(prefix string) (string, bool) {
	pos := skipJSONSpace(prefix, 0)
	if pos >= len(prefix) || prefix[pos] != '{' {
		return "", false
	}
	pos++

	depth := 1
	for pos < len(prefix) && depth > 0 {
		switch prefix[pos] {
		case '"':
			key, escaped, next, ok := scanJSONString(prefix, pos)
			if !ok {
				return "", false
			}
			colon := skipJSONSpace(prefix, next)
			if colon >= len(prefix) || prefix[colon] != ':' {
				// A string in value position names no key.
				pos = next
				continue
			}
			if depth != 1 || (!escaped && !bindsToFrameTypeField(key)) {
				pos = colon + 1
				continue
			}
			if escaped || key != frameTypeKey {
				return "", false
			}
			value, valueEscaped, _, ok := scanJSONStringValue(prefix, colon+1)
			if !ok || valueEscaped {
				return "", false
			}
			return value, true
		case '{', '[':
			depth++
			pos++
		case '}', ']':
			depth--
			pos++
		default:
			pos++
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// The read entry point
// ---------------------------------------------------------------------------

// errPeerSessionLineRefused reports that the reader consumed and dropped ONE
// line on admission grounds and the session survives. It never leaves
// readAdmittedSessionLine's caller.
var errPeerSessionLineRefused = errPeerSessionAdmission("peer session frame line refused by admission")

// errPeerSessionAdmission is a named error type so an admission sentinel is
// distinguishable from a transport error by type as well as by identity.
type errPeerSessionAdmission string

func (e errPeerSessionAdmission) Error() string { return string(e) }

// peerSessionAdmissionTeardown builds the error that ends a session whose
// neighbour kept violating admission.
//
// It wraps protocol.ErrRateLimited on purpose: markPeerDisconnected records the
// wrapped code as LastDisconnectCode, so an operator sees `rate-limited` on the
// peer rather than a bare string, and sessionCloseCauseFromError attributes the
// teardown to the PEER, which is what feeds the disconnect_storm quarantine.
func peerSessionAdmissionTeardown(score float64) error {
	return &peerSessionAdmissionError{score: score}
}

type peerSessionAdmissionError struct {
	score float64
}

func (e *peerSessionAdmissionError) Error() string {
	return fmt.Sprintf("peer session admission: %.1f violations past a budget of %d",
		e.score, peerSessionViolationBudget)
}

func (e *peerSessionAdmissionError) Unwrap() error { return protocol.ErrRateLimited }

// readAdmittedSessionLine reads ONE line for the peer-session reader with the
// per-neighbour raw budget applied AROUND the read rather than after it.
//
// The order is the finding this function exists for:
//
//  1. read up to protocol.MaxFrameLine — the budget every frame type on this
//     node is entitled to, and a bound on allocation that holds for any line;
//  2. AT that boundary, before the remainder is pulled off the socket,
//     grantFrameLineExtension decides whether this neighbour has earned more:
//     the claim must name an entitled type, a solicited-only type must have a
//     request outstanding, and the raw budget must not be empty;
//  3. only then are the remaining bytes read and copied;
//  4. the complete line charges the shared byte and frame budget — still before
//     any parsing — and a neighbour out of budget has the line dropped here,
//     with the parse never paid for and one violation on the ledger. Two kinds
//     of line do not reach this step at all, and both are DATA PLANES this
//     reader only carries: a datagram, which the read loop hands to the plane
//     that charges §5 on the very same bytes, and an admissible file command,
//     which the inbound direction has never charged either
//     (sessionFileCommandIsAdmissible).
//
// It returns errPeerSessionLineRefused when one line was consumed and dropped
// and the loop should continue, and any other error when the session is over.
func (s *Service) readAdmittedSessionLine(reader *bufio.Reader, session *peerSession) (string, error) {
	// The claim the gate read is kept: a line REFUSED during the read never
	// becomes a parsed frame, so the pre-read claim is the only name its drop
	// can ever be attributed to, and a drop with no type is a drop the
	// "dropped by reason" ledger cannot place.
	claimed := ""
	read, err := readFrameLineStaged(reader, protocol.MaxFrameLine, func(prefix string) int {
		var named bool
		claimed, named = claimedFrameTypeFromPrefix(prefix)
		return s.grantFrameLineExtension(session, claimed, named)
	})
	if errors.Is(err, errFrameTooLarge) {
		return "", s.refuseOverBudgetSessionLine(reader, session, read, claimed)
	}
	line := read.line
	if err != nil {
		return line, err
	}

	// The one line this budget does not meter. It is admitted UNCHARGED and
	// handed on unchanged: the read loop's diversion takes it from here to the
	// plane's ingress, which charges §5 on these same raw bytes before it decodes
	// any of them. "The neighbour pays for the bytes it made this node read" is
	// therefore not waived, it is paid in the other currency — and the narrower
	// one, since §5 allows one neighbour 1 MiB/s and 64 frames/s against the
	// 4 MiB/s and 256 frames/s of this file.
	if s.sessionDatagramPaysItsOwnBudget(session, line) {
		return line, nil
	}

	// The other line this budget does not meter: an ADMISSIBLE file command,
	// which the inbound direction has exempted from its own limiter since the
	// plane existed. It stays bounded by protocol.MaxFrameLine — the wide budget
	// is not for sale to this type (hasWideFrameLineBudget).
	if s.sessionFileCommandIsAdmissible(session, line) {
		return line, nil
	}

	// The push sub-budget is charged from the PRE-PARSE claim, which is the
	// only type name available before the parse the charge exists to protect.
	// A line that lies about being a push is refused outright one gate later
	// (admitFrameLinePreParse), so lying can only ever cost the liar more.
	claimed, _ = claimedFrameTypeFromPrefix(line)
	if session.admission.chargeLine(len(line), claimed == "push_message") {
		return line, nil
	}
	return "", s.punishSessionAdmission(session, "raw_budget_exhausted", claimed, len(line))
}

// sessionDatagramPaysItsOwnBudget reports whether ONE fully-read line is
// metered by the datagram plane's §5 per-neighbour budget INSTEAD of this one —
// the only line on this reader that is admitted without charging these buckets.
//
// It answers from three conditions and none of them is negotiable, because each
// one is a way to turn the rule into a free channel:
//
//   - the AUTHORITATIVE classification, and literally the same call the read
//     loop's diversion makes (isDatagramWireLine). A line that skipped this
//     budget and was then NOT diverted would be charged by nobody, and the only
//     way two predicates cannot disagree about what a line IS, is not to be two
//     predicates. The pre-read claim of claimedFrameTypeFromPrefix must never
//     reach here: that claim can only ever BUY work, and a budget skipped by
//     naming a type in the first bytes would hand every neighbour a keyword that
//     switches the meter off;
//   - datagramCarriesOwnBudget, so this carries the condition that already
//     governs the inbound command-limiter exemption: no layer, no replacement
//     budget, no skip. The response plane is on every node, the datagram layer is
//     not, and a defence must not disappear with an experimental flag;
//   - a BILLABLE neighbour on the plane's own key. §5 is charged on
//     datagram.DialedAddressKey, and handleDatagramFrame refuses a zero key
//     ABOVE its own charge, so a session with no dialled address would divert
//     into a budget that is never charged. The key is derived here through the
//     same constructor the ingress uses, so the two cannot disagree about who is
//     billable, and the answer fails CLOSED: not billable there means charged
//     here.
//
// # Why the whole meter and not just the ledger
//
// The two budgets differ in KIND, not in strictness. The budget of this file is
// a READER's defence — it bounds what one neighbour makes the widest reader on
// the node read, hold and decode — and it needs a ledger, because this direction
// has no ban surface and a violation that costs nothing is repeated forever. The
// datagram plane's §5 budget is a per-neighbour SHARE of a data plane, and
// exhausting it is normatively "this frame is not carried, and nothing else": no
// `rate-limited`, no TCP tear-down, no ban score, whatever the frame's dtype,
// class or payload. That rule already holds on the accepted-connection
// direction, where a datagram pays no command bucket at all.
//
// Charging the shared bucket and only sparing the ledger left the same defect
// one frame later. The bucket is shared by construction, so a datagram stream
// emptied it and the NEXT line of any other plane paid: it met an empty bucket,
// took the violation, and a series of them ended the session as `rate-limited`.
// Independence between the two budgets is a property of the METER, not of the
// punishment.
//
// Everything else on this reader keeps both, and the difference is not a
// preference:
//
//   - a line whose type cannot be resolved (frame_line_ambiguous) belongs to no
//     plane at all — §3.4 refuses a duplicate top-level key on this one
//     outright — so it is not a frame of the datagram plane to begin with;
//   - a line PAST the strict budget never reaches this predicate at all: it is
//     refused DURING the read, where the only type name available is the
//     untrusted claim. That refusal has a rule of its own — the claim moves the
//     charge onto the plane's §5 budget and drops the punishment
//     (refuseOverBudgetSessionLine) — because §2.3 is a verdict about the LINE
//     and the neighbour that carried it did not write the frame inside it.
//
// # It is not free
//
// "Not charged here" is not "not metered". §5 charges the neighbour's bytes AND
// frames on the raw wire line at the ingress (dispatchSessionDatagramLine, keyed
// on the dialled address), before anything is decoded, and it is the narrower of
// the two budgets in both dimensions: 1 MiB/s and 64 frames/s per neighbour
// against 4 MiB/s and 256 frames/s here. A datagram stream on this reader is
// bounded by that, and by protocol.MaxFrameLine per line, which the staged read
// enforces before this point for every type not entitled to the wide budget.
func (s *Service) sessionDatagramPaysItsOwnBudget(session *peerSession, line string) bool {
	if !isDatagramWireLine(line) {
		return false
	}
	// The dialled address is this direction's billable key, and handing it to
	// the shared predicate is what keeps the two directions from drifting: the
	// exemption holds exactly where the §5 budget has somebody to charge.
	return s.datagramCarriesOwnBudget(protocol.DatagramFrameType, datagram.DialedAddressKey(session.address))
}

// sessionFileCommandIsAdmissible reports whether ONE fully-read line is a file
// command this reader is going to ACT on — and is therefore outside the shared
// raw budget and outside the violation ledger, exactly as it already is on the
// accepted-connection direction (exemptFrameTypeFromCommandLimit).
//
// # Why the file plane is outside this budget at all
//
// The budget is sized off the file chunk stream and is still too small for it.
// A chunk is domain.DefaultChunkSize (16 KiB) raw and about 29 KiB on the wire,
// so 4 MiB/s is ~144 chunk frames per second — and the file manager
// deliberately places no cap on how many transfers run at once, because the
// stop-and-wait-per-transfer protocol was the bound. Several concurrent
// downloads therefore meet an empty bucket during ordinary use, and on this
// reader an empty bucket is not a dropped chunk: it is a violation, and five of
// them inside the decay window close the session with an error wrapping
// protocol.ErrRateLimited — a peer-health penalty and a reconnect for doing the
// thing the feature exists to do. The inbound direction never charged this
// traffic to a control-plane limiter for the same reason; the two directions of
// one transfer disagreeing about it is the defect.
//
// # What buys the exemption, and what deliberately does not
//
// Naming the type does not. The three conditions are each a way the rule turns
// into a free channel if it is dropped:
//
//   - the AUTHORITATIVE classification (isFileCommandWireLine), which is the
//     same answer protocol.ParseFrameLine gives the dispatch branch one gate
//     later, so a line cannot skip this budget and then be dispatched as
//     something else. The pre-read claim must never reach here: it answers on
//     the first top-level candidate a prefix holds, so it would hand every
//     neighbour a keyword that switches the meter off;
//   - the negotiated CAPABILITY of this very session, read through the same
//     helper the dispatch gate uses (sessionHasCapability, which also requires
//     the handshake to have completed). A frame arriving without it is dropped
//     unread by the file gate, so exempting it would spare a line with no
//     possible effect — the strictly stronger half of what the accepted
//     direction does, where the exemption is decided from the type alone and the
//     capability is only consulted after the parse;
//   - a CONSUMER on this node (fileCommandHasConsumer). No subsystem, no
//     exemption: the same condition the datagram carve-out carries about its
//     layer.
//
// # It is not unmetered
//
// `file_command` cannot buy the wide response budget (hasWideFrameLineBudget),
// so every line of it is still refused past protocol.MaxFrameLine during the
// read, and that refusal keeps both its charge and its score. What is removed is
// the per-neighbour RATE, and the risk is named rather than hidden: unlike the
// datagram plane there is no §5 budget of the file plane's own to move the cost
// onto, so a neighbour that negotiated file_transfer_v1 can stream 128 KiB lines
// at line rate and pay only the file router's own admission (nonce cache, TTL,
// signature). That is the same exposure the accepted direction has carried since
// the plane existed, and it is bounded work per frame; a per-neighbour file
// budget belongs to the file plane, which does not have one yet, and inventing
// one here would silently stall transfers rather than protect anything the size
// cap does not.
func (s *Service) sessionFileCommandIsAdmissible(session *peerSession, line string) bool {
	if !isFileCommandWireLine(line) {
		return false
	}
	if !s.sessionHasCapability(session, domain.CapFileTransferV1) {
		return false
	}
	return s.fileCommandHasConsumer()
}

// refuseOverBudgetSessionLine handles a line the staged reader stopped: the
// remainder is discarded so it cannot be read as frames of its own, and the
// neighbour is charged the bytes it made this node read.
//
// WHAT ELSE it is charged depends on which plane the line claimed, and the two
// answers differ because the two planes price a size violation differently:
//
//   - a line claiming `datagram` is refused by the rule of §2.3 alone
//     (refuseOversizeDatagramClaim): its bytes go to that plane's own §5
//     per-neighbour budget and the refusal costs nothing else — no violation, no
//     `rate-limited`, no teardown. A relay is not the author of the frame it
//     forwards, and §4.4 keeps punishment for what a transit is obliged to
//     check;
//   - everything else keeps both the charge and the score. The response plane
//     has no per-neighbour byte budget to move the cost onto, so the ledger is
//     the only thing that makes a repetition cost the sender anything.
//
// The claim decides which of the two applies, and it may: it buys a narrower
// budget and the loss of a punishment, never any processing (the full argument
// is on refuseOversizeDatagramClaim).
//
// HOW MUCH it is charged is the read's own count plus the discard's, never
// arithmetic over the limits: the read stops on a buffer fill, so both stages
// overshoot their limit by part of one chunk, and a size reconstructed from the
// constants gives that overshoot away on every refusal — on the one plane where
// the bytes are now the whole of the price.
func (s *Service) refuseOverBudgetSessionLine(
	reader *bufio.Reader,
	session *peerSession,
	read frameLineRead,
	claimed string,
) error {
	budgetKey := datagram.DialedAddressKey(session.address)
	discardLimit := maxResponseLineBytes
	if claimed == protocol.DatagramFrameType {
		// A datagram is MaxFrameLine at most (§2.3), so one frame of slack is
		// all a resynchronisation can legitimately need; past that the peer is
		// streaming, not framing.
		discardLimit = oversizeDatagramResyncBytes
	}
	discarded, err := 0, error(nil)
	if !read.delimited {
		discarded, err = discardFrameLineRemainder(reader, discardLimit)
	}
	size := read.consumed + discarded
	if err == nil && s.refuseOversizeDatagramClaim(datagramOutboundSession, claimed, budgetKey, size) {
		return errPeerSessionLineRefused
	}
	// The bytes are charged whichever way the discard went: the node read them,
	// so the neighbour pays for them. The verdict is already decided, so the
	// return value of the charge is not a second decision.
	_ = session.admission.chargeLine(size, false)
	// The refusal lands on the same per-reason counter the post-parse gate
	// feeds. It has moved one step earlier, not changed meaning, and giving it
	// a reason of its own would make the two impossible to add up (§10).
	s.countOversizeDatagramRefusal(claimed)
	if err != nil {
		// Either the peer never terminated the line within the budget above —
		// no longer a misbehaving frame but an unbounded stream — or the socket
		// failed. Both end the session, which is what this reader has always
		// done at the point where a line stops being a frame.
		return err
	}
	return s.punishSessionAdmission(session, "wide_budget_refused", claimed, size)
}

// punishSessionAdmission records one violation, logs it with the identity of
// the neighbour and the reason, and decides between dropping the frame and
// ending the session.
//
// One log line per violation is deliberate: a drop with no trace is how a
// "dropped by reason" ledger loses exactly the refusals the widest reader on
// this node produces, and the rate is self-limiting because the ledger tears
// the session down once they pass peerSessionViolationBudget.
//
// It is reached by every refusal on this reader that this budget can produce.
// A datagram never produces one, because it never charges this budget at all
// (sessionDatagramPaysItsOwnBudget); why that is the meter and not just the
// ledger, and why each of the four reasons that remain keeps its score, are
// argued there.
func (s *Service) punishSessionAdmission(session *peerSession, reason, claimed string, size int) error {
	score, tearDown := session.admission.recordViolation()
	event := log.Warn().
		Str("peer", string(session.address)).
		Str("peer_identity", session.peerIdentity.String()).
		Str("reason", reason).
		Str("claimed_type", claimed).
		Int("size", size).
		Float64("violations", score)
	if !tearDown {
		event.Msg("peer_session_admission_frame_dropped")
		return errPeerSessionLineRefused
	}
	event.Msg("peer_session_admission_session_closed")
	return peerSessionAdmissionTeardown(score)
}

// refuseUnsolicitedReplyLine drops a REPLY-only frame that arrived with no
// request outstanding, before it is parsed and at ANY size.
//
// The size-independent form is the point. A wide unsolicited `contacts` is
// already refused during the read (grantFrameLineExtension), but a peer that
// keeps its line under the strict budget would still have had every one of them
// decoded and pushed into the session inbox, where the dispatcher has no case
// for a reply type and drops it — the node paying for a parse whose only
// possible outcome is a no-op.
//
// It is NOT a violation. A reply to a request that timed out a moment ago is a
// slow peer, not a hostile one, and the frame is cheap by construction here —
// the expensive form of it is the one grantFrameLineExtension already refuses,
// and that one IS scored.
func (s *Service) refuseUnsolicitedReplyLine(session *peerSession, claimed string) bool {
	if claimed == "" || !isSolicitedOnlyFrameType(claimed) {
		return false
	}
	if session.admission.awaitsReply(claimed) {
		return false
	}
	log.Debug().
		Str("peer", string(session.address)).
		Str("type", claimed).
		Msg("peer_session_unsolicited_reply_dropped")
	return true
}

// refuseOversizeContactsReply drops one `contacts` reply that carried more than
// maxContactsPerResponse entries and charges the neighbour for it.
//
// It reports nil while the session survives — the reply is dropped, nothing is
// imported, and the caller continues — and the teardown error once the ledger
// passes peerSessionViolationBudget. Both readings match the model this file
// already uses for the wide-line gate: one violation is a dropped frame, because
// the cap is a NEW protocol rule and a peer built before it can trip it once
// honestly; a series is a neighbour that discovered violations are free.
//
// The same ledger is used deliberately rather than a second counter of its own:
// a peer must not be able to spend four wide-line refusals and four oversize
// replies to stay under two separate budgets.
//
// It CLOSES the session itself when the ledger trips, instead of leaving that to
// the returned error. The callers of the contact sync are not the reader loop —
// syncSenderKeys logs the error and carries on by design — so a teardown that
// depended on the caller acting would be a teardown that mostly did not happen.
func (s *Service) refuseOversizeContactsReply(session *peerSession, offered int) error {
	score, tearDown := session.admission.recordViolation()
	event := log.Warn().
		Str("peer", string(session.address)).
		Str("peer_identity", session.peerIdentity.String()).
		Str("reason", "contacts_count_cap").
		Int("contacts", offered).
		Int("cap", maxContactsPerResponse).
		Float64("violations", score)
	if !tearDown {
		event.Msg("peer_session_admission_contacts_reply_dropped")
		return nil
	}
	event.Msg("peer_session_admission_session_closed")
	teardown := peerSessionAdmissionTeardown(score)
	if err := session.Close(); err != nil {
		log.Debug().Err(err).Str("peer", string(session.address)).
			Msg("peer_session_admission_contacts_close_error")
	}
	return teardown
}
