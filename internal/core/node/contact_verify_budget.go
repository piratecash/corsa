package node

// contact_verify_budget.go owns the node-scoped, per-remote WORK budget of the
// `contacts` reply: one token per identity.VerifyBoxKeyBinding, refilled in
// continuous time, SHARED by every path that imports advertised contacts and
// PERSISTED across connections.
//
// # Why it cannot live on the connection
//
// Both importers used to carry their own counter and both counters were born
// full with the connection that carried them:
//
//   - the fresh recovery dial (syncPeer) built a non-refilling budget of
//     maxContactsPerResponse per dial, on the argument that the connection
//     carries exactly one reply and is then closed;
//   - the peer session carried the same budget inside peerSessionAdmission,
//     whose zero value is a full bucket, so a reconnect started a new one.
//
// The first argument is the one that fails outright. The recovery dial is not
// scheduled by this node's own policy: it is triggered by DM frames whose
// `sender` fingerprint the SENDER writes (triggerSenderKeySyncAsync), and the
// three gates around it — the per-sender single-flight, the per-hop slot and
// maxConcurrentSenderKeySyncPasses — all bound CONCURRENCY, releasing the
// moment a pass finishes. The per-sender cooldown is keyed on that same
// self-declared field, so a neighbour that varies it walks straight past the
// cooldown. A budget that resets whenever the party being metered decides to
// open a new connection, at a cadence that same party chooses, is not a budget:
// it is a per-connection allowance with an attacker-controlled multiplier.
//
// # What the key is, and why it is not an identity
//
// The budget is charged to the CANONICAL IP of the connection that carries the
// reply — or, for an overlay peer, to its .onion / .b32.i2p name — and
// deliberately not to the peer identity the remote states.
//
// On the path this budget exists for, the remote is NOT authenticated. The
// handshake authenticates in ONE direction: the responder issues the challenge
// and the INITIATOR signs it (docs/protocol/handshake.md, `auth_session`). On
// an outbound dial this node is the initiator, so it PROVES itself to the
// remote and learns nothing proven in return — `welcome.address` is a field the
// responder writes, which is exactly why learnIdentityFromWelcome only caches
// key material that self-certifies. `peerSession.peerIdentity` is that same
// unverified `welcome.address`, so keying on it would repeat the finding one
// level up: the remote would pick its own budget key per connection.
//
// The IP the packets actually arrive from is the only attribution an outbound
// connection has that the remote cannot choose — the TCP handshake completed on
// it — and it is the attribution this node's other punishment surface already
// uses (addBanScore and bannedIPSet key on IP). Its cost is stated rather than
// hidden: distinct nodes behind ONE NAT address share one bucket. That is
// acceptable for THIS budget precisely because the numbers are large relative
// to honest demand — a full contact sync is one burst, and the sustained rate
// refills it in sixteen seconds — while it would NOT be acceptable for the raw
// byte budget of the session reader, which is sized off a file-transfer stream
// that several nodes behind one NAT would genuinely have to share.
//
// The overlay carve-out is in contactVerifyKeyFromEndpoint: there the socket is
// the local SOCKS proxy — shared by every overlay peer — and the .onion name is
// both the correct attribution and the only cryptographically pinned one this
// path has.

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// contactVerifiesPerSecond is the SUSTAINED rate of contact-binding
// verifications one remote may impose on this node.
//
// The count cap (maxContactsPerResponse) bounds ONE reply; this bounds a stream
// of them. It matters because this node asks for contacts more often than once
// per session: the unknown-sender-key recovery of triggerSenderKeySyncAsync
// runs a fetch_contacts against the previous hop and up to senderKeySyncFanout
// more peers, and a hostile neighbour producing DMs from fabricated senders is
// what triggers it.
//
// 256/s is ~13 ms of one core per second per remote — across twenty of them
// ~26% of a single core, which is a cost a node absorbs while nothing
// legitimate approaches it: an honest peer answers the replies this node asks
// for, and this node asks for one per session setup plus one per recovery pass.
const contactVerifiesPerSecond = 256

// contactVerifyBurst is the bucket depth, and it is deliberately EQUAL to
// maxContactsPerResponse rather than a multiple of the rate.
//
// The two numbers have to agree: a peer this node has not synced with recently
// meets a full bucket and immediately runs one full contact sync, so a burst
// smaller than the reply the count cap admits would leave an honest
// maximum-size reply half-verified — silently losing contacts instead of
// refusing them. Equal means "one reply at the cap always fits, a second one
// within the refill window does not".
//
// Sixteen seconds to refill completely (burst / rate) is longer than the
// four-second horizon of the byte and frame buckets, and that is intentional:
// demand here is a rare batch, not a stream, so the bucket is sized by the
// largest single legitimate demand rather than by a sustained rate.
const contactVerifyBurst = maxContactsPerResponse

// maxTrackedContactVerifyRemotes caps how many remotes the registry remembers.
//
// A map keyed by peer attribution is itself a DoS surface, so it is bounded and
// the bound is generous relative to reality: a node holds tens of sessions and
// a recovery pass touches at most 1 + senderKeySyncFanout of them, so 1024
// remembered remotes is two orders of magnitude above the working set and still
// only ~100 KB. What the cap must NOT do is hand a reset back: a bucket leaves
// the registry only when leaving forgives nothing (releaseRefilledLocked), and
// what does not fit shares the tail bucket (tailBucketLocked) rather than
// displacing a debtor.
const maxTrackedContactVerifyRemotes = 1024

// contactVerifyIPv6PrefixBits is the boundary an IPv6 endpoint is aggregated to
// before it becomes a budget key — see contactVerifyIPKey for why a single IPv6
// address is not an attribution at all.
const contactVerifyIPv6PrefixBits = 64

// contactVerifySaturationWarnInterval rate-limits the saturated-registry warn to
// one line per interval, each carrying the number of tail charges it speaks for.
// It matches fireAndForgetDropWarnInterval, the other per-event warn in this
// package that fires under sustained pressure.
const contactVerifySaturationWarnInterval = 30 * time.Second

// contactVerifyKey is the attribution one budget is charged to: the remote
// endpoint's canonical IPv4 address, its IPv6 /64, or its overlay name — with a
// transport-address fallback for the paths that have no live socket (unit
// fixtures, a wrapper that has not published its endpoint yet).
//
// It is a named type rather than a bare string so the compiler separates it
// from the many other string-shaped keys in this package (CLAUDE.md: domain
// typing), and because the KIND of the key is part of it: the prefix keeps an
// address-shaped fallback from ever colliding with an IP-shaped key.
type contactVerifyKey string

// contactVerifyKeyUnattributed is the single bucket every import whose remote
// cannot be attributed at all shares.
//
// One shared bucket, not one per caller: an unattributable import must not be
// able to multiply itself by being unattributable in a new way. It is a real
// budget, so such imports still work, they just cannot exceed the allowance of
// a single remote between them.
const contactVerifyKeyUnattributed contactVerifyKey = "unattributed"

// contactVerifyKeyFromEndpoint builds the key of a connection.
//
// remoteAddr is the transport endpoint as the connection wrapper reports it
// (*netcore.NetCore.RemoteAddr()); dialed is the peer address this node aimed
// at. The dialed address is bounded input — it comes from this node's own peer
// table, never off the wire of the connection being metered — so it cannot
// inflate the key space.
//
// The OVERLAY case is checked first and deliberately ignores the socket. A
// .onion / .b32.i2p peer is reached through the local SOCKS proxy (dialPeer),
// so every overlay peer shares one transport endpoint: keying those on the
// socket would put the whole overlay in one bucket, which is both a denial of
// service against honest overlay peers and a free ride for hostile ones. The
// overlay name is also the STRONGER attribution of the two — a v3 onion name IS
// the service's public key, and the circuit terminates only at the holder of
// the matching private key — so this branch is the one place on an outbound
// dial where the remote is cryptographically pinned.
func contactVerifyKeyFromEndpoint(remoteAddr string, dialed domain.PeerAddress) contactVerifyKey {
	host := strings.TrimSpace(string(dialed))
	if hostOnly, _, ok := splitHostPort(host); ok {
		host = hostOnly
	}
	if host != "" && (isOnionAddress(host) || isI2PAddress(host)) {
		return contactVerifyKey("overlay:" + strings.ToLower(host))
	}
	if ip := contactVerifyIPKey(remoteIPFromString(remoteAddr)); ip != "" {
		return ip
	}
	if host == "" {
		return contactVerifyKeyUnattributed
	}
	if ip := contactVerifyIPKey(host); ip != "" {
		return ip
	}
	// A DNS-named bootstrap peer whose socket has not reported an endpoint yet:
	// the name is the most stable attribution left, lower-cased so case variants
	// of one name cannot buy two budgets.
	return contactVerifyKey("host:" + strings.ToLower(host))
}

// contactVerifyIPKey turns a host into the IP-shaped budget key, or "" when the
// host is not an IP literal.
//
// # IPv4 is per address, IPv6 is per /64
//
// The key has to make MINTING AN ENDPOINT COST SOMETHING, because the registry
// is bounded and everything that does not fit shares the tail bucket. For IPv4
// an address is a real allocation, so per-address is the right unit and it is
// also what this node's ban surface uses.
//
// IPv6 is the opposite: the customary customer assignment is a /64, and inside
// it an operator has 2^64 addresses that cost nothing. Keyed per address, one
// allocation would buy an unbounded number of budgets — the registry cap would
// be the only thing standing between an attacker and a fresh burst per address,
// which is exactly the pressure the tail bucket must not be under. Aggregating
// to the /64 makes an IPv6 endpoint cost what an IPv4 endpoint costs: a new
// allocation.
//
// What it costs honest peers: two nodes that genuinely share a /64 — the usual
// case being two machines on one home or one hosting subnet — share one bucket,
// the same collateral IPv4 nodes behind one NAT address already accept. A /64 is
// the narrowest boundary that has this property; anything wider (a /48, a /32)
// would merge unrelated customers of one ISP, and anything narrower is free to
// mint.
func contactVerifyIPKey(host string) contactVerifyKey {
	canonical := canonicalIPFromHost(host)
	if canonical == "" {
		return ""
	}
	ip := net.ParseIP(canonical)
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return contactVerifyKey("ip:" + v4.String())
	}
	prefix := ip.Mask(net.CIDRMask(contactVerifyIPv6PrefixBits, 8*net.IPv6len))
	if prefix == nil {
		return contactVerifyKey("ip:" + canonical)
	}
	return contactVerifyKey(fmt.Sprintf("ip6:%s/%d", prefix.String(), contactVerifyIPv6PrefixBits))
}

// sessionContactVerifyKey builds the key of an established outbound session.
//
// It resolves through the SAME endpoint rule as the fresh dial on purpose: the
// two import paths reach one neighbour over one wire, and a key that differed
// between them would let a peer alternate paths to hold two budgets.
func sessionContactVerifyKey(session *peerSession) contactVerifyKey {
	if session == nil {
		return contactVerifyKeyUnattributed
	}
	remoteAddr := ""
	if session.netCore != nil {
		remoteAddr = session.netCore.RemoteAddr()
	}
	return contactVerifyKeyFromEndpoint(remoteAddr, session.address)
}

// contactVerifyBucket is one remote's token bucket.
//
// Tokens are float64 because the refill is continuous: an integer bucket
// refilled once per charge would round every sub-token refill to zero and turn
// a steady trickle into a permanent block.
type contactVerifyBucket struct {
	lastRefill time.Time
	lastCharge time.Time
	tokens     float64
}

// contactVerifyRegistry is the node-scoped set of per-remote buckets.
//
// # Locking
//
// mu is the registry's OWN lock and is deliberately not one of the seven domain
// mutexes (docs/locking.md). It is a leaf: it guards nothing but arithmetic and
// a bounded map, is never held across I/O or a callback, and adds no edge to
// the canonical order.
//
// # Zero value
//
// The zero value is a live registry: the map is built on first use and a nil
// clock selects the wall clock. That is what lets Service hold it BY VALUE, so
// no construction site — production or the many struct-literal test services —
// can forget to build one, and a missing registry can never silently mean "this
// remote has no budget".
type contactVerifyRegistry struct {
	mu sync.Mutex

	// clock is the injectable time source; a nil pointer selects the wall clock.
	// Tests install one through setClock (CLAUDE.md: time that drives a decision
	// is injected, never read from the wall in business logic).
	//
	// It is an ATOMIC POINTER and not a plain field under mu for one reason: the
	// clock is a CALLBACK, and a callback invoked while mu is held makes the lock
	// accidentally re-entrant for whatever that callback touches. Reading it
	// atomically lets every charge resolve `now` BEFORE it takes the lock, which
	// is the same shape the datagram plane's PeerAdmission uses.
	clock atomic.Pointer[contactVerifyClock]

	remotes map[contactVerifyKey]*contactVerifyBucket

	// tail is the ONE bucket every endpoint shares while the registry is full and
	// nothing in it can be released for free. See tailBucketLocked.
	tail        contactVerifyBucket
	tailStarted bool

	// tailChargesSinceWarn / lastTailWarnAt back the rate-limited saturation
	// warn. The decision to emit is taken here, under mu; the log call itself
	// happens in the caller, after the unlock.
	tailChargesSinceWarn int
	lastTailWarnAt       time.Time
}

// contactVerifyClock is the injectable time source. It is a named type because
// atomic.Pointer needs one to point at.
type contactVerifyClock func() time.Time

// contactVerifySaturationWarn carries the facts of a tail-bucket charge OUT of
// the locked section so the log line can be emitted after the unlock.
//
// A non-nil value means "log this now": the rate limiter has already decided,
// under mu, that this is the line that gets to speak for the interval.
type contactVerifySaturationWarn struct {
	key     contactVerifyKey
	charges int
	granted bool
}

// setClock installs the time source. Production leaves it unset.
func (r *contactVerifyRegistry) setClock(clock func() time.Time) {
	typed := contactVerifyClock(clock)
	r.clock.Store(&typed)
}

// now reads the clock. It is deliberately NOT a *Locked helper: the clock is a
// callback and is never invoked while mu is held.
func (r *contactVerifyRegistry) now() time.Time {
	if clock := r.clock.Load(); clock != nil && *clock != nil {
		return (*clock)()
	}
	return time.Now()
}

// trackedRemotes reports how many per-remote buckets are live. It exists for the
// memory bound's test and for diagnostics.
func (r *contactVerifyRegistry) trackedRemotes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.remotes)
}

// charge takes one token for one signature check and reports whether the check
// may happen. A false answer means the caller must skip the entry WITHOUT
// verifying it.
//
// The second return value is non-nil only when this charge landed in the tail
// bucket AND the rate limiter elected it to carry the saturation warn; the
// caller logs it after this function has returned, so nothing on this path holds
// mu across a log sink.
//
// The strict `< 1` (rather than the "spend while non-empty, then owe" rule the
// byte buckets use) is right here because the price is a known constant: there
// is no oversize unit that an almost-empty bucket could cut in half.
func (r *contactVerifyRegistry) charge(key contactVerifyKey) (bool, *contactVerifySaturationWarn) {
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	bucket, tail := r.bucketLocked(key, now)
	r.refillLocked(bucket, now)
	granted := bucket.tokens >= 1
	if granted {
		bucket.tokens--
		bucket.lastCharge = now
	}
	if !tail {
		return granted, nil
	}
	return granted, r.noteTailChargeLocked(key, granted, now)
}

// bucketLocked returns the bucket this key charges and whether that bucket is
// the shared tail.
//
// A remote that is not in the map starts with a FULL bucket, because this is a
// rate limit and not an admission fee: the first contact sync with a peer this
// node has never metered must not be refused. That is precisely why a bucket may
// only be dropped when dropping it forgives nothing — and why, when nothing can
// be dropped on those terms, the newcomer gets the tail instead of a new bucket.
func (r *contactVerifyRegistry) bucketLocked(key contactVerifyKey, now time.Time) (*contactVerifyBucket, bool) {
	if bucket, known := r.remotes[key]; known {
		return bucket, false
	}
	if r.remotes == nil {
		r.remotes = make(map[contactVerifyKey]*contactVerifyBucket, 16)
	}
	if len(r.remotes) >= maxTrackedContactVerifyRemotes {
		r.releaseRefilledLocked(now)
	}
	if len(r.remotes) >= maxTrackedContactVerifyRemotes {
		return r.tailBucketLocked(now), true
	}
	bucket := &contactVerifyBucket{lastRefill: now, lastCharge: now, tokens: contactVerifyBurst}
	r.remotes[key] = bucket
	return bucket, false
}

// releaseRefilledLocked drops every bucket that has refilled completely.
//
// This is the ONLY way a bucket leaves the registry, and the condition is the
// whole safety argument: a full bucket is byte-for-byte identical to the one a
// newcomer would be given, so forgetting it forgives nothing. A bucket in debt is
// never dropped — the round-8 policy of dropping "the most replenished debtor"
// still forgave that debtor's remaining debt, and an attacker able to cycle
// endpoints (2^64 of them inside one IPv6 assignment, before that key started
// aggregating to the /64) could collect that forgiveness once per cycle.
//
// A full bucket is also, by construction, an idle one: tokens only reach the
// burst after burst/rate seconds without a charge, so this doubles as the idle
// sweep and no separate retention clock is needed.
func (r *contactVerifyRegistry) releaseRefilledLocked(now time.Time) {
	for key, bucket := range r.remotes {
		r.refillLocked(bucket, now)
		if bucket.tokens >= contactVerifyBurst {
			delete(r.remotes, key)
		}
	}
}

// tailBucketLocked returns the ONE bucket shared by every endpoint that does not
// fit in a saturated registry.
//
// # Why a shared bucket and not a refusal, and not an eviction
//
// Three answers were possible for "the registry is full and every bucket in it
// owes tokens":
//
//   - EVICT one anyway (the round-8 behaviour). Rejected: the newcomer gets a
//     full bucket, so cycling more endpoints than the registry holds is a fresh
//     burst per endpoint — the memory bound traded for the budget;
//   - REFUSE outright. Rejected: it converts a memory bound into a denial of
//     service against honest peers. An attacker who can saturate the registry
//     would decide that no new neighbour may ever sync contacts, which is worse
//     than what it defends;
//   - SHARE one bucket. Chosen: everything that does not fit is metered
//     together, so the tail of the world collectively costs what a single
//     neighbour costs. An honest peer arriving during saturation is degraded —
//     it competes with the attacker for one budget — but never locked out, and
//     it gets its own bucket the moment any tracked bucket refills.
//
// # Why the tail's rate is one remote's rate
//
// The tail is not a neighbour, it is "everything else", so the number cannot be
// derived from a neighbour's demand. It is set to one remote's rate and burst
// because that makes the statement checkable and small: WHATEVER the tail
// contains, it never costs more than one more neighbour. Making it larger would
// price the exceptional state above the normal one; making it smaller would not
// buy anything an attacker cares about, while cutting an honest newcomer's first
// sync below one reply.
//
// # Why saturation is a transient state and not an operating mode
//
// Reaching it requires maxTrackedContactVerifyRemotes distinct endpoints to be
// in debt AT THE SAME MOMENT, and debt decays at contactVerifiesPerSecond per
// bucket. Since a bucket only goes into debt by answering a contacts reply this
// node asked for — and the recovery passes that ask are capped at
// maxConcurrentSenderKeySyncPasses (3) with at most 1+senderKeySyncFanout
// endpoints each — a node cannot even be made to spend that much verification
// work in one refill window. The tail is a backstop, and the saturation warn
// exists so that an operator sees it if the reasoning above is ever wrong.
func (r *contactVerifyRegistry) tailBucketLocked(now time.Time) *contactVerifyBucket {
	if !r.tailStarted {
		r.tailStarted = true
		r.tail = contactVerifyBucket{lastRefill: now, lastCharge: now, tokens: contactVerifyBurst}
	}
	return &r.tail
}

// noteTailChargeLocked counts one tail charge and decides whether this one gets
// to carry the saturation warn. Caller holds mu; the returned value is logged
// after the unlock.
//
// It is rate-limited on the pattern this package already uses for a warn that
// fires per event under sustained pressure (logFireAndForgetDrop): one line per
// interval, carrying the number of charges it speaks for. Without it the line
// would fire once per signature check — up to contactVerifiesPerSecond per
// second — which is the shape of log flood that made the drop warn expensive
// enough to be worth aggregating in the first place.
func (r *contactVerifyRegistry) noteTailChargeLocked(key contactVerifyKey, granted bool, now time.Time) *contactVerifySaturationWarn {
	r.tailChargesSinceWarn++
	if !r.lastTailWarnAt.IsZero() && now.Sub(r.lastTailWarnAt) < contactVerifySaturationWarnInterval {
		return nil
	}
	warn := &contactVerifySaturationWarn{key: key, charges: r.tailChargesSinceWarn, granted: granted}
	r.lastTailWarnAt = now
	r.tailChargesSinceWarn = 0
	return warn
}

// refillLocked advances a bucket to now. Caller holds mu.
func (r *contactVerifyRegistry) refillLocked(bucket *contactVerifyBucket, now time.Time) {
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed <= 0 {
		// A backwards or stalled clock refills nothing, and lastRefill is left
		// where it was: advancing it anyway would let a repeated backwards step
		// erase the time the bucket is owed.
		return
	}
	bucket.lastRefill = now
	bucket.tokens += elapsed * contactVerifiesPerSecond
	if bucket.tokens > contactVerifyBurst {
		bucket.tokens = contactVerifyBurst
	}
}

// contactVerifyGrant is the per-import handle on a remote's shared bucket.
//
// It holds the KEY and not the bucket: the bucket may be evicted between two
// replies of one import, and a handle that pinned the pointer would keep
// charging a bucket the registry no longer accounts for.
type contactVerifyGrant struct {
	registry *contactVerifyRegistry
	key      contactVerifyKey
}

// ChargeContactVerify implements contactVerificationBudget.
//
// The log call lives HERE and not inside the registry: charge decides what would
// have to be said while it holds mu, and this is the first place that no longer
// holds it. A log sink is I/O, and I/O under the registry's lock would stall
// contact verification for every remote on the node behind whatever the sink is
// waiting for (CLAUDE.md: no I/O under a mutex).
func (g contactVerifyGrant) ChargeContactVerify() bool {
	granted, warn := g.registry.charge(g.key)
	if warn != nil {
		log.Warn().
			Str("remote", string(warn.key)).
			Int("tracked", maxTrackedContactVerifyRemotes).
			Int("tail_charges", warn.charges).
			Bool("granted", warn.granted).
			Msg("contact_verify_registry_saturated")
	}
	return granted
}

// contactVerifyBudgetFor returns the budget every contact import of one remote
// shares, whichever path imports it and however many connections it takes.
func (s *Service) contactVerifyBudgetFor(key contactVerifyKey) contactVerificationBudget {
	return contactVerifyGrant{registry: &s.contactVerifyBudgets, key: key}
}
