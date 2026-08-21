package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
	"github.com/piratecash/corsa/internal/core/transport"
)

// errIncompatibleProtocol is returned when a peer's protocol version is
// below MinimumProtocolVersion. The retry loop treats this as a permanent
// failure and stops reconnection attempts — the peer must upgrade first.
var errIncompatibleProtocol = errors.New("incompatible protocol version")

// StoreResult describes the outcome of MessageStore.StoreMessage.
// Three distinct states prevent conflating "already seen" with "write error",
// allowing the caller to make the right decision for each case.
type StoreResult int

const (
	// StoreInserted means the message was genuinely new and persisted.
	StoreInserted StoreResult = iota
	// StoreDuplicate means the message already existed in the durable store.
	// The caller should NOT add it to s.topics or emit UI events.
	StoreDuplicate
	// StoreFailed means a write error occurred.  The message was NOT persisted
	// but the caller should still keep it in-memory (s.topics) so it is not
	// lost from the network.
	StoreFailed
	// StoreDeferred means the store cannot decide yet — not that the write
	// failed. The message is NOT persisted and must NOT be treated as
	// received: it stays out of s.topics and out of the dedup mark, no
	// delivery receipt is sent, and the frame is not acked, so the SENDER
	// keeps it and tries again. Anything else silently loses a message the
	// user was never shown.
	//
	// The one producer today is the refusal gate on deleted ids: when the
	// refusals cannot be read, storing on a guess is how a row the user
	// deleted comes back for good.
	StoreDeferred
)

// MessageStore allows the desktop layer to own message persistence, while
// relay-only nodes (corsa-node) leave this nil to skip local storage.
type MessageStore interface {
	StoreMessage(envelope protocol.Envelope, isOutgoing bool) StoreResult
	UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool
}

type Service struct {
	identity *identity.Identity
	// selfBoxKey / selfBoxSig are the box key (base64) and its identity-
	// binding signature this node sends in hello/welcome frames. They are
	// ALWAYS populated — including under cfg.DisableDirectMessages —
	// because deployed peers gate session-auth challenge issuance on all
	// four identity fields (connauth.HasIdentityFields); omitting the box
	// key would route this node's hello into the unauthenticated branch
	// and break every outbound authenticated session. The relay-only DM
	// opt-out therefore suppresses the key only on the CONTACT plane
	// (trust-store self row → s.boxKeys → fetch_contacts, see NewService);
	// direct peers still learn the key from the handshake, and DMs
	// encrypted with such a cached key are dropped by the inbound gate in
	// storeIncomingMessage.
	selfBoxKey string
	selfBoxSig string
	cfg        config.Node
	// externalListenCached memoises externalListenAddress(). cfg is immutable
	// after construction, but isSelfAddress (and thus externalListenAddress) is
	// called per candidate in every routing/gossip target-selection loop, and
	// the ":port" wildcard branch synthesises "127.0.0.1"+ListenAddress on each
	// call — profiling showed hundreds of millions of these one-off string
	// allocations. Computed lazily once and stored as an atomic pointer so the
	// hot path is an atomic load + deref with no allocation; lazy (not set in
	// NewService) so the many struct-literal *Service test fixtures that bypass
	// NewService still get the correct value from their cfg.
	externalListenCached atomic.Pointer[string]
	eventBus             *ebus.Bus
	trust                *trustStore
	// identityResolver is the identity lookup engine (identity_resolver.go).
	// Immutable after NewService; all mutable state lives behind the
	// resolver's own mutex, so the field sits outside the domain-mutex
	// scheme (docs/locking.md "fields outside").
	identityResolver *identityResolver
	// identityFileMu serialises the identity_backup / identity_restore
	// RPC pair: both funnel secrets through one predictable <path>.tmp
	// (identity.Save for restore, writeSecretFile for backup), so two
	// concurrent calls could delete or rename each other's temp file and
	// acknowledge content another call actually wrote. A leaf mutex
	// outside the domain scheme (docs/locking.md "fields outside"): held
	// only inside the two frame handlers around file I/O, takes no domain
	// mutex and no domain mutex ever takes it.
	identityFileMu sync.Mutex
	// trustMutationMu serialises the {trust-store mutation, pin update}
	// PAIRS in trustContact (remember → Pin) and deleteTrustedContactFrame
	// (forget → Unpin). The two halves live in different lock domains
	// (trustStore.mu vs knowledgeMu), so without this outer mutex a
	// concurrent import could re-remember + re-Pin a contact between a
	// delete's forget and its Unpin, ending with a TRUSTED contact whose
	// key knowledge is evictable. Lock order: trustMutationMu →
	// trustStore.mu / knowledgeMu; nothing acquires it while holding
	// either inner lock. Held across trust-store file I/O — acceptable,
	// both operations are rare user/sync actions.
	trustMutationMu sync.Mutex
	// startedAt is the wall-clock time NewService was called, captured
	// once and read without a lock from the NodeStatus RPC handler. It
	// powers the uptime_seconds field of the PIP-0001 integration
	// surface (see docs/rpc/system.md → /rpc/v1/system/node_status).
	// Immutable after construction — no synchronisation required.
	startedAt time.Time

	// datagramMetrics counts every decision the datagram ingress makes
	// (docs/refactoring/datagram-transport.md §10). Deliberately OUTSIDE
	// the seven-domain mutex scheme: the pointer is assigned once by
	// NewService and never replaced, and datagram.Metrics is a set of
	// atomic counters with its own synchronisation — taking a domain mutex
	// to bump a counter on the receive path would put the whole plane
	// behind peerMu for nothing. Every method is nil-safe, so the many test
	// fixtures that build a Service by struct literal keep working. When
	// the pipeline lands it receives THIS pointer, so the wire-level
	// refusals counted here and the pipeline's own verdicts stay one
	// series. See docs/locking.md.
	datagramMetrics *datagram.Metrics

	// datagramLayer is the assembled datagram transport plane: the conveyor
	// of §4.1, the scheduler of §4.3, the §5 budgets and queues, and the
	// components the node drives on a schedule (datagram_layer.go). nil
	// whenever cfg.EnableDatagramV1 is false, which is the whole feature
	// flag: every path that touches the plane is one nil check away from the
	// pre-datagram behaviour.
	//
	// OUTSIDE the seven-domain mutex scheme, for the same reason as
	// datagramMetrics above: every component behind it carries its own
	// synchronisation (the replay cache's mutex, the reverse table's mutex, the
	// queue's mutex, the type registry's atomic snapshot), so no domain mutex is
	// taken to reach it and it adds no edge to the canonical lock order. See
	// docs/locking.md.
	//
	// It is an atomic pointer rather than a plain field because the receive
	// path reads it concurrently with the single store NewService performs, and
	// because the handle it publishes is immutable: a reader takes it once
	// through datagramLayer() and works off that snapshot for the whole of its
	// decision.
	datagramPlane atomic.Pointer[datagramLayer]

	// faultDuringRunStartup is a TEST-ONLY fault injector, nil in production,
	// called once per Run at the LATEST point of its startup: every subsystem
	// defer is registered, the ordered lifecycle teardown is armed and every
	// loop is running.
	//
	// It exists because the shutdown ORDER is only observable by unwinding
	// through it, and nothing in Run's own startup panics on demand — every step
	// there is driven by constants or by node state, not by an injectable
	// dependency. A panic raised here must still join the lifecycle loops BEFORE
	// any subsystem they call into is stopped, which is what
	// TestAPanicDuringRunStartupStillJoinsTheLifecycleLoopsFirst pins.
	faultDuringRunStartup func()

	// heartbeatIntervalOverride replaces the inbound heartbeat's production
	// schedule. Zero means nextHeartbeatDuration(); it exists so a test can
	// enter the window where the heartbeat is INSIDE its ping send, which is
	// the state the connection's join was added for.
	heartbeatIntervalOverride time.Duration

	// sendAdmissionBarrier is a TEST-ONLY synchronisation point, nil in
	// production, run by BOTH send tiers between the peer-state gate and the
	// hand-over to a queue (runSendAdmissionBarrier, peer_management.go). One
	// hook rather than one per tier because the two tiers defend the same
	// window and a seam that only one of them has is a window only one of them
	// is ever tested in.
	//
	// It exists because that window lives entirely between two statements of
	// one function and has no observable edge from outside: a test that
	// approximated it with a sleep would pin the scheduler rather than the
	// teardown order. Same shape and same reason as
	// netcore.NetCore.enqueueBarrier and datagramFrameEmitter.selectionBarrier.
	//
	// It is HALF of an interleaving and proves nothing on its own. Parking the
	// producer here says only "a teardown may now run"; it does not say WHERE in
	// that teardown the producer resumes, so a teardown driven from inside this
	// barrier runs to completion and shows the producer the same world whichever
	// order its two publications are written in. The other half —
	// peerTeardownBarrier below — is what places the resume point between them.
	//
	// Installed before the first send on this Service and never changed
	// afterwards, so it takes part in no domain and adds no edge to the
	// canonical lock order.
	sendAdmissionBarrier func()

	// retryDispatchBarrier is a TEST-ONLY synchronisation point, nil in
	// production, run by retryDueDeliveries between its planning phases
	// and the dispatch loop (runRetryDispatchBarrier, delivery_retry.go).
	//
	// It exists because a deletion's freeze landing in exactly that window
	// is what the last-boundary claim defends against, and the window is
	// two statements of one function with no observable edge from outside:
	// a test that froze before the tick proves only that a freeze taken
	// EARLIER is honoured, which is the easy half.
	//
	// Installed before the first tick on this Service and never changed
	// afterwards, so it takes part in no domain and adds no edge to the
	// canonical lock order. It is fired with no mutex held.
	retryDispatchBarrier func()

	// peerTeardownBarrier is the TEARDOWN half of the same window, nil in
	// production, run by BOTH teardowns that own a queue (retirePeerSession on
	// the dialled tier, trackInboundDisconnect on the accepted one) BETWEEN
	// their two publications — after the queue was fenced, before the peer is
	// published as disconnected (runPeerTeardownBarrier, peer_management.go).
	// One hook for both tiers, for the same reason sendAdmissionBarrier is one:
	// the two tiers defend the same invariant, and a seam only one of them has
	// is an invariant only one of them is ever tested for.
	//
	// It exists because the ORDER of those two publications is what the
	// invariant is, and a teardown that runs to completion inside the producer's
	// barrier makes that order unobservable: whichever way round the two
	// statements are written, the producer resumes after both have happened and
	// sees the same world. Parking the teardown HERE — in its own goroutine,
	// between its own two statements — is what turns the order into an
	// observable: at this point the correct order has the queue already fenced
	// and the disconnect not yet published, and the reversed order has the
	// disconnect published and the queue still accepting.
	//
	// It is called with NO domain mutex and no leaf mutex held, at both sites.
	// That is a requirement rather than an accident: the producer this seam
	// releases goes on to take peerMu for its next candidate's gate, so a
	// barrier fired under a lock would park the teardown on top of it and
	// deadlock the very interleaving it exists to produce.
	//
	// Installed before the first teardown on this Service and never changed
	// afterwards, so it takes part in no domain and adds no edge to the
	// canonical lock order.
	peerTeardownBarrier func()

	// runLoopsWg tracks EVERY loop Run starts that stops on the lifecycle
	// context — the ConnectionManager event loop, bootstrapLoop,
	// hotReadsRefreshLoop, the announce loop, the routing TTL ticker, the probe
	// sender, the listener closer, the gossip dispatch pool AND the datagram
	// plane's four schedules. A wait group rather than done-channels because
	// a group is correct for a loop that was NEVER STARTED — a panic during
	// startup leaves the counter where it was, while a channel nobody will ever
	// close makes the teardown wait for ever. That difference is what lets the
	// whole lifecycle teardown be armed once, before any of them exists.
	runLoopsWg sync.WaitGroup

	// relayDeliveredTo records, per message id, the peer identities that
	// confirmed (via ack_delete) they already hold the message, so the
	// gossip fan-out stops re-sending it to them (relay_delivered.go,
	// Phase 2). Deliberately OUTSIDE the domain-mutex scheme: it has its
	// OWN leaf mutex (relayDeliveredMu) that is only ever taken alone or
	// as the innermost lock (snapshotted before peerMu), never while
	// acquiring a domain mutex — so it adds no edge to the canonical lock
	// order. See docs/locking.md.
	relayDeliveredMu sync.Mutex
	relayDeliveredTo map[protocol.MessageID]map[domain.PeerIdentity]struct{}

	peerMu sync.RWMutex
	peers  []transport.Peer // dial candidates (typed: Address + Source)

	// deliveryMu guards the "message-delivery" domain: the per-recipient
	// pending queues, outbound delivery state, relay retry bookkeeping,
	// delivery receipt store, transit-receipt dedup set, and the set of
	// upstream peers that currently subscribe to our outbox.  Separate from
	// peerMu so message-delivery writers (storeIncomingMessage, storeOutgoing
	// sync, relay retry loop, receipt ingest, drainPendingForIdentities) no
	// longer contend with peer-state writers that live under peerMu.
	//
	// Lock ordering (see docs/locking.md):
	//   peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
	// peerMu OUTER, deliveryMu INNER whenever both are needed.  Peer-domain
	// + delivery-domain mixes (queuePeerFrame, dropPendingForRecipient,
	// evictOrphanedHealthEntries, drainPendingForIdentities,
	// peerDialCandidates, onCMSession*, flushPendingPeerFrames,
	// rebuildPeerHealthSnapshot) take peerMu first, then deliveryMu.
	// Delivery + status aggregate recomputes (refreshAggregateStatus*,
	// computeAggregateStatusLocked) take the full stack peerMu →
	// deliveryMu → statusMu with statusMu INNERMOST.
	//
	// Cross-domain examples that additionally touch gossipMu take the
	// canonical peerMu → deliveryMu → gossipMu order: retryableRelayMessages
	// and deleteBacklogMessageForRecipient hold deliveryMu OUTER and
	// gossipMu INNER.
	//
	// Reverse orders (deliveryMu first, then re-entering peerMu while still
	// holding delivery state; or delivery callers taking gossipMu before
	// deliveryMu) are forbidden.
	//
	// Fields owned by this mutex:
	//   • pending       — recipient → queued envelopes awaiting delivery
	//   • pendingKeys   — envelope-id dedup set for pending frames
	//   • outbound      — envelope-id → last-known outbound delivery state
	//                     (queued/retrying/terminal) for UI + sync-out
	//   • relayRetry    — envelope-id → relay retry bookkeeping (attempts,
	//                     next-try timestamp, peer set)
	//   • awaitingDelivered — locally-sent DMs awaiting their end-to-end
	//                     delivered/seen receipt (delivery_retry.go)
	//   • awaitingSeenAck — locally-sent seen receipts awaiting the original
	//                     sender's seen_ack (delivery_retry.go)
	//   • seenReceipts  — transit-receipt dedup set, bounded sliding window
	//                     (rotatingHashDedup, receipt_dedup.go) — self-
	//                     synchronised, but still a deliveryMu-domain field
	//   • receipts      — recipient → persisted DeliveryReceipt batches
	//   • upstream      — peer address → upstream-subscription marker; set of
	//                     peers that have subscribed to our outbox
	deliveryMu  sync.RWMutex
	pending     map[domain.PeerAddress][]pendingFrame
	pendingKeys map[pendingKey]struct{}
	outbound    map[string]outboundDelivery
	// lastOutboundTerminalSweep throttles sweepTerminalOutbound (guarded
	// by deliveryMu like the map it sweeps).
	lastOutboundTerminalSweep time.Time
	relayRetry                map[string]relayAttempt
	awaitingDelivered         map[protocol.MessageID]*deliveryRetryEntry
	// cancelledDeliveries remembers ids whose delivery the sender
	// withdrew, so a backlog push that took its snapshot a moment earlier
	// does not hand the envelope over anyway. Guarded by deliveryMu, like
	// the retry state it shadows.
	//
	// The snapshot and the emission mark cannot be taken in one lock hold
	// — building the inbox frame reads the gossip domain and then the
	// delivery domain, in that order, and the canonical order runs the
	// other way — so a cancellation can land between them. Without this,
	// that cancellation reports "never emitted", the router recalls the
	// message and schedules nothing, and the snapshot then sends it.
	//
	// Entries are pruned by age on write; the window that matters is the
	// life of one backlog push.
	cancelledDeliveries map[protocol.MessageID]time.Time
	// frozenDeliveries are ids a conversation wipe is deciding about. No
	// path may put them on the wire while they are here, and unlike
	// cancelledDeliveries they never expire: the freeze ends when the
	// wipe commits (the cancellation withdraws them) or aborts (a thaw
	// puts them back), and nothing else. See delivery_freeze.go.
	frozenDeliveries map[protocol.MessageID]struct{}
	// markedNeverEmitted are the ids this process has a durable
	// never-emitted claim standing for. It exists because the backlog
	// replay reaches past the retry engine: it can emit a message whose
	// awaitingDelivered entry was dropped when the attempts ran out, and
	// without this the claim would stay on the row while the peer holds
	// the message. Rebuilt implicitly after a restart — the reseed makes
	// entries out of the same marks. See delivery_retry.go.
	markedNeverEmitted map[protocol.MessageID]struct{}
	awaitingSeenAck    map[protocol.MessageID]*seenAckRetryEntry
	// sentDMIDs is a bounded LRU of message IDs this node has originated as
	// DMs (populated when registering the end-to-end delivery retry). It is
	// the "did we send this?" signal storeDeliveryReceipt uses to reject
	// UNSOLICITED delivered/seen receipts addressed to our own identity, so an
	// authenticated peer cannot flood s.receipts with receipts for phantom
	// message IDs. Survives the delivered→seen transition (unlike
	// awaitingDelivered, which is deleted on the first receipt) because it is
	// only ever evicted by LRU capacity, never on receipt.
	sentDMIDs *boundedKnownIdentities
	// seenAckJournal / deliveryFailureJournal are the optional durable
	// journals behind awaitingSeenAck and the retry-abandonment path
	// (delivery_retry.go). Set once by RegisterDeliveryOutbox before Run —
	// like messageStore they are immutable afterwards, so reads need no
	// mutex. Their SQLite I/O never runs under a domain mutex; the
	// failure-journal write is synchronous (it is the durable boundary —
	// a background hop would race shutdown, which does not wait for
	// backgroundWg before the chatlog closes), while MarkSeenConfirmed may
	// run on the background pool (losing it only costs one redundant,
	// idempotent seen re-send after restart).
	seenAckJournal         SeenAckJournal
	deliveryFailureJournal DeliveryFailureJournal
	// emissionJournal is the durable half of deliveryRetryEntry.Emitted.
	// Set once by RegisterDeliveryOutbox before Run, like the two above,
	// so reads need no mutex.
	emissionJournal DeliveryEmissionJournal
	// emissionMu serializes decide-then-write on emissionJournal so two
	// goroutines cannot land a mark and a clear for the same message in
	// the wrong order. It is NOT one of the seven domain mutexes: it is
	// acquired OUTSIDE all of them and never while one is held, and it is
	// the only lock held across the journal's SQLite I/O. docs/locking.md
	// carries the contract.
	emissionMu sync.Mutex
	// relayRetryScratch is a reusable buffer for the topics["dm"] snapshot
	// taken on each 2s relay-retry cycle (retryableRelayMessages). Copying the
	// whole topics["dm"] slice into a fresh allocation every cycle was a steady
	// allocation-churn source; the scratch is reused via append(...[:0], ...).
	// Only ever touched by retryableRelayMessages under s.deliveryMu.Lock, and
	// never returned to callers, so reuse is race-free.
	relayRetryScratch []protocol.Envelope
	seenReceipts      *rotatingHashDedup
	receipts          map[string][]protocol.DeliveryReceipt
	upstream          map[domain.PeerAddress]struct{}

	// knowledgeMu guards the "cryptographic-knowledge" domain: the set of
	// identities we have ever heard about and the public keys we have
	// learned for them.  Separate from peerMu so identity-learning writers
	// (handshake, trustContact, fetch_contacts ingest) no longer contend
	// with the peer-state / delivery / gossip paths.
	//
	// Lock ordering (see docs/locking.md):
	//   peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
	//   • peer-domain / delivery-domain callers that also need knowledge
	//     fields take peerMu / deliveryMu OUTER, knowledgeMu INNER.
	//   • gossip-domain callers that also need knowledge fields take
	//     knowledgeMu OUTER, gossipMu INNER (canonical knowledgeMu →
	//     gossipMu ordering).  storeIncomingMessage is the canonical
	//     example: the knowledge section and the gossip section are
	//     sequential, not nested, so the two halves are split across
	//     separate lock windows.
	// Reverse orders (knowledgeMu first, then re-entering a held peerMu or
	// deliveryMu; or taking peerMu/deliveryMu after knowledgeMu while still
	// holding peer-domain state) are forbidden.
	//
	// Fields owned by this mutex:
	//   • known    — set of every identity/address we have ever observed
	//   • boxKeys  — address → ed25519 → curve25519 box key mapping learned
	//                through identity exchange or trust store
	//   • pubKeys  — address → ed25519 signing public key mapping used to
	//                verify non-DM sender authenticity
	//   • boxSigs  — address → boxkey-binding signature that ties boxKey
	//                to the ed25519 identity; stored for re-verification
	//                and for re-gossip to peers that are missing it
	//   • selfRecord / selfRecordBody — the node's OWN signed identity
	//                record (docs/protocol/identity-lookup.md §4.1): the
	//                only artifact this node may answer a get_identity
	//                with and the payload of every push_identity. Issued
	//                once in NewService (after the persist has succeeded —
	//                see self_record.go for the ordering contract) and
	//                re-issued only by the seq-bump paths, all of which
	//                write under this mutex.
	knowledgeMu    sync.RWMutex
	known          *boundedKnownIdentities
	boxKeys        map[string]string
	pubKeys        map[string]string
	boxSigs        map[string]string
	selfRecord     protocol.SignedIdentityRecord
	selfRecordBody protocol.IdentityRecordBody

	// gossipMu guards the "mesh-propagation" domain: the per-topic message
	// backlog, its dedup set, the subscriber fan-out, ephemeral notices,
	// local-change event subscribers, and the throttle timestamp that gates
	// cleanupExpiredMessages.  Separate from peerMu so fetch_messages /
	// fetch_inbox / publish_notice readers no longer contend with peer-state
	// / delivery / status writers.
	//
	// Lock ordering (see docs/locking.md):
	//   peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
	//   • peer-domain / delivery-domain callers that also need gossip
	//     fields take peerMu / deliveryMu OUTER, gossipMu INNER.
	//     retryableRelayMessages and deleteBacklogMessageForRecipient are
	//     the canonical examples — deliveryMu covers relayRetry / receipts,
	//     gossipMu covers the topics["dm"] snapshot.
	//   • knowledge+gossip mixes take knowledgeMu OUTER then gossipMu
	//     INNER — the canonical knowledgeMu → gossipMu order.
	//     storeIncomingMessage expresses the two halves as sequential
	//     windows rather than nested locks.
	// Reverse orders (gossipMu first, then re-entering a held peerMu or
	// deliveryMu while still holding peer/delivery state; or peer/delivery
	// callers taking gossipMu before deliveryMu) are forbidden.
	//
	// Fields owned by this mutex:
	//   • topics              — per-topic append-only message backlog used by
	//                           fetch_messages / fetch_inbox / fetchDMHeaders
	//                           / gossip relay of dm traffic
	//   • seen                — dedup set of message IDs already observed,
	//                           backed by a rotating Bloom filter (Phase 4
	//                           13.4): two filters with a 5-minute rotation
	//                           cap memory regardless of message volume,
	//                           trading the legacy map's perfect recall
	//                           for a <0.1% false-positive rate. Eviction
	//                           window is [5, 10] minutes — see
	//                           bloom_dedup.go for the full contract.
	//   • subs                — recipient → subscriber map for push delivery
	//                           via hello node-routes
	//   • notices             — gazeta notice cache keyed by ciphertext id
	//   • events              — local-change event subscriber channels
	//   • lastExpiredCleanup  — throttle timestamp for cleanupExpiredMessages
	gossipMu sync.RWMutex
	topics   map[string][]protocol.Envelope
	seen     *rotatingBloomDedup
	subs     map[string]map[string]*subscriber
	notices  map[string]gazeta.Notice
	events   map[chan protocol.LocalChangeEvent]struct{}
	// lastExpiredCleanup is the timestamp of the last successful
	// cleanupExpiredMessages run. Used to throttle inline callers
	// (storeIncomingMessage, fetch*) so they skip the expensive full-scan
	// when it ran recently. The bootstrapLoop tick (2s) guarantees a
	// bounded upper bound on stale expired messages.
	// Protected by gossipMu (read and written inside cleanupExpiredMessages).
	lastExpiredCleanup time.Time

	sessions map[domain.PeerAddress]*peerSession
	health   map[domain.PeerAddress]*peerHealth
	// peerTypes / peerIDs / peerVersions / peerBuilds are persistence caches,
	// keyed by overlay address, that intentionally outlive the lifetime of any
	// single net.Conn. They record the last-known self-report from
	// welcome/auth/add_peer so that eviction, gossip, dial scoring, reverse
	// identity→address lookups (routing_provider.PeerTransport) and
	// peer.json reload can resolve state for peers that are not currently
	// connected. They are NOT duplicates of *NetCore state — NetCore owns
	// transport state for the current conn, these maps own persistence.
	//
	// Write-path invariant: every live-conn update that writes to peerIDs
	// MUST also mirror identity/address onto the associated NetCore via
	// SetIdentity/SetAddress in the same section (see handleAuthSession for
	// the inbound mirror; peer_management.go:787–788 / :3307–3308 for the
	// outbound mirror). Address-only callers (no conn available, e.g.
	// peer_exchange) update only the map; by-conn readers must tolerate an
	// empty NetCore.Identity() and fall back to the map.
	peerTypes         map[domain.PeerAddress]domain.NodeType
	peerIDs           map[domain.PeerAddress]domain.PeerIdentity
	peerVersions      map[domain.PeerAddress]string
	peerBuilds        map[domain.PeerAddress]int
	inboundHealthRefs map[domain.PeerAddress]int // resolved overlay address → active inbound connection count
	// metaOrphanFirstSeen tracks when an address keyed in the per-peer
	// metadata maps (peerTypes/peerIDs/peerVersions/peerBuilds/
	// observedIPHistoryByPeer) was FIRST observed orphaned — no s.peers
	// row, no health entry, no inbound refs, no session, no
	// persistedMeta. evictOrphanedPeerMetadata deletes the metadata
	// once an address stays orphaned for a full
	// orphanedHealthEvictWindow; the grace protects entries written
	// during an in-flight handshake (metadata lands before the health
	// row exists). Without this sweep, entries written on pre-auth /
	// failed-handshake paths are keyed by ephemeral ip:port and leak
	// one set per reconnect forever (memory-leak audit, 2026-06).
	// Guarded by peerMu (writer). Lazy-allocated.
	metaOrphanFirstSeen map[domain.PeerAddress]time.Time
	// lastMetaOrphanSweep throttles evictOrphanedPeerMetadata: the
	// bootstrapLoop tick fires every 2 s, but a full union-scan of
	// five metadata maps that often is wasted work — the grace window
	// is orphanedHealthEvictWindow (minutes), so a once-a-minute scan
	// loses nothing. Guarded by peerMu.
	lastMetaOrphanSweep time.Time
	connWg              sync.WaitGroup // tracks active handleConn goroutines for graceful shutdown
	backgroundWg        sync.WaitGroup // tracks fire-and-forget goroutines (receipts, gossip) for clean test shutdown
	// conns is the single source of truth for live connection state (core,
	// metered counters, tracked flag). It is keyed by netcore.ConnID so the
	// key is a domain identifier, not a raw net.Conn handle. All reads and
	// writes go through the helpers in conn_registry.go — nothing outside
	// that file touches this map directly. Lifecycle entry points:
	// registerInboundConnLocked, attachOutboundCoreLocked, unregisterConnLocked.
	conns map[netcore.ConnID]*connEntry
	// connIDByNetConn is a secondary index that lets net.Conn-first helpers
	// resolve their input into the primary ConnID key. It is kept strictly
	// in lock-step with `conns` by the lifecycle helpers in conn_registry.go;
	// the invariant is pinned by TestConnRegistry_InvalidationIsAtomic and
	// TestConnRegistry_RegisterSyncsSecondaryIndex in
	// conn_registry_lifecycle_test.go.
	connIDByNetConn map[net.Conn]netcore.ConnID
	connIDCounter   uint64 // monotonic counter for connection IDs (protected by mu)
	bans            map[string]banEntry
	listener        net.Listener
	lastSync        time.Time
	peersStatePath  string
	lastPeerSave    time.Time
	// peerStateDirty records that persisted peer state changed since the
	// last successful flush. Every mutation outside the periodic catch-all
	// — add_peer, a newly learned address→identity binding, remote-ban record
	// (handlePeerBannedNotice) and remote-ban clear (clearRemoteBansOnAuth) —
	// sets it directly while already under peerMu or via markPeerStateDirty
	// of forcing a synchronous full snapshot+marshal+disk write per event,
	// which during startup bootstrap priming made flushPeerState O(peers^2).
	// maybeSavePeerState coalesces the marked changes into a single debounced
	// flush (peerStateDebounceSeconds). Guarded by peerMu.
	peerStateDirty  bool
	lastPeerEvict   time.Time
	dialOrigin      map[domain.PeerAddress]domain.PeerAddress // dial address → primary peer address (for fallback port tracking)
	persistedMeta   map[domain.PeerAddress]*peerEntry         // stable metadata from peers.json, keyed by address
	observedAddrs   map[domain.PeerIdentity]string            // peer identity (fingerprint) → observed IP they reported for us
	reachableGroups map[domain.NetGroup]struct{}              // network groups this node can reach (computed at startup)
	messageStore    MessageStore                              // optional: persistence handler registered by desktop layer
	router          Router                                    // routing strategy for outbound message delivery

	// gossipJobs feeds the bounded gossip-dispatch worker pool that
	// replaces the historical goroutine-per-target gossip fan-out (see
	// gossip_dispatch.go). nil until Run calls startGossipDispatch;
	// dispatchGossipSend falls back to goBackground while the pool is
	// down so unit tests and partially-wired Services keep the old
	// per-send-goroutine semantics.
	gossipJobs chan func()
	// gossipNoticeJobs is the dedicated lane for push_notice fan-out:
	// notices have no retry path, so they must not share the lossy
	// DM/receipt queue — a DM storm could otherwise silently shed
	// them. See gossip_dispatch.go.
	gossipNoticeJobs chan func()
	// gossipPoolMu excludes job enqueues (RLock) from the shutdown
	// supervisor's flag-flip + queue-drain critical section (Lock), so
	// no closure can be parked in gossipJobs after the drain. NOT in
	// the peerMu/ipStateMu lock hierarchy — leaf lock, nothing else is
	// acquired under it.
	gossipPoolMu sync.RWMutex
	// gossipPoolUp flips once startGossipDispatch has the workers
	// running; read lock-free on every dispatch.
	gossipPoolUp atomic.Bool
	// gossipPoolShutdown flips when the pool's ctx is cancelled: late
	// dispatchers drop their job instead of enqueueing it or falling
	// back to a fresh goroutine. Distinct from gossipPoolUp so the
	// "never started" (tests → goBackground fallback) and "started,
	// now tearing down" (→ drop) states cannot be confused.
	gossipPoolShutdown atomic.Bool
	// gossipSendsDropped counts DM/receipt fan-out jobs shed because
	// the dispatch queue was saturated (fire-and-forget; re-covered by
	// the relay retry cycle). Observability only.
	gossipSendsDropped atomic.Uint64
	// gossipNoticesDropped counts push_notice fan-out jobs shed because
	// the dedicated notice lane overflowed — only plausible under a
	// notice flood. Logged at Warn (notices have no retry path).
	gossipNoticesDropped atomic.Uint64
	// digestStats are cumulative observability counters for the route_sync
	// digest-as-heartbeat exchange (docs/protocol/route_sync.md). They answer,
	// without debug logging, whether periodic heartbeats are actually being
	// confirmed (and thus suppressing full syncs) or diverging. All fields are
	// atomic, so they live outside the seven-domain mutex scheme (own
	// synchronization, per CLAUDE.md): incremented from the network-dispatch
	// and announce-send goroutines, read lock-free by the fetchRouteSummary RPC
	// (surfaced as routing.DigestHeartbeatStats via Service.DigestHeartbeatStats).
	digestStats struct {
		heartbeatsSent  atomic.Uint64 // periodic route_sync_digest_v1 frames we emitted
		summaryMatch    atomic.Uint64 // inbound summaries reporting match=true for our digest
		summaryMismatch atomic.Uint64 // inbound summaries reporting match=false
		digestsCompared atomic.Uint64 // inbound digests we compared as receiver
		compareMatch    atomic.Uint64 // of those, how many matched our via-peer view (→ TTL refresh)
	}
	relayStates                    *relayStateStore                             // hop-by-hop relay forwarding state (Iteration 1)
	relayLimiter                   *relayRateLimiter                            // per-peer token bucket for relay fan-out
	announceLimiter                *announceRateLimiter                         // per-peer token bucket for received announce-plane frames (Phase 4 13.7)
	connLimiter                    *connRateLimiter                             // per-IP connection rate limiter at accept level
	cmdLimiter                     *commandRateLimiter                          // per-connection command rate limiter for non-relay frames
	inboundByIP                    map[string]int                               // IP → active inbound connection count (per-IP cap)
	routingTable                   *routing.Table                               // distance-vector routing table (Phase 1.2)
	announceLoop                   *routing.AnnounceLoop                        // periodic + triggered announce_routes sender (Phase 1.2)
	overloadMonitor                *overloadMonitor                             // CPU/backlog backpressure gate for the announce loop (Phase 0)
	probeRegistry                  *probeRegistry                               // Phase 2 outstanding probes (route_probe_v1/route_probe_ack_v1); see routing_probe_loop.go
	queryRateLimit                 *queryRateLimit                              // Phase 2 per-target rate limit for route_query_v1; see routing_query_sender.go
	queryIDCounter                 atomic.Uint64                                // Phase 2 monotonic counter for route_query_v1 IDs (non-zero on the wire)
	senderKeySyncMu                sync.Mutex                                   // guards senderKeySyncInFlight + senderKeySyncHopInFlight + senderKeySyncLastRun (own tiny domain — never held across I/O)
	senderKeySyncInFlight          map[string]struct{}                          // single-flight set for background sender-key recovery passes, keyed by sender fingerprint; see triggerSenderKeySyncAsync
	senderKeySyncHopInFlight       map[string]struct{}                          // per-previous-hop fairness slots (1 pass per hop, keyed by authenticated identity with address fallback) — a hostile hop cannot starve the global pass cap
	senderKeySyncLastRun           map[string]time.Time                         // per-sender cooldown stamps for recovery passes (senderKeySyncCooldown)
	contactVerifyBudgets           contactVerifyRegistry                        // per-remote `contacts` verification budget, SHARED by the session and fresh-dial importers and persisted across connections (contact_verify_budget.go). Own leaf mutex, zero value is live — see docs/locking.md
	relayShapingHint               atomic.Uint64                                // Phase 3 PR 12.6 monotonic hint feeding routing.Table.LookupForRelay; rotation cadence is the counter modulo routing.ShapingProbeRatio
	identitySessions               map[domain.PeerIdentity]int                  // peer identity → active session count (multi-session awareness)
	identityRelaySessions          map[domain.PeerIdentity]int                  // peer identity → relay-capable session count (direct-route lifecycle)
	pendingWithdrawals             map[domain.PeerIdentity]*pendingWithdrawal   // route withdrawal grace period: pending RemoveDirectPeer timers keyed by peer identity. Guarded by peerMu. See routing_withdrawal_grace.go.
	presenceClock                  func() time.Time                             // source for identity presence transition timestamps; immutable after construction, overridden only by tests
	peerQuarantine                 map[domain.PeerIdentity]routeQuarantineEntry // per-peer route quarantine: peer in quarantine has inbound routing announcements dropped and is skipped as next-hop for transit relay. Guarded by peerMu. See routing_route_quarantine.go.
	peerDisconnectHistory          map[domain.PeerIdentity][]time.Time          // sliding window of disconnect timestamps per peer, drives quarantine trigger detection. Guarded by peerMu.
	peerAnnounceHistory            map[domain.PeerIdentity][]time.Time          // sliding window of inbound DELTA announce-frame arrival timestamps per peer (routes_update / v3 kind="delta" only; baselines and request_resync excluded), drives chatty_routes quarantine trigger. Guarded by peerMu. See recordInboundAnnounceAndMaybeArm.
	lastResyncAccepted             map[domain.PeerIdentity]time.Time            // last ACCEPTED request_resync per peer — debounces forced full-sync cycles below the cmd/announce limiter thresholds; see handleRequestResync. Guarded by peerMu.
	disableRateLimiting            bool                                         // test hook: skip per-IP rate limiting, connection caps, and blacklist checks
	markPeerStateIntervalTest      time.Duration                                // test hook: override markPeerStateInterval; -1 = always recompute (0 = use default)
	routeWithdrawalGracePeriodTest time.Duration                                // test hook: override routeWithdrawalGracePeriod (negative = disable grace, run withdrawals synchronously like the pre-grace legacy path; zero = use production default)
	drainDone                      func()                                       // test hook: called after drainPendingForIdentities completes; nil in production
	done                           chan struct{}                                // closed when Run() exits; drain goroutines check this to avoid work after shutdown
	primeBootstrapOnRun            bool                                         // startup hook: apply compiled bootstrap peers via add_peer once CM is ready

	// runCtx is the context passed to Run(). Stored so that callbacks
	// (e.g. onCMSessionEstablished) can start goroutines bound to the
	// Service lifecycle instead of context.Background().
	runCtx context.Context

	// Connection management subsystem (Stage 3 integration).
	peerProvider *PeerProvider                   // single source of dial candidates — replaces peers[] + peerDialCandidates()
	connManager  *ConnectionManager              // event-driven outbound connection lifecycle — replaces ensurePeerSessions()
	bannedIPSet  map[string]domain.BannedIPEntry // IP-wide bans, persisted independently from top-500 trim

	// connectOnly holds the single-peer egress pin (connectOnly command /
	// CORSA_CONNECT_ONLY). nil means "no pin — normal candidate-driven
	// dialing"; a non-nil pointer holds the one normalised PeerAddress the
	// node is allowed to dial. Deliberately OUTSIDE the seven domain
	// mutexes (per docs/locking.md): it is a single pointer with its own
	// atomic synchronisation, read on the Candidates() dial hot path
	// (connectOnlyTarget) without taking any domain lock — the same
	// "hot read = atomic snapshot, no RLock" rule the routing snapshot
	// follows. Writers (enableConnectOnly / disableConnectOnly) swap the
	// whole pointer, so readers always observe a consistent value.
	connectOnly atomic.Pointer[domain.PeerAddress]

	// setupFailures tracks consecutive cm_session_setup_failed events per
	// dial-target address. Above setupFailureBanThreshold the address is
	// pushed out of PeerProvider.Candidates() for setupFailureCooldown to
	// break reconnect storms against peers whose handshake reply gets
	// evicted by their own announce_routes flush. Lives in the peer
	// domain — guarded by s.peerMu. See setup_failure.go.
	setupFailures map[domain.PeerAddress]*setupFailureEntry

	// remoteBannedIPs holds IP-wide bans communicated by remote responders
	// via connection_notice{code=peer-banned, reason=blacklisted}. Separate
	// from bannedIPSet (which is "we banned them") because the direction
	// matters: a blacklisted-reason notice means the responder has banned
	// our egress IP, so every peer address behind that same server IP
	// faces the same rejection and must be skipped by the dialler — not
	// just the single PeerAddress that carried the notice. Keyed by the
	// server-side IP extracted from the notice's peerAddress (host part
	// of host:port). Persisted to peers.json (remote_banned_ips) so the
	// IP-wide gate survives restart; without persistence a crash would
	// reintroduce the retry storm the notice was supposed to end.
	// Protected by s.ipStateMu (the IP/advertise domain), NOT s.peerMu —
	// see recordRemoteIPBanLocked / isRemoteIPBannedLocked, whose
	// contracts require ipStateMu, and the canonical peerMu → ipStateMu
	// lock order in docs/locking.md. Reads that also touch peer-domain
	// state (isPeerRemoteBannedLocked) hold BOTH in that order.
	remoteBannedIPs map[string]remoteIPBanEntry

	// remoteIPBanOffenders tracks, per egress IP, the DISTINCT peer
	// addresses that have delivered a blacklisted-reason peer-banned
	// notice, each with the effective expiry of THAT notice. The IP-wide
	// entry in remoteBannedIPs is escalated only once
	// ipWideRemoteBanMinOffenders distinct offenders whose windows have
	// NOT elapsed agree — a single offender records only a per-peer ban,
	// so an innocent sibling sharing a NAT/VPN/Tor exit is not
	// collaterally suppressed. Storing each offender's expiry (rather
	// than a bare set) is what prevents two long-stale notices from
	// silently combining with a third notice days later to escalate to
	// an IP-wide ban: expired offenders are pruned before counting.
	// Pre-escalation, in-memory only (not persisted): the count is a
	// transient escalation signal, and a restored active IP-wide ban
	// already carries its own Until. Bounded by ipWideRemoteBanMaxOffenders
	// per IP and ipWideRemoteBanMaxTrackedIPs overall; cleared alongside
	// the IP-wide entry. Guarded by s.ipStateMu.
	remoteIPBanOffenders map[string]map[domain.PeerAddress]time.Time

	// peerActivityNanos is an atomic per-peer tracker that lives outside
	// s.peerMu so markPeerWrite/markPeerRead can skip s.peerMu.Lock() entirely
	// on the fast path. Each entry stores the UnixNano timestamp of the
	// last full state recompute for that peer. When less than
	// markPeerStateInterval has elapsed, the hot path returns
	// immediately with zero locking — eliminating the continuous writer
	// pressure that starved s.peerMu.RLock() callers (loadConversation RPCs,
	// fetch_network_stats).
	//
	// Key: domain.PeerAddress (raw, before resolveHealthAddress).
	// Value: *atomic.Int64 (UnixNano of last recompute).
	//
	// Because the key is the RAW address, inbound overlay addresses with
	// ephemeral ports mint a fresh entry per reconnect and no peer-domain
	// sweep ever sees them — evictStalePeerActivity (bootstrapLoop tick)
	// is the ONLY reclaim path. Deleting a live entry is harmless: the
	// next markPeerWrite/markPeerRead re-creates it via LoadOrStore at
	// the cost of one early recompute.
	peerActivityNanos sync.Map

	// peerActivitySweepNanos throttles evictStalePeerActivity to one full
	// Range per peerActivityEvictInterval. Atomic (CAS-guarded) because
	// the whole peerActivityNanos domain deliberately lives outside
	// s.peerMu.
	peerActivitySweepNanos atomic.Int64

	// trafficMu protects lastTrafficSnap. Separate from s.peerMu because
	// emitTrafficDeltas already releases s.peerMu (RLock) before comparing
	// with the previous snapshot. Using s.peerMu would require nesting or
	// a second Lock acquisition.
	trafficMu       sync.Mutex
	lastTrafficSnap map[domain.PeerAddress][2]int64 // [sent, received] from last emission

	// networkStatsSnap / peerHealthSnap / peersExchangeSnap hold the
	// precomputed frames for fetch_network_stats, fetch_peer_health and
	// get_peers respectively.  A single background goroutine
	// (hotReadsRefreshLoop) refreshes all three every
	// networkStatsSnapshotInterval under short s.peerMu.RLock sections; the
	// RPC handlers load the snapshots atomically with zero locking —
	// decoupled from every writer holding s.peerMu.
	//
	// Each Load returns nil until the first refresh of that snapshot
	// completes; the networkStatsFrame handler treats a nil snapshot as an
	// empty-but-valid network_stats frame (toFrame handles nil) and does NOT
	// fall back to a synchronous rebuild — such a fallback would re-acquire
	// s.peerMu.RLock on the RPC goroutine and break the lock-free contract the
	// snapshot infrastructure enforces.  peer_health and peers_exchange
	// handlers likewise accept a nil snapshot and return an empty slice — same
	// semantics the caller saw during a 0-peer startup window.  In production
	// primeHotReadSnapshots() publishes the initial snapshot before the
	// listener opens, so handlers observe a non-nil snapshot on their first
	// load.  See network_stats_snapshot.go / peer_health_snapshot.go /
	// peers_exchange_snapshot.go for the per-path contract.
	networkStatsSnap networkStatsSnapPtr
	// networkStatsAccessNanos mirrors peerHealthAccessNanos for the
	// network_stats snapshot: the Unix-nanos timestamp of the last
	// fetch_network_stats read, gating the periodic rebuild on recent
	// reader activity. Startup priming still publishes an initial snapshot.
	networkStatsAccessNanos atomic.Int64
	peerHealthSnap          peerHealthSnapPtr
	// peerHealthAccessNanos is the Unix-nanos timestamp of the last
	// peerHealthFrames() (fetch_peer_health RPC) call.  Zero until the first
	// read.  maybeRebuildPeerHealthSnapshot gates the periodic rebuild on it
	// so a headless node with no UI polling stops paying for a 2×/s
	// peer-domain snapshot — profiling's top single allocator on servers.
	// Written on every RPC read (Store is cheap); read by the refresher tick.
	peerHealthAccessNanos atomic.Int64
	peersExchangeSnap     peersExchangeSnapPtr
	// peersExchangeAccessNanos mirrors peerHealthAccessNanos for the
	// peers-exchange snapshot: the Unix-nanos timestamp of the last get_peers
	// read, gating the periodic rebuild (persistedMeta/health maps +
	// peerProvider.Candidates(), a top allocator) on recent reader activity.
	peersExchangeAccessNanos atomic.Int64
	// cmSlotsSnap caches ConnectionManager.Slots() so peerHealthFrames and
	// buildPeerExchangeResponse do not call Slots() (which takes cm.mu.RLock)
	// on the RPC path.  Without this cache those handlers would still stall
	// behind a queued CM writer under slot churn even after the s.peerMu
	// decoupling.  Rebuilt by hotReadsRefreshLoop on its own ticker; see
	// cm_slots_snapshot.go.
	cmSlotsSnap cmSlotsSnapPtr
	// routingSnap caches the routing snapshot so fetchRouteTable /
	// fetchRouteSummary / fetchRouteLookup, the file router's RouteSnap
	// callback and the desktop reachability path do not snapshot the
	// routing table on every call.  At ~9000 entries today and a projected
	// 10⁵-10⁶ at 1000 nodes, building that view under routing.Table.t.mu
	// blocks routing writers (announce loop, TickTTL, hop_ack confirmation)
	// for the build duration. The publisher uses routing.Table.
	// SnapshotIncremental (copy-on-write: t.mu.Lock, reuses the unchanged
	// route slices of the previous snapshot, re-copies only churned
	// identities), so the per-publish allocation is proportional to the
	// churn rather than the whole table. Rebuilt by hotReadsRefreshLoop on
	// the same ticker shape as the four snapshots above; see
	// routing_snapshot.go.
	routingSnap routingSnapPtr
	// presenceProjection owns the previous selectable direct/transit source
	// classes used to assign offline-transition ownership. Its leaf mutex
	// serializes routing snapshot capture with RemoveDirectPeer; the order is
	// peerMu -> presenceProjection.mu -> routing.Table.t.mu. No event
	// publication, persistence or I/O occurs while it is held. See
	// routing_snapshot.go and docs/locking.md.
	presenceProjection presenceProjectionState
	// lastRoutingSnapAtNanos coalesces routingSnap rebuilds: under a steady
	// route-announce stream the table is dirty on nearly every 500ms tick, so
	// the dirty gate alone still rebuilt the full deep-copy snapshot 2x/s
	// forever — a dominant churn source on otherwise-idle nodes. rebuildRoutingSnapshot
	// rebuilds at most once per routingSnapshotMinInterval. atomic.Int64
	// (UnixNano) because rebuildRoutingSnapshot runs both at startup priming
	// and on the dedicated ticker goroutine.
	lastRoutingSnapAtNanos atomic.Int64

	// lastRoutingFullSnapAtNanos timestamps the last FULL (non-incremental)
	// routing snapshot. It drives the periodic self-heal of the
	// copy-on-write incremental projection on a wall-clock cadence
	// (routingSnapshotFullInterval): once that long has elapsed, the next
	// rebuild that is happening ANYWAY (the table was dirty) is upgraded to
	// a full re-copy, healing any identity a mis-marked mutation left stale
	// in the reuse cache. It does NOT wake a clean table — see
	// routingSnapshotFullInterval for why a clean idle node has nothing to
	// heal. Wall-clock-gated (not a pass counter) so the cadence does not
	// drift with the refresher tick rate. Stamped on EVERY actual full
	// re-copy (SnapshotIncremental's second return), not only the ones the
	// publisher explicitly forced — a bulk mutation (snapFullDirty) or cold
	// start that produced a full resets the interval too, so the publisher
	// does not redundantly force another full shortly after. atomic.Int64
	// (UnixNano) mirrors lastRoutingSnapAtNanos: set by rebuildRoutingSnapshot
	// on the refresher goroutine and the sequential startup prime.
	lastRoutingFullSnapAtNanos atomic.Int64

	// File transfer subsystem (Iteration 21).
	//
	// Guarded by fileMu, not s.peerMu.  The file subsystem interacts with peer
	// state only through the callbacks it is handed at construction time
	// (sendFileCommandToPeer, isPeerReachable, fileTransferPeerRouteMeta —
	// each re-acquires s.peerMu on its own path), so there is no cross-domain
	// section that needs to see fileStore/fileTransfer/fileRouter atomically
	// with sessions/health.  The file domain owns a dedicated mutex to keep
	// file-subsystem writes off the peer-state critical section — see
	// docs/locking.md for the complete lock map and the canonical
	// peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu →
	// statusMu ordering.
	fileMu       sync.RWMutex
	fileStore    *filetransfer.FileStore // content-addressed file storage in transmit dir
	fileTransfer *filetransfer.Manager   // sender/receiver state machines
	fileRouter   *filerouter.Router      // routing file commands through the mesh

	// ipStateMu guards the "IP-and-advertise" domain: per-IP ban scoring,
	// inbound-connection accounting, and the advertise-convergence
	// observation tables.  Separate from s.peerMu so high-frequency writers
	// on these fields (addBanScore on every bad frame, tryIncrementIPConn
	// / decrementIPConn on every accept/close, recordObservedAddress on
	// every welcome) no longer contend with the peer-state/delivery paths
	// that still hold s.peerMu.
	//
	// Lock ordering (see docs/locking.md):
	//   peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
	// Any section that holds peerMu/deliveryMu/knowledgeMu/gossipMu AND
	// needs to read/write an ipState field acquires ipStateMu NESTED
	// inside the earlier mutex.  Reverse orders (ipStateMu first, then
	// re-entering peerMu/deliveryMu/knowledgeMu/gossipMu) are forbidden.
	//
	// Fields owned by this mutex:
	//   • bans                     — local ban scoring per remote IP
	//   • inboundByIP              — per-IP inbound connection counter
	//   • bannedIPSet              — persisted IP-wide bans we applied
	//   • remoteBannedIPs          — persisted IP-wide bans remote peers
	//                                applied against our egress IP
	//   • observedAddrs            — peer-identity → observed-IP votes
	//                                for NAT / advertise convergence
	//   • observedIPHistoryByPeer  — bounded history of observed-IP hints
	//                                to break advertise ping-pong cycles
	//
	// reachableGroups is intentionally NOT guarded by ipStateMu.  It is
	// populated exactly once by computeReachableGroups during New and
	// treated as immutable for the runtime lifetime of the Service;
	// concurrent reads of the unmutated Go map are safe without a lock.
	// If a future change makes reachableGroups runtime-mutable, it must
	// either move under this mutex (and every reader must take it) or
	// gain its own dedicated synchronisation; silently adding a writer
	// without updating readers would introduce a data race.
	ipStateMu sync.RWMutex

	// Traffic capture subsystem — diagnostic feature for recording raw
	// wire traffic of selected peer connections to disk (plan §4.1).
	// Created by initCaptureManager() in Run(); nil before Run and in
	// unit tests that do not need capture.
	captureManager *capture.Manager

	// advertise-address convergence runtime state. Owned by Service
	// under s.ipStateMu (IP/advertise domain) — no other struct writes
	// here. Cross-domain writers that also mutate peer-domain state
	// (applyAdvertiseValidationResultLocked, handlePeerBannedNotice)
	// nest s.ipStateMu inside the already-held s.peerMu per the
	// canonical peerMu → ipStateMu order. See
	// docs/protocol/handshake.md "Advertise convergence" for contract.
	//
	// observedIPHistoryByPeer tracks the last observedIPHistoryMaxSize
	// observed IP hints received for a single remote peer. Used by the
	// outbound convergence loop to break ping-pong cycles. Runtime only,
	// not persisted (see peerEntry.LastObservedIP for the persisted snapshot).
	observedIPHistoryByPeer map[domain.PeerAddress][]domain.PeerIP

	// networkOverride, when non-nil, replaces the default networkBridge
	// returned by Service.Network(). It is the single seam that lets tests
	// (see internal/core/netcore/netcoretest) drive protocol logic against
	// an in-memory transport without binding to real sockets. Production
	// code never sets this — the field is written only by
	// NewServiceWithNetwork, which is intended solely for test wiring.
	// When nil, Service.Network() falls back to the standard bridge over
	// the live s.conns registry.
	networkOverride netcore.Network

	// statusMu guards the "aggregate-status" domain: the materialized
	// network-health snapshot consumed by Desktop UI and by internal
	// policy decisions (shouldRequestPeers), plus the version-upgrade
	// detection state.  Separate from peerMu so cheap readers
	// (AggregateStatus, VersionPolicySnapshot, aggregateStatusFrame) no
	// longer contend with peer-management writers during a reconnect
	// storm — a stalled peer-domain writer can no longer block a
	// UI thread reading the last known aggregate snapshot.
	//
	// Lock ordering (see docs/locking.md):
	//   peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
	// statusMu is INNERMOST.  Every write to aggregateStatus /
	// versionPolicy is derived from peer-domain state (s.health,
	// s.peerBuilds, s.peerIDs, s.persistedMeta) and delivery-domain state
	// (s.pending), so the full stack peerMu → deliveryMu →
	// statusMu must be held for any recompute.  Pure readers
	// (AggregateStatus, VersionPolicySnapshot, aggregateStatusFrame) take
	// statusMu.RLock alone — no peer/delivery lock is needed because the
	// snapshot was already materialised by an earlier recompute.
	//
	// Reverse orders (statusMu first, then re-entering peerMu or
	// deliveryMu while still holding a status-domain field) are
	// forbidden.
	//
	// Fields owned by this mutex:
	//   • aggregateStatus               — materialized AggregateStatusSnapshot
	//                                     driving UI + shouldRequestPeers policy
	//   • lastPublishedAggregateStatus  — dedup anchor for
	//                                     TopicAggregateStatusChanged
	//   • lastAggregateStatusPublishAt  — heartbeat timestamp for
	//                                     aggregate-status resync
	//   • versionPolicy                 — runtime state for version-upgrade
	//                                     detection policy (lazy init)
	//   • lastVersionPolicyPublishAt    — heartbeat timestamp for
	//                                     TopicVersionPolicyChanged resync
	statusMu sync.RWMutex

	// aggregateStatus is the materialized aggregate network health of the
	// node. It is recomputed on every per-peer state transition via
	// refreshAggregateStatusLocked and is the single source of truth for
	// policy decisions (shouldRequestPeers) and for the Desktop UI.
	// Protected by s.statusMu.
	aggregateStatus domain.AggregateStatusSnapshot

	// lastPublishedAggregateStatus and lastAggregateStatusPublishAt form the
	// dedup anchor for TopicAggregateStatusChanged. The "last published"
	// snapshot is distinct from aggregateStatus because other paths
	// (refresh-without-publish during orphan eviction, init-time refresh)
	// can update aggregateStatus without notifying subscribers — the anchor
	// must reflect what subscribers actually saw. The timestamp drives the
	// heartbeat that guarantees eventual consistency under lossy ebus
	// delivery (subscriber inbox full → Publish drops the event), so a
	// dropped publish during a storm cannot leave the UI permanently stale.
	// Both fields are zero until the first publish.
	// Protected by s.statusMu.
	lastPublishedAggregateStatus domain.AggregateStatusSnapshot
	lastAggregateStatusPublishAt time.Time

	// versionPolicy holds the runtime state for the node-owned version
	// upgrade detection policy. Nil until first observation; created
	// lazily. Protected by s.statusMu.
	versionPolicy *versionPolicyState

	// lastVersionPolicyPublishAt is the wall-clock time of the last
	// TopicVersionPolicyChanged publish. Drives the heartbeat resync for
	// version policy so that a dropped async delivery during a storm is
	// retransmitted within versionPolicyHeartbeatInterval, regardless of
	// whether the snapshot content moves again. Zero until first publish.
	// Protected by s.statusMu.
	lastVersionPolicyPublishAt time.Time
}

// subscriber describes a hello-derived node route registration. The owning connection is identified by connID so that
// subscriber state survives independently of net.Conn identity and can be
// resolved through the ConnID-first registry (netCoreForID). The
// net.Conn-first write path remains available via netCoreForID(connID).Conn()
// until PR 10.6 migrates the write wrappers.
type subscriber struct {
	id        string        // logical subscription ID (frame.Subscriber or derived)
	recipient string        // recipient identity/address this subscription serves
	connID    domain.ConnID // owning connection ID (0 when not tied to a live conn)
}

type peerSession struct {
	address      domain.PeerAddress
	peerIdentity domain.PeerIdentity // peer's Ed25519 identity fingerprint from welcome.Address
	connID       domain.ConnID       // monotonic connection ID for diagnostics
	conn         net.Conn
	metered      *netcore.MeteredConn // tracks bytes for this session; nil when conn is not metered

	// sendCh is the UPPER of the two outbound queues: producers put frames
	// here and the servePeerSession loop moves them into the NetCore writer
	// queue. The channel is NEVER closed — ownership is expressed through
	// sendMu / sendClosed instead. See enqueueSend and closeSendQueue in
	// peer_send_queue.go for the protocol and why closing would be wrong.
	sendCh     chan peerSendItem
	sendMu     sync.Mutex
	sendClosed bool

	inboxCh chan protocol.Frame
	errCh   chan error

	// admission is the response-plane per-neighbour budget, entitlement state
	// and violation ledger this session's reader applies to every line BEFORE
	// it is parsed (peer_session_admission.go).
	//
	// It carries its OWN mutex and is therefore outside the seven domain
	// mutexes on purpose — see docs/locking.md, "peerSession.admission". It is
	// held BY VALUE because its zero value is a live controller with full
	// buckets: no construction site has to remember to build one, and a
	// forgotten one can never silently mean "this neighbour has no budget".
	admission peerSessionAdmission

	version      int
	authOK       bool
	capabilities []domain.Capability // intersection of local and remote capabilities negotiated during handshake

	// declarations mirrors the peer's RAW handshake self-description — the
	// validated raw capability names of §2.2 and the declared dtype set of
	// §6.1 — from the same welcome frame that produced capabilities above.
	// NetCore owns the authoritative copy (applyWelcomeMetadata writes
	// both); this mirror exists because the outbound dispatcher is
	// addressed by PeerAddress and never sees a ConnID. Guarded by peerMu,
	// like every other field of peerSession that outlives the handshake.
	declarations netcore.HandshakeDeclarations

	// netCore owns the outbound connection and is the single writer to the
	// socket. Set at construction by openPeerSession / openPeerSessionForCM
	// before the first peerSessionRequest so that welcome/auth frames go
	// through the managed writer path. nil only in unit tests that build a
	// peerSession manually without the service wiring.
	netCore *netcore.NetCore

	// onClose is invoked by Close() before the underlying connection is shut
	// down. Outbound sessions set it to unregister the netCore from the
	// service-level connection map. nil for unit tests.
	onClose func()

	// welcomeMeta carries handshake metadata that must NOT be applied to
	// Service-level maps until the CM generation check passes. Stale dials
	// (generation mismatch) are discarded without activating side-effects;
	// only onCMSessionEstablished writes these into shared state.
	welcomeMeta *peerWelcomeMeta

	// closeOnce guarantees that the teardown sequence (NetCore.Close +
	// onClose) runs exactly once even under concurrent Close() calls from
	// the defer in openPeerSession, the ctx-watcher, closeOnError in
	// openPeerSessionForCM and the CM failure path.
	closeOnce sync.Once
	closeErr  error

	// ffDropsSinceWarn / ffLastDropWarnAt back the rate-limited
	// fire_and_forget_buffer_full warn (see logFireAndForgetDrop).
	// Only the servePeerSession goroutine reads or writes them, so
	// they need no lock.
	ffDropsSinceWarn int
	ffLastDropWarnAt time.Time

	// contactSyncCh serialises on-demand contact syncs through the
	// session's OWNER (the servePeerSession loop): a recovery goroutine
	// sends a reply channel here, the serve loop — the single legal
	// reader of inboxCh — runs fetch_contacts itself and reports the
	// imported count back. This is the ONLY sanctioned way to run a
	// fetch_contacts over a live session from outside the loop:
	// calling peerSessionRequest from another goroutine would race the
	// loop for inbox frames. Unbuffered by design — a send succeeds
	// only when the loop is alive and idle enough to take the request,
	// so a dead or torn-down session simply times the sender out
	// (requestOwnedContactSync). nil in unit tests that build a
	// peerSession manually; a nil channel makes both the serve-loop
	// select arm and the request helper inert.
	contactSyncCh chan chan int
}

// peerWelcomeMeta holds welcome-frame data deferred until CM activation.
type peerWelcomeMeta struct {
	welcome         protocol.Frame // raw welcome frame for learnIdentityFromWelcome
	clientVersion   string
	clientBuild     int
	observedAddress string
}

// Close shuts down the session. When netCore is set, teardown runs in this
// order:
//  1. netCore.Close() — shuts the send gate, closes the raw socket, signals
//     the writer and waits for it to drain the queue residue and exit.
//     Neither queue on this path is closed as a Go channel: both have many
//     producers on arbitrary goroutines, and a close racing a producer's send
//     is a panic rather than a status. See closeSendQueue for the upper queue
//     and docs/protocol/network_core.md, "Queue ownership", for the lower one.
//  2. onClose() — removes the NetCore registration from s.conns.
//
// The order matters for the single-writer invariant. While writerLoop is
// still alive, the registration MUST remain visible to netCoreFor() so
// that any concurrent writeJSONFrameByID(connID, ...) goes through the
// managed path (enqueueFrameByID → sendRaw). Unregistering first would cause
// netCoreForID to return nil and the writeJSONFrameByID fallback would then call
// conn.Write / io.WriteString directly — a second writer racing with a
// still-live writerLoop. Only after netCore.Close() has returned is it
// safe to remove the map entry: the socket is closed, the send gate refuses
// every later producer, and the writer goroutine has exited.
//
// Concurrent-safe and idempotent via sync.Once (mirrors NetCore.closeOnce).
// The first caller performs the teardown; subsequent callers observe the
// same stored result without re-running the callbacks.
func (ps *peerSession) Close() error {
	if ps == nil {
		return nil
	}
	ps.closeOnce.Do(func() {
		// Fence and finalise the upper queue BEFORE the transport goes
		// away. Sessions that die before servePeerSession ever runs (failed
		// handshake, CM generation mismatch) have no serve-loop exit to
		// hang the drain on, and frames enqueued during initPeerSession
		// would otherwise vanish without a terminal.
		ps.closeSendQueue()
		if ps.netCore != nil {
			ps.netCore.Close()
			if ps.onClose != nil {
				ps.onClose()
			}
			return
		}
		if ps.onClose != nil {
			ps.onClose()
		}
		if ps.conn != nil {
			ps.closeErr = ps.conn.Close()
		}
	})
	return ps.closeErr
}

type peerHealth struct {
	Address             domain.PeerAddress
	Connected           bool
	Direction           domain.PeerDirection // outbound, inbound, or "" (unknown)
	State               string
	LastConnectedAt     time.Time
	LastDisconnectedAt  time.Time
	LastPingAt          time.Time
	LastPongAt          time.Time
	LastUsefulSendAt    time.Time
	LastUsefulReceiveAt time.Time
	ConsecutiveFailures int
	LastError           string
	Score               int       // peer quality score for persistence priority
	BannedUntil         time.Time // peer is not dialled until this time expires
	BytesSent           int64     // total bytes sent to this peer across all sessions
	BytesReceived       int64     // total bytes received from this peer across all sessions

	// Machine-readable error codes — Phase 1 of version upgrade detection.
	//
	// LastErrorCode is the protocol.ErrorCode of the most recent *pre-handshake*
	// protocol rejection (e.g. incompatible-protocol-version). Set by
	// penalizeOldProtocolPeer when the peer is rejected before or during
	// handshake. Cleared on successful reconnect (markPeerConnected) and
	// by operator override (add_peer). Survives across failed reconnects.
	LastErrorCode string
	// LastDisconnectCode is the protocol.ErrorCode that caused the most recent
	// *post-handshake* socket teardown. Set in markPeerDisconnected when the
	// disconnect error wraps a known protocol error (e.g. frame-too-large,
	// rate-limited). Empty string when the disconnect was clean (err == nil),
	// non-protocol (network timeout, EOF), or the error maps to the generic
	// "protocol-error" sentinel. Cleared on successful reconnect
	// (markPeerConnected). Not set for pre-handshake rejections — those go
	// into LastErrorCode via penalizeOldProtocolPeer.
	LastDisconnectCode string

	// Version incompatibility diagnostics — per-address counters.
	//
	// IncompatibleVersionAttempts is the source of truth for per-address
	// ban scoring: each attempt adds peerBanIncrementIncompatible to the
	// overlay ban, and the ban fires when cumulative penalty reaches the
	// threshold. This counter is NOT used for the update_available signal.
	//
	// The update_available signal is driven by a separate per-identity
	// dedup set in versionPolicyState.incompatibleReporters, which counts
	// distinct peer identities that reported incompatibility within the
	// observation window. The two counters measure different things:
	// attempts-per-address (ban scoring) vs reporters-per-identity (update signal).
	IncompatibleVersionAttempts domain.AttemptCount    // cumulative per-address attempts for ban scoring
	LastIncompatibleVersionAt   time.Time              // timestamp of the last incompatible event
	ObservedPeerVersion         domain.ProtocolVersion // last observed remote protocol version
	ObservedPeerMinimumVersion  domain.ProtocolVersion // last observed remote minimum version
}

type pendingFrame struct {
	Frame    protocol.Frame
	QueuedAt time.Time
	Retries  int

	// NextDrainAt gates the event-driven drain fast path
	// (drainPendingForIdentities). When a drain delivery attempt finds no
	// usable route, the frame is stamped with a short backoff so the next
	// routing/announce churn event for the same recipient does NOT re-extract
	// and re-copy it immediately. Without this gate a permanently unroutable
	// frame was re-extracted and re-merged (two full slice copies) on every
	// churn event — profiling flagged drainPendingForIdentities as the single
	// largest alloc_space source under sustained churn. The per-frame-type
	// backstops remain the real delivery path (send_message via the relay retry
	// loop, push_message via the reconnect / inbound pending flush), so gating
	// the fast path only rate-limits an optimization, it never drops a frame.
	// Ephemeral / in-memory only — excluded from queue-state marshaling.
	NextDrainAt time.Time `json:"-"`
}

type relayAttempt struct {
	FirstSeen   time.Time
	LastAttempt time.Time
	Attempts    int
}

type outboundDelivery struct {
	MessageID     string
	Recipient     string
	Status        string
	QueuedAt      time.Time
	LastAttemptAt time.Time
	Retries       int
	Error         string
}

// outboundTerminalRetention is how long a terminal ("expired"/"failed")
// outbound entry stays visible in pendingMessagesFrame before
// sweepTerminalOutbound reclaims it. Terminal entries have no other
// delete path: the receipt that clears live entries never arrives for a
// failed delivery, so without the sweep a node repeatedly messaging an
// unreachable recipient accretes one entry per message forever.
const outboundTerminalRetention = time.Hour

// outboundTerminalSweepInterval spaces sweepTerminalOutbound scans; the
// bootstrapLoop tick (2 s) is far finer than the retention needs.
const outboundTerminalSweepInterval = time.Minute

type banEntry struct {
	Score       int
	Blacklisted time.Time

	// LastScored is the instant of the most recent addBanScore hit.
	// Sub-threshold entries (Score > 0, no blacklist yet) carry no other
	// timestamp, so without it they could never be aged out and s.bans
	// grew monotonically with every misbehaving transient IP. Used by
	// purgeExpiredBansLocked (ban_purge.go) to drop entries idle past
	// banScoreIdleTTL.
	LastScored time.Time
}

const (
	peerStateHealthy       = "healthy"
	peerStateDegraded      = "degraded"
	peerStateStalled       = "stalled"
	peerStateReconnecting  = "reconnecting"
	peerDirectionOutbound  = domain.PeerDirectionOutbound
	peerDirectionInbound   = domain.PeerDirectionInbound
	peerRequestTimeout     = 12 * time.Second
	pendingFrameTTL        = 5 * time.Minute
	relayRetryTTL          = 3 * time.Minute
	maxPendingFrameRetries = 5
	// drainNoRouteBackoff is how long a pending frame is excluded from the
	// event-driven drain fast path after a drain attempt found no usable
	// route. Sized at the relay-retry-loop cadence so the gate never delays
	// delivery beyond the existing backstops: a gated send_message is still
	// picked up by retryRelayDeliveries (every ~2s), and a gated push_message
	// by the reconnect outbound flush (flushPendingPeerFrames) / inbound pending
	// flush (flushPendingFireAndForget). The gate only stops
	// drainPendingForIdentities from re-extracting and re-copying the same
	// unroutable frame on every churn event. See pendingFrame.NextDrainAt.
	drainNoRouteBackoff    = 2 * time.Second
	banThreshold           = 1000
	banIncrementInvalidSig = 100
	// banIncrementIncompatibleVersion stays below banThreshold on purpose: a
	// single incompatible hello must not arm the 24-hour IP-wide blacklist,
	// because NAT gateways, VPN exits, Tor exits and multi-homed hosts share
	// one egress IP across many unrelated peers. Punishing compatible
	// siblings for one misconfigured neighbour behind a shared IP would turn
	// a one-off protocol mismatch into a 24-hour outage for the entire host.
	// The storm-suppression signal the dialler needs is delivered per-peer
	// instead — a connection_notice{code=peer-banned, reason=peer-ban} is
	// emitted on first contact from the inbound incompatible-hello branch
	// (see dispatchNetworkFrame) and arms the dialler-side gate 6c.2 against
	// this specific PeerAddress without touching the IP surface. The
	// transport-level accumulation here remains the safety net for sustained
	// noise from the same IP: 4 attempts cross banThreshold, at which point
	// addBanScore fires a second notice with reason=blacklisted on the
	// connection that crossed the threshold and activates the 24-hour
	// silent-close in handleConn for all subsequent TCP attempts from that IP.
	banIncrementIncompatibleVersion = 250
	banIncrementRateLimit           = 200 // command rate limit violation signals intentional abuse
	banDuration                     = 24 * time.Hour
	// nodeName and networkName are the protocol-level identifiers included
	// in handshake, ping/pong and other frames. Defined once here so that
	// all call-sites stay consistent. When these values become configurable
	// at runtime they should move to config.Node or config.App.
	nodeName    = "corsa"
	networkName = "gazeta-devnet"
)

type incomingMessage struct {
	ID         protocol.MessageID
	Topic      string
	Sender     string
	Recipient  string
	Flag       protocol.MessageFlag
	CreatedAt  time.Time
	TTLSeconds int
	Body       string

	// Hops is the raw wire hop budget (frame Hops field). 0 = absent:
	// either a legacy peer that predates the field or a local send —
	// storeIncomingMessage assigns defaultMessageHopBudget as if this
	// node originated the message. See transit_retention.go.
	Hops int

	// Via is the ingress peer address for messages that arrived over
	// the network (push_message / relay_message); empty for local
	// sends and imports. Used for ingress suppression in gossip
	// fan-out and recorded on the stored Envelope.
	Via domain.PeerAddress

	// ViaIdentity is the authenticated identity of the ingress peer,
	// when known — see protocol.Envelope.ViaIdentity for why address
	// comparison alone cannot cover inbound-only next-hops.
	ViaIdentity domain.PeerIdentity

	// SenderPubKey/SenderBoxKey/SenderBoxSig carry the origin sender's
	// PUBLIC key material attached to the transport frame (relay_message
	// / push_message, DM-class topics only). All three are
	// self-certifying against the Sender address — the address is the
	// fingerprint of the signing key — and are validated by
	// importAttachedSenderKeys before any of them enters the knowledge
	// maps. Empty when the sender (or an intermediate legacy hop that
	// stripped the fields) did not attach them; storeIncomingMessage
	// then falls back to the on-demand contact sync. This is what lets
	// a FIRST-CONTACT DM delivered over relay hops that never met the
	// sender verify without a network round-trip.
	SenderPubKey string
	SenderBoxKey string
	SenderBoxSig string
}

func NewService(cfg config.Node, id *identity.Identity, eventBus *ebus.Bus) *Service {
	// Load persisted peer state and merge with bootstrap peers.
	// Bootstrap peers always appear first; persisted peers are appended
	// in score-descending order, skipping duplicates.
	peersStatePath := cfg.EffectivePeersStatePath()
	peerState, err := loadPeerState(peersStatePath)
	if err != nil {
		log.Error().Str("path", peersStatePath).Err(err).Msg("peer state load failed")
		peerState = peerStateFile{Version: peerStateVersion, Peers: []peerEntry{}}
	}

	peers := make([]transport.Peer, 0, len(cfg.BootstrapPeers)+len(peerState.Peers))
	seenAddrs := make(map[domain.PeerAddress]struct{})
	for _, addr := range cfg.BootstrapPeers {
		pa := domain.PeerAddress(addr)
		peers = append(peers, transport.Peer{
			Address: pa,
			Source:  domain.PeerSourceBootstrap,
		})
		seenAddrs[pa] = struct{}{}
	}
	sortPeerEntries(peerState.Peers)
	// Index persisted entries so we can seed health from their metadata.
	persistedByAddr := make(map[domain.PeerAddress]*peerEntry, len(peerState.Peers))
	for i, entry := range peerState.Peers {
		if _, dup := seenAddrs[entry.Address]; dup {
			// Even for duplicates (bootstrap overlap) keep the metadata for health seeding.
			persistedByAddr[entry.Address] = &peerState.Peers[i]
			continue
		}
		seenAddrs[entry.Address] = struct{}{}
		persistedByAddr[entry.Address] = &peerState.Peers[i]
		peers = append(peers, transport.Peer{
			Address: entry.Address,
			Source:  domain.PeerSourcePersisted,
		})
	}

	selfBoxKey := identity.BoxPublicKeyBase64(id.BoxPublicKey)
	selfBoxSig := identity.SignBoxKeyBinding(id)
	selfContact := trustedContact{
		Address: id.Address,
		PubKey:  identity.PublicKeyBase64(id.PublicKey),
		Source:  "self",
	}
	// The box key enters the CONTACT plane (trust-store self row →
	// s.boxKeys → fetch_contacts) only when this node accepts DMs
	// addressed to itself: a relay-only node (headless without
	// CORSA_ACCEPT_DM) never redistributes its own key, and
	// loadTrustStore refreshes the persisted self row from this value,
	// so flipping the policy purges a previously persisted key.
	//
	// The HANDSHAKE plane is deliberately NOT gated: hello/welcome carry
	// selfBoxKey/selfBoxSig unconditionally because deployed peers issue
	// the session-auth challenge only when all four identity fields are
	// present (connauth.HasIdentityFields) — a keyless hello would break
	// outbound authenticated sessions. Direct peers therefore still
	// cache the key; DMs composed with it are dropped by the inbound
	// gate in storeIncomingMessage.
	if !cfg.DisableDirectMessages {
		selfContact.BoxKey = selfBoxKey
		selfContact.BoxSignature = selfBoxSig
	}
	trust, err := loadTrustStore(cfg.TrustStorePath, selfContact)
	if err != nil {
		panic(err)
	}

	known := newBoundedKnownIdentities(maxKnownIdentities)
	boxKeys := map[string]string{}
	pubKeys := map[string]string{}
	boxSigs := map[string]string{}
	// LRU eviction cascades into the key maps, which is what bounds THEM:
	// every addKnownBoxKey/addKnownPubKey/addKnownBoxSig insert registers
	// its address in `known` under the same knowledgeMu hold, so the key
	// maps stay subsets of known ∪ pinned (≤ maxKnownIdentities + trust
	// store size). The hook is installed BEFORE the seed loops below: a
	// trust store holding more records than the bound must evict key-map
	// entries during the seed too, or the maps start life over the limit.
	known.onEvict = func(address string) {
		delete(boxKeys, address)
		delete(pubKeys, address)
		delete(boxSigs, address)
	}
	// Trusted contacts are PINNED: their key knowledge must survive
	// transit-identity churn (see boundedKnownIdentities.pinned).
	for address, contact := range trust.trustedContacts() {
		known.Pin(address)
		boxKeys[address] = contact.BoxKey
		pubKeys[address] = contact.PubKey
		boxSigs[address] = contact.BoxSignature
	}
	// Persisted signed records reseed the key maps too (they were verified
	// at import and re-verified at load): without this a restart would
	// leave a resolved identity "known on disk" yet unusable for DM — and
	// a repeat lookup would merge as duplicate without ever refilling the
	// maps. Trusted-contact entries above stay authoritative for overlaps.
	for _, body := range trust.recordBodies(networkName) {
		address := body.Address.String()
		if address == id.Address {
			continue
		}
		known.Add(address)
		if _, pinnedContact := pubKeys[address]; pinnedContact {
			continue
		}
		pubKeys[address] = string(body.PubKey)
		if body.DM {
			boxKeys[address] = string(body.BoxKey)
			boxSigs[address] = string(body.BoxSig)
		}
	}
	// Delivery state (pending rings, outbound tracking, relay retry, receipts)
	// is in-memory only: nothing survives a restart, recovery is sender-side
	// end-to-end retry (docs/protocol/relay.md INV-8).
	pendingKeys := make(map[pendingKey]struct{})

	topics := make(map[string][]protocol.Envelope)
	// Phase 4 13.4: dedup set is a rotating Bloom filter, sized via
	// package-level constants (bloomDedupBits / bloomDedupHashes /
	// bloomDedupRotation); the nil clock argument selects the production
	// wall clock — tests build their own dedup with an injected clock via
	// newRotatingBloomDedup directly. It starts empty: message state is
	// in-memory only, so there is nothing to re-seed after a restart.
	seen := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
	receipts := make(map[string][]protocol.DeliveryReceipt)
	// seenReceipts mirrors the seen bloom: a bounded sliding-window dedup
	// (receipt_dedup.go) instead of the old unbounded map. nil clock = wall
	// clock; starts empty (receipt dedup state is in-memory only).
	seenReceipts := newRotatingHashDedup(receiptDedupRotation, maxReceiptDedupEntries, nil)

	// Seed health map from persisted peer metadata so that scores,
	// failure counts and timestamps survive a restart+flush cycle
	// even if the peer hasn't reconnected yet.
	restoredHealth := make(map[domain.PeerAddress]*peerHealth, len(persistedByAddr))
	for addr, entry := range persistedByAddr {
		h := &peerHealth{
			Address:             addr,
			State:               peerStateReconnecting,
			ConsecutiveFailures: entry.ConsecutiveFailures,
			LastError:           entry.LastError,
			Score:               entry.Score,
			// Machine-readable version diagnostics — restored so the
			// operator-visible peerHealthFrames() snapshot retains the
			// exact evidence that created the lockout.
			LastErrorCode:               entry.LastErrorCode,
			LastDisconnectCode:          entry.LastDisconnectCode,
			IncompatibleVersionAttempts: entry.IncompatibleVersionAttempts,
			ObservedPeerVersion:         entry.ObservedPeerVersion,
			ObservedPeerMinimumVersion:  entry.ObservedPeerMinimumVersion,
		}
		if entry.LastConnectedAt != nil {
			h.LastConnectedAt = *entry.LastConnectedAt
		}
		if entry.LastDisconnectedAt != nil {
			h.LastDisconnectedAt = *entry.LastDisconnectedAt
		}
		if entry.BannedUntil != nil && time.Now().UTC().Before(*entry.BannedUntil) {
			h.BannedUntil = *entry.BannedUntil
		}
		if entry.LastIncompatibleVersionAt != nil {
			h.LastIncompatibleVersionAt = *entry.LastIncompatibleVersionAt
		}
		restoredHealth[addr] = h
	}

	// Restore the address-to-identity binding alongside peer health. Without
	// this, PeerHealth.PeerID becomes empty after restart until the next
	// handshake and cannot serve as last-online fallback evidence.
	restoredPeerIDs := make(map[domain.PeerAddress]domain.PeerIdentity)
	for addr, entry := range persistedByAddr {
		if !entry.Identity.IsZero() {
			restoredPeerIDs[addr] = entry.Identity
		}
	}

	// Restore peerVersions from persisted lockout data so that
	// peerHealthFrames() can surface ClientVersion for locked-out
	// peers that haven't reconnected since restart. Without this,
	// the operator-visible snapshot loses the remote client version
	// string even though the lockout and diagnostic evidence survive.
	restoredVersions := make(map[domain.PeerAddress]string)
	for addr, entry := range persistedByAddr {
		if cv := string(entry.VersionLockout.ObservedClientVersion); cv != "" {
			restoredVersions[addr] = cv
		}
	}

	pending := make(map[domain.PeerAddress][]pendingFrame)
	relayRetry := make(map[string]relayAttempt)

	svc := &Service{
		// Default lifecycle context: replaced by Run(ctx) with the real
		// cancellable ctx. The default prevents nil-deref in code paths
		// (e.g. handleInboundPushMessage sender-key recovery) that derive
		// a timeout from s.runCtx before Run() has been called — notably in
		// unit tests that exercise handlers directly without Run().
		runCtx:   context.Background(),
		identity: id,
		// startedAt is captured at construction (not at Run()) so the
		// uptime_seconds reported by getNodeStatus stays meaningful in
		// unit tests that drive the Service without calling Run, and
		// matches the moment the in-memory state machine first became
		// live.
		startedAt:                time.Now().UTC(),
		datagramMetrics:          datagram.NewMetrics(),
		cfg:                      cfg,
		eventBus:                 eventBus,
		selfBoxKey:               selfBoxKey,
		selfBoxSig:               selfBoxSig,
		trust:                    trust,
		peers:                    peers,
		peersStatePath:           peersStatePath,
		persistedMeta:            persistedByAddr,
		known:                    known,
		boxKeys:                  boxKeys,
		pubKeys:                  pubKeys,
		boxSigs:                  boxSigs,
		topics:                   topics,
		receipts:                 receipts,
		notices:                  make(map[string]gazeta.Notice),
		seen:                     seen,
		seenReceipts:             seenReceipts,
		subs:                     make(map[string]map[string]*subscriber),
		sessions:                 make(map[domain.PeerAddress]*peerSession),
		health:                   restoredHealth,
		peerTypes:                make(map[domain.PeerAddress]domain.NodeType),
		peerIDs:                  restoredPeerIDs,
		peerVersions:             restoredVersions,
		peerBuilds:               make(map[domain.PeerAddress]int),
		pending:                  pending,
		pendingKeys:              pendingKeys,
		relayRetry:               relayRetry,
		relayDeliveredTo:         make(map[protocol.MessageID]map[domain.PeerIdentity]struct{}),
		outbound:                 make(map[string]outboundDelivery),
		awaitingDelivered:        make(map[protocol.MessageID]*deliveryRetryEntry),
		cancelledDeliveries:      make(map[protocol.MessageID]time.Time),
		awaitingSeenAck:          make(map[protocol.MessageID]*seenAckRetryEntry),
		sentDMIDs:                newBoundedKnownIdentities(maxSentDMIDs),
		senderKeySyncInFlight:    make(map[string]struct{}),
		senderKeySyncHopInFlight: make(map[string]struct{}),
		senderKeySyncLastRun:     make(map[string]time.Time),
		upstream:                 make(map[domain.PeerAddress]struct{}),
		dialOrigin:               make(map[domain.PeerAddress]domain.PeerAddress),
		observedAddrs:            make(map[domain.PeerIdentity]string),
		observedIPHistoryByPeer:  make(map[domain.PeerAddress][]domain.PeerIP),
		reachableGroups:          computeReachableGroups(cfg),
		inboundHealthRefs:        make(map[domain.PeerAddress]int),
		conns:                    make(map[netcore.ConnID]*connEntry),
		connIDByNetConn:          make(map[net.Conn]netcore.ConnID),
		bans:                     make(map[string]banEntry),
		events:                   make(map[chan protocol.LocalChangeEvent]struct{}),
		identitySessions:         make(map[domain.PeerIdentity]int),
		identityRelaySessions:    make(map[domain.PeerIdentity]int),
		pendingWithdrawals:       make(map[domain.PeerIdentity]*pendingWithdrawal),
		presenceClock:            time.Now,
		peerQuarantine:           make(map[domain.PeerIdentity]routeQuarantineEntry),
		peerDisconnectHistory:    make(map[domain.PeerIdentity][]time.Time),
		peerAnnounceHistory:      make(map[domain.PeerIdentity][]time.Time),
		lastResyncAccepted:       make(map[domain.PeerIdentity]time.Time),
		bannedIPSet:              make(map[string]domain.BannedIPEntry),
		remoteBannedIPs:          make(map[string]remoteIPBanEntry),
		remoteIPBanOffenders:     make(map[string]map[domain.PeerAddress]time.Time),
		setupFailures:            make(map[domain.PeerAddress]*setupFailureEntry),
		aggregateStatus:          domain.AggregateStatusSnapshot{Status: domain.NetworkStatusOffline},
		done:                     make(chan struct{}),
	}
	// Initialize PeerProvider (Stage 3: connection management integration).
	svc.peerProvider = NewPeerProvider(PeerProviderConfig{
		HealthFn: func(addr domain.PeerAddress) *PeerHealthView {
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			addr = svc.resolveHealthAddress(addr)
			h := svc.health[addr]
			if h == nil {
				return nil
			}
			return &PeerHealthView{
				Score:               h.Score,
				ConsecutiveFailures: h.ConsecutiveFailures,
				LastDisconnectedAt:  h.LastDisconnectedAt,
				BannedUntil:         h.BannedUntil,
				Connected:           h.Connected,
				// LastErrorCode is forwarded so PeerProvider.buildBannedIPsSet
				// can distinguish address-scoped ban reasons (self-identity)
				// from IP-scoped ones (incompatible protocol, etc.). Without
				// this field the carve-out at buildBannedIPsSet never fires
				// and a single self-alias ban widens to the whole host/NAT.
				LastErrorCode: h.LastErrorCode,
			}
		},
		ConnectedFn: func() map[string]struct{} {
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			return svc.connectedHostsLocked()
		},
		QueuedFn: func() map[string]struct{} {
			if svc.connManager == nil {
				return make(map[string]struct{})
			}
			return svc.connManager.QueuedIPs()
		},
		ForbiddenFn:   svc.isForbiddenDialIP,
		IsSelfAddress: svc.isSelfAddress,
		NetworksFn: func() map[domain.NetGroup]struct{} {
			// reachableGroups is populated exactly once by
			// computeReachableGroups during New and treated as
			// immutable thereafter, so this read is lock-free.  A
			// defensive copy is still returned so the caller cannot
			// mutate the live map (and if a future change adds a
			// runtime writer it must also add synchronisation — the
			// contract is documented on the ipStateMu field).
			groups := make(map[domain.NetGroup]struct{}, len(svc.reachableGroups))
			for g := range svc.reachableGroups {
				groups[g] = struct{}{}
			}
			return groups
		},
		BannedIPsFn: func() map[string]domain.BannedIPEntry {
			// ipStateMu, not s.peerMu: bannedIPSet is IP-domain state.
			svc.ipStateMu.RLock()
			defer svc.ipStateMu.RUnlock()
			result := make(map[string]domain.BannedIPEntry, len(svc.bannedIPSet))
			for ip, entry := range svc.bannedIPSet {
				result[ip] = entry
			}
			return result
		},
		VersionLockedOutFn: func(addr domain.PeerAddress) bool {
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			return svc.isPeerVersionLockedOutLocked(addr)
		},
		RemoteBannedFn: func(addr domain.PeerAddress) bool {
			// Cross-domain read: persistedMeta (peer domain, s.peerMu) +
			// remoteBannedIPs (ipState domain, s.ipStateMu).  Canonical
			// lock order s.peerMu → s.ipStateMu per docs/locking.md.
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			svc.ipStateMu.RLock()
			defer svc.ipStateMu.RUnlock()
			return svc.isPeerRemoteBannedLocked(addr, time.Now().UTC())
		},
		SetupFailureBannedFn:   svc.IsSetupFailureBanned,
		ConnectOnlyFn:          svc.connectOnlyTarget,
		ListenAddr:             domain.ListenAddress(cfg.ListenAddress),
		DefaultPort:            config.DefaultPeerPort,
		AllowPrivateCandidates: cfg.AllowPrivatePeers,
	})

	// Load persisted peers into PeerProvider.
	for _, entry := range peerState.Peers {
		svc.peerProvider.Restore(domain.RestoreEntry{
			Address: entry.Address,
			Source:  entry.Source,
			AddedAt: func() time.Time {
				if entry.AddedAt != nil {
					return *entry.AddedAt
				}
				return time.Now().UTC()
			}(),
			Network: entry.Network,
		})
	}
	// Add bootstrap peers (merge: Source updated, AddedAt/Network preserved).
	for _, addr := range cfg.BootstrapPeers {
		svc.peerProvider.Add(domain.PeerAddress(addr), domain.PeerSourceBootstrap)
	}

	// Restore IP-wide bans from persisted state. Expired entries are
	// silently dropped — they would be filtered anyway by BannedIPsFn.
	now := time.Now().UTC()
	for _, b := range peerState.BannedIPs {
		if b.BannedUntil.After(now) {
			affected := make([]domain.PeerAddress, len(b.AffectedPeers))
			for i, a := range b.AffectedPeers {
				affected[i] = domain.PeerAddress(a)
			}
			svc.bannedIPSet[b.IP] = domain.BannedIPEntry{
				IP:            b.IP,
				BannedUntil:   b.BannedUntil,
				BanOrigin:     domain.PeerAddress(b.BanOrigin),
				BanReason:     b.BanReason,
				AffectedPeers: affected,
			}
		}
	}

	// Restore remote IP-wide bans ("they banned our egress IP") from
	// persisted state. Expired entries are silently dropped. Without
	// this restore the dial gate would re-dial sibling peers on a
	// blacklisted egress after every restart, reintroducing the exact
	// retry storm the blacklisted-reason notice was designed to end.
	for _, b := range peerState.RemoteBannedIPs {
		if b.Until.After(now) {
			svc.remoteBannedIPs[b.IP] = remoteIPBanEntry{
				Until:  b.Until,
				Reason: b.Reason,
			}
		}
	}

	// Eagerly clear version lockouts whose local version fingerprint is stale
	// (the node upgraded since the lockout was recorded). This runs before
	// PeerProvider and CM are operational, so no concurrent access yet.
	svc.clearStaleVersionLockoutsLocked()

	// Initialize ConnectionManager (Stage 3: connection management integration).
	svc.connManager = NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return svc.cfg.EffectiveMaxOutgoingPeers() },
		Provider:   svc.peerProvider,
		EventBus:   svc.eventBus,
		DialFn: func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
			return svc.dialForCM(ctx, addresses)
		},
		OnSessionEstablished: func(info SessionInfo) {
			svc.onCMSessionEstablished(info)
		},
		OnSessionTeardown: func(info SessionInfo) {
			svc.onCMSessionTeardown(info)
		},
		OnStaleSession: func(session *peerSession) {
			svc.onCMStaleSession(session)
		},
		OnDialFailed: func(address domain.PeerAddress, err error, incompatible bool) {
			svc.onCMDialFailed(address, err, incompatible)
		},
		// Setup-failure cooldown gate consulted in handleActiveSessionLost.
		// Without it the cooldown only engaged when fill() picked a NEW
		// candidate; retryAfterBackoff bypassed PeerProvider and kept
		// dialling the same address for another reconnectMaxRetries
		// cycle (~14s of agitation). With this callback the gate also
		// short-circuits the retry path: a banned address goes straight
		// to replaceSlotLocked + fill(). See setup_failure.go.
		IsSetupFailureBannedFn: svc.IsSetupFailureBanned,
		// Storm-protection pacer. The interval/burst pair caps outbound
		// dial spawn rate so reconnect storms (e.g. cm_session_setup_failed
		// cascades against bootstrap nodes) cannot peg CPU by launching
		// every dial worker in the same tick. Manual addpeer bypasses
		// the pacer — see ConnectionManager.handleManualPeer.
		DialPacerInterval: dialPacerProductionInterval,
		DialPacerBurst:    dialPacerProductionBurst,
	})

	// Initialize distance-vector routing table (Phase 1.2).
	//
	// MaxNextHopsPerOrigin pulls the configured cap (Stage B). The
	// production default is routing.DefaultMaxNextHopsPerOrigin (4)
	// after the second rollout release; the first release shipped
	// with 0 (cap disabled) so deployments observed pre-cap
	// behaviour during the soak period. Operators set
	// CORSA_MAX_NEXT_HOPS_PER_ORIGIN=0 explicitly to roll back. See
	// the "RIB compaction (MaxNextHopsPerOrigin cap)" section in
	// docs/routing.md and the DefaultMaxNextHopsPerOrigin constant
	// docstring for the eviction-policy and two-layer-default
	// contract.
	// Phase 1 P2/P3 knobs. The SeqNo flap cap and fast-invalidation
	// thresholds activate when the operator-configured values are
	// positive; zero (or negative) keeps the corresponding path
	// disabled — same shape as MaxNextHopsPerOrigin. Defaults come
	// from the env-var readers in internal/core/config and fall back
	// to the package-level constants documented on routing.Default*.
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(domain.PeerIdentityFromWire(id.Address)),
		routing.WithMaxNextHopsPerOrigin(cfg.MaxNextHopsPerOrigin),
		routing.WithMaxSeqAdvancePerWindow(cfg.MaxSeqAdvancePerWindow),
		routing.WithSeqAdvanceWindow(cfg.SeqAdvanceWindow),
		routing.WithMaxSaneHops(cfg.MaxSaneHops),
		routing.WithProbeBackoff(cfg.ProbeBackoffEnabled),
	)
	svc.router = NewTableRouter(svc, svc.routingTable)

	// Configurable announce interval and overload-mode gate. Both
	// default to the existing pre-Phase-0 behaviour when their config
	// knobs are zero.
	//
	// AnnounceInterval lets densely-connected receivers cut delta-cycle
	// CPU by raising the period (e.g. 60s) at the cost of
	// state-propagation granularity. Forced-full-sync cadence is
	// independently capped at DefaultTTL/2 inside the AnnounceLoop, so
	// freshness invariants hold regardless of the configured interval.
	//
	// OverloadGate (overloadMonitor) skips delta cycles for peers that
	// don't owe a forced full sync this round when the host is
	// CPU/backlog-saturated. Goroutine count is the proxy; threshold is
	// configured via CORSA_OVERLOAD_GOROUTINE_THRESHOLD and disabled by
	// default (zero threshold). Forced-full-sync still fires on schedule
	// so receivers never see stale routes past TTL/2.
	svc.overloadMonitor = newOverloadMonitor(cfg.OverloadGoroutineThreshold)
	announceLoopOpts := []routing.AnnounceLoopOption{
		routing.WithOverloadGate(svc.overloadMonitor),
		// Trigger pacing: under a route-churn storm every table
		// mutation (withdrawal, quarantine invalidation, MarkInvalid)
		// calls TriggerUpdate, and without pacing each trigger runs a
		// full per-peer delta pass immediately — a sustained CPU burn
		// that also feeds churn back into the mesh. Pacing coalesces
		// all triggers within the window into one deferred cycle;
		// periodic and forced-full cadence are unaffected. See
		// routing.DefaultTriggerMinSpacing.
		routing.WithTriggerMinSpacing(routing.DefaultTriggerMinSpacing),
	}
	if cfg.AnnounceInterval > 0 {
		announceLoopOpts = append(announceLoopOpts, routing.WithAnnounceInterval(cfg.AnnounceInterval))
	}
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		svc,
		svc.routingCapablePeers,
		announceLoopOpts...,
	)

	// Phase 2 probe registry — sender bookkeeping for outstanding
	// route_probe_v1 frames. The timeout callback fires
	// routing.Table.MarkProbeFailure for pairs whose ack does not
	// arrive within HealthProbeTimeout; the registry's mutex is
	// disjoint from t.mu so the timeout fire does not nest locks.
	// See docs/protocol/route_health.md / §2.6.
	svc.probeRegistry = newProbeRegistry(
		routing.HealthProbeTimeout,
		nil, // production clock; tests override via newProbeRegistry directly
		svc.routingTable.MarkProbeFailure,
	)

	// Phase 2 route_query_v1 rate limit — per-target sliding
	// window cap to protect neighbours from a query storm when a
	// node's relay loop repeatedly hits Bad/Dead Lookup outcomes.
	// Production budget = queryFanOutLimit per queryRateWindow
	// (overview §7.5, docs/protocol/route_health.md).
	svc.queryRateLimit = newQueryRateLimit(nil, 0, 0)

	// Datagram transport layer (docs/refactoring/datagram-transport.md).
	// Built after the routing table, because the scheduler's resolver reads
	// it, and before anything can dispatch a frame.
	//
	// A construction failure opts the node OUT rather than aborting startup:
	// the two capabilities and the conveyor are one statement, and a node
	// that advertises "send me datagrams" while having nothing to process
	// them with would attract exactly the traffic it cannot serve. Clearing
	// the flag is what keeps the advertisement and the reality in one place —
	// localDatagramAdvertise reads cfg, and cfg now says no.
	if layer, err := newDatagramLayer(svc, svc.datagramMetrics); err != nil {
		log.Error().Err(err).Msg("datagram_layer_disabled_construction_failed")
		svc.cfg.EnableDatagramV1 = false
	} else {
		svc.datagramPlane.Store(layer)
	}

	// The node's own signed identity record (docs/protocol/identity-lookup.md
	// §4.1). Issued AFTER the datagram layer so the declared dtypes mirror
	// the handshake declaration exactly (localDTypeStrings): absent while the
	// plane is down, the registry's explicit set otherwise. A binary upgrade
	// or rollback that changed the set re-issues with a bumped seq here.
	// A failure is the same class as a trust-store load failure — the node
	// must not run half-identified — hence the same panic.
	selfRecordNetwork, err := domain.ParseNetworkID(networkName)
	if err != nil {
		panic(fmt.Errorf("parse network id %q: %w", networkName, err))
	}
	selfDTypes := domain.AbsentDTypes()
	if svc.localDatagramAdvertise().Endpoint {
		selfDTypes = domain.ExplicitDTypes(svc.localDatagramDTypes())
	}
	selfRecord, selfRecordBody, err := ensureSelfIdentityRecord(svc.trust, id, selfRecordSpec{
		network: selfRecordNetwork,
		dm:      !cfg.DisableDirectMessages,
		dtypes:  selfDTypes,
	}, time.Now())
	if err != nil {
		panic(fmt.Errorf("ensure self identity record: %w", err))
	}
	svc.selfRecord = selfRecord
	svc.selfRecordBody = selfRecordBody

	// Identity lookup engine: constructed after the self-record so the
	// datagram handlers registered above observe a fully identified node.
	svc.identityResolver = newIdentityResolver(svc, loadIdentityIntentStore(cfg.IdentityIntentsPath), selfRecordNetwork)

	svc.relayStates = newRelayStateStore()
	svc.relayLimiter = newRelayRateLimiter()
	svc.announceLimiter = newAnnounceRateLimiter()
	svc.connLimiter = newConnRateLimiter()
	svc.cmdLimiter = newCommandRateLimiter()
	svc.inboundByIP = make(map[string]int)

	// Compute initial aggregate status from restored health entries so that
	// fetch_aggregate_status returns a correct value immediately after
	// restart, before any peer events arrive. Without this, the status
	// stays at the zero-value "offline" even when restored peers are in
	// reconnecting state — which mis-drives bootstrap policy decisions.
	//
	// NewService is single-threaded during startup (no other goroutine
	// sees svc yet), but refreshAggregateStatusLocked's contract still
	// requires the canonical peerMu → deliveryMu → statusMu stack; we
	// take it explicitly so future maintainers do not inherit a hidden
	// contract violation.
	svc.peerMu.Lock()
	svc.deliveryMu.RLock()
	svc.statusMu.Lock()
	svc.refreshAggregateStatusLocked()
	svc.statusMu.Unlock()
	svc.deliveryMu.RUnlock()
	svc.peerMu.Unlock()

	return svc
}

// NewServiceWithNetwork builds a Service like NewService and then pins its
// transport surface to a caller-supplied netcore.Network implementation.
// This is the single injection seam used by tests (see
// internal/core/netcore/netcoretest) to drive protocol logic against an
// in-memory transport without binding to real sockets. Production callers
// must use NewService — passing a non-bridge Network breaks the invariant
// that Service.Network() reflects the live s.conns registry.
//
// When network is nil, the function panics rather than silently downgrading
// to the default bridge: accepting a nil here would defeat the compile-time
// opt-in this constructor exists to provide.
func NewServiceWithNetwork(cfg config.Node, id *identity.Identity, network netcore.Network) *Service {
	if network == nil {
		panic("node.NewServiceWithNetwork: network is nil (use NewService for the default bridge)")
	}
	svc := NewService(cfg, id, nil)
	svc.networkOverride = network
	return svc
}

// RegisterMessageStore sets the optional handler for message persistence.
// Must be called before Run(). Desktop nodes register a store so the UI layer
// owns chatlog; relay-only nodes skip this — messages are relayed but not stored.
func (s *Service) RegisterMessageStore(store MessageStore) {
	s.messageStore = store
}

// goRunLoop starts one of the loops that live for the whole of Run and stop on
// the lifecycle context, tracked by runLoopsWg so stopRunLifecycle can wait for
// it whether or not it ever got as far as being started.
//
// # It is the ONLY way Run may start a long-lived goroutine
//
// Cancellation only ASKS a loop to finish. A loop that is merely cancelled can
// still be inside a network send, a TTL drain or an event publication when Run
// returns, and Run returning is what the runtime treats as permission to close
// the stores and sockets underneath it. Four lifecycle findings in four rounds
// were all this same gap in a different place, so the rule is no longer a
// convention: TestRunStartsNoUnjoinedGoroutine parses this file, walks Run's
// body for `go` statements and fails on any that is not carrying an explicit
// `lifecycle:` justification. A future loop added with a bare `go` turns red
// with a message naming this function; it cannot be forgotten, only refused on
// purpose and in writing.
func (s *Service) goRunLoop(fn func()) {
	s.runLoopsWg.Add(1)
	go func() {
		defer s.runLoopsWg.Done()
		defer crashlog.DeferRecover()
		fn()
	}()
}

// stopRunLifecycle is the ONE ordered teardown of everything Run started: ASK
// every lifecycle loop to stop, then WAIT for all of them.
//
// It is armed before the first loop exists, so it holds on EVERY exit: an
// ordinary return, an error return, and a panic unwinding from anywhere below.
// Three rounds of shutdown defects came from a teardown that was ordered
// correctly only from the point it happened to be registered; this one has no
// such point.
//
// The two steps are both load-bearing and neither may be reordered:
//
//  1. CANCEL. Every loop on runLoopsWg stops on the lifecycle context and on
//     nothing else, so the wait below cannot finish until it is cancelled.
//     Joining without cancelling first is a deadlock, not a slow shutdown;
//  2. WAIT. Cancellation only ASKS: a loop already inside a call runs until that
//     call returns, and what those calls reach is owned by subsystems whose
//     defers sit ABOVE this one and therefore run AFTER it — the file-transfer
//     manager, the relay states, the capture manager and `s.done`. The datagram
//     plane's outbound pump hands frames to the network writer and
//     bootstrapLoop writes peers.json on its way out; a caller entitled to
//     delete the data directory must not be handed control — or a panic —
//     before both have finished.
//
// # The inbound-connection drain is NOT on that list, and runs BEFORE the join
//
// Its defer sits BELOW this one, so LIFO runs `closeAllInboundConns` +
// `connWg.Wait` FIRST, with every lifecycle loop still live. That is the right
// way round rather than an oversight, and it is pinned by
// TestTheInboundDrainRunsBeforeTheLifecycleJoin:
//
//   - the drain STOPS NO SUBSYSTEM a loop calls into, which is the whole hazard
//     this join exists to prevent. It closes sockets and clears per-connection
//     rows — the conn registry, the subscribers, the conn auth, the inbound
//     health refs. A loop reaches a connection only through the netcore
//     registry, which answers "unknown conn" for a closed ConnID instead of
//     handing out a freed handle, and an absent per-connection row is the
//     ordinary state every peer disconnect already produces;
//   - the dependency that IS load-bearing runs the other way. The connection
//     handlers are PRODUCERS into what the loops consume — the CM event loop
//     (`EmitSlot`), the gossip lanes, the announce plane — and their teardown
//     path produces its last work as it exits: `trackInboundDisconnect` →
//     `markPeerDisconnected`, `removeSubscriberConnID`, the session-closed
//     routing bookkeeping. Joining the handlers while those consumers are still
//     alive is the order in which that work can still be absorbed; the reverse
//     leaves producers running against consumers joined a moment earlier;
//   - the drain must in any case precede
//     `cancelAllPendingWithdrawalsForShutdown`, whose defer sits between the
//     two, so it cannot be moved below this one on its own.
//
// One group and one wait, not a sequence of per-subsystem joins: every loop is
// asked to stop by the same cancel, so `wait(A); wait(B)` and `wait(A ∪ B)`
// release the caller at exactly the same instant, and a sequence invites the
// reader to believe in an ordering it does not have.
func (s *Service) stopRunLifecycle(cancel context.CancelFunc) {
	cancel()
	s.runLoopsWg.Wait()
}

func (s *Service) Run(ctx context.Context) error {
	// The Service lifecycle context is DERIVED from the caller's and cancelled
	// on ANY exit from Run, not only on the caller cancelling.
	//
	// Storing the caller's context here made "the Service is running" and "the
	// context is live" two different facts. Every error return — a listen that
	// fails because the address is already bound is the ordinary one — handed
	// the caller an error while the background work Run had already started
	// kept running under a context nobody would ever cancel. The datagram plane
	// is the case that bites: its outbound pump keeps writing to sockets, and a
	// caller that has been given an error is entitled to close them.
	//
	// The two defers below are the FIRST ones registered, so they run LAST and
	// no return path, and no panic, can skip them: cancel, then join the work
	// whose external effects must not outlive Run.
	runCtx, runCancel := context.WithCancel(ctx)
	// Store context so CM callbacks can start goroutines bound to the
	// Service lifecycle (see onCMSessionEstablished).
	s.runCtx = runCtx
	// The BACKSTOP, for a panic raised before the ordered shutdown sequence
	// below is registered. It is idempotent, so on every ordinary path the
	// sequence has already done it and this is a no-op.
	defer runCancel()
	ctx = runCtx

	log.Info().
		Int("pid", os.Getpid()).
		Str("identity", s.identity.Address).
		Str("listen", s.cfg.ListenAddress).
		Str("node_type", string(s.cfg.Type)).
		Msg("node_service_starting")

	// Signal drain goroutines to stop when Run exits. Drain goroutines
	// launched by onPeerSessionEstablished and handleAnnounceRoutes check
	// this channel before doing any work — prevents them from running
	// against a half-torn-down Service during shutdown.
	defer close(s.done)

	// Traffic capture manager — diagnostic feature (plan §4.5).
	s.initCaptureManager()
	defer s.captureManager.Close()
	// Startup traffic recording (env: CORSA_RECORD_ALL_TRAFFIC, default off).
	s.startConfiguredCapture()

	// Phase 3 PR 12.2 — wire the hop-ack budget timeout callback so the
	// routing.Table reputation primitive learns about (Recipient,
	// ForwardedTo) pairs that never produced a hop_ack. The handler
	// lives in routing_relay.go alongside the other relay-plane wiring.
	s.relayStates.onHopAckTimeout = s.onRelayHopAckTimeout
	// Start relay state TTL ticker; stopped on shutdown.
	s.relayStates.start()
	defer s.relayStates.stop()

	// Initialize file transfer subsystem (Iteration 21).
	s.initFileTransfer()
	defer s.stopFileTransfer()

	// THE ORDERED TEARDOWN, armed BEFORE the first lifecycle loop exists so it
	// holds on every exit — an ordinary return, an error, or a panic unwinding
	// from anywhere below. Registering it HERE, immediately after the subsystem
	// defers above and before every `go` below, is what makes `defer`'s LIFO
	// order put the JOIN ahead of `stopFileTransfer`, `relayStates.stop`,
	// `captureManager.Close` and `close(s.done)`: a loop inside a call into one
	// of those must not have it stopped underneath it. It is correct for loops
	// that were never started, which is why it can be armed before any of them
	// is (see the runLoopsWg field comment).
	//
	// The two defers registered BELOW — the pending-withdrawal cancel and the
	// inbound-connection drain — therefore run BEFORE this join, by design and
	// not by accident; stopRunLifecycle's own comment carries the argument.
	defer s.stopRunLifecycle(runCancel)

	// Datagram transport schedules (datagram_layer.go). No-op — and no
	// goroutine — when the plane is not wired. They are ordinary lifecycle
	// loops: they stop on ctx.Done and are joined by the teardown just armed.
	s.startDatagramLayer(ctx)

	// Identity lookup engine (identity_resolver.go). Reseeds the background
	// phase from durable intents and then serves StartResolution calls; a
	// node without the datagram plane keeps the loop — the routing gate
	// refuses sends until a plane exists.
	if s.identityResolver != nil {
		s.goRunLoop(func() { s.identityResolver.run(ctx) })
	}

	// Start routing table TTL ticker and announce loop (Phase 1.2).
	routingCtx, routingCancel := context.WithCancel(ctx)
	defer routingCancel()

	s.goRunLoop(func() { s.announceLoop.Run(routingCtx) })
	s.goRunLoop(func() { s.routingTableTTLLoop(routingCtx) })
	// Phase 2 probe sender (PR 11.3c). Walks Questionable
	// (Identity, Uplink) pairs every HealthProbeInterval and emits
	// route_probe_v1; the outstanding-probe registry arms a
	// HealthProbeTimeout watcher that converts no-ack outcomes into
	// MarkProbeFailure. Honours the Phase 0 overload gate.
	s.goRunLoop(func() { s.probeLoop(routingCtx) })

	// On shutdown: cancel any pending route-withdrawal probation
	// timers AFTER the closeAllInboundConns + connWg.Wait deferred
	// below has completed (defers run LIFO, so this one runs LAST).
	// Ordering rationale: while inbound goroutines are still alive
	// they can call onPeerSessionClosed which schedules new pending
	// timers via maybeScheduleDeferredWithdrawal. If we cancelled
	// here too early, those late schedules would leak and could
	// fire against a Service whose routing-layer state is already
	// being torn down by the CM defer below. Running cancel as the
	// LAST defer guarantees no new timers can be scheduled after we
	// stop them.  See routing_withdrawal_grace.go.
	defer s.cancelAllPendingWithdrawalsForShutdown()

	// On shutdown: close all inbound connections so handleConn goroutines
	// exit, wait for them to finish.
	//
	// Registered BELOW `defer s.stopRunLifecycle`, so LIFO runs this drain
	// FIRST, while every lifecycle loop is still live. The handlers are
	// producers into what those loops consume, and the drain stops nothing the
	// loops call into — see stopRunLifecycle for the whole argument, and
	// TestTheInboundDrainRunsBeforeTheLifecycleJoin for the pin.
	defer func() {
		s.closeAllInboundConns()
		log.Info().Msg("waiting for inbound connections to finish")
		s.connWg.Wait()
	}()

	// Start ConnectionManager event loop (Stage 3).
	s.goRunLoop(func() { s.connManager.Run(ctx) })
	// Wait for CM event loop to be ready before starting bootstrap.
	<-s.connManager.Ready()
	if s.primeBootstrapOnRun {
		s.primeStartupBootstrapPeers()
	}
	// Apply the CORSA_CONNECT_ONLY startup seed after bootstrap priming so the
	// pinned target is registered against the freshly primed peer set and any
	// bootstrap-driven outbound slots are dropped right away.
	s.applyStartupConnectOnly()

	// Bounded gossip-dispatch pool (gossip_dispatch.go): must be up
	// before bootstrapLoop's first retryRelayDeliveries tick enqueues
	// its fan-out, otherwise those sends take the per-goroutine
	// fallback path.
	s.startGossipDispatch(ctx)

	s.goRunLoop(func() { s.bootstrapLoop(ctx) })

	// Prime the atomic snapshots consumed by the hot local RPC paths
	// (fetch_network_stats, fetch_peer_health, get_peers) synchronously on
	// the Run goroutine BEFORE the listener opens.  This establishes the
	// invariant that every hot-path handler sees a non-nil snapshot on its
	// first load, which lets those handlers drop the sync-rebuild fallbacks
	// that otherwise re-couple the RPC path to cm.mu.RLock / s.peerMu.RLock.
	// Running on the main goroutine here means the CM is already Ready()
	// (see above) and no inbound connection can dispatch RPCs yet.
	s.primeHotReadSnapshots()

	// Background refresher for the atomic snapshots consumed by the hot
	// local RPC paths.  Runs in its own goroutine so a stalled rebuild
	// (s.peerMu writer storm) delays the next refresh tick without
	// affecting any other loop; RPC readers keep serving the last good
	// snapshot.  See hot_reads_refresh.go for the contract.
	s.goRunLoop(func() { s.hotReadsRefreshLoop(ctx) })

	// A fault injected at the LATEST point of Run's startup, where every
	// subsystem defer is registered and every loop is running. Nothing in Run's
	// own startup panics on demand, so the shutdown-ordering contract — the
	// lifecycle join runs BEFORE the subsystem teardowns registered above it —
	// is checked by unwinding from here; nil in production.
	if s.faultDuringRunStartup != nil {
		s.faultDuringRunStartup()
	}

	if !s.cfg.EffectiveListenerEnabled() {
		<-ctx.Done()
		return nil
	}

	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.ListenAddress, err)
	}
	defer func() { _ = listener.Close() }()

	log.Trace().Str("site", "Run_setListener").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "Run_setListener").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.listener = listener
	s.peerMu.Unlock()
	log.Trace().Str("site", "Run_setListener").Str("phase", "lock_released").Msg("peer_mu_writer")

	// Joined like every other lifecycle goroutine: it ends the moment the
	// context is cancelled, which stopRunLifecycle does before it waits, so
	// joining it costs nothing and keeps Run's rule free of exceptions.
	s.goRunLoop(func() {
		<-ctx.Done()
		_ = listener.Close()
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			return fmt.Errorf("accept connection: %w", err)
		}

		// Per-IP connection rate limiting — reject before allocating any
		// per-connection resources (goroutines, maps, buffers). Prevents
		// SYN flood and connection exhaustion from a single source.
		ip := remoteIP(conn.RemoteAddr())
		if !s.disableRateLimiting && !s.connLimiter.allowConnect(ip) {
			log.Warn().Str("ip", ip).Str("reason", "conn-rate-limit").Msg("reject connection")
			_ = conn.Close()
			continue
		}

		// Per-IP concurrent connection cap — even within rate limits, no
		// single IP should monopolize connection slots.
		if !s.disableRateLimiting && !s.tryIncrementIPConn(ip) {
			log.Warn().Str("ip", ip).Str("reason", "max-conn-per-ip").Msg("reject connection")
			_ = conn.Close()
			continue
		}

		s.connWg.Add(1)
		// lifecycle: joined by connWg, not runLoopsWg. A connection handler is
		// per-CONNECTION rather than per-Run: it ends when its socket closes,
		// and the teardown that owns it is the closeAllInboundConns +
		// connWg.Wait defer above, which must run at its own point in the
		// order. Tracking it in the lifecycle group as well would give one
		// goroutine two owners.
		go func(c net.Conn, cip string) {
			defer s.connWg.Done()
			defer crashlog.DeferRecover()
			defer s.decrementIPConn(cip)
			s.handleConn(c)
		}(conn, ip)
	}
}

func (s *Service) ListenAddress() string {
	return s.cfg.ListenAddress
}

func (s *Service) NodeType() domain.NodeType {
	return s.cfg.NormalizedType()
}

func (s *Service) Services() []string {
	return s.cfg.ServiceList()
}

func (s *Service) ClientVersion() string {
	if strings.TrimSpace(s.cfg.ClientVersion) == "" {
		return config.CorsaVersion
	}
	return s.cfg.ClientVersion
}

func (s *Service) CanForward() bool {
	return s.NodeType().IsFull()
}

func (s *Service) Address() string {
	return s.identity.Address
}

// WaitBackground blocks until every goroutine this Service owns has finished:
// the fire-and-forget jobs tracked by backgroundWg and the lifecycle loops
// tracked by runLoopsWg. Tests call this before TempDir cleanup to avoid
// "directory not empty" races caused by async disk writes.
//
// The two groups stay apart because RUN may only join one of them: backgroundWg
// tracks fire-and-forget jobs across the whole Service, some of them started
// during the very teardown Run is performing, so joining it from inside Run
// would trade a use-after-teardown for a shutdown deadlock. This function is
// the one place callers see both.
func (s *Service) WaitBackground() {
	s.backgroundWg.Wait()
	s.runLoopsWg.Wait()
}

// goBackground runs fn in a new goroutine that is tracked by
// backgroundWg, so WaitBackground observes it on shutdown. Use this
// helper for every fire-and-forget goroutine that mutates shared
// state or performs still-durable writes (e.g. trust-store / peers.json
// writes), so the work cannot race with TempDir cleanup in tests or with
// process exit in production.
//
// Add(1) is called on the caller's goroutine before the spawn so
// WaitBackground cannot observe the zero counter between spawn
// request and goroutine start.
func (s *Service) goBackground(fn func()) {
	s.backgroundWg.Add(1)
	go func() {
		defer s.backgroundWg.Done()
		fn()
	}()
}

func (s *Service) SubscriberCount(recipient string) int {
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()
	return len(s.subs[recipient])
}

// SubscribeLocalChanges registers a local-change inbox and returns it with
// the cancel that unregisters and closes it.
//
// The returned cancel closes ch, which is only safe because s.gossipMu
// fences the publisher: emitLocalChange holds gossipMu.RLock across its
// offers, so the Lock below waits out every publisher already inside one and
// refuses every later one (the map entry is gone). Snapshotting the
// subscriber set and offering after the unlock — which is how the publisher
// used to work — makes the same close land on a live sender, and a send on a
// closed channel is a panic in the middle of an unrelated goroutine.
func (s *Service) SubscribeLocalChanges() (<-chan protocol.LocalChangeEvent, func()) {
	ch := make(chan protocol.LocalChangeEvent, 16)

	log.Trace().Str("site", "SubscribeLocalChanges_register").Str("phase", "lock_wait").Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "SubscribeLocalChanges_register").Str("phase", "lock_held").Msg("gossipMu_writer")
	s.events[ch] = struct{}{}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "SubscribeLocalChanges_register").Str("phase", "lock_released").Msg("gossipMu_writer")

	cancel := func() {
		log.Trace().Str("site", "SubscribeLocalChanges_cancel").Str("phase", "lock_wait").Msg("gossipMu_writer")
		s.gossipMu.Lock()
		log.Trace().Str("site", "SubscribeLocalChanges_cancel").Str("phase", "lock_held").Msg("gossipMu_writer")
		if _, ok := s.events[ch]; ok {
			delete(s.events, ch)
			close(ch)
		}
		s.gossipMu.Unlock()
		log.Trace().Str("site", "SubscribeLocalChanges_cancel").Str("phase", "lock_released").Msg("gossipMu_writer")
	}

	return ch, cancel
}

// handleConn is the inbound entry boundary. It is net.Conn-first by the
// carve-out list in conn_registry.go: this is the point where a raw socket
// first enters the registry and a domain.ConnID is born. Downstream
// dispatch, auth, subscriber and inbound bookkeeping paths run on ConnID
// alone — RemoteAddr, SendFrame and Close are reached through the
// netcore.Network registry rather than a captured *netcore.NetCore handle.
func (s *Service) handleConn(conn net.Conn) {
	if !s.disableRateLimiting && s.isBlacklistedConn(conn) {
		// The peer-banned notice was already delivered on the session
		// that tripped the blacklist (see addBanScore), so a raw
		// reconnect from the same IP is closed silently. No managed
		// writer is available at this point (pre-registration) and we
		// must not reintroduce a raw conn.Write path here — the
		// dialler-side gate 6c.2 is already armed from the first notice
		// and will suppress further retries until the remote ban window
		// elapses.
		_ = conn.Close()
		return
	}
	metered := netcore.NewMeteredConn(conn)
	if !s.registerInboundConn(metered) {
		log.Warn().Str("addr", conn.RemoteAddr().String()).Str("reason", "max-connections").Msg("reject connection")
		_ = conn.Close()
		return
	}
	// Resolve ConnID once at the entry boundary — after a successful
	// registerInboundConn the mapping is guaranteed to exist. The captured
	// id is plumbed through the read loop and frame dispatch; downstream
	// helpers reach the connection state through the netcore.Network
	// registry (RemoteAddr, SendFrame, Close) instead of holding a
	// *netcore.NetCore handle.
	connID, _ := s.connIDFor(metered)
	var peerOfflineEvidence *peerOfflineEvidence

	// Capture lifecycle hook: notify manager about the new inbound
	// connection so standing rules (by_ip, all) can auto-start capture.
	// Also attach the capture sink to the NetCore for outbound tap. The
	// bridge reads RemoteIP / PeerDir back through the registry, so the
	// raw net.Conn no longer crosses the §2.9 boundary.
	s.notifyCaptureNewConn(connID)

	defer func() {
		// Capture lifecycle hook: stop capture for this connection.
		s.notifyCaptureConnClosed(connID)

		if addr := s.inboundPeerAddress(connID); addr != "" {
			s.trackInboundDisconnectWithPresenceEvidence(connID, addr, peerOfflineEvidence)
		}
		s.accumulateInboundTraffic(metered)
		// Close TCP before waiting for the writer goroutine to drain.
		// Without this, the writer might be stuck in conn.Write with a
		// 30-second deadline and unregisterInboundConn would hang waiting
		// for writerDone. Closing the socket unblocks conn.Write with an
		// error, letting the writer exit promptly.
		_ = metered.Close()
		s.unregisterInboundConn(metered)
		s.removeSubscriberConnID(connID)
		s.clearConnAuth(connID)
	}()

	log.Info().Str("addr", conn.RemoteAddr().String()).Msg("incoming connection")
	enableTCPKeepAlive(conn)
	conn = metered

	var heartbeatStop chan struct{}
	// The heartbeat is joined to THIS handler, and closing its stop channel is
	// not the join. Closing only ASKS: the loop can be inside a ping send or
	// between its two selects, and it touches the Network and peerMu after
	// that. Without the wait, connWg — and therefore Run — could complete while
	// a heartbeat was still writing to a socket the runtime is entitled to have
	// closed. The wait is bounded: every branch of the loop selects on stop,
	// and the one call that is not a select is a frame write under netcore's
	// per-write deadline.
	var heartbeatDone sync.WaitGroup
	defer func() {
		if heartbeatStop != nil {
			close(heartbeatStop)
		}
		heartbeatDone.Wait()
	}()

	reader := bufio.NewReader(conn)
	connKey := conn.RemoteAddr().String()
	defer s.cmdLimiter.removeConn(connKey)
	for {
		// Set a read deadline before each frame read. This prevents
		// Slowloris-style attacks where a peer opens a connection and
		// sends data extremely slowly (or not at all) to hold the
		// connection slot and goroutine indefinitely. Legitimate peers
		// send heartbeat pings every 30s, so 120s is generous.
		if tc, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = tc.SetReadDeadline(time.Now().Add(inboundReadTimeout))
		}

		line, err := s.readInboundCommandLine(reader, connID)
		if errors.Is(err, errInboundLineDropped) {
			// One line consumed and dropped by admission; the connection lives
			// on and the next line is read from where that one ended.
			continue
		}
		if err != nil {
			if sessionCloseProvidesPeerOfflineEvidence(err) {
				peerOfflineEvidence = s.observePeerOffline()
			}
			s.endInboundReadLoop(connID, conn.RemoteAddr().String(), err)
			return
		}

		// Capture tap: record raw inbound line before any parsing (plan §7.2).
		// Strip only the transport newline, not all whitespace — leading
		// spaces/tabs are part of the wire payload for diagnostic purposes.
		s.captureInboundRecv(connID, strings.TrimRight(line, "\r\n"))

		// Per-connection command rate limiting — prevents a single peer
		// from flooding with valid commands to exhaust CPU. WHICH lines skip
		// it, and why the question is asked of the whole line rather than of a
		// peeked type, is frameLineExemptFromCommandLimit.
		if !s.admitInboundCommandLine(connID, connKey, line) {
			log.Debug().Str("addr", conn.RemoteAddr().String()).
				Msg("inbound_read_loop: closing connection — command rate limit exceeded")
			_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeRateLimited})
			s.addBanScore(connID, banIncrementRateLimit)
			return
		}

		// The line goes down UNTRIMMED. Trimming is a parser convenience and
		// dispatchNetworkFrame does it for itself; doing it here instead would
		// destroy the only copy of the wire bytes, and the datagram plane
		// needs exactly those — §5 charges the neighbour's byte budget on the
		// raw line and §3.4 requires the strict parser to read the same one
		// (see rawLineForDispatch for the full argument and for why the
		// announce-plane types stay on the trimmed form).
		if !s.handleCommand(connID, line) {
			return
		}

		// Start the inbound heartbeat goroutine once the peer is fully
		// connected (tracked in inboundHealthRefs). For authenticated
		// peers this happens after auth_session; for unauthenticated
		// peers — immediately after hello. Both sides ping independently
		// so that each node has its own proof of liveness regardless of
		// who initiated the TCP connection.
		if heartbeatStop == nil {
			if addr := s.trackedInboundPeerAddress(connID); addr != "" {
				// Heartbeat liveness ping is driven via netcore.Network.SendFrame
				// using ConnID; if the registry no longer knows about this
				// ConnID (race with teardown) we skip starting the loop. The
				// nil-check on the registry-resolved address mirrors the
				// previous *netcore.NetCore nil-guard.
				if s.Network().RemoteAddr(connID) != "" {
					heartbeatStop = make(chan struct{})
					heartbeatDone.Add(1)
					// lifecycle: joined by THIS connection's handler, which
					// waits on heartbeatDone before returning; the handler is
					// itself tracked by connWg, which Run's teardown joins. A
					// per-connection goroutine belongs to its connection, not
					// to runLoopsWg — one goroutine, one owner.
					go func() {
						defer heartbeatDone.Done()
						s.inboundHeartbeat(connID, addr, heartbeatStop)
					}()
				}
			}
		}
	}
}

// admitInboundCommandLine is the read loop's whole rate decision for ONE
// inbound line: a line whose plane carries its own budget passes for free,
// every other line spends a token of the per-connection command limiter.
//
// It exists as a function so the composition can be tested. Its two halves are
// individually harmless — an exemption that never charges, a limiter that never
// exempts — and every defect this decision has had lived in how they were put
// together.
func (s *Service) admitInboundCommandLine(id domain.ConnID, connKey string, line string) bool {
	if s.frameLineExemptFromCommandLimit(id, line) {
		return true
	}
	return s.cmdLimiter.allowCommand(connKey)
}

// frameLineExemptFromCommandLimit reports whether an inbound command line may
// skip the per-connection command limiter. It is the WHOLE of that decision:
// the read loop asks this and nothing else.
//
// # Why the LINE and not a peeked type
//
// The exemption used to be taken from peekFrameType, which returns the FIRST
// `"type"` found anywhere in the line — nested objects included — while
// encoding/json binds the LAST TOP-LEVEL one. That gap is not a mismatch but a
// bypass, because the SENDER picks which reader gets which answer:
// `{"a":{"type":"file_command"},"type":"ping"}` left the limiter as a
// file_command and was then dispatched as a `ping`, which is a handshake
// command that needs no session — so any socket could hold this node at line
// rate for free. The exemption is therefore decided by the same classification
// the pre-parse diversion and the peer-session admission already decide on
// (classifyFrameLine, through topLevelFrameType), and by nothing else.
//
// # Why an unreadable type pays
//
// FAIL-CLOSED: an exemption is sound only while this scan and the parser name
// the same type, so a line that does not name itself unambiguously is charged
// and the dispatcher below is left to decide what it was. Nothing legitimate is
// charged twice by that rule — every frame this node's peers marshal names its
// type once, literally, as a plain string.
//
// # The exempt classes
//
//  1. file_command — high-throughput data plane (chunk_request /
//     chunk_response) that easily exceeds the control-plane rate (30 cmd/s).
//     Gated behind auth + file_transfer_v1 capability in dispatchNetworkFrame,
//     so the attack surface is limited to authenticated peers.
//
//  2. announce-plane BULK frames — see exemptFrameTypeFromCommandLimit for the
//     boundary and for the control frames deliberately left inside the limiter.
//
//  3. `datagram`, and only while a layer exists to charge the §5 budget that
//     replaces the limiter — datagramCarriesOwnBudget owns that condition, and
//     it is the reason this decision needs the ConnID: the replacement budget
//     is charged on the identity the neighbour PROVED, so before `auth_ok`
//     there is nobody to bill and an exemption would be a free channel rather
//     than a budget swap.
//
// Class 3 is the only one whose answer depends on the CONNECTION rather than
// on the line, and resolving its key is a registry read — so the question is
// asked for a `datagram` and for nothing else. The two branches are disjoint:
// no member of the classes above is a datagram.
func (s *Service) frameLineExemptFromCommandLimit(id domain.ConnID, line string) bool {
	claimed, named := topLevelFrameType(line)
	if !named {
		return false
	}
	if claimed != protocol.DatagramFrameType {
		return exemptFrameTypeFromCommandLimit(claimed)
	}
	return s.datagramCarriesOwnBudget(claimed, s.inboundDatagramBudgetKey(id))
}

// errInboundLineDropped reports that the inbound reader consumed and dropped
// ONE line and the connection survives. It never leaves handleConn's loop.
var errInboundLineDropped = errors.New("inbound command line dropped by admission")

// readInboundCommandLine reads ONE line of the inbound command plane under the
// strict budget of §2.3 — maxCommandLineBytes, which IS protocol.MaxFrameLine —
// and decides what a line that breaches it costs.
//
// The staged reader is used for its GATE and not for an extension: no type on
// this reader is entitled to more than the strict budget, so the callback always
// answers zero. What it is there for is the CLAIM in the line's first bytes,
// which is the only name an over-long line will ever have — the rest of it is
// never read, and nothing downstream will classify it.
func (s *Service) readInboundCommandLine(reader *bufio.Reader, connID domain.ConnID) (string, error) {
	claimed := ""
	read, err := readFrameLineStaged(reader, maxCommandLineBytes, func(prefix string) int {
		claimed, _ = claimedFrameTypeFromPrefix(prefix)
		return 0
	})
	if !errors.Is(err, errFrameTooLarge) {
		return read.line, err
	}
	return "", s.refuseOversizeInboundLine(reader, connID, claimed, read)
}

// refuseOversizeInboundLine decides what an over-long inbound line costs, and
// the answer is one of exactly two:
//
//   - a line claiming `datagram` is the datagram plane's business. §2.3 is a
//     verdict about the LINE, and the neighbour that relayed the frame inside it
//     did not write it, so the refusal is silent: the bytes go to that plane's
//     own §5 per-neighbour budget, the drop is counted, the remainder is skipped
//     so it cannot be read as frames of its own, and the CONNECTION LIVES. That
//     rule is stated once, in refuseOversizeDatagramClaim, and is the same one
//     the peer-session reader applies;
//   - everything else ends the connection with a frame-too-large error, exactly
//     as this reader has always answered it.
//
// The claim buys nothing but that: it cannot make a line be processed, only
// refused more quietly and on a narrower budget. With no layer to charge, or a
// neighbour with no billable key — an unauthenticated socket has proven no
// identity — it buys nothing at all and the connection goes.
//
// What the surviving connection is billed is what the two stages REPORT having
// read, not the limit plus the discard: each stops on a buffer fill and so ends
// somewhere past its limit, and a size reconstructed from the constants
// under-charges every refusal by that overshoot — which on this path is the
// only cost the neighbour bears at all.
func (s *Service) refuseOversizeInboundLine(
	reader *bufio.Reader,
	connID domain.ConnID,
	claimed string,
	read frameLineRead,
) error {
	key := s.inboundDatagramBudgetKey(connID)
	discarded, discardErr := 0, error(nil)
	if !read.delimited && claimed == protocol.DatagramFrameType {
		// Skipping the remainder is what makes "the connection lives" possible
		// at all; it is bounded by one further frame, past which the peer is
		// streaming rather than framing and the connection goes as before.
		discarded, discardErr = discardFrameLineRemainder(reader, oversizeDatagramResyncBytes)
	}
	if discardErr != nil {
		return errFrameTooLarge
	}
	if !s.refuseOversizeDatagramClaim(datagramInbound, claimed, key, read.consumed+discarded) {
		return errFrameTooLarge
	}
	return errInboundLineDropped
}

// endInboundReadLoop reports why handleConn's read loop is stopping and answers
// the peer where the protocol says to.
//
// The three cases are the three ways a read ends, and they are separated
// because only the first two say anything on the wire: a size breach and a
// transport failure each get their error code, while an EOF is the peer closing
// a connection there is nobody left to answer on.
func (s *Service) endInboundReadLoop(connID domain.ConnID, addr string, err error) {
	switch {
	case errors.Is(err, errFrameTooLarge):
		// Capture frame-too-large as a diagnostic event before closing.
		s.captureInboundRecvFrameTooLarge(connID)
		log.Debug().Str("addr", addr).
			Msg("inbound_read_loop: closing connection — frame exceeds max size")
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeFrameTooLarge})
	case !errors.Is(err, io.EOF):
		log.Debug().Err(err).Str("addr", addr).
			Msg("inbound_read_loop: closing connection — read error")
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeRead})
	default:
		log.Debug().Str("addr", addr).
			Msg("inbound_read_loop: peer closed connection (EOF)")
	}
}

// exemptFrameTypeFromCommandLimit is the type-level half of the exemption: the
// two classes whose per-peer budget is owned by another limiter on this node.
// It takes a TOP-LEVEL type, resolved as frameLineNamed by its caller — a
// peeked type must never be handed to it.
//
// Bulk announce frames (announce_routes / routes_update / route_announce_v3)
// chunk route batches: a legitimate full-sync of N routes ships as ceil(N/100)
// frames in a tight burst, which the cmd limiter (100 burst, 30/s) would
// silently truncate. They have their own per-peer route-based budget in
// announceLimiter (10,000-route burst, 200 routes/s refill, ALL bulk frames).
// DELTA frames (routes_update / v3 kind="delta") additionally feed the
// chatty_routes quarantine trigger (50 frames/s × 10s = 500); full baselines do
// NOT — a baseline is idempotent and bounded by the route bucket, while chatty
// targets delta churn (see recordInboundAnnounceAndMaybeArm). Together those
// bound CPU AND let quarantine — not TCP close — own the response to a
// misbehaving delta sender, honouring the design contract ("quarantine does NOT
// close TCP").
//
// NOT exempt (intentional, even though they ARE announce-plane): request_resync,
// route_poison_v1 and route_poison_v2. Those are control frames whose natural
// per-peer rate is well under 1/s (request_resync: bounded by reconnect cycles;
// the poison frames: bounded by route lifecycle). The cmd limiter (100 burst /
// 30 cmd/s) is the right defence — exempting them would leave only the loose
// 200-token-per-second route bucket, which permits 200 control frames/s
// sustained, and chatty_routes does NOT count control frames in its trigger
// window (it is wired only into the bulk handlers). For these types, "high-rate
// flood" is protocol misbehaviour rather than chattiness, and a TCP close is the
// appropriate response.
//
// See isAnnouncePlaneBulkFrameType (routing_announce.go) for the predicate
// boundary and the wider isAnnouncePlaneFrameType for the size-budget
// enforcement that still covers control frames.
func exemptFrameTypeFromCommandLimit(frameType string) bool {
	return frameType == protocol.FileCommandFrameType ||
		isAnnouncePlaneBulkFrameType(frameType)
}

// handleCommand validates that the incoming line is JSON framing and then
// delegates to dispatchNetworkFrame. ConnID-first: every downstream helper
// (writeJSONFrameByID, touchConnActivity, addBanScore, …) is reached by
// ConnID; the remote-address string used for protocol_trace logging is
// resolved on demand via the netcore.Network registry.
//
// `wire` is the line as the reader produced it, terminating newline and all.
// It is passed on untouched — protocol.IsJSONLine trims for itself — because
// the datagram plane is defined on those exact bytes (rawLineForDispatch).
func (s *Service) handleCommand(connID domain.ConnID, wire string) bool {
	if !protocol.IsJSONLine(wire) {
		addr := s.Network().RemoteAddr(connID)
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", addr).
			Str("direction", "recv").
			Str("command", "non-json").
			Bool("accepted", false).
			Msg("protocol_trace")
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON})
		return false
	}
	return s.dispatchNetworkFrame(connID, wire)
}

// dispatchNetworkFrame parses and dispatches an inbound wire frame.
// ConnID-first: every ID-based helper (touchConnActivity, connHasCapability,
// addBanScore, writeJSONFrameByID, …) is reached by id; the only datum
// previously taken from a *netcore.NetCore handle — the remote address used
// for protocol_trace logging and the welcomeFrame ObservedAddress hint — is
// now resolved through the netcore.Network registry once up-front. An empty
// RemoteAddr means the connection has already been unregistered (race with
// teardown) and dispatch fails closed without consulting the registry per-branch.
//
// `wire` is the line as read from the socket; `line` below is its trimmed form
// and is what every handler except the datagram one is given, exactly as
// before. Only the datagram case reads `wire`, and rawLineForDispatch — the
// same split the outbound peer-session reader applies — says why.
func (s *Service) dispatchNetworkFrame(connID domain.ConnID, wire string) bool {
	addr := s.Network().RemoteAddr(connID)
	if addr == "" {
		return false
	}

	// A DATAGRAM NEVER REACHES protocol.ParseFrameLine. §4.1 step 1 charges the
	// neighbour's byte and frame budget "before any decoding", and the
	// universal parser is decoding: leaving it above the layer let a neighbour
	// impose a full JSON unmarshal of every datagram-shaped line for free, and
	// the budget only found out afterwards. The diversion runs before the parse
	// and skips it entirely — the strict parser of §3.4 needs the original
	// bytes anyway.
	if isDatagramWireLine(wire) {
		return s.dispatchInboundDatagramWire(connID, addr, wire)
	}

	// AMBIGUITY IS REFUSED UNPARSED, on this path for the same reason as on the
	// peer-session reader (admitFrameLinePreParse): a line whose type only
	// protocol.ParseFrameLine could resolve used to be dispatched on the type
	// the PARSER produced, so a duplicate `type` key ending in `datagram`
	// reached the layer only after the universal unmarshal — the order §4.1
	// step 1 forbids. The reader's own maxCommandLineBytes bounds the decode
	// here, which is why the BUDGET half of admission is not repeated on this
	// path; the ordering half is not about size.
	//
	// The connection survives: this is a drop, not a protocol error, because
	// nothing on the wire proves what the sender meant. Answering invalid_json
	// would name a verdict this node deliberately did not reach.
	if _, verdict := admitFrameLinePreParse(wire); verdict == preParseRefuseAmbiguous {
		s.dropAmbiguousFrameLine(s.inboundDatagramBudgetKey(connID), addr, wire)
		return true
	}

	line := strings.TrimSpace(wire)
	frame, err := protocol.ParseFrameLine(line)
	if err != nil {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", addr).
			Str("direction", "recv").
			Str("command", "").
			Bool("accepted", false).
			Msg("protocol_trace")
		// All sync reply paths below route through sendFrameViaNetworkSync so
		// the injected Network (production bridge or netcoretest.Backend)
		// owns the write. s.runCtx is the lifecycle ctx seeded by
		// NewService and replaced by Run(ctx); we do not fabricate a
		// per-call ctx here. See network_consumer.go for outcome-tree
		// documentation — notably, this helper explicitly evicts on both
		// ErrSendBufferFull and ErrSendTimeout to preserve the legacy
		// enqueueFrameSyncByID pc.Close semantics that bridge.SendFrameSync
		// does not replicate.
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON, Error: err.Error()})
		return false
	}

	// Auth guard for P2P commands. Handshake commands (hello, ping, pong,
	// auth_session) are available to all connections. Every other command
	// on the data port is part of the authenticated P2P wire protocol and
	// requires a completed auth_session. Data-only commands (fetch_messages,
	// send_message, etc.) are not handled here at all — they live
	// exclusively in handleLocalFrameDispatch / RPC HTTP.
	//
	// The role is derived from server-side auth state, never from
	// frame.Client which the attacker controls — eliminating GAP-0.

	accepted := true
	defer func() {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", addr).
			Str("direction", "recv").
			Str("command", frame.Type).
			Bool("accepted", accepted).
			Msg("protocol_trace")
	}()

	// Update per-connection last activity timestamp for staleness checks.
	s.touchConnActivity(connID)

	// --- Handshake commands (no auth required) ---
	switch frame.Type {
	case "ping":
		if addr := s.trackedInboundPeerAddress(connID); addr != "" {
			s.markPeerRead(addr, frame)
		}
		pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
		// All async reply-writes inside dispatchNetworkFrame go through
		// sendFrameViaNetwork so the injected netcore.Network (live
		// registry or a test backend wired via NewServiceWithNetwork)
		// observes them. s.runCtx is the Service-lifecycle context,
		// pre-initialised to context.Background() in the constructor so
		// this is safe even in unit tests that never call Run().
		_ = s.sendFrameViaNetwork(s.runCtx, connID, pongFrame)
		if addr := s.trackedInboundPeerAddress(connID); addr != "" {
			s.markPeerWrite(addr, pongFrame)
		}
		return true
	case "pong":
		if addr := s.trackedInboundPeerAddress(connID); addr != "" {
			s.markPeerRead(addr, frame)
		}
		return true
	case "hello":
		// Reject re-hello once auth has been initiated (challenge issued)
		// or completed (Verified). Without this guard a second hello
		// between challenge issuance and auth_session would overwrite
		// NetCore identity/address/caps via rememberConnPeerAddr while
		// handleAuthSession still verifies against the original
		// state.Hello — allowing an attacker to authenticate as identity A
		// but bind the connection to an unverified address B, poisoning
		// health tracking and capability context.
		if s.isAuthInitiated(connID) {
			accepted = false
			_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{
				Type:  "error",
				Code:  protocol.ErrCodeHelloAfterAuth,
				Error: "re-hello rejected: authentication in progress or completed",
			})
			return true
		}
		if err := validateProtocolHandshake(frame); err != nil {
			accepted = false
			log.Warn().Err(err).Str("addr", addr).Int("version", frame.Version).Msg("inbound_peer_protocol_too_old")
			_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{
				Type:                   "error",
				Code:                   protocol.ErrCodeIncompatibleProtocol,
				Error:                  err.Error(),
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
			})
			// Per-peer storm-suppression signal. Emit the peer-banned notice
			// on the very first incompatible hello so the dialler-side
			// gate 6c.2 can record THIS PeerAddress as banned and stop
			// fanning out reconnects — keeping cm_session_setup_failed out
			// of the ebus. Reason is peer-ban (not blacklisted) because a
			// one-off incompatible hello must not be treated as an IP-wide
			// lockout: NAT/VPN/Tor exits/multi-homed hosts share an egress
			// IP across many peers, and punishing compatible siblings for
			// one misconfigured neighbour would be a wider blast radius
			// than the misbehaviour justifies. The Until window mirrors
			// the overlay per-peer duration so the dialler's gate and
			// server's overlay converge on the same 24-hour horizon.
			peerBannedUntil := time.Now().UTC().Add(peerBanIncompatible)
			s.emitPeerBannedNoticeByID(connID, peerBannedUntil, protocol.PeerBannedReasonPeerBan)
			// Transport-level IP blacklist (graduated safety net): accumulates
			// banIncrementIncompatibleVersion per attempt, reaching banThreshold
			// after 4 attempts. This is the tool for sustained noise from a
			// single IP — not the first-contact signal, which is handled above
			// by the per-peer notice. When the threshold is crossed, addBanScore
			// flips Blacklisted on the entry and fires a second peer-banned
			// notice with reason=blacklisted on the connection that crossed
			// it; from that moment handleConn silently closes all further TCP
			// attempts from that IP for 24 hours.
			s.addBanScore(connID, banIncrementIncompatibleVersion)
			// Overlay-level penalty: accumulate incompatible-version
			// penalty under the peer's canonical health key (ban triggers
			// after repeated attempts, not immediately). The remote peer's
			// version comes from the hello frame it just sent.
			peerVer := domain.ProtocolVersion(frame.Version)
			peerMin := domain.ProtocolVersion(frame.MinimumProtocolVersion)
			// Build the health key from the verified TCP IP and the
			// self-reported advertise_port — the same shape the accepted
			// inbound path uses (rememberConnPeerAddr). Under the v12 wire
			// contract hello.Listen carries no truth and frame.Address is
			// just the Ed25519 identity, not a host:port — using either as
			// a health key would silently consolidate every peer behind
			// the same observed IP under "<ip>:64646" and break version-
			// gated bans for peers running on a non-default advertise_port.
			// peerAddressFromInbound returns "" when the TCP host is not
			// parseable as an IP, in which case there is no usable health
			// key to penalise — the per-peer notice + IP-level addBanScore
			// above remain the operative signals on that degenerate edge.
			if peerAddr := peerAddressFromInbound(addr, extractAdvertisePort(frame)); peerAddr != "" {
				// Pre-populate client version so the lockout has complete
				// diagnostics. On the inbound rejection path the hello
				// frame is the only source of this metadata.
				if frame.ClientVersion != "" {
					log.Trace().Str("site", "helloRejectStoreVersion").Str("phase", "lock_wait").Str("address", peerAddr).Msg("peer_mu_writer")
					s.peerMu.Lock()
					log.Trace().Str("site", "helloRejectStoreVersion").Str("phase", "lock_held").Str("address", peerAddr).Msg("peer_mu_writer")
					s.peerVersions[domain.PeerAddress(peerAddr)] = frame.ClientVersion
					s.peerMu.Unlock()
					log.Trace().Str("site", "helloRejectStoreVersion").Str("phase", "lock_released").Str("address", peerAddr).Msg("peer_mu_writer")
				}
				s.penalizeOldProtocolPeer(domain.PeerAddress(peerAddr), peerVer, peerMin)
			}
			return true
		}
		// Advertise convergence decision. Runs after version compatibility
		// but before auth. The decision helper never rejects on advertise-
		// learning grounds and never emits any mismatch wire signal — the
		// v10/v11 reject-and-correct path was removed in the v12 cleanup
		// phase. validateAdvertisedAddress surfaces only the persistence
		// write mode and the resolved advertise_port for passive learning.
		// Accept branches are applied to persistedMeta after
		// rememberConnPeerAddr so the domain record stays consistent with
		// NetCore's view of the inbound session.
		advertiseResult := validateAdvertisedAddress(addr, frame)
		// Determine auth path by checking server-verifiable identity fields
		// (Address, PubKey, BoxKey, BoxSig), NOT frame.Client which the
		// attacker controls. This eliminates GAP-0.
		if connauth.HasIdentityFields(frame) {
			// Self-loopback guard at the identity layer. The remote has
			// cryptographic material — if it claims our Ed25519 address
			// AND announces itself as a listener the dial is reflecting
			// back to us (NAT hairpin, peer-exchange mirror, fallback-port
			// alias, onion/clearnet echo). Address helpers (isSelfAddress
			// / isSelfDialIP) can't catch this because the reflected
			// socket arrives on a different host:port tuple. Break the
			// loop here before PrepareAuth can poison connauth state or
			// learnPeerFromFrame can ingest our own key material as a
			// foreign peer. The connection_notice teaches the dialler
			// to cooldown the address; return=false triggers
			// unregisterInboundConn on the handleConn defer, so teardown
			// stays inside the NetCore wrapper.
			//
			// The Listener=="1" gate is critical: local subscribers
			// (RPC-style clients that authenticate with the local
			// identity to subscribe to their own inbox) dial the same
			// TCP port but never declare themselves as listeners. Peer
			// hellos ALWAYS set Listener=1 because the handshake
			// emitter (nodeHelloJSONLine) always reflects
			// EffectiveListenerEnabled, and only listener-enabled peers
			// propagate hello to other nodes. Without this gate the
			// guard would break legitimate self-subscription on the
			// same host. Every real self-loopback path surfaces
			// Listener=1 because it reaches us through OUR outbound
			// dialler code, which always sets it. Pre-v12 the gate
			// keyed on frame.Listen != "" because the wire still
			// carried the host:port form; the v12 cleanup makes Listen
			// empty by contract, so the listener flag is the only
			// remaining signal that distinguishes a peer from a local
			// subscriber.
			if strings.TrimSpace(frame.Listener) == "1" && s.isSelfIdentity(domain.PeerIdentityFromWire(frame.Address)) {
				accepted = false
				log.Warn().
					Str("local_identity", s.identity.Address).
					Str("remote_addr", addr).
					Str("remote_client", frame.Client).
					Msg("inbound_self_identity_rejected")
				s.emitPeerBannedNoticeByID(connID, time.Time{}, protocol.PeerBannedReasonSelfIdentity)
				return false
			}
			authState, err := connauth.PrepareAuth(frame)
			if err != nil {
				accepted = false
				s.addBanScore(connID, banIncrementInvalidSig)
				_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()})
				return false
			}
			s.setConnAuthStateByID(connID, authState)
			s.rememberConnPeerAddr(connID, frame, addr)
			// Advertise convergence persistence (accept branches).
			s.applyAdvertiseOnInboundAccept(addr, advertiseResult)
			if frame.Client == "node" || frame.Client == "desktop" {
				log.Info().Str("client", frame.Client).Str("address", frame.Address).Str("listen", frame.Listen).Str("node_type", frame.NodeType).Str("version", frame.ClientVersion).Msg("hello")
			}
			// Diagnostic: warn when a localhost peer has a different identity
			// than ours — indicates a separate local process (Task #69).
			if remoteIP := net.ParseIP(remoteIPFromString(addr)); remoteIP != nil && remoteIP.IsLoopback() && frame.Address != "" && frame.Address != s.identity.Address {
				log.Warn().
					Int("pid", os.Getpid()).
					Str("local_identity", s.identity.Address).
					Str("remote_identity", frame.Address).
					Str("remote_addr", addr).
					Str("remote_listen", frame.Listen).
					Str("remote_client", frame.Client).
					Msg("localhost_peer_foreign_identity")
			}
			s.sendWelcomeFrame(connID, authState.Challenge, remoteIPFromString(addr))
			return true
		}
		// No identity fields → unauthenticated peer. It stays
		// unauthenticated with access limited to handshake commands
		// (hello, ping, pong, auth_session). All P2P wire commands
		// are blocked until auth_session completes.
		s.rememberConnPeerAddr(connID, frame, addr)
		// Advertise convergence persistence (accept branches).
		s.applyAdvertiseOnInboundAccept(addr, advertiseResult)
		log.Debug().
			Str("client", frame.Client).
			Str("addr", addr).
			Msg("hello_without_identity_fields")
		s.sendWelcomeFrame(connID, "", remoteIPFromString(addr))
		return true
	case "auth_session":
		reply, ok, backlogSub, fullSync := s.handleAuthSession(connID, frame)
		if !ok {
			accepted = false
			_ = s.sendFrameViaNetworkSync(s.runCtx, connID, reply)
			return false
		}
		// Handshake-phase reply: auth_ok is the gate the dialler waits on
		// before the session becomes usable. Use the no-eviction path so a
		// transient outbound burst (broadcast of our own announce_routes
		// to the new peer kicked off by the auth_session handler) does
		// not torpedo the connection right at the threshold of becoming
		// usable. See network_consumer.go.
		//
		// Half-auth window contract: handleAuthSession has ALREADY
		// committed local auth state (setConnAuthStateByID), learned
		// the peer's hello, registered the hello route, fired the
		// announce-to-sessions goroutine, and mirrored the identity
		// into NetCore — see service.go::handleAuthSession. We do NOT
		// roll those side effects back if the auth_ok ack fails to
		// land: the rollback path is wide and concurrent
		// (cross-domain locks, separate goroutines already started),
		// and the failure mode is bounded.
		//
		// What actually happens when the ack is dropped:
		//   1. Dialler side. authenticatePeerSession waits for auth_ok
		//      via peerSessionRequest with peerRequestTimeout
		//      (peer_sessions.go::authenticatePeerSession). It does
		//      NOT reissue auth_session on the same TCP session —
		//      timeout returns an error that bubbles up to
		//      openPeerSessionForCM which closes the connection and
		//      emits DialFailed.
		//   2. CM side. DialFailed cycles through reconnectMaxRetries
		//      with exponential backoff; each retry opens a fresh TCP
		//      connection and a fresh connID. handleAuthSession runs
		//      from scratch on the new connID — its Verified
		//      fast-path applies only to repeat auth on the SAME
		//      connID and does not carry across reconnects.
		//   3. Local side. Our previous session's connID still carries
		//      Verified=true and the side-effect chain (route entry,
		//      announce goroutine output, hello cache). handleConn's
		//      defer (clearConnAuth, removeSubscriberConnID,
		//      trackInboundDisconnect) cleans those up when the peer
		//      finally drops the dead TCP connection or when our own
		//      read loop hits EOF.
		//
		// Window bound: from ack-drop to handleConn defer firing — at
		// most one inbound read deadline (120s) on the failing socket,
		// usually much less because the dialler closes promptly on its
		// own peerRequestTimeout. If the outbound buffer NEVER drains,
		// the heartbeat path (inboundHeartbeat) evicts us on its own
		// schedule. A failed auth_ok is logged at WARN with conn_id
		// for forensics. See docs/protocol/realtime.md § Handshake
		// reply: no-eviction contract for the full discussion.
		if err := s.sendHandshakeReplyViaNetwork(s.runCtx, connID, reply); err != nil {
			log.Warn().Err(err).
				Uint64("conn_id", uint64(connID)).
				Msg("auth_ok ack failed — peer will close on its peerRequestTimeout and CM will redial (fresh connID, half-auth state cleared by handleConn defer)")
			// auth_ok did not land — do NOT replay the backlog: the peer is
			// about to be redialed on a fresh connID, and pushing onto a wedged
			// buffer would be wasted work the peer never reads.
			return true
		}
		// Mandatory initial push_identity (identity-discovery layer), ordered
		// behind auth_ok for the same reason as the backlog below. The proven
		// inbound identity is the push's addressee; the send path skips peers
		// without the plane and closes the connection on an enqueue fault.
		if pushPeer := s.provenInboundPeerIdentity(connID); !pushPeer.IsZero() {
			// lifecycle: joined by backgroundWg (WaitBackground). One bounded
			// SendLocal enqueue; the close callback is a NetCore close, not a
			// goroutine.
			s.goBackground(func() {
				s.sendInitialIdentityPush(s.runCtx, pushPeer, func() { _ = s.Network().Close(s.runCtx, connID) })
			})
		}
		// Auto-subscribe backlog replay — strictly AFTER auth_ok has been
		// enqueued into the writer (sendHandshakeReplyViaNetwork returned nil),
		// so push_message/push_delivery_receipt frames are ordered behind
		// auth_ok on the connection. Fire-and-forget.
		if backlogSub != nil {
			s.goBackground(func() { s.pushBacklogToSubscriber(backlogSub) })
		}
		// The connect-time route table, ordered behind auth_ok for the same
		// reason and by the same rule: the dialler counts a capability as
		// negotiated only once auth_ok has landed, so a full sync that
		// overtakes it is a full sync the peer refuses and never asks for
		// again until its own periodic sweep.
		if fullSync.due {
			// s.runCtx bounds the write so that shutdown can abort a
			// half-flushed inbound send instead of waiting out the full
			// syncFlushTimeout on a stuck hairpin socket.
			s.goBackground(func() {
				// The crash report the original spawn carried: goBackground
				// does not recover, and a panic in a full sync is a crash the
				// operator has to be able to read afterwards.
				defer crashlog.DeferRecover()
				s.sendFullTableSyncToInbound(s.runCtx, connID, fullSync.peer)
			})
		}
		return true

	default:
		// --- P2P wire protocol (auth required) ---
		//
		// Everything below requires a completed auth_session. The role is
		// derived from server-side auth state, never from frame.Client
		// (GAP-0). Data commands (fetch_messages, send_message, etc.) are
		// not handled here — they live exclusively in
		// handleLocalFrameDispatch / RPC HTTP and fall through to the
		// unknown_command response at the bottom.
	}

	// Auth gate for P2P commands. Handshake commands returned above;
	// everything reaching this point is either a P2P command or unknown.
	if !s.isConnAuthenticated(connID) {
		// Check whether the command is a known P2P command before deciding
		// on the error code: known P2P → auth_required, unknown → unknown_command.
		if isP2PWireCommand(frame.Type) {
			accepted = false
			_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired})
			return false
		}
		accepted = false
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand})
		return false
	}

	switch frame.Type {
	case "get_peers":
		// P2P peer discovery: authenticated peers request the peer list
		// for network synchronization (syncPeer, syncPeerSession).
		// Merges active CM slots with PeerProvider candidates, deduplicated
		// by IP. Network group filtering prevents leaking clearnet addresses
		// to Tor/I2P peers and vice versa.
		//
		// Cap the response at maxAnnouncePeers (64). The peers frame is a
		// JSON-encoded line bound by MaxFrameLine (128 KiB) and the
		// receiver enforces the same maxAnnouncePeers cap on
		// announce_peer ingestion — emitting a longer list would either
		// blow past the wire-size budget on dense networks or get
		// silently truncated on the other side. connPeerReachableGroups
		// already samples by reachability group, so the trim is a
		// belt-and-braces guard that also fixes the storm scenario
		// where peerProvider feeds the response a full 800-node table.
		exchanged := s.buildPeerExchangeResponse(s.connPeerReachableGroups(connID))
		if len(exchanged) > maxAnnouncePeers {
			exchanged = exchanged[:maxAnnouncePeers]
		}
		peers := make([]string, len(exchanged))
		for i, a := range exchanged {
			peers[i] = string(a)
		}
		// Handshake-phase reply: peers is one of the session-setup frames
		// emitted while the peer is still inside initPeerSession on the
		// other side. Use sendHandshakeReplyViaNetwork so a transient
		// outbound-queue burst (our own announce_routes flush going to
		// the same peer) does NOT trigger slow-peer eviction and
		// torpedo the handshake. See network_consumer.go.
		_ = s.sendHandshakeReplyViaNetwork(s.runCtx, connID, protocol.Frame{
			Type:  "peers",
			Count: len(peers),
			Peers: peers,
		})
		return true
	case "fetch_contacts":
		// P2P contact sync: authenticated peers fetch the contact list
		// for key material synchronization (syncPeer, syncContactsViaSession).
		// Deprecated wire surface: superseded by the get_identity datagram
		// lookup; served unchanged as the epidemic bridge for peers without
		// the layer. TODO(fetch-contacts-floor): retire with the bridge.
		// Session-setup phase — use the no-eviction handshake-reply path.
		// contactsFrameForNetwork and not contactsFrame: the receiver verifies a
		// signature per entry and refuses a reply past maxContactsPerResponse,
		// so the wire form is BUILT bounded to the number both ends agree on —
		// the walk stops at the cap rather than being cut down to it.
		_ = s.sendHandshakeReplyViaNetwork(s.runCtx, connID, s.contactsFrameForNetwork())
		return true
	case "ack_delete":
		reply, ok := s.handleAckDeleteFrame(connID, frame)
		if !ok {
			accepted = false
			_ = s.sendFrameViaNetworkSync(s.runCtx, connID, reply)
			return false
		}
		_ = s.sendFrameViaNetwork(s.runCtx, connID, reply)
		return true
	case "push_message":
		s.handleInboundPushMessage(connID, frame)
		return true
	case "push_delivery_receipt":
		s.handleInboundPushDeliveryReceipt(connID, frame)
		return true
	case "relay_delivery_receipt":
		// Gossip receipt path: a peer forwards a delivery receipt via
		// the flat-field format (ID, Address, Recipient, Status,
		// DeliveredAt) rather than the ReceiptFrame used by push.
		// Without this handler the frame hits unknown_command and
		// kills the connection — breaking receipt delivery for client
		// nodes whose only return path is gossip through a full node.
		//
		// Deliberately separated from "send_delivery_receipt" (local-only
		// RPC command) to maintain the command-isolation boundary.
		s.handleInboundRelayDeliveryReceipt(connID, frame)
		return true
	case "push_notice":
		s.handleInboundPushNotice(frame)
		return true
	case "announce_peer":
		// Auth gate enforced above. Only authenticated peers may announce,
		// so we always promote.
		nodeType := frame.NodeType
		// node_type is validated for wire compatibility only. For third-party
		// gossip we learn the address, but we do not trust the sender to set
		// or override the announced peer's local role.
		if !isKnownNodeType(nodeType) {
			_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{Type: "announce_peer_ack"})
			return true
		}
		peers := frame.Peers
		if len(peers) > maxAnnouncePeers {
			peers = peers[:maxAnnouncePeers]
		}
		for _, peer := range peers {
			if peer == "" || classifyAddress(domain.PeerAddress(peer)) == domain.NetGroupLocal {
				continue
			}
			s.promotePeerAddress(domain.PeerAddress(peer))
		}
		_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{Type: "announce_peer_ack"})
		return true
	case "relay_message":
		// Auth gate enforced above (INV-9).
		if admit := admitRelayFrame(s.connHasCapability(connID, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			accepted = false
			return true
		}
		senderAddr := s.inboundPeerAddress(connID)
		if senderAddr == "" {
			accepted = false
			return true
		}
		// Look up an outbound session to the relay peer for on-demand
		// key sync. This is safe because the relay arrived on an inbound
		// connection — the outbound session is a separate conn/inboxCh,
		// so peerSessionRequest won't deadlock.
		syncSession, _ := s.activePeerSession(domain.PeerAddress(senderAddr))
		ackStatus := s.handleRelayMessage(domain.PeerAddress(senderAddr), syncSession, frame)
		// Write a single relay_hop_ack with the semantic status directly
		// on the inbound connection. This is the only ack the sender
		// receives — handleRelayMessage itself does not send acks.
		// Empty status means the message was dropped (dedupe, max hops,
		// client node); no ack is sent for drops (INV-5).
		if ackStatus != "" {
			_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{
				Type:   "relay_hop_ack",
				ID:     frame.ID,
				Status: ackStatus,
			})
		}
		return true
	case "relay_hop_ack":
		// Auth gate enforced above.
		if admit := admitRelayFrame(s.connHasCapability(connID, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			accepted = false
			return true
		}
		senderAddr := s.inboundPeerAddress(connID)
		if senderAddr != "" {
			s.handleRelayHopAck(domain.PeerAddress(senderAddr), frame)
		}
		return true
	case "announce_routes":
		// Auth gate enforced above.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		// A routing-only peer (mesh_routing_v1 without mesh_relay_v1) can
		// advertise routes, but this node can never send relay_message to
		// it, making every route through it data-plane unusable. Reject
		// announcements from such peers to avoid storing dead NextHop entries.
		if !s.connHasCapability(connID, domain.CapMeshRelayV1) {
			accepted = false
			return true
		}
		senderIdentity := s.inboundPeerIdentity(connID)
		s.handleAnnounceRoutes(senderIdentity, frame)
		return true
	case "routes_update":
		// Auth gate enforced above. v2 delta frame requires BOTH routing
		// capabilities: v1 is the wire-protocol baseline (every routing-
		// capable peer has it), v2 is the opt-in refinement that enables
		// the delta path. A peer advertising only v1 must continue to
		// receive legacy announce_routes frames; a peer advertising v2
		// without v1 is treated as non-routing per docs/routing.md.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		if !s.connHasCapability(connID, domain.CapMeshRoutingV2) {
			accepted = false
			return true
		}
		// Relay gate mirrors announce_routes: routes through a non-relay
		// neighbor would be data-plane unusable, so accepting the delta
		// would only populate dead NextHop entries.
		if !s.connHasCapability(connID, domain.CapMeshRelayV1) {
			accepted = false
			return true
		}
		senderIdentity := s.inboundPeerIdentity(connID)
		senderAddress := s.inboundConnKeyForID(connID)
		s.handleRoutesUpdate(senderIdentity, senderAddress, frame)
		return true
	case "request_resync":
		// Auth gate enforced above. request_resync is a v2-only control
		// frame: only v2 peers know to emit it, so gating on
		// CapMeshRoutingV2 keeps legacy peers from ever hitting this path.
		// No payload — arrival alone triggers MarkInvalid + TriggerUpdate
		// on the peer's announce state, forcing the next cycle to take the
		// full-sync branch via legacy announce_routes.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV2) {
			accepted = false
			return true
		}
		senderIdentity := s.inboundPeerIdentity(connID)
		s.handleRequestResync(senderIdentity)
		return true
	case "file_command":
		// Auth gate enforced above. Capability check: file_transfer_v1 must
		// be negotiated.
		if !s.connHasCapability(connID, domain.CapFileTransferV1) {
			accepted = false
			return true
		}
		// Pass the inbound peer identity so the file router applies
		// split-horizon forwarding and never reflects the frame back
		// to the neighbor that just delivered it.
		s.handleFileCommandFrame(json.RawMessage(line), s.inboundPeerIdentity(connID))
		return true
	case "route_probe_v1":
		// Auth gate enforced above. Phase 2 reachability probe gated
		// by mesh_route_probe_v1 (overview §7.6,
		// docs/protocol/route_health.md). Peers that
		// do not advertise the capability MUST NOT receive probes; the
		// receive-side gate here is the symmetric guard against an
		// older peer accidentally routing the frame to us.
		//
		// Type string is the raw "route_probe_v1" literal (kept in
		// sync with protocol.RouteProbeFrameType) because the
		// command_scope_test AST inspector only extracts string-literal
		// case labels; using the constant would render this case
		// invisible to the wire-vs-data invariant tests.
		if !s.connHasCapability(connID, domain.CapMeshRouteProbeV1) {
			accepted = false
			return true
		}
		probe, err := protocol.UnmarshalRouteProbeFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteProbe(connID, s.inboundPeerIdentity(connID), probe)
		return true
	case "route_probe_ack_v1":
		// Auth gate enforced above. Same capability gate as the probe
		// request — both sides of the probe round trip share
		// mesh_route_probe_v1. Same string-literal rationale as
		// route_probe_v1 above.
		if !s.connHasCapability(connID, domain.CapMeshRouteProbeV1) {
			accepted = false
			return true
		}
		ack, err := protocol.UnmarshalRouteProbeAckFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteProbeAck(s.inboundPeerIdentity(connID), ack)
		return true
	case "route_query_v1":
		// Auth gate enforced above. Phase 2 targeted route query
		// gated by mesh_route_query_v1 (overview §7.5,
		// docs/protocol/route_health.md). Same
		// string-literal rationale as route_probe_v1: the AST
		// inspector in command_scope_test only matches BasicLit
		// case labels.
		if !s.connHasCapability(connID, domain.CapMeshRouteQueryV1) {
			accepted = false
			return true
		}
		query, err := protocol.UnmarshalRouteQueryFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteQuery(connID, s.inboundPeerIdentity(connID), query)
		return true
	case "route_query_response_v1":
		// Auth gate enforced above. Same capability gate as the
		// query request — both sides share mesh_route_query_v1.
		if !s.connHasCapability(connID, domain.CapMeshRouteQueryV1) {
			accepted = false
			return true
		}
		resp, err := protocol.UnmarshalRouteQueryResponseFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteQueryResponse(s.inboundPeerIdentity(connID), resp)
		return true
	case "route_sync_digest_v1":
		// Auth gate enforced above. Phase 3 PR 12.5 incremental-
		// sync digest gated by mesh_route_sync_v1
		// (docs/cluster-mesh/phase-3-multipath-reputation.md §4.5).
		// Peers that did not negotiate the capability never
		// receive the frame in the first place; the symmetric
		// receive-side gate here is the defence in depth.
		//
		// Type string is the raw literal (kept in sync with
		// protocol.RouteSyncDigestFrameType) so the
		// command_scope_test AST inspector that only extracts
		// string-literal case labels picks it up.
		if !s.connHasCapability(connID, domain.CapMeshRouteSyncV1) {
			accepted = false
			return true
		}
		digestFrame, err := protocol.UnmarshalRouteSyncDigestFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteSyncDigest(connID, s.inboundPeerIdentity(connID), digestFrame)
		return true
	case "route_sync_summary_v1":
		// Auth gate enforced above. Same capability gate as the
		// digest request — both sides share mesh_route_sync_v1.
		if !s.connHasCapability(connID, domain.CapMeshRouteSyncV1) {
			accepted = false
			return true
		}
		summaryFrame, err := protocol.UnmarshalRouteSyncSummaryFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteSyncSummary(s.inboundPeerIdentity(connID), summaryFrame)
		return true
	case "route_poison_v1":
		// Auth gate enforced above. Phase 4 single-hop poison-reverse
		// signal gated by the pair mesh_routing_v1 +
		// mesh_poison_reverse_v1. Relay cap NOT required — poison is a
		// control signal scoped to the (identity, sender) storage slot,
		// not a data-plane delivery, so a routing-capable-but-non-relay
		// neighbour is a valid sender. Type string is the raw literal
		// (kept in sync with protocol.RoutePoisonFrameType) so the
		// command_scope_test AST inspector picks up the case label.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		if !s.connHasCapability(connID, domain.CapMeshPoisonReverseV1) {
			accepted = false
			return true
		}
		poison, err := protocol.UnmarshalRoutePoisonFrame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRoutePoison(s.inboundPeerIdentity(connID), poison)
		return true
	case "route_poison_v2":
		// Auth gate enforced above. Batched poison-reverse, gated by
		// mesh_routing_v1 + mesh_poison_reverse_v2. Same scope rationale as
		// route_poison_v1; type string is the raw literal kept in sync with
		// protocol.RoutePoisonV2FrameType for the command_scope_test inspector.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		if !s.connHasCapability(connID, domain.CapMeshPoisonReverseV2) {
			accepted = false
			return true
		}
		poisonBatch, err := protocol.UnmarshalRoutePoisonV2Frame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRoutePoisonV2(s.inboundPeerIdentity(connID), poisonBatch)
		return true
	case "route_announce_v3":
		// Auth gate enforced above. Phase 4 compact announce gated by the
		// FULL triplet mesh_routing_v1 + mesh_routing_v3 + mesh_relay_v1,
		// mirroring announce_routes / routes_update: v1 is the wire
		// baseline a mixed-version fallback depends on, relay is the
		// data-plane requirement (a non-relay neighbor yields unusable
		// NextHops), and v3 is the compact-frame opt-in. Type string is the
		// raw literal (kept in sync with protocol.RouteAnnounceV3FrameType)
		// so the command_scope_test AST inspector picks up the case label.
		if !s.connHasCapability(connID, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		if !s.connHasCapability(connID, domain.CapMeshRoutingV3) {
			accepted = false
			return true
		}
		if !s.connHasCapability(connID, domain.CapMeshRelayV1) {
			accepted = false
			return true
		}
		v3, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(line))
		if err != nil {
			accepted = false
			return true
		}
		s.handleRouteAnnounceV3(s.inboundPeerIdentity(connID), s.inboundConnKeyForID(connID), v3)
		return true
	case "datagram":
		// UNREACHABLE, and kept as the assertion of that fact — and as the
		// declaration the command_scope_test AST inspector reads, because
		// `datagram` IS a P2P wire command of this port; it is simply dispatched
		// above the parser rather than in this switch. Type string stays the raw
		// literal (kept in sync with protocol.DatagramFrameType) so that
		// inspector, which only reads string-literal case labels, sees it — same
		// rationale as route_probe_v1 above.
		//
		// dispatchNetworkFrame classifies every line before the parser runs: one
		// classifyFrameLine names `datagram` is diverted to the ingress with the
		// RAW wire bytes (§3.4, §5), and one it cannot resolve is refused
		// unparsed (§4.1 step 1). A frame arriving here would mean the scan and
		// encoding/json disagree on a line neither refused, which is a classifier
		// bug — and delivering it would put the universal parse back in front of
		// the neighbour's budget, which is the finding this branch used to be.
		accepted = false
		s.reportDatagramResidueUnreachable("inbound", addr)
		return true
	default:
		accepted = false
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand})
		return false
	}
}

// p2pWireCommands is the set of commands that belong to the authenticated
// P2P wire protocol. Used by the auth gate in dispatchNetworkFrame to
// distinguish "command exists but you need auth" (→ auth_required) from
// "command does not exist on this port" (→ unknown_command).
var p2pWireCommands = map[string]bool{
	"get_peers":              true,
	"fetch_contacts":         true,
	"ack_delete":             true,
	"push_message":           true,
	"push_delivery_receipt":  true,
	"relay_delivery_receipt": true,
	"push_notice":            true,
	"announce_peer":          true,
	"relay_message":          true,
	"relay_hop_ack":          true,
	"announce_routes":        true,
	"routes_update":          true,
	"request_resync":         true,
	"file_command":           true,
	// Phase 2 (mesh_route_probe_v1, additive). String literals kept
	// in sync with protocol.RouteProbeFrameType /
	// protocol.RouteProbeAckFrameType — switch-case labels in
	// dispatchNetworkFrame use the same literals so the
	// command_scope_test AST inspector can cross-check both surfaces.
	"route_probe_v1":     true,
	"route_probe_ack_v1": true,
	// Phase 2 (mesh_route_query_v1, additive). Targeted single-hop
	// route query used for fast recovery when all known uplinks for
	// an identity are Bad/Dead — same string-literal pattern.
	"route_query_v1":          true,
	"route_query_response_v1": true,
	// Phase 3 PR 12.5 (mesh_route_sync_v1, additive). Incremental-
	// sync digest exchange — receiver short-circuits the next
	// forced full sync on a digest match. Same string-literal
	// pattern as the Phase 2 frames so command_scope_test stays
	// happy.
	"route_sync_digest_v1":  true,
	"route_sync_summary_v1": true,
	// Phase 4 (mesh_routing_v3, additive). Compact announce frame that
	// replaces announce_routes / routes_update for v3-capable pairs —
	// same string-literal pattern so command_scope_test stays happy.
	"route_announce_v3": true,
	// Phase 4 (mesh_poison_reverse_v1, additive). Single-hop explicit
	// poison-reverse signal for accelerated count-to-infinity
	// convergence — same string-literal pattern.
	"route_poison_v1": true,
	// Batched poison-reverse (mesh_poison_reverse_v2, additive): one frame
	// carries a list of lost identities — same auth/scope as v1.
	"route_poison_v2": true,
	// Datagram transport layer (mesh_datagram_v1 capability gate). Listed
	// here so an UNAUTHENTICATED peer gets auth_required rather than
	// unknown_command: the command exists on this port, it just needs a
	// completed auth_session first. Same string-literal pattern as the
	// frames above so command_scope_test stays happy.
	"datagram": true,
}

// isP2PWireCommand returns true if the command name belongs to the
// authenticated P2P wire protocol handled by dispatchNetworkFrame.
func isP2PWireCommand(cmd string) bool {
	return p2pWireCommands[cmd]
}

func (s *Service) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	log.Trace().
		Str("protocol", "json/local").
		Str("addr", "local").
		Str("direction", "recv").
		Str("command", frame.Type).
		Msg("local_frame_dispatch_begin")
	resp := s.handleLocalFrameDispatch(frame)
	accepted := resp.Type != "error"
	log.Trace().
		Str("protocol", "json/local").
		Str("addr", "local").
		Str("direction", "recv").
		Str("command", frame.Type).
		Bool("accepted", accepted).
		Msg("protocol_trace")
	return resp
}

func (s *Service) handleLocalFrameDispatch(frame protocol.Frame) protocol.Frame {
	switch frame.Type {
	case "hello":
		if err := validateProtocolHandshake(frame); err != nil {
			return protocol.Frame{
				Type:                   "error",
				Code:                   protocol.ErrCodeIncompatibleProtocol,
				Error:                  err.Error(),
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
			}
		}
		return s.welcomeFrame("", "")
	case "ping":
		return protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
	case "get_peers":
		// Local RPC: unfiltered merge of active CM slots + PeerProvider
		// candidates, no network group filtering.
		exchanged := s.buildPeerExchangeResponse(nil)
		peers := make([]string, len(exchanged))
		for i, a := range exchanged {
			peers[i] = string(a)
		}
		return protocol.Frame{
			Type:  "peers",
			Count: len(peers),
			Peers: peers,
		}
	case "fetch_identities":
		return s.identitiesFrame()
	case "fetch_contacts":
		return s.contactsFrame()
	case "fetch_trusted_contacts":
		return s.trustedContactsFrame()
	case "delete_trusted_contact":
		return s.deleteTrustedContactFrame(domain.PeerIdentityFromWire(frame.Address))
	case "fetch_peer_health":
		return s.peerHealthFrame()
	case "fetch_network_stats":
		return s.networkStatsFrame()
	case "fetch_traffic_totals":
		// Lightweight totals for the metrics collector's per-second traffic
		// sampling — no per-peer / map / slice allocations (only the returned
		// frame value escapes). Deliberately does NOT arm the
		// network_stats rebuild-gate (see trafficTotalsFrame). Local-only —
		// not exposed on the wire/HTTP command tables.
		return s.trafficTotalsFrame()
	case "fetch_aggregate_status":
		return s.aggregateStatusFrame()
	case "fetch_resource_usage":
		return s.resourceUsageFrame()
	case "fetch_pending_messages":
		return s.pendingMessagesFrame(frame.Topic)
	case "import_contacts":
		return s.importContactsFrame(frame.Contacts)
	case "send_message", "send_control_message":
		// Both Frame types funnel through storeMessageFrame /
		// storeIncomingMessage. The control-DM divergence (skip chatlog,
		// emit LocalChangeNewControlMessage) is handled inside
		// storeIncomingMessage based on the frame's Topic field.
		// send_control_message is the canonical chokepoint for control
		// DMs (see docs/dm-commands.md); accepting it here prevents
		// ErrCodeUnknownCommand from rejecting DMCrypto.SendControlMessage.
		return s.storeMessageFrame(frame)
	case "import_message":
		return s.importMessageFrame(frame)
	case "send_delivery_receipt":
		return s.storeDeliveryReceiptFrame(frame)
	case "cancel_message_delivery":
		return s.cancelMessageDeliveryFrame(frame)
	case "cancel_conversation_delivery":
		return s.cancelConversationDeliveryFrame(frame)
	case "freeze_message_delivery":
		return s.freezeMessageDeliveryFrame(frame)
	case "freeze_conversation_delivery":
		return s.freezeConversationDeliveryFrame(frame)
	case "thaw_conversation_delivery":
		return s.thawConversationDeliveryFrame(frame)
	case "fetch_messages":
		return s.fetchMessagesFrame(frame.Topic)
	case "fetch_message_ids":
		return s.fetchMessageIDsFrame(frame.Topic)
	case "fetch_message":
		return s.fetchMessageFrame(frame.Topic, frame.ID)
	case "fetch_inbox":
		return s.fetchInboxFrame(frame.Topic, frame.Recipient)
	case "fetch_delivery_receipts":
		return s.fetchDeliveryReceiptsFrame(frame.Recipient)
	case "publish_notice":
		return s.publishNoticeFrame(frame)
	case "fetch_notices":
		return s.fetchNoticesFrame()
	case "add_peer":
		return s.addPeerFrame(frame)
	case "connect_only":
		return s.connectOnlyFrame(frame)
	case "fetch_dm_headers":
		return s.fetchDMHeadersFrame()
	case "fetch_relay_status":
		return s.relayStatusFrame()
	case "fetch_reachable_ids":
		return s.reachableIDsFrame()
	case "resolve_identity":
		return s.resolveIdentityFrame(frame)
	case "resolve_identity_status":
		return s.resolveIdentityStatusFrame(frame)
	case "identity_backup":
		return s.identityBackupFrame(frame)
	case "identity_restore":
		return s.identityRestoreFrame(frame)
	default:
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand}
	}
}

// netCoreForID returns the NetCore registered for the given ConnID, or
// nil if the connection is not registered (either never attached via
// registerInboundConnLocked / attachOutboundCoreLocked, or already
// unregistered). Non-lock-holding call sites that start from a net.Conn
// cross the boundary once via connIDFor and then operate on ConnID; a
// nil return here on a steady-state ConnID is the hard fail-closed
// signal consumed by the write wrappers (see ErrUnregisteredWrite).
// The bootstrap/handshake edges — see the three sentinel call sites in
// routing_relay.go inside sendNoticeToPeer (node-hello, auth challenge,
// challenge-response writes that run before register / attach publish
// an entry) — never reach this helper because they operate on a raw
// net.Conn before any ConnID has been minted.
func (s *Service) netCoreForID(id domain.ConnID) *netcore.NetCore {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.coreForIDLocked(id)
}

// meteredForID returns the MeteredConn wrapper for the given ConnID, or
// nil if the connection is not registered or was not wrapped in a
// MeteredConn (e.g. outbound dials that do not measure bytes).
func (s *Service) meteredForID(id domain.ConnID) *netcore.MeteredConn {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.meteredForIDLocked(id)
}

// isInboundTrackedByID returns true when the ConnID has been promoted via
// trackInboundConnect (auth complete or auth not required) and has not
// been untracked yet.
func (s *Service) isInboundTrackedByID(id domain.ConnID) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.isInboundTrackedByIDLocked(id)
}

// connWriter is now replaced by NetCore.writerLoop(). The single writer
// goroutine per inbound connection is started inside netcore.New() and
// drains NetCore.sendCh. See net_core.go for sendItem and the implementation.

// ErrUnregisteredWrite signals that a write wrapper (writeJSONFrame,
// writeJSONFrameSync, sendSessionFrameViaNetwork's carve-out fallback)
// attempted to send a frame on a connection that has no associated
// NetCore in s.conns (or no NetCore on the session). Every live connection is registered before the first
// send (see registerInboundConn and attachOutboundNetCore), so this
// outcome means the single-writer invariant was violated: the frame is
// dropped (fail closed) and the error is returned to the caller so it
// can react (early return, scope cleanup) rather than continuing as if
// the write had succeeded. The sentinel is matched via errors.Is;
// callers that intentionally fire-and-forget must acknowledge the
// return with `_ = s.writeJSONFrameByID(...)` so errcheck can flag any
// silently dropped error at lint time — that is the compile-time
// enforcement of the writer-ownership contract, complementing the
// runtime logUnregisteredWrite observability baseline.
var ErrUnregisteredWrite = errors.New("unregistered write: single-writer invariant violated")

type enqueueResult int

const (
	enqueueSent enqueueResult = iota // data accepted into the channel
	// enqueueUnregistered signals that the conn is not in s.conns.
	// After PR 2 every live connection — inbound via registerInboundConn and
	// outbound via attachOutboundNetCore — is registered before any send,
	// so this outcome now means "invariant violation / state inconsistency",
	// not "legitimate outbound peer session without send channel". Callers
	// must fail closed on this result; bypassing the managed writer with a
	// direct conn.Write would reintroduce the broken single-writer property
	// the migration is closing.
	enqueueUnregistered
	enqueueDropped // channel full or closed — data lost, conn closing
)

// String returns a stable machine-friendly label for each enqueueResult,
// used by emitProtocolTrace to surface the effective send outcome in
// observability. The labels are part of the log contract — renaming them
// is an operations-visible change.
func (r enqueueResult) String() string {
	switch r {
	case enqueueSent:
		return "sent"
	case enqueueUnregistered:
		return "unregistered"
	case enqueueDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

// enqueueFrameByID sends the serialised bytes to the per-connection writer
// goroutine via NetCore.sendRaw. Fire-and-forget: the caller does not
// wait for the write. ConnID-first (PR 10.6): the net.Conn handle is no
// longer required — slow-peer eviction is performed through NetCore.Close
// on the already-resolved core.
func (s *Service) enqueueFrameByID(id domain.ConnID, data []byte) enqueueResult {
	pc := s.netCoreForID(id)
	if pc == nil {
		return enqueueUnregistered
	}
	addr := pc.RemoteAddr()
	switch st := pc.SendRaw(data); st {
	case netcore.SendOK:
		return enqueueSent
	case netcore.SendBufferFull:
		// Peer too slow — evict by closing the NetCore (shuts the send gate,
		// closes the raw socket and waits for the writer to drain).
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		pc.Close()
		return enqueueDropped
	case netcore.SendChanClosed, netcore.SendWriterDone:
		// The link is already finished: the channel is closed, or the writer
		// shut the queue the instant a socket write failed. Both are ordinary
		// outcomes on a dying connection, not programming errors — an ERROR
		// line per fire-and-forget frame would be a log storm exactly when the
		// log has to stay readable, and Close() would be a second teardown of
		// a connection that is already tearing itself down.
		return enqueueDropped
	default:
		// netcore.SendStatusInvalid or any unexpected value — programming error.
		log.Error().Str("addr", addr).Str("status", st.String()).Msg("enqueueFrameByID: unexpected netcore.SendStatus")
		return enqueueDropped
	}
}

// enqueueFrameSyncByID sends the serialised bytes to the per-connection writer
// goroutine via NetCore.sendRawSync and blocks until the writer has handed
// the data to the socket.
// Used by writeJSONFrameSyncByID for error-path frames that must be delivered
// before the connection is torn down.
func (s *Service) enqueueFrameSyncByID(id domain.ConnID, data []byte) enqueueResult {
	pc := s.netCoreForID(id)
	if pc == nil {
		return enqueueUnregistered
	}
	addr := pc.RemoteAddr()
	// Inbound error paths use fast-fail semantics: a saturated queue
	// means the peer is unresponsive and must be evicted rather than
	// kept alive while the caller blocks. Outbound control-plane writes
	// that must not be starved by fire-and-forget traffic use the
	// sendRawSyncBlocking variant via peerSessionRequest — that path
	// does not reach this helper.
	switch st := pc.SendRawSync(data); st {
	case netcore.SendOK:
		return enqueueSent
	case netcore.SendBufferFull:
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		pc.Close()
		return enqueueDropped
	case netcore.SendTimeout:
		log.Warn().Str("addr", addr).Msg("sync flush timeout, disconnecting peer")
		pc.Close()
		return enqueueDropped
	case netcore.SendWriterDone, netcore.SendChanClosed:
		// Connection already dying — don't close, let handleConn's
		// deferred cleanup do it.
		return enqueueDropped
	default:
		// netcore.SendStatusInvalid or any unexpected value — programming error.
		log.Error().Str("addr", addr).Str("status", st.String()).Msg("enqueueFrameSyncByID: unexpected netcore.SendStatus")
		return enqueueDropped
	}
}

// peerSendableConnection is one connection the send path would attempt
// when targeting a peer identity. Exactly one of {outbound, inboundID}
// is set: outbound entries point at the corresponding peerSession,
// inbound entries carry the registry ConnID — that field is the
// discriminator (outbound != nil ⇒ outbound tier). The connectedAt
// and protocolVersion fields describe the chosen connection itself —
// they always belong together to the same socket, so callers reading
// both keys never see a mixed snapshot stitched from two sessions.
type peerSendableConnection struct {
	connectedAt     time.Time
	protocolVersion domain.ProtocolVersion
	outbound        *peerSession
	inboundID       domain.ConnID
}

// peerSendableConnectionsLocked returns the ordered list of connections
// the send path would try for (peer, requiredCap) at instant `now`.
//
// This slice is the canonical attempt order: every consumer (live send
// path in sendFrameToIdentity, the file router's diagnostic
// fileTransferPeerRouteMeta, future explainers) walks it in the same
// direction, so they cannot disagree about "which connection bytes
// will use first".
//
// Order:
//  1. Outbound sessions (preferred tier), sorted by:
//     a. oldest LastConnectedAt first — empirically the most stable
//     socket carries the least retry risk;
//     b. tiebreak by sess.address (lexicographic).
//  2. Inbound conns (fall-back tier), sorted by:
//     a. oldest LastConnectedAt first — same stability bias;
//     b. tiebreak by ConnID (monotonic).
//
// Sorting is required because s.sessions and the inbound registry are
// Go maps, whose iteration order is randomised between traversals. A
// raw map walk would let two sequential calls return different "first"
// candidates, and that drift is exactly what made the diagnostic
// rank a peer by an outbound session the next sendFrameToIdentity
// call would never even reach. Within a tier we sort by oldest
// connectedAt (matches the file router's stability tie-break — see
// routeCandidateLess), then by an immutable per-connection key so the
// total order is stable.
//
// Filters: identity match, capability present, health entry that is
// connected and not stalled. The "activation gate" comments inside
// sendFrameToIdentity describe why health is required even for
// just-handshaken outbound and inbound entries; the same rules apply
// here so the file router's diagnostic and the live send path never
// disagree on which connections are eligible.
//
// Caller must hold s.peerMu (R or W). The returned slice is safe to
// retain after the lock is released — connectedAt and protocolVersion
// are values, peerSession pointers are immutable for the lifetime of
// the session, and ConnID is an opaque integer.
func (s *Service) peerSendableConnectionsLocked(peer domain.PeerIdentity, requiredCap domain.Capability, now time.Time) []peerSendableConnection {
	var outbound, inbound []peerSendableConnection

	// Outbound tier collection. Activation gate: outbound bring-up
	// inserts into s.sessions BEFORE markPeerConnected, so during that
	// window health == nil and the session is not authoritative for
	// outbound sends.
	//
	// We deliberately do NOT filter by sess.conn != nil here — that is
	// a runtime invariant of the send path (a fully-constructed session
	// always carries a conn, the check in sendFrameToIdentity is purely
	// defensive). Keeping the helper's selection policy at the level of
	// "identity + capability + health" keeps the shared contract narrow
	// and lets diagnostic surfaces reuse it without inheriting the
	// runtime-only nil-guard.
	for _, sess := range s.sessions {
		if sess == nil || sess.peerIdentity != peer {
			continue
		}
		if !hasCapability(sess.capabilities, requiredCap) {
			continue
		}
		health := s.health[s.resolveHealthAddress(sess.address)]
		if !s.peerHealthAcceptsOutboundFramesLocked(health, now) {
			continue
		}
		outbound = append(outbound, peerSendableConnection{
			connectedAt:     health.LastConnectedAt,
			protocolVersion: domain.ProtocolVersion(sess.version),
			outbound:        sess,
		})
	}

	// Inbound tier collection from the registry carve-out. Outbound
	// NetCores surface through s.sessions above; an outbound NetCore
	// that has completed handshake but is not yet in s.sessions would
	// otherwise be reachable here with health == nil and bypass the
	// activation gate — so skip Outbound and only consider Inbound
	// here. Same activation gate applies: registerInboundConn creates
	// the NetCore before hello/auth, identity/capabilities land before
	// markPeerConnected, so require Connected health to keep
	// partially-handshaken inbound conns out of the send-path view.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity != peer || !info.HasCapability(requiredCap) {
			return true
		}
		health := s.health[s.resolveHealthAddress(info.address)]
		if !s.peerHealthAcceptsOutboundFramesLocked(health, now) {
			return true
		}
		inbound = append(inbound, peerSendableConnection{
			connectedAt:     health.LastConnectedAt,
			protocolVersion: info.protocolVersion,
			inboundID:       info.id,
		})
		return true
	})

	// Stable total order within each tier — see godoc above for why
	// this matters. SliceStable preserves the caller-visible
	// declaration order for inputs that compare equal under our keys,
	// although our tiebreakers (address / ConnID) are themselves
	// total, so equal-comparing inputs are exotic.
	sort.SliceStable(outbound, func(i, j int) bool {
		return peerSendableConnectionLess(outbound[i], outbound[j])
	})
	sort.SliceStable(inbound, func(i, j int) bool {
		return peerSendableConnectionLess(inbound[i], inbound[j])
	})

	if len(outbound) == 0 {
		return inbound
	}
	if len(inbound) == 0 {
		return outbound
	}
	return append(outbound, inbound...)
}

// peerSendableConnectionLess is the within-tier comparator. Outbound
// and inbound entries are NOT compared against each other through this
// function — tier ordering is enforced by the slice concatenation in
// peerSendableConnectionsLocked. Both arms of the conditional treat
// outbound and inbound symmetrically (oldest connectedAt first), but
// the secondary tiebreak differs because outbound entries carry an
// address while inbound entries carry a ConnID.
func peerSendableConnectionLess(a, b peerSendableConnection) bool {
	// Primary key: oldest LastConnectedAt first. Treat zero timestamps
	// as "unknown" and sort them after known ones, matching
	// routeCandidateLess on the filerouter side so the two layers
	// reason about uptime the same way.
	if a.connectedAt.IsZero() != b.connectedAt.IsZero() {
		return !a.connectedAt.IsZero()
	}
	if !a.connectedAt.Equal(b.connectedAt) {
		return a.connectedAt.Before(b.connectedAt)
	}
	// Secondary tiebreak: outbound by address, inbound by ConnID.
	// Mixed arms cannot occur because callers split the tiers before
	// sorting, but the discriminator is preserved here for safety.
	if a.outbound != nil && b.outbound != nil {
		return a.outbound.address < b.outbound.address
	}
	return a.inboundID < b.inboundID
}

// sendFrameToIdentity sends a protocol frame to the peer identified by its
// Ed25519 identity fingerprint. It searches outbound sessions first, then
// inbound connections, checking that the matched connection has the required
// capability. This is the identity-based counterpart of sendFrameToAddress.
//
// Returns true if the frame was accepted into the peer's write queue.
func (s *Service) sendFrameToIdentity(dst domain.PeerIdentity, frame protocol.Frame, requiredCap domain.Capability) bool {
	s.peerMu.RLock()
	candidates := s.peerSendableConnectionsLocked(dst, requiredCap, time.Now().UTC())
	s.peerMu.RUnlock()

	// Tier 1 — outbound sessions. peerSendableConnectionsLocked guarantees
	// outbound entries come before inbound, so iterating in order honours
	// the "outbound first, inbound fall-back" send-path policy without a
	// separate sort.
	//
	// The helper does not filter by sess.conn != nil — that is a runtime
	// invariant only the actual write path needs. We re-check here so a
	// half-constructed session (should never happen in production, but
	// the original code defended against it) is skipped before the
	// non-blocking enqueue.
	hasInbound := false
	for _, c := range candidates {
		if c.outbound != nil {
			if c.outbound.conn == nil {
				continue
			}
			if s.enqueueSessionSendItem(c.outbound, legacyPeerSendItem(frame)) {
				return true
			}
			continue
		}
		hasInbound = true
	}

	if !hasInbound {
		return false
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", dst.String()).
			Str("frame_type", frame.Type).
			Msg("sendFrameToIdentity: marshal failed")
		return false
	}

	data := []byte(line)
	network := s.Network()
	for _, c := range candidates {
		if c.outbound != nil {
			continue
		}
		// NetCore writer loop retains the slice after enqueue, so each
		// candidate gets its own immutable copy. Send via the Network
		// surface so the call path no longer threads *netcore.NetCore
		// through identity-based dispatch — registry resolution and
		// SendStatus → error mapping live behind the bridge.
		payload := append([]byte(nil), data...)
		if network.SendFrame(s.runCtx, c.inboundID, payload) == nil {
			return true
		}
	}
	return false
}

// writeJSONFrameByID marshals the frame and enqueues it on the NetCore
// resolved from s.conns by ConnID. Returns ErrUnregisteredWrite when the
// connection has no registered NetCore — a single-writer-invariant
// violation (see ErrUnregisteredWrite). Returns nil on enqueueSent and
// on enqueueDropped: a dropped frame (buffer full / channel closed) is
// an operational condition handled by enqueueFrameByID (slow-peer
// disconnect + warn log), not an architectural error the caller must
// react to. Callers must acknowledge the return:
// `_ = s.writeJSONFrameByID(...)` for fire-and-forget paths,
// `if err := ...; err != nil { ... }` for paths that want to early-return
// on the invariant violation.
func (s *Service) writeJSONFrameByID(id domain.ConnID, frame protocol.Frame) error {
	addr := ""
	if core := s.netCoreForID(id); core != nil {
		addr = core.RemoteAddr()
	}
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		data := append(fallback, '\n')
		res := s.enqueueFrameByID(id, data)
		emitProtocolTrace(addr, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(addr, frame, "writeJSONFrameByID.marshal_fallback")
			return ErrUnregisteredWrite
		}
		return nil
	}
	res := s.enqueueFrameByID(id, []byte(line))
	emitProtocolTrace(addr, frame, res)
	if res == enqueueUnregistered {
		logUnregisteredWrite(addr, frame, "writeJSONFrameByID")
		return ErrUnregisteredWrite
	}
	return nil
}

// enqueueSessionFrame is the session-scoped counterpart of enqueueFrameByID:
// instead of re-resolving the transport via s.conns (netCoreFor), it uses
// the NetCore that the peerSession already owns. Session-local reply paths
// — pong on outbound sessions, push_message/push_delivery_receipt pushes
// to subscribers — must not fail closed with
// unregistered_write just because the registry entry has been reaped or
// never existed (tests); the authoritative writer is session.netCore.
func (s *Service) enqueueSessionFrame(session *peerSession, data []byte) enqueueResult {
	if session == nil || session.netCore == nil {
		return enqueueUnregistered
	}
	switch st := session.netCore.SendRaw(data); st {
	case netcore.SendOK:
		return enqueueSent
	case netcore.SendBufferFull:
		addr := "unknown"
		if session.conn != nil {
			addr = session.conn.RemoteAddr().String()
		}
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		if session.conn != nil {
			_ = session.conn.Close()
		}
		return enqueueDropped
	case netcore.SendChanClosed, netcore.SendWriterDone:
		// Same rule as enqueueFrameByID: the connection is already finished,
		// so the frame is dropped quietly instead of producing one ERROR line
		// per frame and a second teardown.
		return enqueueDropped
	default:
		addr := "unknown"
		if session.conn != nil {
			addr = session.conn.RemoteAddr().String()
		}
		log.Error().Str("addr", addr).Str("status", st.String()).Msg("enqueueSessionFrame: unexpected netcore.SendStatus")
		return enqueueDropped
	}
}

// writeJSONFrameSyncByID serialises a protocol frame and blocks until the
// per-connection writer goroutine has handed the bytes to the socket.
// Use this instead of writeJSONFrameByID on error paths where the caller is
// about to return false and the deferred cleanup will close the connection:
// the sync variant guarantees the error frame reaches the wire before
// teardown, preserving the "write completed before return" contract that
// error-path callers rely on. Returns ErrUnregisteredWrite on the
// single-writer-invariant violation (see writeJSONFrameByID for the error
// contract), nil otherwise.
//
// Post-PR 10.12 the helper has no production call-sites — top-of-loop
// error replies in handleConn / handleCommand (frame-too-large / read-error
// / rate-limited / invalid-JSON) moved to sendFrameViaNetworkSync through
// the injected Network surface. The helper is retained solely because its
// "unregistered ConnID" fallback test in service_test.go pins the
// protocol_trace send_outcome=unregistered invariant. Removal belongs to
// §6.4(a) scope (iv)/(v) cleanup, once the NetCore-backed enqueueFrame*
// helpers themselves can fold behind the Network interface.
func (s *Service) writeJSONFrameSyncByID(id domain.ConnID, frame protocol.Frame) error {
	addr := ""
	if core := s.netCoreForID(id); core != nil {
		addr = core.RemoteAddr()
	}
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		data := append(fallback, '\n')
		res := s.enqueueFrameSyncByID(id, data)
		emitProtocolTrace(addr, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(addr, frame, "writeJSONFrameSyncByID.marshal_fallback")
			return ErrUnregisteredWrite
		}
		return nil
	}
	res := s.enqueueFrameSyncByID(id, []byte(line))
	emitProtocolTrace(addr, frame, res)
	if res == enqueueUnregistered {
		logUnregisteredWrite(addr, frame, "writeJSONFrameSyncByID")
		return ErrUnregisteredWrite
	}
	return nil
}

// emitProtocolTrace writes the send-side protocol_trace entry after the
// enqueue outcome is known. accepted reflects the *effective* send result:
// a frame that was dropped (unregistered conn, buffer full, writer done)
// must not show up as accepted=true — otherwise operators would see a
// successful trace followed by a drop log for the same frame, which is
// exactly the observability lie the PR 3 reviewer flagged. For a non-error
// frame, accepted == true only when the frame actually reached the managed
// writer queue (enqueueSent); any drop path forces accepted=false and adds
// the concrete outcome so the trace is self-describing.
//
// PR 10.6: takes the already-resolved remote address string instead of
// net.Conn — callers extract addr once from their ConnID/NetCore/session
// context, which keeps the diagnostic helper free of the net.Conn-first
// surface.
func emitProtocolTrace(addr string, frame protocol.Frame, res enqueueResult) {
	ev := log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", addr).
		Str("direction", "send").
		Str("command", frame.Type).
		Str("send_outcome", res.String()).
		Bool("accepted", frame.Type != "error" && res == enqueueSent)
	if frame.Type == "error" {
		ev = ev.Str("code", frame.Code).Str("error", frame.Error)
	}
	ev.Msg("protocol_trace")
}

// logUnregisteredWrite records the invariant violation that the reviewer
// behind PR 3 is asking us to surface: after PR 2 every live connection
// must be registered in s.conns before the first write — an
// enqueueUnregistered outcome means the single-writer invariant was
// attempted to be bypassed. The frame is dropped (fail closed) rather than
// slipped around the managed writer via a direct conn.Write: a silent direct
// write would reintroduce exactly the broken state the migration is closing.
// The log includes origin, remote address, and the command type so the
// responsible call-site is immediately identifiable in production logs.
//
// PR 10.6: takes the already-resolved remote address string instead of
// net.Conn. See emitProtocolTrace for the same rationale.
func logUnregisteredWrite(addr string, frame protocol.Frame, origin string) {
	log.Error().
		Str("origin", origin).
		Str("addr", addr).
		Str("command", frame.Type).
		Msg("unregistered_write: conn missing NetCore — single-writer invariant violation, frame dropped")
}

// sendWelcomeFrame answers a peer's hello.
func (s *Service) sendWelcomeFrame(connID domain.ConnID, challenge string, observedAddr string) {
	_ = s.sendFrameViaNetwork(s.runCtx, connID, s.welcomeFrame(challenge, observedAddr))
}

// welcomeFrame builds the welcome this node answers a hello with.
func (s *Service) welcomeFrame(challenge string, observedAddr string) protocol.Frame {
	// v12 cleanup: welcome no longer carries the local advertise host
	// in Listen — host is no longer a wire concept. The Listener flag
	// still signals "this peer accepts inbound" and AdvertisePort
	// carries the listening port; together they replace the old
	// host:port Listen contract. observed_address is still emitted as
	// NAT-detection telemetry consumed by the dialer's
	// recordObservedAddress (see handshake.md "Advertise Convergence");
	// it is NOT projected back into outbound hello any more — the
	// authoritative consumer (selfAdvertiseEndpoint) was removed
	// together with the wire-host emit.
	var advertisePort domain.PeerPort
	if s.cfg.EffectiveListenerEnabled() {
		advertisePort = s.cfg.EffectiveAdvertisePort()
	}
	datagrams := s.localDatagramAdvertise()
	advertised := s.localHandshakeCapabilityNames()
	return protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Node:                   nodeName,
		Network:                networkName,
		Listener:               listenerFlag(s.cfg.EffectiveListenerEnabled()),
		AdvertisePort:          advertisePort,
		NodeType:               string(s.NodeType()),
		ClientVersion:          s.ClientVersion(),
		ClientBuild:            config.ClientVersionBuild,
		Services:               s.Services(),
		Address:                s.identity.Address,
		PubKey:                 identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:                 s.selfBoxKey,
		BoxSig:                 s.selfBoxSig,
		ObservedAddress:        observedAddr,
		Challenge:              challenge,
		Capabilities:           localHandshakeCapabilityStrings(advertised),
		DTypes:                 s.localDTypeStrings(datagrams),
	}
}

func validateProtocolHandshake(frame protocol.Frame) error {
	if frame.Version < config.MinimumProtocolVersion {
		return fmt.Errorf("protocol version %d is too old; supported %d..%d", frame.Version, config.MinimumProtocolVersion, config.ProtocolVersion)
	}
	return nil
}

// isConnAuthenticated returns true when the connection has completed
// session auth (auth_session verified). Connections that never initiated
// auth (NetCore.auth is nil) are considered unauthenticated — they may
// still issue handshake commands, but they should not trigger high-trust
// side effects such as peer promotion.
//
// ConnID-first: callers that start from a net.Conn cross the boundary
// via s.connIDFor(conn) once and then operate on ConnID. A zero or
// unregistered ConnID returns false (fail-safe).
func (s *Service) isConnAuthenticated(id domain.ConnID) bool {
	state := s.connAuthStateByID(id)
	return state != nil && state.Verified
}

// isAuthInitiated returns true if the connection has started (challenge
// issued, Verified=false) or completed (Verified=true) the auth handshake.
// Used by the re-hello guard to block identity/address overwrites once
// PrepareAuth has recorded the initial hello and challenge.
//
// ConnID-first: same boundary convention as isConnAuthenticated.
func (s *Service) isAuthInitiated(id domain.ConnID) bool {
	return s.connAuthStateByID(id) != nil
}

func ackDeletePayload(address, ackType, id, status string) []byte {
	return []byte("corsa-ack-delete-v1|" + address + "|" + ackType + "|" + id + "|" + status)
}

// handleAuthSession verifies the auth_session frame's Ed25519 signature via
// connauth.VerifyAuthSession, then applies post-auth side effects (peer
// learning, route registration, peer announcement, health tracking).
//
// Ban scoring for invalid signatures is applied here because it depends on
// the connection rate limiter which lives in Service. The remote address
// string consumed by learnPeerFromFrame is resolved via the Network
// surface (RemoteAddr(id) returns "" for an unregistered ConnID, which is
// what learnPeerFromFrame already tolerates).
//
// The third return value is the v20 auto-subscribe backlog subscriber (or nil).
// The caller MUST replay it via pushBacklogToSubscriber ONLY AFTER the auth_ok
// reply has been enqueued into the writer — replaying it here would race the
// push_message/push_delivery_receipt frames ahead of auth_ok, and a v20
// initiator treats auth_ok as the handshake boundary (it no longer waits for a
// subscribed frame), so a backlog frame arriving first would break its
// post-handshake framing expectations.
// Two pieces of work are RETURNED rather than done here, and for one reason:
// both write onto this connection, and both must follow the auth_ok that makes
// the peer treat the connection as negotiated. The backlog replay is the older
// of the two; the connect-time full sync joined it after a round in which the
// dialler, having started refusing capability-gated frames until auth_ok, began
// dropping the very route table this call sends.
func (s *Service) handleAuthSession(
	id domain.ConnID,
	frame protocol.Frame,
) (reply protocol.Frame, ok bool, backlogSub *subscriber, fullSync connectFullSync) {
	// Trace checkpoints along this function share conn_id as the
	// correlation key so the inbound-auth arc can be reconstructed from
	// interleaved goroutine logs (announce is spawned async).
	log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_begin")

	state := s.connAuthStateByID(id)
	verified, reply, ok := connauth.VerifyAuthSession(state, frame)
	if !ok {
		if reply.Code == protocol.ErrCodeInvalidAuthSignature {
			s.addBanScore(id, banIncrementInvalidSig)
		}
		log.Trace().Uint64("conn_id", uint64(id)).Str("reply_code", reply.Code).Msg("handle_auth_session_verify_failed")
		return reply, false, nil, connectFullSync{}
	}
	// Already verified — idempotent re-auth returns success immediately.
	if state != nil && state.Verified {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_idempotent")
		return reply, true, nil, connectFullSync{}
	}

	s.setConnAuthStateByID(id, verified)

	// Resolve the remote address through the Network surface. RemoteAddr
	// returns "" for an unregistered ConnID — learnPeerFromFrame tolerates
	// that, and registerHelloRoute treats it as a silent no-op.
	remoteAddr := s.Network().RemoteAddr(id)
	log.Trace().Uint64("conn_id", uint64(id)).Str("remote_addr", remoteAddr).Str("hello_listen", verified.Hello.Listen).Str("peer_identity", verified.Hello.Address).Msg("handle_auth_session_verified")

	s.learnPeerFromFrame(remoteAddr, verified.Hello)
	log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_after_learn")

	helloSub := s.registerHelloRoute(id, verified.Hello)
	log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_after_register_route")

	// The node-route subscriber installed by registerHelloRoute makes us push
	// this peer's inbox to it; the stored-backlog replay happens at auth too.
	// The MinimumProtocolVersion floor guarantees every peer relies on this
	// auth-time registration (the legacy subscribe_inbox round-trip has been
	// removed from the protocol).
	//
	// We do NOT spawn the replay here: it is returned to the caller, which runs
	// it strictly AFTER the auth_ok reply is enqueued, so backlog frames cannot
	// race ahead of auth_ok on the writer (see the function doc).
	backlogSub = helloSub

	// Announce the newly authenticated peer to all active outbound sessions.
	// Only direct neighbors are notified (no recursive relay) and local
	// addresses are excluded to avoid leaking private network topology.
	//
	// We gossip the observed TCP source host combined with the
	// self-reported hello.advertise_port — never the claimed hello.Listen
	// host or port. The peer can lie about its listen IP (stale DDNS,
	// misconfig, or on purpose) but cannot forge the IP the packets
	// actually arrive from. The listen port is also untrusted under the
	// v12 wire contract: a NAT port-forward setup commonly exposes a
	// different external port than the internal bind port, and only the
	// self-reported advertise_port is the externally dialable value.
	// The TCP source port is an ephemeral NAT mapping that no neighbour
	// could dial into, so it is never used either.
	if announceAddr, ok := s.observedAnnounceAddressFromHello(remoteAddr, verified.Hello); ok && classifyAddress(announceAddr) != domain.NetGroupLocal {
		nodeType := verified.Hello.NodeType
		connID := id
		log.Trace().Uint64("conn_id", uint64(connID)).Str("announce_addr", string(announceAddr)).Msg("handle_auth_session_announce_spawn")
		s.goBackground(func() {
			log.Trace().Uint64("conn_id", uint64(connID)).Str("announce_addr", string(announceAddr)).Msg("announce_goroutine_begin")
			s.announcePeerToSessions(string(announceAddr), nodeType)
			log.Trace().Uint64("conn_id", uint64(connID)).Str("announce_addr", string(announceAddr)).Msg("announce_goroutine_end")
		})
	} else {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_announce_skipped")
	}

	if addr := s.inboundPeerAddress(id); addr != "" {
		log.Trace().Uint64("conn_id", uint64(id)).Str("inbound_addr", string(addr)).Msg("handle_auth_session_inbound_addr")

		// Log duplicate but allow: see the hello-path comment for the
		// full rationale — rejecting breaks one-way gossip when both
		// sides dial simultaneously.
		if s.hasOutboundSessionForInbound(addr) {
			log.Info().Str("peer", string(addr)).Msg("duplicate_inbound_auth_session_allowed")
		}
		s.addPeerID(addr, domain.PeerIdentityFromWire(verified.Hello.Address))
		log.Trace().Uint64("conn_id", uint64(id)).Str("inbound_addr", string(addr)).Msg("handle_auth_session_track_begin")
		fullSync = s.trackInboundConnect(id, addr, domain.PeerIdentityFromWire(verified.Hello.Address))
		log.Trace().Uint64("conn_id", uint64(id)).Str("inbound_addr", string(addr)).Msg("handle_auth_session_track_end")
		s.addPeerVersion(addr, verified.Hello.ClientVersion)
		s.addPeerBuild(addr, verified.Hello.ClientBuild)

		// Mirror identity/address onto the NetCore so that by-conn readers
		// (trackInboundDisconnect) can derive identity from transport state
		// instead of re-reading the address-keyed persistence cache.
		// Invariant: every live-conn update of peerIDs must also update the
		// NetCore mirror so the two sources cannot diverge mid-session.
		// Precedence of NetCore over peerIDs is pinned by
		// TestTrackInboundDisconnect_PrefersNetCoreIdentity and the peerIDs
		// fallback branch by TestTrackInboundDisconnect_FallsBackToPeerIDsMap.
		// Mirror identity/address into the NetCore. The handle is resolved
		// here (not at handleAuthSession entry) because this is the only
		// branch that needs it; nil result means the conn was unregistered
		// between auth completion and this mirror — fail silently, the
		// disconnect path will reconcile.
		if core := s.netCoreForID(id); core != nil {
			core.SetIdentity(domain.PeerIdentityFromWire(verified.Hello.Address))
			core.SetAddress(addr)
		}
	} else {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_no_inbound_addr")
	}

	log.Trace().Uint64("conn_id", uint64(id)).Msg("handle_auth_session_end")
	return reply, true, backlogSub, fullSync
}

// authenticatedAddressForConn returns the verified hello frame for a
// successfully authenticated connection, or zero-value + false otherwise.
// ConnID-first: callers resolve id via s.connIDFor(conn) at the entry
// boundary (handleConn, dispatchNetworkFrame, handleAckDeleteFrame) and
// pass it through here.
func (s *Service) authenticatedAddressForConn(id domain.ConnID) (protocol.Frame, bool) {
	pc := s.netCoreForID(id)
	if pc == nil {
		return protocol.Frame{}, false
	}
	state := pc.Auth()
	if state == nil || !state.Verified {
		return protocol.Frame{}, false
	}
	return state.Hello, true
}

// clearConnAuth drops the auth state from the NetCore owning id. No-op if
// the connection is not registered or has no auth state.
func (s *Service) clearConnAuth(id domain.ConnID) {
	if pc := s.netCoreForID(id); pc != nil {
		pc.ClearAuth()
	}
}

// connAuthStateByID is the ConnID-first primary reader for auth state.
// Returns nil when the connection is not registered or has no auth state.
// ConnAuthState is a thin wrapper over this helper retained only to honor
// the connauth.AuthStore external contract (pinned to net.Conn).
func (s *Service) connAuthStateByID(id domain.ConnID) *connauth.State {
	pc := s.netCoreForID(id)
	if pc == nil {
		return nil
	}
	return pc.Auth()
}

// setConnAuthStateByID is the ConnID-first primary writer for auth state.
// No-op when the connection is not registered. SetConnAuthState is a thin
// wrapper over this helper retained only to honor connauth.AuthStore.
func (s *Service) setConnAuthStateByID(id domain.ConnID, state *connauth.State) {
	if pc := s.netCoreForID(id); pc != nil {
		pc.SetAuth(state)
	}
}

// ConnAuthState implements connauth.AuthStore. Thin wrapper that crosses
// the net.Conn → ConnID boundary once via connIDFor and delegates to
// connAuthStateByID. The external interface pins net.Conn, so this
// carve-out is structural and not subject to Phase 2 migration. Carve-out
// membership is frozen; see the canonical list at the top of conn_registry.go.
func (s *Service) ConnAuthState(conn net.Conn) *connauth.State {
	id, ok := s.connIDFor(conn)
	if !ok {
		return nil
	}
	return s.connAuthStateByID(id)
}

// SetConnAuthState implements connauth.AuthStore. Same carve-out rationale
// as ConnAuthState: thin wrapper over the ConnID-first primary writer.
// Carve-out membership is frozen; see the canonical list at the top of
// conn_registry.go.
func (s *Service) SetConnAuthState(conn net.Conn, state *connauth.State) {
	id, ok := s.connIDFor(conn)
	if !ok {
		return
	}
	s.setConnAuthStateByID(id, state)
}

// rememberConnPeerAddr folds the inbound peer's hello-derived state
// onto the NetCore for later health tracking, relay lookups and
// file-router ranking. Despite the legacy name this populates more
// than the address: identity, capabilities, networks, last activity,
// and the negotiated protocol version (hello.Version) all flow through
// the same ApplyOpts call so they land atomically on the NetCore.
//
// For inbound connections the address is built from the verified TCP
// IP and the self-reported advertise_port (collapsed to the validated
// PeerPort form by extractAdvertisePort). hello.Listen is intentionally
// not consulted: under the v12 wire contract it does not carry an
// authoritative host or port any more. Using the TCP IP as the host
// component prevents a malicious peer from injecting a health entry
// under an arbitrary address while still consolidating multiple
// connections from the same peer under a single health key.
//
// When the TCP IP is unparseable (non-IP transport, malformed wrapper
// output) the helper falls back to hello.Address — the same legacy
// fallback the pre-v12 path used — so health-tracking still gets a
// stable key even on the degenerate edge.
//
// The ProtocolVersion fold-in is the inbound counterpart of
// applyWelcomeMetadata's outbound mirror — it is the value the file
// router's inbound carve-out reads through snapshotEntryLocked when
// ranking next-hops, so dropping it here would silently re-introduce
// the "inbound peers always rank as version 0" bug.
//
// tcpAddr is the real TCP RemoteAddr string (host:port) from the
// connection. For outbound sessions the caller may pass "" to skip
// sanitisation.
func (s *Service) rememberConnPeerAddr(id domain.ConnID, hello protocol.Frame, tcpAddr string) {
	addr := peerAddressFromInbound(tcpAddr, extractAdvertisePort(hello))
	if addr == "" {
		// TCP host not parseable (non-IP transport, malformed wrapper
		// output): fall back to the peer's identity so the NetCore
		// still has a non-empty key. Mirrors the pre-v12 fallback
		// shape — health tracking is best-effort on this branch.
		addr = strings.TrimSpace(hello.Address)
	}
	pc := s.netCoreForID(id)
	if pc == nil {
		return
	}
	// The raw declarations ride the SAME ApplyOpts call as the typed caps so
	// both land on the NetCore atomically: a session whose typed set came
	// from one hello and whose raw set came from another would be two
	// conflicting views of one handshake.
	declarations := declarationsFromHandshake(hello)
	pc.ApplyOpts(netcore.Options{
		Address:         domain.PeerAddress(addr),
		Identity:        domain.PeerIdentityFromWire(strings.TrimSpace(hello.Address)),
		LastActivity:    time.Now().UTC(),
		Networks:        domain.ParseNetGroups(hello.Networks),
		Caps:            intersectCapabilities(localCapabilities(s.cfg.EnableMeshRoutingV3, s.localDatagramAdvertise()), hello.Capabilities),
		ProtocolVersion: domain.ProtocolVersion(hello.Version),
		Declarations:    &declarations,
	})
}

// inboundPeerAddress returns the sanitised overlay address for health
// tracking: verified TCP IP combined with the declared listen port.
// Returns "" if the address is not yet known (pre-hello).
// ConnID-first: callers that start from a net.Conn cross the boundary
// once via connIDFor at the entry point and pass the resolved id here.
func (s *Service) inboundPeerAddress(id domain.ConnID) domain.PeerAddress {
	pc := s.netCoreForID(id)
	if pc == nil {
		return ""
	}
	return pc.Address()
}

// trackedInboundPeerAddress returns the peer overlay address only when
// this specific connection has been promoted via trackInboundConnect
// (i.e. after successful authentication or for peers that do not require
// auth). Returns "" if the peer address is unknown or this connection
// has not been promoted, preventing unauthenticated connections from
// creating or refreshing health entries — even if another legitimate
// connection for the same address is already tracked.
func (s *Service) trackedInboundPeerAddress(id domain.ConnID) domain.PeerAddress {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.trackedInboundAddressByIDLocked(id)
}

// trackInboundConnect increments the inbound connection reference count
// for the given overlay address, marks the concrete connection as promoted,
// and marks the peer as connected when this is the first active inbound
// connection for that address. peerIdentity is the Ed25519 fingerprint
// from the hello/auth frame — used for routing table registration instead
// of the transport address.
// connectFullSync is the connect-time route table this call decided to send,
// described rather than performed.
//
// DATA and not a closure, and the difference is enforced rather than stylistic:
// the lifecycle guard classifies a goroutine by reading the AST at its start,
// so a `goBackground` handed an opaque function value is a goroutine nobody —
// neither the guard nor a reviewer — can tell waits from one that does not. The
// caller builds the literal, and what runs inside it stays visible at the line
// that starts it.
type connectFullSync struct {
	// peer is whose table this is, by the identity the handshake proved.
	peer domain.PeerIdentity
	// due is false when the capability gate refused or the call was a repeat.
	due bool
}

// The connect-time full sync is DESCRIBED rather than sent: it must not reach
// the wire before the auth_ok that makes the peer treat this connection as
// negotiated.
func (s *Service) trackInboundConnect(
	id domain.ConnID,
	address domain.PeerAddress,
	peerIdentity domain.PeerIdentity,
) (fullSync connectFullSync) {
	log.Trace().Uint64("conn_id", uint64(id)).Str("address", string(address)).Str("peer_identity", peerIdentity.String()).Msg("track_inbound_connect_begin")

	log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_before_lock")
	log.Trace().Str("site", "trackInboundConnect").Str("phase", "lock_wait").Uint64("conn_id", uint64(id)).Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "trackInboundConnect").Str("phase", "lock_held").Uint64("conn_id", uint64(id)).Str("address", string(address)).Msg("peer_mu_writer")
	log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_lock_acquired")
	resolved := s.resolveHealthAddress(address)
	first := s.inboundHealthRefs[resolved] == 0
	s.inboundHealthRefs[resolved]++
	s.setTrackedByIDLocked(id, true)
	s.peerMu.Unlock()
	log.Trace().Str("site", "trackInboundConnect").Str("phase", "lock_released").Uint64("conn_id", uint64(id)).Str("address", string(address)).Msg("peer_mu_writer")
	log.Trace().Uint64("conn_id", uint64(id)).Str("resolved", string(resolved)).Bool("first", first).Msg("track_inbound_connect_lock_released")

	log.Info().Str("node", s.identity.Address).Str("peer_identity", peerIdentity.String()).Str("address", string(address)).Str("resolved", string(resolved)).Bool("first", first).Msg("track_inbound_connect")

	if first {
		log.Trace().Uint64("conn_id", uint64(id)).Str("resolved", string(resolved)).Msg("track_inbound_connect_before_mark_connected")
		s.markPeerConnected(resolved, peerDirectionInbound)
		log.Trace().Uint64("conn_id", uint64(id)).Str("resolved", string(resolved)).Msg("track_inbound_connect_after_mark_connected")
	}

	// Downstream helpers (connHasCapability, sendFullTableSyncToInbound,
	// flushPendingFireAndForget) are ConnID-first. If the connection has
	// already been unregistered (race with teardown), skip downstream
	// side-effects — routing and fire-and-forget drains are inherently
	// tied to a live connection. The empty-RemoteAddr probe is the
	// ConnID-first equivalent of the prior *netcore.NetCore nil-guard.
	if s.Network().RemoteAddr(id) == "" {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_conn_gone")
		return
	}

	// Routing table: register direct peer using the identity fingerprint,
	// not the transport address. The hook flattens caps to a relay-cap
	// decision internally and forwards the full list to AnnouncePeerState
	// so routing-announce v2 can record what the peer actually supports
	// without a second s.peerMu RLock round-trip.
	log.Trace().Uint64("conn_id", uint64(id)).Str("peer_identity", peerIdentity.String()).Msg("track_inbound_connect_before_on_peer_session")
	s.onPeerSessionEstablished(peerIdentity, s.connCapabilitiesForID(id))
	log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_after_on_peer_session")
	// The inbound peer's identity may now be directly reachable (a direct
	// route was added above if it is relay-capable): re-arm any held
	// sender-owned DM for it. Self-checked inside the kick, so a non-relay
	// inbound peer (no usable route) is a no-op. The outbound path does the
	// same in servePeerSession.
	s.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		peerIdentity: {},
	})

	// Send full table sync to the inbound peer (Phase 1.2: full sync on
	// connect, symmetric with the outbound path).
	// Both capabilities required: mesh_routing_v1 (understands announce_routes)
	// and mesh_relay_v1 (can carry relay traffic). A routing-only peer would
	// learn routes it cannot deliver on the data plane.
	//
	// Dispatched on its own goroutine: the underlying SendAnnounceRoutes
	// makes a synchronous inbound write bounded by syncFlushTimeout (5s).
	// Running it inline would pin the inbound connection handler for up to
	// that interval when the newly-connected peer already has a half-dead
	// socket, delaying flushPendingFireAndForget and any follow-up frames
	// on the same conn. AnnouncePeerState is thread-safe and sendCacheFn
	// already guards cache mutation with its own mutex.
	if s.connHasCapability(id, domain.CapMeshRoutingV1) && s.connHasCapability(id, domain.CapMeshRelayV1) {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_full_sync_deferred")
		// RETURNED, not spawned. The caller runs it strictly AFTER auth_ok has
		// been enqueued, for the same reason the backlog replay is deferred:
		// this write goes onto the same connection, and the dialler treats a
		// capability as negotiated only once auth_ok has landed. A goroutine
		// started here races the auth_ok it must follow, and the race it can
		// win costs the peer the whole connect-time route table — silently,
		// because on the dialler this frame arrives before its own gate opens.
		fullSync = connectFullSync{peer: peerIdentity, due: true}
	} else {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_full_sync_skipped")
	}

	// Drain fire-and-forget frames (push_message, push_notice) that were
	// queued for this peer before the inbound connection was authenticated.
	// The outbound session might not exist (CM slot full), but the inbound
	// conn can carry these frames. Only fire-and-forget frames are safe to
	// send on the inbound conn because they don't expect a response that
	// would interleave with the peer's request/reply traffic.
	log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_before_flush_ff")
	s.flushPendingFireAndForget(id, resolved)
	log.Trace().Uint64("conn_id", uint64(id)).Msg("track_inbound_connect_end")
	return fullSync
}

// trackInboundDisconnectWithPresenceEvidence decrements the inbound connection reference count
// and removes the per-connection tracked flag.
// Only when the last tracked connection for an address closes is the peer
// marked as disconnected — earlier closes are silent so that the health
// row stays connected while at least one TCP session remains alive.
// If trackInboundConnect was never called for this connection (e.g. auth
// failed), the disconnect is silently ignored to avoid creating phantom
// health entries for unauthenticated connections.
// The explicit evidence argument prevents callers from manufacturing a remote
// FIN when the teardown source was not observed.
func (s *Service) trackInboundDisconnectWithPresenceEvidence(id domain.ConnID, address domain.PeerAddress, presenceEvidence *peerOfflineEvidence) {
	log.Trace().Str("site", "trackInboundDisconnect").Str("phase", "lock_wait").Uint64("conn_id", uint64(id)).Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "trackInboundDisconnect").Str("phase", "lock_held").Uint64("conn_id", uint64(id)).Msg("peer_mu_writer")
	var (
		wasTracked   bool
		peerIdentity domain.PeerIdentity
		core         *netcore.NetCore
	)
	if entry := s.connEntryByIDLocked(id); entry != nil {
		wasTracked = entry.tracked
		entry.tracked = false
		// Prefer NetCore as the source of truth for transport-level
		// identity: it is updated in the inbound auth mirror (see the
		// SetIdentity call next to the peerIDs map write in
		// handleAcceptedHello), so the two sources cannot diverge while
		// the conn is live. Fallback to the persistence cache below
		// keeps behaviour for paths that never call SetIdentity on the
		// NetCore (auth-not-required, legacy tests that bypass the
		// inbound-auth path). This precedence is pinned by
		// TestTrackInboundDisconnect_PrefersNetCoreIdentity and its
		// fallback branch TestTrackInboundDisconnect_FallsBackToPeerIDsMap.
		// connIdentityByIDLocked returns "" both when the entry is missing
		// and when it has no core; the entry-not-nil guard above is held by
		// the surrounding block so the only nil case left for the helper is
		// a missing core, which the fallback handles.
		peerIdentity = s.connIdentityByIDLocked(id)
		// coreForIDLocked is the single carve-out for the live handle, so the
		// queue fence after the unlock reaches it the same way every other
		// production read of a NetCore does (docs/netcore-migration.md scope (v)).
		core = s.coreForIDLocked(id)
	}
	resolved := s.resolveHealthAddress(address)
	if peerIdentity.IsZero() {
		peerIdentity = s.peerIDs[resolved]
	}
	var last bool
	if wasTracked && s.inboundHealthRefs[resolved] > 0 {
		s.inboundHealthRefs[resolved]--
		last = s.inboundHealthRefs[resolved] == 0
		if last {
			delete(s.inboundHealthRefs, resolved)
		}
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "trackInboundDisconnect").Str("phase", "lock_released").Uint64("conn_id", uint64(id)).Bool("last", last).Msg("peer_mu_writer")

	// Stop the queue accepting BEFORE the peer is published as gone, and
	// unconditionally: this connection is being torn down whether or not it was
	// the last reference holding the health row up. Published first, the
	// disconnect let a sender that had just passed the peer-state gate deposit a
	// frame into a queue the teardown below discards, and read SendOK for it —
	// so the datagram emitter counted a delivery and stopped walking the peer's
	// remaining connections. The socket and the writer are left alone; the
	// transport is closed by the caller's own teardown a few statements later
	// (handleConn's defer), which is also where the registry entry this function
	// still needs is removed.
	//
	// Outside peerMu on purpose. Not because an atomic gate raise would be
	// unsafe under it, but because the rule that keeps this file readable is
	// "side effects after the domain mutexes are released" (docs/locking.md) —
	// and the ordering that matters here is against markPeerDisconnected below,
	// which cannot run until this returns.
	if core != nil {
		core.ShutSendQueue()
	}

	// Between the two publications, and outside every lock: the only point from
	// which their order is observable. See the peerTeardownBarrier field.
	s.runPeerTeardownBarrier()

	if last {
		s.markPeerDisconnected(resolved, nil)

		// Notify the ConnectionManager that an inbound slot freed up.
		// This is a hint (non-blocking, safe to drop) — CM may choose
		// to backfill the lost inbound with an outgoing dial.
		if s.connManager != nil {
			ip, _, _ := splitHostPort(string(resolved))
			s.connManager.EmitHint(InboundClosed{IP: ip, Identity: peerIdentity})
		}
	}

	// Routing table: deregister direct peer when the last relay-capable
	// inbound session closes. Uses the identity fingerprint, not transport
	// address, to match what was passed to onPeerSessionEstablished.
	// connCapabilitiesForID returns nil when NetCore is already gone
	// (unregisterConnLocked raced ahead). A nil slice flows through
	// sessionHasCap cleanly — the hook treats it as "no relay cap" and
	// only decrements total session counts, which is the safe default for
	// a torn-down connection.
	if wasTracked {
		s.onPeerSessionClosedWithAttribution(
			peerIdentity,
			s.connCapabilitiesForID(id),
			sessionClosePeerInitiated,
			presenceEvidence,
		)
	}
}

// connPeerReachableGroups returns the set of network groups the remote
// peer can reach, for use in peer exchange filtering.
//
// Priority:
//  1. If the peer declared "networks" in its hello, validate them against
//     the advertised address and use the intersection.  This prevents a
//     clearnet peer from claiming overlay reachability to harvest .onion
//     or .i2p addresses.
//  2. Otherwise infer from the peer's advertised address (listen or identity).
//  3. If we have no usable information, return nil (= no filtering, include
//     all routable addresses).  This is the safe backward-compatible default
//     for old clients that don't send "networks".
func (s *Service) connPeerReachableGroups(id domain.ConnID) map[domain.NetGroup]struct{} {
	pc := s.netCoreForID(id)
	if pc == nil {
		return nil
	}

	// Authenticated peers: trust declared networks and advertised address
	// from the hello frame — the identity behind these claims has been
	// verified by auth_session.
	if s.isConnAuthenticated(id) {
		if nets := pc.Networks(); len(nets) > 0 {
			return validateDeclaredNetworks(nets, pc.Address())
		}
		if addr := pc.Address(); addr != "" {
			g := classifyAddress(addr)
			if g != domain.NetGroupUnknown && g != domain.NetGroupLocal {
				return peerReachableGroups(addr)
			}
		}
		return nil
	}

	// Unauthenticated peers: do NOT trust hello-declared networks or
	// advertised address — both are attacker-controlled. An unauth peer
	// could claim .onion/.i2p reachability to extract overlay addresses
	// it has no proven right to access. Instead, classify by the actual
	// TCP remote address.
	if remote := pc.RemoteAddr(); remote != "" {
		host, _, err := net.SplitHostPort(remote)
		if err == nil {
			g := classifyAddress(domain.PeerAddress(host))
			if g != domain.NetGroupUnknown && g != domain.NetGroupLocal {
				return peerReachableGroups(domain.PeerAddress(host))
			}
		}
	}

	// Cannot determine network from TCP endpoint (e.g. Tor circuit,
	// localhost). Return nil so peersFrame includes all routable addresses.
	return nil
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return remoteIPFromString(addr.String())
}

// remoteIPFromString extracts the IP portion of an already-stringified
// "host:port" address. Counterpart of remoteIP for call sites that have a
// string (e.g. *netcore.NetCore.RemoteAddr()) and therefore no net.Addr.
// Falls back to the raw input if the string is not in host:port form.
func remoteIPFromString(s string) string {
	if s == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(s)
	if err == nil {
		return host
	}
	return s
}

// observedAddrConsensusThreshold is the minimum number of distinct peers
// that must report the same observed IP before the NAT-detection log
// fires. The threshold guards against a single hostile or buggy peer
// triggering a misleading diagnostic on its own.
const observedAddrConsensusThreshold = 2

// recordObservedAddress stores the IP that a remote peer observed for our
// outbound connection. Observations are keyed by peer identity
// (fingerprint), not by dial address, so the same node always
// contributes exactly one vote regardless of how many address aliases
// it has. When enough distinct peers agree on the same public IP and
// it differs from the bind host in cfg.ListenAddress, the node logs
// a NAT detection event.
//
// Telemetry-only under the v12 baseline: there is no authoritative
// consumer of the consensus IP. The legacy selfAdvertiseEndpoint
// helper that used to publish this IP into outbound hello.Listen was
// removed together with the wire-host emit, so observed_address is
// kept on the wire purely as a diagnostic signal for operators.
func (s *Service) recordObservedAddress(peerID domain.PeerIdentity, observedIP string) {
	if peerID.IsZero() || observedIP == "" {
		return
	}
	ip := net.ParseIP(observedIP)
	if ip == nil {
		return
	}
	// Ignore private, loopback, link-local, CGNAT and ULA addresses —
	// they are never useful as externally-visible observations. The same
	// non-routable set is enforced here that advertise validation and
	// announce filtering use, so "what counts as world-reachable" stays
	// single-sourced through isNonRoutableIP.
	if isNonRoutableIP(ip) {
		return
	}

	// ipStateMu, not s.peerMu: observedAddrs lives in the IP/advertise domain.
	log.Trace().Str("site", "recordObservedAddress").Str("phase", "lock_wait").Str("peer_id", peerID.String()).Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "recordObservedAddress").Str("phase", "lock_held").Str("peer_id", peerID.String()).Msg("ip_state_mu_writer")
	defer func() {
		s.ipStateMu.Unlock()
		log.Trace().Str("site", "recordObservedAddress").Str("phase", "lock_released").Str("peer_id", peerID.String()).Msg("ip_state_mu_writer")
	}()

	s.observedAddrs[peerID] = observedIP

	// Count how many distinct peers agree on the same IP.
	votes := make(map[string]int, len(s.observedAddrs))
	for _, obs := range s.observedAddrs {
		votes[obs]++
	}

	best, bestCount := "", 0
	for addr, cnt := range votes {
		if cnt > bestCount {
			best, bestCount = addr, cnt
		}
	}

	if bestCount < observedAddrConsensusThreshold {
		return
	}

	// Compare with the listen address host. Under the v12 wire contract
	// the local node has no "configured advertise host" — neighbours
	// announce this node using observed TCP source host + advertise_port.
	// The comparison here is purely diagnostic: it surfaces "your bind
	// host is loopback / private but peers see you at a public IP" so
	// operators behind unexpected NAT / port-forward setups have a
	// single log line to grep for.
	listenHost, _, ok := splitHostPort(s.cfg.ListenAddress)
	if !ok || listenHost == "" {
		// Wildcard bind (":port") leaves listenHost empty; without a
		// concrete host there is no disagreement to flag — the node
		// listens on every interface, so any observed external IP is
		// expected behaviour, not NAT misconfig.
		return
	}
	if listenHost == best {
		return // bind host matches what peers see — nothing to flag
	}
	listenIP := net.ParseIP(listenHost)
	if listenIP != nil && listenIP.IsUnspecified() {
		// "0.0.0.0" / "::" bind: same as the wildcard ":port" case
		// above — an unspecified address means "all interfaces", so
		// disagreement with a concrete observed IP is normal, not a
		// NAT misconfig signal.
		return
	}
	if listenIP != nil && !listenIP.IsPrivate() && !listenIP.IsLoopback() {
		return // bind host already names a public IP — disagreement is noise
	}
	log.Warn().
		Int("count", bestCount).
		Str("observed_ip", best).
		Str("local_listen", s.cfg.ListenAddress).
		Msg("nat_detected: peers consistently observe a public IP that disagrees with the local bind host; informational only — under the v12 wire contract no host is published, neighbours announce this node using observed TCP source + advertise_port")
}

// isBlacklistedConn is net.Conn-first by the carve-out list in
// conn_registry.go: pre-registration IP policy. The connection is not yet
// in the registry, so no ConnID exists at this call site — only the
// network-level RemoteAddr() of the raw socket is meaningful. The
// peer-banned notice that informs the dialler is emitted on the original
// session at the moment the blacklist flips on (see addBanScore); raw
// reconnects observed here are closed silently.
func (s *Service) isBlacklistedConn(conn net.Conn) bool {
	ip := remoteIP(conn.RemoteAddr())
	if ip == "" {
		return false
	}
	// ipStateMu, not s.peerMu: bans live in the IP/advertise domain.  Writer
	// lock (not RLock) because the expired-entry cleanup below deletes from
	// the map.
	log.Trace().Str("site", "isBlacklistedConn").Str("phase", "lock_wait").Str("ip", ip).Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "isBlacklistedConn").Str("phase", "lock_held").Str("ip", ip).Msg("ip_state_mu_writer")
	defer func() {
		s.ipStateMu.Unlock()
		log.Trace().Str("site", "isBlacklistedConn").Str("phase", "lock_released").Str("ip", ip).Msg("ip_state_mu_writer")
	}()
	entry, ok := s.bans[ip]
	if !ok {
		return false
	}
	if !entry.Blacklisted.IsZero() && time.Now().UTC().Before(entry.Blacklisted) {
		log.Debug().Str("addr", ip).Time("until", entry.Blacklisted).Msg("reject connection: blacklisted")
		return true
	}
	if !entry.Blacklisted.IsZero() && time.Now().UTC().After(entry.Blacklisted) {
		delete(s.bans, ip)
	}
	return false
}

// tryIncrementIPConn atomically checks and increments the per-IP inbound
// connection counter. Returns false if the IP has reached maxConnPerIP.
func (s *Service) tryIncrementIPConn(ip string) bool {
	if ip == "" {
		return true
	}
	// ipStateMu, not s.peerMu: inboundByIP is an IP-domain counter.
	log.Trace().Str("site", "tryIncrementIPConn").Str("phase", "lock_wait").Str("ip", ip).Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "tryIncrementIPConn").Str("phase", "lock_held").Str("ip", ip).Msg("ip_state_mu_writer")
	defer func() {
		s.ipStateMu.Unlock()
		log.Trace().Str("site", "tryIncrementIPConn").Str("phase", "lock_released").Str("ip", ip).Msg("ip_state_mu_writer")
	}()
	if s.inboundByIP[ip] >= maxConnPerIP {
		return false
	}
	s.inboundByIP[ip]++
	return true
}

// decrementIPConn decrements the per-IP inbound connection counter.
func (s *Service) decrementIPConn(ip string) {
	if ip == "" {
		return
	}
	// ipStateMu, not s.peerMu: inboundByIP is an IP-domain counter.
	log.Trace().Str("site", "decrementIPConn").Str("phase", "lock_wait").Str("ip", ip).Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "decrementIPConn").Str("phase", "lock_held").Str("ip", ip).Msg("ip_state_mu_writer")
	defer func() {
		s.ipStateMu.Unlock()
		log.Trace().Str("site", "decrementIPConn").Str("phase", "lock_released").Str("ip", ip).Msg("ip_state_mu_writer")
	}()
	if s.inboundByIP[ip] > 1 {
		s.inboundByIP[ip]--
	} else {
		delete(s.inboundByIP, ip)
	}
}

func (s *Service) addBanScore(id domain.ConnID, delta int) {
	pc := s.netCoreForID(id)
	if pc == nil || delta <= 0 {
		return
	}
	host, _, err := net.SplitHostPort(pc.RemoteAddr())
	if err != nil || host == "" {
		return
	}
	ip := host
	// ipStateMu, not s.peerMu: bans live in the IP/advertise domain.
	log.Trace().Str("site", "addBanScore").Str("phase", "lock_wait").Str("ip", ip).Uint64("conn_id", uint64(id)).Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "addBanScore").Str("phase", "lock_held").Str("ip", ip).Uint64("conn_id", uint64(id)).Msg("ip_state_mu_writer")
	now := time.Now().UTC()
	entry := s.bans[ip]
	previouslyBlacklisted := !entry.Blacklisted.IsZero()
	entry.Score += delta
	entry.LastScored = now
	if entry.Score >= banThreshold {
		entry.Blacklisted = now.Add(banDuration)
	}
	s.bans[ip] = entry
	justBlacklisted := !previouslyBlacklisted && !entry.Blacklisted.IsZero()
	s.ipStateMu.Unlock()
	log.Trace().Str("site", "addBanScore").Str("phase", "lock_released").Str("ip", ip).Uint64("conn_id", uint64(id)).Msg("ip_state_mu_writer")
	if !entry.Blacklisted.IsZero() {
		log.Warn().Str("ip", ip).Int("score", entry.Score).Time("until", entry.Blacklisted).Msg("blacklist")
	}
	if justBlacklisted {
		// Emit a machine-readable peer-banned notice on the still-open
		// ConnID. This rides out through the normal managed-write path
		// (sendFrameViaNetworkSync) so the dialler learns the remote-ban
		// window before the session is torn down. The gate on the dialler
		// side (PeerProvider.RemoteBannedFn) suppresses further retries
		// that would otherwise feed cm_session_setup_failed storms into
		// the ebus. Subsequent raw reconnects from this IP are closed
		// silently by handleConn — no re-emission needed because the
		// dialler-side gate is already armed from this notice.
		s.emitPeerBannedNoticeByID(id, entry.Blacklisted, protocol.PeerBannedReasonBlacklisted)
	}
}

// emitPeerBannedNoticeByID serialises a connection_notice{code=peer-banned}
// frame and routes it through sendFrameViaNetworkSync on the live ConnID.
// Best effort: a marshal or send failure is swallowed (debug-logged) — the
// socket will close and the dialler falls back to its usual retry/back-off,
// at most once. The notice is advisory, not a correctness hinge.
func (s *Service) emitPeerBannedNoticeByID(id domain.ConnID, until time.Time, reason protocol.PeerBannedReason) {
	details, err := protocol.MarshalPeerBannedDetails(until, reason)
	if err != nil {
		log.Debug().Err(err).Msg("peer_banned_notice_marshal_failed")
		return
	}
	notice := protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Status:  protocol.ConnectionStatusClosing,
		Details: details,
	}
	if err := s.sendFrameViaNetworkSync(s.runCtx, id, notice); err != nil {
		log.Debug().Err(err).Str("reason", string(reason)).Msg("peer_banned_notice_send_failed")
	}
}

func (s *Service) identitiesFrame() protocol.Frame {
	s.knowledgeMu.RLock()
	parts := s.known.Snapshot()
	s.knowledgeMu.RUnlock()

	return protocol.Frame{
		Type:       "identities",
		Count:      len(parts),
		Identities: parts,
	}
}

// contactsUnbounded is the limit that means "every contact this node holds",
// and it is what the LOCAL answer asks for: dm_crypto looks a recipient's box
// key up in that list, so a missing entry is a failed lookup rather than a
// smaller reply.
const contactsUnbounded = 0

func (s *Service) contactsFrame() protocol.Frame {
	return s.contactsFrameLimited(contactsUnbounded)
}

// contactsFrameLimited serialises at most `limit` of the known contacts, with
// contactsUnbounded asking for all of them.
//
// The limit is applied DURING the walk, not to its result, and that is the
// point of the parameter existing. A `fetch_contacts` frame costs the requester
// four bytes; answering it by materialising all of s.boxKeys — bounded only by
// maxKnownIdentities plus the pinned trust store — and then cutting the array to
// the wire cap made the responder pay for its whole knowledge base, under
// knowledgeMu.RLock, for a request that paid for a few thousand entries. The
// bounded walk stops at the cap, so the reply costs what the reply is.
//
// The critical section holds no I/O and no callback and builds the wire frames
// in place: the snapshot-then-format shape it replaced copied the same four
// strings twice for every contact, so formatting outside the lock bought a
// second full-size array rather than a shorter lock window.
func (s *Service) contactsFrameLimited(limit int) protocol.Frame {
	s.refreshKnowledgeFromPeers()

	s.knowledgeMu.RLock()
	contacts := make([]protocol.ContactFrame, 0, contactsCapacityFor(limit, len(s.boxKeys)))
	for address, boxKey := range s.boxKeys {
		if limit != contactsUnbounded && len(contacts) >= limit {
			break
		}
		contacts = append(contacts, protocol.ContactFrame{
			Address: address,
			PubKey:  s.pubKeys[address],
			BoxKey:  boxKey,
			BoxSig:  s.boxSigs[address],
		})
	}
	s.knowledgeMu.RUnlock()

	return protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	}
}

// contactsCapacityFor sizes the reply array so a bounded walk never allocates
// for entries it will not visit, and an unbounded one still allocates once.
func contactsCapacityFor(limit, held int) int {
	if limit == contactsUnbounded || limit > held {
		return held
	}
	return limit
}

// contactsFrameForNetwork is the `contacts` reply as it goes ON THE WIRE: the
// same walk as the local answer, stopped at maxContactsPerResponse.
//
// The local builder has no count cap and must not grow one. It answers the RPC
// where dm_crypto looks a recipient's box key up in the list; a trimmed local
// answer would make key lookup fail on a node with many correspondents, for no
// security gain — no wire, no parser and no verification loop is involved there.
//
// On the wire the same list is what the receiver pays a signature verification
// per element for, so it is bounded by the number the receiver accepts. Capping
// HERE rather than only refusing THERE is what keeps the cap from cutting a
// legitimate exchange: a node whose s.boxKeys legitimately exceeds the cap —
// possible up to maxKnownIdentities (50 000) plus the pinned trust store — would
// otherwise have every one of its replies refused by every updated peer.
//
// The cap is spent DURING the walk and not on its result. Building the whole
// array first and slicing it afterwards returned the same bytes but let a
// four-byte request buy a full pass over the responder's knowledge base and two
// arrays sized by it — an amplification of the same class the solicited-reply
// budgets exist to close, and paid under knowledgeMu.RLock.
//
// Which entries survive is decided by Go's randomised map iteration, exactly as
// before: successive fetches from the same peer sample different subsets, so a
// requester converges on the whole set over several passes instead of being
// pinned to one prefix forever. Making the bounded walk deterministic — sorting
// the addresses and taking the first 4096 — was rejected for that reason: it is
// the same cost and it makes the tail of a large node's contact set permanently
// unreachable through this reply. There is no ranking here worth preserving; a
// contact is not more relevant for having been learned earlier.
func (s *Service) contactsFrameForNetwork() protocol.Frame {
	return s.contactsFrameLimited(maxContactsPerResponse)
}

func (s *Service) trustedContactsFrame() protocol.Frame {
	trusted := s.trust.trustedContacts()

	// Short critical section: read s.pubKeys/s.boxKeys under lock, merge
	// with trust-store data outside. Prevents writer starvation on
	// s.knowledgeMu (see peerHealthFrames comment for the same pattern
	// applied to s.peerMu).
	type keySnap struct {
		PubKey string
		BoxKey string
	}
	s.knowledgeMu.RLock()
	keys := make(map[string]keySnap, len(trusted))
	for address := range trusted {
		keys[address] = keySnap{
			PubKey: s.pubKeys[address],
			BoxKey: s.boxKeys[address],
		}
	}
	s.knowledgeMu.RUnlock()

	contacts := make([]protocol.ContactFrame, 0, len(trusted))
	for address, contact := range trusted {
		pubKey := keys[address].PubKey
		if pubKey == "" {
			pubKey = contact.PubKey
		}
		boxKey := keys[address].BoxKey
		if boxKey == "" {
			boxKey = contact.BoxKey
		}
		lastOnlineAt := ""
		if !contact.LastOnlineAt.IsZero() {
			lastOnlineAt = contact.LastOnlineAt.UTC().Format(time.RFC3339Nano)
		}
		contacts = append(contacts, protocol.ContactFrame{
			Address:      address,
			PubKey:       pubKey,
			BoxKey:       boxKey,
			BoxSig:       contact.BoxSignature,
			LastOnlineAt: lastOnlineAt,
		})
	}

	return protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	}
}

func (s *Service) deleteTrustedContactFrame(identity domain.PeerIdentity) protocol.Frame {
	identity = domain.PeerIdentityFromWire(strings.TrimSpace(identity.String()))
	if identity.IsZero() {
		return protocol.Frame{Type: "error", Error: "address is required"}
	}

	// trustMutationMu makes {forget, Unpin} atomic against trustContact's
	// {remember, Pin} — see the field doc for the interleaving this stops.
	s.trustMutationMu.Lock()
	removed, err := s.trust.forget(identity)
	if removed {
		// Revoke the LRU-eviction exemption granted on trust (NewService
		// seed or trustContact). Pins and trust-store rows use the same
		// raw address string, so identity.String() — the key forget just
		// removed — is exactly the pinned key. Without this, add/delete
		// contact churn accretes eviction-immune entries for the life of
		// the process, breaking the "capacity + trust store size" bound
		// on s.known and the key maps. Keyed to removed, NOT to err ==
		// nil: forget applies the in-memory delete before persisting, so
		// on a persist failure the LIVE store has already dropped the
		// contact and the pin must mirror that — the pinned set tracks
		// live trust state, never the disk snapshot.
		s.knowledgeMu.Lock()
		s.known.Unpin(identity.String())
		s.knowledgeMu.Unlock()
	}
	s.trustMutationMu.Unlock()

	// Skip the runtime cleanup only when live state is untouched AND the
	// operation failed (today unreachable: forget errors only after the
	// in-memory delete). Everything else cleans up:
	//   - removed, err == nil — the normal delete;
	//   - removed, err != nil — persist failed, but the LIVE store has
	//     already dropped the contact (same reasoning as Unpin above);
	//     leaving queued/retrying outbound frames and the UI entry
	//     behind would desync them from actual trust state. The persist
	//     error still reaches the caller as an error frame below;
	//   - !removed, err == nil — not in the trust store at all, which is
	//     not an error: the contact may have originated from network
	//     discovery rather than the trusted contacts list, and the
	//     user's delete must still drop its queues and UI entry.
	if err != nil && !removed {
		return protocol.Frame{Type: "error", Error: err.Error()}
	}

	// Drop pending outbound messages destined for the deleted contact.
	// The user explicitly removed this identity, so queued messages
	// should not be delivered.
	s.dropPendingForRecipient(identity.String())

	ebus.PublishContactRemoved(s.eventBus, identity)

	if err != nil {
		return protocol.Frame{Type: "error", Error: err.Error()}
	}
	return protocol.Frame{Type: "ok", Address: identity.String()}
}

// dropPendingForRecipient removes all pending send_message frames addressed
// to the given recipient across every peer queue. It also clears the
// corresponding outbound delivery tracking entries and pending dedup keys.
// All of this is in-memory only.
//
// Cross-domain: writes s.pending / s.pendingKeys / s.outbound
// (delivery-domain, s.deliveryMu) and recomputes the aggregate-status
// snapshot (status-domain, s.statusMu) via refreshAggregatePendingLocked,
// which reads peer-domain fields (health / persistedMeta).  Canonical
// lock order per docs/locking.md: s.peerMu OUTER → s.deliveryMu →
// s.statusMu INNER.  s.peerMu is held for the whole critical section so
// the aggregate recompute sees a stable peer-domain view even though no
// peer-domain mutation happens here.
func (s *Service) dropPendingForRecipient(recipient string) {
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_wait").Str("recipient", recipient).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_held").Str("recipient", recipient).Msg("peer_mu_writer")
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_wait").Str("recipient", recipient).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_held").Str("recipient", recipient).Msg("delivery_mu_writer")

	var dropped int
	var affected []ebus.PeerPendingDelta
	for addr, frames := range s.pending {
		origLen := len(frames)
		kept := frames[:0]
		for _, pf := range frames {
			if pf.Frame.Type == "send_message" && pf.Frame.Recipient == recipient {
				key := pendingFrameKey(addr, pf.Frame)
				delete(s.pendingKeys, key)
				dropped++
				continue
			}
			kept = append(kept, pf)
		}
		if len(kept) == origLen {
			continue // nothing changed for this peer
		}
		if len(kept) == 0 {
			delete(s.pending, addr)
			affected = append(affected, ebus.PeerPendingDelta{Address: addr, Count: 0})
		} else {
			s.pending[addr] = kept
			affected = append(affected, ebus.PeerPendingDelta{Address: addr, Count: len(kept)})
		}
	}

	// Remove outbound delivery entries for the deleted recipient so the
	// UI no longer shows stale "queued"/"retrying" statuses.
	var outboundDropped int
	for id, ob := range s.outbound {
		if ob.Recipient == recipient {
			delete(s.outbound, id)
			outboundDropped++
		}
	}

	// statusMu is INNERMOST per canonical peerMu → deliveryMu → statusMu
	// order — refreshAggregatePendingLocked writes s.aggregateStatus.
	s.statusMu.Lock()
	s.refreshAggregatePendingLocked()
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_released").Str("recipient", recipient).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "dropPendingForRecipient").Str("phase", "lock_released").Str("recipient", recipient).Msg("peer_mu_writer")

	if dropped > 0 || outboundDropped > 0 {
		log.Info().Str("recipient", recipient).Int("pending_dropped", dropped).Int("outbound_dropped", outboundDropped).Msg("dropped_pending_for_deleted_contact")
	}

	for _, d := range affected {
		s.emitPeerPendingChanged(d.Address, d.Count)
	}
	if len(affected) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	}
}

func (s *Service) pendingMessagesFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		topic = "dm"
	}

	// ---------------------------------------------------------------
	// Short critical section: snapshot s.pending and s.outbound under
	// lock, then release before dedup, formatting and sorting.
	// Previously the entire function ran under defer RUnlock — holding
	// the read lock during sort.Slice starved writers (bootstrapLoop's
	// refreshAggregateStatus) and cascaded into UI freezes.
	// ---------------------------------------------------------------
	type pendingSnap struct {
		ID        string
		Recipient string
		Retries   int
		QueuedAt  time.Time
	}
	type outboundSnap struct {
		ID            string
		Recipient     string
		Status        string
		QueuedAt      time.Time
		LastAttemptAt time.Time
		Retries       int
		Error         string
	}

	s.deliveryMu.RLock()
	var pendingItems []pendingSnap
	for _, frames := range s.pending {
		for _, item := range frames {
			f := item.Frame
			if f.Topic != topic || f.Type != "send_message" || f.ID == "" {
				continue
			}
			pendingItems = append(pendingItems, pendingSnap{
				ID:        f.ID,
				Recipient: f.Recipient,
				Retries:   item.Retries,
				QueuedAt:  item.QueuedAt,
			})
		}
	}
	var outboundItems []outboundSnap
	for id, ob := range s.outbound {
		if ob.Status == "" || ob.Status == "sent" {
			continue
		}
		outboundItems = append(outboundItems, outboundSnap{
			ID:            id,
			Recipient:     ob.Recipient,
			Status:        ob.Status,
			QueuedAt:      ob.QueuedAt,
			LastAttemptAt: ob.LastAttemptAt,
			Retries:       ob.Retries,
			Error:         ob.Error,
		})
	}
	// Snapshot outbound map for enrichment lookups (last attempt, retries, error).
	outboundByID := make(map[string]outboundSnap, len(outboundItems))
	for _, ob := range outboundItems {
		outboundByID[ob.ID] = ob
	}
	s.deliveryMu.RUnlock()

	// Build frames from snapshots — no lock held.
	ids := make([]string, 0)
	seen := make(map[string]struct{})
	items := make([]protocol.PendingMessageFrame, 0)
	for _, p := range pendingItems {
		if _, ok := seen[p.ID]; ok {
			continue
		}
		seen[p.ID] = struct{}{}
		ids = append(ids, p.ID)

		status := "queued"
		if p.Retries > 0 {
			status = "retrying"
		}
		lastAttempt := time.Time{}
		retries := p.Retries
		errStr := ""
		if ob, ok := outboundByID[p.ID]; ok {
			lastAttempt = ob.LastAttemptAt
			if ob.Retries > retries {
				retries = ob.Retries
			}
			errStr = ob.Error
		}
		items = append(items, protocol.PendingMessageFrame{
			ID:            p.ID,
			Recipient:     p.Recipient,
			Status:        status,
			QueuedAt:      formatTime(p.QueuedAt),
			LastAttemptAt: formatTime(lastAttempt),
			Retries:       retries,
			Error:         errStr,
		})
	}
	for _, ob := range outboundItems {
		if _, ok := seen[ob.ID]; ok {
			continue
		}
		seen[ob.ID] = struct{}{}
		ids = append(ids, ob.ID)
		items = append(items, protocol.PendingMessageFrame{
			ID:            ob.ID,
			Recipient:     ob.Recipient,
			Status:        ob.Status,
			QueuedAt:      formatTime(ob.QueuedAt),
			LastAttemptAt: formatTime(ob.LastAttemptAt),
			Retries:       ob.Retries,
			Error:         ob.Error,
		})
	}
	sort.Strings(ids)
	sort.Slice(items, func(i, j int) bool {
		if items[i].ID == items[j].ID {
			return items[i].Status < items[j].Status
		}
		return items[i].ID < items[j].ID
	})
	return protocol.Frame{Type: "pending_messages", Topic: topic, Count: len(ids), PendingIDs: ids, PendingMessages: items}
}

func (s *Service) importContactsFrame(contacts []protocol.ContactFrame) protocol.Frame {
	imported := 0
	for _, contact := range contacts {
		address := strings.TrimSpace(contact.Address)
		if address == "" || address == s.identity.Address {
			continue
		}

		before := s.trust.trustedContacts()
		_, existed := before[address]

		s.trustContact(address, contact.PubKey, contact.BoxKey, contact.BoxSig, "import_contacts")

		after := s.trust.trustedContacts()
		if _, ok := after[address]; ok && !existed {
			imported++
		}
	}

	return protocol.Frame{
		Type:  "contacts_imported",
		Count: imported,
	}
}

// registerHelloRoute installs a synthetic "node-route" subscriber that
// forwards pushes for the node's own address back through the inbound
// connection. The remote address string that seeds the subscriber id is
// resolved via s.Network().RemoteAddr(connID); an empty result means the
// ConnID is not registered, in which case the route would be unanchored
// and the function silently no-ops — same semantic as the previous
// nil-NetCore guard.
//
// Returns the installed (or pre-existing) node-route subscriber so the caller
// (handleAuthSession) can replay the inbox backlog to it at auth time.
// Returns nil when no route is installed (non-node client, empty address, or
// unregistered conn).
func (s *Service) registerHelloRoute(connID domain.ConnID, frame protocol.Frame) *subscriber {
	if strings.TrimSpace(frame.Client) != "node" {
		return nil
	}

	recipient := strings.TrimSpace(frame.Address)
	if recipient == "" {
		return nil
	}
	addr := s.Network().RemoteAddr(connID)
	if addr == "" {
		return nil
	}
	subID := "node-route:" + addr

	log.Trace().Str("site", "registerHelloRoute").Str("phase", "lock_wait").Str("recipient", recipient).Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "registerHelloRoute").Str("phase", "lock_held").Str("recipient", recipient).Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	defer func() {
		s.gossipMu.Unlock()
		log.Trace().Str("site", "registerHelloRoute").Str("phase", "lock_released").Str("recipient", recipient).Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	}()

	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}
	s.removeSubscriberConnIDLocked(recipient, connID)
	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}

	existing, exists := s.subs[recipient][subID]
	if exists && existing.connID == connID && connID != 0 {
		return existing
	}

	sub := &subscriber{
		id:        subID,
		recipient: recipient,
		connID:    connID,
	}
	s.subs[recipient][subID] = sub
	log.Debug().Str("recipient", recipient).Str("subscriber", subID).Int("active", len(s.subs[recipient])).Msg("route_via_hello")
	return sub
}

// removeSubscriberConnIDLocked removes every subscriber under the given
// recipient bucket whose connID matches the supplied value. A zero connID is
// treated as "no live connection" and never matches, so callers resolving an
// unregistered conn via connIDFor will not accidentally strip unrelated
// synthetic subscribers.
//
// Precondition: caller must hold gossipMu.Lock (subs is a gossipMu field).
func (s *Service) removeSubscriberConnIDLocked(recipient string, connID domain.ConnID) {
	if recipient == "" || connID == 0 {
		return
	}
	subs := s.subs[recipient]
	for id, sub := range subs {
		if sub != nil && sub.connID == connID {
			delete(subs, id)
		}
	}
	if len(subs) == 0 {
		delete(s.subs, recipient)
	}
}

func (s *Service) refreshKnowledgeFromPeers() {
	// When ConnectionManager is active it owns the outbound session
	// lifecycle — slots are filled continuously via the event loop.
	// Calling the legacy ensurePeerSessions here would bypass CM slot
	// accounting, retry/backoff and generation guards.
	if s.connManager != nil {
		return
	}

	s.peerMu.RLock()
	lastSync := s.lastSync
	s.peerMu.RUnlock()

	// Avoid dialing upstream peers on every UI poll while still making
	// contact discovery responsive for NAT/light clients.
	if !lastSync.IsZero() && time.Since(lastSync) < 3*time.Second {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	s.ensurePeerSessions(ctx)

	log.Trace().Str("site", "refreshKnowledgeFromPeers").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "refreshKnowledgeFromPeers").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.lastSync = time.Now().UTC()
	s.peerMu.Unlock()
	log.Trace().Str("site", "refreshKnowledgeFromPeers").Str("phase", "lock_released").Msg("peer_mu_writer")
}

func (s *Service) storeMessageFrame(frame protocol.Frame) protocol.Frame {
	// Type ↔ Topic invariant. The two Frame.Type values that funnel
	// through this handler imply different chatlog/event behaviour
	// inside storeIncomingMessage, and that decision is keyed off the
	// Topic field. A mismatched (Type, Topic) pair would let a caller
	// pick the "wrong" behaviour:
	//   - send_message + TopicControlDM  → outbound row would skip
	//     chatlog and the regular UI event, leaking control-DM
	//     storage semantics into a callsite that asked for a data DM.
	//   - send_control_message + non-control topic → outbound row
	//     WOULD enter chatlog and emit LocalChangeNewMessage, surfacing
	//     a "[delete]"-shaped row in the sender's chat thread — the
	//     exact failure mode docs/dm-commands.md is designed to avoid.
	// Reject the mismatch synchronously with ErrCodeInvalidSendMessage
	// so neither branch of storeIncomingMessage ever sees an
	// inconsistent frame.
	switch frame.Type {
	case "send_control_message":
		if frame.Topic != protocol.TopicControlDM {
			return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendMessage}
		}
	case "send_message":
		if frame.Topic == protocol.TopicControlDM {
			return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendMessage}
		}
	}

	msg, err := incomingMessageFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendMessage}
	}

	stored, count, errCode := s.storeIncomingMessage(msg, true)
	if errCode != "" {
		return protocol.Frame{Type: "error", Code: errCode}
	}
	if !stored {
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
	}
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
}

func (s *Service) importMessageFrame(frame protocol.Frame) protocol.Frame {
	msg, err := incomingMessageFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidImportMessage}
	}

	stored, count, errCode := s.storeIncomingMessage(msg, false)
	if errCode != "" {
		return protocol.Frame{Type: "error", Code: errCode}
	}
	if !stored {
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
	}
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
}

func (s *Service) storeDeliveryReceiptFrame(frame protocol.Frame) protocol.Frame {
	// seen_ack is wire-only scheduler plumbing: the local
	// send_delivery_receipt command accepts only the user-level statuses
	// (delivered/seen). Injecting seen_ack via local RPC would fake the
	// remote confirmation and silence the seen retry without the original
	// sender ever acking.
	if frame.Status == protocol.ReceiptStatusSeenAck {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendDeliveryReceipt}
	}
	receipt, err := receiptFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendDeliveryReceipt}
	}

	stored, count := s.storeDeliveryReceipt(receipt)
	if !stored {
		return protocol.Frame{Type: "receipt_known", Recipient: receipt.Recipient, Count: count, ID: string(receipt.MessageID)}
	}
	return protocol.Frame{Type: "receipt_stored", Recipient: receipt.Recipient, Count: count, ID: string(receipt.MessageID)}
}

// handleAckDeleteFrame validates an ack_delete frame against the authenticated
// peer's identity, applies the backlog deletion, and returns the reply.
// ConnID-first (PR 10.3b/G1): the caller resolved the id at dispatch; an
// unregistered id (0) drops through to auth_required because
// authenticatedAddressForConn returns ok=false.
func (s *Service) handleAckDeleteFrame(id domain.ConnID, frame protocol.Frame) (protocol.Frame, bool) {
	if id == 0 {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired}, false
	}
	hello, ok := s.authenticatedAddressForConn(id)
	if !ok {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired}, false
	}
	if strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Address) != strings.TrimSpace(hello.Address) || strings.TrimSpace(frame.ID) == "" {
		s.addBanScore(id, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAckDelete, Error: "invalid ack identity or id"}, false
	}
	if err := identity.VerifyPayload(hello.Address, hello.PubKey, ackDeletePayload(frame.Address, frame.AckType, frame.ID, frame.Status), frame.Signature); err != nil {
		s.addBanScore(id, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()}, false
	}

	count := 0
	switch strings.TrimSpace(frame.AckType) {
	case "dm":
		count = s.deleteBacklogMessageForRecipient(frame.Address, protocol.MessageID(frame.ID))
		// The acking peer confirmed it holds this id. For a TRANSIT DM
		// (recipient is a third party) deleteBacklog removes nothing, so
		// record the acker to stop the gossip fan-out re-sending the id to a
		// peer that already has it (Phase 2 — relay_delivered.go). Gate on an
		// ACTIVE relayRetry entry, NOT on count==0: count is also 0 when the
		// id is simply absent, so an authenticated peer could otherwise spam
		// ack_delete for phantom ids and fill relayDeliveredTo to its hard cap,
		// starving real suppression. A relayRetry entry exists only for transit
		// DMs we are actually relaying (recipient-delete clears it, phantom ids
		// never had one). frame.Address is verified == hello.Address above.
		if s.hasRelayRetryEntry(protocol.MessageID(frame.ID)) {
			s.recordRelayDeliveredTo(protocol.MessageID(frame.ID), domain.PeerIdentityFromWire(frame.Address))
		}
	case "receipt":
		count = s.deleteBacklogReceiptForRecipient(frame.Address, protocol.MessageID(frame.ID), frame.Status)
	default:
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAckDelete, Error: "unknown ack type"}, false
	}
	log.Info().Str("node", s.identity.Address).Str("address", frame.Address).Str("type", frame.AckType).Str("id", frame.ID).Str("status", frame.Status).Int("removed", count).Msg("ack_delete_applied")
	return protocol.Frame{Type: "ack_deleted", AckType: frame.AckType, ID: frame.ID, Count: count, Status: "ok"}, true
}

func (s *Service) storeIncomingMessage(msg incomingMessage, validateTimestamp bool) (bool, int, string) {
	// Relay-only DM opt-out enforcement (cfg.DisableDirectMessages).
	// Runs FIRST — before timestamp validation and signature
	// verification — so spam addressed to a node that opted out of DMs
	// costs no crypto work, no lock traffic,
	// and no memory: the message never enters s.topics/s.seen/s.known
	// and no delivery receipt is emitted (the sender must not believe
	// anyone will read it). The (false, 0, "") return deliberately
	// mirrors the duplicate outcome: shouldAckOnStoreResult acks the
	// previous hop (push path) and deliverRelayedMessage reports
	// success for the hop-ack (relay path), so upstream hop-level
	// retries stop instead of looping — the same reconnect-storm
	// amplifier dedup protects against. The sender's own end-to-end
	// retry simply expires by TTL. Clients whose node never obtained
	// this node's box key cannot even compose the DM (the key is not
	// redistributed via fetch_contacts — see Service.selfBoxKey); those
	// that cached it from a direct handshake land here and are dropped.
	if s.dropsInboundDM(msg) {
		log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("topic", msg.Topic).Str("sender", msg.Sender).Msg("inbound_dm_dropped_no_dm_inbox")
		return false, 0, ""
	}

	// Absolute age ceiling on TRANSIT DMs (envelope_retention.go). A transit
	// envelope older than the transit MaxAge — anchored on the IMMUTABLE
	// sender CreatedAt, so a re-injection that would reset the local StoredAt
	// cannot revive it — is neither stored nor propagated: relays are
	// forwarding-only, not a mailbox. Returning the duplicate-style outcome
	// (false, 0, "") acks the previous hop so its retries stop, exactly like
	// dropsInboundDM. Only transit is refused here; local/broadcast envelopes
	// are retained for local history/subscribers and aged out by cleanup.
	if s.transitAgedOnAdmission(msg, time.Now().UTC()) {
		log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("topic", msg.Topic).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Time("created_at", msg.CreatedAt).Msg("transit_dm_dropped_age_ceiling")
		return false, 0, ""
	}

	if validateTimestamp {
		if err := s.validateMessageTiming(msg); err != nil {
			return false, 0, protocol.ErrCodeMessageTimestampOutOfRange
		}
	}

	// Control DMs (topic == TopicControlDM) share the DM envelope shape
	// and must satisfy the same signature/boxkey-binding contract as
	// regular DMs. See docs/dm-commands.md.
	if msg.Topic == "dm" || msg.Topic == protocol.TopicControlDM {
		s.knowledgeMu.RLock()
		senderPubKey := s.pubKeys[msg.Sender]
		senderBoxKey := s.boxKeys[msg.Sender]
		senderBoxSig := s.boxSigs[msg.Sender]
		s.knowledgeMu.RUnlock()
		// First-contact path: the sender is unknown locally, but the
		// transport frame carried the sender's self-certifying PUBLIC
		// key triple. USE the attached signing key for verification
		// WITHOUT importing anything yet — no state (knowledge maps,
		// bounded known set, IdentityAdded events) may change before
		// the envelope signature proves the message is genuine,
		// otherwise a flood of valid-fingerprint-but-forged-envelope
		// frames would churn the shared LRU and evict real cached
		// contacts. Import happens after VerifyEnvelope below.
		// Steady-state cost is zero: this branch runs only when the
		// pubkey is missing.
		if senderPubKey == "" && msg.SenderPubKey != "" {
			if err := validateAttachedSenderPubKey(msg); err != nil {
				log.Warn().Err(err).Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("sender", msg.Sender).Msg("attached_sender_key_rejected")
			} else {
				senderPubKey = msg.SenderPubKey
				// The attached box pair participates in verification
				// only when its binding verifies (VerifyBoxKeyBinding
				// also enforces the 32-byte X25519 size — a signed
				// oversized blob is structurally invalid). A broken
				// half degrades to signing-key-only.
				senderBoxKey, senderBoxSig = "", ""
				if msg.SenderBoxKey != "" && msg.SenderBoxSig != "" &&
					identity.VerifyBoxKeyBinding(msg.Sender, msg.SenderPubKey, msg.SenderBoxKey, msg.SenderBoxSig) == nil {
					senderBoxKey, senderBoxSig = msg.SenderBoxKey, msg.SenderBoxSig
				}
			}
		}
		if senderPubKey == "" {
			log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Msg("storeIncomingMessage: unknown sender key")
			return false, 0, protocol.ErrCodeUnknownSenderKey
		}
		// Boxkey binding at ingest as required by encryption.md, with a
		// self-healing twist: a CACHED pair that fails the binding (or
		// is half-cached — key without signature) is superseded by a
		// binding-valid attached pair from the frame instead of wedging
		// the sender behind ErrCodeInvalidDirectMessageSig forever —
		// the frame's pair is authenticated by the same signing key, so
		// it is at least as trustworthy as the cache it replaces
		// (importVerifiedSenderKeys persists the replacement below).
		// Only when a complete cached pair fails AND no valid
		// replacement is attached does the original reject stand.
		if senderBoxKey != "" && senderBoxSig != "" {
			if err := identity.VerifyBoxKeyBinding(msg.Sender, senderPubKey, senderBoxKey, senderBoxSig); err != nil {
				if !attachedBoxPairValid(msg, senderPubKey) {
					return false, 0, protocol.ErrCodeInvalidDirectMessageSig
				}
				log.Warn().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("sender", msg.Sender).Msg("cached_boxkey_binding_invalid_superseded_by_attached")
			}
		}
		if err := directmsg.VerifyEnvelope(msg.Sender, senderPubKey, msg.Recipient, msg.Body); err != nil {
			return false, 0, protocol.ErrCodeInvalidDirectMessageSig
		}
		// The envelope signature verified — NOW the attached material
		// is proven to belong to a sender that authored a genuine
		// message, so persist whatever the knowledge maps are missing
		// or hold in a broken state (signing key for a first contact;
		// box pair for a contact whose pubkey was known but whose box
		// pair never arrived, arrived half, or fails its binding — the
		// recipient could otherwise read but never reply, and the
		// fallback sync no longer triggers once the pubkey exists).
		s.importVerifiedSenderKeys(msg)
	}

	s.cleanupExpiredMessages()

	// Knowledge-domain section: register sender/recipient into s.known.
	// Taken FIRST because canonical lock order is knowledgeMu → gossipMu,
	// and the s.gossipMu section below covers the gossip domain (seen/topics).
	// DM senders are cryptographically verified above (VerifyEnvelope),
	// so they are safe to register as known identities unconditionally.
	// Non-DM senders are only registered when the node already holds
	// their public key — this prevents an attacker from injecting
	// arbitrary strings into the known-identities set via forged
	// sender fields on non-DM messages.
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_wait").Str("msg_id", string(msg.ID)).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_held").Str("msg_id", string(msg.ID)).Msg("knowledgeMu_writer")
	if msg.Topic == "dm" || msg.Topic == protocol.TopicControlDM {
		// DM-class senders are cryptographically verified above
		// (VerifyEnvelope) — register them in s.known regardless of
		// pre-existing pubkey snapshot, identical to the data-DM path.
		s.known.Add(msg.Sender)
	} else if _, hasPK := s.pubKeys[msg.Sender]; hasPK {
		s.known.Add(msg.Sender)
	}
	if msg.Recipient != "*" {
		s.known.Add(msg.Recipient)
	}
	s.knowledgeMu.Unlock()
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_released").Str("msg_id", string(msg.ID)).Msg("knowledgeMu_writer")

	// Gossip-domain section: dedup via s.seen and append to s.topics.
	// Separate lock window from the knowledge section above — the two are
	// sequential, not nested, per the canonical knowledgeMu → gossipMu
	// order.  Splitting is semantically safe because the known writes are
	// idempotent: a concurrent writer cannot race with us to produce a
	// different outcome, and the dedup decision below depends only on
	// s.seen, not on s.known.
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_wait").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_held").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
	if s.seen.Has(string(msg.ID)) {
		count := len(s.topics[msg.Topic])
		// A duplicate of a DM addressed to us means the sender most likely
		// never received the original delivered receipt and is retrying
		// end-to-end — re-send the receipt or the retries never stop. The
		// dedupe stays silent towards the UI (no message.new below).
		//
		// Bloom positives can be FALSE positives (rotating filter, see
		// bloom_dedup.go), so the receipt is only re-sent when the envelope
		// is genuinely present in the local backlog: confirming delivery of
		// a message we never stored would convert a bloom FP from "lost,
		// sender keeps retrying" into a silent false "delivered".
		confirmedPresent := false
		if s.owesDeliveryReceipt(msg) {
			for i := range s.topics[msg.Topic] {
				if s.topics[msg.Topic][i].ID == msg.ID {
					confirmedPresent = true
					break
				}
			}
		}
		// Recipient-local DM that is NOT visible in the runtime backlog
		// (e.g. cleared by a same-identity subscriber's ack_delete) while a
		// durable MessageStore is registered: fall through and let the
		// chatlog primary key decide — StoreDuplicate re-sends the receipt
		// below; StoreInserted means the bloom hit was a FALSE positive and
		// the message is genuinely new (stored properly instead of lost).
		fallThroughToStore := s.owesDeliveryReceipt(msg) && !confirmedPresent && s.messageStore != nil
		if !fallThroughToStore {
			s.gossipMu.Unlock()
			log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_released_dedup").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
			log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("topic", msg.Topic).Int("topic_count", count).Msg("store_incoming_message_dedup")
			if confirmedPresent {
				s.goBackground(func() { s.resendDeliveryReceipt(msg) })
			}
			return false, count, ""
		}
	}
	// The dedup mark is NOT set here. It is set after the store has
	// answered, because one answer — StoreDeferred — means "ask me again":
	// the message is not persisted, and a mark set now would make the
	// sender's next attempt look like a duplicate and be dropped in
	// silence. Between the check above and the mark below the exact-ID
	// admission scan and the chatlog primary key still catch a genuine
	// concurrent duplicate, so nothing is lost by waiting.

	// Hop budget at admission (transit_retention.go): a frame without
	// the Hops field — legacy peer or local send — is treated as if
	// THIS node originated it (full default budget). Network arrivals
	// spend one hop: the stored value is what WE may stamp on outbound
	// gossip; < 1 means store/deliver only, never re-gossip.
	hopBudget := effectiveHopBudget(msg.Hops)
	if msg.Via != "" {
		hopBudget--
	}

	envelope := protocol.Envelope{
		ID:          msg.ID,
		Topic:       msg.Topic,
		Sender:      msg.Sender,
		Recipient:   msg.Recipient,
		Flag:        msg.Flag,
		TTLSeconds:  msg.TTLSeconds,
		Payload:     []byte(msg.Body),
		CreatedAt:   msg.CreatedAt,
		Hops:        hopBudget,
		Via:         string(msg.Via),
		ViaIdentity: msg.ViaIdentity.String(),
		StoredAt:    time.Now().UTC(),
	}

	// Only messages that belong to this node (sender or recipient) get
	// persisted to chatlog, emit UI events, and push to local subscribers.
	// Transit messages (relayed DMs where neither party is us) are held only
	// in memory (s.topics + relayRetry) for gossip/relay — they are NOT
	// persisted to disk (queue-state persistence was removed; they do not
	// survive a restart) and must NOT pollute the local chat history or wake
	// up the desktop UI.
	isLocal := s.isLocalMessage(msg)

	// Persist message via the registered MessageStore (owned by the desktop
	// layer) BEFORE adding to s.topics. The store result determines whether
	// the message enters in-memory state:
	//
	//   StoreInserted  → add to s.topics, emit event, gossip
	//   StoreDuplicate → skip s.topics (already in chatlog on disk),
	//                     skip event (no beep/unread). This closes both
	//                     the event-path and the DMHeaders header-path
	//                     that could otherwise re-trigger unread counts
	//                     via repairUnreadFromHeaders after a restart.
	//   StoreFailed    → add to s.topics (don't lose the message from
	//                     the network), skip event (stale data).
	//   StoreDeferred  → return immediately, before any routing: no dedup
	//                     mark, no s.topics, no push/gossip/relay, no
	//                     sender-side retry, no event, no receipt, and the
	//                     frame is not acked. The store could not decide,
	//                     so the message is left with the sender.
	//
	// When no store is registered (relay-only node) or for transit messages,
	// the message always enters s.topics.
	storeResult := StoreInserted
	// Control DMs (TopicControlDM) bypass chatlog persistence by contract
	// — see docs/dm-commands.md "Storage rules for control DMs". They
	// reach the application via LocalChangeNewControlMessage on the
	// dedicated ebus.TopicMessageControl below; the chat thread never
	// learns of them.
	if isLocal && s.messageStore != nil && msg.Topic != protocol.TopicControlDM {
		isOutgoing := msg.Sender == s.identity.Address
		// Unlock before calling into the store — it may do SQLite I/O.
		s.gossipMu.Unlock()
		log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_released_mid").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
		storeResult = s.messageStore.StoreMessage(envelope, isOutgoing)
		log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_wait_reacquire").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
		s.gossipMu.Lock()
		log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_held_reacquire").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
	}

	// Duplicate local messages must NOT enter s.topics. The message is
	// already persisted in chatlog, and adding it here would cause
	// fetchDMHeadersFrame() to include it in DMHeaders, which lets
	// repairUnreadFromHeaders() re-increment unread counts on the UI.
	//
	// Control DMs (TopicControlDM) ALSO never enter s.topics: their
	// retry/persistence story lives at the application layer
	// (DMRouter delete intents), not at the node level.
	// Putting them in topics["dm-control"] would (a) be unread by any
	// retry path — retryableRelayMessages reads only topics["dm"] — so the
	// envelopes accumulate forever, and (b) blur the design boundary
	// between data DMs (node-level forwarding/retry) and control DMs
	// (sender-driven application retry). Routing/push-fanout still
	// works because executeGossipTargets and the table-directed relay
	// path send frames on the wire on the fly without depending on
	// s.topics as a backing store.
	if storeResult == StoreDeferred {
		// Out BEFORE any routing. The store could not decide whether this
		// node may keep the message, so this node has not received it:
		// nothing is marked seen, nothing enters the backlog, nothing is
		// pushed, gossiped or relayed, and no sender-side retry is
		// registered. Returning further down would leave the peer holding
		// a message whose local RPC reported an error and whose row is on
		// no disk anywhere.
		count := len(s.topics[msg.Topic])
		s.gossipMu.Unlock()
		log.Info().
			Str("id", string(msg.ID)).
			Str("topic", msg.Topic).
			Msg("store_incoming_message_deferred")
		// Reported as a REFUSAL, not as an arrival: the error code is what
		// keeps shouldAckOnStoreResult from acking the frame, so the
		// previous hop re-attempts and the original sender keeps the
		// message until this node can answer for it.
		return false, count, protocol.ErrCodeStoreDeferred
	}
	s.seen.Add(string(msg.ID))

	beforeCount := len(s.topics[msg.Topic])
	// evictedIDs collects messages displaced by the transit caps; their
	// relayRetry entries are dropped AFTER gossipMu is released (the
	// canonical deliveryMu → gossipMu order forbids nesting here).
	var evictedIDs []protocol.MessageID
	// Forward-once (Phase 3, CORSA_TRANSIT_FORWARD_ONCE): a transit DM is
	// forwarded on the wire below but NEVER stored in s.topics — relays are
	// pure forwarders, not a buffer. This removes the re-gossip storm at its
	// source (nothing stored ⇒ retryRelayDeliveries has nothing to re-emit).
	// Durability is the sender's (HoldDMUntilReachable + delivery_retry). The
	// bloom (s.seen) still dedups in-window; no backlog-dedup is needed because
	// there is no backlog. trackRelayMessage is skipped below for the same id.
	forwardOnceTransit := s.cfg.TransitForwardOnce && s.isTransitEnvelope(envelope)
	if storeResult != StoreDuplicate && msg.Topic != protocol.TopicControlDM && !forwardOnceTransit {
		// Single admission pass over the backlog (transit_retention.go):
		// exact-ID dedup plus transit accounting. The rotating bloom
		// (s.seen) forgets IDs after 5–10 min while the backlog lives
		// beyond the bloom window (transit for transitInFlightWindow,
		// local messages indefinitely), so late re-injections (pending-
		// ring drains, reconnect backlog replays) would otherwise be
		// re-admitted as duplicate envelopes — previously only spotted
		// by the duplicate *diagnostics* below, never prevented.
		scan := scanTopicForAdmission(s.topics[msg.Topic], msg.ID, msg.Recipient, s.isTransitEnvelope)
		if scan.duplicate {
			count := len(s.topics[msg.Topic])
			s.gossipMu.Unlock()
			log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_released_exact_dedup").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")
			log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("topic", msg.Topic).Int("topic_count", count).Msg("store_incoming_message_backlog_dedup")
			// Same contract as the bloom-dedup exit above: a retried DM
			// addressed to us still owes the sender a delivered receipt.
			if s.owesDeliveryReceipt(msg) {
				s.goBackground(func() { s.resendDeliveryReceipt(msg) })
			}
			return false, count, ""
		}
		// Transit retention (layer 1): enforce the per-recipient FIFO
		// cap and the global byte budget before appending.
		if s.isTransitEnvelope(envelope) {
			s.topics[msg.Topic], evictedIDs = evictTransitOverflowLocked(
				s.topics[msg.Topic], scan, msg.Recipient, len(envelope.Payload), s.isTransitEnvelope)
		}
		s.topics[msg.Topic] = append(s.topics[msg.Topic], envelope)
	}
	count := len(s.topics[msg.Topic])
	s.gossipMu.Unlock()
	log.Trace().Str("site", "storeIncomingMessage").Str("phase", "lock_released").Str("msg_id", string(msg.ID)).Msg("gossipMu_writer")

	// Orphan prevention: retryableRelayMessages iterates the TOPIC
	// snapshot, so relayRetry entries of evicted envelopes would never
	// be visited (and never lazily deleted) again.
	s.dropRelayRetryEntries(evictedIDs)
	s.forgetRelayDelivered(evictedIDs)
	if len(evictedIDs) > 0 {
		log.Debug().Str("node", s.identity.Address).Int("evicted", len(evictedIDs)).Str("recipient", msg.Recipient).Msg("transit_backlog_cap_eviction")
	}

	// Only notify the desktop UI for messages this node participates in.
	// Transit relay traffic must not wake up the UI.
	// Emit event only for genuinely new messages (StoreInserted).
	if isLocal && storeResult == StoreInserted {
		event := protocol.LocalChangeEvent{
			Topic:      msg.Topic,
			MessageID:  string(msg.ID),
			Sender:     msg.Sender,
			Recipient:  msg.Recipient,
			Body:       msg.Body,
			Flag:       string(msg.Flag),
			CreatedAt:  msg.CreatedAt.Format(time.RFC3339Nano),
			TTLSeconds: msg.TTLSeconds,
		}
		if msg.Topic == protocol.TopicControlDM {
			// Control DMs publish onto the dedicated ebus topic only
			// for the FINAL RECIPIENT — the side that will execute the
			// inner command (delete a message, retire a delete intent on
			// ack, etc.). The sender side has already done all the
			// local bookkeeping it needs synchronously inside
			// SendControlMessage / DeleteDM and must NOT receive its
			// own control DM as if it were inbound: DMRouter would
			// otherwise try to dispatch handleInboundMessageDelete
			// against its own outgoing message_delete and corrupt
			// state. The wire-level ack arriving from the recipient is
			// a separate inbound event that follows this same recipient
			// gate naturally.
			//
			// Note: sender-side push subscribers (other UI clients on
			// the same node listening to the chatlog gateway) are NOT
			// served by this branch by design — control DMs do not
			// surface in any chat thread, and the gateway has its own
			// fanout mechanism for chatlog updates that we are
			// deliberately bypassing.
			//
			// Execution is intentionally NOT age-gated. The retention
			// ceiling (envelope_retention.go) is PROPAGATION-only — it
			// bounds re-gossip/forwarding of zombies, mirroring bitchat /
			// SimpleX where TTL bounds the queue, not delivered-command
			// execution. The command here is signature-verified
			// (VerifyEnvelope above), so honouring it regardless of the
			// (unsigned, tamperable) outer CreatedAt is required for correct
			// offline delivery: a recipient who was offline longer than the
			// ceiling must still apply an authentic delete/edit on reconnect.
			if msg.Recipient == s.identity.Address {
				event.Type = protocol.LocalChangeNewControlMessage
				s.eventBus.Publish(ebus.TopicMessageControl, event)
			}
		} else {
			event.Type = protocol.LocalChangeNewMessage
			s.emitLocalChange(event)
			s.eventBus.Publish(ebus.TopicMessageNew, event)
		}
	}

	_, callerFile, callerLine, _ := runtime.Caller(1)
	log.Info().Str("node", s.identity.Address).Str("topic", msg.Topic).Str("id", string(msg.ID)).Str("from", msg.Sender).Str("to", msg.Recipient).Str("flag", string(msg.Flag)).Int("before_count", beforeCount).Int("topic_count", count).Bool("is_local", isLocal).Int("hops_budget", envelope.Hops).Str("caller", fmt.Sprintf("%s:%d", callerFile, callerLine)).Msg("stored message")

	// Push and gossip are independent delivery mechanisms:
	//
	// Push  — instant delivery to locally connected subscribers.
	//         An optimization; not a guarantee.  The subscriber may
	//         disconnect before the write completes.
	//
	// Gossip — mesh-wide propagation to peer nodes.  Ensures every
	//          relay in the network stores a copy so the recipient can
	//          retrieve the message from any node it reconnects to.
	//
	// Both happen unconditionally when applicable.  Push without gossip
	// would strand the message on this relay if the subscriber
	// reconnects to a different node.  Gossip without push would add
	// up to relayRetryTTL latency for a locally connected recipient.
	decision := s.router.Route(envelope)

	if len(decision.PushSubscribers) > 0 {
		// Tracked: writePushFrame itself is network-only, but the
		// spawned fan-out must complete before TempDir cleanup so
		// subscriber state is fully torn down before tests assert
		// on side effects.
		s.goBackground(func() { s.pushToSubscriberSnapshot(envelope, decision.PushSubscribers) })
	}

	// An origin-authored message that ARRIVED from the network (Via != "")
	// is an echo of our own DM coming back through the mesh. Re-propagating
	// it would re-inject it with a FRESH hop budget and revive a message the
	// sender-owned engine may already have abandoned — the months-long
	// zombie-DM storm (traffic showed our own months-old DMs re-emitted by
	// THIS path with hops reset to the default). Re-propagation of our own
	// messages is owned EXCLUSIVELY by the sender-owned retry engine
	// (delivery_retry.go), which is finite (TTL / attempts cap). The message
	// is still stored for the local DM view above, and the inbound-push
	// handler still ack-deletes the pushing hop (which actively helps the
	// mesh copies die); we simply never re-gossip or relay it.
	originEcho := msg.Via != "" && s.identity != nil && msg.Sender == s.identity.Address
	// Reachability gate for our OWN first send (sender == self, local origin,
	// Via == ""): emit only when the recipient is reachable — a directed
	// route or a directly connected subscriber. An unreachable recipient is
	// HELD (registered in awaitingDelivered below) instead of blind-gossiped
	// into the void; the sender-owned retry engine delivers it when a route
	// appears, bounded by TTL / attempts cap. This is the origin-send half of
	// the INV-3 reachability gate (the retry half is dispatchEnvelopeRetry).
	// TRANSIT forwarding (sender != self) is NOT gated — blind gossip is how
	// relays propagate other people's messages (INV-3 unchanged for transit).
	originFirstSend := msg.Via == "" && s.identity != nil && msg.Sender == s.identity.Address
	originUnreachableHold := s.cfg.HoldDMUntilReachable && originFirstSend && decision.RelayNextHop == nil && len(decision.PushSubscribers) == 0
	if originUnreachableHold {
		log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Msg("origin_send_held_unreachable")
	}
	// Re-propagation age gate (envelope_retention.go): a broadcast or control
	// DM past its class MaxAge must not be gossiped even once. Transit is
	// already refused at admission; control DMs never enter s.topics so
	// cleanup cannot age them — this emit gate is their only bound. Anchored
	// on the immutable CreatedAt; MaxAge=0 classes (local) are never gated.
	propagationAged := s.envelopePropagationAged(msg.Topic, msg.Sender, msg.Recipient, msg.CreatedAt, time.Now().UTC())
	if s.shouldRouteStoredMessage(msg) && !originEcho && !originUnreachableHold && !propagationAged {
		// Forward-once: do NOT register a relay-retry entry for transit — the
		// frame is forwarded below in a single pass and never re-gossiped over
		// time (it was also not stored above). Local-origin sends still track
		// via the sender-owned engine, not this transit contour.
		if !forwardOnceTransit {
			s.trackRelayMessage(envelope)
		}

		// Gossip is the baseline delivery mechanism. It spreads the
		// envelope so relays can forward it (transit DMs are held IN
		// MEMORY only as the in-flight buffer of the forwarding
		// operation — bounded by the transit retention caps, never a
		// mailbox), plus push delivery to connected clients and the
		// sender-side backlog. Propagation gates live in
		// gossipTargetsForRelay: the table next-hop is dropped (it gets the
		// directed relay_message below — no duplicate push), then the hop
		// budget, ingress suppression, ack_delete suppression and the K-of-N
		// cap are applied. Runs inline: executeGossipTargets only enqueues
		// jobs on the bounded gossip dispatch pool (gossip_dispatch.go).
		gossipTargets := s.gossipTargetsForRelay(envelope, decision)
		s.executeGossipTargets(envelope, gossipTargets)

		// Table-directed relay (Phase 1.2): when the routing table knows a
		// next-hop for this recipient, send relay_message directly to that
		// peer. This is the primary directed delivery path that replaces
		// blind gossip relay for known routes. Gossip still runs above as
		// fallback — receivers dedupe via seen[messageID].
		if (msg.Topic == "dm" || msg.Topic == protocol.TopicControlDM) && msg.Recipient != "" && msg.Recipient != "*" {
			if decision.RelayNextHop != nil {
				s.sendTableDirectedRelay(s.runCtx, envelope, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops)
			} else {
				// No table route — fall back to blind gossip relay to
				// capable full nodes (pre-Phase 1.2 behavior). Same
				// hop/ingress gates as the gossip fan-out above: this
				// path is blind propagation too.
				s.tryRelayToCapableFullNodes(envelope, gossipTargets)
			}
		}
	}

	// Delivery receipts are emitted when this node IS the final recipient,
	// regardless of how the message arrived (gossip, relay, or local).
	// Receipt emission must not be gated by validateTimestamp — relayed
	// DMs arrive via deliverRelayedMessage which passes validateTimestamp=false,
	// but the recipient still needs to acknowledge delivery to the sender.
	// Sender-side e2e retry: our own outgoing DM stays scheduled until the
	// recipient's delivered/seen receipt arrives (delivery_retry.go). No
	// other domain mutex is held here, so taking deliveryMu is safe.
	if msg.Topic == "dm" && msg.Sender == s.identity.Address &&
		msg.Recipient != "" && msg.Recipient != "*" && msg.Recipient != s.identity.Address &&
		storeResult != StoreDuplicate {
		s.deliveryMu.Lock()
		// held = the first send was withheld because the recipient was
		// unreachable (reachability gate). Only held entries are woken by
		// kickDeliveryRetriesForReachable when a route/connection appears.
		s.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), originUnreachableHold)
		s.deliveryMu.Unlock()
		// A withheld send is the one case worth writing down: it is the
		// only proof that survives a restart that the peer cannot have
		// this message. Written AFTER the register and outside the
		// mutex — losing it to a crash here reads as "may have gone out",
		// which is the harmless direction.
		if originUnreachableHold {
			s.syncEmissionMarks([]protocol.MessageID{envelope.ID})
		}
	}

	// A chatlog duplicate (StoreDuplicate) is a sender retry whose original
	// receipt most likely got lost — storeDeliveryReceipt would dedupe the
	// whole emit into a no-op via seenReceipts, so the duplicate path goes
	// through resendDeliveryReceipt, which redistributes without touching
	// local receipt state.
	if s.owesDeliveryReceipt(msg) {
		if storeResult == StoreDuplicate {
			s.goBackground(func() { s.resendDeliveryReceipt(msg) })
		} else {
			s.goBackground(func() { s.emitDeliveryReceipt(msg) })
		}
	}

	return true, count, ""
}

// owesDeliveryReceipt reports whether msg is a data DM addressed to this
// node from another identity — exactly the messages whose arrival (first or
// retried) obliges this node to confirm delivery to the remote sender.
func (s *Service) owesDeliveryReceipt(msg incomingMessage) bool {
	return msg.Topic == "dm" &&
		msg.Recipient != "*" &&
		msg.Recipient == s.identity.Address &&
		msg.Sender != s.identity.Address
}

// dropsInboundDM reports whether msg must be rejected because this node
// opted out of receiving direct messages (cfg.DisableDirectMessages, the
// headless relay-only default). The gate covers exactly the DM-class
// messages whose FINAL RECIPIENT is this node and whose author is someone
// else:
//
//   - data DMs (topic "dm") — nobody reads them here, storing them would
//     grow s.topics unboundedly (local messages are exempt from transit
//     retention caps) and emitting a delivered receipt would lie to the
//     sender;
//   - control DMs (TopicControlDM) — they mutate chat state this node
//     does not keep.
//
// Everything else stays untouched: broadcasts and global topics
// (recipient "*"/empty), transit DMs this node merely relays between two
// other parties, and this node's own outgoing messages echoed back
// (sender == self).
func (s *Service) dropsInboundDM(msg incomingMessage) bool {
	if !s.cfg.DisableDirectMessages {
		return false
	}
	if !protocol.IsDMTopic(msg.Topic) {
		return false
	}
	return msg.Recipient == s.identity.Address && msg.Sender != s.identity.Address
}

// isLocalMessage returns true if this node is a party to the message
// (sender or recipient), meaning the message should be persisted locally.
// Broadcast messages (recipient="*") and global topics are always local.
// Transit DMs — where this node is merely relaying between two other parties —
// return false; they are held in memory only (s.topics + relayRetry) for
// gossip/relay and are NOT persisted to disk (no restart survival).
//
// Control DMs (TopicControlDM) follow the same point-to-point semantics
// as data DMs: only the sender or the recipient should treat them as
// "local" for ebus-event purposes. Transit nodes carry them through
// gossip/relay without emitting LocalChange events.
func (s *Service) isLocalMessage(msg incomingMessage) bool {
	if msg.Topic != "dm" && msg.Topic != protocol.TopicControlDM {
		return true // global/broadcast messages are always local
	}
	if msg.Recipient == "*" || msg.Recipient == "" {
		return true
	}
	return msg.Sender == s.identity.Address || msg.Recipient == s.identity.Address
}

func (s *Service) shouldRouteStoredMessage(msg incomingMessage) bool {
	// Control DMs (TopicControlDM) are point-to-point on the wire and
	// reuse the same routing primitives as data DMs. The condition tree
	// below treats both topics identically — recipient gating, forward
	// capability, and origin checks all apply.
	isDMClass := msg.Topic == "dm" || msg.Topic == protocol.TopicControlDM
	if isDMClass && msg.Recipient == s.identity.Address {
		return false
	}
	if s.CanForward() {
		return true
	}
	if !isDMClass {
		return false
	}
	if msg.Sender != s.identity.Address {
		return false
	}
	return msg.Recipient != "" && msg.Recipient != "*"
}

// maxSentDMIDs bounds the sentDMIDs LRU. Sized above any realistic
// outstanding-DM count so a legitimate receipt is never gated out; only a
// node that has sent more than this many DMs without the corresponding
// (long-since-evicted) receipts is affected, and there the fallback is merely
// the receipt being treated as unsolicited.
const maxSentDMIDs = 50_000

// maxReceiptBacklogPerRecipient bounds the in-memory delivery-receipt backlog
// for a SINGLE recipient, evicting the oldest past the bound. It applies to
// every recipient so an authenticated peer cannot stream unique receipts for
// one active-subscriber recipient (or our own identity) and grow that list
// without limit before an ack_delete/disconnect drains it.
//
// Eviction is safe for both backlog kinds:
//   - own identity (receipts for messages we sent): the durable status is in
//     the chatlog (MessageStore.UpdateDeliveryStatus); the in-memory copy only
//     adds the precise DeliveredAt, and the desktop already falls back to the
//     chatlog status with a synthesized timestamp when the map is empty (e.g.
//     after a restart), so eviction costs only timestamp precision on old
//     messages, never the delivered/seen state itself.
//   - transit/subscriber backlog: receipts are best-effort relay artefacts;
//     the end-to-end sender re-sends them on its own retry schedule, so an
//     evicted old receipt is recovered by a later retransmission.
//
// NOTE: this caps each recipient's list, not the number of distinct recipient
// keys in s.receipts — bounding the active-subscriber key set is tracked
// separately (transit-backlog follow-up).
const maxReceiptBacklogPerRecipient = 4096

// storeDeliveryReceipt persists a receipt addressed to this node, dedupes
// against seenReceipts, clears the corresponding outbound/pending/relayRetry
// bookkeeping and recomputes the aggregate status.
//
// Cross-domain: writes s.seenReceipts/receipts/outbound/pending/relayRetry
// (deliveryMu) and s.aggregateStatus (statusMu), and reads s.subs (gossipMu)
// for the admission check below. Canonical order:
// peerMu → deliveryMu → gossipMu → statusMu — the gossipMu admission read is a
// brief nested RLock inside the deliveryMu section (deliveryMu OUTER → gossipMu
// INNER), released before the statusMu window; see the offline-recipient early
// return.
func (s *Service) storeDeliveryReceipt(receipt protocol.DeliveryReceipt) (bool, int) {
	key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status

	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_wait").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_held").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_wait").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_held").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	if s.seenReceipts.Has(key) {
		count := len(s.receipts[receipt.Recipient])
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_dup").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_dup").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
		// A duplicate "seen" addressed to us means the seen-sender most
		// likely never received our seen_ack and is retrying — answer it
		// again, mirroring the duplicate-DM → delivered re-send contract.
		if receipt.Status == protocol.ReceiptStatusSeen && receipt.Recipient == s.identity.Address {
			s.goBackground(func() { s.sendSeenAck(receipt) })
		}
		return false, count
	}
	// Sender binding for seen_ack addressed to us: the only identity whose
	// ack counts is the one our seen receipt was addressed to — recorded in
	// awaitingSeenAck. Anything else (wrong sender, or no retry pending) is
	// dropped BEFORE the seenReceipts insert, so a spoofed ack can neither
	// stop the retry nor occupy the dedupe key and shadow the genuine ack.
	if receipt.Status == protocol.ReceiptStatusSeenAck && receipt.Recipient == s.identity.Address {
		entry, awaiting := s.awaitingSeenAck[receipt.MessageID]
		if !awaiting || entry.Receipt.Recipient != receipt.Sender {
			count := len(s.receipts[receipt.Recipient])
			s.deliveryMu.Unlock()
			log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_ack_rejected").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
			s.peerMu.Unlock()
			log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_ack_rejected").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
			log.Warn().Str("message_id", string(receipt.MessageID)).Str("sender", receipt.Sender).Bool("awaiting", awaiting).Msg("seen_ack rejected: sender does not match the awaited original sender")
			return false, count
		}
	}
	// Unsolicited-receipt gate: a delivered/seen receipt addressed to our own
	// identity is only meaningful for a message WE actually sent. Without this
	// gate an authenticated peer could stream receipts for phantom message IDs
	// and grow s.receipts without bound (each unique key bypasses the
	// seenReceipts dedup). The message is "ours" if it is still awaiting a
	// receipt (awaitingDelivered) OR was recorded in the sentDMIDs LRU when we
	// originated it — the latter survives the delivered→seen transition (which
	// deletes the awaitingDelivered entry), the former covers the edge where a
	// still-outstanding id was evicted from the bounded LRU. A genuine receipt
	// passes; a phantom one is dropped before any store / persist / event.
	_, stillAwaiting := s.awaitingDelivered[receipt.MessageID]
	if receipt.Recipient == s.identity.Address &&
		(receipt.Status == protocol.ReceiptStatusDelivered || receipt.Status == protocol.ReceiptStatusSeen) &&
		!stillAwaiting && !s.sentDMIDs.Has(string(receipt.MessageID)) {
		count := len(s.receipts[receipt.Recipient])
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_unsolicited").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_unsolicited").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
		log.Warn().Str("message_id", string(receipt.MessageID)).Str("sender", receipt.Sender).Str("status", receipt.Status).Msg("delivery receipt dropped: no matching locally-sent message")
		return false, count
	}
	// Admission: gate the buffer/dedup/track/distribute ONLY for the
	// store-and-forward-for-a-subscriber case — a receipt that ARRIVED for some
	// recipient R that we hold because R subscribes to us. If R lost its last
	// subscriber (it disconnected between the handler's hasSubscriber gate,
	// which released gossipMu, and here, so its teardown drop already ran),
	// holding the receipt only leaks: drop it cleanly so no seenReceipts key is
	// poisoned and no s.receipts/relayRetry orphan is left.
	//
	// Two cases are ALWAYS admitted, never gated on a subscriber:
	//   - receipt.Recipient == us — addressed to this node (bounded separately);
	//   - receipt.Sender == us — a receipt WE generated (deliveredReceiptFor for
	//     an inbound/relayed DM, emitDeliveryReceipt): its Recipient is the
	//     original DM sender, who is reachable via relay/gossip and is NOT
	//     required to be a local subscriber. Gating it would silently swallow
	//     the delivered/seen receipt, so the sender never learns the DM landed.
	// seen_ack is plumbing (never buffered), also always admitted.
	//
	// Reading s.subs under a nested gossipMu.RLock while holding deliveryMu
	// (deliveryMu OUTER → gossipMu INNER, canonical order) makes the decision
	// atomic with the disconnect-drop: both take deliveryMu, so a recipient that
	// disconnects after admission is reclaimed by its drop, serialised after
	// this store.
	if receipt.Recipient != s.identity.Address &&
		receipt.Sender != s.identity.Address &&
		receipt.Status != protocol.ReceiptStatusSeenAck {
		s.gossipMu.RLock()
		offline := len(s.subs[receipt.Recipient]) == 0
		s.gossipMu.RUnlock()
		if offline {
			// Race: the recipient lost its last subscriber between the inbound
			// handler's hasSubscriber gate (which released gossipMu) and here,
			// so its teardown drop already ran. Drop the receipt CLEANLY and
			// early — do not dedup, buffer, track, distribute, clear
			// outbound/pending/relayRetry, touch the message store, emit a local
			// change, or claim it was stored. Continuing would let a stale
			// receipt erase live delivery state on a colliding MessageID and
			// surface a phantom UI event. The sender's end-to-end retry
			// re-delivers once the recipient reconnects.
			count := len(s.receipts[receipt.Recipient])
			s.deliveryMu.Unlock()
			log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_offline_recipient").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
			s.peerMu.Unlock()
			log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released_offline_recipient").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")
			log.Debug().Str("message_id", string(receipt.MessageID)).Str("sender", receipt.Sender).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("delivery receipt dropped: recipient subscriber gone")
			return false, count
		}
	}
	s.seenReceipts.Add(key)
	// seen_ack is scheduler plumbing, not a deliverable artefact: dedupe it
	// via seenReceipts but never store it in s.receipts, so it cannot leak
	// through fetch_delivery_receipts or the auth-time backlog replay
	// (neither surface has a v23 gate).
	if receipt.Status != protocol.ReceiptStatusSeenAck {
		list := append(s.receipts[receipt.Recipient], receipt)
		// Hard-cap every recipient's backlog. Own-identity receipts are never
		// drained by ack_delete (that only fires for relay/forward recipients),
		// and a transit/subscriber backlog can be kept growing by a peer that
		// streams unique receipts while holding a subscriber active — both grow
		// without bound otherwise. Keep the most recent; see
		// maxReceiptBacklogPerRecipient for why eviction is safe for each kind.
		if len(list) > maxReceiptBacklogPerRecipient {
			// Evicting an entry must also drop its dedup + retry shadows.
			// Otherwise (a) a later re-send of the evicted receipt hits the
			// seenReceipts duplicate branch above and is silently suppressed
			// instead of restoring the backlog, and (b) its relayRetry entry is
			// orphaned — retryableRelayReceipts only walks the live s.receipts,
			// so the entry never reaches TTL cleanup and accumulates toward
			// maxRelayRetryEntries, eventually starving live receipt retries.
			evicted := list[:len(list)-maxReceiptBacklogPerRecipient]
			for i := range evicted {
				ev := evicted[i]
				s.seenReceipts.Delete(ev.Recipient + ":" + string(ev.MessageID) + ":" + ev.Status)
				delete(s.relayRetry, relayReceiptKey(ev))
			}
			trimmed := make([]protocol.DeliveryReceipt, maxReceiptBacklogPerRecipient)
			copy(trimmed, list[len(list)-maxReceiptBacklogPerRecipient:])
			list = trimmed
		}
		s.receipts[receipt.Recipient] = list
	}
	now := time.Now().UTC()
	switch {
	case receipt.Recipient == s.identity.Address &&
		(receipt.Status == protocol.ReceiptStatusDelivered || receipt.Status == protocol.ReceiptStatusSeen):
		// End-to-end confirmation for a message WE sent — the delivery
		// retry scheduler can stop re-sending it.
		delete(s.awaitingDelivered, receipt.MessageID)
	case receipt.Recipient == s.identity.Address && receipt.Status == protocol.ReceiptStatusSeenAck:
		// The original sender confirmed our seen receipt — stop retrying it.
		delete(s.awaitingSeenAck, receipt.MessageID)
	case receipt.Sender == s.identity.Address && receipt.Status == protocol.ReceiptStatusSeen:
		// Our own outgoing seen receipt — retry it until the original
		// sender's seen_ack arrives (etap 3.3 contract).
		s.registerAwaitingSeenAckLocked(receipt, now)
	}
	delete(s.outbound, string(receipt.MessageID))
	msgAffected := s.clearPendingMessageLocked(receipt.MessageID)
	rcptAffected := s.clearPendingReceiptLocked(receipt.MessageID, receipt.Recipient, receipt.Status)
	delete(s.relayRetry, relayMessageKey(receipt.MessageID))
	delete(s.relayRetry, relayReceiptKey(receipt))
	count := len(s.receipts[receipt.Recipient])
	pendingDeltas := mergePendingDeltas(msgAffected, rcptAffected)
	// statusMu is INNERMOST per canonical peerMu → deliveryMu → statusMu
	// order — refreshAggregatePendingLocked writes s.aggregateStatus and
	// the subsequent snapshot read must observe that write under the
	// same lock.
	s.statusMu.Lock()
	if len(pendingDeltas) > 0 {
		s.refreshAggregatePendingLocked()
	}
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "storeDeliveryReceipt").Str("phase", "lock_released").Str("msg_id", string(receipt.MessageID)).Msg("peer_mu_writer")

	// Emit pending count deltas for all peers whose queues were modified.
	for _, d := range pendingDeltas {
		s.emitPeerPendingChanged(d.Address, d.Count)
	}
	if len(pendingDeltas) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	}

	// Update delivery status via the registered MessageStore BEFORE emitting
	// local change so the desktop UI can safely read the new status from
	// SQLite when it reacts to the event. Same invariant as
	// storeIncomingMessage. seen_ack is scheduler-plumbing, not a message
	// lifecycle state — chatlog knows only sent→delivered→seen, so it never
	// reaches the store or the UI event below.
	receiptStoreOK := true
	if s.messageStore != nil && receipt.Status != protocol.ReceiptStatusSeenAck {
		receiptStoreOK = s.messageStore.UpdateDeliveryStatus(receipt)
	}
	if receipt.Status == protocol.ReceiptStatusSeenAck {
		receiptStoreOK = false
	}

	if receiptStoreOK {
		event := protocol.LocalChangeEvent{
			Type:        protocol.LocalChangeReceiptUpdate,
			Topic:       "dm",
			MessageID:   string(receipt.MessageID),
			Sender:      receipt.Sender,
			Recipient:   receipt.Recipient,
			Status:      receipt.Status,
			DeliveredAt: receipt.DeliveredAt,
		}
		s.emitLocalChange(event)
		s.eventBus.Publish(ebus.TopicReceiptUpdated, event)
	}

	log.Info().Str("message_id", string(receipt.MessageID)).Str("sender", receipt.Sender).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Time("delivered_at", receipt.DeliveredAt).Msg("stored delivery receipt")

	if receipt.Recipient == s.identity.Address {
		// First arrival of a "seen" addressed to us — confirm it to the
		// seen-sender so their retry loop stops (etap 3.3).
		if receipt.Status == protocol.ReceiptStatusSeen {
			s.goBackground(func() { s.sendSeenAck(receipt) })
		}
		// Persist the confirmation so the seen retry does not resurrect
		// after a restart (the journal is chatlog-backed; SQLite I/O runs
		// outside every domain mutex on the background pool).
		if receipt.Status == protocol.ReceiptStatusSeenAck && s.seenAckJournal != nil {
			journal := s.seenAckJournal
			id := receipt.MessageID
			s.goBackground(func() {
				if err := journal.MarkSeenConfirmed(id); err != nil {
					log.Warn().Str("message_id", string(id)).Err(err).Msg("seen_ack_journal_write_failed")
				}
			})
		}
		return true, count
	}

	// Transit receipt bookkeeping: seen_ack is excluded — it is not stored
	// in s.receipts, so retryableRelayReceipts could never revisit its
	// relayRetry entry, and the sender-side scheduler already owns its
	// end-to-end retry.
	if receipt.Status == protocol.ReceiptStatusSeenAck {
		s.distributeReceipt(receipt)
		return true, count
	}

	s.trackRelayReceipt(receipt)
	// Relay receipt return path first, gossip fallback, plus live
	// subscribers — shared with the duplicate re-send paths and the
	// delivery retry scheduler.
	s.distributeReceipt(receipt)

	return true, count
}

// isTransitReceiptSeen returns true if the receipt was already recorded in
// seenReceipts (read-only check — does not mark). Used as a fast-path guard
// at the handler level to avoid redundant relay-chain or gossip processing
// for receipts that were already successfully delivered on a prior arrival.
func (s *Service) isTransitReceiptSeen(receipt protocol.DeliveryReceipt) bool {
	key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status

	s.deliveryMu.RLock()
	ok := s.seenReceipts.Has(key)
	s.deliveryMu.RUnlock()
	return ok
}

// markTransitReceiptSeen records a transit receipt in seenReceipts and returns
// true if this receipt was already seen (duplicate — caller should drop it).
// Unlike storeDeliveryReceipt, this does NOT store the receipt locally or clear
// outbound/pending/relayRetry state — those side-effects are only meaningful
// for receipts addressed to this node. The shared seenReceipts key format
// ensures that a receipt seen via local delivery is also suppressed on the
// transit path and vice versa.
//
// For the gossip fallback path, callers pre-mark BEFORE launching the gossip
// goroutine to eliminate the race window where duplicates slip through.
// gossipTransitReceipt calls unmarkTransitReceiptSeen on complete failure
// to restore retry eligibility. For the relay chain path, callers mark
// AFTER confirmed forwarding (no rollback needed — relay is synchronous).
func (s *Service) markTransitReceiptSeen(receipt protocol.DeliveryReceipt) bool {
	key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status

	log.Trace().Str("site", "markTransitReceiptSeen").Str("phase", "lock_wait").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "markTransitReceiptSeen").Str("phase", "lock_held").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	duplicate := s.seenReceipts.MarkIfAbsent(key)
	s.deliveryMu.Unlock()
	if duplicate {
		log.Trace().Str("site", "markTransitReceiptSeen").Str("phase", "lock_released_dup").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
		return true
	}
	log.Trace().Str("site", "markTransitReceiptSeen").Str("phase", "lock_released").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	return false
}

// unmarkTransitReceiptSeen removes a previously marked transit receipt from
// seenReceipts. Called by gossipTransitReceipt when gossip fails completely
// (no routing targets or all sends rejected) to restore retry eligibility.
// The pre-mark + unmark-on-failure pattern eliminates the race window where
// a duplicate receipt could slip through between the synchronous mark (in
// the caller) and the deferred mark (inside the goroutine).
func (s *Service) unmarkTransitReceiptSeen(receipt protocol.DeliveryReceipt) {
	key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status

	log.Trace().Str("site", "unmarkTransitReceiptSeen").Str("phase", "lock_wait").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "unmarkTransitReceiptSeen").Str("phase", "lock_held").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
	s.seenReceipts.Delete(key)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "unmarkTransitReceiptSeen").Str("phase", "lock_released").Str("msg_id", string(receipt.MessageID)).Msg("delivery_mu_writer")
}

// gossipTransitReceipt is the transit-receipt variant of gossipReceipt.
// It resolves routing targets first; if none exist the receipt is unmarked
// (via unmarkTransitReceiptSeen) — allowing a future arrival of the same
// receipt to retry after routes recover. When targets exist, sends are
// attempted synchronously (sendReceiptToPeer is an in-memory channel/queue
// write, not a network I/O call). If every send fails (all channels full,
// all sessions stalled), the receipt is unmarked and eligible for retry.
//
// The caller must pre-mark the receipt via markTransitReceiptSeen before
// calling gossipTransitReceipt. Pre-marking eliminates the race window
// where a duplicate receipt could slip through before the mark. On
// complete failure (no targets or all sends rejected) the mark is rolled
// back so retry eligibility is preserved.
func (s *Service) gossipTransitReceipt(receipt protocol.DeliveryReceipt) {
	defer crashlog.DeferRecover()

	targets := s.routingTargetsForRecipient(receipt.Recipient)
	if len(targets) == 0 {
		s.unmarkTransitReceiptSeen(receipt)
		log.Debug().
			Str("message_id", string(receipt.MessageID)).
			Str("recipient", receipt.Recipient).
			Msg("transit receipt gossip skipped: no routing targets, receipt eligible for retry")
		return
	}

	delivered := false
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if s.sendReceiptToPeer(address, receipt) {
			delivered = true
		}
	}

	if !delivered {
		s.unmarkTransitReceiptSeen(receipt)
		log.Debug().
			Str("message_id", string(receipt.MessageID)).
			Str("recipient", receipt.Recipient).
			Int("targets", len(targets)).
			Msg("transit receipt gossip: all sends failed, receipt eligible for retry")
		return
	}
	// At least one target accepted — receipt stays marked as seen.
}

// mergePendingDeltas deduplicates two slices of PeerPendingDelta by address.
// When the same address appears in both, the entry from b wins (it reflects
// the later mutation under the same lock hold).
func mergePendingDeltas(a, b []ebus.PeerPendingDelta) []ebus.PeerPendingDelta {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	seen := make(map[domain.PeerAddress]int, len(a)+len(b))
	result := make([]ebus.PeerPendingDelta, 0, len(a)+len(b))
	for _, d := range a {
		seen[d.Address] = len(result)
		result = append(result, d)
	}
	for _, d := range b {
		if idx, ok := seen[d.Address]; ok {
			result[idx] = d // b wins — later mutation
		} else {
			result = append(result, d)
		}
	}
	return result
}

// clearPendingMessageLocked removes every queued frame that carries the
// message (matched by messageID) from the pending queue: the sender-side
// send_message AND any relay_message queued for the same id by the relay
// fallback (sendRelayMessage → queuePeerFrame when the session is
// unavailable). Both must go together — once the message is confirmed
// delivered or abandoned, flushing a leftover relay_message later would
// re-emit a finished delivery. Returns affected (address, newCount) pairs
// so the caller can emit TopicPeerPendingChanged after releasing the lock.
// Caller MUST hold s.deliveryMu.Lock (mutates s.pending / s.pendingKeys).
func (s *Service) clearPendingMessageLocked(messageID protocol.MessageID) []ebus.PeerPendingDelta {
	if strings.TrimSpace(string(messageID)) == "" {
		return nil
	}
	var affected []ebus.PeerPendingDelta
	for address, items := range s.pending {
		origLen := len(items)
		remaining := items[:0]
		for _, item := range items {
			if (item.Frame.Type == "send_message" || item.Frame.Type == "relay_message") && item.Frame.ID == string(messageID) {
				delete(s.pendingKeys, pendingFrameKey(address, item.Frame))
				continue
			}
			remaining = append(remaining, item)
		}
		if len(remaining) == origLen {
			continue // nothing changed for this peer
		}
		if len(remaining) == 0 {
			delete(s.pending, address)
			affected = append(affected, ebus.PeerPendingDelta{Address: address, Count: 0})
			continue
		}
		s.pending[address] = append([]pendingFrame(nil), remaining...)
		affected = append(affected, ebus.PeerPendingDelta{Address: address, Count: len(remaining)})
	}
	return affected
}

// clearPendingReceiptLocked removes a specific relay_delivery_receipt from the
// pending queue (matched by messageID+recipient+status). Returns affected
// (address, newCount) pairs so the caller can emit TopicPeerPendingChanged
// after releasing the lock.
// Caller MUST hold s.deliveryMu.Lock (mutates s.pending / s.pendingKeys).
func (s *Service) clearPendingReceiptLocked(messageID protocol.MessageID, recipient, status string) []ebus.PeerPendingDelta {
	if strings.TrimSpace(string(messageID)) == "" || strings.TrimSpace(recipient) == "" || strings.TrimSpace(status) == "" {
		return nil
	}
	var affected []ebus.PeerPendingDelta
	for address, items := range s.pending {
		origLen := len(items)
		remaining := items[:0]
		for _, item := range items {
			if item.Frame.Type == "relay_delivery_receipt" &&
				item.Frame.ID == string(messageID) &&
				item.Frame.Recipient == recipient &&
				item.Frame.Status == status {
				delete(s.pendingKeys, pendingFrameKey(address, item.Frame))
				continue
			}
			remaining = append(remaining, item)
		}
		if len(remaining) == origLen {
			continue // nothing changed for this peer
		}
		if len(remaining) == 0 {
			delete(s.pending, address)
			affected = append(affected, ebus.PeerPendingDelta{Address: address, Count: 0})
			continue
		}
		s.pending[address] = append([]pendingFrame(nil), remaining...)
		affected = append(affected, ebus.PeerPendingDelta{Address: address, Count: len(remaining)})
	}
	return affected
}

// localTopicSnapshot returns a copy of s.topics[topic] with transit
// envelopes excluded. Transit DMs (neither sender nor recipient is this
// node) live in s.topics solely as the in-flight buffer of their own
// forwarding operation (transit_retention.go) — they are NOT a mailbox and
// must never leak through fetch/backlog surfaces. Non-DM topics are local
// by contract (isTransitEnvelope returns false), so they pass unfiltered.
func (s *Service) localTopicSnapshot(topic string) []protocol.Envelope {
	s.gossipMu.RLock()
	src := s.topics[topic]
	out := make([]protocol.Envelope, 0, len(src))
	for _, env := range src {
		if s.isTransitEnvelope(env) {
			continue
		}
		out = append(out, env)
	}
	s.gossipMu.RUnlock()
	return out
}

func (s *Service) fetchMessagesFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessages}
	}
	// Query paths bypass the throttle so callers always see accurate data
	// (expired messages removed). Only storeIncomingMessage uses the
	// throttled variant to avoid repeated scans during message bursts.
	s.cleanupExpiredMessagesForce()

	messages := s.localTopicSnapshot(topic)

	items := make([]protocol.MessageFrame, 0, len(messages))
	for _, msg := range messages {
		items = append(items, messageFrame(msg))
	}

	if len(items) > 0 {
		ids := make([]string, len(items))
		for i, m := range items {
			ids[i] = m.ID
		}
		log.Debug().Str("node", s.identity.Address).Str("topic", topic).Int("count", len(items)).Strs("ids", ids).Msg("fetch_messages_result")
	}

	return protocol.Frame{Type: "messages", Topic: topic, Count: len(items), Messages: items}
}

func (s *Service) fetchMessageIDsFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessageIDs}
	}
	s.cleanupExpiredMessagesForce()

	messages := s.localTopicSnapshot(topic)

	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, string(msg.ID))
	}

	return protocol.Frame{Type: "message_ids", Topic: topic, Count: len(ids), IDs: ids}
}

func (s *Service) fetchMessageFrame(topic, messageID string) protocol.Frame {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(messageID) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessage}
	}
	s.cleanupExpiredMessagesForce()

	messages := s.localTopicSnapshot(topic)

	for _, msg := range messages {
		if string(msg.ID) == messageID {
			item := messageFrame(msg)
			return protocol.Frame{Type: "message", Topic: topic, ID: messageID, Item: &item}
		}
	}

	return protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownMessageID}
}

func (s *Service) fetchInboxFrame(topic, recipient string) protocol.Frame {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(recipient) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchInbox}
	}
	s.cleanupExpiredMessagesForce()

	// Locality filter: for a remote recipient this leaves only the DMs this
	// node itself sent (sender-owned retry); transit envelopes addressed to
	// them are forwarding state, not a mailbox, and must not be replayed by
	// the auth-time backlog push built on this frame.
	messages := s.localTopicSnapshot(topic)

	items := make([]protocol.MessageFrame, 0, len(messages))
	for _, msg := range messages {
		if topic == "dm" && msg.Recipient == recipient {
			// Receipt lookup is a delivery-domain read — taken as a
			// separate lock window after the gossipMu snapshot was
			// already released, so no nested-lock concern.
			s.deliveryMu.RLock()
			delivered := s.hasReceiptForMessageLocked(msg.Sender, msg.ID)
			s.deliveryMu.RUnlock()
			if delivered {
				continue
			}
		}
		if msg.Recipient == recipient || msg.Recipient == "*" {
			items = append(items, messageFrame(msg))
		}
	}

	return protocol.Frame{Type: "inbox", Topic: topic, Recipient: recipient, Count: len(items), Messages: items}
}

func (s *Service) fetchDeliveryReceiptsFrame(recipient string) protocol.Frame {
	if strings.TrimSpace(recipient) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchReceipts}
	}

	s.deliveryMu.RLock()
	items := append([]protocol.DeliveryReceipt(nil), s.receipts[recipient]...)
	s.deliveryMu.RUnlock()

	frames := make([]protocol.ReceiptFrame, 0, len(items))
	for _, item := range items {
		frames = append(frames, receiptFrame(item))
	}

	return protocol.Frame{
		Type:      "delivery_receipts",
		Recipient: recipient,
		Count:     len(frames),
		Receipts:  frames,
	}
}

// countInboundConnsLocked returns the number of inbound entries currently
// in the primary registry. Outbound entries (created by
// attachOutboundNetCore) are not counted — the inbound cap only governs
// incoming TCP acceptance. Reads peer-domain inbound-conn registry state —
// caller MUST hold s.peerMu (read or write).
func (s *Service) countInboundConnsLocked() int {
	return s.inboundConnCountLocked()
}

// registerInboundConn is the public lifecycle wrapper that births the
// (net.Conn, ConnID) binding for an inbound socket. net.Conn-first by the
// carve-out list in conn_registry.go: there is no ConnID before this
// function runs.
func (s *Service) registerInboundConn(conn net.Conn) bool {
	log.Trace().Str("site", "registerInboundConn").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "registerInboundConn").Str("phase", "lock_held").Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "registerInboundConn").Str("phase", "lock_released").Msg("peer_mu_writer")
	}()

	limit := s.cfg.EffectiveMaxIncomingPeers()
	if limit > 0 && s.countInboundConnsLocked() >= limit {
		return false
	}

	s.connIDCounter++
	pc := netcore.New(netcore.ConnID(s.connIDCounter), conn, netcore.Inbound, netcore.Options{})
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok && addr.IP.IsLoopback() {
		pc.SetLocal(true)
	}

	var mc *netcore.MeteredConn
	if metered, ok := conn.(*netcore.MeteredConn); ok {
		mc = metered
	}
	s.registerInboundConnLocked(conn, pc, mc)
	return true
}

// attachOutboundNetCore creates an outbound NetCore for the given dialled
// connection, registers it in s.conns (the single primary registry that
// holds both inbound and outbound entries) and wires session.netCore /
// session.onClose so that peerSession.Close() removes the registration
// atomically with the NetCore teardown.
//
// The NetCore must exist before peerSessionRequest runs so that the welcome
// and auth frames are routed through the managed single-writer path instead
// of raw io.WriteString. This is the Phase 1 gate C1 — all outbound writes
// share the same back-pressure and deadline discipline as inbound.
func (s *Service) attachOutboundNetCore(session *peerSession) *netcore.NetCore {
	pc := netcore.New(session.connID, session.conn, netcore.Outbound, netcore.Options{})

	log.Trace().Str("site", "attachOutboundNetCore").Str("phase", "lock_wait").Uint64("conn_id", uint64(session.connID)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "attachOutboundNetCore").Str("phase", "lock_held").Uint64("conn_id", uint64(session.connID)).Msg("peer_mu_writer")
	s.attachOutboundCoreLocked(session.conn, pc)
	s.peerMu.Unlock()
	log.Trace().Str("site", "attachOutboundNetCore").Str("phase", "lock_released").Uint64("conn_id", uint64(session.connID)).Msg("peer_mu_writer")

	// Capture lifecycle hook: attach sink and notify manager.
	s.notifyCaptureNewConn(session.connID)

	session.netCore = pc
	conn := session.conn
	connID := session.connID
	session.onClose = func() {
		// Capture lifecycle hook: stop capture before registry removal.
		s.notifyCaptureConnClosed(connID)
		log.Trace().Str("site", "attachOutboundNetCore_onClose").Str("phase", "lock_wait").Uint64("conn_id", uint64(connID)).Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "attachOutboundNetCore_onClose").Str("phase", "lock_held").Uint64("conn_id", uint64(connID)).Msg("peer_mu_writer")
		s.unregisterConnLocked(conn)
		s.peerMu.Unlock()
		log.Trace().Str("site", "attachOutboundNetCore_onClose").Str("phase", "lock_released").Uint64("conn_id", uint64(connID)).Msg("peer_mu_writer")
	}
	return pc
}

// unregisterInboundConn is the public lifecycle wrapper that tears down the
// (net.Conn, ConnID) binding for an inbound socket. net.Conn-first by the
// carve-out list in conn_registry.go: the secondary index
// s.connIDByNetConn cannot be trimmed without the same net.Conn that
// registerInboundConn registered.
func (s *Service) unregisterInboundConn(conn net.Conn) {
	// Resolve the ConnID under the lock, then run the teardown via the
	// netcore.Network surface BEFORE unregister so the bridge can still
	// look the entry up. NetCore.Close() handles the full shutdown
	// sequence:
	//   1. shut the send gate — every producer still holding the NetCore
	//      is answered SendChanClosed instead of racing the teardown
	//   2. rawConn.Close() — unblocks writer stuck in conn.Write
	//   3. signal closing — writer releases the queue residue and returns
	//   4. <-writerExited — waits for that to complete
	//
	// Step 1 is why the ordering here (Close, then unregister) is not a
	// bug: the registry is not a lease, a sender may hold the NetCore
	// across the whole teardown, and Close is what refuses it.
	//
	// Note: handleConn calls metered.Close() before unregisterInboundConn,
	// which is now redundant (NetCore.Close does it). The double Close on
	// net.Conn is safe — subsequent calls return an error but don't panic.
	s.peerMu.RLock()
	id, ok := s.connIDForLocked(conn)
	s.peerMu.RUnlock()

	if ok {
		// Errors here mean the conn was already torn down on a parallel
		// path (ErrUnknownConn) or the runCtx is cancelled — both are
		// expected during shutdown. The eviction below is idempotent and
		// must run regardless.
		_ = s.Network().Close(s.runCtx, id)
	}

	log.Trace().Str("site", "unregisterInboundConn").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "unregisterInboundConn").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.unregisterConnLocked(conn)
	s.peerMu.Unlock()
	log.Trace().Str("site", "unregisterInboundConn").Str("phase", "lock_released").Msg("peer_mu_writer")
}

// closeAllInboundConns closes every tracked inbound connection so that
// handleConn goroutines unblock and exit. Called during graceful shutdown
// before connWg.Wait(). Uses the netcore.Network surface (Enumerate + Close)
// instead of the raw net.Conn path so the shutdown loop no longer carries
// the socket handle — identity of a connection in-flight is its ConnID.
func (s *Service) closeAllInboundConns() {
	ctx := context.Background()
	network := s.Network()

	ids := make([]domain.ConnID, 0)
	network.Enumerate(ctx, netcore.Inbound, func(id domain.ConnID) bool {
		ids = append(ids, id)
		return true
	})

	for _, id := range ids {
		_ = network.Close(ctx, id)
	}
	if len(ids) > 0 {
		log.Info().Int("count", len(ids)).Msg("closed inbound connections for shutdown")
	}
}

// pushToSubscriberSnapshot delivers a message to a pre-captured snapshot of
// subscribers. The snapshot is taken under s.gossipMu by the caller
// (subscribersForRecipient snapshots s.subs, which is gossip-domain
// state) so that the decision "route exists → push, not gossip" and the
// actual send targets are determined atomically against the gossip
// fan-out table. If a subscriber's connection has broken by the time we
// write, the message is still safe in s.topics and will be delivered
// via backlog replay on the peer's next authentication.
func (s *Service) pushToSubscriberSnapshot(msg protocol.Envelope, subs []*subscriber) {
	defer crashlog.DeferRecover()
	if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
		return
	}
	log.Info().Str("id", string(msg.ID)).Str("topic", msg.Topic).Str("recipient", msg.Recipient).Int("subscribers", len(subs)).Msg("push_message")

	frame := protocol.Frame{
		Type:      "push_message",
		Topic:     msg.Topic,
		Recipient: msg.Recipient,
		Item: func() *protocol.MessageFrame {
			item := messageFrame(msg)
			return &item
		}(),
	}
	// Node-route subscribers run storeIncomingMessage on their side: a
	// recipient that never met the sender needs the self-certifying key
	// triple to verify a first-contact DM (see attachKnownSenderKeys).
	s.attachKnownSenderKeys(&frame, msg.Topic, msg.Sender)

	if len(subs) > 0 && !s.noteOwnEnvelopeEmitted(msg.Sender, msg.ID) {
		// Withdrawn while we were building the frame, or the durable
		// "never emitted" claim could not be withdrawn — either way this
		// push must not happen. The retry engine still owns the message.
		log.Info().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).
			Msg("push_message_withheld")
		return
	}
	for _, sub := range subs {
		s.goBackground(func() { s.writePushFrame(sub, frame) })
	}
}

func (s *Service) pushReceiptToSubscribers(receipt protocol.DeliveryReceipt) {
	defer crashlog.DeferRecover()
	subs := s.subscribersForRecipient(receipt.Recipient)
	if len(subs) == 0 {
		return
	}
	log.Info().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Int("subscribers", len(subs)).Msg("push_delivery_receipt")

	frame := protocol.Frame{
		Type:      "push_delivery_receipt",
		Recipient: receipt.Recipient,
		Receipt: func() *protocol.ReceiptFrame {
			item := receiptFrame(receipt)
			return &item
		}(),
	}

	for _, sub := range subs {
		// seen_ack is additive in ProtocolVersion 23 — a pre-v23 subscriber
		// would only reject the unknown status at parse time, so skip it.
		// TODO(seen-ack-gate-removal): delete this gate (and the matching
		// one in sendReceiptToPeer) once MinimumProtocolVersion reaches
		// config.ProtocolVersionSeenAck.
		if receipt.Status == protocol.ReceiptStatusSeenAck && !s.connSupportsProtocol(sub.connID, config.ProtocolVersionSeenAck) {
			log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("subscriber", sub.id).Msg("push_receipt_skipped_pre_v23_subscriber")
			continue
		}
		s.goBackground(func() { s.writePushFrame(sub, frame) })
	}
}

func (s *Service) pushBacklogToSubscriber(sub *subscriber) {
	defer crashlog.DeferRecover()
	if sub == nil || strings.TrimSpace(sub.recipient) == "" {
		return
	}

	inbox := s.fetchInboxFrame("dm", sub.recipient)
	log.Info().Str("node", s.identity.Address).Str("recipient", sub.recipient).Int("backlog_count", len(inbox.Messages)).Msg("pushBacklogToSubscriber")

	// Record the whole batch as emitted in ONE delivery-domain section,
	// before any frame goes out. Marking per message would take the
	// writer mutex once per row of the backlog from this background
	// goroutine; the answer is the same either way, and it has to be
	// written first — see noteOwnEnvelopesEmitted.
	own := make([]protocol.MessageID, 0, len(inbox.Messages))
	for _, item := range inbox.Messages {
		if item.Sender == s.identity.Address {
			own = append(own, protocol.MessageID(item.ID))
		}
	}
	// Marking and asking "was any of this withdrawn while we were
	// building the snapshot" happen under the delivery mutex, so a
	// cancellation is either visible here or has not run yet — and if it
	// has not, it will find the entry already marked emitted and report
	// the message as possibly-out, which schedules the peer-side delete.
	// Either way the user is never told a message was recalled and then
	// handed it to the peer anyway. The same return also names the ids
	// whose durable never-emitted claim could not be withdrawn: those are
	// not ours to send yet either.
	withheld := s.noteOwnEnvelopesEmitted(own)

	for _, item := range inbox.Messages {
		if _, skip := withheld[protocol.MessageID(item.ID)]; skip {
			log.Info().
				Str("recipient", sub.recipient).
				Str("message_id", item.ID).
				Msg("backlog_push_withheld")
			continue
		}
		if createdAt, err := time.Parse(time.RFC3339, item.CreatedAt); err == nil && s.messageDeliveryExpired(createdAt.UTC(), item.TTLSeconds) {
			continue
		}
		msgFrame := item
		replay := protocol.Frame{
			Type:      "push_message",
			Topic:     "dm",
			Recipient: sub.recipient,
			Item:      &msgFrame,
		}
		// Backlog replay is the primary delivery path for a recipient
		// that reconnects after the original relay/push attempt — a
		// first-contact recipient needs the sender's self-certifying
		// key triple here just as on the live push path.
		s.attachKnownSenderKeys(&replay, "dm", msgFrame.Sender)
		s.writePushFrame(sub, replay)
	}

	receipts := s.fetchDeliveryReceiptsFrame(sub.recipient)
	for _, item := range receipts.Receipts {
		receiptFrame := item
		s.writePushFrame(sub, protocol.Frame{
			Type:      "push_delivery_receipt",
			Recipient: sub.recipient,
			Receipt:   &receiptFrame,
		})
	}
}

func (s *Service) publishNoticeFrame(frame protocol.Frame) protocol.Frame {
	ttl := time.Duration(frame.TTLSeconds) * time.Second
	if ttl <= 0 || strings.TrimSpace(frame.Ciphertext) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidPublishNotice}
	}

	s.cleanupExpiredNotices()

	id := gazeta.ID(frame.Ciphertext)
	expiresAt := time.Now().UTC().Add(ttl)

	log.Trace().Str("site", "publishNoticeFrame").Str("phase", "lock_wait").Str("notice_id", id).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "publishNoticeFrame").Str("phase", "lock_held").Str("notice_id", id).Msg("gossipMu_writer")
	if existing, ok := s.notices[id]; ok && existing.ExpiresAt.After(time.Now().UTC()) {
		s.gossipMu.Unlock()
		log.Trace().Str("site", "publishNoticeFrame").Str("phase", "lock_released_dup").Str("notice_id", id).Msg("gossipMu_writer")
		return protocol.Frame{Type: "notice_known", ID: id, ExpiresAt: existing.ExpiresAt.Unix()}
	}

	s.notices[id] = gazeta.Notice{
		ID:         id,
		Ciphertext: frame.Ciphertext,
		ExpiresAt:  expiresAt,
	}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "publishNoticeFrame").Str("phase", "lock_released").Str("notice_id", id).Msg("gossipMu_writer")

	if s.CanForward() {
		s.goBackground(func() { s.gossipNotice(ttl, frame.Ciphertext) })
	}

	return protocol.Frame{Type: "notice_stored", ID: id, ExpiresAt: expiresAt.Unix()}
}

func (s *Service) fetchNoticesFrame() protocol.Frame {
	s.cleanupExpiredNotices()

	s.gossipMu.RLock()
	items := make([]protocol.NoticeFrame, 0, len(s.notices))
	for _, notice := range s.notices {
		items = append(items, protocol.NoticeFrame{
			ID:         notice.ID,
			ExpiresAt:  notice.ExpiresAt.Unix(),
			Ciphertext: notice.Ciphertext,
		})
	}
	s.gossipMu.RUnlock()

	return protocol.Frame{Type: "notices", Count: len(items), Notices: items}
}

// nodeHelloJSONLine builds the marshalled hello line this node opens a session
// with.
func (s *Service) nodeHelloJSONLine() string {
	// v12 cleanup: hello no longer carries the local advertise host in
	// Listen — host is no longer a wire concept and is learned by the
	// receiver from the inbound TCP RemoteAddr. The Listener flag still
	// signals "this peer accepts inbound" and AdvertisePort carries the
	// listening port; together they replace the old host:port Listen
	// contract. AdvertisePort is the sole port truth source on the
	// receive side and carries CORSA_ADVERTISE_PORT, with fallback to
	// DefaultPeerPort resolved by EffectiveAdvertisePort.
	var advertisePort domain.PeerPort
	if s.cfg.EffectiveListenerEnabled() {
		advertisePort = s.cfg.EffectiveAdvertisePort()
	}
	datagrams := s.localDatagramAdvertise()
	// reachableGroups is startup-immutable (see ipStateMu field doc); the
	// read is intentionally lock-free.
	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		Listener:      listenerFlag(s.cfg.EffectiveListenerEnabled()),
		AdvertisePort: advertisePort,
		NodeType:      string(s.NodeType()),
		ClientVersion: s.ClientVersion(),
		ClientBuild:   config.ClientVersionBuild,
		Services:      s.Services(),
		Networks:      reachableGroupNames(s.reachableGroups),
		Address:       s.identity.Address,
		PubKey:        identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:        s.selfBoxKey,
		BoxSig:        s.selfBoxSig,
		Capabilities:  localHandshakeCapabilityStrings(s.localHandshakeCapabilityNames()),
		DTypes:        s.localDTypeStrings(datagrams),
	})
	if err != nil {
		return ""
	}
	return line
}

// listenerEnabledFromFrame returns true ONLY when the frame explicitly
// sets Listener="1". Absent / empty / any other value (including a
// non-empty hello.Listen on a frame whose Listener flag is missing)
// counts as "not a listener" — the v12 wire contract makes the
// listener flag the single source of truth for the "is this peer
// reachable?" gate. Pre-v12 the fallback `Listen != "" → listener`
// existed to recognise legacy peers that omitted the explicit flag,
// but under the v14 floor those peers cannot complete a handshake
// in the first place, so the fallback is dropped.
func listenerEnabledFromFrame(frame protocol.Frame) bool {
	switch strings.TrimSpace(frame.Listener) {
	case "1":
		return true
	default:
		return false
	}
}

func listenerFlag(enabled bool) string {
	if enabled {
		return "1"
	}
	return "0"
}

func (s *Service) learnIdentityFromWelcome(frame protocol.Frame, dialAddress domain.PeerAddress) {
	// Self-loopback guard: if the welcome carries our own Ed25519
	// address the remote is actually us reflected back — silently
	// skip the learning pass so we do not re-ingest our own listen
	// address as a peer candidate or our own box key as a contact.
	// Call sites that also need to abort their broader pipeline
	// (auth_session) consult isSelfIdentity
	// independently; this guard is a defence-in-depth boundary.
	if s.isSelfIdentity(domain.PeerIdentityFromWire(frame.Address)) {
		log.Warn().
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", frame.Listen).
			Str("welcome_client", frame.Client).
			Msg("welcome_self_identity_skipped")
		return
	}
	if listenerEnabledFromFrame(frame) {
		// v12 wire contract: welcome.Listen carries no truth, so we
		// promote / version the peer UNCONDITIONALLY under the dial
		// address — the host:port we successfully reached the peer
		// at. Consulting welcome.Listen, even as a "prefer if
		// non-empty" hint, would open a wedge for a hostile or buggy
		// responder to redirect our local bookkeeping at an arbitrary
		// host. The dial address is the only value we can attest to.
		if normalizedAddr, ok := s.normalizePeerAddress(dialAddress, dialAddress); ok {
			s.promotePeerAddress(normalizedAddr)
			s.rememberPeerType(normalizedAddr, frame.NodeType)
			s.addPeerID(normalizedAddr, domain.PeerIdentityFromWire(frame.Address))
			s.addPeerVersion(normalizedAddr, frame.ClientVersion)
			s.addPeerBuild(normalizedAddr, frame.ClientBuild)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(domain.PeerIdentityFromWire(frame.Address))
	}
	// Validated ingest: the welcome is NOT authenticated at this point, so
	// key material is cached only when it self-certifies — see
	// learnWireIdentityKeys for the fingerprint/binding/length gates that
	// close the pre-auth poisoning and oversized-blob paths.
	s.learnWireIdentityKeys(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig)
}

// addPeerFrame handles the local "add_peer" console command.
// The peer is prepended to the peer list so it becomes the first dial
// candidate on the next bootstrap tick.
func (s *Service) fetchDMHeadersFrame() protocol.Frame {
	s.cleanupExpiredMessagesForce()

	s.gossipMu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics["dm"]...)
	s.gossipMu.RUnlock()

	myAddr := s.identity.Address
	headers := make([]protocol.DMHeaderFrame, 0, len(messages))
	for _, msg := range messages {
		// Skip transit DMs — only include messages where this node is sender or recipient.
		if msg.Sender != myAddr && msg.Recipient != myAddr {
			continue
		}
		headers = append(headers, protocol.DMHeaderFrame{
			ID:        string(msg.ID),
			Sender:    msg.Sender,
			Recipient: msg.Recipient,
			CreatedAt: msg.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	return protocol.Frame{
		Type:      "dm_headers",
		DMHeaders: headers,
		Count:     len(headers),
	}
}

// isKnownNodeType returns true if the node type is one we recognize and can
// work with. Unknown types from future protocol versions are rejected so we
// don't add peers we cannot meaningfully interact with.
func isKnownNodeType(raw string) bool {
	_, ok := domain.ParseNodeType(raw)
	return ok
}

func (s *Service) addKnownIdentity(identity domain.PeerIdentity) {
	if identity.IsZero() {
		return
	}

	address := identity.String()
	log.Trace().Str("site", "addKnownIdentity").Str("phase", "lock_wait").Str("address", address).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "addKnownIdentity").Str("phase", "lock_held").Str("address", address).Msg("knowledgeMu_writer")
	existed := !s.known.Add(address)
	s.knowledgeMu.Unlock()
	log.Trace().Str("site", "addKnownIdentity").Str("phase", "lock_released").Str("address", address).Msg("knowledgeMu_writer")

	if !existed {
		ebus.PublishIdentityAdded(s.eventBus, identity)
	}
}

// suppressesSelfBoxKey reports whether box-key material for address must
// be kept out of the contact plane (knowledge maps / trust store). True
// only for THIS node's own identity under the relay-only DM opt-out
// (cfg.DisableDirectMessages): the handshake still hands the key to
// direct peers — session auth requires all four identity fields — so a
// neighbor can echo our genuine, validly-signed key back through
// fetch_contacts sync or import_contacts. Without this guard the echo
// re-enters s.boxKeys / the trust store and contactsFrame redistributes
// the key network-wide, silently defeating the contact-plane opt-out
// established at NewService. Every network-sourced key import funnels
// through addKnownBoxKey/addKnownBoxSig/trustContact, which all consult
// this predicate.
func (s *Service) suppressesSelfBoxKey(address string) bool {
	return s.cfg.DisableDirectMessages && address == s.identity.Address
}

func (s *Service) addKnownBoxKey(address, boxKey string) {
	if address == "" || boxKey == "" {
		return
	}
	if s.suppressesSelfBoxKey(address) {
		return
	}

	log.Trace().Str("site", "addKnownBoxKey").Str("phase", "lock_wait").Str("address", address).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "addKnownBoxKey").Str("phase", "lock_held").Str("address", address).Msg("knowledgeMu_writer")
	defer func() {
		s.knowledgeMu.Unlock()
		log.Trace().Str("site", "addKnownBoxKey").Str("phase", "lock_released").Str("address", address).Msg("knowledgeMu_writer")
	}()
	// Register the RAW address in the bounded known set so the entry
	// written below is always reachable by the set's eviction hook —
	// the invariant that keeps s.boxKeys bounded (see NewService).
	s.known.Add(address)
	s.boxKeys[address] = boxKey
}

func (s *Service) addKnownPubKey(address, pubKey string) {
	if address == "" || pubKey == "" {
		return
	}

	log.Trace().Str("site", "addKnownPubKey").Str("phase", "lock_wait").Str("address", address).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "addKnownPubKey").Str("phase", "lock_held").Str("address", address).Msg("knowledgeMu_writer")
	defer func() {
		s.knowledgeMu.Unlock()
		log.Trace().Str("site", "addKnownPubKey").Str("phase", "lock_released").Str("address", address).Msg("knowledgeMu_writer")
	}()
	// Same bounded-set registration as addKnownBoxKey — see NewService.
	s.known.Add(address)
	s.pubKeys[address] = pubKey
}

func (s *Service) addKnownBoxSig(address, boxSig string) {
	if address == "" || boxSig == "" {
		return
	}
	if s.suppressesSelfBoxKey(address) {
		return
	}

	log.Trace().Str("site", "addKnownBoxSig").Str("phase", "lock_wait").Str("address", address).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "addKnownBoxSig").Str("phase", "lock_held").Str("address", address).Msg("knowledgeMu_writer")
	defer func() {
		s.knowledgeMu.Unlock()
		log.Trace().Str("site", "addKnownBoxSig").Str("phase", "lock_released").Str("address", address).Msg("knowledgeMu_writer")
	}()
	// Same bounded-set registration as addKnownBoxKey — see NewService.
	s.known.Add(address)
	s.boxSigs[address] = boxSig
}

// forgetKnownBoxKey drops the box-key half of an identity's knowledge —
// the enforcement arm of an authoritative dm:false record: leaving the
// old key in the maps would let fetch_contacts and the direct-send paths
// keep encrypting to a key its owner has revoked.
func (s *Service) forgetKnownBoxKey(address string) {
	if address == "" {
		return
	}
	log.Trace().Str("site", "forgetKnownBoxKey").Str("phase", "lock_wait").Str("address", address).Msg("knowledgeMu_writer")
	s.knowledgeMu.Lock()
	log.Trace().Str("site", "forgetKnownBoxKey").Str("phase", "lock_held").Str("address", address).Msg("knowledgeMu_writer")
	defer func() {
		s.knowledgeMu.Unlock()
		log.Trace().Str("site", "forgetKnownBoxKey").Str("phase", "lock_released").Str("address", address).Msg("knowledgeMu_writer")
	}()
	delete(s.boxKeys, address)
	delete(s.boxSigs, address)
}

// attachKnownSenderKeys stamps the DM sender's PUBLIC key triple
// (Ed25519 signing key, X25519 box key, box-key binding signature) onto
// an outgoing DM transport frame (relay_message / push_message). The
// receiver validates the triple against the sender address — the
// address IS the signing key's fingerprint — and imports it via
// importAttachedSenderKeys, which is what lets a first-contact DM
// verify on a node that has never met the sender and whose relay hops
// do not know the sender either (the on-demand fetch_contacts recovery
// only reaches the previous hop, which a transit relay path does not
// oblige to hold the origin's keys).
//
// Best-effort by design: fields are left empty when the sender's
// signing key is unknown locally (a transit forwarder that stored the
// envelope had the key at store time, but LRU eviction may have dropped
// it since) — the receiver then falls back to the legacy sync path.
// The box key pair is attached only when BOTH box fields are present:
// an unmatched half cannot pass VerifyBoxKeyBinding on the receiver and
// would only waste wire bytes. For this node's OWN identity under the
// relay-only DM opt-out the box key is absent from s.boxKeys by
// construction (NewService seeds the self contact keyless), so an
// opt-out sender ships only its signing key — recipients can verify its
// envelopes but still cannot compose a DM back, preserving the opt-out
// contract. Only PUBLIC material is ever attached; private keys do not
// enter frames.
//
// No-op for non-DM topics: broadcast gossip has no envelope signature
// to verify, and stamping keys there would bloat every fan-out frame.
func (s *Service) attachKnownSenderKeys(frame *protocol.Frame, topic, sender string) {
	if !protocol.IsDMTopic(topic) || sender == "" {
		return
	}
	s.knowledgeMu.RLock()
	pubKey := s.pubKeys[sender]
	boxKey := s.boxKeys[sender]
	boxSig := s.boxSigs[sender]
	s.knowledgeMu.RUnlock()
	if pubKey == "" {
		return
	}
	frame.PubKey = pubKey
	if boxKey != "" && boxSig != "" {
		frame.BoxKey = boxKey
		frame.BoxSig = boxSig
	}
}

// maxAttachedKeyFieldLen caps each attached sender-key field on the
// wire. Real values are tiny (base64 of a 32-byte key = 44 chars,
// base64url of a 64-byte signature = 86 chars); the generous cap
// exists purely as a cheap pre-crypto DoS guard — an oversized field
// is rejected by length comparison alone, before any base64 decode or
// signature verification, and (on the transit path) is never copied
// into a forwarded frame, so a hostile origin cannot use the key
// fields as a wire-amplification channel that bypasses the body-size
// admission cap.
const maxAttachedKeyFieldLen = 256

// validateAttachedSenderPubKey performs the cheap structural +
// self-certification checks on a frame-attached signing key: length
// caps on all three fields first (pure comparisons — no allocation, no
// crypto), then the fingerprint match (identity.
// VerifyPublicKeyFingerprint; forging it requires a SHA-256 preimage).
// It deliberately performs NO imports and touches NO Service state —
// the caller uses the validated key for envelope verification and
// persists it only after the envelope signature proves the message
// genuine (importVerifiedSenderKeys).
func validateAttachedSenderPubKey(msg incomingMessage) error {
	if msg.Sender == "" || msg.SenderPubKey == "" {
		return fmt.Errorf("no attached sender key")
	}
	if len(msg.SenderPubKey) > maxAttachedKeyFieldLen ||
		len(msg.SenderBoxKey) > maxAttachedKeyFieldLen ||
		len(msg.SenderBoxSig) > maxAttachedKeyFieldLen {
		return fmt.Errorf("attached key field exceeds %d bytes", maxAttachedKeyFieldLen)
	}
	return identity.VerifyPublicKeyFingerprint(msg.Sender, msg.SenderPubKey)
}

// attachedBoxPairValid reports whether the frame carried a COMPLETE
// box pair whose binding verifies under the given (already
// fingerprint-validated) signing key. Pure check, no state. Length
// caps run first so an oversized blob costs a comparison, not a
// signature verification.
func attachedBoxPairValid(msg incomingMessage, senderPubKey string) bool {
	if msg.SenderBoxKey == "" || msg.SenderBoxSig == "" {
		return false
	}
	if len(msg.SenderBoxKey) > maxAttachedKeyFieldLen || len(msg.SenderBoxSig) > maxAttachedKeyFieldLen {
		return false
	}
	return identity.VerifyBoxKeyBinding(msg.Sender, senderPubKey, msg.SenderBoxKey, msg.SenderBoxSig) == nil
}

// importVerifiedSenderKeys persists frame-attached sender key material
// AFTER storeIncomingMessage has fully verified the DM envelope with
// it. Caller contract: the envelope signature verified against a
// signing key that fingerprint-matches msg.Sender — so writing here
// can only ever happen on behalf of a sender that authored a genuine,
// correctly-signed message. That ordering is what keeps the shared
// bounded LRU (s.known + key maps) safe from churn by
// valid-fingerprint-but-forged-envelope floods: a forged envelope is
// rejected before any state changes or IdentityAdded events fire.
//
// Write policy:
//   - signing key: written only when absent (first contact). A cached
//     pubkey is never overwritten — a fingerprint-matching key is
//     byte-identical anyway. addKnownIdentity runs FIRST: the
//     addKnown* key chokepoints register the raw address in s.known as
//     part of their bounded-set invariant, and doing that before
//     addKnownIdentity would make the first-sight check see the
//     identity as already known and swallow the IdentityAdded event —
//     leaving realtime known-ID consumers stale.
//   - box pair: written when the attached pair is binding-valid AND
//     the cached state is not a healthy pair — absent, half-cached
//     (key without signature or vice versa), or failing its binding.
//     A contact whose pubkey arrived earlier without a usable box pair
//     would otherwise stay reply-incapable forever: the
//     unknown-sender fallback sync never triggers once the pubkey
//     exists. The replacement is safe — the attached pair is
//     authenticated by the sender's own signing key.
//
// Imports funnel through the addKnown* chokepoints (bounded known set,
// relay-only self-box-key suppression).
func (s *Service) importVerifiedSenderKeys(msg incomingMessage) {
	if msg.Sender == "" || msg.SenderPubKey == "" {
		return
	}
	if validateAttachedSenderPubKey(msg) != nil {
		return
	}
	s.knowledgeMu.RLock()
	havePub := s.pubKeys[msg.Sender] != ""
	cachedBoxKey := s.boxKeys[msg.Sender]
	cachedBoxSig := s.boxSigs[msg.Sender]
	s.knowledgeMu.RUnlock()

	imported := false
	if !havePub {
		// Identity registration BEFORE the key write — see the doc
		// comment for why the reverse order swallows IdentityAdded.
		s.addKnownIdentity(domain.PeerIdentityFromWire(msg.Sender))
		s.addKnownPubKey(msg.Sender, msg.SenderPubKey)
		imported = true
	}
	// Steady-state fast path: a cached pair byte-identical to the
	// attached one was verified when it was imported — skip the
	// per-message signature check and only pay for it when the cache
	// actually diverges from the frame.
	cachedPairHealthy := cachedBoxKey != "" && cachedBoxSig != "" &&
		((cachedBoxKey == msg.SenderBoxKey && cachedBoxSig == msg.SenderBoxSig) ||
			identity.VerifyBoxKeyBinding(msg.Sender, msg.SenderPubKey, cachedBoxKey, cachedBoxSig) == nil)
	if !cachedPairHealthy && msg.SenderBoxKey != "" && msg.SenderBoxSig != "" {
		if attachedBoxPairValid(msg, msg.SenderPubKey) {
			s.addKnownBoxKey(msg.Sender, msg.SenderBoxKey)
			s.addKnownBoxSig(msg.Sender, msg.SenderBoxSig)
			imported = true
		} else {
			log.Warn().
				Str("node", s.identity.Address).
				Str("id", string(msg.ID)).
				Str("sender", msg.Sender).
				Msg("attached_sender_boxkey_binding_rejected")
		}
	}
	if imported {
		s.notifyIdentityKeysImported(msg.Sender)
		log.Info().
			Str("node", s.identity.Address).
			Str("id", string(msg.ID)).
			Str("sender", msg.Sender).
			Msg("attached_sender_keys_imported")
	}
}

// emitLocalChange offers event to every registered local-change inbox.
//
// The offers run UNDER gossipMu.RLock, which is the fence that lets the
// cancel returned by SubscribeLocalChanges close its channel: the closer
// takes gossipMu with write intent, so it cannot run while a publisher is
// inside an offer. A snapshot taken under the lock and used after it is
// released is not a licence to send — the entry it names may be closed by
// then, and the send panics.
//
// Holding a domain mutex over the offers does not break the "no I/O under a
// lock" rule: every offer is non-blocking, so the section is bounded by the
// number of subscribers and contains no call that can re-enter Service. The
// warn line is the side effect and stays outside, per the cross-domain rule
// in docs/locking.md.
func (s *Service) emitLocalChange(event protocol.LocalChangeEvent) {
	dropped := s.offerLocalChangeToSubscribers(event)
	if dropped == 0 {
		return
	}
	log.Warn().
		Str("type", string(event.Type)).
		Str("message_id", event.MessageID).
		Int("dropped_subscribers", dropped).
		Msg("local change event dropped (channel full)")
}

// offerLocalChangeToSubscribers performs the fenced offers and reports how
// many inboxes were full. It takes s.gossipMu itself — the name carries no
// *Locked suffix for exactly that reason — and exists as its own function so
// the lock is held for the offers and for nothing else.
func (s *Service) offerLocalChangeToSubscribers(event protocol.LocalChangeEvent) int {
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()

	dropped := 0
	for ch := range s.events {
		select {
		case ch <- event:
		default:
			dropped++
		}
	}
	return dropped
}

func (s *Service) trustContact(address, pubKey, boxKey, boxSig, source string) {
	if address == "" || pubKey == "" || boxKey == "" || boxSig == "" {
		return
	}
	// Relay-only contact-plane opt-out: a re-imported SELF contact would
	// persist our box key back into the trust store (and would otherwise
	// trip a spurious errTrustConflict against the keyless self row
	// seeded at startup). See suppressesSelfBoxKey.
	if s.suppressesSelfBoxKey(address) {
		return
	}

	if err := identity.VerifyBoxKeyBinding(address, pubKey, boxKey, boxSig); err != nil {
		return
	}

	// Register the identity (and fire the first-sight IdentityAdded event)
	// BEFORE entering the mutation mutex: ebus sync subscribers run inline
	// in this goroutine, and one that re-enters a trust mutation path
	// would self-deadlock on trustMutationMu. Known-set membership carries
	// no trust semantics (see known_identities.go), so registering ahead
	// of remember() is safe even if remember later fails. This must also
	// stay ahead of Pin for the event's sake: Pin pre-adds the raw
	// address, and when it equals identity.String() a later
	// addKnownIdentity would see the identity as already known and
	// suppress the event.
	identityFingerprint := domain.PeerIdentityFromWire(address)
	s.addKnownIdentity(identityFingerprint)

	// trustMutationMu makes {remember, Pin} atomic against
	// deleteTrustedContactFrame's {forget, Unpin} — see the field doc.
	s.trustMutationMu.Lock()
	before := s.trust.trustedContacts()
	_, existed := before[address]

	stored, rememberErr := s.trust.remember(trustedContact{
		Address:      address,
		PubKey:       pubKey,
		BoxKey:       boxKey,
		BoxSignature: boxSig,
		Source:       source,
	})
	if !stored {
		s.trustMutationMu.Unlock()
		if errors.Is(rememberErr, errTrustConflict) {
			log.Warn().Str("address", address).Str("source", source).Msg("trust conflict")
		} else if rememberErr != nil {
			log.Warn().Err(rememberErr).Str("address", address).Str("source", source).Msg("trust store persist failed on conflict path")
		}
		return
	}

	// Pin the raw address: a newly trusted contact's key knowledge must be
	// exempt from LRU eviction, same as the trust-store seeds in NewService.
	// Keyed to stored, NOT to rememberErr == nil: the contact is in the
	// LIVE store even when only the disk persist failed, and the pinned
	// set mirrors live trust state (symmetric with the Unpin in
	// deleteTrustedContactFrame). ONLY {remember, Pin} lives under
	// trustMutationMu — everything that can publish ebus events
	// (addKnownIdentity above, PublishContactAdded below) stays outside it.
	s.knowledgeMu.Lock()
	s.known.Pin(address)
	s.knowledgeMu.Unlock()
	s.trustMutationMu.Unlock()
	if rememberErr != nil {
		// Contact is live-trusted this session but its row did not reach
		// disk; it will be missing after a restart unless re-imported.
		log.Warn().Err(rememberErr).Str("address", address).Str("source", source).Msg("trust store persist failed; contact trusted in-memory only")
	}

	s.addKnownBoxKey(address, boxKey)
	s.addKnownPubKey(address, pubKey)
	s.notifyIdentityKeysImported(address)
	if !existed {
		log.Info().Str("address", address).Str("source", source).Msg("trusted new contact")
	}

	ebus.PublishContactAdded(s.eventBus, ebus.ContactAddedEvent{
		Address: identityFingerprint,
		PubKey:  domain.PeerPublicKey(pubKey),
		BoxKey:  domain.PeerBoxKey(boxKey),
		BoxSig:  domain.PeerBoxSignature(boxSig),
	})
}

// isVerifiedSender checks whether the given sender address corresponds to
// a known, cryptographically authenticated identity. The sender is accepted
// when any of these conditions is true:
//
//  1. sender is this node's own identity (local authorship)
//  2. sender matches the relay peer's authenticated identity (direct authorship)
//  3. sender has a registered public key in s.pubKeys (previously authenticated
//     through identity exchange — hello/welcome, fetch_contacts, or trust store)
//
// This prevents arbitrary sender strings from entering the message store
// and poisoning s.known. For DM messages, storeIncomingMessage enforces
// VerifyEnvelope independently, so this gate targets non-DM topics only.
func (s *Service) isVerifiedSender(sender string, relayPeerIdentity domain.PeerIdentity) bool {
	if sender == s.identity.Address {
		return true
	}
	if !relayPeerIdentity.IsZero() && sender == relayPeerIdentity.String() {
		return true
	}
	s.knowledgeMu.RLock()
	_, hasPubKey := s.pubKeys[sender]
	s.knowledgeMu.RUnlock()
	return hasPubKey
}

// handleInboundPushMessage processes a push_message frame received on an
// authenticated inbound TCP connection. Two delivery paths converge here:
//
//  1. Backlog push — remote peer replays stored messages for this node's
//     identity at auth time (registerHelloRoute backlog replay).
//  2. Gossip push — remote peer forwards a message as part of epidemic
//     dissemination (sender ≠ relay peer, same as Bitcoin's tx relay).
//
// Sender spoofing protection:
//   - DM messages: VerifyEnvelope validates the cryptographic signature
//     against the sender's public key — spoofing is impossible without
//     the private key.
//   - Non-DM messages: the sender must be a verified identity — either
//     the relay peer itself, this node, or a peer whose public key was
//     previously exchanged through the identity protocol. Unverified
//     senders are rejected and the relay peer's ban score is incremented.
func (s *Service) handleInboundPushMessage(connID domain.ConnID, frame protocol.Frame) {
	if frame.Item == nil {
		return
	}

	msg, err := incomingMessageFromFrame(protocol.Frame{
		ID:         frame.Item.ID,
		Topic:      frame.Topic,
		Address:    frame.Item.Sender,
		Recipient:  frame.Item.Recipient,
		Flag:       frame.Item.Flag,
		CreatedAt:  frame.Item.CreatedAt,
		TTLSeconds: frame.Item.TTLSeconds,
		Hops:       frame.Item.Hops,
		Body:       frame.Item.Body,
		// Attached PUBLIC sender keys ride the top-level frame fields
		// (see attachKnownSenderKeys); validated on import.
		PubKey: frame.PubKey,
		BoxKey: frame.BoxKey,
		BoxSig: frame.BoxSig,
	})
	if err != nil {
		return
	}

	peerAddr := s.inboundPeerAddress(connID)
	peerIdentity := s.inboundPeerIdentity(connID)
	// Ingress link for hop accounting + echo suppression
	// (transit_retention.go).
	msg.Via = peerAddr
	msg.ViaIdentity = peerIdentity

	// Non-DM sender verification: reject messages whose sender is not a
	// known identity. DM messages — both data DM ("dm") and control DM
	// (TopicControlDM) — have their own cryptographic verification in
	// storeIncomingMessage (VerifyEnvelope), so this gate targets only
	// topics where no per-message signature exists.
	if msg.Topic != "dm" && msg.Topic != protocol.TopicControlDM && !s.isVerifiedSender(msg.Sender, peerIdentity) {
		log.Warn().
			Str("node", s.identity.Address).
			Str("peer", string(peerAddr)).
			Str("relay_identity", peerIdentity.String()).
			Str("id", string(msg.ID)).
			Str("sender", msg.Sender).
			Str("topic", msg.Topic).
			Msg("push_message rejected: non-DM sender identity not verified")
		s.addBanScore(connID, banIncrementInvalidSig)
		return
	}

	stored, _, errCode := s.storeIncomingMessage(msg, true)
	if !stored && errCode == protocol.ErrCodeUnknownSenderKey {
		if peerAddr != "" {
			// Legacy keyless frame — schedule a BACKGROUND single-flight
			// contact sync (narrow recovery: contact/key sync only, no
			// peer exchange — see docs/peer-discovery-conditional-get-
			// peers.ru.md Step 5) and reject this attempt. Running the
			// recovery inline blocked the inbound read loop for the
			// whole dial while frames piled up behind it; the async
			// trigger keeps the loop responsive. No ack_delete goes out
			// for this result, so the pushing hop redelivers and
			// succeeds once the keys are imported.
			log.Info().
				Str("peer", string(peerAddr)).
				Str("id", string(msg.ID)).
				Str("sender", msg.Sender).
				Str("recipient", msg.Recipient).
				Msg("push_message_key_sync_scheduled")
			// Observability: keep this recovery visible in the
			// peer_exchange_skipped stream under its own path label,
			// exactly as the previous inline recovery did.
			s.logPeerExchangeSkipped(peerExchangePathUnknownSenderRecovery, peerAddr, peerExchangeSkipByNarrowRecovery)
			// activePeerSession resolves fallback-port aliases
			// (resolveSessionLocked) and checks health — a bare
			// s.sessions[peerAddr] lookup would miss a session stored
			// under the alias address.
			ownedSession, _ := s.activePeerSession(peerAddr)
			s.triggerSenderKeySyncAsync(peerAddr, msg.Sender, ownedSession)
		}
	}
	if shouldAckOnStoreResult(stored, errCode) && msg.Topic == "dm" {
		// Ack-delete on stored=true OR on the dedup branch (stored=false
		// && errCode==""): both outcomes mean "this hop has the message —
		// release the per-hop push/backlog resource". This is backlog
		// cleanup between hops, NOT the end-to-end delivery confirmation:
		// the sender's retry stops only on the delivered/seen receipt.
		// Without the dedup arm a duplicate push_message would never be
		// acknowledged and the pushing hop would loop the same id forever,
		// which is one of the reconnect-storm amplifiers. errCode!=""
		// leaves the peer to re-attempt once it addresses the underlying
		// failure (unknown_sender_key triggers a sync upstream; other
		// codes surface in the warn log).
		//
		// Prefer the outbound session for ack_delete (single write queue,
		// no interleaving risk). Fall back to the inbound conn when no
		// outbound session exists — this is the fix for the case where the
		// remote peer connected to us but we haven't dialed them back.
		if session := s.peerSession(peerAddr); session != nil && session.authOK {
			s.sendAckDeleteToPeer(peerAddr, "dm", msg.ID, "")
		} else {
			s.sendAckDeleteByID(connID, "dm", msg.ID, "")
		}
		if !stored {
			log.Debug().Str("node", s.identity.Address).Str("peer", string(peerAddr)).Str("id", string(msg.ID)).Msg("push_message_dedup_acked")
		}
	} else if !stored {
		log.Warn().Str("node", s.identity.Address).Str("peer", string(peerAddr)).Str("relay_identity", peerIdentity.String()).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Str("err_code", errCode).Msg("push_message_store_failed")
	}
	log.Info().Str("node", s.identity.Address).Str("peer", string(peerAddr)).Str("relay_identity", peerIdentity.String()).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Str("topic", msg.Topic).Bool("stored", stored).Msg("received pushed message (inbound)")
}

// handleInboundPushDeliveryReceipt processes a push_delivery_receipt frame
// received on an inbound connection. This happens when the remote peer
// pushes delivery receipts destined for this node's identity (auth-time
// backlog replay or live push).
//
// Identity binding: the receipt's Recipient (the DM sender who should receive
// the delivery confirmation) must match either our own identity or an identity
// we actively subscribe to via the inbound peer. Without this check an
// authenticated peer could push a receipt with arbitrary Sender/Recipient and
// corrupt the delivery state for a conversation it does not participate in.
func (s *Service) handleInboundPushDeliveryReceipt(connID domain.ConnID, frame protocol.Frame) {
	if frame.Receipt == nil {
		return
	}
	receipt, err := receiptFromReceiptFrame(*frame.Receipt)
	if err != nil {
		return
	}

	peerAddr := s.inboundPeerAddress(connID)

	// Identity gate: accept only receipts whose Recipient matches our own
	// identity or an identity with an active inbound subscriber (full-node
	// relay holding receipts for connected clients).
	if receipt.Recipient != s.identity.Address && !s.hasSubscriber(receipt.Recipient) {
		log.Warn().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("receipt_recipient", receipt.Recipient).
			Str("local_identity", s.identity.Address).
			Msg("push_delivery_receipt rejected: recipient does not match local identity or active subscriber")
		s.addBanScore(connID, banIncrementInvalidSig)
		return
	}

	s.storeDeliveryReceipt(receipt)
	if session := s.peerSession(peerAddr); session != nil && session.authOK {
		s.sendAckDeleteToPeer(peerAddr, "receipt", receipt.MessageID, receipt.Status)
	} else {
		s.sendAckDeleteByID(connID, "receipt", receipt.MessageID, receipt.Status)
	}
	log.Info().Str("peer", string(peerAddr)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received pushed delivery receipt (inbound)")
}

// handleInboundRelayDeliveryReceipt processes a relay_delivery_receipt frame
// received on an inbound TCP connection. This is the gossip receipt path:
// the remote peer forwards a receipt using flat Frame fields (ID, Address,
// Recipient, Status, DeliveredAt) rather than the nested ReceiptFrame used
// by push_delivery_receipt.
//
// This command is intentionally named differently from the local-only
// "send_delivery_receipt" to enforce command-isolation: local RPC commands
// must never be callable from the P2P wire (see docs/command-isolation.md).
//
// Identity binding: the receipt's Recipient must match this node's identity
// or an active subscriber for local delivery. If neither matches, the node
// checks relay state — a transit node may need to forward this receipt one
// hop back along the relay chain. When no relay path exists, the receipt
// is broadcast via gossipReceipt as a last resort.
//
// Deduplication: transit receipts are recorded in seenReceipts only after
// confirming that delivery can proceed (relay chain forwarded successfully,
// or gossip has usable routing targets). If neither path is available on
// the first observation, the receipt is NOT marked — allowing a later
// arrival of the same receipt to retry after routes recover, instead of
// being permanently suppressed.
//
// No ban scoring is applied at this handler level. Malformed frames are
// silently dropped (consistent with push_delivery_receipt and other receipt
// handlers). Non-local receipts are legitimate hop-by-hop transit traffic
// and must not penalise the sending peer.
func (s *Service) handleInboundRelayDeliveryReceipt(connID domain.ConnID, frame protocol.Frame) {
	receipt, err := receiptFromFrame(frame)
	if err != nil {
		return
	}

	peerAddr := s.inboundPeerAddress(connID)

	// Fast path: receipt is addressed to this node or an active subscriber.
	if receipt.Recipient == s.identity.Address || s.hasSubscriber(receipt.Recipient) {
		s.storeDeliveryReceipt(receipt)
		log.Info().Str("peer", string(peerAddr)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received relay_delivery_receipt (inbound)")
		return
	}

	// Fast-path dedupe: read-only check suppresses receipts that were already
	// successfully delivered on a prior arrival. Does not mark — marking only
	// happens after confirmed delivery (relay success or gossip-with-targets).
	if s.isTransitReceiptSeen(receipt) {
		log.Debug().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("recipient", receipt.Recipient).
			Msg("relay_delivery_receipt dropped: duplicate transit receipt (inbound)")
		return
	}

	// Transit path: attempt to forward the receipt along the relay chain.
	// On success, mark as seen to suppress duplicates. On failure, fall back
	// to gossip — consistent with the contract in handleRelayReceipt
	// ("caller is responsible for gossip fallback") and the pattern in
	// retryRelayDeliveries.
	if s.handleRelayReceipt(receipt) {
		s.markTransitReceiptSeen(receipt)
		log.Info().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("recipient", receipt.Recipient).
			Msg("relay_delivery_receipt forwarded via relay chain")
		return
	}

	// Gossip fallback: no reverse relay path or send failed — broadcast
	// the receipt to routing targets so it can still reach the sender.
	// Pre-mark so rapid-fire duplicate receipts from the same peer are
	// suppressed. gossipTransitReceipt unmarks on complete failure to
	// preserve retry eligibility.
	if s.markTransitReceiptSeen(receipt) {
		log.Debug().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("recipient", receipt.Recipient).
			Msg("relay_delivery_receipt dropped: duplicate transit receipt pre-gossip (inbound)")
		return
	}
	s.gossipTransitReceipt(receipt)
	log.Debug().
		Str("peer", string(peerAddr)).
		Str("message_id", string(receipt.MessageID)).
		Str("receipt_recipient", receipt.Recipient).
		Str("local_identity", s.identity.Address).
		Msg("relay_delivery_receipt gossip fallback: no relay path or send failed")
}

// handleInboundPushNotice processes a push_notice frame received on an
// authenticated P2P connection (inbound TCP or outbound session). The remote
// peer gossips an encrypted notice; we store it locally and re-gossip to our
// own routing targets if new. Deduplication via notice ID prevents infinite
// loops.
func (s *Service) handleInboundPushNotice(frame protocol.Frame) {
	ttl := time.Duration(frame.TTLSeconds) * time.Second
	if ttl <= 0 || strings.TrimSpace(frame.Ciphertext) == "" {
		return
	}

	s.cleanupExpiredNotices()

	id := gazeta.ID(frame.Ciphertext)
	expiresAt := time.Now().UTC().Add(ttl)

	log.Trace().Str("site", "handleInboundPushNotice").Str("phase", "lock_wait").Str("notice_id", id).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "handleInboundPushNotice").Str("phase", "lock_held").Str("notice_id", id).Msg("gossipMu_writer")
	if existing, ok := s.notices[id]; ok && existing.ExpiresAt.After(time.Now().UTC()) {
		s.gossipMu.Unlock()
		log.Trace().Str("site", "handleInboundPushNotice").Str("phase", "lock_released_dup").Str("notice_id", id).Msg("gossipMu_writer")
		return
	}
	s.notices[id] = gazeta.Notice{
		ID:         id,
		Ciphertext: frame.Ciphertext,
		ExpiresAt:  expiresAt,
	}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "handleInboundPushNotice").Str("phase", "lock_released").Str("notice_id", id).Msg("gossipMu_writer")

	if s.CanForward() {
		s.goBackground(func() { s.gossipNotice(ttl, frame.Ciphertext) })
	}
}

func expectedReplyType(requestType string) string {
	switch requestType {
	case "send_message":
		return ""
	case "publish_notice":
		return ""
	default:
		return ""
	}
}

// isFireAndForgetFrame returns true for frame types that should be written to
// the peer session without waiting for a response. These frames are delivered
// on a best-effort basis; the remote side may send an ack asynchronously, but
// the sender must not block the session waiting for it.
//
// push_message, push_notice, and relay_delivery_receipt are fire-and-forget
// because the gossip path writes them via enqueuePeerFrame → sendCh, and
// the remote dispatcher stores the payload without writing a response frame.
// Blocking the session on a reply that never comes would stall gossip delivery.
//
// announce_routes / routes_update / request_resync are the announce-plane
// trio: announce_routes is the legacy v1 baseline / forced-full / delta
// frame, routes_update is the v2 incremental delta, request_resync is the
// v2 control frame asking the peer to clear its announce state. None of
// the three has a response frame — receivers update local state and stay
// silent on the wire — so all three must skip the peerSessionRequest
// reply-wait. Without classifying routes_update and request_resync here,
// the session dequeue loop falls through to expectedReplyType("") and
// blocks reading the next inbound frame as the "reply", which either
// stalls the session for the read deadline or consumes an unrelated
// inbound frame meant for the dispatcher.
//
// route_sync_digest_v1 / route_sync_summary_v1 are the Phase 3 incremental
// reconnect-sync pair, and they are ASYMMETRICALLY one-way: emitting a
// digest does not yield a synchronous reply on the session — the peer's
// route_sync_summary_v1 arrives later as its own unsolicited inbound frame
// and is routed to handleRouteSyncSummary by the dispatcher. If the digest
// were not fire-and-forget the session send path would route it through
// peerSessionRequest and, with expectedReplyType("") , consume the first
// subsequent inbound frame (often the very summary, or an unrelated frame)
// as the "reply" and drop it — leaving the reconnect optimisation inert on
// the outbound path. Both directions are therefore fire-and-forget; the
// session loop additionally dispatches an inbound route_sync frame that
// lands mid-peerSessionRequest (see peer_management.go).
//
// datagram is fire-and-forget by the layer's own definition: the transport is
// best-effort and a datagram has no synchronous reply on the session
// (docs/refactoring/datagram-transport.md §2). An answer to a request-mode
// datagram, where one exists at all, arrives later as its own unsolicited
// inbound frame. Routing it through peerSessionRequest would make the session
// wait with expectedReplyType("") and swallow whatever unrelated frame came
// next as the "reply".
func isFireAndForgetFrame(frameType string) bool {
	switch frameType {
	case "announce_routes", "routes_update", "request_resync",
		protocol.RouteAnnounceV3FrameType, protocol.RoutePoisonFrameType,
		"push_message", "push_notice", "relay_delivery_receipt",
		protocol.RouteSyncDigestFrameType, protocol.RouteSyncSummaryFrameType,
		protocol.DatagramFrameType:
		return true
	default:
		return isRelayFrame(frameType) || frameType == protocol.FileCommandFrameType
	}
}

// heartbeatInterval is the base interval between ping/pong heartbeats.
// Mirrors Bitcoin's 2-minute PING_INTERVAL to reduce idle chatter while
// keeping the connection provably alive.
const heartbeatInterval = 2 * time.Minute

// pongStallTimeout is the maximum time to wait for a pong reply before
// declaring the peer stalled and tearing down the session.
const pongStallTimeout = 45 * time.Second

func (s *Service) subscribersForRecipient(recipient string) []*subscriber {
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()

	group := s.subs[recipient]
	subs := make([]*subscriber, 0, len(group))
	for _, sub := range group {
		subs = append(subs, sub)
	}
	return subs
}

// hasSubscriber returns true if at least one active subscriber exists for
// the given recipient identity. Used by push_delivery_receipt identity
// binding to allow a full-node relay to accept receipts for identities it
// serves (i.e., identities with active subscriber registrations).
func (s *Service) hasSubscriber(recipient string) bool {
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()
	return len(s.subs[recipient]) > 0
}

func (s *Service) writePushFrame(sub *subscriber, frame protocol.Frame) {
	defer crashlog.DeferRecover()

	// Resolve the owning connection through the ConnID registry. If the
	// subscriber's connection has already been unregistered, drop the
	// subscriber and bail out — the message is safe in s.topics and will be
	// delivered through backlog replay on the peer's next authentication.
	core := s.netCoreForID(sub.connID)
	if core == nil {
		s.removeSubscriberByID(sub.recipient, sub.id)
		return
	}

	log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", core.RemoteAddr()).
		Str("direction", "send").
		Str("command", frame.Type).
		Bool("accepted", true).
		Msg("protocol_trace")

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		// Caller-side marshal bug — the subscriber is innocent. Leave it
		// in place and let the next push attempt try again with a fresh
		// frame. This matches the legacy split: marshal failure is NOT a
		// transport drop, so it must NOT trigger removeSubscriberByID.
		// The raw-bytes helper below has no marshal-fallback path
		// precisely so this branch stays caller-local.
		return
	}
	// Network-routed push through the injected Network surface: full
	// outcome tree surfaces through the return. Any non-nil —
	// ErrUnregisteredWrite, ErrSendBufferFull (post-eviction),
	// ErrSendWriterDone, ErrSendChanClosed,
	// ctx-error, or unknown sentinel — means "frame did not reach the peer",
	// which preserves the legacy `!= enqueueSent → remove` union test
	// without enumerating each sentinel. ctx is Service lifecycle.
	if err := s.sendFrameBytesViaNetwork(s.runCtx, sub.connID, []byte(line)); err != nil {
		s.removeSubscriberByID(sub.recipient, sub.id)
	}
}

// dropReceiptBacklogForOfflineRecipients reclaims the in-memory delivery-receipt
// backlog for recipients whose LAST subscription was just removed (peer
// disconnected / transport dead). An unsubscribed recipient is offline, and the
// node is deliberately NOT a store-and-forward mailbox: the durable delivery
// guarantee is the original sender's end-to-end retry (chatlog-backed), which
// re-delivers when the recipient returns. Holding the backlog for an offline
// recipient only leaks memory — its s.receipts key is otherwise reclaimed only
// by an ack_delete that will never arrive once the recipient is gone. The
// dedup/retry shadows are cleared too, so a future re-send is not silently
// suppressed and no relayRetry orphan accumulates (same hygiene as the cap
// eviction). Own identity is never passed here (it is not a peer subscription;
// its backlog is bounded separately by the unsolicited gate + per-recipient cap).
//
// Caller MUST NOT hold gossipMu: this takes deliveryMu OUTER and, nested,
// gossipMu.RLock INNER across the whole check+drop loop, and the canonical
// order is deliveryMu → gossipMu, so the drop must run AFTER the caller's
// gossipMu section — entering with gossipMu held would be the forbidden
// reverse edge.
func (s *Service) dropReceiptBacklogForOfflineRecipients(recipients []string) {
	if len(recipients) == 0 {
		return
	}
	s.deliveryMu.Lock()
	// Hold gossipMu.RLock across the WHOLE check+drop loop, not just a momentary
	// hasSubscriber probe. The caller released gossipMu before calling us, so a
	// concurrent reconnect (registerHelloRoute) may re-subscribe a recipient in
	// the gap; if we only sampled subscriber-presence and then dropped, a
	// reconnect landing between the sample and the delete would wipe a freshly
	// online subscriber's backlog. Keeping gossipMu.RLock held for the duration
	// makes "still offline?" and the delete atomic against s.subs writers
	// (registerHelloRoute takes gossipMu.Lock and so cannot interleave). The
	// nesting is deliveryMu OUTER → gossipMu INNER — the canonical order — so it
	// is deadlock-free; released LIFO below. The body does only in-memory work
	// (no I/O, no callbacks), so the held window stays short.
	s.gossipMu.RLock()
	for _, recipient := range recipients {
		if recipient == s.identity.Address {
			continue
		}
		if len(s.subs[recipient]) > 0 {
			// Reconnected while we held the locks — keep the backlog.
			continue
		}
		list := s.receipts[recipient]
		if len(list) == 0 {
			continue
		}
		for i := range list {
			ev := list[i]
			s.seenReceipts.Delete(ev.Recipient + ":" + string(ev.MessageID) + ":" + ev.Status)
			delete(s.relayRetry, relayReceiptKey(ev))
		}
		delete(s.receipts, recipient)
	}
	s.gossipMu.RUnlock()
	s.deliveryMu.Unlock()
}

// removeSubscriberConnID removes every subscriber owned by the given
// connection. The lifecycle caller (handleConn teardown defer) resolves the
// ConnID once up-front because removeSubscriberConnID runs after
// unregisterInboundConn has already stripped the conn→ID mapping.
func (s *Service) removeSubscriberConnID(connID domain.ConnID) {
	if connID == 0 {
		return
	}
	log.Trace().Str("site", "removeSubscriberConnID").Str("phase", "lock_wait").Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "removeSubscriberConnID").Str("phase", "lock_held").Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	var orphaned []string
	for recipient, group := range s.subs {
		for id, sub := range group {
			if sub != nil && sub.connID == connID {
				delete(group, id)
			}
		}
		if len(group) == 0 {
			delete(s.subs, recipient)
			orphaned = append(orphaned, recipient)
		}
	}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "removeSubscriberConnID").Str("phase", "lock_released").Uint64("conn_id", uint64(connID)).Msg("gossipMu_writer")
	// Recipients that lost their last subscription are offline now — drop their
	// receipt backlog. Separate lock section (deliveryMu → gossipMu order).
	s.dropReceiptBacklogForOfflineRecipients(orphaned)
}

func (s *Service) removeSubscriberByID(recipient, id string) {
	log.Trace().Str("site", "removeSubscriberByID").Str("phase", "lock_wait").Str("recipient", recipient).Str("sub_id", id).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "removeSubscriberByID").Str("phase", "lock_held").Str("recipient", recipient).Str("sub_id", id).Msg("gossipMu_writer")

	group := s.subs[recipient]
	if group == nil {
		s.gossipMu.Unlock()
		log.Trace().Str("site", "removeSubscriberByID").Str("phase", "lock_released").Str("recipient", recipient).Str("sub_id", id).Msg("gossipMu_writer")
		return
	}
	delete(group, id)
	orphaned := false
	if len(group) == 0 {
		delete(s.subs, recipient)
		orphaned = true
	}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "removeSubscriberByID").Str("phase", "lock_released").Str("recipient", recipient).Str("sub_id", id).Msg("gossipMu_writer")
	if orphaned {
		// Last subscription gone — recipient is offline, drop its receipt
		// backlog (separate section: deliveryMu → gossipMu order).
		s.dropReceiptBacklogForOfflineRecipients([]string{recipient})
	}
}

func (s *Service) cleanupExpiredNotices() {
	now := time.Now().UTC()

	log.Trace().Str("site", "cleanupExpiredNotices").Str("phase", "lock_wait").Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "cleanupExpiredNotices").Str("phase", "lock_held").Msg("gossipMu_writer")
	defer func() {
		s.gossipMu.Unlock()
		log.Trace().Str("site", "cleanupExpiredNotices").Str("phase", "lock_released").Msg("gossipMu_writer")
	}()

	for id, notice := range s.notices {
		if !notice.ExpiresAt.After(now) {
			delete(s.notices, id)
		}
	}
}

// expiredCleanupThrottle is the minimum interval between full-scan cleanup
// runs. Inline callers (storeIncomingMessage, fetch*) skip the scan when
// bootstrapLoop already ran it recently. This prevents the Lock()-guarded
// full iteration from becoming a writer-starvation bottleneck on the hot
// relay_message path.
const expiredCleanupThrottle = 10 * time.Second

// cleanupExpiredMessages removes TTL-expired envelopes from s.topics.
// Used only by storeIncomingMessage (hot path during message bursts) —
// throttled so repeated calls within expiredCleanupThrottle are no-ops.
// Query paths (fetchMessagesFrame, fetchInboxFrame, etc.) and the
// bootstrapLoop tick call cleanupExpiredMessagesForce directly to
// guarantee callers always see accurate data.
func (s *Service) cleanupExpiredMessages() {
	now := time.Now().UTC()

	// Fast-path: check throttle under read lock to avoid the write lock
	// entirely when the last scan was recent.
	s.gossipMu.RLock()
	if now.Sub(s.lastExpiredCleanup) < expiredCleanupThrottle {
		s.gossipMu.RUnlock()
		return
	}
	s.gossipMu.RUnlock()

	s.cleanupExpiredMessagesForce()
}

// cleanupExpiredMessagesForce unconditionally scans and removes expired
// messages. Used by bootstrapLoop where throttling is already handled by
// the tick interval.
//
// Two-phase approach avoids holding s.peerMu.Lock() during the full scan:
//
//	Phase 1 (RLock): collect expired message IDs — read-only, concurrent
//	                  with other readers.
//	Phase 2 (Lock):  rebuild only affected topic slices — short critical
//	                  section proportional to expired count, not total count.
func (s *Service) cleanupExpiredMessagesForce() {
	now := time.Now().UTC()
	drift := s.effectiveClockDrift()

	// Phase 1: collect expired IDs under read lock.
	type expiredEntry struct {
		topic string
		id    string
	}
	var expired []expiredEntry

	s.gossipMu.RLock()
	for topic, messages := range s.topics {
		for _, message := range messages {
			if message.Flag == protocol.MessageFlagAutoDeleteTTL && message.TTLSeconds > 0 {
				expiresAt := message.CreatedAt.Add(time.Duration(message.TTLSeconds) * time.Second)
				if !expiresAt.After(now) {
					expired = append(expired, expiredEntry{topic, string(message.ID)})
					continue
				}
			}
			// Absolute age ceiling (envelope_retention.go): drop any
			// envelope past its class MaxAge, anchored on the IMMUTABLE
			// sender CreatedAt. This is what re-injection cannot reset —
			// unlike the StoredAt-anchored transit window below — so a
			// re-circulated months-old transit DM finally dies, and
			// broadcast/global topics (which had NO age bound at all)
			// are bounded too. Local classes carry MaxAge=0 (lifetime
			// owned by chatlog / the sender engine) and are unaffected.
			pol := s.envelopeRetentionPolicy(topic, message.Sender, message.Recipient)
			if envelopeAgeExceeded(message.CreatedAt, now, pol.MaxAge, drift) {
				expired = append(expired, expiredEntry{topic, string(message.ID)})
				continue
			}
			// Forwarding-only transit (transit_retention.go): a
			// transit envelope (neither party is this node) is the
			// in-flight buffer of its forwarding operation, dropped
			// unconditionally once transitInFlightWindow closes —
			// relays do NOT store user messages. This StoredAt-anchored
			// window still bounds the SHORT forwarding op; the CreatedAt
			// ceiling above is the re-injection-proof global cap.
			if s.isTransitEnvelope(message) && transitExpired(message, now) {
				expired = append(expired, expiredEntry{topic, string(message.ID)})
			}
		}
	}
	s.gossipMu.RUnlock()

	// Short-circuit: update throttle timestamp and return.
	if len(expired) == 0 {
		log.Trace().Str("site", "cleanupExpiredMessagesForce_empty").Str("phase", "lock_wait").Msg("gossipMu_writer")
		s.gossipMu.Lock()
		log.Trace().Str("site", "cleanupExpiredMessagesForce_empty").Str("phase", "lock_held").Msg("gossipMu_writer")
		s.lastExpiredCleanup = now
		s.gossipMu.Unlock()
		log.Trace().Str("site", "cleanupExpiredMessagesForce_empty").Str("phase", "lock_released").Msg("gossipMu_writer")
		return
	}

	// Phase 2: remove collected entries under write lock. The critical
	// section touches only topics that have expired messages — all other
	// topics are untouched.
	expiredIDs := make(map[string]map[string]struct{}, len(expired))
	for _, e := range expired {
		if expiredIDs[e.topic] == nil {
			expiredIDs[e.topic] = make(map[string]struct{})
		}
		expiredIDs[e.topic][e.id] = struct{}{}
	}

	log.Trace().Str("site", "cleanupExpiredMessagesForce").Str("phase", "lock_wait").Int("expired", len(expired)).Msg("gossipMu_writer")
	s.gossipMu.Lock()
	log.Trace().Str("site", "cleanupExpiredMessagesForce").Str("phase", "lock_held").Int("expired", len(expired)).Msg("gossipMu_writer")
	s.lastExpiredCleanup = now

	for topic, ids := range expiredIDs {
		messages := s.topics[topic]
		filtered := messages[:0]
		for _, msg := range messages {
			if _, drop := ids[string(msg.ID)]; drop {
				log.Debug().Str("node", s.identity.Address).Str("topic", topic).Str("id", string(msg.ID)).Msg("cleanupExpiredMessages: removing expired")
				continue
			}
			filtered = append(filtered, msg)
		}
		// Release dropped Envelopes left in the shared backing array —
		// without this the removed payloads stay GC-reachable until
		// future appends overwrite the tail, which can be never on a
		// quiet topic. Defeats the whole point of the retention sweep.
		clear(messages[len(filtered):])
		if len(filtered) == 0 {
			delete(s.topics, topic)
		} else {
			s.topics[topic] = filtered
		}
	}
	s.gossipMu.Unlock()
	log.Trace().Str("site", "cleanupExpiredMessagesForce").Str("phase", "lock_released").Msg("gossipMu_writer")

	// Orphan prevention (transit_retention.go): retryableRelayMessages
	// iterates the TOPIC snapshot, so the relayRetry entry of a removed
	// envelope is never visited again and its lazy TTL delete never
	// fires. Sequential lock use (gossipMu released above, deliveryMu
	// inside) keeps the canonical deliveryMu → gossipMu order intact.
	removedIDs := make([]protocol.MessageID, 0, len(expired))
	for _, e := range expired {
		removedIDs = append(removedIDs, protocol.MessageID(e.id))
	}
	s.dropRelayRetryEntries(removedIDs)
	s.forgetRelayDelivered(removedIDs)
}

func (s *Service) validateMessageTiming(msg incomingMessage) error {
	now := time.Now().UTC()
	drift := s.effectiveClockDrift()

	if msg.CreatedAt.After(now.Add(drift)) {
		return fmt.Errorf("message timestamp %s outside allowed future drift %s", msg.CreatedAt.Format(time.RFC3339), drift)
	}

	if (msg.Topic == "dm" || msg.Topic == protocol.TopicControlDM) && msg.Recipient != "" && msg.Recipient != "*" {
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
			return fmt.Errorf("message timestamp %s expired for delivery", msg.CreatedAt.Format(time.RFC3339))
		}
		return nil
	}

	if msg.CreatedAt.Before(now.Add(-drift)) {
		return fmt.Errorf("message timestamp %s outside allowed drift %s", msg.CreatedAt.Format(time.RFC3339), drift)
	}

	return nil
}

func (s *Service) messageDeliveryExpired(createdAt time.Time, ttlSeconds int) bool {
	if ttlSeconds <= 0 {
		return false
	}
	expiresAt := createdAt.Add(time.Duration(ttlSeconds) * time.Second)
	return !expiresAt.After(time.Now().UTC())
}

func (s *Service) pendingFrameExpired(frame protocol.Frame, queuedAt time.Time, now time.Time) bool {
	if frame.Type != "send_message" || frame.Topic != "dm" {
		return false
	}
	createdAt, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		return false
	}
	if frame.TTLSeconds <= 0 {
		return false
	}
	expiresAt := createdAt.UTC().Add(time.Duration(frame.TTLSeconds) * time.Second)
	return !expiresAt.After(now)
}

func incomingMessageFromFrame(frame protocol.Frame) (incomingMessage, error) {
	timestamp, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		return incomingMessage{}, fmt.Errorf("parse message timestamp: %w", err)
	}

	msg := incomingMessage{
		ID:         protocol.MessageID(strings.TrimSpace(frame.ID)),
		Topic:      strings.TrimSpace(frame.Topic),
		Sender:     strings.TrimSpace(frame.Address),
		Recipient:  strings.TrimSpace(frame.Recipient),
		Flag:       protocol.MessageFlag(strings.TrimSpace(frame.Flag)),
		CreatedAt:  timestamp.UTC(),
		TTLSeconds: frame.TTLSeconds,
		Hops:       frame.Hops,
		Body:       strings.TrimSpace(frame.Body),
		// Attached PUBLIC sender key material (optional, DM transport
		// frames only) — validated later by importAttachedSenderKeys,
		// never trusted as-is.
		SenderPubKey: strings.TrimSpace(frame.PubKey),
		SenderBoxKey: strings.TrimSpace(frame.BoxKey),
		SenderBoxSig: strings.TrimSpace(frame.BoxSig),
	}

	if msg.Topic == "" || msg.Sender == "" || msg.Recipient == "" || msg.Body == "" || msg.ID == "" || !msg.Flag.Valid() {
		return incomingMessage{}, fmt.Errorf("missing required message field")
	}

	if msg.Flag == protocol.MessageFlagAutoDeleteTTL && msg.TTLSeconds <= 0 {
		return incomingMessage{}, fmt.Errorf("ttl message requires positive ttl_seconds")
	}
	if msg.TTLSeconds < 0 {
		return incomingMessage{}, fmt.Errorf("ttl_seconds must not be negative")
	}

	return msg, nil
}

func messageFrame(msg protocol.Envelope) protocol.MessageFrame {
	// Deliberately NO Hops here: messageFrame serves FINAL-delivery
	// surfaces — fetch_messages / fetch_inbox responses and subscriber
	// push — where the receiver is the endpoint (the recipient pulls
	// its own mail; subscribers do not relay). Stamping a budget here
	// would change the long-standing local/query API contract (clients
	// expect hops absent), and propagation budgets belong exclusively
	// to the mesh fan-out builder, gossipPushFrame. An endpoint that
	// DOES later re-inject a fetched message into the mesh goes
	// through admission, where the absent field correctly reads as
	// "originated here".
	return protocol.MessageFrame{
		ID:         string(msg.ID),
		Sender:     msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       string(msg.Flag),
		CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds: msg.TTLSeconds,
		Body:       string(msg.Payload),
	}
}
