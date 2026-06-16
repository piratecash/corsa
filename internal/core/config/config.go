package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

type App struct {
	Name     string
	Network  string
	Profile  string
	Language string
	Version  string
}

// NodeType is an alias for domain.NodeType kept for backward compatibility
// with existing config consumers. New code should use domain.NodeType directly.
type NodeType = domain.NodeType

const (
	NodeTypeFull   = domain.NodeTypeFull
	NodeTypeClient = domain.NodeTypeClient
)

type Node struct {
	ListenAddress string
	// AdvertisePort is the validated self-reported listening port published
	// in hello.advertise_port. nil means "operator did not supply a valid
	// CORSA_ADVERTISE_PORT" — callers must resolve the runtime value through
	// EffectiveAdvertisePort (falls back to DefaultPeerPort). Explicit
	// *domain.PeerPort models the optional state at the type level instead
	// of reusing 0 as a sentinel, per the project's "zero is not a business
	// signal" rule, and the domain type prevents silent mix-ups with raw
	// ints elsewhere in the config. Any invalid input (empty, non-numeric,
	// out of 1..65535) collapses to nil at env-parse time.
	AdvertisePort    *domain.PeerPort
	BootstrapPeers   []string
	IdentityPath     string
	TrustStorePath   string
	PeersStatePath   string
	ChatLogDir       string // directory for chatlog/state files
	DownloadDir      string // directory for downloaded files (defaults to "<DataDir>/downloads")
	ProxyAddress     string // SOCKS5 proxy for .onion peers (e.g. "127.0.0.1:9050")
	Type             NodeType
	ListenerEnabled  bool
	ListenerSet      bool
	ClientVersion    string
	MaxClockDrift    time.Duration
	MaxOutgoingPeers int
	MaxIncomingPeers int

	// PendingRingSize bounds the per-peer in-memory ring of frames queued
	// for a directly-connected peer that is momentarily offline (env:
	// CORSA_PENDING_RING_SIZE). This is a HARD memory bound: at capacity the
	// oldest queued frame for that peer is evicted to make room for the new
	// one (ring semantics), so RAM stays bounded at roughly PendingRingSize ×
	// connected-peers regardless of churn. Zero (the default) means "use the
	// built-in default" (node.maxPendingFramesPerPeer). The queue is no
	// longer persisted to disk — see the queue-state deprecation in
	// internal/core/node — so this knob only trades RAM for how far back a
	// reconnecting peer can still receive missed frames within the uptime.
	PendingRingSize int
	// DeliveryRetryMaxAttempts caps how many times the sender-side delivery
	// retry scheduler re-sends a single message (or seen receipt) that has
	// not been confirmed end-to-end. 0 selects the node-package default.
	// Env: CORSA_DELIVERY_RETRY_MAX_ATTEMPTS.
	DeliveryRetryMaxAttempts int

	// HoldDMUntilReachable gates sender-owned DM emission on recipient
	// reachability: a locally-authored DM (first send and every retry) is
	// emitted only when a directed route or a direct session to the recipient
	// exists; an unreachable recipient is HELD (no blind gossip) and delivered
	// when a route/connection appears. ENABLED by default — this is the cure
	// for the blind-gossip storm to offline/long-gone recipients. The env var
	// CORSA_HOLD_DM_UNTIL_REACHABLE is a kill-switch (set falsey to restore the
	// legacy blind-gossip baseline). NOTE: tests that build config.Node
	// literally get the zero value (false), so the legacy path stays covered
	// while the gated path is exercised by explicit-flag tests.
	HoldDMUntilReachable bool

	// EnvelopeRetentionEnabled turns on the unified message-lifetime layer
	// (internal/core/node/envelope_retention.go): an absolute age ceiling on
	// transit/broadcast envelopes anchored on the immutable sender CreatedAt.
	// ENABLED by default — this is the cure for the transit gossip storm
	// (months-old DMs re-circulating forever). The env var CORSA_ENVELOPE_RETENTION
	// is a kill-switch (set falsey to restore the legacy no-ceiling behaviour).
	// NOTE: a literal config.Node (tests) gets the zero value (false), so the
	// legacy path stays covered while the gated path is exercised explicitly.
	EnvelopeRetentionEnabled bool

	// TransitMaxAge is the absolute lifetime ceiling for transit DMs (neither
	// party is this node) and the control-DM backstop. Zero selects the
	// node-package default (24h). Env: CORSA_TRANSIT_MAX_AGE_HOURS.
	TransitMaxAge time.Duration

	// BroadcastMaxAge is the absolute lifetime ceiling for broadcast/global
	// topics. Zero selects the node-package default (24h).
	// Env: CORSA_BROADCAST_MAX_AGE_HOURS.
	BroadcastMaxAge time.Duration

	// ProbeBackoffEnabled delays ONLY the Good→Questionable transition for
	// proven-stable routes (60s → 90s, an additive +30s once a route has been
	// confirmed healthy enough), so a stable route is actively probed ~90s
	// instead of ~60s. The Bad (122s) and Dead (182s) thresholds are UNCHANGED,
	// so a stable route that then dies silently is still detected just as fast —
	// there is no failure-detection regression, only fewer probes for healthy
	// stable routes. New or recently-failed routes keep the flat 60s timeline,
	// so onboarding is unaffected. ENABLED by default; CORSA_PROBE_BACKOFF is a
	// kill-switch (set falsey to restore the flat 60/122/182s timeline).
	ProbeBackoffEnabled bool

	// PoisonBatchEnabled makes poison-reverse fan-out use the batched
	// route_poison_v2 frame (one frame per peer carrying the whole lost-identity
	// list) toward peers that advertise mesh_poison_reverse_v2, instead of one
	// route_poison_v1 frame per (identity × peer). ENABLED by default — it
	// collapses the per-disconnect poison burst (hundreds of frames → ~one per
	// peer). The env var CORSA_POISON_BATCH is a kill-switch (set falsey to
	// force the legacy per-identity v1 fan-out everywhere). Peers without the v2
	// capability always get the v1 fan-out regardless.
	PoisonBatchEnabled bool

	// TransitForwardOnce makes a relay forward transit DMs (neither party is
	// this node) WITHOUT storing them in s.topics or tracking them in the
	// relay-retry contour: the frame is gossiped/relayed in a single pass and
	// forgotten (no buffering, no re-gossip over time).
	// Delivery durability is the sender's job (HoldDMUntilReachable +
	// delivery_retry), so a relay is purely a forwarder — no in-flight buffer,
	// no re-gossip, which removes the transit gossip-storm at its source.
	// Behaviour-changing (drops the 5-min forwarding-retry buffer), so opt-in.
	// Env: CORSA_TRANSIT_FORWARD_ONCE.
	TransitForwardOnce bool

	// GossipFanoutLimit is a general BLIND-PROPAGATION cap: it bounds how many
	// peers a single message reaches per emission via the deterministic K-of-N
	// subset (seeded by message id). The cap is applied to the shared target
	// set BEFORE the per-path capability filter, so it limits BOTH the gossip
	// push fan-out AND the blind relay fallback (tryRelayToCapableFullNodes) —
	// a small limit can therefore exclude a relay-capable peer from a given
	// pass; set it high enough that the subset still spans forwarding-capable
	// full nodes (the table-directed relay path to a KNOWN route is unaffected,
	// it does not go through this cap). Zero (the default) applies NO extra
	// K-of-N cap on top of the legacy target selection — which already bounds
	// session fan-out itself (routingTargetsFiltered caps scored session
	// targets at maxTargets=3) — so the legacy behaviour is preserved exactly.
	// Topology-affecting, so it is opt-in. Env: CORSA_GOSSIP_FANOUT_LIMIT.
	GossipFanoutLimit int

	// AllowPrivatePeers disables the private/loopback IP filter for peer
	// addresses. Production nodes never set this — it exists solely for
	// unit tests that use RFC 1918 addresses as fake peers.
	AllowPrivatePeers bool

	// MaxNextHopsPerOrigin caps the number of LIVE UplinkClaim entries
	// the routing table keeps per Identity (per-Identity bucket
	// post-Phase-A — the legacy (Identity, Origin) bucket key is gone
	// because storage dropped the Origin dimension). The knob name
	// kept the historical "...PerOrigin" suffix for config / env-var
	// stability across the migration. A positive value activates the
	// cap admission policy in routing.Table — see the "RIB
	// compaction (MaxNextHopsPerOrigin cap)" section in
	// docs/routing.md and routing.WithMaxNextHopsPerOrigin for the
	// exact eviction rules. Zero disables the K-cap entirely (every accepted entry
	// is stored, the table grows unbounded with the number of
	// next-hops that learned the same destination, and the FOUR
	// K-cap counters in the `cap_admission` JSON envelope —
	// `accepted` / `accepted_replaced` / `rejected_full` /
	// `rejected_all_protected` — stay at zero). The other two
	// counters in the same envelope (`seqno_flap_holdowns` from
	// Phase 1 P2 and `fast_invalidations` from Phase 1 P3) are
	// gated by independent knobs (`MaxSeqAdvancePerWindow` +
	// `SeqAdvanceWindow`, and `MaxSaneHops` respectively) — they
	// can still grow when the K-cap is disabled, and vice versa.
	// Operators set CORSA_MAX_NEXT_HOPS_PER_ORIGIN=0 explicitly to
	// opt out of the K-cap — for instance to roll back if a field
	// regression surfaces.
	//
	// Production default after the second rollout release is
	// routing.DefaultMaxNextHopsPerOrigin (4). The first release
	// shipped with the default at 0 so existing deployments
	// observed pre-cap behaviour exactly during the soak period;
	// after dev/staging observation the default flipped to 4. The
	// loader picks up the flip when CORSA_MAX_NEXT_HOPS_PER_ORIGIN
	// is unset, so no operator action is required to upgrade.
	//
	// Recommended ceiling for production is
	// routing.DefaultMaxNextHopsPerOrigin (4). The cap bucket is
	// per-Identity UplinkClaim storage; a direct route by
	// construction has Uplink == Identity, so a single bucket holds
	// at most one direct entry — direct routes do NOT stack across
	// buckets. Setting K below the recommended ceiling does not
	// "run out of slots for direct peers"; it tightens the
	// strict-better admission floor for transit candidates per
	// destination. The "rejected_all_protected" counter is a
	// sanity-counter for the K=1 corner case (the single slot is
	// held by the direct/local row of that bucket and a non-direct
	// arrival has nowhere to go) and for synthetic / test /
	// direct-restore edge cases — it does NOT track fan-out
	// sizing. The value is taken as-is by the table without further
	// validation, on the assumption that the operator understands
	// the trade-off.
	MaxNextHopsPerOrigin int

	// MaxSeqAdvancePerWindow caps the per-Identity outbound-SeqNo
	// advance rate (Phase 1 P2 SeqNo flap cap). When more than this
	// many fresh outbound-SeqNo allocations land for a single
	// Identity inside SeqAdvanceWindow, `routing.Table.AnnounceTo`
	// (via the internal `routeStore.AnnounceProjectionFor` layer)
	// suppresses wire emit for that Identity for
	// `routing.DefaultSeqHoldDownDuration` (DefaultTTL/2). Zero (or
	// negative) disables the cap entirely — fast-loop test fixtures
	// and the bare-Table default both leave it disabled so existing
	// unit tests stay deterministic. Production default is
	// routing.DefaultMaxSeqAdvancePerWindow (10) read from
	// CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW; operators on noisy meshes
	// can raise this if false positives surface during the rollout
	// soak. See routing.RouteCapStats.SeqNoFlapHoldowns for the
	// operator-facing counter that surfaces engage events.
	MaxSeqAdvancePerWindow int

	// SeqAdvanceWindow is the sliding-window length for the SeqNo
	// flap-cap detector (Phase 1 P2). Outbound-SeqNo advances older
	// than `now - window` are trimmed from the per-Identity
	// velocity tracker on every record / clearExpired pass. Zero
	// disables the cap entirely (same effect as
	// MaxSeqAdvancePerWindow=0 — the bare-Table default). Production
	// default is routing.DefaultSeqAdvanceWindow (5 min) read from
	// CORSA_SEQNO_ADVANCE_WINDOW_SECONDS.
	SeqAdvanceWindow time.Duration

	// MaxSaneHops bounds the per-claim hop count above which a
	// fresh ingest is fast-invalidated locally (Phase 1 P3). Claims
	// with `Hops > MaxSaneHops` are recorded as tombstones at the
	// observed SeqNo and dropped from Lookup immediately, sparing
	// the next 120s TTL window the cost of steering traffic onto a
	// count-to-infinity uplink. Zero (or negative) disables the
	// path. Production default is routing.MaxSaneHops (8) read from
	// CORSA_MAX_SANE_HOPS; operators on deep meshes can raise this
	// if 9+ hop chains are routinely legitimate.
	MaxSaneHops int

	// AnnounceInterval overrides the periodic routing-announce cadence
	// (env: CORSA_ANNOUNCE_INTERVAL_SECONDS). The default is
	// routing.DefaultAnnounceInterval (30s) which produces a delta
	// cycle every 30s and a forced full sync every 60s
	// (DefaultTTL/2). For densely-connected meshes (>10 direct peers)
	// operators can raise this to 45-60s to halve the periodic
	// delta-computation CPU; the forced-full-sync cadence is
	// independently capped at DefaultTTL/2 so freshness invariants
	// hold even when AnnounceInterval is increased. Setting
	// AnnounceInterval == DefaultTTL/2 effectively makes every tick
	// a forced full sync (no in-between deltas), trading freshness
	// granularity for CPU savings — appropriate for dense mesh
	// receivers that are CPU-bound on inbound announce processing
	// from many peers. Zero or negative is interpreted as "use
	// routing.DefaultAnnounceInterval".
	AnnounceInterval time.Duration

	// OverloadGoroutineThreshold sets the goroutine-count threshold
	// (env: CORSA_OVERLOAD_GOROUTINE_THRESHOLD) above which the
	// routing announce loop suppresses non-critical delta sends to
	// preserve CPU. Forced full sync still fires on schedule so the
	// freshness invariant (DefaultTTL/2) holds. Zero or negative
	// disables the throttle entirely (rollout default — existing
	// deployments observe pre-Phase-0 behaviour exactly until
	// operators opt in). The proxy is goroutine count because each
	// inbound frame spawns at least one goroutine; sustained backlog
	// produces visible pile-up. A reasonable value for hub nodes with
	// 30+ direct peers is 5000-10000; tune from observed
	// runtime.NumGoroutine() under normal load × 2-3 as the trip
	// point.
	OverloadGoroutineThreshold int

	// EnableMeshRoutingV3 controls whether this node advertises the Phase 4
	// compact-announce capability (env: CORSA_ENABLE_MESH_ROUTING_V3,
	// default TRUE). When true, the local capability set includes
	// CapMeshRoutingV3, the announce loop emits route_announce_v3 to
	// peers that also advertise it, and connect-time / forced-full syncs
	// to those peers use the v3 kind="full" frame. Peers without the
	// capability continue to receive legacy announce_routes /
	// routes_update unchanged (overview §5.2 Phase B), so a default-on node
	// stays backward compatible with opted-out or older peers. The v3 path
	// has completed the default-off→soak→flip-on rollout cadence the v2
	// rollout used; operators can still opt out via
	// CORSA_ENABLE_MESH_ROUTING_V3=0 (or false/no/off). See
	// docs/cluster-mesh/phase-4-compact-wire-signed.md §7.
	EnableMeshRoutingV3 bool

	// DisableDirectMessages opts this node out of direct messages
	// (topics "dm" / "dm-control") addressed to its own identity.
	//
	// The zero value (false = DMs accepted) is today's behaviour, so the
	// desktop client and every existing config.Node construction —
	// including struct-literal test fixtures — are untouched; same
	// zero-value convention as AllowPrivatePeers. Only the headless
	// binary (cmd/corsa-node via internal/app/node) sets it, from
	// CORSA_ACCEPT_DM (see AcceptDirectMessagesFromEnv): a console node
	// has no user reading messages, so an inbound DM addressed to it is
	// unread spam that would otherwise accumulate in memory forever and
	// trigger false "delivered" receipts to the sender.
	//
	// When true the node:
	//   - keeps its own box key out of the contact plane (trust-store
	//     self row and fetch_contacts), so the key is never
	//     redistributed and clients whose node has not handshaked with
	//     this node directly cannot encrypt a DM to it. The handshake
	//     plane (hello/welcome) still carries the key — deployed peers
	//     require all four identity fields to issue the session-auth
	//     challenge (connauth.HasIdentityFields), so direct peers do
	//     cache it;
	//   - drops inbound DM-class messages addressed to itself before any
	//     crypto work, acking the previous hop like a duplicate so
	//     hop-level retries stop (see Service.storeIncomingMessage) —
	//     this is the authoritative protection, covering DMs composed
	//     with a handshake-cached key;
	//   - omits "messages" from ServiceList.
	//
	// Relay/transit of OTHER parties' DMs is unaffected — the node stays
	// a full relay.
	DisableDirectMessages bool

	// PprofAddr enables Go's net/http/pprof profiling server on the
	// given address when non-empty (env: CORSA_PPROF_ADDR). Default
	// empty = OFF: no listener, no debug surface, zero overhead. Intended
	// strictly for diagnosing memory/CPU on a single node — set e.g.
	// "127.0.0.1:6060", then `go tool pprof http://127.0.0.1:6060/debug/
	// pprof/heap`. For safety the listener MUST bind to a loopback
	// address; a non-loopback host is rejected at startup (the pprof
	// surface exposes process internals and must never face the network).
	// Leave unset in production except during an active investigation.
	PprofAddr string

	// RecordAllTraffic starts recording all peer traffic at node startup
	// (env: CORSA_RECORD_ALL_TRAFFIC, default FALSE = off). Equivalent to
	// issuing the recordAllPeerTraffic RPC command right after the capture
	// manager is initialised: it installs the standing scope=all rule, so
	// every connection established afterwards is captured automatically.
	// Diagnostic feature — capture files grow with traffic volume, so
	// leave it off in production except during an active investigation.
	RecordAllTraffic bool

	// RecordTrafficFormat selects the capture file format used by the
	// startup recording rule (env: CORSA_RECORD_TRAFFIC_FORMAT,
	// "compact"|"pretty", default "compact"). Only consulted when
	// RecordAllTraffic is true. An unrecognised value is logged and
	// ignored at startup (Service.Run) — the node starts, recording off.
	RecordTrafficFormat string
}

type RPC struct {
	Host     string
	Port     string
	Username string
	Password string
}

type Config struct {
	App  App
	Node Node
	RPC  RPC
}

const (
	// ClientVersionMajor / ClientVersionMinor / ClientVersionBuild are the
	// three numeric components of the human-readable Corsa release version.
	// CorsaVersion is derived from them as "MAJOR.MINOR.BUILD" — the single
	// place to bump on a release.
	//
	// ClientVersionBuild also feeds the wire-protocol "client_build" integer
	// (protocol.Frame.ClientBuild) used by the version-policy heuristic to
	// detect peers running newer builds. Keep it monotonically increasing
	// across releases so older peers continue to interpret the comparison
	// correctly.
	ClientVersionMajor = 1
	ClientVersionMinor = 0
	ClientVersionBuild = 60
	// ProtocolVersion is the wire version this build emits in hello/welcome.
	// MinimumProtocolVersion is the floor below which inbound peers are
	// rejected. Both are bumped only by an explicit wire/runtime contract
	// change documented in docs/protocol/handshake.md. The current floor
	// is well above v12, so this build does not carry any v10..v13
	// compatibility paths.
	//
	// v20 raised the floor past the subscribe_inbox era: a v20+ responder
	// auto-registers the peer's inbox subscription AND replays the backlog at
	// auth time (see handleAuthSession → registerHelloRoute), so the explicit
	// subscribe_inbox/subscribed round-trip became redundant. With the floor
	// at 20 the legacy commands (subscribe_inbox, subscribed, request_inbox)
	// and the pre-v20 initiator round-trip have been removed entirely.
	// v23: ReceiptStatusSeenAck — the end-to-end acknowledgement the
	// original message sender returns for a received "seen" receipt, riding
	// the existing receipt frames (push_delivery_receipt /
	// relay_delivery_receipt). Additive: pre-v23 binaries reject the unknown
	// status at parse time and drop the frame; the seen-sender's bounded
	// retry absorbs that until both ends are v23+.
	ProtocolVersion        = 24
	MinimumProtocolVersion = 20
	// ProtocolVersionSeenAck is the version that introduced
	// ReceiptStatusSeenAck. Receipt senders gate seen_ack emission on the
	// peer advertising >= this version (a pre-v23 binary only rejects the
	// unknown status at parse time, so sending it is pure waste).
	// TODO(seen-ack-gate-removal): delete this constant and the gates in
	// internal/core/node (sendReceiptToPeer, pushReceiptToSubscribers) once
	// MinimumProtocolVersion reaches ProtocolVersionSeenAck.
	ProtocolVersionSeenAck = 23
	DefaultOutgoingPeers   = 8
	DefaultPeerPort        = "64646"
)

// CorsaVersion is the canonical release version string ("MAJOR.MINOR.BUILD").
// It is derived from ClientVersionMajor / ClientVersionMinor / ClientVersionBuild
// so the three integer constants are the single source of truth — bump them on
// a release and CorsaVersion follows. Declared as var (not const) because Go
// const expressions cannot call fmt.Sprintf; treat it as immutable at runtime.
var CorsaVersion = fmt.Sprintf("%d.%d.%d", ClientVersionMajor, ClientVersionMinor, ClientVersionBuild)

func Default() Config {
	listenAddress := envOrDefault("CORSA_LISTEN_ADDRESS", ":"+DefaultPeerPort)
	nodeType := nodeTypeFromEnv()
	listenerEnabled, listenerSet := listenerFromEnv()
	advertisePort := advertisePortFromEnv()
	bootstrapPeers := bootstrapPeersFromEnv(listenAddress)
	identityPath := resolveStartupPath(envOrDefault("CORSA_IDENTITY_PATH", defaultIdentityPath(listenAddress)))
	trustStorePath := resolveStartupPath(envOrDefault("CORSA_TRUST_STORE_PATH", defaultTrustStorePath(listenAddress)))
	peersStatePath := resolveStartupPath(envOrDefault("CORSA_PEERS_PATH", defaultPeersStatePath(listenAddress)))
	chatLogDir := resolveStartupPath(envOrDefault("CORSA_CHATLOG_DIR", defaultChatLogDir()))
	downloadDir := resolveStartupPath(envOrDefault("CORSA_DOWNLOAD_DIR", ""))
	proxyAddress := envOrDefault("CORSA_PROXY", "")
	maxClockDrift := maxClockDriftFromEnv()
	maxOutgoingPeers := maxOutgoingPeersFromEnv()
	maxIncomingPeers := maxIncomingPeersFromEnv()
	maxNextHopsPerOrigin := maxNextHopsPerOriginFromEnv()
	maxSeqAdvancePerWindow := maxSeqAdvancePerWindowFromEnv()
	seqAdvanceWindow := seqAdvanceWindowFromEnv()
	maxSaneHops := maxSaneHopsFromEnv()
	announceInterval := announceIntervalFromEnv()
	overloadGoroutineThreshold := overloadGoroutineThresholdFromEnv()
	enableMeshRoutingV3 := enableMeshRoutingV3FromEnv()
	pendingRingSize := pendingRingSizeFromEnv()
	deliveryRetryMaxAttempts := deliveryRetryMaxAttemptsFromEnv()
	holdDMUntilReachable := holdDMUntilReachableFromEnv()
	envelopeRetentionEnabled := envelopeRetentionEnabledFromEnv()
	transitMaxAge := transitMaxAgeFromEnv()
	broadcastMaxAge := broadcastMaxAgeFromEnv()
	gossipFanoutLimit := gossipFanoutLimitFromEnv()
	transitForwardOnce := transitForwardOnceFromEnv()
	poisonBatchEnabled := poisonBatchEnabledFromEnv()
	probeBackoffEnabled := probeBackoffEnabledFromEnv()

	return Config{
		App: App{
			Name:     "corsa",
			Network:  "gazeta-devnet",
			Profile:  "default",
			Language: appLanguageFromEnv(),
			Version:  CorsaVersion,
		},
		Node: Node{
			ListenAddress:              listenAddress,
			AdvertisePort:              advertisePort,
			BootstrapPeers:             bootstrapPeers,
			IdentityPath:               identityPath,
			TrustStorePath:             trustStorePath,
			PeersStatePath:             peersStatePath,
			ChatLogDir:                 chatLogDir,
			DownloadDir:                downloadDir,
			ProxyAddress:               proxyAddress,
			Type:                       nodeType,
			ListenerEnabled:            listenerEnabled,
			ListenerSet:                listenerSet,
			ClientVersion:              CorsaVersion,
			MaxClockDrift:              maxClockDrift,
			MaxOutgoingPeers:           maxOutgoingPeers,
			MaxIncomingPeers:           maxIncomingPeers,
			PendingRingSize:            pendingRingSize,
			DeliveryRetryMaxAttempts:   deliveryRetryMaxAttempts,
			HoldDMUntilReachable:       holdDMUntilReachable,
			EnvelopeRetentionEnabled:   envelopeRetentionEnabled,
			TransitMaxAge:              transitMaxAge,
			BroadcastMaxAge:            broadcastMaxAge,
			GossipFanoutLimit:          gossipFanoutLimit,
			TransitForwardOnce:         transitForwardOnce,
			PoisonBatchEnabled:         poisonBatchEnabled,
			ProbeBackoffEnabled:        probeBackoffEnabled,
			MaxNextHopsPerOrigin:       maxNextHopsPerOrigin,
			MaxSeqAdvancePerWindow:     maxSeqAdvancePerWindow,
			SeqAdvanceWindow:           seqAdvanceWindow,
			MaxSaneHops:                maxSaneHops,
			AnnounceInterval:           announceInterval,
			OverloadGoroutineThreshold: overloadGoroutineThreshold,
			EnableMeshRoutingV3:        enableMeshRoutingV3,
			PprofAddr:                  envOrDefault("CORSA_PPROF_ADDR", ""),
			RecordAllTraffic:           recordAllTrafficFromEnv(),
			RecordTrafficFormat:        envOrDefault("CORSA_RECORD_TRAFFIC_FORMAT", ""),
		},
		RPC: RPC{
			Host:     envOrDefault("CORSA_RPC_HOST", "127.0.0.1"),
			Port:     envOrDefault("CORSA_RPC_PORT", "46464"),
			Username: envOrDefault("CORSA_RPC_USERNAME", ""),
			Password: envOrDefault("CORSA_RPC_PASSWORD", ""),
		},
	}
}

func (n Node) NormalizedType() NodeType {
	switch n.Type {
	case NodeTypeClient:
		return NodeTypeClient
	default:
		return NodeTypeFull
	}
}

func (n Node) ServiceList() []string {
	services := []string{"identity", "contacts"}
	// "messages" advertises a live DM inbox. A node that drops inbound
	// DMs (headless relay without CORSA_ACCEPT_DM) must not claim it —
	// peers surface the list in their console UI.
	if !n.DisableDirectMessages {
		services = append(services, "messages")
	}
	services = append(services, "gazeta")
	if n.NormalizedType() == NodeTypeFull {
		services = append(services, "relay")
	}
	return services
}

func (n Node) EffectiveListenerEnabled() bool {
	if n.ListenerSet {
		return n.ListenerEnabled
	}
	return n.NormalizedType() != NodeTypeClient
}

// EffectiveAdvertisePort returns the runtime value that fills
// hello.advertise_port and feeds candidate-building on inbound observed-IP
// learning. A nil AdvertisePort (operator did not supply a valid
// CORSA_ADVERTISE_PORT) collapses to DefaultPeerPort — the single-source
// fallback required by the advertise-address phase 1 deprecation contract.
// The explicit nil check mirrors the "absence is a type, not a zero value"
// invariant and keeps the fallback logic in exactly one place. Callers that
// need the string form (net.JoinHostPort, persisted JSON row) convert
// through strconv.Itoa at their boundary — the runtime path never stores
// the port as a string. The fallback path re-parses DefaultPeerPort so the
// string constant remains the single source of truth for the default; a
// malformed DefaultPeerPort (programmer error only) yields a zero PeerPort,
// which the rest of the pipeline already treats as "absent / invalid".
func (n Node) EffectiveAdvertisePort() domain.PeerPort {
	if n.AdvertisePort != nil && n.AdvertisePort.IsValid() {
		return *n.AdvertisePort
	}
	value, err := strconv.Atoi(DefaultPeerPort)
	if err != nil {
		return 0
	}
	return domain.PeerPort(value)
}

func (n Node) EffectiveMaxOutgoingPeers() int {
	if n.MaxOutgoingPeers > 0 {
		return n.MaxOutgoingPeers
	}
	return DefaultOutgoingPeers
}

func (n Node) EffectiveMaxIncomingPeers() int {
	if n.MaxIncomingPeers > 0 {
		return n.MaxIncomingPeers
	}
	return 0
}

func (n Node) EffectivePeersStatePath() string {
	if strings.TrimSpace(n.PeersStatePath) != "" {
		return n.PeersStatePath
	}
	return defaultPeersStatePath(n.ListenAddress)
}

func (n Node) EffectiveChatLogDir() string {
	if strings.TrimSpace(n.ChatLogDir) != "" {
		return n.ChatLogDir
	}
	return defaultChatLogDir()
}

// EffectiveDataDir returns the base data directory for all node-local state
// (transmit files, transfers metadata, downloads). It reuses EffectiveChatLogDir
// so that CORSA_CHATLOG_DIR moves everything together.
func (n Node) EffectiveDataDir() string {
	return n.EffectiveChatLogDir()
}

// EffectiveDownloadDir returns the directory where received files are saved.
// Override via CORSA_DOWNLOAD_DIR environment variable.
func (n Node) EffectiveDownloadDir() string {
	if strings.TrimSpace(n.DownloadDir) != "" {
		return n.DownloadDir
	}
	return filepath.Join(n.EffectiveDataDir(), "downloads")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func resolveStartupPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return path
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}

func defaultIdentityPath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(defaultChatLogDir(), "identity-"+port+".json")
}

func defaultTrustStorePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(defaultChatLogDir(), "trust-"+port+".json")
}

func defaultPeersStatePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(defaultChatLogDir(), "peers-"+port+".json")
}

func portSuffix(listenAddress string) string {
	port := "default"
	if idx := strings.LastIndex(listenAddress, ":"); idx >= 0 && idx < len(listenAddress)-1 {
		port = listenAddress[idx+1:]
	}
	return port
}

func bootstrapPeersFromEnv(listenAddress string) []string {
	if raw := os.Getenv("CORSA_BOOTSTRAP_PEERS"); raw != "" {
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			value := strings.TrimSpace(part)
			if value != "" {
				out = append(out, value)
			}
		}
		if len(out) > 0 {
			return out
		}
	}

	if raw := os.Getenv("CORSA_BOOTSTRAP_PEER"); raw != "" {
		return []string{strings.TrimSpace(raw)}
	}

	_ = listenAddress
	return append([]string(nil), chainParamSeedsMain...)
}

func nodeTypeFromEnv() NodeType {
	if t, ok := domain.ParseNodeType(os.Getenv("CORSA_NODE_TYPE")); ok {
		return t
	}
	return NodeTypeFull
}

// advertisePortFromEnv parses CORSA_ADVERTISE_PORT. Valid input is a
// string integer in the inclusive range 1..65535; any other value
// (empty, non-numeric, 0, >65535, negative) returns nil so the caller can
// fall back to DefaultPeerPort via EffectiveAdvertisePort. The pointer
// return models optional-presence explicitly at the type level — zero is
// never used as a sentinel for "operator did not configure this" — and
// the domain.PeerPort element type keeps the value distinct from other
// small integers as it flows through the config and handshake layers.
func advertisePortFromEnv() *domain.PeerPort {
	raw := strings.TrimSpace(os.Getenv("CORSA_ADVERTISE_PORT"))
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	port := domain.PeerPort(value)
	if !port.IsValid() {
		return nil
	}
	return &port
}

func listenerFromEnv() (bool, bool) {
	raw := strings.TrimSpace(os.Getenv("CORSA_LISTENER"))
	if raw == "" {
		return false, false
	}
	switch raw {
	case "1", "true", "TRUE", "yes", "YES":
		return true, true
	default:
		return false, true
	}
}

func appLanguageFromEnv() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_LANGUAGE"))) {
	case "ru", "es", "fr", "ar", "zh":
		return strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_LANGUAGE")))
	default:
		return "en"
	}
}

func maxClockDriftFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_CLOCK_DRIFT_SECONDS"))
	if raw == "" {
		return protocol.DefaultMessageTimeDrift
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return protocol.DefaultMessageTimeDrift
	}

	return time.Duration(seconds) * time.Second
}

func maxOutgoingPeersFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_OUTGOING_PEERS"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func defaultChatLogDir() string {
	return appdata.DefaultDir()
}

func maxNextHopsPerOriginFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_NEXT_HOPS_PER_ORIGIN"))
	if raw == "" {
		// Production default after the second rollout release. The
		// first release shipped with `0` (cap disabled) so existing
		// deployments observed pre-cap behaviour exactly; after the
		// observation period on dev/staging the default flips to
		// `routing.DefaultMaxNextHopsPerOrigin` (4) — the recommended
		// ceiling. Operators that want to roll back set the env to
		// "0" explicitly; we keep that as a first-class opt-out so a
		// future field regression can be neutralised without a
		// rebuild.
		return routing.DefaultMaxNextHopsPerOrigin
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		// Unparsable or negative values fall back to the production
		// default — same shape as maxClockDriftFromEnv. No
		// operator-facing log line is emitted (here or at the
		// service call-site): the MaxNextHopsPerOrigin rename +
		// deprecation surface is deferred to the Phase 6 cleanup
		// window, and shipping a warn that operators would have
		// to silence in dashboards for two phases provides no
		// migration value today. An explicit "0" is NOT covered
		// by this branch — it parses fine and falls through to the
		// return below as the
		// operator's deliberate cap-disabled signal.
		return routing.DefaultMaxNextHopsPerOrigin
	}
	return value
}

// maxSeqAdvancePerWindowFromEnv reads CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW
// and returns the SeqNo flap-cap threshold (Phase 1 P2). The production
// default is routing.DefaultMaxSeqAdvancePerWindow (10) when the env
// var is unset, whitespace-only, or unparsable; explicit "0" AND any
// valid negative integer pass through unchanged and reach the routing
// layer's `maxSeqAdvancePerWindow <= 0` short-circuit to disable the
// cap. The negative pass-through matches the Node.MaxSeqAdvancePerWindow
// doc-comment ("Zero (or negative) disables the cap entirely") and
// gives operators a documented kill-switch — see also the
// TestMaxSeqAdvancePerWindowFromEnv table-driven regression. This
// shape DIFFERS from maxNextHopsPerOriginFromEnv on purpose: the K-cap
// loader keeps the pre-P3 "negative coerces to default" semantic for
// stable-knob compatibility (see TestMaxNextHopsPerOriginRolloutDefaults).
func maxSeqAdvancePerWindowFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW"))
	if raw == "" {
		return routing.DefaultMaxSeqAdvancePerWindow
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		// Only malformed input falls back to the production default.
		// A valid negative integer passes through and disables the
		// cap in the routing layer (`maxSeqAdvancePerWindow <= 0`
		// short-circuit in `FlapDetector.recordSeqAdvanceLocked`),
		// matching the Node.MaxSeqAdvancePerWindow doc-comment
		// ("Zero (or negative) disables the cap entirely"). Without
		// the negative pass-through, `CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW=-1`
		// would silently enable the production default (10), which
		// contradicts the documented kill-switch contract.
		return routing.DefaultMaxSeqAdvancePerWindow
	}
	return value
}

// seqAdvanceWindowFromEnv reads CORSA_SEQNO_ADVANCE_WINDOW_SECONDS and
// returns the SeqNo flap-cap sliding-window length (Phase 1 P2). Zero
// (or negative) disables the cap entirely (same effect as
// MaxSeqAdvancePerWindow=0); production default is
// routing.DefaultSeqAdvanceWindow (5 minutes). Operator-set values
// pass through as Duration(seconds * time.Second).
func seqAdvanceWindowFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("CORSA_SEQNO_ADVANCE_WINDOW_SECONDS"))
	if raw == "" {
		return routing.DefaultSeqAdvanceWindow
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		// Same shape as maxSeqAdvancePerWindowFromEnv: only
		// malformed input falls back to default. Negative passes
		// through and disables the cap via the `seqAdvanceWindow <= 0`
		// short-circuit in `FlapDetector.recordSeqAdvanceLocked` /
		// `isInSeqHoldDownLocked`, matching the Node.SeqAdvanceWindow
		// doc-comment.
		return routing.DefaultSeqAdvanceWindow
	}
	return time.Duration(seconds) * time.Second
}

// maxSaneHopsFromEnv reads CORSA_MAX_SANE_HOPS and returns the fast-
// invalidation hops threshold (Phase 1 P3). Zero or negative disables
// the path (the routeStore admission flow returns early before the
// fast-invalidation branch). Production default is routing.MaxSaneHops
// (8) when the env var is unset; an explicit "0" (or any negative)
// disables the cap.
func maxSaneHopsFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_SANE_HOPS"))
	if raw == "" {
		return routing.MaxSaneHops
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		// Only malformed input falls back to default. Negative
		// passes through and disables the fast-invalidation path
		// via the `s.maxSaneHops > 0` gate in `ApplyUpdate`,
		// matching the Node.MaxSaneHops doc-comment
		// ("Zero (or negative) disables the path").
		return routing.MaxSaneHops
	}
	return value
}

// announceIntervalFromEnv reads CORSA_ANNOUNCE_INTERVAL_SECONDS and
// returns the routing-announce period for the AnnounceLoop. Zero is
// returned when the env var is unset, invalid, or non-positive — the
// caller (Service init) treats zero as "use the package default
// (routing.DefaultAnnounceInterval, 30s)" so the routing layer keeps
// the fallback in one place.
//
// The cap on forced-full-sync cadence (DefaultTTL/2) is independent
// of this value and is enforced inside the AnnounceLoop, so an
// operator-supplied interval longer than DefaultTTL/2 cannot push
// freshness past the invariant — it just turns each tick into a
// forced full sync with no in-between deltas. See the AnnounceInterval
// doc-comment on Node for the trade-off.
func announceIntervalFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("CORSA_ANNOUNCE_INTERVAL_SECONDS"))
	if raw == "" {
		return 0
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// overloadGoroutineThresholdFromEnv reads
// CORSA_OVERLOAD_GOROUTINE_THRESHOLD and returns the goroutine-count
// trip point for the announce-loop overload gate. Zero is returned
// when the env var is unset, invalid, non-positive, or the operator
// explicitly sets "0" — the Service init treats zero as "disable the
// gate" so existing deployments observe pre-Phase-0 behaviour exactly
// until they opt in. There is no domain default for this knob: turning
// it on without a deliberate value would either over-suppress (low
// threshold) or never engage (high threshold), neither of which is
// useful as an unattended default.
func overloadGoroutineThresholdFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_OVERLOAD_GOROUTINE_THRESHOLD"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

// holdDMUntilReachableFromEnv reads CORSA_HOLD_DM_UNTIL_REACHABLE. Defaults to
// ENABLED (reachability-gated sender-owned DM emission) — this is the storm
// cure: a node no longer blind-gossips its own DMs to unreachable recipients.
// The variable is a kill-switch: an explicit falsey value restores the legacy
// blind-gossip baseline.
func holdDMUntilReachableFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_HOLD_DM_UNTIL_REACHABLE"))) {
	case "0", "false", "no", "off":
		return false
	default:
		// Unset, empty, truthy, or unrecognised → enabled (the new default).
		return true
	}
}

// envelopeRetentionEnabledFromEnv reads CORSA_ENVELOPE_RETENTION. Defaults to
// ENABLED (absolute age ceiling on transit/broadcast envelopes) — the cure for
// the transit gossip storm. The variable is a kill-switch: an explicit falsey
// value restores the legacy no-ceiling behaviour.
func envelopeRetentionEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_ENVELOPE_RETENTION"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// probeBackoffEnabledFromEnv reads CORSA_PROBE_BACKOFF. Defaults to ENABLED
// (stable routes age/probe progressively slower). Kill-switch: an explicit
// falsey value restores the flat 60/122/182s health timeline for every route.
func probeBackoffEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_PROBE_BACKOFF"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// poisonBatchEnabledFromEnv reads CORSA_POISON_BATCH. Defaults to ENABLED
// (batched route_poison_v2 toward v2-capable peers — collapses the poison
// burst). The variable is a kill-switch: an explicit falsey value forces the
// legacy per-identity v1 fan-out everywhere.
func poisonBatchEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_POISON_BATCH"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// transitForwardOnceFromEnv reads CORSA_TRANSIT_FORWARD_ONCE. Defaults to OFF
// (the legacy in-flight buffer + relay-retry behaviour); a truthy value makes
// relays forward transit DMs once without storing them. Opt-in because it
// drops the per-hop forwarding-retry buffer (delivery falls back to sender
// end-to-end retry).
func transitForwardOnceFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_TRANSIT_FORWARD_ONCE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// gossipFanoutLimitFromEnv reads CORSA_GOSSIP_FANOUT_LIMIT (positive integer
// peer count). Unset/invalid/non-positive returns 0 — no extra K-of-N cap on
// top of the legacy target selection (itself already bounded), the legacy
// default. Topology-affecting, so it stays opt-in.
func gossipFanoutLimitFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_GOSSIP_FANOUT_LIMIT"))
	if raw == "" {
		return 0
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0
	}
	return limit
}

// transitMaxAgeFromEnv reads CORSA_TRANSIT_MAX_AGE_HOURS (positive integer
// hours). Unset/invalid/non-positive selects the node-package default (24h),
// signalled here as 0 so the node layer applies its own constant.
func transitMaxAgeFromEnv() time.Duration {
	return positiveHoursFromEnv("CORSA_TRANSIT_MAX_AGE_HOURS")
}

// broadcastMaxAgeFromEnv reads CORSA_BROADCAST_MAX_AGE_HOURS. See
// transitMaxAgeFromEnv for the unset/invalid → 0 (node default) contract.
func broadcastMaxAgeFromEnv() time.Duration {
	return positiveHoursFromEnv("CORSA_BROADCAST_MAX_AGE_HOURS")
}

// positiveHoursFromEnv parses a positive integer hour count into a Duration,
// returning 0 when the variable is unset, malformed, or non-positive — the
// caller treats 0 as "use the node-package default".
func positiveHoursFromEnv(key string) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0
	}
	hours, err := strconv.Atoi(raw)
	if err != nil || hours <= 0 {
		return 0
	}
	return time.Duration(hours) * time.Hour
}

// enableMeshRoutingV3FromEnv reads CORSA_ENABLE_MESH_ROUTING_V3 and returns
// whether this node advertises the Phase 4 compact-announce wire frame.
//
// The Phase 4 v3 path has soaked and is now ENABLED BY DEFAULT: an unset,
// empty, or unrecognised value yields true. Operators retain an explicit
// escape hatch — "0", "false", "no", or "off" (any case after trimming)
// turns it back off, dropping the node to the legacy announce_routes /
// routes_update wire path. Negotiation stays backward compatible regardless:
// v3 frames are only sent to peers that also advertise CapMeshRoutingV3, so a
// default-on node still talks legacy to any peer that opted out or runs an
// older build (overview §5.2 Phase B;
// docs/cluster-mesh/phase-4-compact-wire-signed.md §7).
func enableMeshRoutingV3FromEnv() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_ENABLE_MESH_ROUTING_V3")))
	switch raw {
	case "0", "false", "no", "off":
		return false
	default:
		// Unset, empty, truthy, or unrecognised → enabled (the new default).
		return true
	}
}

// AcceptDirectMessagesFromEnv reads CORSA_ACCEPT_DM and returns whether a
// HEADLESS node should accept direct messages addressed to its own
// identity. Default FALSE: a console node has no user behind it, so DM
// acceptance is opt-in ("1", "true", "yes", "on" after trimming, any
// case). Exported because the decision of WHICH default applies belongs
// to the binary: internal/app/node (headless) derives
// Node.DisableDirectMessages from this, while the desktop app keeps the
// zero value (DMs accepted) — a GUI client always has a user who wants
// to chat.
func AcceptDirectMessagesFromEnv() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_ACCEPT_DM")))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// recordAllTrafficFromEnv reads CORSA_RECORD_ALL_TRAFFIC. Default FALSE:
// startup traffic recording is opt-in (it is a diagnostic feature whose
// capture files grow with traffic volume). Only explicit truthy values
// enable it; unset, empty, or unrecognised → disabled.
func recordAllTrafficFromEnv() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_RECORD_ALL_TRAFFIC")))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func maxIncomingPeersFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_MAX_INCOMING_PEERS"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

// pendingRingSizeFromEnv reads CORSA_PENDING_RING_SIZE — the per-peer
// in-memory pending ring capacity. Returns 0 (meaning "use the built-in
// default") when unset, empty, non-numeric, or <= 0; a positive value caps
// the per-peer ring at exactly that many frames. See Node.PendingRingSize.
func pendingRingSizeFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_PENDING_RING_SIZE"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

// deliveryRetryMaxAttemptsFromEnv reads CORSA_DELIVERY_RETRY_MAX_ATTEMPTS —
// the cap on sender-side end-to-end delivery retries. Empty, non-numeric or
// non-positive values select the node-package default.
func deliveryRetryMaxAttemptsFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("CORSA_DELIVERY_RETRY_MAX_ATTEMPTS"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

func (r RPC) ListenAddress() string {
	return net.JoinHostPort(r.Host, r.Port)
}

// ValidateAuth returns an error if auth is partially configured.
// Either both Username and Password must be set, or neither.
func (r RPC) ValidateAuth() error {
	hasUser := r.Username != ""
	hasPass := r.Password != ""
	if hasUser != hasPass {
		return fmt.Errorf("rpc auth partially configured: both CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD must be set together")
	}
	return nil
}

func (r RPC) AuthEnabled() bool {
	return r.Username != "" && r.Password != ""
}
