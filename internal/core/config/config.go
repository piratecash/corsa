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
	QueueStatePath   string
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

	// AllowPrivatePeers disables the private/loopback IP filter for peer
	// addresses. Production nodes never set this — it exists solely for
	// unit tests that use RFC 1918 addresses as fake peers.
	AllowPrivatePeers bool

	// MaxNextHopsPerOrigin caps the number of LIVE next-hops the
	// routing table keeps per (Identity, Origin) pair. A positive
	// value activates the cap admission policy in routing.Table —
	// see docs/routing-rib-compaction-and-snapshot-refactor.md
	// (Stage B) and routing.WithMaxNextHopsPerOrigin for the exact
	// eviction rules. Zero disables the cap entirely (every accepted
	// entry is stored, the table grows unbounded with the number of
	// next-hops that learned the same destination, and the
	// `cap_admission` counters stay at zero); operators set
	// CORSA_MAX_NEXT_HOPS_PER_ORIGIN=0 explicitly to opt out — for
	// instance to roll back if a field regression surfaces.
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
	// keyed by (Identity, Origin), and a direct route by
	// construction has NextHop == Identity, so a single bucket holds
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
	ClientVersionBuild = 44
	// ProtocolVersion is the wire version this build emits in hello/welcome.
	// MinimumProtocolVersion is the floor below which inbound peers are
	// rejected. Both are bumped only by an explicit wire/runtime contract
	// change documented in docs/protocol/handshake.md. The current floor
	// is well above v12, so this build does not carry any v10..v13
	// compatibility paths.
	ProtocolVersion        = 15
	MinimumProtocolVersion = 14
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
	queueStatePath := resolveStartupPath(envOrDefault("CORSA_QUEUE_STATE_PATH", defaultQueueStatePath(listenAddress)))
	peersStatePath := resolveStartupPath(envOrDefault("CORSA_PEERS_PATH", defaultPeersStatePath(listenAddress)))
	chatLogDir := resolveStartupPath(envOrDefault("CORSA_CHATLOG_DIR", defaultChatLogDir()))
	downloadDir := resolveStartupPath(envOrDefault("CORSA_DOWNLOAD_DIR", ""))
	proxyAddress := envOrDefault("CORSA_PROXY", "")
	maxClockDrift := maxClockDriftFromEnv()
	maxOutgoingPeers := maxOutgoingPeersFromEnv()
	maxIncomingPeers := maxIncomingPeersFromEnv()
	maxNextHopsPerOrigin := maxNextHopsPerOriginFromEnv()
	announceInterval := announceIntervalFromEnv()
	overloadGoroutineThreshold := overloadGoroutineThresholdFromEnv()

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
			QueueStatePath:             queueStatePath,
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
			MaxNextHopsPerOrigin:       maxNextHopsPerOrigin,
			AnnounceInterval:           announceInterval,
			OverloadGoroutineThreshold: overloadGoroutineThreshold,
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
	services := []string{"identity", "contacts", "messages", "gazeta"}
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

func (n Node) EffectiveQueueStatePath() string {
	if strings.TrimSpace(n.QueueStatePath) != "" {
		return n.QueueStatePath
	}
	return defaultQueueStatePath(n.ListenAddress)
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

func defaultQueueStatePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(defaultChatLogDir(), "queue-"+port+".json")
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
		// default — same shape as maxClockDriftFromEnv. We
		// deliberately do NOT log a warning here: this helper is
		// invoked once during config loading and the operator-facing
		// log line belongs at the call site, not in a generic env
		// parser. An explicit "0" is NOT covered by this branch — it
		// parses fine and falls through to the return below as the
		// operator's deliberate cap-disabled signal.
		return routing.DefaultMaxNextHopsPerOrigin
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
