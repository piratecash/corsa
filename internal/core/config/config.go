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
	ListenAddress    string
	AdvertiseAddress string
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
	CorsaVersion     = "0.42 alpha"
	CorsaWireVersion = "0.42-alpha"
	ClientBuild      = 42
	// ProtocolVersion is the wire version this build emits in hello/welcome.
	//
	// Current value 12 prepares the advertise-address legacy cleanup phase 2:
	// raising the network-wide MinimumProtocolVersion floor to 12 lets the
	// next release physically remove the deprecated advertise-address code
	// paths (observed-address-mismatch reconnect, hello-carried external IP,
	// CORSA_ADVERTISE_ADDRESS operator config) that were kept only for
	// dual-stack compatibility with peers below this floor. Until the floor
	// is raised, this build still ships those paths and accepts older peers;
	// the bump itself is what signals downstream operators that the floor
	// will move next. See docs/advertise-address-phase2-minproto12-cleanup.md
	// for the cleanup plan and preconditions.
	//
	// History: v11 introduced the advertise-address phase 1 deprecation
	// rollout (observed-address-mismatch no longer produced on the штатный
	// runtime path, hello carries the new additive field advertise_port,
	// inbound mismatch between observed TCP IP and hello.listen.host no
	// longer rejects the connection). v12 builds on top of that to enable
	// the floor raise; the wire change between v11 and v12 is additive
	// (no payload differences) but the floor semantics differ. Bump is
	// mandatory even though MinimumProtocolVersion stays at 8 in this build
	// — the floor raise itself is the v12 contract.
	ProtocolVersion        = 13
	MinimumProtocolVersion = 10
	DefaultOutgoingPeers   = 8
	DefaultPeerPort        = "64646"
)

func Default() Config {
	listenAddress := envOrDefault("CORSA_LISTEN_ADDRESS", ":"+DefaultPeerPort)
	nodeType := nodeTypeFromEnv()
	listenerEnabled, listenerSet := listenerFromEnv()
	// CORSA_ADVERTISE_ADDRESS is deprecated in the advertise-address phase 1
	// deprecation rollout: it is kept for backwards compatibility and as a
	// manual fallback, but it no longer participates in truth-of-advertise
	// selection. See docs/advertise-address-phase1-deprecation.md §7.1.
	advertiseAddress := envOrDefault("CORSA_ADVERTISE_ADDRESS", defaultAdvertiseAddress(listenAddress, listenerSet, listenerEnabled, nodeType))
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

	return Config{
		App: App{
			Name:     "corsa",
			Network:  "gazeta-devnet",
			Profile:  "default",
			Language: appLanguageFromEnv(),
			Version:  CorsaVersion,
		},
		Node: Node{
			ListenAddress:    listenAddress,
			AdvertiseAddress: advertiseAddress,
			AdvertisePort:    advertisePort,
			BootstrapPeers:   bootstrapPeers,
			IdentityPath:     identityPath,
			TrustStorePath:   trustStorePath,
			QueueStatePath:   queueStatePath,
			PeersStatePath:   peersStatePath,
			ChatLogDir:       chatLogDir,
			DownloadDir:      downloadDir,
			ProxyAddress:     proxyAddress,
			Type:             nodeType,
			ListenerEnabled:  listenerEnabled,
			ListenerSet:      listenerSet,
			ClientVersion:    wireClientVersion(CorsaVersion),
			MaxClockDrift:    maxClockDrift,
			MaxOutgoingPeers: maxOutgoingPeers,
			MaxIncomingPeers: maxIncomingPeers,
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

func defaultAdvertiseAddress(listenAddress string, listenerSet bool, listenerEnabled bool, nodeType NodeType) string {
	effectiveListener := listenerEnabled
	if !listenerSet {
		effectiveListener = nodeType != NodeTypeClient
	}
	if !effectiveListener {
		return ""
	}
	if strings.HasPrefix(listenAddress, ":") {
		return "127.0.0.1" + listenAddress
	}
	return listenAddress
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

func wireClientVersion(version string) string {
	value := strings.TrimSpace(version)
	if value == "" {
		return CorsaWireVersion
	}
	return strings.ReplaceAll(value, " ", "-")
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
