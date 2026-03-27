package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"corsa/internal/core/protocol"
)

type App struct {
	Name     string
	Network  string
	Profile  string
	Language string
	Version  string
}

type NodeType string

const (
	NodeTypeFull   NodeType = "full"
	NodeTypeClient NodeType = "client"
)

type Node struct {
	ListenAddress    string
	AdvertiseAddress string
	BootstrapPeers   []string
	IdentityPath     string
	TrustStorePath   string
	QueueStatePath   string
	PeersStatePath   string
	ChatLogDir       string // directory for chatlog .jsonl files (defaults to ".corsa")
	ProxyAddress     string // SOCKS5 proxy for .onion peers (e.g. "127.0.0.1:9050")
	Type             NodeType
	ListenerEnabled  bool
	ListenerSet      bool
	ClientVersion    string
	MaxClockDrift    time.Duration
	MaxOutgoingPeers int
	MaxIncomingPeers int
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
	CorsaVersion           = "0.16 alpha"
	CorsaWireVersion       = "0.16-alpha"
	ProtocolVersion        = 2
	MinimumProtocolVersion = 2
	DefaultOutgoingPeers   = 8
	DefaultPeerPort        = "64646"
)

func Default() Config {
	listenAddress := envOrDefault("CORSA_LISTEN_ADDRESS", ":"+DefaultPeerPort)
	nodeType := nodeTypeFromEnv()
	listenerEnabled, listenerSet := listenerFromEnv()
	advertiseAddress := envOrDefault("CORSA_ADVERTISE_ADDRESS", defaultAdvertiseAddress(listenAddress, listenerSet, listenerEnabled, nodeType))
	bootstrapPeers := bootstrapPeersFromEnv(listenAddress)
	identityPath := envOrDefault("CORSA_IDENTITY_PATH", defaultIdentityPath(listenAddress))
	trustStorePath := envOrDefault("CORSA_TRUST_STORE_PATH", defaultTrustStorePath(listenAddress))
	queueStatePath := envOrDefault("CORSA_QUEUE_STATE_PATH", defaultQueueStatePath(listenAddress))
	peersStatePath := envOrDefault("CORSA_PEERS_PATH", defaultPeersStatePath(listenAddress))
	chatLogDir := envOrDefault("CORSA_CHATLOG_DIR", defaultChatLogDir())
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
			BootstrapPeers:   bootstrapPeers,
			IdentityPath:     identityPath,
			TrustStorePath:   trustStorePath,
			QueueStatePath:   queueStatePath,
			PeersStatePath:   peersStatePath,
			ChatLogDir:       chatLogDir,
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

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func defaultIdentityPath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(".corsa", "identity-"+port+".json")
}

func defaultTrustStorePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(".corsa", "trust-"+port+".json")
}

func defaultQueueStatePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(".corsa", "queue-"+port+".json")
}

func defaultPeersStatePath(listenAddress string) string {
	port := portSuffix(listenAddress)

	return filepath.Join(".corsa", "peers-"+port+".json")
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
	return []string{net.JoinHostPort("65.108.204.190", DefaultPeerPort)}
}

func nodeTypeFromEnv() NodeType {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CORSA_NODE_TYPE"))) {
	case string(NodeTypeClient):
		return NodeTypeClient
	default:
		return NodeTypeFull
	}
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
	return ".corsa"
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
