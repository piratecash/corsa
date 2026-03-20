package config

import (
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
	Type             NodeType
	ListenerEnabled  bool
	ListenerSet      bool
	ClientVersion    string
	MaxClockDrift    time.Duration
	MaxOutgoingPeers int
	MaxIncomingPeers int
}

type Config struct {
	App  App
	Node Node
}

const (
	CorsaVersion           = "0.9 alpha"
	CorsaWireVersion       = "0.9-alpha"
	ProtocolVersion        = 1
	MinimumProtocolVersion = 1
	DefaultOutgoingPeers   = 8
)

func Default() Config {
	listenAddress := envOrDefault("CORSA_LISTEN_ADDRESS", ":64646")
	nodeType := nodeTypeFromEnv()
	listenerEnabled, listenerSet := listenerFromEnv()
	advertiseAddress := envOrDefault("CORSA_ADVERTISE_ADDRESS", defaultAdvertiseAddress(listenAddress, listenerSet, listenerEnabled, nodeType))
	bootstrapPeers := bootstrapPeersFromEnv(listenAddress)
	identityPath := envOrDefault("CORSA_IDENTITY_PATH", defaultIdentityPath(listenAddress))
	trustStorePath := envOrDefault("CORSA_TRUST_STORE_PATH", defaultTrustStorePath(listenAddress))
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
			Type:             nodeType,
			ListenerEnabled:  listenerEnabled,
			ListenerSet:      listenerSet,
			ClientVersion:    wireClientVersion(CorsaVersion),
			MaxClockDrift:    maxClockDrift,
			MaxOutgoingPeers: maxOutgoingPeers,
			MaxIncomingPeers: maxIncomingPeers,
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
	return []string{"65.108.204.190:64646"}
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
