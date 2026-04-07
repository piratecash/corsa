package sdk

import (
	"net"
	"path/filepath"
	"strings"
	"time"

	coreconfig "github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// NodeType describes the node role exposed by the public SDK.
type NodeType string

const (
	NodeTypeFull   NodeType = "full"
	NodeTypeClient NodeType = "client"
)

// AppConfig contains SDK-level app metadata.
type AppConfig struct {
	Name     string
	Network  string
	Profile  string
	Language string
	Version  string
}

// NodeConfig contains node runtime settings configured directly in Go code.
// ListenerEnabled is optional: nil means "use role-based default".
type NodeConfig struct {
	ListenAddress    string
	AdvertiseAddress string
	BootstrapPeers   []string
	IdentityPath     string
	TrustStorePath   string
	QueueStatePath   string
	PeersStatePath   string
	ChatLogDir       string
	DownloadDir      string
	ProxyAddress     string
	Type             NodeType
	ListenerEnabled  *bool
	ClientVersion    string
	MaxClockDrift    time.Duration
	MaxOutgoingPeers int
	MaxIncomingPeers int
}

// RPCConfig configures the optional HTTP RPC server.
type RPCConfig struct {
	Enabled  bool
	Host     string
	Port     string
	Username string
	Password string
}

// Config is the public SDK configuration.
type Config struct {
	App  AppConfig
	Node NodeConfig
	RPC  RPCConfig
}

// DefaultConfig returns SDK defaults without reading environment variables.
func DefaultConfig() Config {
	listenAddress := ":" + coreconfig.DefaultPeerPort
	return Config{
		App: AppConfig{
			Name:     "corsa",
			Network:  "gazeta-devnet",
			Profile:  "default",
			Language: "en",
			Version:  coreconfig.CorsaVersion,
		},
		Node: NodeConfig{
			ListenAddress:    listenAddress,
			AdvertiseAddress: "127.0.0.1" + listenAddress,
			BootstrapPeers:   []string{net.JoinHostPort("65.108.204.190", coreconfig.DefaultPeerPort)},
			IdentityPath:     filepath.Join(".corsa", "identity-"+portSuffix(listenAddress)+".json"),
			TrustStorePath:   filepath.Join(".corsa", "trust-"+portSuffix(listenAddress)+".json"),
			QueueStatePath:   filepath.Join(".corsa", "queue-"+portSuffix(listenAddress)+".json"),
			PeersStatePath:   filepath.Join(".corsa", "peers-"+portSuffix(listenAddress)+".json"),
			ChatLogDir:       ".corsa",
			Type:             NodeTypeFull,
			ClientVersion:    coreconfig.CorsaWireVersion,
			MaxClockDrift:    protocol.DefaultMessageTimeDrift,
		},
		RPC: RPCConfig{
			Enabled: false,
			Host:    "127.0.0.1",
			Port:    "46464",
		},
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func portSuffix(listenAddress string) string {
	port := "default"
	if idx := strings.LastIndex(listenAddress, ":"); idx >= 0 && idx < len(listenAddress)-1 {
		port = listenAddress[idx+1:]
	}
	return port
}

func normalizeConfig(cfg Config) Config {
	base := DefaultConfig()

	if strings.TrimSpace(cfg.App.Name) == "" {
		cfg.App.Name = base.App.Name
	}
	if strings.TrimSpace(cfg.App.Network) == "" {
		cfg.App.Network = base.App.Network
	}
	if strings.TrimSpace(cfg.App.Profile) == "" {
		cfg.App.Profile = base.App.Profile
	}
	if strings.TrimSpace(cfg.App.Language) == "" {
		cfg.App.Language = base.App.Language
	}
	if strings.TrimSpace(cfg.App.Version) == "" {
		cfg.App.Version = base.App.Version
	}

	if strings.TrimSpace(cfg.Node.ListenAddress) == "" {
		cfg.Node.ListenAddress = base.Node.ListenAddress
	}
	if cfg.Node.Type == "" {
		cfg.Node.Type = base.Node.Type
	}
	if cfg.Node.ListenerEnabled == nil {
		switch cfg.Node.Type {
		case NodeTypeClient:
			cfg.Node.ListenerEnabled = boolPtr(false)
		default:
			cfg.Node.ListenerEnabled = boolPtr(true)
		}
	}
	if strings.TrimSpace(cfg.Node.AdvertiseAddress) == "" && cfg.Node.ListenerEnabled != nil && *cfg.Node.ListenerEnabled {
		if strings.HasPrefix(cfg.Node.ListenAddress, ":") {
			cfg.Node.AdvertiseAddress = "127.0.0.1" + cfg.Node.ListenAddress
		} else {
			cfg.Node.AdvertiseAddress = cfg.Node.ListenAddress
		}
	}
	if cfg.Node.BootstrapPeers == nil {
		cfg.Node.BootstrapPeers = append([]string(nil), base.Node.BootstrapPeers...)
	}

	port := portSuffix(cfg.Node.ListenAddress)
	if strings.TrimSpace(cfg.Node.IdentityPath) == "" {
		cfg.Node.IdentityPath = filepath.Join(".corsa", "identity-"+port+".json")
	}
	if strings.TrimSpace(cfg.Node.TrustStorePath) == "" {
		cfg.Node.TrustStorePath = filepath.Join(".corsa", "trust-"+port+".json")
	}
	if strings.TrimSpace(cfg.Node.QueueStatePath) == "" {
		cfg.Node.QueueStatePath = filepath.Join(".corsa", "queue-"+port+".json")
	}
	if strings.TrimSpace(cfg.Node.PeersStatePath) == "" {
		cfg.Node.PeersStatePath = filepath.Join(".corsa", "peers-"+port+".json")
	}
	if strings.TrimSpace(cfg.Node.ChatLogDir) == "" {
		cfg.Node.ChatLogDir = base.Node.ChatLogDir
	}
	if strings.TrimSpace(cfg.Node.ClientVersion) == "" {
		cfg.Node.ClientVersion = base.Node.ClientVersion
	}
	if cfg.Node.MaxClockDrift <= 0 {
		cfg.Node.MaxClockDrift = base.Node.MaxClockDrift
	}

	if strings.TrimSpace(cfg.RPC.Host) == "" {
		cfg.RPC.Host = base.RPC.Host
	}
	if strings.TrimSpace(cfg.RPC.Port) == "" {
		cfg.RPC.Port = base.RPC.Port
	}

	return cfg
}

func (c Config) internal() coreconfig.Config {
	cfg := normalizeConfig(c)

	nodeType := coreconfig.NodeTypeFull
	if cfg.Node.Type == NodeTypeClient {
		nodeType = coreconfig.NodeTypeClient
	}

	listenerEnabled := false
	listenerSet := false
	if cfg.Node.ListenerEnabled != nil {
		listenerEnabled = *cfg.Node.ListenerEnabled
		listenerSet = true
	}

	return coreconfig.Config{
		App: coreconfig.App{
			Name:     cfg.App.Name,
			Network:  cfg.App.Network,
			Profile:  cfg.App.Profile,
			Language: cfg.App.Language,
			Version:  cfg.App.Version,
		},
		Node: coreconfig.Node{
			ListenAddress:    cfg.Node.ListenAddress,
			AdvertiseAddress: cfg.Node.AdvertiseAddress,
			BootstrapPeers:   append([]string(nil), cfg.Node.BootstrapPeers...),
			IdentityPath:     cfg.Node.IdentityPath,
			TrustStorePath:   cfg.Node.TrustStorePath,
			QueueStatePath:   cfg.Node.QueueStatePath,
			PeersStatePath:   cfg.Node.PeersStatePath,
			ChatLogDir:       cfg.Node.ChatLogDir,
			DownloadDir:      cfg.Node.DownloadDir,
			ProxyAddress:     cfg.Node.ProxyAddress,
			Type:             nodeType,
			ListenerEnabled:  listenerEnabled,
			ListenerSet:      listenerSet,
			ClientVersion:    cfg.Node.ClientVersion,
			MaxClockDrift:    cfg.Node.MaxClockDrift,
			MaxOutgoingPeers: cfg.Node.MaxOutgoingPeers,
			MaxIncomingPeers: cfg.Node.MaxIncomingPeers,
		},
		RPC: coreconfig.RPC{
			Host:     cfg.RPC.Host,
			Port:     cfg.RPC.Port,
			Username: cfg.RPC.Username,
			Password: cfg.RPC.Password,
		},
	}
}
