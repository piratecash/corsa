package sdk

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	coreconfig "github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
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
//
// Identity resolution order:
//  1. PrivateKey — base64-encoded Ed25519 private key (preferred for SDK)
//  2. IdentityPath — path to an existing identity JSON file
//
// The SDK never auto-generates a new identity. If neither PrivateKey
// nor a valid IdentityPath with an existing file is provided,
// sdk.New returns an error.
type NodeConfig struct {
	ListenAddress string
	// AdvertisePort overrides the listening port published in
	// hello/welcome for peer discovery. nil means "use the
	// internal default" (config.DefaultPeerPort = 64646), matching
	// the operator-level CORSA_ADVERTISE_PORT fallback. Set this
	// explicitly whenever the SDK process binds to a non-default
	// port and other peers MUST be told to dial that port — bind
	// port and advertised port are independent (operators behind
	// NAT / port-forward typically run them on different values),
	// so the SDK never silently derives one from the other. Values
	// outside the inclusive 1..65535 range are ignored at the
	// boundary and collapse to the default fallback, matching
	// PeerPort.IsValid.
	AdvertisePort  *uint16
	BootstrapPeers []string
	// PrivateKey is the Base64 Ed25519 signing key — the single most
	// sensitive value in the whole config. json:"-" keeps it out of any
	// marshalled NodeConfig, and NodeConfig.String/GoString keep it out of
	// every fmt verb; both are covered by tests, because an embedder that
	// dumps its config for support is the likeliest way this key ever
	// leaves the machine.
	PrivateKey     string `json:"-"`
	IdentityPath   string
	TrustStorePath string
	PeersStatePath string
	ChatLogDir     string

	// StateDBPath overrides the shared SQLite state database location.
	// Empty keeps the historical chatlog-<identity_short>-<port>.db file
	// inside ChatLogDir. An explicit path is never populated from that
	// default: an empty file there means a deliberately new database.
	StateDBPath string

	DownloadDir      string
	ProxyAddress     string
	Type             NodeType
	ListenerEnabled  *bool
	ClientVersion    string
	MaxClockDrift    time.Duration
	MaxOutgoingPeers int
	MaxIncomingPeers int
	// HoldDMUntilReachable gates sender-owned DM emission on recipient
	// reachability (no blind gossip to unreachable recipients). nil means
	// "use the default", which is ENABLED — matching the operator default
	// and CORSA_HOLD_DM_UNTIL_REACHABLE. Set to a pointer to false to restore
	// the legacy blind-gossip baseline for an embedded/SDK runtime.
	HoldDMUntilReachable *bool
	// EnvelopeRetentionEnabled turns on the message-lifetime ceiling that
	// drops aged BROADCAST envelopes. nil means "use the default", which is
	// ENABLED — matching the operator default and CORSA_ENVELOPE_RETENTION.
	// Set to a pointer to false to restore the legacy no-ceiling behaviour.
	// Broadcast uses its built-in 24h default for SDK runtimes.
	//
	// Since protocol v30 there is NO transit ceiling: a relay no longer drops
	// somebody else's addressed message for being old, because doing so was
	// silent (no hop-ack) and the sender read the silence as a black hole.
	// Transit envelopes are bounded by the forwarding window instead.
	EnvelopeRetentionEnabled *bool
	// PoisonBatchEnabled batches poison-reverse fan-out (route_poison_v2)
	// toward v2-capable peers instead of one frame per identity. nil means
	// "use the default", which is ENABLED — matching the operator default and
	// CORSA_POISON_BATCH. Set to a pointer to false to force the legacy
	// per-identity v1 fan-out for an embedded/SDK runtime.
	PoisonBatchEnabled *bool
	// ProbeBackoffEnabled delays the Good→Questionable transition (60s→90s) for
	// proven-stable routes so they are actively probed less often; Bad/Dead are
	// unchanged, so failure detection is not slowed. nil means "use the
	// default", which is ENABLED — matching the operator default and
	// CORSA_PROBE_BACKOFF. Set to a pointer to false to restore the flat
	// 60/122/182s timeline for an embedded/SDK runtime.
	ProbeBackoffEnabled *bool
}

// redactedSecret is what every formatting verb prints in place of a secret.
const redactedSecret = "[redacted]"

// String redacts PrivateKey for %v, %s and %+v — an embedder dumping its
// config into a log or a support ticket is the likeliest way the signing key
// ever leaves the machine.
//
// The redaction goes through a local alias type on purpose: the alias has no
// String method, so fmt renders every other field of the copy without
// recursing, and a field added to NodeConfig later shows up here
// automatically instead of silently vanishing from the diagnostics.
func (c NodeConfig) String() string {
	type nodeConfigFields NodeConfig
	redacted := nodeConfigFields(c)
	if redacted.PrivateKey != "" {
		redacted.PrivateKey = redactedSecret
	}
	return fmt.Sprintf("sdk.NodeConfig%+v", redacted)
}

// GoString covers %#v, which ignores Stringer. It returns the same redacted
// text rather than valid Go syntax on purpose: a representation that could be
// pasted back into code is exactly what must not exist for a secret.
func (c NodeConfig) GoString() string {
	return c.String()
}

// Format renders the config for EVERY verb. String and GoString cover %v, %s,
// %+v and %#v and nothing else — a numeric verb (%d, %x) falls through to
// fmt's reflective walk and prints the key. Formatter is asked first for every
// verb, so no verb is left over.
func (c NodeConfig) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, c.String())
}

// RPCConfig configures the optional HTTP RPC server. Password is a secret and
// is kept out of marshalling and formatting the same way NodeConfig.PrivateKey
// is.
type RPCConfig struct {
	Enabled  bool
	Host     string
	Port     string
	Username string
	Password string `json:"-"`
}

// String redacts the password for %v, %s and %+v.
func (c RPCConfig) String() string {
	type rpcConfigFields RPCConfig
	redacted := rpcConfigFields(c)
	if redacted.Password != "" {
		redacted.Password = redactedSecret
	}
	return fmt.Sprintf("sdk.RPCConfig%+v", redacted)
}

// GoString covers %#v — see NodeConfig.GoString.
func (c RPCConfig) GoString() string {
	return c.String()
}

// Format covers every verb — see NodeConfig.Format.
func (c RPCConfig) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, c.String())
}

// Config is the public SDK configuration.
type Config struct {
	App  AppConfig
	Node NodeConfig
	RPC  RPCConfig
}

// withoutSecrets returns a copy with every secret cleared.
//
// This is what a long-lived struct is allowed to keep. Redaction through
// String/GoString protects the value only where fmt can reach a method, and
// fmt reaches none through an UNEXPORTED field: printing a struct that holds
// a Config privately walks it by reflection and prints the key verbatim, and
// numeric verbs bypass Stringer even on exported paths. Neither hole can be
// closed by the config's own methods — so the secret does not stay in the
// struct that outlives its use.
//
// The secrets are consumed once, during construction: the private key becomes
// an identity.Identity (itself fail-closed against serialisation) and the RPC
// password goes into the server's own config before this copy is stored.
func (c Config) withoutSecrets() Config {
	c.Node.PrivateKey = ""
	c.RPC.Password = ""
	return c
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
			ListenAddress:  listenAddress,
			BootstrapPeers: []string{net.JoinHostPort("65.108.204.190", coreconfig.DefaultPeerPort)},
			IdentityPath:   filepath.Join(".corsa", "identity-"+portSuffix(listenAddress)+".json"),
			TrustStorePath: filepath.Join(".corsa", "trust-"+portSuffix(listenAddress)+".json"),
			PeersStatePath: filepath.Join(".corsa", "peers-"+portSuffix(listenAddress)+".json"),
			ChatLogDir:     ".corsa",
			Type:           NodeTypeFull,
			ClientVersion:  coreconfig.CorsaVersion,
			MaxClockDrift:  protocol.DefaultMessageTimeDrift,
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

	// AdvertisePort optional-mapping. Modelled as *uint16 at the SDK
	// boundary so the absence-of-value state is type-visible (matches
	// ListenerEnabled *bool); converted to *domain.PeerPort here so
	// the internal layers continue consuming the validated domain
	// type. Out-of-range values (PeerPort.IsValid == false) collapse
	// to nil so EffectiveAdvertisePort falls back to DefaultPeerPort
	// — same semantics CORSA_ADVERTISE_PORT applies on the operator
	// side. The conversion lives at this single boundary so domain
	// types do not leak into the public SDK API.
	var advertisePort *domain.PeerPort
	if cfg.Node.AdvertisePort != nil {
		port := domain.PeerPort(*cfg.Node.AdvertisePort)
		if port.IsValid() {
			advertisePort = &port
		}
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
			AdvertisePort:    advertisePort,
			BootstrapPeers:   append([]string(nil), cfg.Node.BootstrapPeers...),
			IdentityPath:     cfg.Node.IdentityPath,
			TrustStorePath:   cfg.Node.TrustStorePath,
			PeersStatePath:   cfg.Node.PeersStatePath,
			ChatLogDir:       cfg.Node.ChatLogDir,
			StateDBPath:      cfg.Node.StateDBPath,
			DownloadDir:      cfg.Node.DownloadDir,
			ProxyAddress:     cfg.Node.ProxyAddress,
			Type:             nodeType,
			ListenerEnabled:  listenerEnabled,
			ListenerSet:      listenerSet,
			ClientVersion:    cfg.Node.ClientVersion,
			MaxClockDrift:    cfg.Node.MaxClockDrift,
			MaxOutgoingPeers: cfg.Node.MaxOutgoingPeers,
			MaxIncomingPeers: cfg.Node.MaxIncomingPeers,
			// Default ON (the storm cure) unless the embedder explicitly
			// opts out via a non-nil pointer to false.
			HoldDMUntilReachable:     cfg.Node.HoldDMUntilReachable == nil || *cfg.Node.HoldDMUntilReachable,
			EnvelopeRetentionEnabled: cfg.Node.EnvelopeRetentionEnabled == nil || *cfg.Node.EnvelopeRetentionEnabled,
			PoisonBatchEnabled:       cfg.Node.PoisonBatchEnabled == nil || *cfg.Node.PoisonBatchEnabled,
			ProbeBackoffEnabled:      cfg.Node.ProbeBackoffEnabled == nil || *cfg.Node.ProbeBackoffEnabled,
			// BroadcastMaxAge left zero → the node-package default (24h)
			// applies when retention is enabled. There is no transit
			// counterpart: the transit age ceiling was removed with protocol
			// v30 (see node/envelope_retention.go). GossipFanoutLimit /
			// TransitForwardOnce stay at their opt-in OFF defaults.
		},
		RPC: coreconfig.RPC{
			Host:     cfg.RPC.Host,
			Port:     cfg.RPC.Port,
			Username: cfg.RPC.Username,
			Password: cfg.RPC.Password,
		},
	}
}
