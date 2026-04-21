package service

import (
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/transport"
)

// AppInfo is an immutable snapshot of application + identity configuration.
//
// It replaces the config-accessor portion of the former DesktopClient
// "god-service" so that callers that only need config data do not depend on
// the full desktop orchestrator. All methods are read-only and safe to call
// from any goroutine — AppInfo holds no mutable state.
//
// AppInfo is constructed once at process start by NewDesktopClient and
// passed by value to consumers. Sub-services that need configuration embed
// it instead of the raw *DesktopClient.
type AppInfo struct {
	appCfg  config.App
	nodeCfg config.Node
	id      *identity.Identity
}

// NewAppInfo builds an AppInfo from the typed configuration slices and the
// resolved identity. The identity pointer is kept by reference so the same
// key material is shared across all sub-services without copying.
func NewAppInfo(appCfg config.App, nodeCfg config.Node, id *identity.Identity) AppInfo {
	return AppInfo{appCfg: appCfg, nodeCfg: nodeCfg, id: id}
}

// NetworkName returns the configured network name (e.g. "mainnet", "testnet").
func (a AppInfo) NetworkName() string { return a.appCfg.Network }

// ProfileName returns the active configuration profile.
func (a AppInfo) ProfileName() string { return a.appCfg.Profile }

// AppName returns the human-readable application name.
func (a AppInfo) AppName() string { return a.appCfg.Name }

// Language returns the configured UI language tag.
func (a AppInfo) Language() string { return a.appCfg.Language }

// Version returns the application version string.
func (a AppInfo) Version() string { return a.appCfg.Version }

// DesktopVersion is an alias for Version preserved for the rpc.DiagnosticProvider
// interface, which distinguishes the desktop client from the generic RPC surface.
func (a AppInfo) DesktopVersion() string { return a.appCfg.Version }

// ListenAddress returns the configured local listener address.
func (a AppInfo) ListenAddress() string { return a.nodeCfg.ListenAddress }

// Address returns the local node identity as a typed domain value.
func (a AppInfo) Address() domain.PeerIdentity { return domain.PeerIdentity(a.id.Address) }

// Identity returns the raw identity pointer. Use sparingly — prefer Address()
// for read-only consumers.
func (a AppInfo) Identity() *identity.Identity { return a.id }

// ChatLogDir returns the effective directory where the local chatlog store
// persists SQLite data.
func (a AppInfo) ChatLogDir() string { return a.nodeCfg.EffectiveChatLogDir() }

// TransmitDir returns the absolute path to the directory where files awaiting
// transfer are stored.
func (a AppInfo) TransmitDir() string {
	return filepath.Join(a.nodeCfg.EffectiveDataDir(), domain.TransmitSubdir)
}

// BootstrapPeers materialises the configured bootstrap peer list into the
// transport-layer representation used by the connection manager.
func (a AppInfo) BootstrapPeers() []transport.Peer {
	peers := make([]transport.Peer, 0, len(a.nodeCfg.BootstrapPeers))
	for _, addr := range a.nodeCfg.BootstrapPeers {
		peers = append(peers, transport.Peer{
			Address: domain.PeerAddress(addr),
			Source:  domain.PeerSourceBootstrap,
		})
	}
	return peers
}

// AppConfig returns the raw typed application config. Kept for sub-services
// (LocalRPCClient, NodeProber) that need to forge hello frames carrying the
// protocol version and client build. External callers should prefer the
// dedicated getters above.
func (a AppInfo) AppConfig() config.App { return a.appCfg }

// NodeConfig returns the raw typed node config for sub-services that need
// access to bootstrap lists, listen address, or data directory paths beyond
// the dedicated getters above.
func (a AppInfo) NodeConfig() config.Node { return a.nodeCfg }
