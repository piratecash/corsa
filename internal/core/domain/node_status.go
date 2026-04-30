package domain

import "time"

// NodeStatus is an immutable point-in-time snapshot of the local Corsa
// node's identity and liveness, returned by the getNodeStatus RPC
// endpoint.
//
// It is the integration surface defined by PIP-0001 ("Masternode
// Messenger Service Integration"): PirateCash Core uses this snapshot
// at masternode startup (Stage 1), as a liveness/version probe (Stage
// 2), and as the source of the public key the v21 proof-of-service
// will be tied to (Stage 3).
//
// Wire contract:
//
//   - Field names use explicit snake_case JSON tags. The struct is part
//     of an external integration; renaming fields silently would break
//     PirateCash Core checks. Add new fields, never repurpose names.
//   - All key material is PUBLIC. The struct intentionally does NOT
//     carry private keys, seeds, RPC credentials, or any other secret —
//     leaking those would defeat the integration's threat model. Any
//     future field MUST honor the same invariant.
//   - Stages 1 and 2 do not require a signature on the response.
//     Stage 3 (v21 PoSe) will introduce a separately-fetched signed
//     proof-of-service object; this snapshot stays signature-free so
//     it remains cheap to call from health-check loops.
type NodeStatus struct {
	// Identity is the node identity fingerprint (sha256 over the
	// ed25519 public key, hex-encoded). Stable for the lifetime of the
	// keypair and used as the routing identifier across the messenger
	// network. This is the PIP-0001-facing field name PirateCash Core
	// should consume.
	Identity string `json:"identity"`

	// Address is the Corsa-native name for the same identity
	// fingerprint. Kept alongside Identity because existing Corsa APIs
	// and docs use "address" for the local routing identifier.
	Address string `json:"address"`

	// PublicKey is the base64-encoded ed25519 public key that backs
	// Address. The v21 proof-of-service will sign challenges with the
	// matching private key; PirateCash Core verifies signatures against
	// the value reported here.
	PublicKey string `json:"public_key"`

	// BoxPublicKey is the base64-encoded NaCl curve25519 public key used
	// for end-to-end DM encryption. Returned so PirateCash Core can pin
	// the full identity material in a single call without a follow-up
	// fetch_contacts round-trip.
	BoxPublicKey string `json:"box_public_key"`

	// ProtocolVersion is the wire protocol version this node currently
	// speaks (config.ProtocolVersion). Stage 2 uses this to refuse
	// masternodes that fall behind the network.
	ProtocolVersion int `json:"protocol_version"`

	// MinimumProtocolVersion is the lowest peer protocol version this
	// node will accept on the wire (config.MinimumProtocolVersion).
	// Useful for PirateCash Core to detect impending hard cutovers.
	MinimumProtocolVersion int `json:"minimum_protocol_version"`

	// ClientVersion is the Corsa node implementation version
	// (Service.ClientVersion()). Free-form for informational logging on
	// the PirateCash side.
	ClientVersion string `json:"client_version"`

	// ClientBuild is the integer build identifier (config.ClientBuild)
	// monotonically bumped per release. Easier to compare than
	// ClientVersion when policy gates have to fire on a specific build.
	ClientBuild int `json:"client_build"`

	// ConnectedPeers is the number of **distinct peer identities** the
	// node currently has at least one live connection with, counting
	// both outbound and inbound sessions. Multiple sockets from the
	// same peer identity collapse to one — the field measures relay
	// reach, not socket count. Stage 2 may use this as a service-health
	// signal: a masternode reporting zero usable peers is unlikely to
	// be relaying messenger traffic.
	//
	// For per-connection detail (separate inbound/outbound rows,
	// remote addresses, slot lifecycle), use the getActiveConnections
	// command instead.
	ConnectedPeers int `json:"connected_peers"`

	// StartedAt is the wall-clock time the Service was constructed
	// (RFC3339Nano, UTC). Combined with CurrentTime it provides
	// uptime without requiring the caller to trust a single clock.
	StartedAt time.Time `json:"started_at"`

	// UptimeSeconds is the number of whole seconds elapsed since
	// StartedAt. Convenience field: equivalent to
	// CurrentTime.Sub(StartedAt) but avoids re-implementing the
	// arithmetic in every PirateCash health probe.
	UptimeSeconds int64 `json:"uptime_seconds"`

	// CurrentTime is the node's wall-clock time at the moment the
	// snapshot was generated (RFC3339Nano, UTC). PirateCash Core uses
	// this to detect drift between its own clock and the masternode's.
	CurrentTime time.Time `json:"current_time"`
}
