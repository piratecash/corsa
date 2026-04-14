package node

import (
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// peerExchangePath labels the call-site from which a peer exchange decision
// originated. It is used only for observability so that logs can distinguish
// the CM initial-sync path, the legacy outbound session path, the legacy
// direct-dial path and the forced-refresh narrow-recovery paths.
//
// These constants are NOT part of any wire or RPC contract and must stay in
// the node layer. Do not leak them across transport boundaries.
type peerExchangePath string

const (
	peerExchangePathSessionOutbound       peerExchangePath = "session_outbound"
	peerExchangePathSessionCM             peerExchangePath = "session_cm"
	peerExchangePathLegacyDial            peerExchangePath = "legacy_dial"
	peerExchangePathSenderKeyViaSession   peerExchangePath = "sender_key_via_session"
	peerExchangePathSenderKeyFreshDial    peerExchangePath = "sender_key_fresh_dial"
	peerExchangePathUnknownSenderRecovery peerExchangePath = "unknown_sender_recovery"
)

// peerExchangeSkipReason explains why a particular sync did not send
// get_peers. See docs/peer-discovery-conditional-get-peers.ru.md Step 6.
type peerExchangeSkipReason string

const (
	// peerExchangeSkipByAggregateHealthy — policy decided against peer
	// exchange because the aggregate node/network status is already healthy
	// and steady-state discovery should rely on announce_peer gossip.
	peerExchangeSkipByAggregateHealthy peerExchangeSkipReason = "aggregate_status_healthy"

	// peerExchangeSkipByNarrowRecovery — the caller is a forced-refresh
	// narrow recovery path (sender-key / unknown-sender) that must never
	// initiate peer exchange regardless of aggregate status.
	peerExchangeSkipByNarrowRecovery peerExchangeSkipReason = "narrow_recovery"
)

// shouldRequestPeers decides whether the node should send a get_peers request
// during the current sync cycle. The decision is based on the materialized
// aggregate network status of the node.
//
// Policy:
//   - If aggregate status is NOT healthy (offline, reconnecting, limited,
//     warning), the node needs to discover more peers quickly → return true.
//   - If aggregate status IS healthy, peer discovery should rely on the
//     background announce_peer gossip mechanism → return false.
//
// The function is pure in the sense that it performs no I/O and no network
// actions. It reads the already-materialized aggregate status snapshot via the
// thread-safe AggregateStatus() getter, so it is safe to call from any
// goroutine without holding locks.
//
// The policy is re-evaluated at each sync call site, not fixed at startup.
//
// See docs/peer-discovery-conditional-get-peers.ru.md Step 2b.
func (s *Service) shouldRequestPeers() bool {
	snap := s.AggregateStatus()

	needed := !snap.Status.IsHealthy()

	log.Debug().
		Str("aggregate_status", snap.Status.String()).
		Int("usable_peers", snap.UsablePeers).
		Int("connected_peers", snap.ConnectedPeers).
		Int("total_peers", snap.TotalPeers).
		Bool("peer_exchange_needed", needed).
		Msg("peer_exchange_policy_evaluated")

	return needed
}

// logPeerExchangeSkipped records that a sync cycle consciously skipped
// get_peers. The reason distinguishes a policy-driven skip (aggregate status
// already healthy) from a forced-refresh narrow-recovery skip, so that
// operators can tell a steady-state pass from a regression in which narrow
// recovery wrongly widens into peer exchange.
//
// The snapshot is captured here rather than passed in from the caller: a
// single AggregateStatus() RLock call is cheap, and it keeps the observability
// surface consistent across all four skip sites without adding a parameter to
// syncPeer / syncPeerSession.
//
// See docs/peer-discovery-conditional-get-peers.ru.md Step 6.
func (s *Service) logPeerExchangeSkipped(
	path peerExchangePath,
	peer domain.PeerAddress,
	reason peerExchangeSkipReason,
) {
	snap := s.AggregateStatus()
	log.Info().
		Str("path", string(path)).
		Str("peer", string(peer)).
		Str("reason", string(reason)).
		Str("aggregate_status", snap.Status.String()).
		Int("usable_peers", snap.UsablePeers).
		Int("connected_peers", snap.ConnectedPeers).
		Msg("peer_exchange_skipped")
}

// logPeerExchangeExecuted records that get_peers was actually sent and the
// response was received. peersReceived is the raw number of addresses in the
// wire response; peersImported is the number that addPeerAddress() actually
// stored — i.e. new peers after self-address filtering, same-IP collapse,
// and blocked-destination checks. Operators need both: the delta (received −
// imported) signals how much of the response was redundant, which is the
// core steady-state vs cold-start comparison the document promises.
//
// See docs/peer-discovery-conditional-get-peers.ru.md Step 6.
func (s *Service) logPeerExchangeExecuted(
	path peerExchangePath,
	peer domain.PeerAddress,
	peersReceived int,
	peersImported int,
) {
	snap := s.AggregateStatus()
	log.Info().
		Str("path", string(path)).
		Str("peer", string(peer)).
		Int("peers_received", peersReceived).
		Int("peers_imported", peersImported).
		Str("aggregate_status", snap.Status.String()).
		Int("usable_peers", snap.UsablePeers).
		Int("connected_peers", snap.ConnectedPeers).
		Msg("peer_exchange_executed")
}
