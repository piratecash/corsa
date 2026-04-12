package node

import (
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// PeerProvider — single source of dial candidates
// ---------------------------------------------------------------------------
//
// PeerProvider aggregates all known peers from every discovery source
// (bootstrap, persisted, peer exchange, announce, manual), deduplicates
// them by IP, applies filtering (banned, connected, queued, cooldown,
// network reachability), and returns a sorted candidate list for
// ConnectionManager.
//
// Scoring and health tracking remain in Service — PeerProvider reads
// them through callback functions (healthFn, bannedIPsFn, etc.) only
// when Candidates() is called.

// knownPeer is the internal record stored by PeerProvider.
type knownPeer struct {
	Address    domain.PeerAddress
	Source     domain.PeerSource
	IP         string
	Port       string
	Network    domain.NetGroup
	AddedAt    time.Time
	FreshUntil *time.Time
}

// PeerHealthView is the subset of peerHealth that PeerProvider needs
// for filtering and scoring. Returned by healthFn callback.
// Internal to node package — not exposed beyond Service ↔ PeerProvider
// boundary.
type PeerHealthView struct {
	Score               int
	ConsecutiveFailures int
	LastDisconnectedAt  time.Time
	BannedUntil         time.Time
	Connected           bool
}

// PeerProviderConfig holds all callback functions that PeerProvider uses
// to read external state. All fields are required.
//
// SNAPSHOT CONTRACT: Every callback that returns a map or pointer must
// return a fresh snapshot (new map / copied struct). PeerProvider iterates
// results without holding the caller's lock, so returning a live mutable
// map will cause concurrent map read/write panics when Service mutates it.
// Stage 3 wiring must copy maps before returning them.
type PeerProviderConfig struct {
	// HealthFn returns a snapshot of health view for a peer address.
	// Nil means no health record. The returned *PeerHealthView must not
	// be mutated by the caller after return.
	HealthFn func(domain.PeerAddress) *PeerHealthView

	// ConnectedFn returns a snapshot copy of IPs that already have an active
	// connection (both inbound and outbound). Must be a new map each call.
	ConnectedFn func() map[string]struct{}

	// QueuedFn returns a snapshot copy of IPs currently in ConnectionManager
	// queue. Must be a new map each call.
	QueuedFn func() map[string]struct{}

	// ForbiddenFn checks whether an IP must never be dialled
	// (loopback when not allowed, self-address, link-local, etc.).
	ForbiddenFn func(net.IP) bool

	// NetworksFn returns a snapshot copy of reachable NetGroups for this node.
	// Must be a new map each call.
	NetworksFn func() map[domain.NetGroup]struct{}

	// BannedIPsFn returns a snapshot copy of the current IP-wide ban set
	// from Service.bannedIPSet. Must be a new map each call.
	BannedIPsFn func() map[string]domain.BannedIPEntry

	// ListenAddr is this node's own listen address (for self-filtering).
	ListenAddr domain.ListenAddress

	// DefaultPort is the default peer port (for fallback dial addresses).
	DefaultPort string

	// IsSelfAddress checks if an address is self (handles advertise, listen, external).
	IsSelfAddress func(domain.PeerAddress) bool

	// NowFn returns the current time. Injected for testability.
	NowFn func() time.Time
}

// PeerProvider manages the set of known peers and produces filtered
// candidate lists for ConnectionManager.
type PeerProvider struct {
	mu     sync.RWMutex
	known  map[domain.PeerAddress]*knownPeer
	config PeerProviderConfig
}

const (
	// Freshly discovered peers get a short-lived ranking bump so the next
	// candidate selection can try them without bypassing normal filters.
	freshPeerTTL   = 2 * time.Minute
	freshPeerBonus = 5
)

// NewPeerProvider creates a PeerProvider with the given configuration.
func NewPeerProvider(cfg PeerProviderConfig) *PeerProvider {
	if cfg.NowFn == nil {
		cfg.NowFn = time.Now
	}
	if cfg.DefaultPort == "" {
		cfg.DefaultPort = config.DefaultPeerPort
	}
	return &PeerProvider{
		known:  make(map[domain.PeerAddress]*knownPeer),
		config: cfg,
	}
}

// Add registers a peer discovered at runtime (peer exchange, announce, manual).
// If the address already exists, Source is updated only if the new source
// has higher priority (bootstrap > others). AddedAt and Network are never
// overwritten by Add — use Restore for startup loading.
func (pp *PeerProvider) Add(address domain.PeerAddress, source domain.PeerSource) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	if address == "" {
		return
	}

	host, port, ok := splitHostPort(string(address))
	if !ok {
		return
	}

	network := classifyAddress(address)

	pp.mu.Lock()
	defer pp.mu.Unlock()

	if existing, exists := pp.known[address]; exists {
		// Merge rule: bootstrap source takes priority.
		if source == domain.PeerSourceBootstrap {
			existing.Source = source
		}
		return
	}

	pp.known[address] = &knownPeer{
		Address: address,
		Source:  source,
		IP:      host,
		Port:    port,
		Network: network,
		AddedAt: pp.config.NowFn(),
	}
}

// Promote updates an existing peer's Source and refreshes AddedAt so that it
// sorts higher in Candidates(). Used by add_peer to move a known peer to the
// front of the dial queue. If the address is unknown, it is added as new.
func (pp *PeerProvider) Promote(address domain.PeerAddress, source domain.PeerSource) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	if address == "" {
		return
	}

	host, port, ok := splitHostPort(string(address))
	if !ok {
		return
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	if existing, exists := pp.known[address]; exists {
		existing.Source = source
		existing.AddedAt = pp.config.NowFn()
		return
	}

	pp.known[address] = &knownPeer{
		Address: address,
		Source:  source,
		IP:      host,
		Port:    port,
		Network: classifyAddress(address),
		AddedAt: pp.config.NowFn(),
	}
}

// MarkFresh gives a known peer a temporary candidate-ranking boost.
// Callers use it only on first discovery; repeated sightings do not refresh it.
func (pp *PeerProvider) MarkFresh(address domain.PeerAddress, ttl time.Duration) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	if address == "" {
		return
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	peer := pp.known[address]
	if peer == nil {
		return
	}

	freshUntil := pp.config.NowFn().Add(ttl)
	if peer.FreshUntil != nil && peer.FreshUntil.After(freshUntil) {
		return
	}
	peer.FreshUntil = &freshUntil
}

// Restore loads a peer from persisted state (peers.json) at startup.
// Preserves all static fields from the persisted entry.
func (pp *PeerProvider) Restore(entry domain.RestoreEntry) {
	address := domain.PeerAddress(strings.TrimSpace(string(entry.Address)))
	if address == "" {
		return
	}

	host, port, ok := splitHostPort(string(address))
	if !ok {
		return
	}

	network := entry.Network
	if network == "" {
		network = classifyAddress(address)
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.known[address] = &knownPeer{
		Address: address,
		Source:  entry.Source,
		IP:      host,
		Port:    port,
		Network: network,
		AddedAt: entry.AddedAt,
	}
}

// Remove deletes a peer from the known set.
func (pp *PeerProvider) Remove(address domain.PeerAddress) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	delete(pp.known, address)
}

// Count returns the number of known peers.
func (pp *PeerProvider) Count() int {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	return len(pp.known)
}

// Candidates returns the filtered, deduplicated, sorted list of dial
// candidates. Implements the full 10-step algorithm from the spec.
func (pp *PeerProvider) Candidates() []domain.CandidatePeer {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	cfg := pp.config
	now := cfg.NowFn()

	// Step 1: connected IPs (inbound + outbound).
	connectedIPs := cfg.ConnectedFn()

	// Step 2: queued IPs in CM.
	queuedIPs := cfg.QueuedFn()

	// Step 3: forbidden IPs — checked per-peer via ForbiddenFn.

	// Step 4: reachable network groups.
	reachableGroups := cfg.NetworksFn()

	// Step 5: build bannedIPs set from two sources.
	bannedIPs := pp.buildBannedIPsSet(now)

	// Steps 6-8: filter, score, and deduplicate.
	type scored struct {
		peer    *knownPeer
		score   int
		addedAt time.Time
	}

	// First pass: filter peers, collect scored entries.
	var filtered []scored
	for _, kp := range pp.known {
		// 6a: skip self-address.
		if cfg.IsSelfAddress != nil && cfg.IsSelfAddress(kp.Address) {
			continue
		}

		// 6a.1: never resurrect loopback / RFC1918 IPv4 peers from persisted
		// state. They may be valid in local dev, but stale private addresses
		// from peers.json are not useful dial candidates after restart.
		if shouldSkipPersistedPrivatePeer(kp) {
			continue
		}

		// 6b: skip forbidden IPs.
		ip := net.ParseIP(kp.IP)
		if cfg.ForbiddenFn != nil && cfg.ForbiddenFn(ip) {
			continue
		}

		// 6c: skip banned IPs.
		if _, banned := bannedIPs[kp.IP]; banned {
			continue
		}

		// 6d: skip already connected IPs.
		if _, connected := connectedIPs[kp.IP]; connected {
			continue
		}

		// 6e: skip already queued IPs.
		if _, queued := queuedIPs[kp.IP]; queued {
			continue
		}

		// 6f: check health — cooldown.
		peerScore := 0
		if cfg.HealthFn != nil {
			if h := cfg.HealthFn(kp.Address); h != nil {
				peerScore = h.Score

				// Skip peers in cooldown.
				// A single failure does NOT trigger cooldown — the peer gets
				// an immediate retry. This avoids stalling reconnection when
				// a peer was simply not started yet.
				if h.ConsecutiveFailures > 1 && !h.LastDisconnectedAt.IsZero() {
					cooldown := peerCooldownDuration(h.ConsecutiveFailures - 1)
					if now.Sub(h.LastDisconnectedAt) < cooldown {
						continue
					}
				}
			}
		}
		if kp.FreshUntil != nil && now.Before(*kp.FreshUntil) {
			peerScore += freshPeerBonus
		}

		// 6g: skip unreachable network groups.
		if _, reachable := reachableGroups[kp.Network]; !reachable {
			continue
		}

		filtered = append(filtered, scored{
			peer:    kp,
			score:   peerScore,
			addedAt: kp.AddedAt,
		})
	}

	// Step 7: deduplicate by IP — keep entry with max score.
	// Tie-breaking mirrors the final sort: score desc → AddedAt asc → Address asc.
	// Without the Address tie-breaker, two ports on the same IP with equal
	// score and AddedAt would flap depending on map iteration order.
	bestByIP := make(map[string]scored, len(filtered))
	for _, s := range filtered {
		existing, ok := bestByIP[s.peer.IP]
		if !ok {
			bestByIP[s.peer.IP] = s
			continue
		}
		switch {
		case s.score > existing.score:
			bestByIP[s.peer.IP] = s
		case s.score == existing.score && s.addedAt.Before(existing.addedAt):
			bestByIP[s.peer.IP] = s
		case s.score == existing.score && s.addedAt.Equal(existing.addedAt) && s.peer.Address < existing.peer.Address:
			bestByIP[s.peer.IP] = s
		}
	}

	// Collect deduplicated entries.
	deduped := make([]scored, 0, len(bestByIP))
	for _, s := range bestByIP {
		deduped = append(deduped, s)
	}

	// Step 8: sort by score desc, tie-break by AddedAt asc,
	// then by Address asc for full determinism when score and AddedAt
	// are equal (e.g. bootstrap peers added in the same batch).
	sort.Slice(deduped, func(i, j int) bool {
		if deduped[i].score != deduped[j].score {
			return deduped[i].score > deduped[j].score
		}
		if !deduped[i].addedAt.Equal(deduped[j].addedAt) {
			return deduped[i].addedAt.Before(deduped[j].addedAt)
		}
		return deduped[i].peer.Address < deduped[j].peer.Address
	})

	// Steps 9-10: generate DialAddresses and build result.
	result := make([]domain.CandidatePeer, 0, len(deduped))
	for _, s := range deduped {
		dialAddrs := pp.buildDialAddresses(s.peer)
		if len(dialAddrs) == 0 {
			continue
		}
		result = append(result, domain.CandidatePeer{
			Address:       s.peer.Address,
			DialAddresses: dialAddrs,
			Source:        s.peer.Source,
			Score:         s.score,
		})
	}

	return result
}

// KnownPeers returns the full unfiltered list of all known peers with
// diagnostic ExcludeReasons explaining why each was excluded from
// Candidates() (if applicable). Empty ExcludeReasons means the peer
// is a valid candidate.
func (pp *PeerProvider) KnownPeers() []domain.KnownPeerInfo {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	cfg := pp.config
	now := cfg.NowFn()

	connectedIPs := cfg.ConnectedFn()
	queuedIPs := cfg.QueuedFn()
	reachableGroups := cfg.NetworksFn()
	bannedIPs := pp.buildBannedIPsSet(now)

	result := make([]domain.KnownPeerInfo, 0, len(pp.known))

	for _, kp := range pp.known {
		var reasons []domain.ExcludeReason

		if shouldSkipPersistedPrivatePeer(kp) {
			reasons = append(reasons, domain.ExcludeReasonForbidden)
		}

		// Forbidden check.
		ip := net.ParseIP(kp.IP)
		if cfg.ForbiddenFn != nil && cfg.ForbiddenFn(ip) {
			if len(reasons) == 0 || reasons[len(reasons)-1] != domain.ExcludeReasonForbidden {
				reasons = append(reasons, domain.ExcludeReasonForbidden)
			}
		}

		// Self-address check — also falls under forbidden.
		if cfg.IsSelfAddress != nil && cfg.IsSelfAddress(kp.Address) {
			if len(reasons) == 0 || reasons[len(reasons)-1] != domain.ExcludeReasonForbidden {
				reasons = append(reasons, domain.ExcludeReasonForbidden)
			}
		}

		// Banned (IP-wide).
		if _, banned := bannedIPs[kp.IP]; banned {
			reasons = append(reasons, domain.ExcludeReasonBanned)
		}

		// Cache health snapshot once per peer to avoid inconsistency
		// from multiple HealthFn calls against mutable state.
		var health *PeerHealthView
		if cfg.HealthFn != nil {
			health = cfg.HealthFn(kp.Address)
		}

		// Banned direct (per-address).
		if health != nil && !health.BannedUntil.IsZero() && now.Before(health.BannedUntil) {
			reasons = append(reasons, domain.ExcludeReasonBannedDirect)
		}

		// Cooldown.
		if health != nil && health.ConsecutiveFailures > 1 && !health.LastDisconnectedAt.IsZero() {
			cooldown := peerCooldownDuration(health.ConsecutiveFailures - 1)
			if now.Sub(health.LastDisconnectedAt) < cooldown {
				reasons = append(reasons, domain.ExcludeReasonCooldown)
			}
		}

		// Connected.
		if _, connected := connectedIPs[kp.IP]; connected {
			reasons = append(reasons, domain.ExcludeReasonConnected)
		}

		// Queued.
		if _, queued := queuedIPs[kp.IP]; queued {
			reasons = append(reasons, domain.ExcludeReasonQueued)
		}

		// Unreachable.
		if _, reachable := reachableGroups[kp.Network]; !reachable {
			reasons = append(reasons, domain.ExcludeReasonUnreachable)
		}

		peerScore := 0
		failures := 0
		var bannedUntil time.Time
		isConnected := false
		if health != nil {
			peerScore = health.Score
			failures = health.ConsecutiveFailures
			bannedUntil = health.BannedUntil
			isConnected = health.Connected
		}
		// Reflect IP-level connectivity: if any port on this IP is
		// connected, this address is effectively connected too.
		// Keeps Connected consistent with ExcludeReasons=[connected].
		if _, ipConnected := connectedIPs[kp.IP]; ipConnected {
			isConnected = true
		}

		// If this peer's IP is banned (IP-wide or propagated from another
		// port), reflect the effective ban expiry so operators see a
		// non-zero BannedUntil consistent with the ExcludeReasons=[banned].
		if entry, ipBanned := bannedIPs[kp.IP]; ipBanned {
			if entry.BannedUntil.After(bannedUntil) {
				bannedUntil = entry.BannedUntil
			}
		}

		result = append(result, domain.KnownPeerInfo{
			Address:        kp.Address,
			Source:         kp.Source,
			AddedAt:        kp.AddedAt,
			Network:        kp.Network,
			Score:          peerScore,
			Failures:       failures,
			BannedUntil:    bannedUntil,
			Connected:      isConnected,
			ExcludeReasons: reasons,
		})
	}

	// IP dedup pass: among peers with no other exclude reasons, only the
	// highest-scoring entry per IP actually appears in Candidates(). Mark
	// the rest with ExcludeReasonIPDedup so list_peers accurately explains
	// why they are not dial candidates despite passing all other filters.
	type ipWinner struct {
		score   int
		addedAt time.Time
		address domain.PeerAddress
		idx     int
	}
	bestByIP := make(map[string]ipWinner)
	for i := range result {
		r := &result[i]
		if len(r.ExcludeReasons) > 0 {
			continue // already excluded by other filters
		}
		kp := pp.known[r.Address]
		if kp == nil {
			continue
		}
		ip := kp.IP
		existing, ok := bestByIP[ip]
		if !ok {
			bestByIP[ip] = ipWinner{score: r.Score, addedAt: r.AddedAt, address: r.Address, idx: i}
			continue
		}
		// Same tie-breaking as Candidates() Step 7.
		replace := false
		switch {
		case r.Score > existing.score:
			replace = true
		case r.Score == existing.score && r.AddedAt.Before(existing.addedAt):
			replace = true
		case r.Score == existing.score && r.AddedAt.Equal(existing.addedAt) && r.Address < existing.address:
			replace = true
		}
		if replace {
			// Previous winner loses — mark it as deduped.
			result[existing.idx].ExcludeReasons = append(result[existing.idx].ExcludeReasons, domain.ExcludeReasonIPDedup)
			bestByIP[ip] = ipWinner{score: r.Score, addedAt: r.AddedAt, address: r.Address, idx: i}
		} else {
			// Current entry loses.
			r.ExcludeReasons = append(r.ExcludeReasons, domain.ExcludeReasonIPDedup)
		}
	}

	// Sort for deterministic output: score desc, AddedAt asc, Address asc.
	// Third tie-breaker by Address ensures stable ordering when score and
	// timestamp are equal (same treatment as Candidates sort).
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score > result[j].Score
		}
		if !result[i].AddedAt.Equal(result[j].AddedAt) {
			return result[i].AddedAt.Before(result[j].AddedAt)
		}
		return result[i].Address < result[j].Address
	})

	return result
}

// BannedIPs returns the list of banned IPs with origin and affected peers.
func (pp *PeerProvider) BannedIPs() []domain.BannedIPInfo {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	cfg := pp.config
	now := cfg.NowFn()

	// Collect all ban sources.
	bannedIPs := pp.buildBannedIPsSet(now)

	// For each banned IP, merge persisted AffectedPeers (survives
	// top-500 trim) with live pp.known scan (catches newly discovered
	// peers on the same IP). Dedup by address.
	result := make([]domain.BannedIPInfo, 0, len(bannedIPs))
	for ip, entry := range bannedIPs {
		seen := make(map[domain.PeerAddress]struct{})
		var affected []domain.PeerAddress

		// Persisted affected peers from ban creation time.
		for _, a := range entry.AffectedPeers {
			if _, dup := seen[a]; !dup {
				seen[a] = struct{}{}
				affected = append(affected, a)
			}
		}
		// Live scan — catch peers added after the ban.
		for _, kp := range pp.known {
			if kp.IP == ip {
				if _, dup := seen[kp.Address]; !dup {
					seen[kp.Address] = struct{}{}
					affected = append(affected, kp.Address)
				}
			}
		}
		// Sort affected for deterministic output.
		sort.Slice(affected, func(i, j int) bool {
			return affected[i] < affected[j]
		})

		result = append(result, domain.BannedIPInfo{
			IP:            ip,
			BannedUntil:   entry.BannedUntil,
			BanOrigin:     entry.BanOrigin,
			BanReason:     entry.BanReason,
			AffectedPeers: affected,
		})
	}

	// Sort by IP for deterministic output.
	sort.Slice(result, func(i, j int) bool {
		return result[i].IP < result[j].IP
	})

	return result
}

// Addresses returns all known peer addresses (for persistence building).
func (pp *PeerProvider) Addresses() []domain.PeerAddress {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	addrs := make([]domain.PeerAddress, 0, len(pp.known))
	for addr := range pp.known {
		addrs = append(addrs, addr)
	}
	return addrs
}

// KnownPeerStatic returns the static fields of a known peer for persistence.
// Returns nil if the address is not known.
func (pp *PeerProvider) KnownPeerStatic(address domain.PeerAddress) *knownPeer {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	kp, ok := pp.known[address]
	if !ok {
		return nil
	}
	// Return a copy to avoid mutation.
	cp := *kp
	return &cp
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// buildBannedIPsSet constructs the set of banned IPs from two sources:
// (a) Service.bannedIPSet (IP-wide bans) via bannedIPsFn,
// (b) per-address BannedUntil from healthFn (direct ban → whole IP banned).
// Must be called with pp.mu held (read lock).
func (pp *PeerProvider) buildBannedIPsSet(now time.Time) map[string]domain.BannedIPEntry {
	cfg := pp.config
	banned := make(map[string]domain.BannedIPEntry)

	// Source (a): IP-wide bans from Service.bannedIPSet.
	if cfg.BannedIPsFn != nil {
		for ip, entry := range cfg.BannedIPsFn() {
			if now.Before(entry.BannedUntil) {
				banned[ip] = entry
			}
		}
	}

	// Source (b): per-address BannedUntil → propagate to whole IP.
	// Multiple ports on the same IP may have different BannedUntil times.
	// We aggregate by taking the maximum BannedUntil so the IP stays
	// banned until ALL ports' bans expire. BanOrigin tracks the port
	// that produced the latest ban.
	if cfg.HealthFn != nil {
		for _, kp := range pp.known {
			h := cfg.HealthFn(kp.Address)
			if h == nil {
				continue
			}
			if h.BannedUntil.IsZero() || !now.Before(h.BannedUntil) {
				continue
			}
			existing, exists := banned[kp.IP]
			if exists && !h.BannedUntil.After(existing.BannedUntil) {
				// Already banned with equal or later expiry — keep existing.
				continue
			}
			banned[kp.IP] = domain.BannedIPEntry{
				IP:          kp.IP,
				BannedUntil: h.BannedUntil,
				BanOrigin:   kp.Address,
				BanReason:   "incompatible_protocol",
			}
		}
	}

	return banned
}

// buildDialAddresses generates the ordered list of addresses to try for
// a given peer: [0] primary, [1] fallback (default port) if applicable.
// Delegates to BuildDialAddresses for the actual logic.
func (pp *PeerProvider) buildDialAddresses(kp *knownPeer) []domain.PeerAddress {
	return pp.BuildDialAddresses(kp.Address)
}

func shouldSkipPersistedPrivatePeer(kp *knownPeer) bool {
	if kp == nil || kp.Source != domain.PeerSourcePersisted {
		return false
	}
	return isLoopbackOrPrivateIPv4(net.ParseIP(kp.IP))
}

// BuildDialAddresses generates the ordered dial address list for an
// arbitrary address: [0] primary, [1] fallback (default port) when the
// primary uses a non-default port on a non-loopback, non-forbidden IP.
//
// This is the public counterpart of buildDialAddresses — intended for
// callers that have a raw domain.PeerAddress but no *knownPeer (e.g.
// addPeerFrame → ManualPeerRequested).
func (pp *PeerProvider) BuildDialAddresses(address domain.PeerAddress) []domain.PeerAddress {
	host, port, err := net.SplitHostPort(string(address))
	if err != nil {
		return []domain.PeerAddress{address}
	}
	addresses := []domain.PeerAddress{address}

	if port != pp.config.DefaultPort {
		ip := net.ParseIP(host)
		if ip != nil && !ip.IsLoopback() {
			if pp.config.ForbiddenFn == nil || !pp.config.ForbiddenFn(ip) {
				fallback := domain.PeerAddress(net.JoinHostPort(host, pp.config.DefaultPort))
				addresses = append(addresses, fallback)
			}
		}
	}

	return addresses
}

// Score returns the current peer score for the given address.
// Returns 0 if the address is unknown or has no health record.
// Thread-safe: acquires read lock internally.
func (pp *PeerProvider) Score(address domain.PeerAddress) int {
	if pp.config.HealthFn == nil {
		return 0
	}
	h := pp.config.HealthFn(address)
	if h == nil {
		return 0
	}
	return h.Score
}
