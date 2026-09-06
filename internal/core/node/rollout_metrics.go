package node

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/routing"
)

// Rollout telemetry owned by the node: how outbound attempts end, and what the
// live neighbourhood advertises.
//
// Both are read by fetchRouteSummary, which today acquires NO domain mutex of
// the Service (docs/locking.md). That property is a requirement, not an
// accident: the counters are atomics and the census is published as an
// immutable value behind an atomic.Pointer, so the RPC path stays exactly as
// lock-free as it was. A metric that made the hot read take peerMu would slow
// down the subsystem it was added to observe.
//
// Reference: docs/refactoring/dht/05-rollout-metrics.md.

// localAdvertisedCapabilities is what this node puts on the wire, for the
// telemetry that must say whose absence capped an announce ladder.
//
// Read live rather than captured once: part of the set depends on runtime
// readiness (the datagram roles), so a value frozen at construction would
// describe a node that never existed.
func (s *Service) localRoutingCapabilities() []routing.PeerCapability {
	return localCapabilities(s.cfg.EnableMeshRoutingV3, s.localDatagramAdvertise())
}

// rolloutCapabilities is the fixed list of capabilities the census reports.
//
// FIXED, and that is the whole design. The alternative — "report whatever the
// neighbours advertised" — makes the key space of a published map into
// something remote nodes choose, which is the defect ObserveUnknownDType
// refuses to have (datagram/metrics.go) and the one localRefusalDTypeCap caps.
// A capability this build has never heard of cannot be acted on anyway: it is
// counted in Connections like every other neighbour, and it gets no row of its
// own.
var rolloutCapabilities = []domain.Capability{
	domain.CapMeshRoutingV1,
	domain.CapMeshRoutingV2,
	domain.CapMeshRoutingV3,
	domain.CapMeshRelayV1,
	domain.CapMeshDatagramV1,
	domain.CapMeshDatagramTransitV1,
}

// sessionOutcomeCounters accumulates how outbound session attempts ended.
//
// One attempt increments exactly one outcome plus Attempts, so the four
// outcomes always sum to the denominator. That invariant is what makes a
// missing case visible: a failure mode nobody classified shows up as
// ErrorsOther rising, not as numbers that quietly stop adding up.
// There is deliberately NO separate attempts counter. Attempts is DERIVED from
// the four outcomes at read time, because a fifth atomic could not be
// incremented together with its outcome: a reader landing between the two
// increments — or loading attempts before a new attempt finished and succeeded
// after — would publish numbers that do not add up, and "they always add up"
// is the only property that makes the four readable. Deriving it makes the
// invariant hold by construction instead of by hoping the window is narrow.
type sessionOutcomeCounters struct {
	startedAt     time.Time
	succeeded     atomic.Uint64
	errorsConnect atomic.Uint64
	errorsCompat  atomic.Uint64
	errorsOther   atomic.Uint64
}

// record classifies one completed outbound attempt.
//
// Classification is by TYPED error, never by message text: errors.Is against
// the two sentinels the dial path already sets. Matching on strings would make
// the metric depend on wording that nobody treats as a contract, and it is
// forbidden for exactly that reason (CLAUDE.md, "Ошибки").
func (c *sessionOutcomeCounters) record(err error) {
	if c == nil {
		return
	}
	switch {
	case err == nil:
		c.succeeded.Add(1)
	case errors.Is(err, errIncompatibleProtocol):
		c.errorsCompat.Add(1)
	case errors.Is(err, errPeerDialTransport):
		c.errorsConnect.Add(1)
	default:
		c.errorsOther.Add(1)
	}
}

// snapshot projects the counters. Lock-free.
func (c *sessionOutcomeCounters) snapshot() domain.SessionOutcomeStats {
	if c == nil {
		return domain.SessionOutcomeStats{}
	}
	// Load each outcome ONCE and derive the total from those exact values, so
	// the published numbers are consistent with each other even though the
	// four loads are not atomic together. A concurrent attempt landing
	// mid-snapshot makes the total slightly stale — never contradictory, which
	// is the trade a reader can actually work with.
	stats := domain.SessionOutcomeStats{
		StartedAt: c.startedAt,
		// Stamped before the loads for the same reason the mode counters are:
		// a window that is slightly too wide under-states a rate, and a metric
		// that errs towards "quieter than reality" is the safer of the two.
		ReadAt:        time.Now().UTC(),
		Succeeded:     c.succeeded.Load(),
		ErrorsConnect: c.errorsConnect.Load(),
		ErrorsCompat:  c.errorsCompat.Load(),
		ErrorsOther:   c.errorsOther.Load(),
	}
	stats.Attempts = stats.Succeeded + stats.ErrorsConnect + stats.ErrorsCompat + stats.ErrorsOther
	return stats
}

// SessionOutcomeStats returns the cumulative outbound-attempt outcomes.
// Lock-free: the underlying counters are in-memory atomics.
func (s *Service) SessionOutcomeStats() domain.SessionOutcomeStats {
	return s.sessionOutcomes.snapshot()
}

// ModeSelectionStats returns which wire format each announce chose and why.
// Lock-free: the counters are atomics owned by routing.ModeSelectionCounters.
func (s *Service) ModeSelectionStats() routing.ModeSelectionStats {
	return s.modeSelection.Snapshot()
}

// NeighbourComposition returns the most recent census of live neighbours.
//
// Lock-free by construction: the census is built by a background refresh and
// published as an immutable value behind an atomic.Pointer, so this reader
// takes no lock, walks nothing, and cannot be made slower by the number of
// connections. Before the first refresh it reports Ready=false rather than a
// zeroed census, because "not measured yet" and "measured, nobody there" are
// different answers.
func (s *Service) NeighbourComposition() domain.NeighbourComposition {
	if snap := s.neighbourComposition.Load(); snap != nil {
		return *snap
	}
	return domain.NeighbourComposition{}
}

// announceTargets is the peersFn handed to the AnnounceLoop. It returns the
// announce targets and, on the same tick, refreshes the neighbour census.
//
// The refresh rides an existing cadence instead of owning a timer: the census
// is diagnostic, the announce interval is the rate at which peer state matters
// anyway, and a second ticker would be a second thing to stop, to test and to
// get wrong on shutdown.
//
// It is a WRAPPER rather than an addition inside routingCapablePeers because
// the two do different jobs. routingCapablePeers answers "who can receive an
// announce" — it filters to routing+relay — and the census must count the
// neighbours that filter removes, since an un-upgraded neighbour is precisely
// what a rollout is waiting for. Folding the census into the filter would have
// counted only the already-upgraded and reported a rollout that is always
// complete.
func (s *Service) announceTargets() []routing.AnnounceTarget {
	s.refreshNeighbourComposition()
	return s.routingCapablePeers()
}

// refreshNeighbourComposition walks live neighbour connections once and
// publishes an immutable census.
//
// ONE read lock for the whole walk, no I/O and no callbacks under it: the
// walk reads maps that peerMu guards, builds plain values, and the atomic
// store happens after the lock is released.
func (s *Service) refreshNeighbourComposition() {
	composition := s.collectNeighbourComposition()
	s.neighbourComposition.Store(&composition)
}

// advertisedNamesOf returns the RAW capability names the neighbour put on the
// wire.
//
// RAW, and this is the correction that matters. session.capabilities and
// connInfo.capabilities are the NEGOTIATED set — the intersection of ours and
// theirs — so a capability this build does not advertise is absent from them
// for every neighbour alive. A census built on that reports a fleet that never
// upgraded whenever the local config turns something off, and points the
// operator at the network instead of at their own configuration.
//
// nil is ambiguous by construction and stays that way: it means either "the
// peer advertised nothing this validator kept" or "a bounds breach emptied the
// set". Both are "we know of no advertisement", which is what the zero rows
// say — the census never turns that into a claim about the peer.
func advertisedNamesOf(declarations netcore.HandshakeDeclarations) []domain.CapabilityName {
	return declarations.AdvertisedNames
}

// advertisedContains reports whether the raw advertised set names capability.
func advertisedContains(advertised []domain.CapabilityName, capability domain.Capability) bool {
	target := domain.CapabilityName(capability)
	for _, name := range advertised {
		if name == target {
			return true
		}
	}
	return false
}

// advertisedSupportsV3 reports whether ONE connection advertised the complete
// v3 triplet. Evaluated on the raw set for the same reason the rows are.
func advertisedSupportsV3(advertised []domain.CapabilityName) bool {
	return advertisedContains(advertised, domain.CapMeshRoutingV1) &&
		advertisedContains(advertised, domain.CapMeshRoutingV3) &&
		advertisedContains(advertised, domain.CapMeshRelayV1)
}

// collectNeighbourComposition builds the census under a single peerMu.RLock.
func (s *Service) collectNeighbourComposition() domain.NeighbourComposition {
	// Per-capability tallies. connections counts sockets; peers is a set of
	// identities, so two sockets of one neighbour never look like two
	// neighbours.
	connCounts := make(map[domain.Capability]int, len(rolloutCapabilities))
	peerSets := make(map[domain.Capability]map[domain.PeerIdentity]struct{}, len(rolloutCapabilities))
	for _, capability := range rolloutCapabilities {
		peerSets[capability] = make(map[domain.PeerIdentity]struct{})
	}
	var (
		connections      int
		identityUnproven int
		identityUnknown  int
		tripletConns     int
	)
	tripletPeers := make(map[domain.PeerIdentity]struct{})
	allPeers := make(map[domain.PeerIdentity]struct{})

	// count folds ONE connection into the tallies.
	//
	// proven says whether the remote identity was proved to this node. Only an
	// accepted connection carries that proof: the handshake authenticates the
	// dialler to the listener, so on a session WE dialled the welcome address
	// is a name the remote picked (datagram.AuthorityClaimed is the zero value
	// for exactly this reason). The census reports the split instead of
	// pretending the two are the same kind of evidence.
	count := func(identity domain.PeerIdentity, advertised []domain.CapabilityName, proven bool) {
		connections++
		switch {
		case identity.IsZero():
			identityUnknown++
		case !proven:
			identityUnproven++
		}
		if !identity.IsZero() {
			allPeers[identity] = struct{}{}
		}
		for _, capability := range rolloutCapabilities {
			if !advertisedContains(advertised, capability) {
				continue
			}
			connCounts[capability]++
			if !identity.IsZero() {
				peerSets[capability][identity] = struct{}{}
			}
		}
		// The triplet is evaluated on THIS connection's advertised set. It is
		// never assembled from several connections of the same peer: a peer
		// whose two sockets advertise different halves supports the triplet on
		// neither, and claiming otherwise would send v3 frames down a
		// connection that refuses them.
		if advertisedSupportsV3(advertised) {
			tripletConns++
			if !identity.IsZero() {
				tripletPeers[identity] = struct{}{}
			}
		}
	}

	s.peerMu.RLock()
	for _, session := range s.sessions {
		// Outbound: we proved ourselves to them, they proved nothing to us.
		count(session.peerIdentity, advertisedNamesOf(session.declarations), false)
	}
	for id, entry := range s.conns {
		info, ok := snapshotEntryLocked(id, entry)
		if !ok || info.dir != netcore.Inbound || !info.tracked {
			continue
		}
		// Inbound: the remote signed our challenge, so its identity is proven.
		count(info.identity, advertisedNamesOf(entry.core.Declarations()), true)
	}
	s.peerMu.RUnlock()

	usage := make([]domain.CapabilityUsage, 0, len(rolloutCapabilities))
	for _, capability := range rolloutCapabilities {
		usage = append(usage, domain.CapabilityUsage{
			Capability:  capability,
			Connections: connCounts[capability],
			Peers:       len(peerSets[capability]),
		})
	}

	return domain.NeighbourComposition{
		Ready:            true,
		UpdatedAt:        time.Now().UTC(),
		Connections:      connections,
		Peers:            len(allPeers),
		IdentityUnproven: identityUnproven,
		IdentityUnknown:  identityUnknown,
		Capabilities:     usage,
		RoutingV3Triplet: domain.CapabilityUsage{
			Capability:  domain.CapMeshRoutingV3,
			Connections: tripletConns,
			Peers:       len(tripletPeers),
		},
	}
}
