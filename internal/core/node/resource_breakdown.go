package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// resource_breakdown.go assembles the answer ResourceUsage cannot give: not
// how much this process holds, but WHO holds it.
//
// The rules it follows are the measurement step's, and both of them are about
// not disturbing the node it measures:
//
//   - no container is walked whose KEY SPACE is unbounded. Most numbers are a
//     len, a counter the owner already maintains, or a figure the coalesced
//     routing snapshot has already paid for. Three sums iterate a map to add
//     up its slice lengths, and each is named here with the bound that makes
//     it affordable, because "we do not walk anything" was the claim this file
//     made first and it was not true:
//
//     topics — a handful, fixed by the protocol;
//     announce peers — tens, bounded by the connection count;
//     pending frames — at most maxPendingFramesTotal keys (2000), since the
//     admission gate refuses beyond that total and a map cannot hold more
//     keys than frames.
//
//     Two containers are NOT summed for exactly this reason and report their
//     cardinality instead: the receipt backlog is keyed by recipient with no
//     cap on how many recipients there are, and the observed-IP history is
//     keyed by peer address. Reporting how many recipients hold a backlog is
//     a smaller answer than how many receipts they hold — and a bounded one;
//   - no two subsystems are sampled under one lock. Each domain mutex is
//     taken, read and released on its own, in the canonical order
//     (docs/locking.md), and nothing external is called while one is held. A
//     globally consistent sample would need every domain lock at once, which
//     is a stall in exchange for a tidier timestamp.
//
// What comes out is a floor, and domain.ResourceBreakdown says so: counts are
// exact, per-entry costs exclude what an entry points at, and Go's own map
// overhead is not counted at all. A floor from exact counts is worth more than
// an estimate from a walk nobody can afford to run on a busy node.
//
// Reference: docs/refactoring/dht/13-measurements.md §2, §4, §5.

// Per-entry costs, resolved once at initialisation.
var (
	pendingFrameBytes     = domain.SizeOfAll(domain.PeerAddress(""), pendingFrame{})
	relayAttemptBytes     = domain.SizeOfAll("", relayAttempt{})
	outboundBytes         = domain.SizeOfAll("", outboundDelivery{})
	deliveryRetryBytes    = domain.SizeOfAll(protocol.MessageID(""), deliveryRetryEntry{})
	receiptRecipientBytes = domain.SizeOfAll("", []protocol.DeliveryReceipt(nil))
	envelopeBytes         = domain.SizeOfAll(protocol.Envelope{})
	messageIDBytes        = domain.SizeOfAll(protocol.MessageID(""))
	sessionBytes          = domain.SizeOfAll(domain.PeerAddress(""), peerSession{})
	peerHealthBytes       = domain.SizeOfAll(domain.PeerAddress(""), peerHealth{})
	connEntryBytes        = domain.SizeOfAll(netcore.ConnID(0), connEntry{})
	keyMaterialBytes      = domain.SizeOfAll("", "")
	banEntryBytes         = domain.SizeOfAll("", banEntry{})
	bannedIPBytes         = domain.SizeOfAll("", domain.BannedIPEntry{})
	remoteBanBytes        = domain.SizeOfAll("", remoteIPBanEntry{})
	observedIPPeerBytes   = domain.SizeOfAll(domain.PeerAddress(""), []domain.PeerIP(nil))
	knownIdentityBytes    = domain.SizeOfAll(domain.PeerIdentity{}, time.Time{})
	receiptDedupKeyBytes  = domain.SizeOfAll([16]byte{})
)

// ResourceBreakdown reports which subsystem holds what, right now.
//
// It is a pure read: no domain mutex is held across another, nothing is
// allocated per entry, and the heaviest thing it does is take each domain
// RLock once. Surfaced by the getResourceBreakdown RPC command.
func (s *Service) ResourceBreakdown() domain.ResourceBreakdown {
	sampledAt := time.Now().UTC()
	// Canonical lock order, one domain at a time: peerMu → deliveryMu →
	// knowledgeMu → gossipMu → ipStateMu. Each helper takes and releases its
	// own, so no two are ever held together and no ordering edge is created.
	subsystems := []domain.SubsystemUsage{
		s.routePlaneUsage(),
		s.announceUsage(),
		s.datagramUsage(),
		s.sessionsUsage(),
		s.deliveryUsage(),
		s.knowledgeUsage(),
		s.banUsage(),
	}
	return domain.NewResourceBreakdown(sampledAt, subsystems...)
}

// FetchResourceBreakdown renders the breakdown for an operator.
//
// It is a SEPARATE command from getResourceUsage and not an extension of it,
// for one reason that decides the whole shape: the desktop client samples
// resource usage once a second to draw the Info tab, and that sampler has no
// use for a per-subsystem breakdown. Folding the two would make every node
// with a UI attached pay for a dozen domain-lock acquisitions per second to
// render numbers nothing displays.
//
// Wire schema:
//
//	{
//	  "sampled_at": "2026-09-05T12:00:00Z",
//	  "floor_bytes": 47458816,
//	  "floor_human": "45.26 MB",
//	  "dominant": "route_plane",              // omitted while the node holds nothing
//	  "subsystems": [
//	    {
//	      "subsystem": "route_plane",
//	      "floor_bytes": 41000000,
//	      "floor_human": "39.10 MB",
//	      "gauges": [
//	        {"name": "route_claims", "kind": "memory", "count": 320000,
//	         "entry_bytes": 128, "floor_bytes": 40960000, "floor_human": "39.06 MB"}
//	      ]
//	    }
//	  ]
//	}
//
// Every byte figure is a FLOOR and is named one: counts are exact, but a
// per-entry cost covers the key and the value a container stores and not what
// those point at, nor Go's own map overhead. Real consumption is higher by a
// factor that differs per container — compare it against getResourceUsage's
// process figures rather than expecting the two to add up.
//
// A gauge of kind "saturation" contributes no bytes to any total. Its count is
// an occupancy to be read against a limit, and the entries behind it are a
// SUBSET of entries some memory gauge has already counted — adding them would
// report the same records twice and leave the floor above the truth.
func (s *Service) FetchResourceBreakdown() (json.RawMessage, error) {
	breakdown := s.ResourceBreakdown()

	type wireGauge struct {
		Name string `json:"name"`
		// Kind separates the two questions a gauge answers. A "memory" gauge's
		// bytes are part of the totals; a "saturation" gauge reports how full a
		// quota is and contributes NOTHING, because the entries behind it are a
		// subset of ones a memory gauge already counted. Without this field a
		// reader would see a zero floor beside a non-zero count and read it as
		// a bug rather than as the deliberate refusal to count twice.
		Kind       string `json:"kind"`
		Count      uint64 `json:"count"`
		EntryBytes uint64 `json:"entry_bytes"`
		FloorBytes uint64 `json:"floor_bytes"`
		FloorHuman string `json:"floor_human"`
	}
	type wireSubsystem struct {
		Subsystem  string      `json:"subsystem"`
		Gauges     []wireGauge `json:"gauges"`
		FloorBytes uint64      `json:"floor_bytes"`
		FloorHuman string      `json:"floor_human"`
	}

	usages := breakdown.Subsystems()
	subsystems := make([]wireSubsystem, 0, len(usages))
	for _, usage := range usages {
		gauges := usage.Gauges()
		wire := wireSubsystem{
			Subsystem:  usage.Subsystem().String(),
			Gauges:     make([]wireGauge, 0, len(gauges)),
			FloorBytes: usage.FloorBytes(),
			FloorHuman: formatBytes(usage.FloorBytes()),
		}
		for _, gauge := range gauges {
			wire.Gauges = append(wire.Gauges, wireGauge{
				Name:       gauge.Name(),
				Kind:       gauge.Kind().String(),
				Count:      gauge.Count(),
				EntryBytes: gauge.EntryBytes(),
				FloorBytes: gauge.FloorBytes(),
				FloorHuman: formatBytes(gauge.FloorBytes()),
			})
		}
		subsystems = append(subsystems, wire)
	}

	answer := map[string]any{
		"sampled_at":  breakdown.SampledAt().Format(time.RFC3339Nano),
		"floor_bytes": breakdown.FloorBytes(),
		"floor_human": formatBytes(breakdown.FloorBytes()),
		"subsystems":  subsystems,
	}
	// Omitted rather than reported as a guess: a node that has just started
	// holds nothing, and naming an arbitrary subsystem its dominant consumer
	// would be an answer with no content behind it.
	if dominant, named := breakdown.Dominant(); named {
		answer["dominant"] = dominant.Subsystem().String()
	}

	data, err := json.Marshal(answer)
	if err != nil {
		return nil, fmt.Errorf("marshal resource breakdown: %w", err)
	}
	return data, nil
}

// routePlaneUsage reports the routing table's cardinalities, plus the one
// number the table cannot cheaply produce.
//
// The claim count is a sum over every per-identity bucket. There is no
// maintained counter for it, and counting it live would be exactly the walk
// under t.mu this whole surface refuses — so it is read from the coalesced
// snapshot, where the incremental publisher already computed it as a
// by-product. The price is that this one figure lags by the snapshot's
// republish interval; that is the right trade for a gauge nobody watches at
// sub-second resolution.
func (s *Service) routePlaneUsage() domain.SubsystemUsage {
	if s.routingTable == nil {
		return domain.NewSubsystemUsage(domain.ResourceSubsystemRoutePlane)
	}
	usage := s.routingTable.Usage()
	snapshot := s.loadRoutingSnapshot()
	claims := domain.NewResourceGauge("route_claims", snapshot.TotalEntries, routing.UplinkClaimBytes())
	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemRoutePlane,
		append([]domain.ResourceGauge{claims}, usage.Gauges()...)...,
	)
}

// announceUsage reports what the announce loop keeps on each peer's behalf.
func (s *Service) announceUsage() domain.SubsystemUsage {
	if s.announceLoop == nil {
		return domain.NewSubsystemUsage(domain.ResourceSubsystemAnnounce)
	}
	registry := s.announceLoop.StateRegistry()
	if registry == nil {
		return domain.NewSubsystemUsage(domain.ResourceSubsystemAnnounce)
	}
	return registry.Usage()
}

// datagramUsage reports the datagram plane, or an empty subsystem on a node
// built without it — which is a real deployment and not a failure.
func (s *Service) datagramUsage() domain.SubsystemUsage {
	layer := s.datagramLayer()
	if layer == nil {
		return domain.NewSubsystemUsage(domain.ResourceSubsystemDatagram)
	}
	return datagram.CollectUsage(
		layer.queue, layer.replayCache, layer.reverse, layer.admission, layer.scheduler,
	)
}

// sessionsUsage reports what live connections cost in per-peer records.
//
// The fixed buffers a socket allocates — the writer channel, the inbox, the
// read buffer — are NOT counted per entry here: they are a constant per
// connection rather than a property of these maps, and the constant is
// measured on the bench rather than asserted in a comment (13-measurements.md
// §2, "стоимость одной сессии").
func (s *Service) sessionsUsage() domain.SubsystemUsage {
	s.peerMu.RLock()
	sessions := len(s.sessions)
	health := len(s.health)
	conns := len(s.conns)
	s.peerMu.RUnlock()

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemSessions,
		domain.NewResourceGauge("peer_health", health, peerHealthBytes),
		domain.NewResourceGauge("sessions", sessions, sessionBytes),
		domain.NewResourceGauge("connections", conns, connEntryBytes),
	)
}

// deliveryUsage reports the message-delivery domain and the transit backlog.
//
// Two sums iterate here and both are bounded: pending frames by
// maxPendingFramesTotal, transit envelopes by the topic count. The receipt
// backlog is deliberately NOT summed — it is keyed by recipient and nothing
// caps how many recipients there are, so summing it would put an unbounded
// walk under deliveryMu on a node whose delivery path is exactly what a
// diagnostic must not delay. Its cardinality is reported instead: fewer
// answers, but an affordable one.
func (s *Service) deliveryUsage() domain.SubsystemUsage {
	s.deliveryMu.RLock()
	pending := 0
	for _, frames := range s.pending {
		pending += len(frames)
	}
	relayRetry := len(s.relayRetry)
	outbound := len(s.outbound)
	awaiting := len(s.awaitingDelivered)
	receiptRecipients := len(s.receipts)
	frozen := len(s.frozenDeliveries)
	neverEmitted := len(s.markedNeverEmitted)
	sentIDs := s.sentDMIDs.Len()
	s.deliveryMu.RUnlock()

	// seenReceipts keeps its own leaf mutex, asked outside the domain lock.
	//
	// StoredLen, not Len: Len walks the whole previous generation — up to
	// maxReceiptDedupEntries keys — to subtract the overlap between the two,
	// and it takes this mutex to do it. finishReceipt takes the SAME mutex
	// while holding deliveryMu, so asking for a diagnostic would put a
	// 50 000-key scan in front of the delivery domain. StoredLen is two len
	// reads, and for a memory figure it is also the more correct question: a
	// key present in both generations occupies a slot in each.
	seenReceipts := s.seenReceipts.StoredLen()

	s.gossipMu.RLock()
	envelopes := 0
	for _, backlog := range s.topics {
		envelopes += len(backlog)
	}
	s.gossipMu.RUnlock()

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemDelivery,
		// The transit backlog: other people's messages this node is carrying.
		// Bounded by a byte ceiling rather than a count, so a large number
		// here is not by itself a fault.
		domain.NewResourceGauge("transit_envelopes", envelopes, envelopeBytes),
		domain.NewResourceGauge("pending_frames", pending, pendingFrameBytes),
		// Recipients holding a backlog, not receipts held. Each backlog is
		// capped per recipient; the number of recipients is not, which is
		// precisely why this counts keys rather than contents.
		domain.NewResourceGauge("receipt_recipients", receiptRecipients, receiptRecipientBytes),
		// A dedup set that once grew without a bound and cost ~30 MB an hour.
		// It is here so the next such growth is visible before it is a report.
		domain.NewResourceGauge("receipt_dedup", seenReceipts, receiptDedupKeyBytes),
		domain.NewResourceGauge("sent_message_ids", sentIDs, messageIDBytes),
		domain.NewResourceGauge("relay_retry", relayRetry, relayAttemptBytes),
		domain.NewResourceGauge("outbound_deliveries", outbound, outboundBytes),
		domain.NewResourceGauge("awaiting_receipt", awaiting, deliveryRetryBytes),
		domain.NewResourceGauge("frozen_deliveries", frozen, messageIDBytes),
		domain.NewResourceGauge("never_emitted_marks", neverEmitted, messageIDBytes),
	)
}

// knowledgeUsage reports the identity cache and the key material hanging off
// it.
func (s *Service) knowledgeUsage() domain.SubsystemUsage {
	s.knowledgeMu.RLock()
	known := s.known.Len()
	boxKeys := len(s.boxKeys)
	pubKeys := len(s.pubKeys)
	boxSigs := len(s.boxSigs)
	s.knowledgeMu.RUnlock()

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemKnowledge,
		domain.NewResourceGauge("known_identities", known, knownIdentityBytes),
		domain.NewResourceGauge("box_keys", boxKeys, keyMaterialBytes),
		domain.NewResourceGauge("public_keys", pubKeys, keyMaterialBytes),
		domain.NewResourceGauge("box_signatures", boxSigs, keyMaterialBytes),
	)
}

// banUsage reports the IP-level ban and observation state.
//
// It is a subsystem of its own because it has already been the answer once: a
// memory leak traced to ban maps that were only ever cleaned lazily. A line
// that would have shown it is cheaper than the investigation that found it.
func (s *Service) banUsage() domain.SubsystemUsage {
	s.ipStateMu.RLock()
	bans := len(s.bans)
	bannedIPs := len(s.bannedIPSet)
	remoteBans := len(s.remoteBannedIPs)
	// Peers with an address history, not addresses remembered. Each history is
	// capped at observedIPHistoryMaxSize; the number of peers holding one is
	// bounded only by who has connected, so this counts keys.
	observedPeers := len(s.observedIPHistoryByPeer)
	s.ipStateMu.RUnlock()

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemBans,
		domain.NewResourceGauge("peer_bans", bans, banEntryBytes),
		domain.NewResourceGauge("banned_ips", bannedIPs, bannedIPBytes),
		domain.NewResourceGauge("remote_banned_ips", remoteBans, remoteBanBytes),
		domain.NewResourceGauge("observed_ip_peers", observedPeers, observedIPPeerBytes),
	)
}
