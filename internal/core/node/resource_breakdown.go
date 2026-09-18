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
//     keys than frames;
//     sessions and connections — bounded by the connection count, read for
//     their queue len/cap;
//     relay forwarding states — at most maxRelayStates (10 000), under the
//     store's own leaf mutex, summed for their stashed frame bytes;
//     route health — the orphan gauge in routing.Table.Usage probes storage
//     once per health entry under the table's read lock. It is the one walk
//     proportional to a container that has no cap of its own, and it is
//     paid because its answer is the invariant "health keys ⊆ storage keys",
//     which a len cannot express; a node with 30 000 entries answers in
//     low milliseconds, and getResourceBreakdown is a command an operator
//     runs, not a sampler;
//     non-contact identity records — bounded by their own budget, priced
//     by the byte total the cache already maintains.
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
	relayStateBytes       = domain.SizeOfAll("", relayForwardState{})
	trustContactBytes     = domain.SizeOfAll("", trustedContact{})
	trustRecordBytes      = domain.SizeOfAll(trustRecordKey{}, trustedIdentityRecord{})
	identityCooldownBytes = domain.SizeOfAll(domain.PeerIdentity{}, time.Time{})
	sessionSendSlotBytes  = domain.SizeOfAll(peerSendItem{})
	sessionInboxSlotBytes = domain.SizeOfAll(protocol.Frame{})
	connWriterSlotBytes   = netcore.WriterQueueSlotBytes()
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

	// The shared connection ceiling is reported beside the subsystems rather
	// than as one of them: it is not memory this node is holding, it is the
	// admission decision that bounds how much of it there can be. Usage here
	// INCLUDES attempts in flight, which is the only figure that says whether
	// a node is actually against the ceiling — a count of established
	// sessions would leave out exactly what a ceiling is defeated by.
	budget := s.connBudget.Snapshot()
	answer := map[string]any{
		"sampled_at":  breakdown.SampledAt().Format(time.RFC3339Nano),
		"floor_bytes": breakdown.FloorBytes(),
		"floor_human": formatBytes(breakdown.FloorBytes()),
		"subsystems":  subsystems,
		"connection_budget": map[string]any{
			"enabled": budget.Enabled,
			// Total 0 with enabled=false is the default: the shared
			// ceiling is off while the per-direction limits below still
			// apply. The two facts are reported separately so "off" is
			// never read as "unlimited admission".
			"total":             budget.Total,
			"outbound_reserve":  budget.OutboundReserve,
			"non_slot_capacity": budget.NonSlotCapacity,
			"max_outbound":      budget.MaxOutbound,
			"max_inbound":       budget.MaxInbound,
			// Auxiliary dials are reported apart from peer slots because
			// they are bounded apart: they never compete for the node's
			// persistent neighbourhood, only for the shared ceiling.
			"max_auxiliary": budget.MaxAuxiliary,
			"used":          budget.Used,
			"outbound":      budget.Outbound,
			"inbound":       budget.Inbound,
			"auxiliary":     budget.Auxiliary,
			"refused": map[string]any{
				"total_exhausted":   budget.RefusedTotal,
				"outbound_reserved": budget.RefusedReserved,
				"direction_limit":   budget.RefusedDirection,
				"unknown_direction": budget.RefusedUnknown,
			},
		},
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
//
// The queues ARE counted, as slots: a buffered channel allocates every slot
// at construction, so an outbound session costs its send and inbox
// capacities and a connection its writer capacity whether or not anything
// is queued. The walks here are over the live sessions and connections —
// bounded by the connection count, which is the one number the operator
// already knows — and read only channel len/cap, which need no lock beyond
// the map's own. Occupancy is reported beside each as a saturation gauge:
// the slots are already priced, and what sits in them is a copy of frames
// counted elsewhere.
func (s *Service) sessionsUsage() domain.SubsystemUsage {
	s.peerMu.RLock()
	sessions := len(s.sessions)
	health := len(s.health)
	conns := len(s.conns)
	var sendSlots, sendQueued, inboxSlots, inboxQueued, writerSlots, writerQueued int
	for _, session := range s.sessions {
		sendSlots += cap(session.sendCh)
		sendQueued += len(session.sendCh)
		inboxSlots += cap(session.inboxCh)
		inboxQueued += len(session.inboxCh)
	}
	for _, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		queued, capacity := entry.core.WriterQueue()
		writerSlots += capacity
		writerQueued += queued
	}
	s.peerMu.RUnlock()

	relayStates, relayFrameBytes := 0, 0
	if s.relayStates != nil {
		relayStates = s.relayStates.count()
		relayFrameBytes = s.relayStates.frameLineBytes()
	}

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemSessions,
		domain.NewResourceGauge("peer_health", health, peerHealthBytes),
		domain.NewResourceGauge("sessions", sessions, sessionBytes),
		domain.NewResourceGauge("connections", conns, connEntryBytes),
		domain.NewResourceGauge("session_send_slots", sendSlots, sessionSendSlotBytes),
		domain.NewSaturationGauge("session_send_queued", sendQueued),
		domain.NewResourceGauge("session_inbox_slots", inboxSlots, sessionInboxSlotBytes),
		domain.NewSaturationGauge("session_inbox_queued", inboxQueued),
		domain.NewResourceGauge("conn_writer_slots", writerSlots, connWriterSlotBytes),
		domain.NewSaturationGauge("conn_writer_queued", writerQueued),
		// Transit forwarding state, one record per relayed message for up
		// to 180 s, capped at maxRelayStates. The frame bytes stashed on
		// those records for failover are priced separately and exactly:
		// they are the part of the record that is a payload rather than a
		// header, and the part that is released early — on the hop ack.
		domain.NewResourceGauge("relay_states", relayStates, relayStateBytes),
		domain.NewResourceGauge("relay_frame_bytes", relayFrameBytes, 1),
	)
}

// deliveryUsage reports the message-delivery domain and the transit backlog.
//
// Two sums iterate here and both are bounded: pending frames by
// maxPendingFramesTotal, topic backlogs by the transit byte budget and the
// per-recipient caps (the walk is the one admission runs per message). The receipt
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

	// One pass over the backlogs, the same pass admission already pays on
	// every stored message (scanTopicForAdmission). Transit is classified
	// by the same predicate the retention policy uses, so the gauge and
	// the byte budget it is read against agree on what a transit
	// envelope is; summing every backlog blindly, as this once did,
	// counted the node's own inbox as transit.
	s.gossipMu.RLock()
	transitEnvelopes, transitPayload, localEnvelopes := 0, 0, 0
	for _, backlog := range s.topics {
		for i := range backlog {
			if s.isTransitEnvelope(backlog[i]) {
				transitEnvelopes++
				transitPayload += len(backlog[i].Payload)
				continue
			}
			localEnvelopes++
		}
	}
	s.gossipMu.RUnlock()

	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemDelivery,
		// The transit backlog: other people's messages this node is carrying.
		// Bounded by maxTransitBacklogBytes of payload (reported exactly
		// beside it) rather than by a count, so a large number here is not
		// by itself a fault; a payload figure near the budget is the
		// number to read.
		domain.NewResourceGauge("transit_envelopes", transitEnvelopes, envelopeBytes),
		domain.NewResourceGauge("transit_payload_bytes", transitPayload, 1),
		// This node's own messages held in topic backlogs: its inbox and
		// the broadcast topics. Not transit, and not subject to the transit
		// budget.
		domain.NewResourceGauge("local_envelopes", localEnvelopes, envelopeBytes),
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
//
// The key maps are walked for their string bytes: an entry of each is priced
// as two string headers, and the base64 material they point at is where the
// memory actually is. The walk is bounded by maxKnownIdentities plus the
// pinned trust set, because every key map is a subset of the known set.
//
// The trust store and the identity resolver keep their own leaf mutexes and
// are read after the knowledge domain is released.
func (s *Service) knowledgeUsage() domain.SubsystemUsage {
	s.knowledgeMu.RLock()
	known := s.known.Len()
	pinned := s.known.PinnedLen()
	boxKeys := len(s.boxKeys)
	pubKeys := len(s.pubKeys)
	boxSigs := len(s.boxSigs)
	keyBytes := 0
	for address, key := range s.boxKeys {
		keyBytes += len(address) + len(key)
	}
	for address, key := range s.pubKeys {
		keyBytes += len(address) + len(key)
	}
	for address, sig := range s.boxSigs {
		keyBytes += len(address) + len(sig)
	}
	s.knowledgeMu.RUnlock()

	gauges := []domain.ResourceGauge{
		domain.NewResourceGauge("known_identities", known, knownIdentityBytes),
		// Members the bound does not apply to: the trust-store mirror. A
		// subset of known_identities.
		domain.NewSaturationGauge("pinned_identities", pinned),
		domain.NewResourceGauge("box_keys", boxKeys, keyMaterialBytes),
		domain.NewResourceGauge("public_keys", pubKeys, keyMaterialBytes),
		domain.NewResourceGauge("box_signatures", boxSigs, keyMaterialBytes),
		// The bytes the three maps' strings point at — the part the entry
		// price above deliberately excludes.
		domain.NewResourceGauge("key_material_bytes", keyBytes, 1),
	}
	if s.trust != nil {
		contacts, conflicts, records := s.trust.persistentUsage()
		cache := s.trust.sessionRecordUsage()
		gauges = append(gauges,
			domain.NewResourceGauge("trust_contacts", contacts, trustContactBytes),
			domain.NewResourceGauge("trust_conflicts", conflicts, keyMaterialBytes),
			// Persistent identity records: the node's own and its contacts'.
			// Bounded by the contact count, which the user controls.
			domain.NewResourceGauge("trust_records", records, trustRecordBytes),
			// Session-peer identity records: memory only, under a count,
			// byte and TTL budget (trust_session_records.go). The bytes
			// gauge is the cache's own exact account — structural cost
			// plus the signed bytes and key strings each entry holds — so
			// it is the ONLY memory figure here; the count is saturation
			// against maxSessionIdentityRecords, and pricing it again by
			// the struct size would charge every entry's header twice.
			domain.NewSaturationGauge("identity_record_cache", cache.count),
			domain.NewResourceGauge("identity_record_cache_bytes", cache.bytes, 1),
			// Entries the live set holds against the TTL and the budget:
			// identities with a session or an open lookup, whose seq floor
			// must not be evicted. A subset of the count above; when it
			// approaches the budget, the cache is carrying its whole
			// working set and can no longer shed anything.
			domain.NewSaturationGauge("identity_record_cache_protected", cache.protected),
			// Monotonic counters, not occupancies: how many records the
			// budget has pushed out and how many the TTL has retired since
			// start. Reported as saturation so they add no bytes.
			domain.NewSaturationGauge("identity_record_cache_evicted", int(cache.evicted)),
			domain.NewSaturationGauge("identity_record_cache_expired", int(cache.expired)),
		)
	}
	if s.identityResolver != nil {
		gauges = append(gauges,
			// Lookup cooldowns: one per target that reached a terminal within
			// the last cooldown window. Swept on the resolver's tick.
			domain.NewResourceGauge("identity_lookup_cooldowns", s.identityResolver.cooldownCount(), identityCooldownBytes),
		)
	}
	return domain.NewSubsystemUsage(domain.ResourceSubsystemKnowledge, gauges...)
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
