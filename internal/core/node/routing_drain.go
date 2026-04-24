// routing_drain.go hosts event-driven pending queue recovery: when a new
// or reconfirmed route appears (route learned, transit restored, backup
// exposed, TTL expiry with surviving backup), drainPendingForIdentities
// extracts matching send_message / push_message frames from the pending
// queue, attempts delivery via the routing table, and merges failed frames
// back into their original positions under the canonical
// peerMu → deliveryMu → statusMu order. The extract-attempt-return pattern
// and its retry/crash-loss contract are specified in the
// "Event-driven pending queue drain" section of docs/routing.md.
//
// Companion files: announce-plane wire path in routing_announce.go,
// relay-plane forwarding in routing_relay.go, address↔identity resolution
// in routing_resolver.go, session-lifecycle routing hooks in
// routing_session.go, hop_ack route confirmation in routing_hop_ack.go.
// The per-file scope table is the durable record — see the
// "internal/core/node/" section of the file map in docs/routing.md.
package node

import (
	"context"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// drainPendingForIdentities scans the pending queue for own outbound
// send_message frames whose Recipient matches one of the given identities
// and attempts to deliver them via the routing table.
//
// Only send_message is drained — other frame types are not route-recoverable:
//   - relay_message, announce_peer: other peers' traffic, handled by relay
//     retry loop and peer session flush respectively.
//   - relay_delivery_receipt: delivered via relayStates hop chain
//     (lookupReceiptForwardTo), not via the routing table. A new route to
//     the receipt's Recipient does not create a delivery path for the receipt.
//     Receipts are retried through flushPendingPeerFrames (peer reconnect)
//     and the relay retry loop, both of which already handle them correctly.
//
// This is an event-driven optimization: instead of waiting for the original
// target peer to reconnect (flushPendingPeerFrames) or for the 2-second
// relay retry loop to fire, we react immediately when a new route appears.
//
// Concurrency safety: matched frames are atomically removed from pending
// under the lock before delivery is attempted. This prevents concurrent
// drain goroutines (servePeerSession + handleAnnounceRoutes) from
// sending the same frame twice. Frames that fail delivery are returned to
// pending so the normal retry mechanisms can pick them up.
//
// Crash safety: all outbound state changes (markOutboundTerminalLocked,
// markOutboundRetryingLocked, clearOutboundQueuedLocked) are collected as
// deferred actions during the delivery loop and applied under the lock
// together with the pending queue return in a single persist. No
// intermediate writes to queue-*.json occur while extracted frames are
// absent from s.pending.
//
// recipientFromPendingFrame extracts the recipient from a queued frame.
// send_message uses flat Frame.Recipient; push_message keeps it in Item.
// Returns "" for frame types that are not identity-addressed.
func recipientFromPendingFrame(frame protocol.Frame) string {
	switch frame.Type {
	case "send_message":
		return frame.Recipient
	case "push_message":
		if frame.Item != nil {
			return frame.Item.Recipient
		}
		return ""
	default:
		return ""
	}
}

// Called from servePeerSession (direct peer connect, inboxCh is actively
// read so Lock contention cannot cause overflow) and handleAnnounceRoutes
// (transit route learned) to minimize delivery latency.
func (s *Service) drainPendingForIdentities(identities map[domain.PeerIdentity]struct{}) {
	defer crashlog.DeferRecover()

	if len(identities) == 0 {
		return
	}

	// Bail out if the service is shutting down. Drain goroutines are
	// fire-and-forget; without this check they could run against a
	// half-torn-down Service (closed connections, cancelled contexts),
	// causing write-deadline timeouts that serialize on s.peerMu and stall
	// other goroutines sharing the same lock.
	select {
	case <-s.done:
		return
	default:
	}

	type pendingMatch struct {
		peerAddr domain.PeerAddress
		item     pendingFrame
		origIdx  int // position in original s.pending[peerAddr] before extraction
	}

	// Per-address extraction metadata. Stored during the extract phase so
	// the return phase can merge failed frames back into their original
	// positions — preserving queue order across recipients sharing the
	// same peer address.
	type addrExtractMeta struct {
		origLen      int   // length of s.pending[addr] before extraction
		extractedPos []int // original indices of extracted frames (ascending)
	}

	// Atomically extract matching frames from the pending queue.
	// Removing them under the lock prevents concurrent drain goroutines
	// from processing the same frame (double-send prevention).
	//
	// No persist happens here — the on-disk state still contains the
	// extracted frames. If the process crashes before we persist below,
	// frames survive in queue-*.json and will be retried on restart.
	log.Trace().Str("site", "drainPendingForIdentities_extract").Str("phase", "lock_wait").Int("identities", len(identities)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "drainPendingForIdentities_extract").Str("phase", "lock_held").Int("identities", len(identities)).Msg("delivery_mu_writer")
	var extracted []pendingMatch
	extractMeta := make(map[domain.PeerAddress]*addrExtractMeta)
	for addr, frames := range s.pending {
		var kept []pendingFrame
		var meta *addrExtractMeta
		for i, item := range frames {
			recipient := recipientFromPendingFrame(item.Frame)
			if recipient == "" {
				kept = append(kept, item)
				continue
			}
			if _, ok := identities[domain.PeerIdentity(recipient)]; !ok {
				kept = append(kept, item)
				continue
			}
			if meta == nil {
				meta = &addrExtractMeta{origLen: len(frames)}
			}
			meta.extractedPos = append(meta.extractedPos, i)
			extracted = append(extracted, pendingMatch{peerAddr: addr, item: item, origIdx: i})
			delete(s.pendingKeys, pendingFrameKey(addr, item.Frame))
		}
		if meta != nil {
			extractMeta[addr] = meta
		}
		if len(kept) == 0 {
			delete(s.pending, addr)
		} else {
			s.pending[addr] = kept
		}
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "drainPendingForIdentities_extract").Str("phase", "lock_released").Int("extracted", len(extracted)).Msg("delivery_mu_writer")

	if len(extracted) == 0 {
		return
	}

	// Attempt delivery for each extracted frame. Track failures
	// so they can be returned to the pending queue.
	//
	// Outbound state changes (terminal, retrying, cleared) are collected
	// as deferred actions and applied under the lock in a single batch
	// at the end. This eliminates intermediate persists that would
	// snapshot s.pending without the extracted frames, creating a
	// crash-loss window.
	type deferredOutbound struct {
		action string // "terminal", "retrying", "clear"
		frame  protocol.Frame
		// Fields for "retrying":
		queuedAt time.Time
		retries  int
		errText  string
		// Fields for "terminal":
		status string
	}

	now := time.Now().UTC()
	var failed []pendingMatch
	var deferred []deferredOutbound

	for i, m := range extracted {
		// Re-check shutdown before each delivery attempt. If the service
		// is shutting down, return all remaining extracted frames to
		// pending without attempting delivery — the next startup will
		// retry them from the persisted queue state.
		select {
		case <-s.done:
			failed = append(failed, extracted[i:]...)
			goto returnFrames
		default:
		}

		if s.pendingFrameExpired(m.item.Frame, m.item.QueuedAt, now) {
			deferred = append(deferred, deferredOutbound{
				action:  "terminal",
				frame:   m.item.Frame,
				status:  "expired",
				errText: "message delivery expired",
			})
			continue
		}

		// push_message (gossip) drain: try enqueuePeerFrame directly.
		// Gossip doesn't need table-directed relay — just push to the
		// original peer address.
		var delivered, attempted bool
		if m.item.Frame.Type == "push_message" {
			delivered = s.enqueuePeerFrame(m.peerAddr, m.item.Frame)
			attempted = delivered
		} else {
			// drainPendingForIdentities is a fire-and-forget goroutine
			// (s.goBackground); its natural lifetime is the service itself,
			// so s.runCtx is the right context to bound the inbound
			// sync-flush wait inside sendRelayToAddress.
			delivered, attempted = s.drainSendMessage(s.runCtx, m.item.Frame, m.peerAddr)
		}
		if !delivered {
			// Only increment retry counter when a real delivery attempt was
			// made (route existed, address resolved, send was tried). When
			// no usable route exists the frame goes back to pending without
			// burning retry budget — a future routing event will trigger
			// another drain cycle.
			if attempted {
				m.item.Retries++
			}
			if m.item.Retries >= maxPendingFrameRetries {
				deferred = append(deferred, deferredOutbound{
					action:  "terminal",
					frame:   m.item.Frame,
					status:  "failed",
					errText: "max retries exceeded",
				})
				continue
			}
			// Only emit a "retrying" outbound state change when a real
			// delivery attempt was made. When no usable route exists, the
			// frame goes back to pending silently — no Status flip to
			// "retrying", no LastAttemptAt update. This preserves the
			// semantic that "retrying" in outbound state means a real
			// network handoff was tried and failed, matching the behavior
			// of flushPendingPeerFrames where retrying only fires on
			// sendCh backpressure.
			if attempted {
				deferred = append(deferred, deferredOutbound{
					action:   "retrying",
					frame:    m.item.Frame,
					queuedAt: m.item.QueuedAt,
					retries:  m.item.Retries,
					errText:  "drain delivery failed",
				})
			}
			failed = append(failed, m)
		} else {
			deferred = append(deferred, deferredOutbound{
				action: "clear",
				frame:  m.item.Frame,
			})
		}
	}

returnFrames:
	// Apply all state changes in a single lock+persist. This is the
	// only persist in the entire drain cycle — no intermediate writes
	// that could snapshot s.pending without extracted frames.
	//
	// Cross-domain: deliveryMu covers pending / pendingKeys / outbound
	// mutations; statusMu covers aggregate status recomputation via
	// refreshAggregatePendingLocked.  Canonical order
	// peerMu → deliveryMu → statusMu with statusMu INNERMOST.
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_wait").Int("failed", len(failed)).Int("deferred", len(deferred)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_held").Int("failed", len(failed)).Int("deferred", len(deferred)).Msg("peer_mu_writer")
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_wait").Int("failed", len(failed)).Int("deferred", len(deferred)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_held").Int("failed", len(failed)).Int("deferred", len(deferred)).Msg("delivery_mu_writer")

	// Merge failed frames back into their original positions in the
	// per-address pending queue. The extraction phase recorded each
	// frame's origIdx and per-addr metadata (origLen, extractedPos).
	// We walk through the original position range 0..origLen-1: at
	// extracted positions, emit the returning frame (if any); at kept
	// positions, consume the next frame from the current queue. Frames
	// added by other goroutines during the unlocked delivery window
	// (beyond the kept portion) are appended at the end. This preserves
	// exact original ordering across recipients sharing the same peer
	// address — flushPendingPeerFrames delivers in slice order, so any
	// reordering would change DM delivery sequence after route churn.
	type returningEntry struct {
		origIdx int
		item    pendingFrame
	}
	returningByAddr := make(map[domain.PeerAddress][]returningEntry, len(failed))
	for _, m := range failed {
		key := pendingFrameKey(m.peerAddr, m.item.Frame)
		if _, dup := s.pendingKeys[key]; dup {
			continue
		}
		returningByAddr[m.peerAddr] = append(returningByAddr[m.peerAddr], returningEntry{
			origIdx: m.origIdx,
			item:    m.item,
		})
		s.pendingKeys[key] = struct{}{}
	}
	for addr, rets := range returningByAddr {
		meta := extractMeta[addr]
		if meta == nil {
			// No metadata (defensive) — prepend as fallback.
			fallback := make([]pendingFrame, 0, len(rets)+len(s.pending[addr]))
			for _, r := range rets {
				fallback = append(fallback, r.item)
			}
			s.pending[addr] = append(fallback, s.pending[addr]...)
			continue
		}

		current := s.pending[addr]

		// current = [kept frames in order] + [new frames from other goroutines].
		// keptCount is the number of original non-extracted frames.
		keptCount := meta.origLen - len(meta.extractedPos)
		var keptPortion, newFrames []pendingFrame
		if keptCount <= len(current) {
			keptPortion = current[:keptCount]
			newFrames = current[keptCount:]
		} else {
			// Some kept frames were removed by a concurrent flush — use
			// whatever is left; merge degrades gracefully.
			keptPortion = current
		}

		merged := make([]pendingFrame, 0, len(rets)+len(current))
		ki := 0 // index into keptPortion
		ri := 0 // index into rets (sorted ascending by origIdx via extraction order)
		ei := 0 // index into meta.extractedPos

		for pos := 0; pos < meta.origLen; pos++ {
			if ei < len(meta.extractedPos) && meta.extractedPos[ei] == pos {
				// This position was extracted.
				if ri < len(rets) && rets[ri].origIdx == pos {
					merged = append(merged, rets[ri].item)
					ri++
				}
				// else: delivered, expired, or terminal — gap is correct.
				ei++
			} else {
				// This position was kept.
				if ki < len(keptPortion) {
					merged = append(merged, keptPortion[ki])
					ki++
				}
			}
		}
		// Defensive: drain any remaining kept or returning frames.
		for ; ki < len(keptPortion); ki++ {
			merged = append(merged, keptPortion[ki])
		}
		for ; ri < len(rets); ri++ {
			merged = append(merged, rets[ri].item)
		}
		// Append frames queued by other goroutines during the unlock window.
		merged = append(merged, newFrames...)

		if len(merged) == 0 {
			delete(s.pending, addr)
		} else {
			s.pending[addr] = merged
		}
	}
	for _, d := range deferred {
		switch d.action {
		case "terminal":
			s.markOutboundTerminalLocked(d.frame, d.status, d.errText)
		case "retrying":
			s.markOutboundRetryingLocked(d.frame, d.queuedAt, d.retries, d.errText)
		case "clear":
			s.clearOutboundQueuedLocked(d.frame.ID)
		}
	}
	// Collect final pending counts for all addresses that were touched during
	// the extract/re-insert cycle so the UI pending badge updates immediately.
	var pendingDeltas []ebus.PeerPendingDelta
	for addr := range extractMeta {
		pendingDeltas = append(pendingDeltas, ebus.PeerPendingDelta{
			Address: addr,
			Count:   len(s.pending[addr]),
		})
	}
	// statusMu is INNERMOST per canonical peerMu → deliveryMu → statusMu
	// order — refreshAggregatePendingLocked writes s.aggregateStatus and
	// the snapshot read must observe that write under the same lock.
	s.statusMu.Lock()
	if len(pendingDeltas) > 0 {
		s.refreshAggregatePendingLocked()
	}
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()

	drainDone := s.drainDone
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_released").Int("pending_deltas", len(pendingDeltas)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "drainPendingForIdentities_return").Str("phase", "lock_released").Int("pending_deltas", len(pendingDeltas)).Msg("peer_mu_writer")
	s.queuePersist.MarkDirty()
	for _, d := range pendingDeltas {
		s.emitPeerPendingChanged(d.Address, d.Count)
	}
	if len(pendingDeltas) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	}
	if drainDone != nil {
		drainDone()
	}
}

// drainSendMessage attempts to deliver a single send_message frame via the
// routing table. Uses sendRelayToAddress directly (not sendTableDirectedRelay)
// to get a definitive success/failure signal.
//
// Returns two booleans:
//   - delivered: true when the frame was durably handed off to a relay session.
//   - attempted: true when a real send was attempted (route existed, address
//     resolved). When attempted is false the caller should NOT increment the
//     retry counter — no network resources were consumed, the route simply
//     does not exist yet and a future routing event may provide one.
//
// On success, the caller is responsible for clearing outbound state —
// this function does not call clearOutboundQueued to avoid intermediate
// persists during the drain cycle.
//
// ctx is the drain cycle's context (caller passes s.runCtx or a scoped
// drain timeout). Propagated into sendRelayToAddress so that an inbound
// next-hop with a stuck sync-flush wait does not pin the whole drain.
func (s *Service) drainSendMessage(ctx context.Context, frame protocol.Frame, originalPeer domain.PeerAddress) (delivered, attempted bool) {
	createdAt, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		// Parse error is a data problem, not a routing problem.
		// Treat as attempted — the frame is malformed and retries won't help.
		return false, true
	}
	envelope := protocol.Envelope{
		ID:         protocol.MessageID(frame.ID),
		Topic:      frame.Topic,
		Sender:     frame.Address,
		Recipient:  frame.Recipient,
		Flag:       protocol.MessageFlag(frame.Flag),
		TTLSeconds: frame.TTLSeconds,
		Payload:    []byte(frame.Body),
		CreatedAt:  createdAt,
	}

	decision := s.router.Route(envelope)
	if decision.RelayNextHop == nil {
		// No route — not a real delivery attempt, don't burn retry budget.
		return false, false
	}

	// Resolve the next-hop address. Use the validated address from the
	// routing decision when available, otherwise re-resolve.
	address := decision.RelayNextHopAddress
	if address == "" {
		address = s.resolveRouteNextHopAddress(*decision.RelayNextHop, decision.RelayNextHopHops)
	}
	if address == "" {
		// Route exists but address unresolvable — transient, don't burn retry budget.
		return false, false
	}

	// sendRelayToAddress returns true only when the frame is durably
	// enqueued in a session or persistent peer queue, with relayForwardState
	// stored. No silent gossip fallback — if this fails, the frame goes
	// back to pending for the normal retry loop.
	if !s.sendRelayToAddress(ctx, address, envelope, decision.RelayRouteOrigin) {
		// Real send attempt failed — this is a genuine delivery failure.
		return false, true
	}

	log.Info().
		Str("id", frame.ID).
		Str("recipient", frame.Recipient).
		Str("next_hop", string(*decision.RelayNextHop)).
		Str("original_peer", string(originalPeer)).
		Msg("pending_drained_via_new_route")
	return true, true
}
