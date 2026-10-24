package node

import (
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"sync/atomic"
)

// Test-only single-attempt retention. Each successful CAS writer stores one
// genuine original opaque handle before publishing ready. No callback, clock,
// allocation, relookup, reset or operational method is added to the live path.
// The Service field is nil in every normal construction; only the closed test
// factory allocates these two cells before the original Run starts.
type ownedFindNodeEndpointTestSlot struct {
	relayTraceClaim  atomic.Bool
	relayTraceReady  atomic.Bool
	relayTraceWork   findnode.OwnedAnswerWork
	targetTraceClaim atomic.Bool
	targetTraceReady atomic.Bool
	targetTraceWork  findnode.OwnedLocalSignWork
}

// This takes the same raw-only receive cell AFTER the original raw/command
// admission gates, keeping the successful-read point historical and unrefreshed.
func (s *Service) dispatchOwnedFindNodeAnswer(receipt ownedFindNodeReceive) error {
	cell := receipt.cell
	if s == nil || s.bridgeCapture == nil || cell == nil || cell.capture != s.bridgeCapture || cell.transport == nil || cell.transport.life == nil || cell.transport.token != s.proofToken || !cell.capture.consumer.Matches(cell.stamp, cell.received) {
		return findnode.ErrUnauthenticated
	}
	if !cell.consumed.CompareAndSwap(false, true) {
		return findnode.ErrStaleWork
	}
	if err := s.runCtx.Err(); err != nil {
		return err
	}
	// Select the exact original common, not a new session at this numeric ID.
	s.peerMu.RLock()
	entry := s.connEntryByIDLocked(cell.transport.id)
	if entry == nil || entry.common != cell.transport.life {
		s.peerMu.RUnlock()
		return findnode.ErrUnauthenticated
	}
	target, found, err := s.collectOwnedFindNodeRawMemberLocked(s.runCtx, cell.transport.id)
	s.peerMu.RUnlock()
	if err != nil {
		return err
	}
	if !found || target.ConnID() != cell.transport.id {
		return findnode.ErrUnauthenticated
	}
	input, err := cell.capture.sink.SealAnswer(cell.stamp, cell.received, target, []byte(cell.wire))
	if err != nil {
		return err
	}
	work, err := cell.capture.intake.MatchOwnedAnswer(input)
	if err != nil {
		return err
	}
	// Match has released its original R lease before this fixed-value handoff.
	if slot := s.ownedFindNodeEndpointTest; slot != nil && slot.relayTraceClaim.CompareAndSwap(false, true) {
		slot.relayTraceWork = work
		slot.relayTraceReady.Store(true)
	}
	return s.queueOwnedFindNodeAnswer(work)
}

// Shared local/relayed answer endpoint. All Node/registry/common waits precede
// queue publication; the actual single writer retains this exact core pointer.
func (s *Service) queueOwnedFindNodeAnswer(work findnode.OwnedAnswerWork) error {
	source := work.SourceSession()
	if source == (findnode.Session{}) {
		_ = work.CancelAnswer()
		return findnode.ErrUnauthenticated
	}
	s.peerMu.RLock()
	entry := s.connEntryByIDLocked(source.ConnID())
	var core *netcore.NetCore
	if entry != nil && entry.common != nil {
		common := entry.common
		common.mu.Lock()
		if !common.ended && common.token == s.proofToken && common.findNode != nil && common.findNode.published && common.findNode.session == source {
			core = entry.core
		}
		common.mu.Unlock()
	}
	s.peerMu.RUnlock()
	if core == nil {
		_ = work.CancelAnswer()
		return findnode.ErrUnauthenticated
	}
	status := core.SendOwnedFindNodeAnswer(work, source)
	if status != netcore.SendOK {
		return netcore.SendStatusToError(status)
	}
	return nil
}

// This is called only from the actual original outbound read loop AFTER raw
// admission and BEFORE any strict owned parser. It is shared by request/answer.
func (s *Service) dispatchOutboundOwnedFindNode(session *peerSession, receipt ownedFindNodeReceive) error {
	cell := receipt.cell
	if session == nil || cell == nil || cell.transport == nil || session.common != cell.transport.life || session.connID != cell.transport.id {
		return findnode.ErrUnauthenticated
	}
	if !s.outboundControlFrameAllowed(session) {
		return findnode.ErrBudget
	}
	return s.dispatchOwnedFindNodeReceive(receipt)
}
