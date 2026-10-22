package node

import (
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// Fix the SAME original F1/CM Session, common and concrete core before queueing.
// The writer keeps this core pointer; no subsequent ConnID lookup exists.
func (s *Service) queueOwnedFindNodeForward(work findnode.OwnedForwardWork) error {
	target := work.RouteTarget()
	if target == (findnode.Session{}) {
		_ = work.Cancel()
		return findnode.ErrUnauthenticated
	}
	s.peerMu.RLock()
	entry := s.connEntryByIDLocked(target.ConnID())
	var core *netcore.NetCore
	if entry != nil && entry.common != nil {
		common := entry.common
		common.mu.Lock()
		if !common.ended && common.token == s.proofToken && common.findNode != nil && common.findNode.published && common.findNode.session == target {
			core = entry.core
		}
		common.mu.Unlock()
	}
	s.peerMu.RUnlock()
	if core == nil {
		_ = work.Cancel()
		return findnode.ErrUnauthenticated
	}
	status := core.SendOwnedFindNode(work, target)
	if status != netcore.SendOK {
		return netcore.SendStatusToError(status)
	}
	return nil
}
