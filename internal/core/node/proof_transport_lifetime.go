package node

import (
	"github.com/piratecash/corsa/internal/core/findnode"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Nonzero-sized, constructor-only token: pointer identity identifies one Service,
// without retaining it or any backend. No runtime setter or public authority.
type proofTransportServiceToken struct{ _ byte }

// One memory-only gate for observed terminal, N-in stage and canonical FindNode
// publication. Never acquire peerMu/CM/store or perform diagnostics/I/O under mu.
type proofTransportLifetime struct {
	mu             sync.Mutex
	ended          bool
	token          *proofTransportServiceToken
	role           sessionv2.Role
	outbound       bool
	id             domain.ConnID
	entry          *connEntry
	peer           *peerSession
	evidence       *proofTransportEvidence
	nin            *matureInboundWitness
	findNode       *findNodeConnectionBinding
	degreeWitness  *degreeWitness
	bridgeTerminal *findNodeBridgeTerminal // immutable before literal publication
}
type proofTransportOwner struct {
	life  *proofTransportLifetime
	id    domain.ConnID
	token *proofTransportServiceToken
}

func (l *proofTransportLifetime) ownerLocked() *proofTransportOwner {
	if l == nil || l.ended || l.id == 0 {
		return nil
	}
	return &proofTransportOwner{life: l, id: l.id, token: l.token}
}
func (o *proofTransportOwner) matchesTransportLocked(s *Service, id domain.ConnID) bool {
	return o != nil && o.life != nil && !o.life.ended && o.id == id && o.life.id == id && o.token != nil && o.token == s.proofToken && o.life.token == o.token
}
func (o *proofTransportOwner) end() {
	if o != nil {
		o.life.end()
	}
}
func (l *proofTransportLifetime) end() {
	if l == nil {
		return
	}
	var retiredIntake findnode.DormantIntake
	var retiredLocal findnode.OwnedLocalTarget
	var retiredSession findnode.Session
	l.mu.Lock()
	defer func() {
		l.mu.Unlock()
		if retiredIntake != (findnode.DormantIntake{}) && retiredSession != (findnode.Session{}) {
			_ = retiredIntake.EndOriginalSession(retiredSession)
		}
		if retiredLocal != (findnode.OwnedLocalTarget{}) && retiredSession != (findnode.Session{}) {
			_ = retiredLocal.EndOriginalSession(retiredSession)
		}
	}()
	if !l.ended && l.findNode != nil {
		retiredIntake, retiredSession = l.findNode.retirement, l.findNode.session
		retiredLocal = l.findNode.localRetirement
	}
	if l.ended {
		return
	}
	if l.bridgeTerminal != nil {
		l.endBridgeLocked()
		return
	}
	l.ended = true
	l.degreeEndLocked()
	if l.findNode != nil {
		l.findNode.end()
	}
	if w := l.nin; w != nil {
		w.phase = inboundTerminal
		w.binding = identityPinEntryBinding{}
		w.session = sessionv2.ProvenSession{}
		w.producer = nil
		w.group = ""
		w.ready = 0
	}
	l.evidence = nil
	l.entry = nil
	l.peer = nil
}
func (ps *peerSession) endProofTransport() {
	if ps != nil {
		ps.common.end()
	}
}
