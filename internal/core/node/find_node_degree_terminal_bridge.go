package node

import (
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Minted before Service or either original lifetime can escape. Never derive
// the expected owner, token or publisher from the mutable witness pointer.
type findNodeBridgeTerminal struct {
	publisher findnodebridge.Publisher
	owner     *degreeOwner
	token     *proofTransportServiceToken
}

// Requires common.mu. The expected owner mutex precedes clock then W. No
// witness mutation occurs before exact immutable lifetime/owner validation.
func (l *proofTransportLifetime) endBridgeLocked() {
	t := l.bridgeTerminal
	w := l.degreeWitness
	if w == nil {
		publication := t.publisher.BeginPublication()
		l.clearBridgeTerminalLocked()
		publication.Complete()
		return
	}
	o := t.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	valid := t.token != nil && t.token == l.token && o.token == t.token && o.bridgePublisher == t.publisher
	valid = valid && w.origin != nil && w.origin == l && w.owner == o && !w.closed && validDegreeStatisticKey(w.key)
	if valid {
		if w.untracked {
			valid = w.slot == -1 && w.generation == 0 && o.unknown > 0
		} else {
			valid = w.slot >= 0 && w.slot < len(o.active) && w.generation > 0
			if valid {
				row := &o.active[w.slot]
				valid = row.life == l && row.witness == w && row.generation == w.generation
			}
		}
	}
	publication := t.publisher.BeginClockedPublication()
	point, err := publication.Sample()
	clockValid := err == nil && point.Elapsed() >= o.last
	if clockValid {
		o.last = point.Elapsed()
	}
	if !clockValid || !valid {
		o.poisoned = true
	}
	if valid {
		w.closed = true
		if w.untracked {
			o.unknown--
		} else {
			row := &o.active[w.slot]
			row.life, row.witness = nil, nil
		}
		if clockValid && !o.poisoned {
			o.degreeHistoryLocked(w.key, point.Elapsed())
		}
	}
	l.degreeWitness = nil
	l.clearBridgeTerminalLocked()
	publication.Complete()
}

// The exact pre-existing common/F1/N-in cleanup, executed inside one W scope.
// The terminal LP is ended's store. No later mutex, clock, callback or IO.
func (l *proofTransportLifetime) clearBridgeTerminalLocked() {
	l.ended = true
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
