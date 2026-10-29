//go:build d1_resource_probe

package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// D1FindNodeMemberRow names one original registered proven listener lifetime.
// PublishedF1 denotes published membership, not a negotiation/retirement claim.
type D1FindNodeMemberRow struct {
	ConnID      domain.ConnID
	Identity    domain.PeerIdentity
	PublishedF1 bool
}

// D1FindNodeCheckpoint is membership-only, not an admission or drain barrier.
// Counts/rows describe one stable shared-W generation, not future membership.
type D1FindNodeCheckpoint struct {
	MembershipComplete                   bool
	UnknownReason                        string
	ServiceIdentity                      string
	Scope                                string
	RegistryVisits, ActiveCount, F1Count int
	Rows                                 [2]D1FindNodeMemberRow
	RetirementStatus, DrainStatus        string
}

type d1RegistryProof struct {
	life    *proofTransportLifetime
	witness *degreeWitness
	id      domain.ConnID
	member  findnode.Session
	f1      bool
}

func unknownD1Membership(reason string) D1FindNodeCheckpoint {
	return D1FindNodeCheckpoint{UnknownReason: reason, Scope: "registered-original-listener-small2",
		RetirementStatus: "UNKNOWN:no original completion barrier", DrainStatus: "UNKNOWN:not a drain barrier"}
}

// CheckpointForService reads original state without a clock, Seal, admission,
// mutation, polling or worker. Existing peer/common/degree mutex waits can block;
// cardinality bounds are not a no-wait/physical-deadline guarantee.
func (r *D1FindNodeResourceReadout) CheckpointForService(expected *Service) D1FindNodeCheckpoint {
	unknown := unknownD1Membership("UNKNOWN: original membership unavailable")
	if !r.boundTo(expected) {
		return unknown
	}
	s, ctx := expected, expected.runCtx
	before, err := r.capture.observer.BeginCapture()
	if err != nil {
		return unknown
	}
	var originals [2]d1RegistryProof
	count, visits, f1Count := 0, 0, 0
	var walkErr error
	s.peerMu.RLock()
	s.forEachConnIDLocked(func(id domain.ConnID) bool {
		if walkErr = ctx.Err(); walkErr != nil {
			return false
		}
		if visits == 4 {
			walkErr = findnode.ErrCapacity
			return false
		}
		visits++
		member, found, err := s.collectOwnedFindNodeRawMemberLocked(ctx, id)
		if err != nil {
			walkErr = err
			return false
		}
		entry := s.connEntryByIDLocked(id)
		if entry == nil || entry.common == nil {
			if found {
				walkErr = findnode.ErrUnauthenticated
				return false
			}
			return true
		}
		l := entry.common
		l.mu.Lock()
		defer l.mu.Unlock()
		if walkErr = ctx.Err(); walkErr != nil {
			return false
		}
		if l.ended {
			if found {
				walkErr = findnode.ErrStaleWork
				return false
			}
			return true
		}
		w := l.degreeWitness
		if w == nil {
			if found {
				walkErr = findnode.ErrUnauthenticated
				return false
			}
			return true
		}
		if l.outbound || l.role != sessionv2.RoleListener || l.token != r.token || l.bridgeTerminal != r.terminal || l.id != id || id == 0 || l.entry != entry || entry.common != l || entry.core == nil || entry.core.ConnID() != id {
			walkErr = findnode.ErrUnauthenticated
			return false
		}
		if found {
			b := l.findNode
			if b == nil || !b.published || b.session != member || b.retirement != r.capture.intake || b.localRetirement != r.capture.localTarget {
				walkErr = findnode.ErrUnauthenticated
				return false
			}
		} else if l.findNode != nil && l.findNode.published {
			walkErr = findnode.ErrStaleWork
			return false
		}
		if count == len(originals) {
			walkErr = findnode.ErrCapacity
			return false
		}
		originals[count] = d1RegistryProof{life: l, witness: w, id: id, member: member, f1: found}
		count++
		if found {
			f1Count++
		}
		return true
	})
	s.peerMu.RUnlock()
	if walkErr != nil || ctx.Err() != nil {
		return unknown
	}

	// Never acquire common/peer locks while degree.mu is held. Its original
	// witness key proves identity even after accepted publication drops evidence.
	o := r.owner
	var rows [2]D1FindNodeMemberRow
	active := 0
	valid := true
	o.mu.Lock()
	if o.token != r.token || o.bridgePublisher != r.publisher || o.profile.MaxActive != 2 || len(o.active) != 2 || o.unknown != 0 || o.poisoned {
		valid = false
	} else {
		for index := range o.active {
			row := &o.active[index]
			if row.life == nil {
				if row.witness != nil {
					valid = false
				}
				continue
			}
			w := row.witness
			if w == nil || w.origin != row.life || w.owner != o || w.closed || w.untracked || w.slot != index || w.generation == 0 || w.generation != row.generation || row.life.token != r.token || !validDegreeStatisticKey(w.key) || w.key.outgoing || w.key.identity == domain.PeerIdentityFromWire(r.address) {
				valid = false
				break
			}
			match := -1
			for i := 0; i < count; i++ {
				if originals[i].life == row.life && originals[i].witness == w {
					if match != -1 {
						valid = false
						break
					}
					match = i
				}
			}
			if !valid || match < 0 {
				valid = false
				break
			}
			original := originals[match]
			if original.f1 && (original.member.ConnID() != original.id || original.member.Identity() != w.key.identity) {
				valid = false
				break
			}
			rows[match] = D1FindNodeMemberRow{ConnID: original.id, Identity: w.key.identity, PublishedF1: original.f1}
			active++
		}
	}
	o.mu.Unlock()
	if !valid || active != count || ctx.Err() != nil {
		return unknown
	}
	for i := 0; i < count; i++ {
		if rows[i].ConnID == 0 || rows[i].Identity.IsZero() {
			return unknown
		}
		for j := 0; j < i; j++ {
			if rows[i].ConnID == rows[j].ConnID {
				return unknown
			}
		}
	}
	after, err := r.capture.observer.BeginCapture()
	if err != nil || after != before || !r.boundTo(expected) {
		return unknown
	}
	if count == 2 && rows[0].ConnID > rows[1].ConnID {
		rows[0], rows[1] = rows[1], rows[0]
	}
	result := unknownD1Membership("")
	result.MembershipComplete = true
	result.ServiceIdentity = r.address
	result.RegistryVisits, result.ActiveCount, result.F1Count = visits, active, f1Count
	result.Rows = rows
	return result
}
