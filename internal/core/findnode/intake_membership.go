package findnode

import "github.com/piratecash/corsa/internal/core/domain"

type candidateMembership struct {
	owner   *intakeCoordinator
	at      Instant
	maximum domain.ConnID
	classes map[Session]SessionClass
	aged    map[domain.PeerIdentity][4]Session
}

// captureCandidateMembership executes trusted classification outside owner
// locks. Complete live membership is caller-owned, not proven by a wire field.
// Only offered facts and four witnesses per target survive this capture.
func captureCandidateMembership(owner *intakeCoordinator, targets []domain.PeerIdentity, offered, live []Session, at Instant, classify PrioritySource) (candidateMembership, error) {
	if owner == nil || at < 0 || classify == nil || len(targets) > MaxPendingTotal || len(offered) > MaxPendingTotal {
		return candidateMembership{}, ErrInvalid
	}
	out := candidateMembership{owner: owner, at: at, classes: make(map[Session]SessionClass), aged: make(map[domain.PeerIdentity][4]Session)}
	wanted := make(map[Session]struct{}, len(offered))
	for _, source := range offered {
		wanted[source] = struct{}{}
	}
	for _, target := range targets {
		if target.IsZero() {
			return candidateMembership{}, ErrInvalid
		}
		out.aged[target] = [4]Session{}
	}
	seen := make(map[domain.ConnID]Session, len(live))
	for _, source := range live {
		if !source.valid() || source.state.opened > at {
			return candidateMembership{}, ErrUnauthenticated
		}
		if prior, ok := seen[source.ConnID()]; ok {
			if prior != source {
				return candidateMembership{}, ErrInvalid
			}
			continue
		}
		seen[source.ConnID()] = source
		out.maximum = max(out.maximum, source.ConnID())
		class, err := classify(SessionInfo{ConnID: source.ConnID(), Proof: source.state.identityProof, Group: source.state.group, Opened: source.state.opened})
		if err != nil {
			return candidateMembership{}, err
		}
		switch class {
		case ClassOrdinary, ClassContact, ClassSelected, ClassIncomingPinned:
		default:
			return candidateMembership{}, ErrInvalid
		}
		if _, ok := wanted[source]; ok {
			out.classes[source] = class
		}
		if class == ClassContact || class == ClassSelected || (class != ClassIncomingPinned && at-source.state.opened < candidateGroupWindow) {
			continue
		}
		for target, oldest := range out.aged {
			if source.Identity() == target {
				continue
			}
			for index, prior := range oldest {
				if prior.state != nil && !candidateOlder(source, prior) {
					continue
				}
				copy(oldest[index+1:], oldest[index:len(oldest)-1])
				oldest[index] = source
				break
			}
			out.aged[target] = oldest
		}
	}
	if len(out.classes) != len(wanted) {
		return candidateMembership{}, ErrPrioritySnapshot
	}
	return out, nil
}
func candidateOlder(a, b Session) bool {
	if a.state.opened != b.state.opened {
		return a.state.opened < b.state.opened
	}
	return a.ConnID() < b.ConnID()
}
func (m candidateMembership) priority(owner *intakeCoordinator, source Session, target domain.PeerIdentity, at Instant) (Priority, error) {
	if m.owner != owner || m.at != at || !source.valid() || source.Identity() == target {
		return 0, ErrPrioritySnapshot
	}
	class, ok := m.classes[source]
	if !ok {
		return 0, ErrPrioritySnapshot
	}
	oldest, ok := m.aged[target]
	if !ok {
		return 0, ErrPrioritySnapshot
	}
	if class == ClassContact || class == ClassSelected {
		return PriorityProtected, nil
	}
	for _, s := range oldest {
		if s == source {
			return PriorityAged, nil
		}
	}
	return PriorityNew, nil
}
