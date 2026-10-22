package findnode

import (
	"strconv"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

// Unused private fragment, not a Node cut or a domain/completeness receipt.
// The copied vector is immutable after preparation and never target-filtered.
type ownedOriginalSet struct{ members []Session }

// Preparation precedes all final locks. maxMembers is an explicit caller bound,
// not a deployment default or a proof of aggregate memory affordability.
func prepareOwnedOriginalSet(originals []Session, maxMembers int) (ownedOriginalSet, error) {
	if maxMembers <= 0 || maxMembers > int(^uint(0)>>1)/(strconv.IntSize/8) || len(originals) > maxMembers {
		return ownedOriginalSet{}, ErrInvalid
	}
	seen := make(map[domain.ConnID]struct{}, len(originals))
	for _, member := range originals {
		if !member.valid() || member.state.connID == 0 || member.state.identity.IsZero() || member.state.group == "" || member.state.opened < 0 {
			return ownedOriginalSet{}, ErrUnauthenticated
		}
		identity, proven := member.state.identityProof.Identity()
		if !proven || identity != member.state.identity {
			return ownedOriginalSet{}, ErrUnauthenticated
		}
		if _, duplicate := seen[member.state.connID]; duplicate {
			return ownedOriginalSet{}, ErrInvalid
		}
		seen[member.state.connID] = struct{}{}
	}
	members := make([]Session, len(originals))
	copy(members, originals)
	return ownedOriginalSet{members: members}, nil
}

// Caller owns one actual combined reservation throughout this call and the
// eventual commit. No point keeps that lease alive; caller must release once
// on success and failure. This helper neither releases nor grants admission.
func (set ownedOriginalSet) sampleAndCheckOriginals(lease *findnodebridge.CommitReservation) (findnodebridge.OwnedInstant, error) {
	if set.members == nil {
		return findnodebridge.OwnedInstant{}, ErrInvalid
	}
	at, err := lease.Sample()
	if err != nil {
		return findnodebridge.OwnedInstant{}, err
	}
	for _, member := range set.members {
		if !member.valid() {
			return findnodebridge.OwnedInstant{}, ErrUnauthenticated
		}
	}
	return at, nil
}
