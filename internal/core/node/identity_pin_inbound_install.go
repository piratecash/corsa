package node

import (
	"context"
	"crypto/rand"
	"io"
	"math"
)

type ninInstallState uint8

const (
	ninInstallUnavailable ninInstallState = iota
	ninInstallAttachedUnconfirmed
	ninInstallReady
)

// Historical strict commit evidence and current journal readiness are separate.
// A later content change can leave a successful captured snapshot unconfirmed.
// This result never authorizes producer attachment without rechecking the owner.
type ninInstallResult struct {
	owner   *secureSessionStore
	state   ninInstallState
	created bool
	strict  identityPinMetadataCommit
}

// installUnknownInboundPinJournal is private constructor-time installation,
// never verified migration/KnownEmpty authority. The typed files port is owned
// by its caller; no port, DTO, event history or callback is retained by the store.
func (s *secureSessionStore) installUnknownInboundPinJournal(ctx context.Context, files identityPinDurableFiles) (ninInstallResult, error) {
	result := ninInstallResult{owner: s}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if s == nil || files == nil {
		return result, errNinJournal
	}
	if err := files.Preflight(ctx); err != nil {
		return result, err
	}
	if err := s.startupRefusal(); err != nil {
		return result, err
	}
	// Entropy is obtained outside all owner locks and only when possibly needed.
	// A concurrent first installer can win; its installed domain then survives.
	s.mu.Lock()
	needsDomain := s.ninJournal == nil
	s.mu.Unlock()
	var domainID [16]byte
	if needsDomain {
		if _, err := io.ReadFull(rand.Reader, domainID[:]); err != nil {
			return result, err
		}
		if domainID == ([16]byte{}) {
			return result, errNinJournal
		}
	}
	// This barrier drains all earlier full writers, including format2 snapshots.
	// Every later snapshot observes format3. The closure releases ALL three locks
	// before the common strict confirmer reacquires gate.R -> writeMu -> mu.
	err := func() error {
		s.pinMutationGate.Lock()
		defer s.pinMutationGate.Unlock()
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.pinMetadataMutationRefusalLocked(); err != nil {
			return err
		}
		if s.dir == "" || s.memoryOnlyLocked() || s.ninElapsed == nil || s.pinNetworkStarted || s.pinOfflineRecovery || s.pinRecovery != nil || (s.pinRemovalScope != nil && !s.pinSupervisedStartup) {
			return errNinJournal
		}
		if s.ninFormat3 != (s.ninJournal != nil) {
			return errNinJournal
		}
		if s.ninJournal == nil {
			if s.changes == math.MaxUint64 || domainID == ([16]byte{}) {
				return errNinJournal
			}
			// The final cancellation observation is under mu, after every wait and
			// immediately before the one atomic format/journal/dirty publication.
			if err := ctx.Err(); err != nil {
				return err
			}
			s.ninFormat3 = true
			s.ninJournal = &ninJournal{domain: domainID, generation: 1, phase: ninUnknown, rows: 1}
			s.sampleNinQuotaLocked() // first reading anchors; it earns no uptime
			s.changes++
			result.created = true
		}
		result.state = ninInstallAttachedUnconfirmed
		return nil
	}()
	if err != nil {
		return result, err
	}
	// A failed or uncertain write never detaches the conservative journal. The
	// existing writer supplies exact captured stamps, revisions and dirty cover.
	result.strict, err = s.confirmPinMetadata(ctx, files)
	s.mu.Lock()
	if err == nil && s.ninFormat3 && s.ninJournal != nil && s.ninJournal.strictConfirmed() && s.pinMetadataMutationRefusalLocked() == nil {
		result.state = ninInstallReady
	}
	s.mu.Unlock()
	if err == nil && result.state != ninInstallReady {
		err = errPinMetadataCommitUnconfirmed
	}
	return result, err
}
