package node

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	addressNeverWorkedAge  = 6 * time.Hour
	addressWorkedAge       = 7 * 24 * time.Hour
	addressRecheckInterval = 6 * time.Hour
)

type addressStanding string

const (
	addressUntried       addressStanding = "untried"
	addressWorking       addressStanding = "working"
	addressFailing       addressStanding = "failing"
	addressStaleStanding addressStanding = "stale"
)

func addressStale(entry peerEntry, now time.Time) bool {
	if entry.LastSessionAt != nil {
		return entry.DialAttempts >= 10 && !now.Before(entry.LastSessionAt.Add(addressWorkedAge))
	}
	return entry.DialAttempts >= 3 && entry.AddedAt != nil && !now.Before(entry.AddedAt.Add(addressNeverWorkedAge))
}

func addressStandingOf(entry peerEntry, now time.Time) addressStanding {
	switch {
	case addressStale(entry, now):
		return addressStaleStanding
	case entry.DialAttempts > 0:
		return addressFailing
	case entry.LastSessionAt != nil:
		return addressWorking
	default:
		return addressUntried
	}
}

func addressRecheckAt(entry peerEntry) time.Time {
	if entry.LastDialAttemptAt == nil {
		return time.Time{}
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(entry.Address))
	_, _ = hash.Write([]byte(entry.LastDialAttemptAt.UTC().Format(time.RFC3339Nano)))
	jitter := time.Duration(hash.Sum64() % uint64(36*time.Minute))
	return entry.LastDialAttemptAt.Add(addressRecheckInterval + jitter)
}

func addressRankBefore(a, b peerEntry, now time.Time) bool {
	as, bs := addressStandingOf(a, now) == addressStaleStanding, addressStandingOf(b, now) == addressStaleStanding
	if as != bs {
		return !as
	}
	aa, ba := min(a.DialAttempts, 3), min(b.DialAttempts, 3)
	if aa != ba {
		return aa < ba
	}
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	at, bt := peerTime(a.AddedAt), peerTime(b.AddedAt)
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	return a.Address < b.Address
}

func sortAddressEntries(entries []peerEntry, now time.Time) {
	sort.Slice(entries, func(i, j int) bool { return addressRankBefore(entries[i], entries[j], now) })
}

func (s *Service) peerNow() time.Time {
	if s.peerClock != nil {
		return s.peerClock().UTC()
	}
	return time.Now().UTC()
}

type addressDialAttemptKey struct{}
type addressDialAttempt struct{ performed bool }

// localAddressDialOutcomes are decisions that never reached the endpoint.
// Refusals by the remote listener remain chargeable for aging, independently
// of the separate score policy in unchargedDialOutcomes.
var localAddressDialOutcomes = []error{
	context.Canceled,
	errLegacyRefusedBackoff, errV2RetryHeld, errLegacyProbeLocalFailure,
	errDialAbortedLocally, errPinStoreFull, errPinRoomAwaited, domain.ErrIdentityPinRevisionExhausted,
	errV2DialLocalFailure, errOldVersionAwaited,
	errOverlayWithoutProxy, errProxyUnavailable, errInvalidOverlayAddress, errAddressRecheckHeld,
}

func localAddressDialOutcome(err error) bool {
	for _, local := range localAddressDialOutcomes {
		if errors.Is(err, local) {
			return true
		}
	}
	return false
}

func (s *Service) recordAddressDialFailure(address domain.PeerAddress) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	entry := s.persistedMeta[address]
	if entry == nil {
		return
	}
	now := s.peerNow()
	entry.DialAttempts = min(entry.DialAttempts+1, 255)
	entry.LastDialAttemptAt = &now
}

// noteAddressSessionLocked requires peerMu (write). It uses the actual endpoint
// before health aliases collapse fallback ports to a primary address.
func (s *Service) noteAddressSessionLocked(address domain.PeerAddress, direction domain.PeerDirection) {
	if direction != peerDirectionOutbound {
		return
	}
	entry := s.persistedMeta[address]
	if entry == nil {
		return
	}
	now := s.peerNow()
	entry.DialAttempts = 0
	entry.LastSessionAt = &now
	entry.LastDialAttemptAt = &now
}

func protectedAddressSource(source domain.PeerSource) bool {
	return source == domain.PeerSourceManual || source == domain.PeerSourceBootstrap
}

// makeAutomaticAddressRoomLocked requires peerMu (write), with peerAdmissionMu
// held across the caller's insertion and finishAddressEviction side effects.
func (s *Service) makeAutomaticAddressRoomLocked(now time.Time) (*addressEviction, bool) {
	count := 0
	var victim *peerEntry
	for _, peer := range s.peers {
		entry := s.persistedMeta[peer.Address]
		source := peer.Source
		if entry != nil {
			source = entry.Source
		}
		if protectedAddressSource(source) || protectedAddressSource(peer.Source) {
			continue
		}
		count++
		if entry == nil || !s.addressEvictableLocked(*entry, now) {
			continue
		}
		if victim == nil || addressVictimBefore(*entry, *victim) {
			victim = entry
		}
	}
	if count < maxPersistedPeers {
		return nil, true
	}
	if victim == nil {
		return nil, false
	}
	address := victim.Address
	id := s.peerIDs.identityOf(address)
	s.forgetPeerAddressLocked(address)
	eviction := &addressEviction{address: address}
	if !id.IsZero() && len(s.orphanedIdentitiesLocked([]domain.PeerIdentity{id})) > 0 {
		eviction.identity = id
	}
	return eviction, true
}

// addressEvictableLocked requires peerMu (write).
func (s *Service) addressEvictableLocked(entry peerEntry, now time.Time) bool {
	if protectedAddressSource(entry.Source) || !addressStale(entry, now) {
		return false
	}
	if entry.VersionLockout.IsActive() || peerTime(entry.BannedUntil).After(now) || peerTime(entry.RemoteBannedUntil).After(now) {
		return false
	}
	if h := s.health[entry.Address]; h != nil && (h.Connected || h.BannedUntil.After(now)) {
		return false
	}
	if entry.ActiveDials > 0 || s.sessions[entry.Address] != nil {
		return false
	}
	if target, active := s.connectOnlyTarget(); active && target == entry.Address {
		return false
	}
	return entry.LastDialAttemptAt == nil || !now.Before(entry.LastDialAttemptAt.Add(time.Minute))
}

func addressVictimBefore(a, b peerEntry) bool {
	if (a.LastSessionAt == nil) != (b.LastSessionAt == nil) {
		return a.LastSessionAt == nil
	}
	if a.DialAttempts != b.DialAttempts {
		return a.DialAttempts > b.DialAttempts
	}
	at, bt := peerTime(a.LastSessionAt), peerTime(b.LastSessionAt)
	if a.LastSessionAt == nil {
		at = peerTime(a.AddedAt)
	}
	if b.LastSessionAt == nil {
		bt = peerTime(b.AddedAt)
	}
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	return a.Address < b.Address
}

// forgetPeerAddressLocked requires peerMu (write). Security-store pins,
// endpoint bindings, and downgrade marks have their own lifetime and survive.
func (s *Service) forgetPeerAddressLocked(address domain.PeerAddress) {
	for i, peer := range s.peers {
		if peer.Address == address {
			s.peers = append(s.peers[:i], s.peers[i+1:]...)
			break
		}
	}
	delete(s.persistedMeta, address)
	delete(s.health, address)
	delete(s.peerTypes, address)
	delete(s.peerIDs, address)
	delete(s.peerVersions, address)
	delete(s.peerBuilds, address)
	delete(s.metaOrphanFirstSeen, address)
	delete(s.dialOrigin, address)
}

type addressEviction struct {
	address  domain.PeerAddress
	identity domain.PeerIdentity
}

func (s *Service) finishAddressEviction(eviction *addressEviction) {
	if eviction == nil {
		return
	}
	address := eviction.address
	s.ipStateMu.Lock()
	if !eviction.identity.IsZero() {
		delete(s.observedAddrs, eviction.identity)
	}
	delete(s.observedIPHistoryByPeer, address)
	s.ipStateMu.Unlock()
	s.deliveryMu.Lock()
	if _, ok := s.pending[address]; ok {
		s.capPendingRingLocked(address, 0)
		s.statusMu.Lock()
		s.refreshAggregatePendingLocked()
		s.statusMu.Unlock()
	}
	s.deliveryMu.Unlock()
	if s.peerProvider != nil {
		s.peerProvider.Remove(address)
	}
	s.markPeerStateDirty()
}

var errAddressRecheckHeld = errors.New("stale address awaits its recheck")

// beginAddressDial takes peerMu itself; the returned release owns a sync.Once,
// so late CM disposal and a running session can both release the same lease.
func (s *Service) beginAddressDial(address domain.PeerAddress) (func(), error) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	entry := s.persistedMeta[address]
	if entry == nil {
		return func() {}, nil
	}
	now := s.peerNow()
	if !entry.AllowImmediateDial && addressStale(*entry, now) && now.Before(addressRecheckAt(*entry)) {
		return nil, errAddressRecheckHeld
	}
	entry.AllowImmediateDial = false
	entry.ActiveDials++
	return sync.OnceFunc(func() { s.peerMu.Lock(); entry.ActiveDials--; s.peerMu.Unlock() }), nil
}
