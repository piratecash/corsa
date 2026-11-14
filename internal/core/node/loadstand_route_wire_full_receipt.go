package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

type routeWireFullClaim struct {
	hops uint8
	seq  uint64
}

type routeWireFullReceive struct {
	epoch   uint64
	claims  map[domain.PeerIdentity]routeWireFullClaim
	receipt *domain.RouteWireFullReceipt
}

// Called under the observer lock for full frames wholly inside the window.
// Repeated stable entries contribute once to the window's set.
func (o *LoadstandRouteWireObservation) recordFullReceive(conn *routeWireConnection, line string) bool {
	frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(line))
	if err != nil || frame.Kind != protocol.RouteAnnounceV3KindFull || frame.Epoch == 0 ||
		len(frame.Entries) == 0 || len(frame.Entries) > maxRoutesPerAnnounceFrame {
		return false
	}
	full := conn.fullReceive
	if full == nil {
		full = &routeWireFullReceive{epoch: frame.Epoch, claims: make(map[domain.PeerIdentity]routeWireFullClaim)}
		conn.fullReceive = full
	}
	if full.epoch != frame.Epoch {
		return false
	}
	for _, entry := range frame.Entries {
		id := domain.PeerIdentityFromWire(entry.Identity)
		if id.IsZero() || id == conn.peer || entry.Hops < 1 || entry.Hops >= routing.HopsInfinity-1 {
			return false
		}
		claim := routeWireFullClaim{hops: entry.Hops, seq: entry.SeqNo}
		if prior, found := full.claims[id]; found {
			if prior != claim {
				return false
			}
			continue
		}
		if o.fullRows >= o.fullBudget || len(full.claims) >= domain.RouteWireFullMaxDestinations {
			return false
		}
		full.claims[id] = claim
		o.fullRows++
	}
	return true
}

// CaptureLoadstandRouteWireFullReceipts reads the table before stopping the
// original session owners, on the fixture's census path.
func (s *Service) CaptureLoadstandRouteWireFullReceipts() error {
	o := s.routeWireObserver()
	if o == nil || o.fullBudget == 0 || s.routingTable == nil {
		return errRouteWireIncomplete
	}
	return o.captureFullReceipts(s.routingTable.Snapshot)
}

func (o *LoadstandRouteWireObservation) captureFullReceipts(snapshot func() routing.Snapshot) error {
	o.mu.Lock()
	if o.sealed || o.incomplete || o.window.advance() != nil || o.window.last <= o.window.windowEnd || o.fullPassesPending != 0 {
		o.mu.Unlock()
		return errRouteWireIncomplete
	}
	at := o.window.last
	hasClaims := o.fullRows != 0
	taken := o.fullTaken
	o.mu.Unlock()
	if taken {
		return nil
	}

	// Avoid holding the observer lock while taking the routing table lock.
	var table routing.Snapshot
	if hasClaims {
		table = snapshot()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || o.incomplete || o.window.advance() != nil {
		return errRouteWireIncomplete
	}
	if o.fullTaken {
		return nil
	}
	for _, conn := range o.connections {
		full := conn.fullReceive
		if full == nil {
			continue
		}
		sha, ok := full.claimDigest(conn.peer, table)
		if !ok {
			o.incomplete = true
			return errRouteWireIncomplete
		}
		full.receipt = &domain.RouteWireFullReceipt{
			Destinations: len(full.claims), IdentitySHA256: sha, Epoch: full.epoch, SnapshotAfterNS: at,
		}
		full.claims = nil
	}
	o.fullTaken = true
	return nil
}

func (f *routeWireFullReceive) claimDigest(sender domain.PeerIdentity, table routing.Snapshot) (string, bool) {
	if sender.IsZero() || len(f.claims) == 0 || table.TakenAt.IsZero() {
		return "", false
	}
	ids := slices.SortedFunc(maps.Keys(f.claims), func(a, b domain.PeerIdentity) int { return bytes.Compare(a[:], b[:]) })
	hash := sha256.New()
	for _, id := range ids {
		want := f.claims[id]
		found := false
		for _, row := range table.Routes[id] {
			if row.NextHop != sender {
				continue
			}
			if found || row.Identity != id || row.Source != routing.RouteSourceAnnouncement || row.Hops != int(want.hops)+1 ||
				row.SeqNo != want.seq || row.IsWithdrawn() || row.ExpiresAt.IsZero() || row.IsExpired(table.TakenAt) {
				return "", false
			}
			found = true
		}
		if !found {
			return "", false
		}
		_, _ = hash.Write(id[:])
	}
	return hex.EncodeToString(hash.Sum(nil)), true
}
