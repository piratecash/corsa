package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

type routeWireFullSend struct {
	projection domain.RouteWireFullSend
	offered    routeWireCounter
	active     bool
}

// Owned by one SendRouteAnnounceV3 invocation. Its entries remain owned
// by that caller. The connection's shared state uses the observer mutex.
type routeWireFullPass struct {
	observer    *LoadstandRouteWireObservation
	begin       time.Duration
	entries     []routing.AnnounceEntry
	epoch       uint64
	source      domain.RouteWireFullSource
	identitySHA string
	next        int
	connID      domain.ConnID
	send        *routeWireFullSend
}

func (o *LoadstandRouteWireObservation) startFullPass(epoch uint64, entries []routing.AnnounceEntry, source domain.RouteWireFullSource) *routeWireFullPass {
	if o == nil || o.fullBudget == 0 {
		return nil
	}
	o.mu.Lock()
	if o.reportSealed || o.window.advance() != nil {
		o.incomplete = true
		o.mu.Unlock()
		return nil
	}
	if o.window.last > o.window.windowEnd {
		o.mu.Unlock()
		return nil
	}
	p := &routeWireFullPass{observer: o, begin: o.window.last, epoch: epoch, source: source}
	o.fullPassesPending++
	o.mu.Unlock()
	if p.begin <= o.window.begin || p.begin >= o.window.windowEnd {
		return p
	}
	if epoch == 0 || len(entries) == 0 || len(entries) > domain.RouteWireFullMaxDestinations ||
		source != domain.RouteWireFullPeriodic && source != domain.RouteWireFullTriggered && source != domain.RouteWireFullConnect {
		o.fail()
		return p
	}
	ids := make([]domain.PeerIdentity, len(entries))
	for i, entry := range entries {
		ids[i] = entry.Identity
		if entry.Identity.IsZero() || entry.Hops < 1 || entry.Hops >= routing.HopsInfinity-1 {
			o.fail()
			return p
		}
	}
	slices.SortFunc(ids, func(a, b domain.PeerIdentity) int { return bytes.Compare(a[:], b[:]) })
	digest := sha256.New()
	for i, id := range ids {
		if i > 0 && id == ids[i-1] {
			o.fail()
			return p
		}
		_, _ = digest.Write(id[:])
	}
	p.entries, p.identitySHA = entries, hex.EncodeToString(digest.Sum(nil))
	return p
}

func (p *routeWireFullPass) frame(id domain.ConnID, line string) {
	if p == nil || p.entries == nil {
		return
	}
	frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(line))
	o := p.observer
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[id]
	if o.reportSealed || conn == nil || conn.peer.IsZero() || conn.finished[routeWireWrite] ||
		p.send != nil && id != p.connID || err != nil || frame.Kind != protocol.RouteAnnounceV3KindFull ||
		frame.Epoch != p.epoch || len(frame.Entries) == 0 || len(frame.Entries) > len(p.entries)-p.next {
		o.incomplete = true
		return
	}
	for i, entry := range frame.Entries {
		want := p.entries[p.next+i]
		if want.Identity == conn.peer || domain.PeerIdentityFromWire(entry.Identity) != want.Identity ||
			int(entry.Hops) != want.Hops || entry.SeqNo != want.SeqNo {
			o.incomplete = true
			return
		}
	}
	if p.send == nil {
		full := conn.fullSend
		if full == nil {
			full = &routeWireFullSend{projection: domain.RouteWireFullSend{
				Destinations: len(p.entries), IdentitySHA256: p.identitySHA, Epoch: p.epoch,
			}, offered: routeWireCounter{digest: sha256.New()}}
			conn.fullSend = full
		}
		if full.active || full.projection.Destinations != len(p.entries) || full.projection.IdentitySHA256 != p.identitySHA ||
			full.projection.Epoch != p.epoch {
			o.incomplete = true
			return
		}
		full.active = true
		p.send, p.connID = full, id
	}
	p.next += len(frame.Entries)
	p.send.offered.frames++
	p.send.offered.bytes += uint64(len(line))
	_, _ = p.send.offered.digest.Write([]byte(line))
}

func (p *routeWireFullPass) finish(sentAll bool) {
	o := p.observer
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fullPassesPending--
	if p.send != nil {
		p.send.active = false
	}
	if o.reportSealed || o.window.advance() != nil {
		o.incomplete = true
		return
	}
	if o.window.outside(p.begin) {
		return
	}
	if p.begin <= o.window.begin || o.window.last >= o.window.windowEnd {
		o.incomplete = true
		return
	}
	if !sentAll {
		// No frame supplied means no full pass and no wire intent to certify.
		// Earlier failures remain sticky; any supplied frame still requires
		// the original complete-pass and actual-writer proof.
		if p.send != nil || p.next != 0 {
			o.incomplete = true
		}
		return
	}
	if p.send == nil || p.next != len(p.entries) {
		o.incomplete = true
		return
	}
	p.send.projection.Passes++
	switch p.source {
	case domain.RouteWireFullPeriodic:
		p.send.projection.PeriodicPasses++
	case domain.RouteWireFullTriggered:
		p.send.projection.TriggeredPasses++
	case domain.RouteWireFullConnect:
		p.send.projection.ConnectPasses++
	}
}

func (c *routeWireConnection) fullSendProof(actual RouteWireCount) (*domain.RouteWireFullSend, bool) {
	if c.fullSend == nil {
		return nil, actual.Frames == 0
	}
	f := c.fullSend
	if f.active || !f.projection.ValidPasses() || f.offered.frames != actual.Frames || f.offered.bytes != actual.Bytes ||
		hex.EncodeToString(f.offered.digest.Sum(nil)) != actual.SHA256 {
		return nil, false
	}
	return &f.projection, true
}
