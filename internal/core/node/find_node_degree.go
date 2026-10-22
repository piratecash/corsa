package node

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Inactive statistical observation only. No F1 session, pin, admission, frame or
// routing authority is created here. Ordinary constructors leave Service.degree nil.
const degreeRecent = 600 * time.Second
const degreeHistoryLimit = 64
const degreeAllocationUnit = 256 // conservative row-storage arithmetic, not RSS acceptance
var errDegreeUnavailable = errors.New("degree observation unavailable")
var errDegreeProfile = errors.New("invalid degree profile")

type degreeProfile struct{ MaxActive int }

// A concrete owned clock, never an arbitrary callback under either mutex.
// fixture is minted only by package tests; ordinary candidate composition uses
// an origin obtained from time.Now, preserving Go's monotonic component.
type degreeClock struct {
	origin  time.Time
	fixture *atomic.Int64
}

func (c degreeClock) elapsed() time.Duration {
	if c.fixture != nil {
		return time.Duration(c.fixture.Load())
	}
	return time.Since(c.origin)
}

type degreeKey struct {
	outgoing bool
	group    domain.SourceGroup
	identity domain.PeerIdentity
}
type degreeHistoryRow struct {
	key  degreeKey
	last time.Duration
}
type degreeActiveRow struct {
	life       *proofTransportLifetime
	witness    *degreeWitness
	generation uint64
}
type degreeWitness struct {
	origin            *proofTransportLifetime
	owner             *degreeOwner
	key               degreeKey
	slot              int
	generation        uint64
	untracked, closed bool
}
type degreeOwner struct {
	mu              sync.Mutex
	token           *proofTransportServiceToken
	clock           degreeClock
	profile         degreeProfile
	active          []degreeActiveRow
	history         [degreeHistoryLimit]degreeHistoryRow
	historyN        int
	unknown         uint64
	last            time.Duration
	poisoned        bool
	bridgePublisher findnodebridge.Publisher // immutable; zero selects legacy statistics
}
type degreeCut struct {
	at                         time.Duration
	outgoing, incoming, window int
}

// Unused constructor; required finite profile is copied before one ordinary
// Service is constructed. No default M, late setter, exported configuration or
// new network/background work. The caller owns any ordinary startup refusal.
func newDegreeCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile) (*Service, error) {
	return newDegreeService(cfg, id, bus, profile, nil)
}

// Explicit test-fixture composition, also unused by production. No caller Time
// or elapsed callback can be presented as evidence of a monotonic origin.
func newDegreeFixtureCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile, fixture *atomic.Int64) (*Service, error) {
	if fixture == nil {
		return nil, errDegreeProfile
	}
	return newDegreeService(cfg, id, bus, profile, fixture)
}

// Both roots are source-closed. The production root always passes nil; the
// only other root supplies the concrete atomic fixture. Origin is minted HERE,
// never accepted as a time.Time argument, so Round(0)/wall-only input cannot be
// confused with time.Now's original monotonic reading.
func newDegreeService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile, fixture *atomic.Int64) (*Service, error) {
	maxInt := int(^uint(0) >> 1)
	if profile.MaxActive <= 0 || profile.MaxActive > maxInt/degreeAllocationUnit {
		return nil, errDegreeProfile
	}
	clock := degreeClock{origin: time.Now()}
	if fixture != nil {
		clock = degreeClock{fixture: fixture}
	}
	s := newServiceWithDatagramConstructor(cfg, id, bus, nil, productionDatagramConstructor{})
	s.degree = &degreeOwner{token: s.proofToken, clock: clock, profile: profile, active: make([]degreeActiveRow, profile.MaxActive)}
	return s, nil
}

// Requires owner.mu. Sample after waits. Negative/regressing time permanently
// poisons new observations; ordinary connection/pin/queued work is unaffected.
func (o *degreeOwner) degreeNowLocked() (time.Duration, bool) {
	if o.bridgePublisher != (findnodebridge.Publisher{}) {
		return 0, false
	}
	now := o.clock.elapsed()
	if now < 0 || now < o.last {
		o.poisoned = true
		return 0, false
	}
	o.last = now
	return now, true
}
func (o *degreeOwner) degreePoison() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.bridgePublisher != (findnodebridge.Publisher{}) {
		publication := o.bridgePublisher.BeginPublication()
		o.poisoned = true
		publication.Complete()
	} else {
		o.poisoned = true
	}
	o.mu.Unlock()
}

// Keys are immutable. Greedy newest-first rank basis of a partition matroid:
// OUT has no per-group quota; IN has two distinct identities per group, total64.
// Refreshing an existing key cannot create another contribution. Fixed workspace
// avoids retaining a map/list of previously observed transports or identities.
func degreeKeyLess(a, b degreeKey) bool {
	if a.outgoing != b.outgoing {
		return a.outgoing
	}
	if a.group != b.group {
		return a.group < b.group
	}
	for i := range a.identity {
		if a.identity[i] != b.identity[i] {
			return a.identity[i] < b.identity[i]
		}
	}
	return false
}
func (o *degreeOwner) degreeHistoryLocked(key degreeKey, at time.Duration) {
	var trial [degreeHistoryLimit + 1]degreeHistoryRow
	n := 0
	for i := 0; i < o.historyN; i++ {
		row := o.history[i]
		if at-row.last >= degreeRecent {
			continue
		}
		if row.key == key {
			continue
		}
		trial[n] = row
		n++
	}
	trial[n] = degreeHistoryRow{key: key, last: at}
	n++
	for i := 1; i < n; i++ {
		row := trial[i]
		j := i
		for j > 0 && (row.last > trial[j-1].last || (row.last == trial[j-1].last && degreeKeyLess(row.key, trial[j-1].key))) {
			trial[j] = trial[j-1]
			j--
		}
		trial[j] = row
	}
	var next [degreeHistoryLimit]degreeHistoryRow
	used := 0
	for i := 0; i < n && used < degreeHistoryLimit; i++ {
		row := trial[i]
		if !row.key.outgoing {
			group := 0
			for j := 0; j < used; j++ {
				if !next[j].key.outgoing && next[j].key.group == row.key.group {
					group++
				}
			}
			if group >= 2 {
				continue
			}
		}
		next[used] = row
		used++
	}
	o.history, o.historyN = next, used
}

// Genuine accepted tail: id may still be0 and there need not be a registry/CM
// entry. Do not borrow ownerLocked, GetProvenProof, F1 seed or current pin class.
func (s *Service) publishDegreeInbound(conn net.Conn) {
	if s.degree == nil {
		return
	}
	wire, ok := conn.(*peekedConn)
	if !ok {
		s.degree.degreePoison()
		return
	}
	s.publishDegreeTransport(wire.common.Load())
}
func (s *Service) publishDegreeTransport(l *proofTransportLifetime) {
	o := s.degree
	if o == nil {
		return
	}
	if l == nil {
		o.degreePoison()
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return
	} // a pre-publication End earns no late presence
	if l.degreeWitness != nil {
		return
	}
	e := l.evidence
	if l.token != s.proofToken || o.token != s.proofToken || e == nil || e.raw == nil || e.wire == nil || e.raw.common.Load() != l || e.wire.common.Load() != l {
		o.degreePoison()
		return
	}
	peer, pe := e.session.Peer()
	role, re := e.session.Role()
	protected, ce := e.session.Conn()
	if pe != nil || re != nil || ce != nil || peer.Identity.IsZero() || peer.Identity != e.identity || protected != e.protected || role != l.role || (role != sessionv2.RoleDialer && role != sessionv2.RoleListener) || l.outbound != (role == sessionv2.RoleDialer) || e.group == "" {
		o.degreePoison()
		return
	}
	key := degreeKey{outgoing: role == sessionv2.RoleDialer, identity: peer.Identity}
	if !key.outgoing {
		key.group = e.group
	}
	w := &degreeWitness{origin: l, owner: o, key: key, slot: -1}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.poisoned {
		return
	}
	var at time.Duration
	if o.bridgePublisher != (findnodebridge.Publisher{}) {
		publication := o.bridgePublisher.BeginClockedPublication()
		defer publication.Complete()
		t := l.bridgeTerminal
		if t == nil || t != s.bridgeTerminal || t.owner != o || t.token != l.token || t.publisher != o.bridgePublisher {
			o.poisoned = true
			return
		}
		point, err := publication.Sample()
		if err != nil || point.Elapsed() < o.last {
			o.poisoned = true
			return
		}
		at = point.Elapsed()
		o.last = at
	} else {
		var ok bool
		at, ok = o.degreeNowLocked()
		if !ok {
			return
		}
	}
	for i := range o.active {
		row := &o.active[i]
		if row.life != nil {
			continue
		}
		if row.generation == ^uint64(0) {
			o.poisoned = true
			return
		}
		row.generation++
		row.life, row.witness = l, w
		w.slot, w.generation = i, row.generation
		break
	}
	if w.slot < 0 {
		if o.unknown == ^uint64(0) {
			o.poisoned = true
			return
		}
		o.unknown++
		w.untracked = true
	}
	l.degreeWitness = w
	o.degreeHistoryLocked(key, at)
}

// Called only by the common at-most-once End, with common.mu held, BEFORE any
// common evidence/entry/peer clear. No store, peerMu, CM, I/O or callback here.
func (l *proofTransportLifetime) degreeEndLocked() {
	w := l.degreeWitness
	if w == nil {
		return
	}
	o := w.owner
	if o == nil || o.bridgePublisher != (findnodebridge.Publisher{}) {
		return // an owned owner never enters the legacy retirement clock
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if w.closed {
		o.poisoned = true
		return
	}
	w.closed = true
	at, valid := o.degreeNowLocked()
	if w.untracked {
		if o.unknown == 0 {
			o.poisoned = true
		} else {
			o.unknown--
		}
	} else if w.slot < 0 || w.slot >= len(o.active) {
		o.poisoned = true
	} else {
		row := &o.active[w.slot]
		if row.life != l || row.witness != w || row.generation != w.generation {
			o.poisoned = true
		} else {
			row.life, row.witness = nil, nil
		}
	}
	if valid && !o.poisoned {
		o.degreeHistoryLocked(w.key, at)
	}
	l.degreeWitness = nil
}

// A complete local observation only. No ordinary connection/registry scan and
// no common lock while owner.mu is held. Unknown active coverage returns no
// partial answer, not a first-M truncation or a fictitious zero-degree fallback.
func (o *degreeOwner) degreeSnapshot() (degreeCut, error) {
	if o == nil || o.bridgePublisher != (findnodebridge.Publisher{}) {
		return degreeCut{}, errDegreeUnavailable
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.poisoned {
		return degreeCut{}, errDegreeUnavailable
	}
	at, valid := o.degreeNowLocked()
	if !valid || o.unknown != 0 {
		return degreeCut{}, errDegreeUnavailable
	}
	for i := range o.active {
		row := &o.active[i]
		if row.life == nil {
			continue
		}
		if row.witness == nil || row.witness.owner != o || row.witness.closed || row.witness.slot != i || row.witness.generation != row.generation {
			o.poisoned = true
			return degreeCut{}, errDegreeUnavailable
		}
		o.degreeHistoryLocked(row.witness.key, at)
	}
	out := degreeCut{at: at}
	for i := 0; i < o.historyN; i++ {
		row := o.history[i]
		if at-row.last >= degreeRecent {
			continue
		}
		if row.key.outgoing {
			out.outgoing++
		} else {
			out.incoming++
		}
	}
	out.window = out.outgoing + out.incoming
	return out, nil
}
