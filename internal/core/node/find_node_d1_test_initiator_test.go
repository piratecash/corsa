package node

import (
	"context"
	"crypto/rand"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// D1 has no public lookup API. This test/13b driver is deliberately compiled
// only into the Node test executable. Its workload declares its finite target
// set before running; per-target windows have no attacker-created map keys.
// The sole owner calls Start/Poll/Receive; there is no request goroutine or
// background reader. The workload's original bounded reader feeds Receive.
type d1TestOutcome string

const (
	d1TestFound        d1TestOutcome = "Found"
	d1TestNoNeighbours d1TestOutcome = "NoOverlayNeighbours"
	d1TestTimeout      d1TestOutcome = "Timeout"
	d1TestInvalid      d1TestOutcome = "InvalidAnswer"
	d1TestLimited      d1TestOutcome = "Limited"
)

type d1TestResult struct {
	status d1TestOutcome
	answer protocol.FindNodeAnswer
	err    error
}
type d1TestOriginal struct {
	session findnode.Session
	common  *proofTransportLifetime
	core    *netcore.NetCore
}
type d1TestTarget struct {
	target domain.PeerIdentity
	starts [3]time.Duration
	count  int
}
type d1TestLookup struct {
	owner    *d1TestInitiator
	ctx      context.Context
	request  protocol.FindNodeRequest
	started  time.Duration
	peers    [3]d1TestOriginal
	waiting  [3]bool
	count    int
	invalid  bool
	terminal bool
	result   d1TestResult
}
type d1TestInitiator struct {
	service   *Service
	now       func() time.Duration
	mu        sync.Mutex
	targets   []d1TestTarget
	pending   [16]*d1TestLookup
	next      domain.FindNodeRequestID
	exhausted bool
	last      time.Duration
}

func newD1TestInitiator(s *Service, targets []domain.PeerIdentity, now func() time.Duration) *d1TestInitiator {
	driver := &d1TestInitiator{service: s, now: now}
	for _, target := range targets {
		found := false
		for _, entry := range driver.targets {
			if entry.target == target {
				found = true
				break
			}
		}
		if !target.IsZero() && !found {
			driver.targets = append(driver.targets, d1TestTarget{target: target})
		}
	}
	return driver
}

// Both the canonical send ordering and the ORIGINAL proof are checked. A
// registry identity/capability claim alone cannot become a D1 relay. Retain
// the exact common, Session and writer pointer; never re-resolve a ConnID.
func d1TestOriginals(ctx context.Context, s *Service) ([]d1TestOriginal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	identities := map[domain.PeerIdentity]bool{}
	for _, peer := range s.sessions {
		if peer != nil && !peer.peerIdentity.IsZero() {
			identities[peer.peerIdentity] = true
		}
	}
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if !info.identity.IsZero() {
			identities[info.identity] = true
		}
		return true
	})
	ordered := make([]domain.PeerIdentity, 0, len(identities))
	for id := range identities {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].String() < ordered[j].String() })
	result := make([]d1TestOriginal, 0, 3)
	for _, id := range ordered {
		if s.isSelfIdentity(id) {
			continue
		}
		for _, candidate := range s.peerSendableConnectionsLocked(id, domain.CapOverlayResponderV1, time.Now().UTC()) {
			if !candidate.proven {
				continue
			}
			connID := candidate.inboundID
			if candidate.outbound != nil {
				connID = candidate.outbound.connID
			}
			entry := s.connEntryByIDLocked(connID)
			if entry == nil || entry.common == nil || entry.core == nil {
				continue
			}
			if candidate.outbound != nil && (entry.common != candidate.outbound.common || entry.core != candidate.outbound.netCore) {
				continue
			}
			session, found, err := s.collectOwnedFindNodeRawMemberLocked(ctx, connID)
			if err != nil {
				return nil, err
			}
			if !found || session.Identity() != id {
				continue
			}
			common := entry.common
			common.mu.Lock()
			// Publication deliberately retires staging evidence. The collector
			// above validates the original token/role/publication tuple; use
			// that SAME core's retained proof, never a registry capability claim.
			valid := !common.ended && common.token == s.proofToken && common.findNode != nil && common.findNode.published && common.findNode.session == session
			if candidate.outbound != nil {
				peer := candidate.outbound.proven
				valid = valid && common.outbound && common.peer == candidate.outbound && peer != nil
				if valid {
					proof, proven := peer.Proof()
					identity, identityOK := proof.Identity()
					valid = proven && identityOK && identity == id && peer.Identity == id && slices.Contains(peer.Intro.Capabilities, string(domain.CapOverlayResponderV1))
				}
			} else {
				auth := entry.core.Auth()
				valid = valid && !common.outbound && auth != nil
				if valid {
					proof, proven := auth.ProvenIdentity()
					identity, identityOK := proof.Identity()
					valid = proven && identityOK && identity == id && slices.Contains(auth.Hello.Capabilities, string(domain.CapOverlayResponderV1))
				}
			}
			common.mu.Unlock()
			if !valid {
				continue
			}
			result = append(result, d1TestOriginal{session: session, common: common, core: entry.core})
			break // one canonical original connection per relay identity
		}
		if len(result) == 3 {
			break
		}
	}
	return result, nil
}
func (driver *d1TestInitiator) sameOriginal(peer d1TestOriginal) bool {
	s := driver.service
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	entry := s.connEntryByIDLocked(peer.session.ConnID())
	if entry == nil || entry.common != peer.common || entry.core != peer.core {
		return false
	}
	peer.common.mu.Lock()
	defer peer.common.mu.Unlock()
	return !peer.common.ended && peer.common.token == s.proofToken && peer.common.findNode != nil && peer.common.findNode.published && peer.common.findNode.session == peer.session
}
func (driver *d1TestInitiator) finish(lookup *d1TestLookup, result d1TestResult) d1TestResult {
	lookup.terminal = true
	lookup.result = result
	for i, current := range driver.pending {
		if current == lookup {
			driver.pending[i] = nil
			break
		}
	}
	return result
}
func (driver *d1TestInitiator) pollLocked(lookup *d1TestLookup) d1TestResult {
	if lookup == nil || lookup.owner != driver {
		return d1TestResult{err: findnode.ErrUnauthenticated}
	}
	if lookup.terminal {
		return lookup.result
	}
	if err := lookup.ctx.Err(); err != nil {
		return driver.finish(lookup, d1TestResult{err: err})
	}
	now := driver.now()
	if now < 0 || now < driver.last {
		return driver.finish(lookup, d1TestResult{err: findnode.ErrClock})
	}
	driver.last = now
	if now-lookup.started >= peerRequestTimeout {
		status := d1TestTimeout
		if lookup.invalid {
			status = d1TestInvalid
		}
		return driver.finish(lookup, d1TestResult{status: status})
	}
	return d1TestResult{}
}
func (driver *d1TestInitiator) Poll(lookup *d1TestLookup) d1TestResult {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.pollLocked(lookup)
}
func (driver *d1TestInitiator) Start(ctx context.Context, target domain.PeerIdentity) (*d1TestLookup, d1TestResult) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, d1TestResult{err: err}
	}
	now := driver.now()
	if now < 0 || now < driver.last {
		return nil, d1TestResult{err: findnode.ErrClock}
	}
	driver.last = now
	for _, lookup := range driver.pending {
		if lookup != nil {
			driver.pollLocked(lookup)
		}
	}
	vacancy := -1
	for i, lookup := range driver.pending {
		if lookup == nil {
			vacancy = i
			break
		}
	}
	if vacancy < 0 || driver.exhausted {
		return nil, d1TestResult{status: d1TestLimited}
	}
	var window *d1TestTarget
	for i := range driver.targets {
		if driver.targets[i].target == target {
			window = &driver.targets[i]
			break
		}
	}
	if window == nil {
		return nil, d1TestResult{err: findnode.ErrInvalid}
	}
	count := 0
	for i := 0; i < window.count; i++ {
		if now-window.starts[i] < 30*time.Second {
			window.starts[count] = window.starts[i]
			count++
		}
	}
	window.count = count
	if count == 3 {
		return nil, d1TestResult{status: d1TestLimited}
	}
	peers, err := d1TestOriginals(ctx, driver.service)
	if err != nil {
		return nil, d1TestResult{err: err}
	}
	if len(peers) == 0 {
		return nil, d1TestResult{status: d1TestNoNeighbours}
	}
	lookup := &d1TestLookup{owner: driver, ctx: ctx, started: now, request: protocol.FindNodeRequest{ID: driver.next, Target: target}}
	if _, err := rand.Read(lookup.request.Challenge[:]); err != nil {
		return nil, d1TestResult{err: err}
	}
	raw, err := protocol.MarshalFindNodeRequest(lookup.request)
	if err != nil {
		return nil, d1TestResult{err: err}
	}
	raw = append(raw, '\n')
	if driver.next == math.MaxUint64 {
		driver.exhausted = true
	} else {
		driver.next++
	}
	window.starts[window.count] = now
	window.count++
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, d1TestResult{err: err}
		}
		if !driver.sameOriginal(peer) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, d1TestResult{err: err}
		}
		current := driver.now()
		if current < driver.last {
			return nil, d1TestResult{err: findnode.ErrClock}
		}
		driver.last = current
		if current-lookup.started >= peerRequestTimeout {
			return nil, d1TestResult{status: d1TestTimeout}
		}
		if peer.core.SendRaw(raw) != netcore.SendOK {
			continue
		}
		lookup.peers[lookup.count] = peer
		lookup.waiting[lookup.count] = true
		lookup.count++
	}
	if lookup.count == 0 {
		return nil, d1TestResult{status: d1TestLimited}
	}
	driver.pending[vacancy] = lookup
	return lookup, d1TestResult{}
}

// The owning original bounded reader supplies the exact common it read from.
// No negative wire response is invented. Wrong ID/foreign transport cannot
// resolve an attempt. Invalid signed content is local to that retained peer;
// a still-pending parallel relay may nevertheless produce a valid answer.
func (driver *d1TestInitiator) Receive(lookup *d1TestLookup, original *proofTransportLifetime, raw []byte) d1TestResult {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if lookup != nil && lookup.owner == driver && lookup.terminal {
		return d1TestResult{err: findnode.ErrStaleWork}
	}
	result := driver.pollLocked(lookup)
	if result.status != "" || result.err != nil {
		return result
	}
	index := -1
	for i := 0; i < lookup.count; i++ {
		if lookup.waiting[i] && lookup.peers[i].common == original {
			index = i
			break
		}
	}
	if index < 0 || !driver.sameOriginal(lookup.peers[index]) {
		return d1TestResult{err: findnode.ErrUnauthenticated}
	}
	answer, err := protocol.ParseFindNodeAnswer(raw)
	if err == nil && answer.ID != lookup.request.ID {
		return d1TestResult{}
	}
	if err == nil {
		err = protocol.VerifyFindNodeAnswer(answer, lookup.request, domain.NetworkID(networkName))
	}
	if err == nil {
		return driver.finish(lookup, d1TestResult{status: d1TestFound, answer: answer})
	}
	lookup.invalid = true
	lookup.waiting[index] = false
	for i := 0; i < lookup.count; i++ {
		if lookup.waiting[i] {
			return d1TestResult{}
		}
	}
	return driver.finish(lookup, d1TestResult{status: d1TestInvalid})
}
