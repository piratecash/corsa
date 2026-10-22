package node

import (
	"errors"
	"io"
	"math"
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// Only this tagged test domain is sealed. The production constructing root
// still has no reader/Ready/cut authority. No stamp is held across Node waits.
func registryFixtureSeal(t *testing.T, d findnodebridge.FixtureDomain) {
	t.Helper()
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
}
func registryFreshReadable(t *testing.T, d findnodebridge.FixtureDomain) {
	t.Helper()
	stamp := pinPublicationCapture(t, d)
	lease, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal("fresh registry generation refused", err)
	}
	lease.Release()
}
func registryMapsAbsent(s *Service, conn net.Conn, id domain.ConnID) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	_, primary := s.conns[id]
	_, secondary := s.connIDByNetConn[conn]
	return !primary && !secondary
}
func registryUnregister(s *Service, conn net.Conn) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	s.unregisterConnLocked(conn)
}

func TestRegistryPairedInsertPublication(t *testing.T) {
	for _, outbound := range []bool{false, true} {
		name := "inbound"
		if outbound {
			name = "outbound"
		}
		t.Run(name, func(t *testing.T) {
			s, _, d := ownedF1TestService(t)
			left, right := net.Pipe()
			owner := newF1FixtureOwner()
			owner.rawServer, owner.rawClient = left, right
			t.Cleanup(func() { owner.stopAndJoin(t) }) // before the writer starts
			direction := netcore.Inbound
			if outbound {
				direction = netcore.Outbound
			}
			id := s.allocateConnID("registry_publication_plain", domain.PeerAddress("pipe"))
			core := newTestNetCore(t, id, left, direction, netcore.Options{})
			owner.inboundCore = core
			// This ordinary core intentionally has no proof/common: a separate genuine
			// P1/common publication must not mask an omitted paired-map publication.
			registryFixtureSeal(t, d)
			before := pinPublicationCapture(t, d)
			s.peerMu.Lock()
			if outbound {
				s.attachOutboundCoreLocked(left, core)
			} else {
				s.registerInboundConnLocked(left, core, nil, nil)
			}
			entry := s.conns[id]
			reciprocal := entry != nil && entry.core == core && entry.common == nil && s.connIDByNetConn[left] == id
			s.peerMu.Unlock()
			if !reciprocal || !pinPublicationRefused(d, before) {
				t.Fatal("paired insertion did not invalidate its own generation")
			}
			registryFreshReadable(t, d)
			core.Close() // original writer must exit before its row is removed
			registryUnregister(s, left)
			if !registryMapsAbsent(s, left, id) {
				t.Fatal("ordinary exact keys retained")
			}
		})
	}
}

func TestRegistryOriginalEndThenDeletePublication(t *testing.T) {
	s, _, d := ownedF1TestService(t)
	peer, _, _ := ownedF1TestService(t)
	f := f1PairOn(t, true, nil, s, peer)
	l := f.inbound.life
	s.publishAcceptedProofTransport(f.inbound)
	members := f1Members(t, s)
	if len(members.sessions) != 1 {
		t.Fatal("genuine canonical F1 absent")
	}
	l.mu.Lock()
	w := l.degreeWitness
	entry := l.entry
	l.mu.Unlock()
	if w == nil || w.origin != l || w.untracked || w.slot < 0 || entry == nil {
		t.Fatal("original did not pass genuine P1/registry attach")
	}
	registryFixtureSeal(t, d)
	a := pinPublicationCapture(t, d)
	l.end()
	s.degree.mu.Lock()
	retired := w.closed && s.degree.active[w.slot].life == nil
	s.degree.mu.Unlock()
	_, err := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, members.sessions, findnode.Instant(math.MaxInt64), func(findnode.SessionInfo) (findnode.SessionClass, error) { return findnode.ClassOrdinary, nil })
	s.peerMu.RLock()
	stillListed := s.conns[f.inbound.id] == entry && s.connIDByNetConn[f.fixtureOwner.inboundWire] == f.inbound.id
	s.peerMu.RUnlock()
	if !retired || !errors.Is(err, findnode.ErrUnauthenticated) || !stillListed || !pinPublicationRefused(d, a) {
		t.Fatal("End did not independently retire original authority")
	}
	// AFTER End: omitting only the registry delete publication leaves b valid.
	b := pinPublicationCapture(t, d)
	f.fixtureOwner.inboundCore.Close()
	registryUnregister(s, f.fixtureOwner.inboundWire)
	if !registryMapsAbsent(s, f.fixtureOwner.inboundWire, f.inbound.id) || !pinPublicationRefused(d, b) {
		t.Fatal("separate paired-delete publication absent")
	}
	registryFreshReadable(t, d)
	registryUnregister(s, f.fixtureOwner.inboundWire)
	registryFreshReadable(t, d)
}

func TestRegistryCleanupIgnoresObservationFailure(t *testing.T) {
	for _, kind := range []string{"cancelled", "poisoned"} {
		t.Run(kind, func(t *testing.T) {
			s, clock, d := ownedF1TestService(t)
			peer, _, _ := ownedF1TestService(t)
			f := f1PairOn(t, true, nil, s, peer)
			l := f.inbound.life
			s.publishAcceptedProofTransport(f.inbound)
			members := f1Members(t, s)
			if len(members.sessions) != 1 {
				t.Fatal("genuine cleanup handle missing")
			}
			budget, err := connbudget.New(connbudget.Config{})
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := budget.Reserve(connbudget.DirectionInbound)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reservation.Release)
			s.peerMu.Lock()
			entry := s.conns[f.inbound.id]
			if entry == nil || entry.budget != nil {
				s.peerMu.Unlock()
				t.Fatal("fixture must own its new exact reservation")
			}
			entry.budget = reservation
			receive := entry.receive
			s.peerMu.Unlock()
			l.mu.Lock()
			w := l.degreeWitness
			l.mu.Unlock()
			if w == nil || w.origin != l || w.untracked || w.slot < 0 || budget.Snapshot().Used != 1 {
				t.Fatal("cleanup fixture lacks genuine mint/reservation")
			}
			if kind == "cancelled" {
				s.runCancel()
			} else {
				clock.Store(-1)
				sample := d.Publisher().BeginClockedPublication()
				_, sampleErr := sample.Sample()
				sample.Complete()
				if sampleErr == nil {
					t.Fatal("fixture did not produce lawful clock poison")
				}
			}
			f.fixtureOwner.inboundCore.Close()
			registryUnregister(s, f.fixtureOwner.inboundWire)
			s.degree.mu.Lock()
			retired := w.closed && s.degree.active[w.slot].life == nil
			s.degree.mu.Unlock()
			_, viewErr := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, members.sessions, findnode.Instant(math.MaxInt64), func(findnode.SessionInfo) (findnode.SessionClass, error) { return findnode.ClassOrdinary, nil })
			if !registryMapsAbsent(s, f.fixtureOwner.inboundWire, f.inbound.id) || !retired || receive.ReceiveReady() || budget.Snapshot().Used != 0 || !errors.Is(viewErr, findnode.ErrUnauthenticated) {
				t.Fatal("observation refusal skipped original cleanup", kind)
			}
			registryUnregister(s, f.fixtureOwner.inboundWire)
			if budget.Snapshot().Used != 0 {
				t.Fatal("duplicate cleanup returned capacity twice")
			}
		})
	}
}

// Override only the fixture's original server LocalAddr before TLS/dial
// construction. The sockets and both TLS proofs are genuine; this supplies a
// stable dial-address key for a controlled replacement, not a receipt/identity.
type registryFixtureAddressConn struct{ net.Conn }

func (c registryFixtureAddressConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 64001}
}
func registryPairAtSameAddress(t *testing.T, accepted, dialer *Service) f1ProofFixture {
	t.Helper()
	owner := newF1FixtureOwner()
	t.Cleanup(func() { owner.stopAndJoin(t) })
	server, client := tcpPair(t)
	original := registryFixtureAddressConn{Conn: server}
	owner.rawServer, owner.rawClient = original, client
	return f1PairOnRaw(t, true, nil, accepted, dialer, owner, original, client)
}

func TestRegistryCMLateOwnerKeepsReplacement(t *testing.T) {
	for _, kind := range []string{"teardown", "refused", "served"} {
		t.Run(kind, func(t *testing.T) {
			d, _, _ := ownedF1TestService(t)
			a, _, _ := ownedF1TestService(t)
			b, _, _ := ownedF1TestService(t)
			first := registryPairAtSameAddress(t, a, d)
			second := registryPairAtSameAddress(t, b, d)
			old, current := first.outbound, second.outbound
			if old.address != current.address || old == current || old.common == current.common || old.connID == current.connID {
				t.Fatal("fixture needs two genuine original owners at the same fixture dial-address key")
			}
			// Registry ownership only: both Sessions/proofs/P1 are genuine, but this
			// fixture deliberately does not forge an accepted CM receipt or claim a
			// published B F1. Actual accepted CM/F1 is covered by the existing S1 test.
			if !d.registerCMSession(old, old.address) || !d.registerCMSession(current, current.address) {
				t.Fatal("genuine replacement registration refused")
			}
			// Controlled pre-existing alias bookkeeping, not a fallback proof/class.
			alias := domain.PeerAddress("127.0.0.1:64002")
			d.peerMu.Lock()
			d.dialOrigin[current.address] = alias
			d.peerMu.Unlock()
			old.endProofTransport()
			current.common.mu.Lock()
			w := current.common.degreeWitness
			lifeLive := !current.common.ended
			current.common.mu.Unlock()
			if w == nil || w.origin != current.common || w.untracked || w.slot < 0 || !lifeLive {
				t.Fatal("B genuine degree origin absent")
			}
			switch kind {
			case "teardown":
				d.onCMSessionTeardown(SessionInfo{Address: old.address, DialAddress: old.address, Session: old})
			case "refused":
				d.withdrawRefusedCMSession(old, old.address, 1, io.EOF)
			case "served":
				d.withdrawServedCMSession(old, old.address, 1, io.EOF)
			}
			d.peerMu.RLock()
			d.deliveryMu.Lock()
			same := d.sessions[current.address] == current
			_, upstream := d.upstream[current.address]
			sameAlias := d.dialOrigin[current.address] == alias
			d.deliveryMu.Unlock()
			d.peerMu.RUnlock()
			current.common.mu.Lock()
			commonLive := !current.common.ended && current.common.degreeWitness == w
			current.common.mu.Unlock()
			d.degree.mu.Lock()
			degreeLive := !w.closed && d.degree.active[w.slot].life == current.common
			d.degree.mu.Unlock()
			if !same || !upstream || !sameAlias || !commonLive || !degreeLive {
				t.Fatal("late A cleanup retargeted current B", kind)
			}
		})
	}
}

func TestRegistryOrdinaryPublisherStaysNil(t *testing.T) {
	s := &Service{conns: make(map[domain.ConnID]*connEntry), connIDByNetConn: make(map[net.Conn]domain.ConnID)}
	if s.beginRegistryPublication() != nil {
		t.Fatal("ordinary nil attachment allocated a publication")
	}
	left, right := net.Pipe()
	owner := newF1FixtureOwner()
	owner.rawServer, owner.rawClient = left, right
	t.Cleanup(func() { owner.stopAndJoin(t) })
	core := newTestNetCore(t, domain.ConnID(1), left, netcore.Inbound, netcore.Options{})
	owner.inboundCore = core
	s.peerMu.Lock()
	s.registerInboundConnLocked(left, core, nil, nil)
	s.peerMu.Unlock()
	core.Close()
	registryUnregister(s, left)
	if !registryMapsAbsent(s, left, 1) {
		t.Fatal("nil publisher changed mandatory ordinary behavior")
	}
}
