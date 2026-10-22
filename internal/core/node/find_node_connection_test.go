package node

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

type f1ProofFixture struct {
	accepted, dialer           *Service
	inbound                    *proofTransportOwner
	outbound                   *peerSession
	acceptedProof, dialedProof sessionv2.ProvenSession
	intro                      protocol.Frame
	raw, dialerRaw             *peekedConn
	fixtureOwner               *f1FixtureOwner
}

// f1FixtureOwner owns only this fixture's original transports and workers.
// Phase fields are assigned on the test goroutine before teardown can start.
// The one stop waiter owns ALL potentially blocking Close and join work.
type f1FixtureOwner struct {
	serviceJobs               [2]*Service
	rawServer, rawClient      net.Conn
	acceptDone                <-chan struct{}
	authDone                  <-chan struct{}
	caseDone, observerDone    <-chan struct{}
	diagnostic                *f1ArmedDiagnostic
	writeGate                 *n1HeldWrite
	inboundService            *Service
	inboundWire               *peekedConn
	inboundID                 domain.ConnID
	inboundEntry              *connEntry
	inboundCore, outboundCore *netcore.NetCore
	inboundLife               *proofTransportLifetime
	inbound                   *proofTransportOwner
	outbound                  *peerSession
	serverDone, readerDone    <-chan struct{}
	stopOnce                  sync.Once
	stopDeadline              time.Time
	stopped                   chan struct{}
}

func newF1FixtureOwner() *f1FixtureOwner { return &f1FixtureOwner{stopped: make(chan struct{})} }

func (o *f1FixtureOwner) stopAndJoin(t *testing.T) bool {
	t.Helper()
	o.stopOnce.Do(func() {
		o.stopDeadline = time.Now().Add(2 * time.Second)
		go func() {
			defer close(o.stopped)
			if o.diagnostic != nil {
				o.diagnostic.unblock() // even Fatal before the logger cleanup runs
			}
			for _, service := range o.serviceJobs {
				if service != nil {
					service.runCancel()
					service.closeBackgroundAdmission()
				}
			}
			if o.writeGate != nil {
				o.writeGate.unblock()
			}
			for _, raw := range []net.Conn{o.rawServer, o.rawClient} {
				if raw != nil {
					_ = raw.Close()
				}
			}
			if o.inboundLife != nil {
				o.inboundLife.end()
			}
			if o.inbound != nil {
				o.inbound.end()
			}
			if o.outbound != nil {
				_ = o.outbound.Close()
			}
			// Close returns only after the EXACT original writerExited. Never
			// substitute a current registry/core lookup after controlled A→B.
			if o.inboundCore != nil {
				o.inboundCore.Close()
			}
			if o.outboundCore != nil {
				o.outboundCore.Close()
			}
			for _, done := range []<-chan struct{}{o.acceptDone, o.serverDone, o.readerDone, o.authDone, o.caseDone, o.observerDone} {
				if done != nil {
					<-done
				}
			}
			// Only the explicitly owned CM/diagnostic Services enter this
			// set. Ordinary f1PairOn does NOT join arbitrary Service/N-in loops.
			for _, service := range o.serviceJobs {
				if service != nil {
					service.runLoopsWg.Wait()
					service.backgroundWg.Wait()
				}
			}
			if o.inboundService != nil && o.inboundEntry != nil {
				o.inboundService.peerMu.Lock()
				if o.inboundService.conns[o.inboundID] == o.inboundEntry {
					o.inboundService.unregisterConnLocked(o.inboundWire)
				}
				o.inboundService.peerMu.Unlock()
			}
		}()
	})
	select {
	case <-o.stopped:
		return true
	default:
	}
	timer := time.NewTimer(max(time.Until(o.stopDeadline), 0))
	defer timer.Stop()
	select {
	case <-o.stopped:
		return true
	case <-timer.C:
		t.Error("original fixture stop/close/join unconfirmed within2s")
		return false
	}
}

func f1Service(t *testing.T) *Service { return f1ServiceOwned(t, nil) }
func f1ServiceOwned(t *testing.T, owner *f1FixtureOwner) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s, err := newFindNodeCandidateService(cfg, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	// These transport fixtures exclude route-grace timing. Keep withdrawal
	// synchronous in its owned caller; a raw AfterFunc is not joined by WGs.
	s.routeWithdrawalGracePeriodTest = -1
	if owner == nil {
		t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	} else {
		// Explicit diagnostic fixtures delegate every Service cleanup to the
		// same bounded owner, including a Fatal before pair/CM construction.
		t.Cleanup(func() { owner.stopAndJoin(t) })
	}
	return s
}

// Explicit-intro genuine TLS fixture. Caps are on the actual handshake, not
// injected by a production factory. Normal Service hello still has no F1 cap.
// Registry/auth/mandatory pin and outbound post-setup publication remain real.
// Prepared diagnostics own the SAME genuine raw pair before any TLS/auth/core
// worker. Endpoint strings are only diagnostic filters, never proof authority.
type f1PreparedDiagnosticPair struct {
	accepted, dialer *Service
	owner            *f1FixtureOwner
	server, client   net.Conn
}

func f1PrepareDiagnosticPair(t *testing.T) f1PreparedDiagnosticPair {
	t.Helper()
	owner := newF1FixtureOwner()
	accepted := f1ServiceOwned(t, owner)
	owner.serviceJobs[0] = accepted
	dialer := f1ServiceOwned(t, owner)
	owner.serviceJobs[1] = dialer
	t.Cleanup(func() { owner.stopAndJoin(t) })
	server, client := tcpPair(t)
	owner.rawServer, owner.rawClient = server, client
	return f1PreparedDiagnosticPair{accepted, dialer, owner, server, client}
}
func (p f1PreparedDiagnosticPair) address() string       { return p.server.LocalAddr().String() }
func (p f1PreparedDiagnosticPair) remoteAddress() string { return p.client.RemoteAddr().String() }
func (p f1PreparedDiagnosticPair) finish(t *testing.T, writeGate *n1HeldWrite) f1ProofFixture {
	t.Helper()
	f := f1PairOnRaw(t, true, writeGate, p.accepted, p.dialer, p.owner, p.server, p.client)
	if string(f.outbound.address) != p.address() || f.outbound.netCore.RemoteAddr() != p.remoteAddress() || f.outbound.peerIdentity != domain.PeerIdentityFromWire(p.accepted.identity.Address) {
		t.Fatal("prepared raw-pair diagnostic endpoint drift")
	}
	return f
}
func f1DiagnosticPair(t *testing.T, writeGate *n1HeldWrite) f1ProofFixture {
	t.Helper()
	return f1PrepareDiagnosticPair(t).finish(t, writeGate)
}

func f1Pair(t *testing.T, withCaps bool) f1ProofFixture { return f1PairWithWrite(t, withCaps, nil) }
func f1PairWithWrite(t *testing.T, withCaps bool, writeGate *n1HeldWrite) f1ProofFixture {
	return f1PairOn(t, withCaps, writeGate, f1Service(t), f1Service(t))
}
func f1PairOn(t *testing.T, withCaps bool, writeGate *n1HeldWrite, s, d *Service) f1ProofFixture {
	t.Helper()
	return f1PairOnOwned(t, withCaps, writeGate, s, d, newF1FixtureOwner())
}
func f1PairOnOwned(t *testing.T, withCaps bool, writeGate *n1HeldWrite, s, d *Service, fixtureOwner *f1FixtureOwner) f1ProofFixture {
	t.Helper()
	t.Cleanup(func() { fixtureOwner.stopAndJoin(t) })
	server, client := tcpPair(t)
	fixtureOwner.rawServer, fixtureOwner.rawClient = server, client
	return f1PairOnRaw(t, withCaps, writeGate, s, d, fixtureOwner, server, client)
}
func f1PairOnRaw(t *testing.T, withCaps bool, writeGate *n1HeldWrite, s, d *Service, fixtureOwner *f1FixtureOwner, server, client net.Conn, extraCaps ...domain.Capability) f1ProofFixture {
	t.Helper()
	if server == nil || client == nil || fixtureOwner.rawServer != server || fixtureOwner.rawClient != client {
		t.Fatal("fixture must finish on its exact original raw pair")
	}
	t.Cleanup(func() { fixtureOwner.stopAndJoin(t) }) // before any pair worker
	fixtureOwner.writeGate = writeGate
	sm, err := netcore.NewMeteredConn(server, &s.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	clientTap := &n1ReadTap{Conn: client, successful: new(atomic.Int64), failed: new(atomic.Int64), writeGate: writeGate}
	if writeGate != nil {
		t.Cleanup(writeGate.unblock)
	}
	cm, err := netcore.NewMeteredConn(clientTap, &d.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	rawS := &peekedConn{Conn: sm, reader: sm}
	rawD := &peekedConn{Conn: cm, reader: cm}
	welcome, hello := s.welcomeFrame("", ""), d.nodeHelloFrame()
	// Test-only opt-in capabilities are part of the ORIGINAL authenticated
	// handshake, never a post-proof mutation of a negotiated session.
	for _, capability := range extraCaps {
		welcome.Capabilities = append(welcome.Capabilities, string(capability))
		hello.Capabilities = append(hello.Capabilities, string(capability))
	}
	if withCaps {
		welcome.Capabilities = append(welcome.Capabilities, string(domain.CapOverlayFindNodeV1))
		hello.Capabilities = append(hello.Capabilities, string(domain.CapOverlayFindNodeV1))
	}
	type accepted struct {
		proof sessionv2.ProvenSession
		err   error
	}
	result := make(chan accepted, 1)
	joined := make(chan struct{})
	fixtureOwner.acceptDone = joined
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		defer close(joined)
		p, e := sessionv2.Accept(ctx, rawS, s.secureSessions.local, welcome, s.secureSessions.certificates, sessionv2.DefaultTimeouts)
		result <- accepted{p, e}
	}()
	dp, err := sessionv2.Dial(ctx, rawD, d.secureSessions.local, hello, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	ap := <-result
	if ap.err != nil {
		t.Fatal(ap.err)
	}
	peer, err := ap.proof.Peer()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := s.secureSessions.store.noteProvenInboundBinding(peer.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if proof, ok := peer.Proof(); ok {
		s.noteLocalContactProof(proof, binding)
	}
	protected, err := ap.proof.Conn()
	if err != nil {
		t.Fatal(err)
	}
	l := &proofTransportLifetime{token: s.proofToken, bridgeTerminal: s.bridgeTerminal, role: sessionv2.RoleListener}
	fixtureOwner.inboundLife = l
	wire := &peekedConn{Conn: protected, reader: protected}
	l.evidence = &proofTransportEvidence{session: ap.proof, protected: protected, raw: rawS, wire: wire, group: domain.SourceGroupLoopback, identity: peer.Identity}
	if s.inboundPins != nil {
		l.nin = &matureInboundWitness{transport: l, producer: s.inboundPins, binding: binding, session: ap.proof, group: l.evidence.group}
	}
	s.captureFindNodeSeed(l, ap.proof, welcome)
	if !wire.bindProofTransport(l) || !rawS.bindProofTransport(l) {
		t.Fatal("original tuple bind refused")
	}
	s.publishDegreeTransport(l) // genuine sole mint; legacy degree-nil fixtures remain inert
	if err := s.registerInboundConn(wire, nil); err != nil {
		t.Fatal(err)
	}
	// Capture the actual newly created core NOW, before any fallible auth.
	// Its cleanup closes that pointer, not whatever a registry key names later.
	s.peerMu.RLock()
	registeredID, _ := s.connIDForLocked(wire)
	entry := s.conns[registeredID]
	fixtureOwner.inboundService, fixtureOwner.inboundWire = s, wire
	fixtureOwner.inboundID, fixtureOwner.inboundEntry = registeredID, entry
	if entry != nil {
		fixtureOwner.inboundCore = entry.core
	}
	s.peerMu.RUnlock()
	t.Cleanup(func() { fixtureOwner.stopAndJoin(t) })
	if fixtureOwner.inboundCore == nil {
		t.Fatal("newly registered original inbound core missing")
	}
	id, owner := s.inboundTerminalForWire(wire)
	fixtureOwner.inbound = owner
	if owner == nil {
		t.Fatal("actual accepted registry lost proof owner")
	}
	if !s.acceptProvenInboundOwned(id, peer, wire.RemoteAddr().String(), owner) {
		t.Fatal("actual accepted auth publication refused")
	}
	// The mandatory ordinary pin publication above is NOT strict N-in ACK.
	if binding.owner == nil {
		t.Fatal("mandatory pin absent")
	}
	address := domain.PeerAddress(server.LocalAddr().String())
	tr := &dialledTransport{connID: d.allocateConnID("f1Pair", address), raw: client, metered: cm, proofRaw: rawD}
	tr, err = d.provenTransport(ctx, tr, dp, address, domain.DialToAddress(), hello)
	if err != nil {
		t.Fatal(err)
	}
	expectsOutboundOwner := d.findNode != nil || d.hasContactRecordPushTransportCapability()
	p := &peerSession{common: tr.common, connID: tr.connID, conn: tr.wire, metered: cm, address: address, proven: tr.proven, sendCh: make(chan peerSendItem, 1), inboxCh: make(chan protocol.Frame, 1), errCh: make(chan error, 1)}
	fixtureOwner.outbound = p // original peer is owned before any fallible consistency/auth assertion
	if (p.common != nil) != expectsOutboundOwner {
		t.Fatal("fixture outbound common does not match immutable constructor capability")
	}
	if p.common != nil {
		p.common.mu.Lock()
		p.common.peer = p
		p.common.mu.Unlock()
	}
	if err := d.attachOutboundNetCore(p); err != nil {
		t.Fatal(err)
	}
	fixtureOwner.outboundCore = p.netCore
	t.Cleanup(func() { fixtureOwner.stopAndJoin(t) }) // BEFORE fallible auth
	advertise := d.localDatagramAdvertise()
	d.peerMu.Lock()
	applyWelcomeMetadata(p, tr.proven.Intro, d.cfg.EnableMeshRoutingV3, advertise)
	d.peerMu.Unlock()
	if err := d.completeOutboundAuth(p); err != nil {
		t.Fatal("actual outbound auth completion", err)
	}
	// Match real dial setup: retain this original handshake's pin binding
	// before any reader/CM publication; never recover it by identity later.
	p.selectedProof = d.captureSelectedProof(tr)
	// Neither decoded reader nor background task needs to run for these pure
	// post-proof owners. Actual TLS transport remains in both wrappers.
	return f1ProofFixture{s, d, owner, p, ap.proof, dp, hello, rawS, rawD, fixtureOwner}
}
func f1AssertUnpublished(t *testing.T, p *peerSession) {
	t.Helper()
	p.common.mu.Lock()
	defer p.common.mu.Unlock()
	if p.common.findNode != nil && p.common.findNode.published {
		t.Fatal("pre-setup transport published Session")
	}
}
func TestFindNodeOriginalProofCanonicalAcceptedAndCM(t *testing.T) {
	f := f1Pair(t, true)
	if f.accepted.inboundPins != nil || f.dialer.inboundPins != nil {
		t.Fatal("F1 activated N-in")
	}
	// Both are genuine TLS roles, with the real Dial firstByteConn on the
	// outbound path. Refuse a missing original attach before any CM assertion.
	for _, original := range []struct {
		name string
		svc  *Service
		id   domain.ConnID
		life *proofTransportLifetime
	}{{"listener", f.accepted, f.inbound.id, f.inbound.life}, {"dialer", f.dialer, f.outbound.connID, f.outbound.common}} {
		original.svc.peerMu.RLock()
		entry := original.svc.conns[original.id]
		original.life.mu.Lock()
		bound := entry != nil && entry.common == original.life && original.life.entry == entry && original.life.id == original.id
		original.life.mu.Unlock()
		original.svc.peerMu.RUnlock()
		if !bound {
			t.Fatal("genuine original tuple did not attach", original.name)
		}
	}
	f.accepted.publishAcceptedProofTransport(f.inbound)
	first := f1Members(t, f.accepted)
	if len(first.sessions) != 1 || first.coverage != registeredAcceptedAndCMPostSetup {
		t.Fatal("accepted reader missing")
	}
	f.accepted.publishAcceptedProofTransport(f.inbound)
	again := f1Members(t, f.accepted)
	if len(again.sessions) != 1 || again.sessions[0] != first.sessions[0] {
		t.Fatal("canonical Session reminted")
	}
	f1AssertUnpublished(t, f.outbound)
	if f.dialer.publishFindNodeCM(f.outbound, f.outbound.address, 9) {
		t.Fatal("fabricated mirrored generation published")
	}
	// Exercise the actual CM generation acceptance, application setup, and
	// publishAndServe boundary. The real protected peer answers ordinary commands.
	f1StartActualCM(t, f, 9)
	waitFor(t, 2*time.Second, "CM post-setup F1 publication", func() bool { return len(f1Members(t, f.dialer).sessions) == 1 })
	f.dialer.peerMu.RLock()
	generation := f.outbound.selectedGeneration
	receipt := f.outbound.findNodeReceipt
	f.dialer.peerMu.RUnlock()
	if receipt == nil || generation == 9 || receipt.generation != generation {
		t.Fatal("CM did not mint accepted new generation")
	}
	if !f.dialer.publishFindNodeCM(f.outbound, f.outbound.address, generation) {
		t.Fatal("repeated canonical publication refused")
	}
	if f.dialer.publishFindNodeCM(f.outbound, f.outbound.address, 9) {
		t.Fatal("stale dial generation re-published")
	}
	f.outbound.endProofTransport()
	if len(f1Members(t, f.dialer).sessions) != 0 {
		t.Fatal("terminal CM reader retained membership")
	}
}
func TestFindNodeActualDefaultCapsWithholdSeed(t *testing.T) {
	f := f1Pair(t, false)
	f.accepted.publishAcceptedProofTransport(f.inbound)
	if len(f1Members(t, f.accepted).sessions) != 0 || f.inbound.life.findNode != nil || f.outbound.common.findNode != nil {
		t.Fatal("default capabilities silently activated FindNode")
	}
	if f.accepted.inboundPins != nil {
		t.Fatal("factory installed journal/producer")
	}
}

func f1StartActualCM(t *testing.T, f f1ProofFixture, generation uint64) *f1FixtureOwner {
	t.Helper()
	start := f1PrepareActualCM(t, f, generation)
	start()
	return f.fixtureOwner
}

// All fixture ownership and cleanup registration precede asynchronous dispatch.
// The returned thunk starts exactly the original CM event, never a fake receipt.
func f1PrepareActualCM(t *testing.T, f f1ProofFixture, generation uint64) func() {
	t.Helper()
	p := f.outbound
	owner := f.fixtureOwner
	serverDone, readerDone := make(chan struct{}), make(chan struct{})
	owner.serverDone, owner.readerDone = serverDone, readerDone
	owner.serviceJobs = [2]*Service{f.accepted, f.dialer}
	// SAME idempotent owner, registered before either reader or actual CM start.
	t.Cleanup(func() { owner.stopAndJoin(t) })
	f.accepted.peerMu.RLock()
	core := f.accepted.coreForIDLocked(f.inbound.id)
	f.accepted.peerMu.RUnlock()
	go func() {
		defer close(serverDone)
		reader := bufio.NewReader(core.Conn())
		for {
			line, err := f.accepted.readInboundCommandLineOwned(reader, f.inbound.id, f.inbound)
			if err != nil {
				return
			}
			if !f.accepted.handleCommandOwned(f.inbound.id, line, f.inbound) {
				return
			}
		}
	}()
	go func() { defer close(readerDone); f.dialer.readPeerSession(bufio.NewReader(p.conn), p) }()
	cm := f.dialer.connManager
	cm.mu.Lock()
	cm.generation = generation
	cm.slots = []*slot{{Address: p.address, State: domain.SlotStateDialing, Generation: generation}}
	cm.mu.Unlock()
	return func() {
		cm.handleDialSucceeded(f.dialer.runCtx, DialSucceeded{Address: p.address, ConnectedAddress: p.address, SlotGeneration: generation, Session: p})
	}
}

func f1Members(t *testing.T, s *Service) findNodeMemberSnapshot {
	t.Helper()
	out, err := s.findNodeMembers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The nil-core registry state is a controlled traversal oracle, not a new
// proof producer or a supported ordinary transport state. Its canonical
// Session was already published by the genuine accepted-proof fixture.
func TestFindNodeMembersRetainRegisteredNilCoreSession(t *testing.T) {
	f := f1Pair(t, true)
	f.accepted.publishAcceptedProofTransport(f.inbound)
	before := f1Members(t, f.accepted)
	if len(before.sessions) != 1 {
		t.Fatal("genuine accepted reader missing before nil-core traversal")
	}

	f.accepted.peerMu.Lock()
	entry := f.accepted.connEntryByIDLocked(f.inbound.id)
	if entry == nil || entry.common != f.inbound.life || entry.core == nil {
		f.accepted.peerMu.Unlock()
		t.Fatal("original registered entry missing before nil-core traversal")
	}
	originalCore := entry.core
	// Restore before the fixture's earlier original-core/registry cleanup.
	// Only the same captured entry may be restored; never mutate replacement B.
	t.Cleanup(func() {
		f.accepted.peerMu.Lock()
		if f.accepted.connEntryByIDLocked(f.inbound.id) == entry {
			entry.core = originalCore
		}
		f.accepted.peerMu.Unlock()
	})
	entry.core = nil
	f.accepted.peerMu.Unlock()

	snapshot := f1Members(t, f.accepted)
	if snapshot.coverage != registeredAcceptedAndCMPostSetup || len(snapshot.sessions) != 1 || snapshot.sessions[0] != before.sessions[0] {
		t.Fatal("registered published nil-core reader lost canonical membership")
	}
	f.inbound.end()
	if len(f1Members(t, f.accepted).sessions) != 0 {
		t.Fatal("ended nil-core reader retained membership")
	}
}

func TestFindNodeCancelledSnapshotHasNoPartialCoverage(t *testing.T) {
	f := f1Pair(t, true)
	f.accepted.publishAcceptedProofTransport(f.inbound)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	snapshot, err := f.accepted.findNodeMembers(ctx)
	if err != context.Canceled || snapshot.coverage != 0 || len(snapshot.sessions) != 0 {
		t.Fatal("cancelled observation claimed complete registered coverage", snapshot, err)
	}
}

func TestFindNodeGeneralTupleCannotBindAnotherRawOrService(t *testing.T) {
	for _, kind := range []string{"another-raw", "another-service", "another-proof"} {
		t.Run(kind, func(t *testing.T) {
			f := f1Pair(t, true)
			original := f.inbound.life
			original.mu.Lock()
			copyEvidence := *original.evidence
			original.mu.Unlock()
			cell := &proofTransportLifetime{token: f.accepted.proofToken, role: sessionv2.RoleListener, evidence: &copyEvidence}
			wire := &peekedConn{Conn: copyEvidence.protected, reader: copyEvidence.protected}
			copyEvidence.wire = wire
			switch kind {
			case "another-raw":
				copyEvidence.raw = &peekedConn{Conn: copyEvidence.raw.Conn, reader: copyEvidence.raw.Conn}
				copyEvidence.raw.bindProofTransport(cell)
			case "another-service":
				cell.token = f.dialer.proofToken
			case "another-proof":
				copyEvidence.session = f.dialedProof
			}
			wire.bindProofTransport(cell)
			if err := f.accepted.registerInboundConn(wire, nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.accepted.unregisterInboundConn(wire) })
			_, owner := f.accepted.inboundTerminalForWire(wire)
			if owner != nil || cell.entry != nil {
				t.Fatal("general tuple rebound unrelated proof/raw/service", kind)
			}
			if len(f1Members(t, f.accepted).sessions) != 0 {
				t.Fatal("mismatched tuple published Session")
			}
		})
	}
	for _, direction := range []string{"listener-mismatched-raw", "dialer-mismatched-raw"} {
		t.Run(direction, func(t *testing.T) {
			f := f1Pair(t, true)
			svc, proof, raw, id := f.accepted, f.acceptedProof, f.raw, f.inbound.id
			if direction == "dialer-mismatched-raw" {
				svc, proof, raw, id = f.dialer, f.dialedProof, f.dialerRaw, f.outbound.connID
			}
			protected, err := proof.Conn()
			if err != nil {
				t.Fatal(err)
			}
			peer, err := proof.Peer()
			if err != nil {
				t.Fatal(err)
			}
			role, err := proof.Role()
			if err != nil {
				t.Fatal(err)
			}
			svc.peerMu.RLock()
			actual := svc.conns[id]
			svc.peerMu.RUnlock()
			if actual == nil || actual.common == nil || actual.metered == nil || raw.Conn != actual.metered {
				t.Fatal("genuine direction fixture missing original registry/metered tuple", direction)
			}
			// Fresh TEST R' shares only the metered socket handle. Genuine P/T
			// still names original R; no original cell is reset or overwritten.
			foreignRaw := &peekedConn{Conn: raw.Conn, reader: raw.reader}
			wire := &peekedConn{Conn: protected, reader: protected}
			cell := &proofTransportLifetime{token: svc.proofToken, role: role, outbound: role == sessionv2.RoleDialer}
			cell.evidence = &proofTransportEvidence{session: proof, protected: protected, raw: foreignRaw, wire: wire, identity: peer.Identity}
			t.Cleanup(cell.end)
			if !foreignRaw.bindProofTransport(cell) || !wire.bindProofTransport(cell) {
				t.Fatal("fresh mismatched-raw fixture refused its own memory cell")
			}
			// Borrow the already owned real core; create no second writer or
			// registry entry. Every gate except P/T's exact original R matches.
			entry := &connEntry{core: actual.core, metered: actual.metered}
			svc.peerMu.Lock()
			svc.bindProofTransportLocked(entry, id, wire)
			svc.peerMu.Unlock()
			if entry.common != nil || cell.entry != nil {
				t.Fatal("genuine proof accepted a different original raw", direction)
			}
			if raw.common.Load() != actual.common {
				t.Fatal("mismatched-raw probe overwrote the genuine original", direction)
			}
		})
	}
}

func TestFindNodeNinCompanionKeepsListenerPolicyIndependent(t *testing.T) {
	base := n1NewServiceFixture(t)
	s, err := newFindNodeInboundCandidateService(base.service.cfg, base.service.identity, nil, inboundPinClock{elapsed: base.clock.read})
	if err != nil {
		t.Fatal(err)
	}
	// This inline accepted listener bypasses f1ServiceOwned; same pre-worker
	// fixture-only exclusion of route-grace callbacks, not a production default.
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal("loaded journal strict obligation", err)
	}
	f := n1AcceptedOnService(t, n1ServiceFixture{s, base.dialer, base.clock})
	s.publishAcceptedProofTransport(f.owner)
	if f.owner.life.nin == nil || f.owner.life.nin.transport != f.owner.life {
		t.Fatal("N-in acquired a second lifetime gate")
	}
	if len(f1Members(t, s).sessions) != 0 {
		t.Fatal("ordinary hello capability changed in combined factory")
	}
	base.clock.advance(10 * time.Minute)
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal("shared gate lost original N-in mature proof", err)
	}
	s.inboundPins.store.mu.Lock()
	earned := s.inboundPins.store.inboundAllocations
	s.inboundPins.store.mu.Unlock()
	if earned != 1 {
		t.Fatal("original mature listener did not earn exactly one N-in", earned)
	}
	// A real dialer F1 cell independently never receives an N-in companion.
	outgoing := f1Pair(t, true)
	if outgoing.outbound.common.nin != nil {
		t.Fatal("dialer transport gained listener class authority")
	}
}

func TestFindNodeOrdinaryConstructorRemainsInactive(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0"})
	s := NewService(cfg, id, nil)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if s.findNode != nil || s.inboundPins != nil {
		t.Fatal("ordinary constructor activated optional transport policy")
	}
	for _, intro := range []protocol.Frame{s.nodeHelloFrame(), s.welcomeFrame("", "")} {
		for _, cap := range intro.Capabilities {
			if cap == string(domain.CapOverlayFindNodeV1) || cap == string(domain.CapOverlayResponderV1) {
				t.Fatal("ordinary hello advertised inactive overlay", cap)
			}
		}
	}
}

func TestFindNodeClockIndependentOfNinClockFault(t *testing.T) {
	base := n1NewServiceFixture(t)
	s, err := newFindNodeInboundCandidateService(base.service.cfg, base.service.identity, nil, inboundPinClock{elapsed: base.clock.read})
	if err != nil {
		t.Fatal(err)
	}
	// This inline accepted listener bypasses f1ServiceOwned; same pre-worker
	// fixture-only exclusion of route-grace callbacks, not a production default.
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	f := f1PairOn(t, true, nil, s, f1Service(t))
	base.clock.failed.Store(true)
	s.publishAcceptedProofTransport(f.inbound)
	if len(f1Members(t, s).sessions) != 1 || f1Ended(f.inbound.life) {
		t.Fatal("N-in clock fault suppressed independent F1 publication")
	}
	f.inbound.life.mu.Lock()
	phase := f.inbound.life.nin.phase
	f.inbound.life.mu.Unlock()
	if phase != inboundProvisional {
		t.Fatal("clock fault minted N-in maturity")
	}
	s.inboundPins.store.mu.Lock()
	count := s.inboundPins.store.inboundAllocations
	s.inboundPins.store.mu.Unlock()
	if count != 0 {
		t.Fatal("clock failure earned N-in")
	}
}
