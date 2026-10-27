package node

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// These capacities and durations are functional fixtures, not deployment,
// retention, clock-attribution or resource-profile choices. No source-shaped
// reducer computes a percentage: assertions inspect actual owner facts only.
func advertisedActualRequest(clock func() (time.Duration, error), connections, slots, history int) k3AdvertisedRequest {
	return k3AdvertisedRequest{ConnectionCapacity: connections, SlotCapacity: slots, HistoryCapacity: history, Instance: [16]byte{19}, elapsed: clock}
}
func advertisedActualService(t *testing.T, request *k3AdvertisedRequest) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{k3Advertised: request})
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}

// The older genuine TLS helper predates this source and calls the global
// welcome writer directly. Feed its ORIGINAL captured welcome through the
// selected production owner method; no proof or raw claim is fabricated.
func advertisedActualPair(t *testing.T, accepted, dialer *Service) f1ProofFixture {
	t.Helper()
	f := k3ActualPair(t, accepted, dialer)
	if dialer.k3Advertised != nil {
		peer, err := f.dialedProof.Peer()
		if err != nil {
			t.Fatal(err)
		}
		local := dialer.localHandshakeCapabilities()
		dialer.peerMu.Lock()
		dialer.applyK3AdvertisedWelcomeLocked(f.outbound, peer.Intro, local)
		dialer.peerMu.Unlock()
	}
	return f
}
func advertisedActualSnapshot(t *testing.T, s *Service) k3AdvertisedSnapshot {
	t.Helper()
	got := s.k3AdvertisedSnapshot()
	if got.Population != "AdvertisedConnections" || got.Qualification != "UNKNOWN" || got.AttributionBound || !got.PastOnly || got.CoveredSequence > got.CompletedSequence || got.CoveredBatches > got.CompletedBatches || len(got.Facts) != int(got.CoveredSequence) {
		t.Fatal("source labels or covered/completed boundary", got)
	}
	for i, row := range got.Facts {
		if row.Sequence != uint64(i+1) || row.Index < 1 || row.Count < 1 || row.Index > row.Count || row.Clock.Before < 0 || row.Clock.After < row.Clock.Before {
			t.Fatal("malformed retained actual owner fact", row)
		}
		if row.Owner != "peer" && row.Owner != "cm" {
			t.Fatal("unknown actual owner domain", row)
		}
		if row.Index == 1 {
			if i > 0 && got.Facts[i-1].Index != got.Facts[i-1].Count {
				t.Fatal("prior owner batch truncated", got)
			}
		} else {
			if i == 0 {
				t.Fatal("prefix starts in partial owner batch", got)
			}
			previous := got.Facts[i-1]
			if row.Batch != previous.Batch || row.Index != previous.Index+1 || row.Count != previous.Count || row.Owner != previous.Owner || row.OwnerSequence != previous.OwnerSequence || row.Clock != previous.Clock {
				t.Fatal("owner batch interleaved or split", got)
			}
		}
		if i == len(got.Facts)-1 && row.Index != row.Count {
			t.Fatal("covered prefix ends in partial transaction", got)
		}
	}
	return got
}
func advertisedSameCovered(t *testing.T, a, b k3AdvertisedSnapshot) {
	t.Helper()
	if a.Baseline != b.Baseline || a.Cut != b.Cut || a.CoveredSequence != b.CoveredSequence || a.CoveredBatches != b.CoveredBatches || a.PeerOwnersAtCut != b.PeerOwnersAtCut || a.SlotsAtCut != b.SlotsAtCut {
		t.Fatal("last cut-confirmed prefix moved", a, b)
	}
	n := int(a.CoveredSequence)
	if len(a.Facts) < n || len(b.Facts) < n || !reflect.DeepEqual(a.Facts[:n], b.Facts[:n]) {
		t.Fatal("last cut-confirmed values changed", a, b)
	}
}
func advertisedFactFor(t *testing.T, got k3AdvertisedSnapshot, id domain.ConnID, direction string) k3AdvertisedPeerFact {
	t.Helper()
	for i := len(got.Facts) - 1; i >= 0; i-- {
		r := got.Facts[i]
		if r.Peer.ConnID == id && r.Peer.Direction == direction {
			if r.Removed {
				t.Fatal("requested owner was removed", r)
			}
			return r.Peer
		}
	}
	t.Fatal("actual owner fact absent", id, direction, got)
	return k3AdvertisedPeerFact{}
}
func advertisedSlotFor(t *testing.T, got k3AdvertisedSnapshot, address domain.PeerAddress) k3AdvertisedSlotFact {
	t.Helper()
	for i := len(got.Facts) - 1; i >= 0; i-- {
		row := got.Facts[i]
		if row.Owner == "cm" && row.Slot.Address == address {
			if row.Removed {
				t.Fatal("requested slot was removed", row)
			}
			return row.Slot
		}
	}
	t.Fatal("actual slot fact absent", address, got)
	return k3AdvertisedSlotFact{}
}

// An original registry insertion and original hello metadata method, with a
// no-I/O Core fixture. This is raw legacy-shaped input, not a successful TLS,
// sessionv1 auth, App.Run or external network workload claim.
func advertisedActualInbound(t *testing.T, s *Service, hello protocol.Frame) (net.Conn, *netcore.NetCore, domain.ConnID) {
	t.Helper()
	wire := newNoIOConn(t, &net.TCPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 5001})
	id := s.allocateConnID("advertised_actual_fixture", "10.0.0.9:64646")
	core := newTestNetCore(t, id, wire, netcore.Inbound, netcore.Options{})
	s.peerMu.Lock()
	s.registerInboundConnLocked(wire, core, nil, nil)
	s.peerMu.Unlock()
	t.Cleanup(func() { core.Close(); registryUnregister(s, wire) })
	s.rememberConnPeerAddr(id, hello, "10.0.0.9:5001")
	return wire, core, id
}

type advertisedClockStep struct {
	value time.Duration
	err   error
	hook  func()
}
type advertisedProgramClock struct {
	mu    sync.Mutex
	steps []advertisedClockStep
	index int
	next  time.Duration
}

func (c *advertisedProgramClock) read() (time.Duration, error) {
	c.mu.Lock()
	i := c.index
	c.index++
	var step advertisedClockStep
	if i < len(c.steps) {
		step = c.steps[i]
	} else {
		c.next += time.Second
		step.value = c.next
	}
	c.mu.Unlock()
	if step.hook != nil {
		step.hook()
	}
	return step.value, step.err
}
func (c *advertisedProgramClock) set(steps ...advertisedClockStep) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.steps = steps
	c.index = 0
	c.next = 100 * time.Second
	for _, step := range steps {
		if step.value >= c.next {
			c.next = step.value + 100*time.Second
		}
	}
}
func (c *advertisedProgramClock) calls() int { c.mu.Lock(); defer c.mu.Unlock(); return c.index }
func advertisedOwnedTasks(t *testing.T) (func(func()) <-chan struct{}, func(<-chan struct{}), func(chan struct{})) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var releases []chan struct{}
	release := func(ch chan struct{}) {
		mu.Lock()
		defer mu.Unlock()
		for _, old := range releases {
			if old == ch {
				return
			}
		}
		close(ch)
		releases = append(releases, ch)
	}
	run := func(fn func()) <-chan struct{} {
		done := make(chan struct{})
		wg.Add(1)
		go func() { defer wg.Done(); defer close(done); fn() }()
		return done
	}
	wait := func(ch <-chan struct{}) { k3Wait(t, ch, deadline) }
	t.Cleanup(func() { done := make(chan struct{}); go func() { wg.Wait(); close(done) }(); k3Wait(t, done, deadline) })
	return run, wait, release
}

func TestK3AdvertisedActualOwners(t *testing.T) {
	t.Run("constructor-same-Service-CM-empty-baseline-copy-and-default-dark", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		original := request
		clock.setHook(func() {
			request.ConnectionCapacity = 1
			request.SlotCapacity = 1
			request.HistoryCapacity = 1
			request.Instance = [16]byte{99}
		})
		s := advertisedActualService(t, &request)
		clock.setHook(nil)
		got := advertisedActualSnapshot(t, s)
		if s.k3Advertised == nil || s.connManager == nil || s.connManager.config.advertised != s.k3Advertised || got.Instance != original.Instance || got.PeerOwnersAtCut != 0 || got.SlotsAtCut != 0 || got.CompletedSequence != 0 || got.CoveredSequence != 0 || got.Pending != 0 {
			t.Fatal("source not installed once before both original owners", got)
		}
		ordinary := advertisedActualService(t, nil)
		off := advertisedActualSnapshot(t, ordinary)
		if ordinary.k3Advertised != nil || ordinary.connManager.config.advertised != nil || ordinary.k3AdvertisedClaims != nil || ordinary.k3AdvertisedGenerations != nil || off.Status != "OFF" || off.Facts != nil {
			t.Fatal("nil observation performed source work", off)
		}
		// The request was copied before the first clock, not merely after return.
		if s.k3Advertised.connectionCapacity != original.ConnectionCapacity || s.k3Advertised.slotCapacity != original.SlotCapacity {
			t.Fatal("caller mutation changed installed bounds")
		}
		cfg, nodeIdentity := pendingConstructionInputs(t)
		restoredAddress := domain.PeerAddress("10.0.0.40:64646")
		if err := savePeerState(cfg.PeersStatePath, peerStateFile{Version: peerStateVersion, Peers: []peerEntry{{Address: restoredAddress, Identity: idPeerB, Source: domain.PeerSourceBootstrap, Score: 5}}}); err != nil {
			t.Fatal(err)
		}
		restoredReq := advertisedActualRequest(newK3ActualClock().read, 32, 8, 256)
		restored := newServiceWithConstruction(cfg, nodeIdentity, nil, nil, productionDatagramConstructor{}, serviceConstruction{k3Advertised: &restoredReq})
		t.Cleanup(func() { restored.runCancel(); restored.closeBackgroundAdmission(); restored.WaitBackground() })
		restored.peerMu.RLock()
		health := restored.health[restoredAddress]
		cached := restored.peerIDs.identityOf(restoredAddress)
		validRestored := health != nil && !health.Connected && cached == idPeerB
		restored.peerMu.RUnlock()
		loaded := advertisedActualSnapshot(t, restored)
		if !validRestored || loaded.Status == "UNKNOWN" || loaded.PeerOwnersAtCut != 0 || loaded.SlotsAtCut != 0 || loaded.CompletedSequence != 0 {
			t.Fatal("persisted disconnected health/cache was denied or invented live claims", loaded)
		}
		wctx, wcancel := context.WithCancel(context.Background())
		d, err := findnodebridge.NewFixtureDomain(wctx.Done(), new(atomic.Int64))
		if err != nil {
			wcancel()
			t.Fatal(err)
		}
		terminal := newFindNodeTerminalConstruction(wctx, wcancel, d.Publisher(), degreeProfile{MaxActive: 8})
		terminal.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: terminal.terminal, done: wctx.Done()}}
		wcfg, widentity := pendingConstructionInputs(t)
		wclock := newK3ActualClock()
		wreq := advertisedActualRequest(wclock.read, 8, 8, 64)
		ws := newServiceWithConstruction(wcfg, widentity, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: terminal, k3Advertised: &wreq})
		ws.routeWithdrawalGracePeriodTest = -1
		t.Cleanup(func() { wcancel(); ws.closeBackgroundAdmission(); ws.WaitBackground() })
		registryFixtureSeal(t, d)
		wwire := newNoIOConn(t, &net.TCPAddr{IP: net.IPv4(10, 0, 0, 90), Port: 5004})
		wid := ws.allocateConnID("advertised_actual_W", "10.0.0.90:64646")
		wcore := newTestNetCore(t, wid, wwire, netcore.Inbound, netcore.Options{})
		t.Cleanup(func() { wcore.Close(); registryUnregister(ws, wwire) })
		probes := 0
		var probeErrors []error
		wclock.setHook(func() {
			stamp, err := d.Observer().BeginCapture()
			if err == nil {
				read, readErr := d.Consumer().TryReadClock(stamp)
				err = readErr
				if err == nil {
					read.Release()
				}
			}
			probes++
			if err != nil {
				probeErrors = append(probeErrors, err)
			}
		})
		ws.peerMu.Lock()
		ws.registerInboundConnLocked(wwire, wcore, nil, nil)
		ws.peerMu.Unlock()
		wclock.setHook(nil)
		if probes != 2 || len(probeErrors) != 0 {
			t.Fatal("actual source Before/After was inside original registry publication W", probes, probeErrors)
		}
	})
	t.Run("actual-raw-legacy-and-genuine-v2-bidirectional-conflicting-claims", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		peer := advertisedActualService(t, nil)
		_, _, legacyID := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 30})
		legacy := advertisedFactFor(t, advertisedActualSnapshot(t, s), legacyID, "inbound")
		if !legacy.ClaimSeen || legacy.Identity != idPeerB || legacy.Version != 30 {
			t.Fatal("actual raw legacy hello claim missing", legacy)
		}
		f := advertisedActualPair(t, s, peer)
		v2 := advertisedFactFor(t, advertisedActualSnapshot(t, s), f.inbound.id, "inbound")
		if !v2.ClaimSeen || v2.Identity.IsZero() || v2.Version < 31 {
			t.Fatal("genuine v2 original metadata missing", v2)
		}
		// Actual prepublication dialled Core is already a connection owner. This
		// unproven fixture attaches through the original builder; its pre-filled
		// session.peerIdentity is NOT a raw welcome and must not fill unknown facts.
		preAddress := domain.PeerAddress("10.0.0.42:64646")
		preSession, _ := unprovenOutboundSession(t, s, preAddress, v2.Identity)
		s.markPeerConnected(preAddress, domain.PeerDirectionOutbound)
		cm := s.connManager
		originalEstablished := cm.config.OnSessionEstablished
		cm.config.OnSessionEstablished = nil
		t.Cleanup(cm.shutdown)
		cm.mu.Lock()
		preSlot := cm.admitManualPeerLocked(ManualPeerRequested{Address: preAddress}, 8, nil).admitted
		cm.mu.Unlock()
		if preSlot == nil {
			t.Fatal("actual prepublication CM admission refused")
		}
		cm.handleDialSucceeded(context.Background(), DialSucceeded{Address: preAddress, SlotGeneration: preSlot.Generation, Session: preSession, ConnectedAddress: preAddress})
		preUnknown := advertisedActualSnapshot(t, s)
		missing := advertisedFactFor(t, preUnknown, preSession.connID, "outbound")
		if missing.ClaimSeen || !missing.Identity.IsZero() || missing.Version != 0 || !missing.Connected || !missing.ContextKnown || advertisedSlotFor(t, preUnknown, preAddress).State != domain.SlotStateInitializing {
			t.Fatal("eligible prepublication missing claim was omitted or invented", missing, preUnknown)
		}
		beforePublishOwners := preUnknown.PeerOwnersAtCut
		beforePublishGeneration := missing.Generation
		preLocal := s.localHandshakeCapabilities()
		s.peerMu.Lock()
		s.applyK3AdvertisedWelcomeLocked(preSession, protocol.Frame{Address: v2.Identity.String(), Version: 30}, preLocal)
		s.peerMu.Unlock()
		preKnown := advertisedActualSnapshot(t, s)
		lower := advertisedFactFor(t, preKnown, preSession.connID, "outbound")
		if !lower.ClaimSeen || lower.Identity != v2.Identity || lower.Version != 30 || !lower.Connected || !lower.ContextKnown || lower.Generation != beforePublishGeneration {
			t.Fatal("prepublication lower-version actual claim was hidden", lower)
		}
		// Original rememberConnPeerAddr also permits an outbound ConnID. Its
		// mirrored Address can become a raw/untrusted hello field; it must not
		// replace the actual attach-time dial key used by the health filter.
		s.rememberConnPeerAddr(preSession.connID, protocol.Frame{Address: v2.Identity.String(), Version: 30}, "")
		remirroredSnapshot := advertisedActualSnapshot(t, s)
		remirrored := advertisedFactFor(t, remirroredSnapshot, preSession.connID, "outbound")
		if preSession.netCore.Address() == preAddress || remirroredSnapshot.Status == "UNKNOWN" || remirrored.Address != preAddress || remirrored.HealthAddress != lower.HealthAddress || !remirrored.ContextKnown || !remirrored.Connected {
			t.Fatal("raw mirror erased actual outbound dial/health context", remirrored)
		}
		k3Publish(t, s, preSession)
		published := advertisedActualSnapshot(t, s)
		if published.PeerOwnersAtCut != beforePublishOwners || advertisedFactFor(t, published, preSession.connID, "outbound").Generation != beforePublishGeneration {
			t.Fatal("same Core/ConnID session alias double-counted or replaced its generation", published)
		}
		cm.shutdown()
		cm.config.OnSessionEstablished = originalEstablished
		// Proof and F1 were not prerequisites for the raw legacy claim. Both real
		// connection owners coexist; no identity/renderer preference can drop one.
		reverse := advertisedActualPair(t, peer, s)
		k3Publish(t, s, reverse.outbound)
		s.markPeerConnected(reverse.outbound.address, domain.PeerDirectionOutbound)
		both := advertisedActualSnapshot(t, s)
		out := advertisedFactFor(t, both, reverse.outbound.connID, "outbound")
		if out.Identity != v2.Identity || out.Version < 31 {
			t.Fatal("original outbound claim absent", out, v2)
		}
		s.rememberConnPeerAddr(f.inbound.id, protocol.Frame{Address: out.Identity.String(), Version: 30}, "127.0.0.1:64001")
		conflict := advertisedActualSnapshot(t, s)
		inbound := advertisedFactFor(t, conflict, f.inbound.id, "inbound")
		outbound := advertisedFactFor(t, conflict, reverse.outbound.connID, "outbound")
		if inbound.Identity != outbound.Identity || inbound.Version != 30 || outbound.Version < 31 || !inbound.Connected || !outbound.Connected || conflict.PeerOwnersAtCut < 3 {
			t.Fatal("same-identity disagreement was collapsed", inbound, outbound, conflict)
		}
		// Auth-only mirror is not another announcement. Neither a real proof nor
		// its Core identity mirror can manufacture/erase the raw hello version.
		actualAuth := k3Auth(t, s, f.inbound.id)
		s.completeInboundAuth(f.inbound.id, actualAuth)
		mirrored := advertisedFactFor(t, advertisedActualSnapshot(t, s), f.inbound.id, "inbound")
		if mirrored.Version != 30 || mirrored.Identity != inbound.Identity {
			t.Fatal("auth-only mirror overwrote the raw announcement", mirrored)
		}
		s.rememberConnPeerAddr(f.inbound.id, protocol.Frame{}, "127.0.0.1:64001")
		s.completeInboundAuth(f.inbound.id, actualAuth)
		unknownRaw := advertisedFactFor(t, advertisedActualSnapshot(t, s), f.inbound.id, "inbound")
		if !unknownRaw.ClaimSeen || unknownRaw.Version != 0 || !unknownRaw.Identity.IsZero() {
			t.Fatal("proof/Core mirror normalized missing raw claim into an announcement", unknownRaw)
		}
		s.rememberConnPeerAddr(f.inbound.id, protocol.Frame{Address: idTargetX.String(), Version: 30}, "127.0.0.1:64001")
		s.completeInboundAuth(f.inbound.id, actualAuth)
		contradiction := advertisedFactFor(t, advertisedActualSnapshot(t, s), f.inbound.id, "inbound")
		if contradiction.Identity != idTargetX || contradiction.Version != 30 || !contradiction.ClaimConflict || contradiction.MetadataIdentity == idTargetX {
			t.Fatal("contradictory raw claim was covertly normalized to proof", contradiction)
		}
		// Original welcome setter followed by original session publication, using
		// the genuine outbound owner. This observes raw metadata, not proof kind.
		local := s.localHandshakeCapabilities()
		s.peerMu.Lock()
		s.applyK3AdvertisedWelcomeLocked(reverse.outbound, protocol.Frame{}, local)
		s.peerMu.Unlock()
		k3Publish(t, s, reverse.outbound)
		noWelcome := advertisedFactFor(t, advertisedActualSnapshot(t, s), reverse.outbound.connID, "outbound")
		if !noWelcome.ClaimSeen || noWelcome.Version != 0 || !noWelcome.Identity.IsZero() {
			t.Fatal("missing new welcome was hidden by old metadata", noWelcome)
		}
	})
	t.Run("raw-zero-is-unknown-and-stale-original-Core-cannot-poison-replacement", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		wire, core, id := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 31})
		first := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if first.Identity != idPeerB || first.Version != 31 {
			t.Fatal("known raw claim absent", first)
		}
		s.applyK3AdvertisedInboundOpts(id, core, netcore.Options{LastActivity: time.Now()}, k3AdvertisedRawClaim{})
		partial := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if !partial.ClaimSeen || partial.Identity != idPeerB || partial.Version != 31 {
			t.Fatal("partial metadata was treated as a new zero announcement", partial)
		}
		s.rememberConnPeerAddr(id, protocol.Frame{}, "10.0.0.9:5001")
		zero := advertisedActualSnapshot(t, s)
		raw := advertisedFactFor(t, zero, id, "inbound")
		if !raw.ClaimSeen || !raw.Identity.IsZero() || raw.Version != 0 || zero.Status == "UNKNOWN" {
			t.Fatal("raw absent input was masked or made a source gap", raw, zero)
		}
		if core.ProtocolVersion() != 31 || core.Identity() != idPeerB {
			t.Fatal("original ApplyOpts zero-skip policy changed")
		}
		registryUnregister(s, wire)
		replacementWire := newNoIOConn(t, &net.TCPAddr{IP: net.IPv4(10, 0, 0, 10), Port: 5002})
		replacement := newTestNetCore(t, id, replacementWire, netcore.Inbound, netcore.Options{})
		s.peerMu.Lock()
		s.registerInboundConnLocked(replacementWire, replacement, nil, nil)
		s.peerMu.Unlock()
		t.Cleanup(func() { replacement.Close(); registryUnregister(s, replacementWire) })
		s.rememberConnPeerAddr(id, protocol.Frame{Address: idTargetX.String(), Version: 31}, "10.0.0.10:5002")
		before := advertisedActualSnapshot(t, s)
		// Actual production helper after its original lookup: old Core is deliberately
		// supplied after genuine unregister/registry replacement. No test callback,
		// retry lookup or forged positive source publication is involved.
		s.applyK3AdvertisedInboundOpts(id, core, netcore.Options{Identity: idPeerB, ProtocolVersion: 30}, k3AdvertisedRawClaim{Seen: true, Identity: idPeerB, Version: 30})
		after := advertisedActualSnapshot(t, s)
		last := advertisedFactFor(t, after, id, "inbound")
		if replacement.Identity() != idTargetX || replacement.ProtocolVersion() != 31 || last.Identity != idTargetX || last.Version != 31 || after.CompletedSequence != before.CompletedSequence {
			t.Fatal("stale original lookup rewrote replacement metadata or observation", last, before, after)
		}
		// A completely late ConnID is also a no-op, never a new member.
		registryUnregister(s, replacementWire)
		gone := advertisedActualSnapshot(t, s)
		s.rememberConnPeerAddr(id, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		if late := advertisedActualSnapshot(t, s); late.CompletedSequence != gone.CompletedSequence {
			t.Fatal("late ConnID resurrected a raw claim", late)
		}
	})
	t.Run("actual-CM-admission-replacement-and-whole-shutdown-batches", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		cm := s.connManager
		t.Cleanup(cm.shutdown)
		cm.mu.Lock()
		first := cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.11:64646"}, 1, nil)
		cm.mu.Unlock()
		if first.admitted == nil {
			t.Fatal("actual CM initial admission refused")
		}
		a := advertisedActualSnapshot(t, s)
		if a.SlotsAtCut != 1 || a.CompletedSequence == 0 {
			t.Fatal("actual CM slot not recorded before read", a)
		}
		cm.mu.Lock()
		second := cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.12:64646"}, 1, map[*slot]int{first.admitted: 0})
		cm.mu.Unlock()
		if second.admitted == nil || second.admitted == first.admitted {
			t.Fatal("original manual replacement absent")
		}
		b := advertisedActualSnapshot(t, s)
		tail := b.Facts[int(a.CoveredSequence):]
		if b.SlotsAtCut != 1 || len(tail) != 2 || tail[0].Batch != tail[1].Batch || tail[0].Owner != "cm" || tail[1].Owner != "cm" || !tail[0].Removed || tail[1].Removed || tail[0].Slot.Address != first.admitted.Address || tail[1].Slot.Address != second.admitted.Address || tail[0].Count != 2 || tail[1].Count != 2 || tail[0].Index != 1 || tail[1].Index != 2 {
			t.Fatal("eviction+replacement exposed a half transaction", tail, b)
		}
		cm.mu.Lock()
		third := cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.13:64646"}, 3, nil)
		cm.mu.Unlock()
		if third.admitted == nil {
			t.Fatal("actual second live slot admission refused")
		}
		before := advertisedActualSnapshot(t, s)
		cm.shutdown()
		closed := advertisedActualSnapshot(t, s)
		suffix := closed.Facts[int(before.CoveredSequence):]
		if closed.SlotsAtCut != 0 || len(suffix) != 2 || suffix[0].Batch != suffix[1].Batch || suffix[0].Count != 2 || suffix[1].Count != 2 || !suffix[0].Removed || !suffix[1].Removed {
			t.Fatal("multi-slot shutdown was not one complete original transaction", closed, suffix)
		}
		lifeClock := newK3ActualClock()
		lifeReq := advertisedActualRequest(lifeClock.read, 32, 8, 256)
		lifeSvc := advertisedActualService(t, &lifeReq)
		lifeCM := lifeSvc.connManager
		peer := advertisedActualService(t, nil)
		genuine := advertisedActualPair(t, peer, lifeSvc)
		t.Cleanup(lifeCM.shutdown)
		// The CM state handlers are the real handlers. Disable only the outbound
		// application-init callback; no App/Service Run or dial event loop is started.
		lifeCM.config.OnSessionEstablished = nil
		lifeCM.mu.Lock()
		initial := lifeCM.admitManualPeerLocked(ManualPeerRequested{Address: genuine.outbound.address}, 8, nil).admitted
		lifeCM.mu.Unlock()
		if initial == nil {
			t.Fatal("lifecycle admission refused")
		}
		initialGeneration := initial.Generation
		lifeCM.handleDialSucceeded(context.Background(), DialSucceeded{Address: initial.Address, SlotGeneration: initialGeneration, Session: genuine.outbound, ConnectedAddress: genuine.outbound.address})
		initializing := advertisedSlotFor(t, advertisedActualSnapshot(t, lifeSvc), initial.Address)
		if initializing.State != domain.SlotStateInitializing || initializing.ConnID != genuine.outbound.connID || initializing.Generation <= initialGeneration {
			t.Fatal("actual init owner binding not recorded", initializing)
		}
		lifeCM.handleSessionInitReady(context.Background(), SessionInitReady{Address: initial.Address, SlotGeneration: initializing.Generation})
		active := advertisedSlotFor(t, advertisedActualSnapshot(t, lifeSvc), initial.Address)
		if active.State != domain.SlotStateActive || active.Generation != initializing.Generation {
			t.Fatal("actual promotion missing", active)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		lifeCM.handleActiveSessionLost(cancelled, ActiveSessionLost{Address: initial.Address, SlotGeneration: active.Generation, WasHealthy: true, Error: errors.New("controlled existing-session loss")})
		reconnect := advertisedSlotFor(t, advertisedActualSnapshot(t, lifeSvc), initial.Address)
		if reconnect.State != domain.SlotStateReconnecting || reconnect.Generation <= active.Generation || reconnect.ConnID != 0 {
			t.Fatal("deactivate+reconnect original transaction omitted", reconnect)
		}
		beforeStale := advertisedActualSnapshot(t, lifeSvc)
		lifeCM.handleSessionInitReady(cancelled, SessionInitReady{Address: initial.Address, SlotGeneration: active.Generation})
		if stale := advertisedActualSnapshot(t, lifeSvc); stale.CompletedSequence != beforeStale.CompletedSequence {
			t.Fatal("stale CM generation changed source values", stale)
		}
		lifeCM.handleDialFailed(cancelled, DialFailed{Address: initial.Address, SlotGeneration: reconnect.Generation, Error: errors.New("controlled cancelled retry")})
		retry := advertisedSlotFor(t, advertisedActualSnapshot(t, lifeSvc), initial.Address)
		if retry.State != domain.SlotStateRetryWait || retry.Generation <= reconnect.Generation || retry.ConnID != 0 {
			t.Fatal("actual retry-wait not recorded", retry)
		}
		lifeCM.shutdown()
	})
	t.Run("actual-health-filter-and-existing-owner-context", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		_, _, id := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 31})
		fallback := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if fallback.HealthPresent || !fallback.Connected {
			t.Fatal("original inbound-only no-health fallback lost", fallback)
		}
		address := fallback.Address
		s.markPeerConnected(address, domain.PeerDirectionInbound)
		connected := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if !connected.HealthPresent || !connected.Connected || connected.HealthAddress != address {
			t.Fatal("actual health Connected mutation not projected", connected)
		}
		// Connected health ages through three allowed states; the source need not
		// invent a state timer. Cross-check the original helper, not a copied filter.
		s.peerMu.RLock()
		h := s.health[address]
		now := h.LastUsefulReceiveAt
		for _, at := range []time.Time{now, now.Add(heartbeatInterval), now.Add(heartbeatInterval + pongStallTimeout)} {
			state := s.computePeerStateAtLocked(h, at)
			if !isActiveConnectionState(h.Connected, uint64(id), state, "") {
				s.peerMu.RUnlock()
				t.Fatal("original connected state was not eligible", state)
			}
		}
		s.peerMu.RUnlock()
		s.publishPeerDisconnected(address, cleanDisconnect{})
		disconnected := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if !disconnected.HealthPresent || disconnected.Connected {
			t.Fatal("actual disconnected health did not affect existing connection", disconnected)
		}
		s.peerMu.RLock()
		state := s.computePeerStateAtLocked(s.health[address], time.Now())
		eligible := isActiveConnectionState(s.health[address].Connected, uint64(id), state, "")
		s.peerMu.RUnlock()
		if eligible {
			t.Fatal("original disconnected filter changed")
		}
		s.markPeerConnected(address, domain.PeerDirectionInbound)
		if !advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound").Connected {
			t.Fatal("real reconnect context did not become available")
		}
		s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address))
		cooled := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if !cooled.HealthPresent || cooled.Connected {
			t.Fatal("original cooldown did not change existing-owner context", cooled)
		}
		s.peerMu.Lock()
		s.forgetPeerAddressLocked(address)
		s.peerMu.Unlock()
		forgotten := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if forgotten.HealthPresent || !forgotten.Connected {
			t.Fatal("actual health deletion did not preserve original explicit inbound fallback", forgotten)
		}
		s.peerMu.Lock()
		created := s.ensurePeerHealthLocked(address)
		s.peerMu.Unlock()
		recreated := advertisedFactFor(t, advertisedActualSnapshot(t, s), id, "inbound")
		if created == nil || !recreated.HealthPresent || recreated.Connected {
			t.Fatal("standalone actual health creation missed existing owner", recreated)
		}
	})
	t.Run("same-address-atomic-replacement-alias-and-stale-session-cleanup", func(t *testing.T) {
		clock := newK3ActualClock()
		request := advertisedActualRequest(clock.read, 32, 8, 512)
		s := advertisedActualService(t, &request)
		a, b := advertisedActualService(t, nil), advertisedActualService(t, nil)
		first := advertisedActualPair(t, a, s)
		second := advertisedActualPair(t, b, s)
		old, current := first.outbound, second.outbound
		if old.address != current.address || old.connID == current.connID {
			t.Fatal("fixture is not two actual owners at one original address")
		}
		prepublished := advertisedActualSnapshot(t, s)
		oldBinding := advertisedFactFor(t, prepublished, old.connID, "outbound")
		currentBinding := advertisedFactFor(t, prepublished, current.connID, "outbound")
		if prepublished.PeerOwnersAtCut != 2 {
			t.Fatal("registered prepublication Cores were dropped", prepublished)
		}
		k3Publish(t, s, old)
		k3Publish(t, s, current)
		replaced := advertisedActualSnapshot(t, s)
		if replaced.PeerOwnersAtCut != 2 || advertisedFactFor(t, replaced, old.connID, "outbound").Generation != oldBinding.Generation || advertisedFactFor(t, replaced, current.connID, "outbound").Generation != currentBinding.Generation {
			t.Fatal("map replacement removed still-registered old Core or doubled alias/new generation", replaced)
		}
		s.onCMSessionTeardown(SessionInfo{Address: old.address, DialAddress: old.address, Session: old})
		stale := advertisedActualSnapshot(t, s)
		if stale.PeerOwnersAtCut != 2 || advertisedFactFor(t, stale, current.connID, "outbound").Generation != currentBinding.Generation {
			t.Fatal("stale owner teardown changed replacement binding", stale)
		}
		s.peerMu.RLock()
		same := s.sessions[current.address] == current
		s.peerMu.RUnlock()
		if !same {
			t.Fatal("source changed original stale session policy")
		}
		// Original cancelled fallback route owns alias registration/removal; the
		// existing current session prevents speculative cleanup of its actual alias.
		primary := domain.PeerAddress("127.0.0.1:64002")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.dialForCM(ctx, []domain.PeerAddress{primary, current.address}); err == nil {
			t.Fatal("cancelled actual fallback unexpectedly connected")
		}
		s.peerMu.RLock()
		alias := s.dialOrigin[current.address]
		s.peerMu.RUnlock()
		aliased := advertisedFactFor(t, advertisedActualSnapshot(t, s), current.connID, "outbound")
		if alias != primary || aliased.HealthAddress != primary {
			t.Fatal("actual fallback alias failed to update existing owner context", alias, aliased)
		}
		s.peerMu.Lock()
		s.forgetUnpublishedDialOriginLocked(old)
		s.peerMu.Unlock()
		if after := advertisedActualSnapshot(t, s); advertisedFactFor(t, after, current.connID, "outbound").HealthAddress != primary {
			t.Fatal("stale alias cleanup changed replacement context", after)
		}
		// Only the LAST registry/session view disappears. A mere map alias deletion
		// cannot be treated as loss of a still-registered actual connection.
		_ = old.Close()
		oldClosed := advertisedActualSnapshot(t, s)
		if oldClosed.PeerOwnersAtCut != 1 || advertisedFactFor(t, oldClosed, current.connID, "outbound").Generation != currentBinding.Generation {
			t.Fatal("actual old Core close did not remove exactly its binding", oldClosed)
		}
		s.onCMSessionTeardown(SessionInfo{Address: current.address, DialAddress: current.address, Session: current})
		mapGone := advertisedActualSnapshot(t, s)
		if mapGone.PeerOwnersAtCut != 1 || advertisedFactFor(t, mapGone, current.connID, "outbound").Generation != currentBinding.Generation {
			t.Fatal("map-only withdrawal hid registered Core", mapGone)
		}
		_ = current.Close()
		removed := advertisedActualSnapshot(t, s)
		if removed.PeerOwnersAtCut != 0 {
			t.Fatal("last actual connection binding was not removed", removed)
		}
		third := advertisedActualPair(t, b, s)
		k3Publish(t, s, third.outbound)
		readded := advertisedActualSnapshot(t, s)
		fresh := advertisedFactFor(t, readded, third.outbound.connID, "outbound")
		if readded.PeerOwnersAtCut != 1 || fresh.Generation <= currentBinding.Generation || third.outbound.connID == current.connID || readded.CompletedSequence <= removed.CompletedSequence {
			t.Fatal("real removal and later new connection hid their gap", removed, readded)
		}
	})
	t.Run("valid-cross-owner-reverse-finish-pending-prefix-and-idle-epoch-cut", func(t *testing.T) {
		clock := &advertisedProgramClock{}
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		_, _, id := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 31})
		before := advertisedActualSnapshot(t, s)
		t.Cleanup(s.connManager.shutdown)
		run, wait, release := advertisedOwnedTasks(t)
		pb, cb, pa := make(chan struct{}), make(chan struct{}), make(chan struct{})
		allowPB, allowCB, allowPA := make(chan struct{}), make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { release(allowPB); release(allowCB); release(allowPA) })
		clock.set(
			advertisedClockStep{value: 1000 * time.Second, hook: func() { close(pb); <-allowPB }},
			advertisedClockStep{value: 2000 * time.Second, hook: func() { close(cb); <-allowCB }},
			advertisedClockStep{value: 2500 * time.Second, hook: func() { close(pa); <-allowPA }},
			advertisedClockStep{value: 3000 * time.Second},
		)
		peerDone := run(func() {
			s.rememberConnPeerAddr(id, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		})
		wait(pb)
		cm := s.connManager
		var admitted *slot
		cmDone := run(func() {
			cm.mu.Lock()
			defer cm.mu.Unlock()
			admitted = cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.20:64646"}, 8, nil).admitted
		})
		wait(cb)
		release(allowPB)
		wait(pa)
		release(allowCB)
		wait(cmDone)
		pending := advertisedActualSnapshot(t, s)
		if pending.Pending != 1 || pending.CompletedSequence <= pending.CoveredSequence || pending.CompletedBatches <= pending.CoveredBatches {
			t.Fatal("completed CM suffix falsely became covered while peer Before was pending", pending)
		}
		advertisedSameCovered(t, before, pending)
		release(allowPA)
		wait(peerDone)
		if admitted == nil {
			t.Fatal("original CM mutation did not complete")
		}
		done := advertisedActualSnapshot(t, s)
		if done.Status == "UNKNOWN" || done.Pending != 0 || done.CompletedSequence != done.CoveredSequence || done.CompletedBatches != done.CoveredBatches {
			t.Fatal("legal reverse finish rejected or not closed", done)
		}
		tail := done.Facts[int(before.CoveredSequence):]
		if len(tail) != 2 || tail[0].Owner != "cm" || tail[1].Owner != "peer" || tail[0].Clock.Before != 2000*time.Second || tail[0].Clock.After != 3000*time.Second || tail[1].Clock.Before != 1000*time.Second || tail[1].Clock.After != 2500*time.Second {
			t.Fatal("append order manufactured physical clock order", tail)
		}
		// Two actual Snapshot calls in one mutation epoch can finish in reverse.
		// A later completed idle extension makes the earlier sampled cut stale;
		// it must not manufacture a duration-clock regression/source gap.
		cutHit, allowCut := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { release(allowCut) })
		clock.set(advertisedClockStep{value: 20000 * time.Second, hook: func() { close(cutHit); <-allowCut }}, advertisedClockStep{value: 30000 * time.Second})
		var older k3AdvertisedSnapshot
		olderDone := run(func() { older = s.k3AdvertisedSnapshot() })
		wait(cutHit)
		newer := advertisedActualSnapshot(t, s)
		release(allowCut)
		wait(olderDone)
		if newer.Status == "UNKNOWN" || older.Status == "UNKNOWN" || older.Cut != newer.Cut {
			t.Fatal("stale earlier Snapshot faulted a later confirmed idle cut", older, newer)
		}
		advertisedSameCovered(t, newer, older)
		done = newer
		// Snapshot has no owner lock: pause its idle clock outside source.mu, make
		// an actual peer mutation, then recheck the epoch rather than closing across it.
		idleHit, allowIdle := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { release(allowIdle) })
		clock.set(advertisedClockStep{value: 50000 * time.Second, hook: func() { close(idleHit); <-allowIdle }}, advertisedClockStep{value: 50100 * time.Second}, advertisedClockStep{value: 50200 * time.Second})
		var raced k3AdvertisedSnapshot
		idleDone := run(func() { raced = s.k3AdvertisedSnapshot() })
		wait(idleHit)
		mutationDone := run(func() {
			s.rememberConnPeerAddr(id, protocol.Frame{Address: idPeerB.String(), Version: 31}, "10.0.0.9:5001")
		})
		wait(mutationDone)
		release(allowIdle)
		wait(idleDone)
		advertisedSameCovered(t, done, raced)
		quiet := advertisedActualSnapshot(t, s)
		if quiet.Status == "UNKNOWN" || quiet.CoveredSequence <= done.CoveredSequence {
			t.Fatal("fresh quiescent read could not close completed epoch", quiet)
		}
	})
	t.Run("capacity-clock-unwind-and-value-copies-freeze-last-confirmed-prefix", func(t *testing.T) {
		clock := &advertisedProgramClock{}
		request := advertisedActualRequest(clock.read, 32, 8, 256)
		s := advertisedActualService(t, &request)
		_, core, id := advertisedActualInbound(t, s, protocol.Frame{Address: idPeerB.String(), Version: 31})
		_ = advertisedActualSnapshot(t, s)
		detached := advertisedActualSnapshot(t, s)
		if len(detached.Facts) == 0 {
			t.Fatal("actual raw owner prefix empty")
		}
		detached.Facts[0].Peer.Address = "caller-forgery"
		detached.Facts[0].Peer.Version = 999
		again := advertisedActualSnapshot(t, s)
		if reflect.DeepEqual(detached.Facts, again.Facts) {
			t.Fatal("Snapshot exposed mutable source history")
		}
		confirmed := again
		// Completed CM suffix + older pending peer ticket. The peer After clock
		// fails after the original Core write, so that suffix never acquires a cut.
		t.Cleanup(s.connManager.shutdown)
		run, wait, release := advertisedOwnedTasks(t)
		beforeHit, afterHit := make(chan struct{}), make(chan struct{})
		allowBefore, allowAfter := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { release(allowBefore); release(allowAfter) })
		clock.set(advertisedClockStep{value: 1000 * time.Second, hook: func() { close(beforeHit); <-allowBefore }}, advertisedClockStep{value: 2000 * time.Second}, advertisedClockStep{value: 3000 * time.Second}, advertisedClockStep{value: 3500 * time.Second, err: errors.New("controlled After clock refusal"), hook: func() { close(afterHit); <-allowAfter }})
		peerDone := run(func() {
			s.rememberConnPeerAddr(id, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		})
		wait(beforeHit)
		cm := s.connManager
		cm.mu.Lock()
		actual := cm.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.21:64646"}, 8, nil).admitted
		cm.mu.Unlock()
		if actual == nil {
			t.Fatal("original CM admission refused")
		}
		pending := advertisedActualSnapshot(t, s)
		advertisedSameCovered(t, confirmed, pending)
		if pending.CompletedSequence <= pending.CoveredSequence {
			t.Fatal("fixture lacks actual completed unclosed CM suffix", pending)
		}
		release(allowBefore)
		wait(afterHit)
		release(allowAfter)
		wait(peerDone)
		unknown := advertisedActualSnapshot(t, s)
		if unknown.Status != "UNKNOWN" || unknown.Reason == "" || core.ProtocolVersion() != 30 {
			t.Fatal("After error changed original mutation or failed sticky source", unknown)
		}
		advertisedSameCovered(t, confirmed, unknown)
		count := clock.calls()
		_ = s.k3AdvertisedSnapshot()
		if clock.calls() != count {
			t.Fatal("sticky source sampled clocks after loss of continuity")
		}
		// Capacity is observation-only: second original connection is STILL admitted
		// while covered prefix/count/cut remain exactly the prior coherent evidence.
		capClock := newK3ActualClock()
		capReq := advertisedActualRequest(capClock.read, 1, 8, 64)
		limited := advertisedActualService(t, &capReq)
		advertisedActualInbound(t, limited, protocol.Frame{Address: idPeerB.String(), Version: 31})
		capBefore := advertisedActualSnapshot(t, limited)
		_, _, second := advertisedActualInbound(t, limited, protocol.Frame{Address: idTargetX.String(), Version: 30})
		capAfter := advertisedActualSnapshot(t, limited)
		limited.peerMu.RLock()
		stillRegistered := limited.conns[second] != nil
		limited.peerMu.RUnlock()
		if !stillRegistered || capAfter.Status != "UNKNOWN" {
			t.Fatal("capacity fault became connection admission policy", capAfter)
		}
		advertisedSameCovered(t, capBefore, capAfter)
		// Entire eviction+replacement batch exceeds the remaining history by one:
		// none of that original transaction can be presented as covered evidence.
		batchClock := newK3ActualClock()
		batchReq := advertisedActualRequest(batchClock.read, 8, 8, 2)
		batchSvc := advertisedActualService(t, &batchReq)
		batchCM := batchSvc.connManager
		t.Cleanup(batchCM.shutdown)
		batchCM.mu.Lock()
		one := batchCM.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.22:64646"}, 1, nil).admitted
		batchCM.mu.Unlock()
		if one == nil {
			t.Fatal("first actual capped CM admission refused")
		}
		batchBefore := advertisedActualSnapshot(t, batchSvc)
		batchCM.mu.Lock()
		two := batchCM.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.23:64646"}, 1, map[*slot]int{one: 0}).admitted
		actualSlots := len(batchCM.slots)
		batchCM.mu.Unlock()
		batchAfter := advertisedActualSnapshot(t, batchSvc)
		if two == nil || actualSlots != 1 || batchAfter.Status != "UNKNOWN" {
			t.Fatal("whole-batch overflow altered original replacement or was hidden", batchAfter)
		}
		advertisedSameCovered(t, batchBefore, batchAfter)
		panicClock := &advertisedProgramClock{}
		panicReq := advertisedActualRequest(panicClock.read, 8, 8, 64)
		panicSvc := advertisedActualService(t, &panicReq)
		_, panicCore, panicID := advertisedActualInbound(t, panicSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		panicBefore := advertisedActualSnapshot(t, panicSvc)
		panicClock.set(advertisedClockStep{value: 1000 * time.Second}, advertisedClockStep{value: 1001 * time.Second, hook: func() { panic("controlled actual After clock panic") }})
		didPanic := func() (panicked bool) {
			defer func() { panicked = recover() != nil }()
			panicSvc.rememberConnPeerAddr(panicID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
			return false
		}()
		panicAfter := advertisedActualSnapshot(t, panicSvc)
		if !didPanic || panicCore.ProtocolVersion() != 30 || panicAfter.Status != "UNKNOWN" {
			t.Fatal("natural clock unwind changed original behavior or left source live", panicAfter)
		}
		advertisedSameCovered(t, panicBefore, panicAfter)
		unwindClock := newK3ActualClock()
		unwindReq := advertisedActualRequest(unwindClock.read, 8, 8, 64)
		unwindSvc := advertisedActualService(t, &unwindReq)
		advertisedActualInbound(t, unwindSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		unwindBefore := advertisedActualSnapshot(t, unwindSvc)
		badWire := newNoIOConn(t, &net.TCPAddr{IP: net.IPv4(10, 0, 0, 80), Port: 5003})
		badID := unwindSvc.allocateConnID("advertised_negative_unwind", "10.0.0.80:64646")
		badCore := newTestNetCore(t, badID, badWire, netcore.Inbound, netcore.Options{})
		t.Cleanup(badCore.Close)
		// Explicit corrupted-map negative only: force an ORIGINAL registry write
		// panic after Begin, own the original peerMu unlock, and restore the fixture
		// map. This does not claim healthy-node map panic reachability or publish a
		// synthetic positive owner. No production panic hook is introduced.
		originalMutationPanicked := func() (panicked bool) {
			unwindSvc.peerMu.Lock()
			defer unwindSvc.peerMu.Unlock()
			saved := unwindSvc.conns
			unwindSvc.conns = nil
			defer func() { unwindSvc.conns = saved; panicked = recover() != nil }()
			unwindSvc.registerInboundConnLocked(badWire, badCore, nil, nil)
			return false
		}()
		unwindAfter := advertisedActualSnapshot(t, unwindSvc)
		unwindSvc.peerMu.RLock()
		activeCleared := unwindSvc.k3AdvertisedActive == nil
		unwindSvc.peerMu.RUnlock()
		if !originalMutationPanicked || !activeCleared || unwindAfter.Status != "UNKNOWN" {
			t.Fatal("original mutation unwind left a live ticket/source", unwindAfter)
		}
		advertisedSameCovered(t, unwindBefore, unwindAfter)
		ownerClock := &advertisedProgramClock{}
		ownerReq := advertisedActualRequest(ownerClock.read, 8, 8, 64)
		ownerSvc := advertisedActualService(t, &ownerReq)
		ownerCM := ownerSvc.connManager
		t.Cleanup(ownerCM.shutdown)
		ownerBefore := advertisedActualSnapshot(t, ownerSvc)
		ownerClock.set(advertisedClockStep{value: 1000 * time.Second}, advertisedClockStep{value: 1001 * time.Second, hook: func() { panic("controlled owning-root CM After panic") }})
		cancelledOwner, cancelOwner := context.WithCancel(context.Background())
		cancelOwner()
		ownerPanicked := func() (panicked bool) {
			defer func() { panicked = recover() != nil }()
			ownerCM.handleManualPeer(cancelledOwner, ManualPeerRequested{Address: "10.0.0.81:64646"})
			return false
		}()
		if !ownerCM.mu.TryLock() {
			t.Fatal("actual owning CM root leaked cm.mu on selected-clock panic")
		}
		originalCMAdmitted := len(ownerCM.slots) == 1
		ownerCM.mu.Unlock()
		ownerAfter := advertisedActualSnapshot(t, ownerSvc)
		if !ownerPanicked || !originalCMAdmitted || ownerAfter.Status != "UNKNOWN" {
			t.Fatal("owning-root panic changed original admitted table or certified it", ownerAfter)
		}
		advertisedSameCovered(t, ownerBefore, ownerAfter)
		partialClock := newK3ActualClock()
		partialReq := advertisedActualRequest(partialClock.read, 16, 8, 128)
		partialSvc := advertisedActualService(t, &partialReq)
		partialPeer := advertisedActualService(t, nil)
		partialOwner := advertisedActualPair(t, partialPeer, partialSvc).outbound
		partialBefore := advertisedActualSnapshot(t, partialSvc)
		// Corrupted delivery-map negative: the original sessions assignment happens
		// BEFORE the forced upstream write panic. It is not a completed transaction.
		savedUpstream := partialSvc.upstream
		partialSvc.upstream = nil
		partialPanicked := func() (panicked bool) {
			defer func() { panicked = recover() != nil }()
			partialSvc.registerCMSession(partialOwner, partialOwner.address)
			return false
		}()
		if !partialSvc.peerMu.TryLock() {
			t.Fatal("registerCMSession original-write panic leaked peerMu")
		}
		if !partialSvc.deliveryMu.TryLock() {
			partialSvc.peerMu.Unlock()
			t.Fatal("registerCMSession original-write panic leaked deliveryMu")
		}
		partiallyWritten := partialSvc.sessions[partialOwner.address] == partialOwner
		partialSvc.upstream = savedUpstream
		partialSvc.deliveryMu.Unlock()
		partialSvc.peerMu.Unlock()
		partialAfter := advertisedActualSnapshot(t, partialSvc)
		if !partialPanicked || !partiallyWritten || partialAfter.Status != "UNKNOWN" {
			t.Fatal("deferred finish certified partial original CM publication", partialAfter)
		}
		advertisedSameCovered(t, partialBefore, partialAfter)
		regressClock := newK3ActualClock()
		regressReq := advertisedActualRequest(regressClock.read, 8, 8, 64)
		regressSvc := advertisedActualService(t, &regressReq)
		_, regressCore, regressID := advertisedActualInbound(t, regressSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		regressBefore := advertisedActualSnapshot(t, regressSvc)
		regressClock.configure(0, 0, nil)
		regressSvc.rememberConnPeerAddr(regressID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		regressAfter := advertisedActualSnapshot(t, regressSvc)
		if regressCore.ProtocolVersion() != 30 || regressAfter.Status != "UNKNOWN" {
			t.Fatal("clock regression silently advanced or became admission policy", regressAfter)
		}
		advertisedSameCovered(t, regressBefore, regressAfter)
		cutClock := &advertisedProgramClock{}
		cutReq := advertisedActualRequest(cutClock.read, 8, 8, 64)
		cutSvc := advertisedActualService(t, &cutReq)
		_, cutCore, cutID := advertisedActualInbound(t, cutSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		cutClock.set(advertisedClockStep{value: 1000 * time.Second})
		closedCut := advertisedActualSnapshot(t, cutSvc)
		if closedCut.Cut.After != 1000*time.Second {
			t.Fatal("idle cut fixture was not closed", closedCut)
		}
		// The next event is after its own old owner floor but BEFORE an already
		// certified idle cut. Unlike overlapping unfinished owners, this may not
		// retroactively insert a transition into the closed prefix's time range.
		cutClock.set(advertisedClockStep{value: 500 * time.Second}, advertisedClockStep{value: 600 * time.Second})
		cutSvc.rememberConnPeerAddr(cutID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		behindCut := advertisedActualSnapshot(t, cutSvc)
		if cutCore.ProtocolVersion() != 30 || behindCut.Status != "UNKNOWN" {
			t.Fatal("event behind a closed cut was inserted or changed original mutation", behindCut)
		}
		advertisedSameCovered(t, closedCut, behindCut)
		crossClock := &advertisedProgramClock{}
		crossReq := advertisedActualRequest(crossClock.read, 8, 8, 64)
		crossSvc := advertisedActualService(t, &crossReq)
		crossCM := crossSvc.connManager
		t.Cleanup(crossCM.shutdown)
		_, crossCore, crossID := advertisedActualInbound(t, crossSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		crossBefore := advertisedActualSnapshot(t, crossSvc)
		// The CM transaction has ALREADY completed before the new peer Begin.
		// No idle Snapshot closes a newer cut between them. The new Before must
		// still follow all owner floors that were completed when it began; this
		// does not constrain a previously pending peer's legal reverse finish.
		crossClock.set(advertisedClockStep{value: 2000 * time.Second}, advertisedClockStep{value: 3000 * time.Second}, advertisedClockStep{value: 2500 * time.Second}, advertisedClockStep{value: 2600 * time.Second})
		crossCM.mu.Lock()
		crossAdmitted := crossCM.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.82:64646"}, 8, nil).admitted
		crossCM.mu.Unlock()
		if crossAdmitted == nil {
			t.Fatal("cross-owner fixture CM transaction did not complete")
		}
		crossSvc.rememberConnPeerAddr(crossID, protocol.Frame{Address: idPeerB.String(), Version: 30}, "10.0.0.9:5001")
		// A subsequent recovered reading cannot hide the already observed gap.
		crossClock.set(advertisedClockStep{value: 10000 * time.Second})
		crossAfter := advertisedActualSnapshot(t, crossSvc)
		if crossCore.ProtocolVersion() != 30 || crossAfter.Status != "UNKNOWN" {
			t.Fatal("new peer Begin behind an already-completed CM bracket was certified", crossAfter)
		}
		advertisedSameCovered(t, crossBefore, crossAfter)
		invalidReq := advertisedActualRequest(newK3ActualClock().read, 0, 8, 64)
		invalid := advertisedActualService(t, &invalidReq)
		if got := advertisedActualSnapshot(t, invalid); got.Status != "UNKNOWN" || invalid.connManager == nil {
			t.Fatal("invalid observation denied original construction or appeared valid", got)
		}
		equalClock := newK3ActualClock()
		equalClock.configure(0, 0, nil)
		equalReq := advertisedActualRequest(equalClock.read, 8, 8, 64)
		equalSvc := advertisedActualService(t, &equalReq)
		advertisedActualInbound(t, equalSvc, protocol.Frame{Address: idPeerB.String(), Version: 31})
		if equal := advertisedActualSnapshot(t, equalSvc); equal.Status != "NO_PROGRESS" || equal.CoveredSequence == 0 {
			t.Fatal("equal clocks were a gap or falsely positive temporal progress", equal)
		}
		slotClock := newK3ActualClock()
		slotReq := advertisedActualRequest(slotClock.read, 8, 1, 64)
		slotSvc := advertisedActualService(t, &slotReq)
		slotCM := slotSvc.connManager
		t.Cleanup(slotCM.shutdown)
		slotCM.mu.Lock()
		slotOne := slotCM.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.24:64646"}, 3, nil).admitted
		slotCM.mu.Unlock()
		slotBefore := advertisedActualSnapshot(t, slotSvc)
		slotCM.mu.Lock()
		slotTwo := slotCM.admitManualPeerLocked(ManualPeerRequested{Address: "10.0.0.25:64646"}, 3, nil).admitted
		slotCount := len(slotCM.slots)
		slotCM.mu.Unlock()
		slotAfter := advertisedActualSnapshot(t, slotSvc)
		if slotOne == nil || slotTwo == nil || slotCount != 2 || slotAfter.Status != "UNKNOWN" {
			t.Fatal("slot observation cap changed original table admission", slotAfter)
		}
		advertisedSameCovered(t, slotBefore, slotAfter)
		addressClock := newK3ActualClock()
		addressReq := advertisedActualRequest(addressClock.read, 8, 8, 64)
		addressSvc := advertisedActualService(t, &addressReq)
		addressCM := addressSvc.connManager
		t.Cleanup(addressCM.shutdown)
		addressBefore := advertisedActualSnapshot(t, addressSvc)
		longAddress := domain.PeerAddress(strings.Repeat("a", k3AdvertisedMaxAddressBytes+1))
		addressCM.mu.Lock()
		longSlot := addressCM.admitManualPeerLocked(ManualPeerRequested{Address: longAddress}, 3, nil).admitted
		addressCM.mu.Unlock()
		addressAfter := advertisedActualSnapshot(t, addressSvc)
		if longSlot == nil || longSlot.Address != longAddress || addressAfter.Status != "UNKNOWN" {
			t.Fatal("address bound became original admission policy", addressAfter)
		}
		advertisedSameCovered(t, addressBefore, addressAfter)
	})
}
