package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/transport"
)

func TestAddressAgingPromotionPreservesFirstDiscovery(t *testing.T) {
	cfg := testProviderConfig()
	now := cfg.NowFn()
	cfg.NowFn = func() time.Time { return now }
	pp := NewPeerProvider(cfg)
	address := domain.PeerAddress("8.8.8.8:64646")
	pp.Add(address, domain.PeerSourcePeerExchange)
	first := pp.StaticSnapshotAll()[address].AddedAt
	now = now.Add(8 * time.Hour)
	pp.Promote(address, domain.PeerSourceManual)
	if got := pp.StaticSnapshotAll()[address].AddedAt; !got.Equal(first) {
		t.Fatalf("promotion changed discovery time: %v -> %v", first, got)
	}
}

func TestAddressAgingNeverPeriodicallyDeletesByScore(t *testing.T) {
	address := domain.PeerAddress("8.8.8.8:64646")
	old := time.Now().Add(-60 * 24 * time.Hour)
	svc := &Service{peers: []transport.Peer{{Address: address, Source: domain.PeerSourcePeerExchange}}, health: map[domain.PeerAddress]*peerHealth{address: {Address: address, Score: -50, LastConnectedAt: old}}, persistedMeta: map[domain.PeerAddress]*peerEntry{address: {Address: address, AddedAt: &old}}}
	svc.evictStalePeers()
	if len(svc.peers) != 1 {
		t.Fatal("periodic score sweep deleted an address without table pressure")
	}
}

func TestAddressAgingThresholds(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first := now.Add(-6 * time.Hour)
	success := now.Add(-7 * 24 * time.Hour)
	for _, tc := range []struct {
		name  string
		entry peerEntry
		stale bool
	}{
		{"never third", peerEntry{AddedAt: &first, DialAttempts: 3}, true},
		{"never second", peerEntry{AddedAt: &first, DialAttempts: 2}, false},
		{"worked tenth", peerEntry{AddedAt: &first, LastSessionAt: &success, DialAttempts: 10}, true},
		{"worked ninth", peerEntry{AddedAt: &first, LastSessionAt: &success, DialAttempts: 9}, false},
		{"old alone", peerEntry{AddedAt: &success, DialAttempts: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := addressStale(tc.entry, now); got != tc.stale {
				t.Fatalf("stale=%v want %v", got, tc.stale)
			}
		})
	}
}

func TestAddressAgingOneFailureAndExactEndpointSuccess(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	a, b := domain.PeerAddress("8.8.8.8:1234"), domain.PeerAddress("8.8.8.8:64646")
	svc := &Service{peerClock: func() time.Time { return now }, persistedMeta: map[domain.PeerAddress]*peerEntry{a: {Address: a, DialAttempts: 4}, b: {Address: b, DialAttempts: 7}}}
	svc.recordAddressDialFailure(a)
	if svc.persistedMeta[a].DialAttempts != 5 {
		t.Fatal("failure not counted")
	}
	svc.peerMu.Lock()
	svc.noteAddressSessionLocked(b, peerDirectionInbound)
	if svc.persistedMeta[b].DialAttempts != 7 {
		t.Fatal("inbound cleared aging")
	}
	svc.noteAddressSessionLocked(b, peerDirectionOutbound)
	svc.peerMu.Unlock()
	if svc.persistedMeta[a].DialAttempts != 5 || svc.persistedMeta[a].LastSessionAt != nil {
		t.Fatal("success at B cleared A")
	}
	if svc.persistedMeta[b].DialAttempts != 0 || svc.persistedMeta[b].LastSessionAt == nil {
		t.Fatal("outbound B not recovered")
	}
}

func TestAddressAgingCapacityRejectsHealthyAndEvictsOnlyStale(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first := now.Add(-8 * time.Hour)
	svc := &Service{persistedMeta: map[domain.PeerAddress]*peerEntry{}}
	for i := 0; i < maxPersistedPeers; i++ {
		address := domain.PeerAddress(fmt.Sprintf("8.%d.%d.1:64646", i/250, i%250))
		svc.peers = append(svc.peers, transport.Peer{Address: address, Source: domain.PeerSourcePeerExchange})
		svc.persistedMeta[address] = &peerEntry{Address: address, Source: domain.PeerSourcePeerExchange, AddedAt: &first}
	}
	svc.peerMu.Lock()
	if _, ok := svc.makeAutomaticAddressRoomLocked(now); ok {
		t.Fatal("full healthy table allowed replacement")
	}
	victim := svc.peers[0].Address
	svc.persistedMeta[victim].DialAttempts = 3
	if got, ok := svc.makeAutomaticAddressRoomLocked(now); !ok || got == nil || got.address != victim {
		t.Fatalf("eviction=%v %v", got, ok)
	}
	svc.peerMu.Unlock()
	if len(svc.peers) != 499 || svc.persistedMeta[victim] != nil {
		t.Fatal("memory not evicted")
	}
}

func TestAddressAgingDialLadderCountsNetworkBeforeLocalHold(t *testing.T) {
	svc := newDialChoiceService(t)
	a, b := domain.PeerAddress("198.51.100.7:64646"), domain.PeerAddress("198.51.100.8:64646")
	svc.addPeerAddress(a, "full", domain.PeerIdentity{})
	svc.addPeerAddress(b, "full", domain.PeerIdentity{})
	svc.secureSessions.marks.noteV2Unanswered(b)
	peer := newScriptedPeer(answerClientHello(tlsHandshakeGarbage))
	raw := peer.dialer()
	sockets := 0
	svc.peerTCPDial = func(ctx context.Context, address string, _ time.Duration) (net.Conn, error) {
		sockets++
		return raw(ctx)
	}
	_, err := svc.dialForCM(context.Background(), []domain.PeerAddress{a, b})
	if !errors.Is(err, errV2RetryHeld) {
		t.Fatalf("final error=%v", err)
	}
	if sockets != 1 {
		t.Fatalf("sockets=%d want 1", sockets)
	}
	if got := svc.persistedMeta[a].DialAttempts; got != 1 {
		t.Fatalf("actual first branch lost before local final refusal: %d", got)
	}
	if svc.persistedMeta[b].DialAttempts != 0 {
		t.Fatal("ladder counted more than once")
	}
	peer.sockets(t)
}

func TestAddressAgingBusyIsAttemptWithoutScorePenalty(t *testing.T) {
	svc := newDialChoiceService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	svc.addPeerAddress(address, "full", domain.PeerIdentity{})
	peer := newScriptedPeer(answerClientHello(sessionv2.RefusalAlert()))
	raw := peer.dialer()
	svc.peerTCPDial = func(ctx context.Context, _ string, _ time.Duration) (net.Conn, error) { return raw(ctx) }
	_, err := svc.dialForCM(context.Background(), []domain.PeerAddress{address})
	if !errors.Is(err, errV2RefusedByListener) {
		t.Fatalf("busy outcome=%v", err)
	}
	svc.onCMDialFailed(address, err, false)
	if svc.persistedMeta[address].DialAttempts != 1 {
		t.Fatal("busy listener not counted exactly once")
	}
	if h := svc.health[address]; h != nil && (h.Score != 0 || h.ConsecutiveFailures != 0) {
		t.Fatalf("busy charged score: %+v", h)
	}
	peer.sockets(t)
}

func TestAddressAgingCommonDialGateHoldsStaleCMRetry(t *testing.T) {
	svc := newDialChoiceService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	svc.addPeerAddress(address, "full", domain.PeerIdentity{})
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first, last := now.Add(-7*time.Hour), now.Add(-time.Hour)
	svc.peerClock = func() time.Time { return now }
	svc.persistedMeta[address].AddedAt = &first
	svc.persistedMeta[address].DialAttempts = 3
	svc.persistedMeta[address].LastDialAttemptAt = &last
	svc.peerTCPDial = func(context.Context, string, time.Duration) (net.Conn, error) {
		t.Fatal("CM retry bypassed aging hold")
		return nil, errors.New("unreachable")
	}
	for range 3 {
		if _, err := svc.dialForCM(context.Background(), []domain.PeerAddress{address}); !errors.Is(err, errAddressRecheckHeld) {
			t.Fatalf("retry error=%v", err)
		}
	}
	if svc.persistedMeta[address].DialAttempts != 3 {
		t.Fatal("local aging hold counted")
	}
}

func TestAddressAgingEveryScoreRefusalClassified(t *testing.T) {
	remote := []error{errV2RefusedByListener, errLegacyRefusedPinned, errLegacyRefusedV2Bound, errEndpointOwnerMismatch, errLegacyProbeAnsweredV2, errLegacyRefusedAfterV2Welcome, errLegacyProbeAdmittedNothing, errV2FailedBeforeProof}
	for _, outcome := range unchargedDialOutcomes {
		classes := 0
		if localAddressDialOutcome(outcome) {
			classes++
		}
		for _, charge := range remote {
			if errors.Is(outcome, charge) {
				classes++
			}
		}
		if classes != 1 {
			t.Fatalf("outcome %v has %d aging classifications", outcome, classes)
		}
	}
}

func TestAddressAgingCapacityProtectsActiveAndOperatorAndSecurity(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first := now.Add(-8 * time.Hour)
	svc := newDialChoiceService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	id := domaintest.ID("aging-protected")
	if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(address), id, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	svc.secureSessions.marks.noteOutboundV2(address)
	entry := peerEntry{Address: address, AddedAt: &first, DialAttempts: 3, Source: domain.PeerSourcePeerExchange}
	for _, tc := range []struct {
		name   string
		change func(*peerEntry)
	}{
		{"manual", func(e *peerEntry) { e.Source = domain.PeerSourceManual }},
		{"bootstrap", func(e *peerEntry) { e.Source = domain.PeerSourceBootstrap }},
		{"in flight", func(e *peerEntry) { e.ActiveDials = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := entry
			tc.change(&candidate)
			svc.peerMu.Lock()
			defer svc.peerMu.Unlock()
			if svc.addressEvictableLocked(candidate, now) {
				t.Fatal("protected address evictable")
			}
		})
	}
	svc.observedAddrs[id] = "8.8.8.8"
	svc.peerIDs[address] = claimedAddressIdentity(id)
	svc.persistedMeta[address] = &entry
	svc.peers = []transport.Peer{{Address: address}}
	svc.peerMu.Lock()
	svc.forgetPeerAddressLocked(address)
	orphan := svc.orphanedIdentitiesLocked([]domain.PeerIdentity{id})
	svc.peerMu.Unlock()
	if len(orphan) != 1 {
		t.Fatal("last identity anchor not orphaned")
	}
	svc.finishAddressEviction(&addressEviction{address: address, identity: id})
	if _, exists := svc.observedAddrs[id]; exists {
		t.Fatal("orphaned address vote leaked")
	}
	if !svc.secureSessions.store.identityPinned(id) || !svc.secureSessions.store.endpointRequiresV2(address) || !svc.secureSessions.marks.v2Strong(address) {
		t.Fatal("address eviction erased downgrade protection")
	}
}

func TestAddressAgingMigrationAndRoundTripPreserveHistoryAndSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	address := domain.PeerAddress("198.51.100.7:64646")
	if err := savePeerState(path, peerStateFile{Version: 4, Peers: []peerEntry{{Address: address, Source: domain.PeerSourceManual, AddedAt: &now, LastConnectedAt: &now}}}); err != nil {
		t.Fatal(err)
	}
	state, err := loadPeerState(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 5 || state.Peers[0].LastSessionAt == nil || state.Peers[0].Source != domain.PeerSourceManual {
		t.Fatalf("migration=%+v", state)
	}
	state.Peers[0].DialAttempts = 9
	if err := savePeerState(path, state); err != nil {
		t.Fatal(err)
	}
	svc := NewService(config.Node{ListenAddress: "127.0.0.1:0", Type: config.NodeTypeFull, TrustStorePath: filepath.Join(t.TempDir(), "trust.json"), PeersStatePath: path}, testIdentityForNetworkConsumerTest(t), nil)
	if len(svc.peers) != 1 || svc.peers[0].Source != domain.PeerSourceManual || svc.persistedMeta[address].DialAttempts != 9 {
		t.Fatal("restart lost manual provenance or history")
	}
}

func TestAddressAgingRecheckRotationSurvivesStreamOfNewAddresses(t *testing.T) {
	cfg := testProviderConfig()
	now := cfg.NowFn()
	old := now.Add(-8 * 24 * time.Hour)
	worked := []domain.PeerAddress{"8.8.8.8:64646", "9.9.9.9:64646"}
	cfg.HealthFn = func(address domain.PeerAddress) *PeerHealthView {
		for _, w := range worked {
			if address == w {
				return &PeerHealthView{Aging: peerEntry{Address: address, AddedAt: &old, LastSessionAt: &old, DialAttempts: 10}}
			}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)
	for _, address := range worked {
		pp.Add(address, domain.PeerSourcePeerExchange)
	}
	seen := map[domain.PeerAddress]bool{}
	for i := 0; i < 8; i++ {
		pp.Add(domain.PeerAddress(fmt.Sprintf("11.%d.1.1:64646", i)), domain.PeerSourcePeerExchange)
		candidates := pp.Candidates()
		if len(candidates) == 0 {
			t.Fatal("no candidates")
		}
		seen[candidates[0].Address] = true
		if i%4 != 3 && candidates[0].Address == worked[0] {
			t.Fatal("stale address ranked ahead of fresh outside reserved recheck")
		}
	}
	for _, address := range worked {
		if !seen[address] {
			t.Fatalf("previously working %s starved", address)
		}
	}
}

func TestAddressAgingMemoryAndFileHaveSameAutomaticBudget(t *testing.T) {
	svc := newDialChoiceService(t)
	now := time.Now().Add(-8 * time.Hour)
	for i := 0; i < maxPersistedPeers; i++ {
		address := domain.PeerAddress(fmt.Sprintf("8.%d.%d.1:64646", i/250, i%250))
		if !svc.addPeerAddress(address, "full", domain.PeerIdentity{}) {
			t.Fatalf("rejected automatic address %d before capacity", i)
		}
	}
	if svc.addPeerAddress("9.9.9.9:64646", "full", domain.PeerIdentity{}) {
		t.Fatal("full healthy table admitted address")
	}
	victim := svc.peers[0].Address
	svc.persistedMeta[victim].AddedAt = &now
	svc.persistedMeta[victim].DialAttempts = 3
	if !svc.addPeerAddress("9.9.9.9:64646", "full", domain.PeerIdentity{}) {
		t.Fatal("full table did not replace stale address")
	}
	svc.peerMu.Lock()
	entries := svc.buildPeerEntriesLocked(nil)
	svc.peerMu.Unlock()
	if len(entries) != 500 || len(trimPeerEntries(entries)) != 500 || len(svc.peers) != 500 || svc.peerProvider.Count() != 500 {
		t.Fatal("automatic memory/provider/file capacity diverged")
	}
	if svc.persistedMeta[victim] != nil {
		t.Fatal("eviction kept stale metadata")
	}
}

func TestAddressAgingTransportSuccessDoesNotResetAndFullPinStoreIsLocal(t *testing.T) {
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	_ = listener
	target := domain.PeerAddress(normalizeAddress(address))
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("pin_store_full_%v", full), func(t *testing.T) {
			dialer, stop := startQuietNode(t, func(svc *Service) {
				if full {
					fillPinStore(t, svc.secureSessions.store)
				}
			})
			defer stop()
			dialer.recordAddedPeer(target, addPeerModeOperator)
			dialer.peerMu.Lock()
			dialer.persistedMeta[target].DialAttempts = 5
			dialer.peerMu.Unlock()
			result, err := dialer.dialForCM(context.Background(), []domain.PeerAddress{target})
			if full && !errors.Is(err, errPinStoreFull) {
				t.Fatalf("full pin store error=%v", err)
			}
			if !full && err != nil {
				t.Fatalf("handshake failed=%v", err)
			}
			if result.Session != nil {
				dialer.onCMStaleSession(result.Session)
				_ = result.Session.Close()
			}
			dialer.peerMu.RLock()
			entry := *dialer.persistedMeta[target]
			dialer.peerMu.RUnlock()
			if entry.DialAttempts != 5 || entry.LastSessionAt != nil {
				t.Fatalf("unpublished transport/local failure changed aging: %+v", entry)
			}
		})
	}
}

func TestAddressAgingPublishedOutboundClearsOnlyItsEndpointAndInboundClearsNeither(t *testing.T) {
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	target := domain.PeerAddress(normalizeAddress(address))
	other := domain.PeerAddress("198.51.100.19:64646")
	dialer, stop := dialerNode(t, address, func(svc *Service) {
		svc.recordAddedPeer(target, addPeerModeBootstrap)
		svc.persistedMeta[target].DialAttempts = 5
		svc.persistedMeta[other] = &peerEntry{Address: other, DialAttempts: 8}
		listener.peerMu.Lock()
		listener.persistedMeta[target] = &peerEntry{Address: target, DialAttempts: 5}
		listener.persistedMeta[other] = &peerEntry{Address: other, DialAttempts: 8}
		id := peerIdentityOf(t, svc)
		listener.peerIDs[target] = claimedAddressIdentity(id)
		listener.peerIDs[other] = claimedAddressIdentity(id)
		listener.peerMu.Unlock()
	})
	defer stop()
	waitForConditionMsg(t, 10*time.Second, "outbound session did not publish aging success", func() bool {
		dialer.peerMu.RLock()
		defer dialer.peerMu.RUnlock()
		return dialer.persistedMeta[target].LastSessionAt != nil
	})
	dialer.peerMu.RLock()
	if dialer.persistedMeta[target].DialAttempts != 0 || dialer.persistedMeta[other].DialAttempts != 8 || dialer.persistedMeta[other].LastSessionAt != nil {
		t.Error("outbound success reset another endpoint")
	}
	dialer.peerMu.RUnlock()
	waitForConditionMsg(t, 10*time.Second, "inbound session not authenticated", func() bool { v2, _ := authenticatedInbound(listener); return v2 > 0 })
	listener.peerMu.RLock()
	defer listener.peerMu.RUnlock()
	if listener.persistedMeta[target].DialAttempts != 5 || listener.persistedMeta[other].DialAttempts != 8 || listener.persistedMeta[target].LastSessionAt != nil || listener.persistedMeta[other].LastSessionAt != nil {
		t.Fatal("inbound proof reset outbound endpoint history")
	}

}

func TestAddressAgingExplicitOperatorRetryPreservesHistory(t *testing.T) {
	svc := newDialChoiceService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	svc.addPeerAddress(address, "full", domain.PeerIdentity{})
	now := time.Now().UTC()
	first, last := now.Add(-8*time.Hour), now.Add(-time.Hour)
	svc.persistedMeta[address].AddedAt = &first
	svc.persistedMeta[address].DialAttempts = 3
	svc.persistedMeta[address].LastDialAttemptAt = &last
	svc.promotePeerAddress(address)
	if _, err := svc.beginAddressDial(address); !errors.Is(err, errAddressRecheckHeld) {
		t.Fatalf("announce bypassed recheck=%v", err)
	}
	svc.recordAddedPeer(address, addPeerModeOperator)
	release, err := svc.beginAddressDial(address)
	if err != nil {
		t.Fatalf("explicit operator retry refused=%v", err)
	}
	release()
	if svc.persistedMeta[address].DialAttempts != 3 || svc.persistedMeta[address].LastSessionAt != nil || !svc.persistedMeta[address].AddedAt.Equal(first) {
		t.Fatal("operator intent invented discovery or session success")
	}
	if _, err := svc.beginAddressDial(address); !errors.Is(err, errAddressRecheckHeld) {
		t.Fatal("operator retry authorization persisted beyond one dial")
	}
}

func TestAddressAgingSuccessfulFallbackPacesPrimaryWithoutReset(t *testing.T) {
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	target := domain.PeerAddress(normalizeAddress(address))
	primary := domain.PeerAddress("198.51.100.7:64646")
	dialer, stop := startQuietNode(t, nil)
	defer stop()
	dialer.recordAddedPeer(primary, addPeerModeBootstrap)
	now := time.Now().UTC()
	old, last := now.Add(-8*24*time.Hour), now.Add(-7*time.Hour)
	dialer.peerMu.Lock()
	entry := dialer.persistedMeta[primary]
	entry.AddedAt = &old
	entry.LastSessionAt = &old
	entry.DialAttempts = 10
	entry.LastDialAttemptAt = &last
	dialer.peerMu.Unlock()
	busy := newScriptedPeer(answerClientHello(sessionv2.RefusalAlert()))
	rawBusy := busy.dialer()
	dialer.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
		if address == string(primary) {
			return rawBusy(ctx)
		}
		return dialTCPDirect(ctx, address, timeout)
	}
	result, err := dialer.dialForCM(context.Background(), []domain.PeerAddress{primary, target})
	if err != nil {
		t.Fatalf("fallback failed: %v", err)
	}
	busy.sockets(t)
	dialer.onCMStaleSession(result.Session)
	_ = result.Session.Close()
	dialer.peerMu.RLock()
	after := *dialer.persistedMeta[primary]
	dialer.peerMu.RUnlock()
	if after.DialAttempts != 10 || !after.LastSessionAt.Equal(old) || after.LastDialAttemptAt.Before(now) {
		t.Fatalf("fallback changed primary history or lost pacing: %+v", after)
	}
	if _, err := dialer.dialForCM(context.Background(), []domain.PeerAddress{primary, target}); !errors.Is(err, errAddressRecheckHeld) {
		t.Fatalf("existing-slot retry bypassed successful fallback pacing: %v", err)
	}
}

func TestAddressAgingSameIPDedupAndDiagnosticsPreferUntriedPort(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lastAttempt time.Duration
		held        bool
	}{
		{name: "stale recheck due", lastAttempt: -7 * time.Hour},
		{name: "stale recheck held", lastAttempt: -time.Hour, held: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testProviderConfig()
			now := cfg.NowFn()
			old, last := now.Add(-8*24*time.Hour), now.Add(tc.lastAttempt)
			a, b := domain.PeerAddress("8.8.8.8:1234"), domain.PeerAddress("8.8.8.8:64646")
			cfg.HealthFn = func(address domain.PeerAddress) *PeerHealthView {
				if address == a {
					return &PeerHealthView{Score: 100, Aging: peerEntry{Address: a, AddedAt: &old, LastSessionAt: &old, LastDialAttemptAt: &last, DialAttempts: 10}}
				}
				return nil
			}
			pp := NewPeerProvider(cfg)
			pp.Add(a, domain.PeerSourcePeerExchange)
			pp.Add(b, domain.PeerSourcePeerExchange)

			candidates := pp.Candidates()
			if len(candidates) != 1 || candidates[0].Address != b {
				t.Fatalf("same-IP candidates = %v, want only untried B %s", candidates, b)
			}
			known := pp.KnownPeers()
			if len(known) != 2 || known[0].Address != b || known[1].Address != a {
				t.Fatalf("diagnostic order = %v, want B then A", known)
			}
			if slices.Contains(known[0].ExcludeReasons, domain.ExcludeReasonIPDedup) {
				t.Errorf("untried B incorrectly excluded by IP dedup: %v", known[0].ExcludeReasons)
			}
			if tc.held {
				if !slices.Contains(known[1].ExcludeReasons, domain.ExcludeReasonCooldown) {
					t.Errorf("recent stale A lacks cooldown: %v", known[1].ExcludeReasons)
				}
			} else if !slices.Contains(known[1].ExcludeReasons, domain.ExcludeReasonIPDedup) {
				t.Errorf("due stale A lacks IP dedup against B: %v", known[1].ExcludeReasons)
			}
		})
	}
}

func TestAddressAgingRestoredBootstrapOverlapSurvivesFullAutomaticTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	x := domain.PeerAddress("9.9.9.9:64646")
	now := time.Now().UTC()
	old, last := now.Add(-8*24*time.Hour), now.Add(-7*time.Hour)
	if err := savePeerState(path, peerStateFile{Version: 5, Peers: []peerEntry{{
		Address: x, Source: domain.PeerSourcePeerExchange, AddedAt: &old,
		LastSessionAt: &old, LastDialAttemptAt: &last, DialAttempts: 10,
	}}}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(config.Node{
		ListenAddress: "127.0.0.1:0", Type: config.NodeTypeFull,
		BootstrapPeers: []string{string(x)}, PeersStatePath: path,
		TrustStorePath: filepath.Join(t.TempDir(), "trust.json"),
	}, testIdentityForNetworkConsumerTest(t), nil)
	t.Cleanup(svc.WaitBackground)
	if entry := svc.persistedMeta[x]; entry == nil || entry.Source != domain.PeerSourceBootstrap {
		t.Errorf("restored metadata source = %+v, want configured bootstrap", entry)
	}
	if len(svc.peers) != 1 || svc.peers[0].Address != x || svc.peers[0].Source != domain.PeerSourceBootstrap {
		t.Fatalf("runtime bootstrap provenance = %v", svc.peers)
	}
	for i := 0; i < maxPersistedPeers; i++ {
		address := domain.PeerAddress(fmt.Sprintf("8.%d.%d.1:64646", i/250, i%250))
		if !svc.addPeerAddress(address, "full", domain.PeerIdentity{}) {
			t.Fatalf("automatic address %d rejected before its independent capacity", i)
		}
	}
	if svc.addPeerAddress("7.7.7.7:64646", "full", domain.PeerIdentity{}) {
		t.Error("new automatic address replaced protected stale bootstrap in a full healthy table")
	}
	if len(svc.peers) != maxPersistedPeers+1 || svc.persistedMeta[x] == nil {
		t.Fatalf("full automatic admission lost bootstrap: peers=%d bootstrap=%+v", len(svc.peers), svc.persistedMeta[x])
	}
}
