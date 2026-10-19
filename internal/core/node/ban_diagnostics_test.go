package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestBanDiagnosticsIncludeLocalAndRemoteSources(t *testing.T) {
	now := time.Now().UTC()
	until := now.Add(time.Hour)
	svc := &Service{
		bans:          map[string]banEntry{"192.0.2.1": {Score: 2}, "192.0.2.2": {Score: 10, Blacklisted: until}},
		persistedMeta: map[domain.PeerAddress]*peerEntry{"192.0.2.3:64646": {RemoteBannedUntil: &until}},
	}
	svc.rebuildBanDiagnosticsSnapshot()
	raw, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Version        int               `json:"version"`
		LocalBans      []json.RawMessage `json:"local_bans"`
		RemotePeerBans []json.RawMessage `json:"remote_peer_bans"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Version != 2 || len(reply.LocalBans) != 2 || len(reply.RemotePeerBans) != 1 {
		t.Fatalf("ban diagnostic omits local score/blacklist or remote peer ban: %s", raw)
	}
}

func TestBanDiagnosticsExposeProvenIdentityPenalties(t *testing.T) {
	now := time.Now().UTC()
	class := newLoopbackInboundClass(productionLoopbackInboundBounds(64), productionIdentityPenaltyPolicy(), func() time.Time { return now })
	peer := domain.PeerIdentity{1}
	class.identities.charge(peer, productionIdentityPenaltyPolicy().threshold)
	svc := &Service{loopbackInbound: class}
	svc.rebuildBanDiagnosticsSnapshot()
	raw, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	var reply struct {
		LoopbackIdentitiesScored int `json:"loopback_identities_scored"`
		LoopbackIdentitiesBanned int `json:"loopback_identities_banned"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.LoopbackIdentitiesScored != 1 || reply.LoopbackIdentitiesBanned != 1 {
		t.Fatalf("identity penalty hidden: %s", raw)
	}
}

func TestBanDiagnosticsFreshBarrierObservesMutationAndCachedReadIsExplicit(t *testing.T) {
	svc := &Service{banDiagnosticsRequests: make(chan banDiagnosticsRequest, 1), bans: map[string]banEntry{}}
	svc.rebuildBanDiagnosticsSnapshot()
	svc.ipStateMu.Lock()
	svc.bans["192.0.2.7"] = banEntry{Score: 3}
	svc.ipStateMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); svc.runBanDiagnosticsRefresh(ctx) }()
	awaitBanDiagnosticsWorker(t, svc)
	defer func() { cancel(); <-stopped }()
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	raw, err := svc.FreshBanDiagnostics(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Fresh     bool                 `json:"fresh"`
		Ready     bool                 `json:"ready"`
		LocalBans []localBanDiagnostic `json:"local_bans"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(err)
	}
	if !reply.Fresh || !reply.Ready || len(reply.LocalBans) != 1 {
		t.Fatalf("stale snapshot passed barrier: %s", raw)
	}
	cached, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(cached, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Fresh {
		t.Fatalf("cached read claimed barrier freshness: %s", cached)
	}
}

func TestBanDiagnosticsFreshBarrierCancelsWhenWriterBlocksRefresh(t *testing.T) {
	svc := &Service{banDiagnosticsRequests: make(chan banDiagnosticsRequest, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	svc.peerMu.Lock()
	go func() { defer close(stopped); svc.runBanDiagnosticsRefresh(ctx) }()
	awaitBanDiagnosticsWorker(t, svc)
	readCtx, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if _, err := svc.FreshBanDiagnostics(readCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("blocked read: %v", err)
	}
	svc.peerMu.Unlock()
	cancel()
	<-stopped
}

func TestBanDiagnosticsCachedReaderDoesNotTakeDomainLocks(t *testing.T) {
	svc := &Service{}
	svc.rebuildBanDiagnosticsSnapshot()
	svc.peerMu.Lock()
	svc.ipStateMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := svc.ListBannedJSON(); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("cached reader acquired a domain lock")
	}
	svc.ipStateMu.Unlock()
	svc.peerMu.Unlock()
}

func TestBanDiagnosticsEmptyBeforePrimingIsUnavailable(t *testing.T) {
	svc := &Service{}
	raw, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot banDiagnosticsSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Ready || snapshot.Fresh || snapshot.GeneratedAt != nil {
		t.Fatalf("unprimed read claimed observed empty state: %s", raw)
	}
	if _, err := svc.FreshBanDiagnostics(context.Background()); !errors.Is(err, errBanDiagnosticsNotRunning) {
		t.Fatalf("unstarted service accepted barrier: %v", err)
	}
}

func TestBanDiagnosticsAllSourcesExpireAndSortDeterministically(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)
	svc := &Service{
		bans:            map[string]banEntry{"192.0.2.2": {Score: 2}, "192.0.2.1": {Score: 1}, "192.0.2.0": {Blacklisted: past}},
		health:          map[domain.PeerAddress]*peerHealth{"192.0.2.3:1": {BannedUntil: future}, "192.0.2.4:1": {BannedUntil: past}, "192.0.2.5:1": {BannedUntil: future, LastErrorCode: protocol.ErrCodeSelfIdentity}},
		persistedMeta:   map[domain.PeerAddress]*peerEntry{"192.0.2.6:1": {RemoteBannedUntil: &future}, "192.0.2.7:1": {RemoteBannedUntil: &past}},
		bannedIPSet:     map[string]domain.BannedIPEntry{"192.0.2.8": {BannedUntil: future}, "192.0.2.9": {BannedUntil: past}},
		remoteBannedIPs: map[string]remoteIPBanEntry{"192.0.2.10": {Until: future}, "192.0.2.11": {Until: past}},
	}
	svc.rebuildBanDiagnosticsSnapshot()
	snapshot := svc.banDiagnosticsSnap.Load()
	if len(snapshot.LocalBans) != 2 || snapshot.LocalBans[0].IP != "192.0.2.1" || len(snapshot.PeerBans) != 2 || len(snapshot.RemotePeerBans) != 1 || len(snapshot.LocalIPBans) != 1 || len(snapshot.RemoteIPBans) != 1 {
		t.Fatalf("source coverage or ordering: %+v", snapshot)
	}
	if !snapshot.PeerBans[1].SelfIdentityCooldown {
		t.Fatal("self route cooldown was attributed as an ordinary peer ban")
	}
	first, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same immutable snapshot rendered nondeterministically")
	}
}

func TestReviewerFreshBanDiagnosticsMustFailDuringShutdown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	provider := NewPeerProvider(PeerProviderConfig{BannedIPsFn: func() map[string]domain.BannedIPEntry { close(entered); <-release; return nil }})
	svc := &Service{banDiagnosticsRequests: make(chan banDiagnosticsRequest, 1), peerProvider: provider, loopbackInbound: newLoopbackInboundClass(productionLoopbackInboundBounds(64), productionIdentityPenaltyPolicy(), time.Now)}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); svc.runBanDiagnosticsRefresh(workerCtx) }()
	awaitBanDiagnosticsWorker(t, svc)
	result := make(chan error, 1)
	go func() { _, err := svc.FreshBanDiagnostics(context.Background()); result <- err }()
	<-entered
	stopWorker()
	returned := false
	select {
	case err := <-result:
		returned = true
		if !errors.Is(err, errBanDiagnosticsNotRunning) {
			t.Errorf("shutdown fresh read: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("caller waited for blocked collection after shutdown")
	}
	close(release)
	if !returned {
		<-result
	}
	<-workerDone
	if _, err := svc.FreshBanDiagnostics(context.Background()); !errors.Is(err, errBanDiagnosticsNotRunning) {
		t.Errorf("stopped worker served fresh result: %v", err)
	}
}

func awaitBanDiagnosticsWorker(t *testing.T, svc *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for svc.banDiagnosticsLifecycle.Load() == nil {
		if ctx.Err() != nil {
			t.Fatal("ban diagnostic worker did not start")
		}
		runtime.Gosched()
	}
}

func TestBanDiagnosticsNewServiceBeforeRunAndAfterStop(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerEnabled: false, ListenerSet: true, Type: domain.NodeTypeClient})
	svc := NewService(cfg, id, nil)
	defer svc.runCancel()
	assertUnavailable := func(phase string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if _, err := svc.FreshBanDiagnostics(ctx); !errors.Is(err, errBanDiagnosticsNotRunning) {
			t.Fatalf("%s: got %v, want worker unavailable without waiting for read timeout", phase, err)
		}
	}
	assertUnavailable("before Run")
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- svc.Run(ctx) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-exited
		}
		svc.WaitBackground()
	}()
	awaitBanDiagnosticsWorker(t, svc)
	raw, err := svc.FreshBanDiagnostics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot banDiagnosticsSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Fresh || !snapshot.Ready || !snapshot.LoopbackIdentityPenaltiesAvailable {
		t.Errorf("running service failed full observation: %s", raw)
	}
	cancel()
	runErr := <-exited
	joined = true
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Error(runErr)
	}
	svc.WaitBackground()
	assertUnavailable("after Stop")
}
