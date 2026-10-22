package node

import (
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readyTestInputs(t *testing.T) (config.Node, *identity.Identity) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	cfg.TrustStorePath = filepath.Join(t.TempDir(), "trust.json")
	cfg.SecureSessionStorePath = filepath.Join(t.TempDir(), "pins.json")
	return cfg, id
}
func readyTestPrepare(t *testing.T, cfg config.Node, id *identity.Identity) *findNodeReadyConstruction {
	t.Helper()
	r, err := prepareFindNodeReadyConstruction(cfg, id, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	return r
}

type readyInspectConstructor struct {
	t      *testing.T
	r      *findNodeReadyConstruction
	mode   string
	called bool
	joined chan struct{}
}

func (c *readyInspectConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	c.called = true
	r := c.r
	if s != r.service || s.trust != r.trust || s.bridgeCapture != r.bridge.capture || *s.bridgeCapture != r.originalCapture || s.ownedDegreeStatisticProducer != r.originalStatistic || s.ownedDegreeStatisticProducer.installed || s.secureSessions != nil || r.finalized {
		c.t.Fatal("original construction escaped before callback bindings")
	}
	if _, err := s.bridgeCapture.observer.BeginCapture(); err == nil {
		c.t.Fatal("builder observed premature Ready")
	}
	if c.joined != nil {
		if !s.goBackground(func() { <-s.runCtx.Done(); close(c.joined) }) {
			c.t.Fatal("constructor cleanup fixture did not start")
		}
	}
	switch c.mode {
	case "cancel":
		r.originalCancel()
	case "panic":
		panic("ready constructor callback panic")
	case "foreign-consumer":
		otherCfg, otherID := readyTestInputs(c.t)
		other := readyTestPrepare(c.t, otherCfg, otherID)
		s.bridgeCapture.consumer = other.bridge.capture.consumer
	}
	return nil, nil
}
func TestOwnedReadyRuntimeFinalizationAndOriginalGenuineCut(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	callback := &readyInspectConstructor{t: t, r: r}
	s, err := r.build(cfg, id, nil, callback)
	if err != nil || s == nil || !callback.called || !r.finalized || !r.checked || !r.bridge.statisticInstalled || !s.ownedDegreeStatisticProducer.installed {
		t.Fatal("coherent genuine Runtime did not finalize", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	if !s.bridgeCapture.consumer.IsRuntime() || s.secureSessions.store != r.store || s.trust != r.trust {
		t.Fatal("Ready replaced original stores/roles")
	}
	if _, err := s.captureOwnedFindNodeCut(context.Background()); err != nil {
		t.Fatal("Ready Runtime complete empty cut refused", err)
	}
	// Real TLS/session proofs retain the original Runtime domain; no FixtureDomain
	// or test-only Seal/caller instant participates in this positive path.
	remoteCfg, remoteID := readyTestInputs(t)
	remote, err := newFindNodeReadyCandidateService(remoteCfg, remoteID, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	remote.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { remote.runCancel(); remote.closeBackgroundAdmission(); remote.WaitBackground() })
	original, _ := ownedRawAccepted(t, s, remote)
	if _, err := s.captureOwnedFindNodeCut(context.Background()); err != nil {
		t.Fatal("Ready genuine original member cut refused", err)
	}
	request := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request)); err != nil {
		t.Fatal("Ready original local admission refused", err)
	}
	answer := ownedLocalNodeReadAnswer(t, original)
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
		t.Fatal("Ready original local T/sign/answer Finish lost exact request", err)
	}
	waitFor(t, 2*time.Second, "Ready original local slot released after Complete/answer writer", func() bool { return len(s.bridgeCapture.localLoop.slots) == 0 })
	if _, err := r.build(cfg, id, nil, callback); err == nil || s.runCtx.Err() != nil {
		t.Fatal("construction replay changed live original lifecycle")
	}
	if err := r.finalize(s); err == nil {
		t.Fatal("Ready finalized twice")
	}
}
func TestOwnedReadyCancellationAndPanicJoinOriginalLifecycle(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "foreign-consumer"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			r := readyTestPrepare(t, cfg, id)
			joined := make(chan struct{})
			callback := &readyInspectConstructor{t: t, r: r, mode: mode, joined: joined}
			panicked := false
			func() {
				defer func() {
					if v := recover(); v != nil {
						if v != "ready constructor callback panic" {
							panic(v)
						}
						panicked = true
					}
				}()
				s, err := r.build(cfg, id, nil, callback)
				if s != nil || err == nil {
					t.Fatal("failed Ready returned usable Service")
				}
			}()
			if panicked != (mode == "panic") || r.originalContext.Err() == nil || r.finalized || r.bridge.statisticInstalled {
				t.Fatal("failed Ready lifecycle/install state", mode)
			}
			select {
			case <-joined:
			default:
				t.Fatal("Ready error/panic did not join original callback worker")
			}
			if r.service == nil || r.service.goBackground(func() {}) {
				t.Fatal("Ready cleanup left background admission open")
			}
		})
	}
}
func TestOwnedReadyOriginalStoreAndFinalizationOrderStayExact(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	if err := r.finalize(nil); err == nil {
		t.Fatal("Ready allowed finalization before Service/store installation")
	}
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: r.bridge, ready: r})
	if !r.checked || r.failure != nil || !r.bridge.statisticInstalled {
		t.Fatal("original preinstall checks missing", r.failure)
	}
	original := s.trust
	replacement, err := loadTrustStoreForConstruction("", trustedContact{}, r.bridge)
	if err != nil {
		t.Fatal(err)
	}
	if replacement == original || replacement.trustPublisher != original.trustPublisher {
		t.Fatal("same-domain replacement fixture invalid")
	}
	s.trust = replacement
	if err := r.finalize(s); err == nil {
		t.Fatal("same-domain replacement store passed original binding check")
	}
	s.trust = original
	r.originalCancel()
	if err := r.finalize(s); err == nil || r.finalized {
		t.Fatal("canceled lifecycle became usable Ready")
	}
}
func TestOwnedReadyDecodedHintPreservesOriginalDateAndNoSecondRead(t *testing.T) {
	cfg, id := readyTestInputs(t)
	prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), valid: true}
	if err := savePeerState(cfg.EffectivePeersStatePath(), peerStateFile{Version: peerStateVersion, OverlayDWin: overlayDegreeHint{stat: prior}}); err != nil {
		t.Fatal(err)
	}
	r := readyTestPrepare(t, cfg, id)
	if err := os.WriteFile(cfg.EffectivePeersStatePath(), []byte("{broken after sole decode"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil {
		t.Fatal(err)
	}
	if s.overlayDWin != prior || r.peerState.OverlayDWin.stat != prior {
		t.Fatal("Ready reread or renewed original hint tuple/date")
	}
	if stat, err := s.captureOwnedOverlayDWinForFlush(); !errors.Is(err, errDegreeUnavailable) || stat.valid {
		t.Fatal("partial observation replaced original hint")
	}
	s.flushPeerState()
	loaded, err := loadPeerState(cfg.EffectivePeersStatePath())
	if err != nil || loaded.OverlayDWin.stat != prior {
		t.Fatal("partial Ready flush refreshed original hint date", err)
	}
	// Explicit empty preloaded state also avoids a later disk value appearing.
	emptyCfg, emptyID := readyTestInputs(t)
	empty := readyTestPrepare(t, emptyCfg, emptyID)
	if err := savePeerState(emptyCfg.EffectivePeersStatePath(), peerStateFile{Version: peerStateVersion, OverlayDWin: overlayDegreeHint{stat: prior}}); err != nil {
		t.Fatal(err)
	}
	es, err := empty.build(emptyCfg, emptyID, nil, productionDatagramConstructor{})
	if err != nil || es.overlayDWin.valid {
		t.Fatal("empty decoded input was treated as missing and reloaded", err)
	}
}
