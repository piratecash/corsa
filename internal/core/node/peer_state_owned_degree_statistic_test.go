package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
)

func ownedStatisticTestService(t *testing.T, prior overlayDegreeStatistic) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	if prior.valid {
		if err := savePeerState(cfg.EffectivePeersStatePath(), peerStateFile{Version: peerStateVersion, OverlayDWin: overlayDegreeHint{stat: prior}}); err != nil {
			t.Fatal(err)
		}
	}
	s, err := newFindNodeTerminalStatisticCandidateService(cfg, id, nil, degreeProfile{MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}

func TestOwnedStatisticRuntimeShortWindowPreservesPriorWithGenuineP1(t *testing.T) {
	prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), valid: true}
	s := ownedStatisticTestService(t, prior)
	d := ownedStatisticTestService(t, overlayDegreeStatistic{})
	l := ownedTerminalOriginal(t, s, d)
	if l.degreeWitness == nil {
		t.Fatal("owned statistic genuine P1 absent")
	}
	p := s.ownedDegreeStatisticProducer
	p.owner.mu.Lock()
	installed, start := p.installed, p.coverageStart
	p.owner.mu.Unlock()
	if !installed || p.owner != s.degree || p.publisher != s.bridgeTerminal.publisher || s.degreeStatisticProducer != nil {
		t.Fatal("owned statistic early binding mismatch")
	}
	got, err := s.captureOverlayDWinForFlush()
	if !errors.Is(err, errDegreeUnavailable) || got.valid {
		t.Fatal("short runtime window became complete", got, err)
	}
	p.owner.mu.Lock()
	last := p.owner.last
	p.owner.mu.Unlock()
	if last < start.Elapsed() {
		t.Fatal("partial owned sample lost HWM")
	}
	s.flushPeerState()
	loaded, err := loadPeerState(s.peersStatePath)
	if err != nil || !loaded.OverlayDWin.stat.valid || loaded.OverlayDWin.stat.value != 16 || !loaded.OverlayDWin.stat.at.Equal(prior.at) {
		t.Fatal("owned partial flush changed original prior tuple", err)
	}
}

func TestOwnedStatisticShortRestartsNeverRefreshPrior(t *testing.T) {
	prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second), valid: true}
	for pass := 0; pass < 3; pass++ {
		s := ownedStatisticTestService(t, prior)
		s.flushPeerState()
		loaded, err := loadPeerState(s.peersStatePath)
		if err != nil || !loaded.OverlayDWin.stat.valid || loaded.OverlayDWin.stat.value != 16 || !loaded.OverlayDWin.stat.at.Equal(prior.at) {
			t.Fatal("short owned restart refreshed or zeroed prior", pass, err)
		}
	}
}

type ownedStatisticInspectConstructor struct {
	t     *testing.T
	early findnodebridge.OwnedInstant
}

func (c *ownedStatisticInspectConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	p := s.ownedDegreeStatisticProducer
	if p == nil || p.owner != s.degree || p.publisher != s.bridgeTerminal.publisher {
		c.t.Fatal("callback saw missing early owned binding")
	}
	if got, err := s.captureOverlayDWinForFlush(); !errors.Is(err, errDegreeUnavailable) || got.valid {
		c.t.Fatal("callback received pre-install statistic")
	}
	p.owner.mu.Lock()
	if p.installed {
		p.owner.mu.Unlock()
		c.t.Fatal("observer installed before constructor callback")
	}
	publication := p.publisher.BeginClockedPublication()
	point, _, err := publication.SampleRuntimePair()
	publication.Complete()
	p.owner.mu.Unlock()
	if err != nil {
		c.t.Fatal(err)
	}
	c.early = point
	return nil, nil
}

func TestOwnedStatisticInstallFollowsConstructorCallbackAndStaysOnce(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	b, err := prepareFindNodeTerminalConstruction(degreeProfile{MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	b.statistic = &ownedDegreeStatisticProducer{owner: b.owner, publisher: b.terminal.publisher} // before FIRST Service literal
	constructor := &ownedStatisticInspectConstructor{t: t}
	s := newServiceWithConstruction(cfg, id, nil, nil, constructor, serviceConstruction{bridge: b})
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	p := s.ownedDegreeStatisticProducer
	p.owner.mu.Lock()
	start, installed := p.coverageStart, p.installed
	p.owner.mu.Unlock()
	if !b.statisticInstalled || !installed || !start.NotBefore(constructor.early) {
		t.Fatal("coverage backdated into constructor work")
	}
	if s.installOwnedDegreeStatistic() {
		t.Fatal("owned observer installed twice")
	}
	p.owner.mu.Lock()
	unchanged := p.coverageStart == start
	p.owner.mu.Unlock()
	if !unchanged {
		t.Fatal("second install retimed original start")
	}
}

// These tests are explicitly fixture projection/arithmetic, NOT evidence of a
// completed600-second runtime observation. No runtime origin is moved.
func TestOwnedStatisticFixtureProjectionUsesGenuineMintAndIsPure(t *testing.T) {
	s, clock, _ := ownedTerminalTestService(t, 2)
	d, _, _ := ownedTerminalTestService(t, 1)
	l := ownedTerminalOriginal(t, s, d)
	clock.Store(int64(700 * time.Second))
	o := s.degree
	o.mu.Lock()
	publication := o.bridgePublisher.BeginClockedPublication()
	point, err := publication.Sample()
	if err != nil {
		publication.Complete()
		o.mu.Unlock()
		t.Fatal(err)
	}
	o.last = point.Elapsed()
	history, n := o.history, o.historyN
	out, in, window, state := projectOwnedDegreeStatisticLocked(o, point.Elapsed())
	pure := o.history == history && o.historyN == n && !o.poisoned
	publication.Complete()
	o.mu.Unlock()
	if state != ownedDegreeComplete || out != 0 || in != 1 || window != 1 || !pure || l.degreeWitness.origin != l {
		t.Fatal("owned fixture pure projection incomplete", out, in, window, state)
	}
}

func TestOwnedStatisticFixtureFaultAndUnknownDoNotMutateOwner(t *testing.T) {
	for _, kind := range []string{"unknown", "origin", "future-history", "empty-row-witness"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _ := ownedTerminalTestService(t, 1)
			d, _, _ := ownedTerminalTestService(t, 1)
			l := ownedTerminalOriginal(t, s, d)
			o := s.degree
			o.mu.Lock()
			switch kind {
			case "unknown":
				o.unknown = 1
			case "origin":
				l.degreeWitness.origin = nil
			case "future-history":
				o.history[0].last = time.Second
			case "empty-row-witness":
				o.active[0].life = nil
			}
			history, historyN, last, unknown, poison := o.history, o.historyN, o.last, o.unknown, o.poisoned
			_, _, _, state := projectOwnedDegreeStatisticLocked(o, 0)
			unchanged := o.history == history && o.historyN == historyN && o.last == last && o.unknown == unknown && o.poisoned == poison
			o.mu.Unlock()
			want := ownedDegreeFault
			if kind == "unknown" {
				want = ownedDegreeUnavailable
			}
			if state != want || !unchanged {
				t.Fatal("pure owned refusal changed semantic authority", kind, state)
			}
		})
	}
}

func TestOwnedStatisticFixtureZeroAndExpiryAreNotRuntimeCoverage(t *testing.T) {
	s, _, _ := ownedTerminalTestService(t, 1)
	o := s.degree
	o.mu.Lock()
	_, _, window, state := projectOwnedDegreeStatisticLocked(o, degreeRecent)
	o.mu.Unlock()
	if state != ownedDegreeComplete || window != 0 {
		t.Fatal("empty owned projection did not yield genuine fixture zero")
	}
	if degreeStatisticCoverageReady(1, degreeRecent) || !degreeStatisticCoverageReady(1, degreeRecent+1) {
		t.Fatal("full600 boundary arithmetic changed")
	}
	now := time.Now().UTC().Truncate(time.Second)
	prior := overlayDegreeStatistic{value: 16, at: now.Add(-23 * time.Hour), valid: true}
	if got := selectOverlayDegreeStatistic(prior, overlayDegreeStatistic{}, now); got != prior {
		t.Fatal("unavailable refreshed prior tuple")
	}
	if freshOverlayDegreeStatistic(prior, now.Add(2*time.Hour)).valid {
		t.Fatal("old hint survived expiry during observation")
	}
	zero := overlayDegreeStatistic{value: 0, at: now, valid: true} // isolated selector DTO, not runtime authority
	if got := selectOverlayDegreeStatistic(prior, zero, now); !got.valid || got.value != 0 || !got.at.Equal(now) {
		t.Fatal("mature selector zero did not replace prior")
	}
}

func TestOwnedStatisticFixtureTerminalHistoryHalfOpenBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		q    time.Duration
		want int
	}{{"before", degreeRecent - 1, 1}, {"exact", degreeRecent, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			s, clock, _ := ownedTerminalTestService(t, 1)
			d, _, _ := ownedTerminalTestService(t, 1)
			l := ownedTerminalOriginal(t, s, d)
			l.end() // genuine terminal event at fixture0
			clock.Store(int64(tc.q))
			o := s.degree
			o.mu.Lock()
			publication := o.bridgePublisher.BeginClockedPublication()
			point, err := publication.Sample()
			if err != nil {
				publication.Complete()
				o.mu.Unlock()
				t.Fatal(err)
			}
			o.last = point.Elapsed()
			history, n := o.history, o.historyN
			_, _, window, state := projectOwnedDegreeStatisticLocked(o, point.Elapsed())
			unchanged := o.history == history && o.historyN == n
			publication.Complete()
			o.mu.Unlock()
			if state != ownedDegreeComplete || window != tc.want || !unchanged {
				t.Fatal("owned fixture half-open terminal projection", window, state)
			}
		})
	}
}
