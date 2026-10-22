package node

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Distinct fixture domain is minted BEFORE Service construction. No late degree,
// terminal or witness attachment and no production Ready/Seal are used here.
func ownedTerminalTestService(t *testing.T, maximum int) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	clock := new(atomic.Int64)
	d, err := findnodebridge.NewFixtureDomain(ctx.Done(), clock)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), degreeProfile{MaxActive: maximum})
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b})
	t.Cleanup(func() { cancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s, clock, d
}

func ownedTerminalOriginal(t *testing.T, s, dialer *Service) *proofTransportLifetime {
	t.Helper()
	wire, _ := degreeActualPair(t, s, dialer)
	l := wire.common.Load()
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.degreeWitness
	if l.bridgeTerminal != s.bridgeTerminal || w == nil || w.origin != l || w.owner != s.degree {
		t.Fatal("owned witness did not pass genuine production mint")
	}
	return l
}

func TestOwnedTerminalGenuineTrackedAndOverflowCleanup(t *testing.T) {
	s, clock, d := ownedTerminalTestService(t, 1)
	d1, _, _ := ownedTerminalTestService(t, 1)
	d2, _, _ := ownedTerminalTestService(t, 1)
	first := ownedTerminalOriginal(t, s, d1)
	second := ownedTerminalOriginal(t, s, d2)
	w1, w2 := first.degreeWitness, second.degreeWitness
	if w1.untracked || !w2.untracked || s.degree.unknown != 1 {
		t.Fatal("genuine tracked/overflow branches not reached")
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("constructing terminal root became Ready")
	}
	clock.Store(int64(2 * time.Second))
	second.end()
	first.end()
	if !first.ended || !second.ended || !w1.closed || !w2.closed || s.degree.active[0].life != nil || s.degree.unknown != 0 || s.degree.poisoned {
		t.Fatal("owned valid terminal did not retire exact units")
	}
	clock.Store(int64(3 * time.Second))
	first.end()
	second.end()
	if s.degree.last != 2*time.Second {
		t.Fatal("repeated End sampled or published again")
	}
}

func TestOwnedTerminalSameOwnerForeignOriginPreservesRejectedUnit(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(map[bool]string{false: "tracked", true: "overflow"}[overflow], func(t *testing.T) {
			maximum := 2
			if overflow {
				maximum = 1
			}
			s, _, _ := ownedTerminalTestService(t, maximum)
			d1, _, _ := ownedTerminalTestService(t, 1)
			d2, _, _ := ownedTerminalTestService(t, 1)
			a := ownedTerminalOriginal(t, s, d1)
			b := ownedTerminalOriginal(t, s, d2)
			wa, wb := a.degreeWitness, b.degreeWitness
			if wb.untracked != overflow {
				t.Fatal("wrong genuine B representation")
			}
			beforeUnknown, beforeHistory := s.degree.unknown, s.degree.history
			a.mu.Lock()
			a.degreeWitness = wb
			a.mu.Unlock() // explicit negative cross-attachment of TWO genuine mints
			a.end()
			if !a.ended || a.degreeWitness != nil || a.evidence != nil || !s.degree.poisoned || wb.closed || wa.closed || s.degree.unknown != beforeUnknown || s.degree.history != beforeHistory {
				t.Fatal("foreign origin mutated rejected witness/unit or skipped own cleanup")
			}
			if overflow {
				if s.degree.active[0].witness != wa {
					t.Fatal("foreign origin retired A slot")
				}
			} else if s.degree.active[1].witness != wb {
				t.Fatal("foreign origin retired B slot")
			}
		})
	}
}

func TestOwnedTerminalClockRefusalAndCancellationStillRetire(t *testing.T) {
	for _, q := range []int64{-1, math.MaxInt64} {
		t.Run(map[int64]string{-1: "negative", math.MaxInt64: "saturated"}[q], func(t *testing.T) {
			s, clock, _ := ownedTerminalTestService(t, 1)
			d, _, _ := ownedTerminalTestService(t, 1)
			l := ownedTerminalOriginal(t, s, d)
			w := l.degreeWitness
			history := s.degree.history
			clock.Store(q)
			s.runCancel()
			l.end()
			if !l.ended || !w.closed || s.degree.active[0].life != nil || !s.degree.poisoned || s.degree.history != history {
				t.Fatal("clock refusal skipped cleanup or invented history")
			}
		})
	}
}

func TestOwnedTerminalMalformedWitnessPoisonsExpectedOwner(t *testing.T) {
	for _, kind := range []string{"nil-origin", "closed", "generation", "unknown-underflow", "foreign-owner"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _ := ownedTerminalTestService(t, 1)
			d, _, _ := ownedTerminalTestService(t, 1)
			l := ownedTerminalOriginal(t, s, d)
			if kind == "unknown-underflow" {
				d2, _, _ := ownedTerminalTestService(t, 1)
				l = ownedTerminalOriginal(t, s, d2)
			}
			w := l.degreeWitness
			s.degree.mu.Lock()
			switch kind {
			case "nil-origin":
				w.origin = nil // negative only; no positive origin factory
			case "closed":
				w.closed = true
			case "generation":
				w.generation++
			case "unknown-underflow":
				s.degree.unknown = 0
			case "foreign-owner":
				w.owner = d.degree
			}
			beforeClosed, beforeUnknown, beforeHistory := w.closed, s.degree.unknown, s.degree.history
			s.degree.mu.Unlock()
			l.end()
			if !l.ended || !s.degree.poisoned || w.closed != beforeClosed || s.degree.unknown != beforeUnknown || s.degree.history != beforeHistory || d.degree.poisoned {
				t.Fatal("invalid witness changed foreign ownership or refused mandatory cleanup")
			}
		})
	}
}

func TestOwnedTerminalNilWitnessVersionsCommonCleanupWithoutClock(t *testing.T) {
	s, clock, _ := ownedTerminalTestService(t, 1)
	l := &proofTransportLifetime{token: s.proofToken, bridgeTerminal: s.bridgeTerminal, nin: &matureInboundWitness{group: domain.SourceGroupLoopback}}
	clock.Store(-1)
	l.end()
	if !l.ended || l.nin.phase != inboundTerminal || l.nin.group != "" || s.degree.poisoned || s.degree.last != 0 {
		t.Fatal("nil witness acquired fake degree clock or skipped common cleanup")
	}
}

func TestOwnedTerminalLegacyObserversFailBeforeOwnedWrites(t *testing.T) {
	s, _, _ := ownedTerminalTestService(t, 1)
	o := s.degree
	o.mu.Lock()
	last, history := o.last, o.history
	if _, ok := o.degreeNowLocked(); ok {
		t.Fatal("owned owner entered legacy clock")
	}
	if _, _, ok := sampleDegreeStatisticClockLocked(o); ok {
		t.Fatal("owned owner entered legacy statistic clock")
	}
	if _, _, _, err := projectDegreeStatisticLocked(o, 0); !errors.Is(err, errDegreeUnavailable) {
		t.Fatal("owned owner entered legacy projection")
	}
	if o.last != last || o.history != history || o.poisoned {
		t.Fatal("legacy refusal modified owned authority")
	}
	o.mu.Unlock()
	if _, err := o.degreeSnapshot(); !errors.Is(err, errDegreeUnavailable) {
		t.Fatal("owned owner returned a legacy complete cut")
	}
	if _, err := s.captureOverlayDWinForFlush(); !errors.Is(err, errDegreeUnavailable) || s.degreeStatisticProducer != nil {
		t.Fatal("Constructing root installed statistics or fake zero")
	}
}
