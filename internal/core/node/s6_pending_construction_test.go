package node

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func pendingConstructionKey() PendingObservationKey {
	return PendingObservationKey{Anchor: "anchor-a", ArtifactSHA256: strings.Repeat("a", 64),
		Boot: "boot-a", ProcessInstance: "instance-a", ClockDomain: "boot-duration-a"}
}

func pendingConstructionInputs(t *testing.T) (config.Node, *identity.Identity) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	return config.Node{ListenAddress: "127.0.0.1:64646", Type: config.NodeTypeFull,
		AllowPrivatePeers: true, PendingRingSize: 1, ChatLogDir: dir,
		IdentityPath: filepath.Join(dir, "identity.json"), TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"), IdentityIntentsPath: filepath.Join(dir, "intents.json"),
		SecureSessionStorePath: filepath.Join(dir, "secure.json")}, id
}

type pendingConstructionClock struct {
	mu    sync.Mutex
	at    time.Duration
	err   error
	calls int
}

func (c *pendingConstructionClock) read() (time.Duration, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.at, c.err
}
func (c *pendingConstructionClock) set(at time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at, c.err = at, err
}

func constructPendingService(t *testing.T, key PendingObservationKey, clock func() (time.Duration, error)) (*Service, *PendingObservation) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	s, o, err := newServiceWithPendingObservation(cfg, id, nil, key, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newUnstartedRuntimeCleanup(s, s.runCancel)(); s.WaitBackground() })
	return s, o
}

func TestS6PendingConstructionActualOwners(t *testing.T) {
	clock := &pendingConstructionClock{}
	s, o := constructPendingService(t, pendingConstructionKey(), clock.read)
	initial := o.Snapshot()
	if !initial.Available || !initial.CoverageValid || initial.Count != 0 || initial.Sequence != 0 || initial.AttributionBound {
		t.Fatalf("unpublished baseline = %+v", initial)
	}
	if got := s.PendingQueueObservation(); got.SourceID != initial.SourceID {
		t.Fatal("handle is not bound to the actual Service source")
	}
	addr := domain.PeerAddress("10.80.0.8:64646")
	frame := s6PendingFrame(s, "construction-one")
	clock.set(time.Second, nil)
	if !s.queuePeerFrame(addr, frame) {
		t.Fatal("first actual queue admission refused")
	}
	first := o.Snapshot()
	if first.Count != 1 || first.Sequence != 1 || !first.CoverageValid || !first.RunHasIncrease {
		t.Fatalf("first mutation was not covered: %+v", first)
	}
	clock.set(2*time.Second, nil)
	frame.Body = "replacement"
	if !s.queuePeerFrame(addr, frame) {
		t.Fatal("same-key replacement refused")
	}
	frame.ID = "construction-two"
	if !s.queuePeerFrame(addr, frame) {
		t.Fatal("ring replacement refused")
	}
	replaced := o.Snapshot()
	if !replaced.CoverageValid || replaced.Count != 1 || replaced.Sequence != 3 || replaced.RunStart != first.RunStart {
		t.Fatalf("net-zero owner replaced the run: %+v", replaced)
	}
	clock.set(3*time.Second, nil)
	result, err := s.CancelOutgoingDelivery(protocol.MessageID(frame.ID), idTargetX)
	if err != nil || result.PendingFrames != 1 {
		t.Fatalf("actual cancellation = %+v, %v", result, err)
	}
	end := o.Snapshot()
	if !end.CoverageValid || end.Count != 0 || end.Sequence != 4 || end.RunHasIncrease || end.AttributionBound {
		t.Fatalf("actual removal not observed: %+v", end)
	}
	if first.Count != 1 || first.Sequence != 1 || first.SourceID != end.SourceID {
		t.Fatal("a later read mutated the prior snapshot or replaced the source")
	}
}

func TestS6PendingConstructionKeyAndIsolation(t *testing.T) {
	key := pendingConstructionKey()
	clock := &pendingConstructionClock{}
	s, o := constructPendingService(t, key, clock.read)
	key.Anchor = "changed"
	copyKey := o.Key()
	copyKey.ProcessInstance = "changed"
	if o.Key() != pendingConstructionKey() || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
		t.Fatal("mutable or falsely attributed construction key")
	}
	otherKey := pendingConstructionKey()
	otherKey.ProcessInstance = "instance-b"
	other, otherReadout := constructPendingService(t, otherKey, clock.read)
	if !s.queuePeerFrame("10.80.0.9:64646", s6PendingFrame(s, "isolated")) {
		t.Fatal("enqueue refused")
	}
	a, b := o.Snapshot(), otherReadout.Snapshot()
	if a.Count != 1 || b.Count != 0 || a.SourceID == b.SourceID || !a.CoverageValid || !b.CoverageValid || a.AttributionBound || b.AttributionBound {
		t.Fatalf("source isolation lost: %+v / %+v", a, b)
	}
	if other.PendingQueueObservation().SourceID != b.SourceID {
		t.Fatal("second handle does not read its own Service")
	}
}

func TestS6PendingConstructionRejectsInvalidKey(t *testing.T) {
	for _, name := range []string{"empty", "oversized", "control", "digest", "zero-digest"} {
		t.Run(name, func(t *testing.T) {
			key := pendingConstructionKey()
			switch name {
			case "empty":
				key.Boot = ""
			case "oversized":
				key.Anchor = strings.Repeat("x", 129)
			case "control":
				key.ClockDomain = "boot\n"
			case "digest":
				key.ArtifactSHA256 = strings.Repeat("A", 64)
			case "zero-digest":
				key.ArtifactSHA256 = strings.Repeat("0", 64)
			}
			cfg, id := pendingConstructionInputs(t)
			calls := 0
			s, o, err := newServiceWithPendingObservation(cfg, id, nil, key, func() (time.Duration, error) { calls++; return 0, nil })
			if err == nil || s != nil || o != nil || calls != 0 {
				t.Fatalf("invalid key constructed a source: %p/%p/%v, reads%d", s, o, err, calls)
			}
		})
	}
}

func TestS6PendingConstructionInitialUnavailable(t *testing.T) {
	for _, name := range []string{"nil", "error", "regression"} {
		t.Run(name, func(t *testing.T) {
			var read func() (time.Duration, error)
			calls := 0
			if name != "nil" {
				read = func() (time.Duration, error) {
					calls++
					if name == "error" {
						return 0, errors.New("unavailable")
					}
					return time.Duration(3 - calls), nil
				}
			}
			s, o := constructPendingService(t, pendingConstructionKey(), read)
			before := calls
			if !s.queuePeerFrame("10.80.0.10:64646", s6PendingFrame(s, "unavailable")) {
				t.Fatal("measurement initialization changed queue admission")
			}
			v := o.Snapshot()
			if v.Available || v.CoverageValid || v.AttributionBound || v.UnknownReason != "initial_source_unavailable" || calls != before {
				t.Fatalf("unavailable source revived or claimed zero coverage: %+v, reads%d->%d", v, before, calls)
			}
			if s.StartupRefusal() != nil {
				t.Fatal("observation failure became a startup refusal")
			}
		})
	}
}

func TestS6PendingConstructionLaterClockLoss(t *testing.T) {
	for _, name := range []string{"error", "regression"} {
		t.Run(name, func(t *testing.T) {
			clock := &pendingConstructionClock{at: time.Second}
			s, o := constructPendingService(t, pendingConstructionKey(), clock.read)
			if name == "error" {
				clock.set(0, errors.New("lost"))
			} else {
				clock.set(0, nil)
			}
			if !s.queuePeerFrame("10.80.0.11:64646", s6PendingFrame(s, "clock-loss")) {
				t.Fatal("clock loss changed actual admission")
			}
			v := o.Snapshot()
			if !v.Available || v.CoverageValid || v.DurationValid || !v.CountValid || v.Count != 1 || v.UnknownReason == "" || v.AttributionBound {
				t.Fatalf("clock loss accepted as covered: %+v", v)
			}
			clock.set(2*time.Second, nil)
			if o.Snapshot().CoverageValid {
				t.Fatal("later clock success bridged a gap")
			}
		})
	}
}

func TestS6PendingConstructionOrdinaryInactive(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	s := NewService(cfg, id, nil)
	t.Cleanup(func() { newUnstartedRuntimeCleanup(s, s.runCancel)(); s.WaitBackground() })
	if s.pendingObservation != nil {
		t.Fatal("ordinary construction installed observation")
	}
	if !s.queuePeerFrame("10.80.0.12:64646", s6PendingFrame(s, "ordinary")) {
		t.Fatal("ordinary admission changed")
	}
	if v := s.PendingQueueObservation(); v.Available || v.CoverageValid || v.UnknownReason != "source_unavailable" {
		t.Fatalf("ordinary source = %+v", v)
	}
	var absent *PendingObservation
	if absent.Snapshot().Available || absent.Key() != (PendingObservationKey{}) || !absent.Identity().IsZero() {
		t.Fatal("nil handle fabricated evidence")
	}
}
