package node

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
)

func connectionTotalConstructionKey() ConnectionTotalObservationKey {
	return ConnectionTotalObservationKey{Anchor: "total-anchor", ArtifactSHA256: strings.Repeat("b", 64), Boot: "total-boot", ProcessInstance: "total-process", Epoch: [16]byte{131}, ClockDomain: [16]byte{132}, Configuration: [16]byte{133}}
}
func connectionTotalConfig(t *testing.T) config.Node {
	t.Helper()
	cfg, _ := pendingConstructionInputs(t)
	cfg.MaxTotalConnections = 3
	cfg.MaxOutgoingPeers = 1
	cfg.MaxIncomingPeers = 2
	cfg.MaxAuxiliaryConnections = 1
	return cfg
}
func connectionTotalService(t *testing.T, cfg config.Node, key ConnectionTotalObservationKey, capacity int, clock func() (time.Duration, error)) (*Service, *ConnectionTotalObservation) {
	t.Helper()
	_, id := pendingConstructionInputs(t)
	request := ConnectionTotalObservationRequest{Key: key, Capacity: capacity}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotal: &request}, clock)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if r.ConnectionTotal == nil || r.ConnectionTotal.service != s || r.ConnectionTotal.budget != s.connBudget || s.connManager.config.Budget != s.connBudget {
		t.Fatal("readout is not CM's original Service budget")
	}
	return s, r.ConnectionTotal
}
func connectionTotalTake(t *testing.T, b *connbudget.Budget, d connbudget.Direction) *connbudget.Reservation {
	t.Helper()
	r, err := b.Reserve(d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Release)
	return r
}
func TestS6ConnectionTotalConstructionActualBudget(t *testing.T) {
	d := &pendingConstructionClock{}
	cfg := connectionTotalConfig(t)
	s, o := connectionTotalService(t, cfg, connectionTotalConstructionKey(), 8, d.read)
	base := o.Snapshot()
	want := connbudget.Config{Total: 3, OutboundReserve: 1, MaxOutbound: 1, MaxInbound: 2, MaxAuxiliary: 1}
	if !base.Available || base.AttributionBound || base.SchemaVersion != 1 || base.Status != "COMPLETE" || base.Config != want || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
		t.Fatal("actual five-field config/source/identity lost", base)
	}
	out, err := s.connManager.reserveOutbound()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(out.Release)
	in := connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
	d.set(2*time.Second, nil)
	aux := connectionTotalTake(t, s.connBudget, connbudget.DirectionAuxiliary)
	full := o.Snapshot()
	if !full.Cut.Full || full.Cut.Sequence != 1 || len(full.Rows) != 1 || full.Rows[0].Count != 3 || full.Rows[0].Lower != 2*time.Second || full.Rows[0].Upper != 2*time.Second {
		t.Fatal("same CM/shared owner did not reach total full", full)
	}
	if _, err := s.connBudget.Reserve(connbudget.DirectionInbound); !errors.Is(err, connbudget.ErrTotalExhausted) {
		t.Fatal(err)
	}
	if _, err := s.connManager.reserveOutbound(); !errors.Is(err, connbudget.ErrDirectionLimit) {
		t.Fatal("original direction-before-total precedence changed", err)
	}
	d.set(4*time.Second, nil)
	in.Release()
	d.mu.Lock()
	calls := d.calls
	d.mu.Unlock()
	in.Release()
	d.mu.Lock()
	afterCalls := d.calls
	d.mu.Unlock()
	if afterCalls != calls {
		t.Fatal("double release changed original observation CAS")
	}
	after := o.Snapshot()
	if after.Cut.Full || after.Cut.Sequence != 2 || after.Rows[1].Count != 2 || after.Rows[1].Lower != 4*time.Second || after.Rows[1].Upper != 4*time.Second {
		t.Fatal("first release did not free full total", after)
	}
	if got := o.source.Snapshot(); !reflect.DeepEqual(got, after.TotalObservationSnapshot) {
		t.Fatal("Node readout changed exported raw snapshot", got, after)
	}
	full.Rows[0].Count = 999
	key := o.Key()
	key.Epoch[0] = 0
	if got := o.Snapshot(); got.Rows[0].Count != 3 || o.Key() != connectionTotalConstructionKey() {
		t.Fatal("Node readout aliases caller/source")
	}
	fresh := connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
	fresh.Release()
	out.Release()
	aux.Release()
}
func TestS6ConnectionTotalConstructionRefusalIsNotFull(t *testing.T) {
	for _, name := range []string{"direction", "protected"} {
		t.Run(name, func(t *testing.T) {
			cfg := connectionTotalConfig(t)
			if name == "protected" {
				cfg.MaxOutgoingPeers = 2
			}
			d := &pendingConstructionClock{}
			s, o := connectionTotalService(t, cfg, connectionTotalConstructionKey(), 8, d.read)
			if name == "direction" {
				r, e := s.connManager.reserveOutbound()
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(r.Release)
				if _, e = s.connManager.reserveOutbound(); !errors.Is(e, connbudget.ErrDirectionLimit) {
					t.Fatal(e)
				}
			} else {
				_ = connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
				if _, e := s.connBudget.Reserve(connbudget.DirectionAuxiliary); !errors.Is(e, connbudget.ErrOutboundReserved) {
					t.Fatal(e)
				}
			}
			v := o.Snapshot()
			if v.Status != "COMPLETE" || v.Cut.Full || v.Cut.Sequence != 0 || len(v.Rows) != 0 || s.connBudget.Snapshot().Used != 1 {
				t.Fatal("spare-total direction/reserve refusal fabricated fullness", v)
			}
		})
	}
}
func TestS6ConnectionTotalConstructionDisabledInvalidAndDefaults(t *testing.T) {
	t.Run("disabled-no-clock", func(t *testing.T) {
		cfg := connectionTotalConfig(t)
		cfg.MaxTotalConnections = 0
		key := connectionTotalConstructionKey()
		key.Epoch = [16]byte{}
		key.ClockDomain = [16]byte{}
		key.Configuration = [16]byte{}
		s, o := connectionTotalService(t, cfg, key, int(^uint(0)>>1), func() (time.Duration, error) { t.Fatal("disabled budget read observation clock"); return 0, nil })
		r, err := s.connManager.reserveOutbound()
		if err != nil {
			t.Fatal(err)
		}
		r.Release()
		v := o.Snapshot()
		if v.Status != "DISABLED" || v.Reason != "total-disabled" || !v.PastOnly || v.Cut.Available || v.Rows != nil || v.Config.Total != 0 || s.StartupRefusal() != nil {
			t.Fatal("disabled total produced zero evidence or changed startup", v)
		}
	})
	t.Run("original-invalid-config", func(t *testing.T) {
		cfg := connectionTotalConfig(t)
		cfg.MaxTotalConnections = -1
		s, o := connectionTotalService(t, cfg, connectionTotalConstructionKey(), 8, func() (time.Duration, error) { t.Fatal("invalid config read provider"); return 0, nil })
		if !errors.Is(s.connBudgetErr, connbudget.ErrInvalidConfig) || !errors.Is(s.StartupRefusal(), connbudget.ErrInvalidConfig) || s.connBudget != nil || o.Snapshot().Available || o.Snapshot().Status != "UNKNOWN" {
			t.Fatal("original invalid configuration lost startup refusal", s.connBudgetErr, o.Snapshot())
		}
	})
	t.Run("ordinary-and-nils", func(t *testing.T) {
		cfg := connectionTotalConfig(t)
		_, id := pendingConstructionInputs(t)
		s, readouts, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{}, func() (time.Duration, error) { t.Fatal("ordinary observation clock called"); return 0, nil })
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		if readouts.ConnectionTotal != nil {
			t.Fatal("ordinary constructor installed source")
		}
		var missing *ConnectionTotalObservation
		if v := missing.Snapshot(); v.Available || v.Status != "INACTIVE" || v.AttributionBound || missing.Key() != (ConnectionTotalObservationKey{}) || missing.Identity() != (domain.PeerIdentity{}) {
			t.Fatal("nil handle fabricated evidence", v)
		}
		if bad, readouts, e := NewServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotal: &ConnectionTotalObservationRequest{Capacity: 8}}); e == nil || bad != nil || readouts.ConnectionTotal != nil {
			t.Fatal("malformed caller references accepted")
		}
	})
}
func TestS6ConnectionTotalConstructionUnknownKeepsAdmission(t *testing.T) {
	for _, name := range []string{"tokens", "capacity", "capacity-arithmetic", "initial-clock", "nil-clock", "later-clock", "gap"} {
		t.Run(name, func(t *testing.T) {
			d := &pendingConstructionClock{}
			key := connectionTotalConstructionKey()
			capacity := 8
			clock := d.read
			want := "clock-error"
			switch name {
			case "tokens":
				key.Epoch = [16]byte{}
				want = "invalid-source-identity"
			case "capacity":
				capacity = 0
				want = "invalid-capacity"
			case "capacity-arithmetic":
				capacity = int(^uint(0) >> 1)
				want = "invalid-capacity"
			case "initial-clock":
				d.set(0, errors.New("missing duration"))
			case "nil-clock":
				clock = nil
				want = "unsupported-clock"
			case "gap":
				capacity = 1
				want = "transition-capacity-gap"
			}
			s, o := connectionTotalService(t, connectionTotalConfig(t), key, capacity, clock)
			if name == "later-clock" {
				d.set(time.Second, errors.New("lost duration"))
			}
			out, e := s.connManager.reserveOutbound()
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(out.Release)
			in := connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
			aux := connectionTotalTake(t, s.connBudget, connbudget.DirectionAuxiliary)
			before := o.Snapshot()
			aux.Release()
			d.set(100*time.Second, nil)
			v := o.Snapshot()
			if !v.Available || v.Status != "UNKNOWN" || v.Reason != want || !v.PastOnly || v.AttributionBound || s.connBudget.Snapshot().Used != 2 || s.StartupRefusal() != nil {
				t.Fatal("source error changed admission/coverage", v, want)
			}
			if name == "gap" && (!before.Cut.Full || v.Cut != before.Cut || !reflect.DeepEqual(v.Rows, before.Rows)) {
				t.Fatal("gap extended past prefix", before, v)
			}
			in.Release()
		})
	}
}
func TestS6ConnectionTotalConstructionCoexistenceAndFrozenRequest(t *testing.T) {
	cfg := connectionTotalConfig(t)
	_, id := pendingConstructionInputs(t)
	d := &pendingConstructionClock{}
	request := ConnectionTotalObservationRequest{Key: connectionTotalConstructionKey(), Capacity: 8}
	hk := HandshakeObservationRequest{Key: handshakeConstructionKey(), Capacity: 8}
	pk, tk, gk := pendingConstructionKey(), terminalConstructionKey(), guardConstructionKey()
	s, r, e := newServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk, Handshake: &hk, ConnectionTotal: &request}, d.read)
	if e != nil {
		t.Fatal(e)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if r.Pending == nil || r.Terminal == nil || r.Guard == nil || r.Handshake == nil || r.ConnectionTotal == nil || r.ConnectionTotal.budget != s.connBudget || s.connManager.config.Budget != s.connBudget || r.Handshake.gate != s.inboundHandshakes {
		t.Fatal("common construction lost original owners")
	}
	owner := domain.PeerIdentityFromWire(s.Address())
	if r.Pending.Identity() != owner || r.Terminal.Identity() != owner || r.Guard.Identity() != owner || r.Handshake.Identity() != owner || r.ConnectionTotal.Identity() != owner {
		t.Fatal("source owners differ")
	}
	request.Key.Epoch[0] = 0
	request.Capacity = 0
	if r.ConnectionTotal.Key() != connectionTotalConstructionKey() {
		t.Fatal("caller mutated construction key")
	}
	p0, t0, g0, h0 := r.Pending.Snapshot(), r.Terminal.Snapshot(), r.Guard.Snapshot(), r.Handshake.Snapshot()
	out, e := s.connManager.reserveOutbound()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(out.Release)
	_ = connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
	aux := connectionTotalTake(t, s.connBudget, connbudget.DirectionAuxiliary)
	aux.Release()
	if v := r.ConnectionTotal.Snapshot(); v.Cut.Sequence != 2 || v.AttributionBound || r.Pending.Snapshot().Sequence != p0.Sequence || r.Terminal.Snapshot().Version != t0.Version || r.Guard.Snapshot().Sequence != g0.Sequence || r.Handshake.Snapshot().Cut.Sequence != h0.Cut.Sequence {
		t.Fatal("connection budget mutated another source", v)
	}
}
