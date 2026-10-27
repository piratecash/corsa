package node

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

func handshakeConstructionKey() HandshakeObservationKey {
	return HandshakeObservationKey{Anchor: "handshake-anchor", ArtifactSHA256: strings.Repeat("d", 64), Boot: "handshake-boot", ProcessInstance: "handshake-process", Epoch: [16]byte{111}, ClockDomain: [16]byte{112}, Configuration: [16]byte{113}}
}
func constructHandshakeService(t *testing.T, key HandshakeObservationKey, capacity int, clock func() (time.Duration, error)) (*Service, *HandshakeObservation) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	request := HandshakeObservationRequest{Key: key, Capacity: capacity}
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Handshake: &request}, clock)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if r.Handshake == nil || r.Handshake.service != s || r.Handshake.gate != s.inboundHandshakes {
		t.Fatal("handle is not the original Service gate")
	}
	return s, r.Handshake
}
func handshakeProjection(t *testing.T, s *Service, o *HandshakeObservation) HandshakeObservationSnapshot {
	t.Helper()
	v, raw := o.Snapshot(), s.inboundHandshakes.fullSnapshot()
	if !v.Available || v.AttributionBound || v.SchemaVersion != 1 || v.Status != raw.status || v.Reason != raw.reason || v.PastOnly != raw.pastOnly || v.CoverageStart != raw.coverageStart {
		t.Fatal("snapshot scalar projection changed", v, raw)
	}
	if v.Source != (HandshakeObservationSource{Anchor: raw.key.anchor, Epoch: raw.key.epoch, ClockDomain: raw.key.clockDomain, Configuration: raw.key.configuration, Kind: uint8(raw.key.kind)}) || v.Bounds != (HandshakeObservationBounds{Node: raw.bounds.node, Loopback: raw.bounds.loopback, PerSource: raw.bounds.perSource}) || v.Cut != (HandshakeObservationCut{Available: raw.cut.available, Rows: raw.cut.rows, Sequence: raw.cut.sequence, At: raw.cut.at, Full: raw.cut.full}) || len(v.Rows) != len(raw.rows) {
		t.Fatal("snapshot source/bounds/cut projection changed", v, raw)
	}
	for i, row := range raw.rows {
		if v.Rows[i] != (HandshakeObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}) {
			t.Fatal("snapshot lost an original row field", i, v.Rows[i], row)
		}
	}
	return v
}
func handshakeEnterFrom(t *testing.T, h *inboundHandshakes, ip string) func() {
	t.Helper()
	source := handshakeSourceOf(&net.TCPAddr{IP: net.ParseIP(ip), Port: 40000}, true)
	leaveSource, err := h.admitSource(source)
	if err != nil {
		t.Fatal("actual source admission refused", ip, err)
	}
	leaveNode, err := h.enter(source)
	if err != nil {
		leaveSource()
		t.Fatal("actual node admission refused", ip, err)
	}
	return func() { leaveNode(); leaveSource() }
}
func handshakeFill(t *testing.T, h *inboundHandshakes, loopbacks int) []func() {
	t.Helper()
	var releases []func()
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})
	for i := 0; i < 48; i++ {
		releases = append(releases, handshakeEnterFrom(t, h, fmt.Sprintf("198.18.0.%d", i+1)))
	}
	for i := 0; i < loopbacks; i++ {
		releases = append(releases, handshakeEnterFrom(t, h, "127.0.0.1"))
	}
	return releases
}

func TestS6HandshakeConstructionActualGate(t *testing.T) {
	clock := &pendingConstructionClock{}
	s, o := constructHandshakeService(t, handshakeConstructionKey(), 8, clock.read)
	if s.inboundHandshakes.bounds != productionHandshakeBounds() || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
		t.Fatal("production gate bounds or owner changed")
	}
	initial := handshakeProjection(t, s, o)
	if initial.Status != "COMPLETE" || !initial.Cut.Available || initial.Cut.Full || initial.Cut.Sequence != 0 || initial.PastOnly {
		t.Fatal("missing original prepublication baseline", initial)
	}
	clock.set(time.Second, nil)
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	full := handshakeProjection(t, s, o)
	if !full.Cut.Full || full.Cut.Sequence != 1 || len(full.Rows) != 1 || full.Rows[0].Count != 64 || full.Rows[0].Lower != time.Second || full.Rows[0].Upper != time.Second {
		t.Fatal("last actual slot did not enter full", full)
	}
	_, err := s.inboundHandshakes.enter(handshakeSource{loopback: true})
	if k5RefusedBy(t, err) != refusalHandshakesNode {
		t.Fatal("original node-before-loopback refusal order changed")
	}
	clock.set(3*time.Second, nil)
	releases[0]()
	releases[0]() // Original once-only release, not a counter write.
	after := handshakeProjection(t, s, o)
	if after.Cut.Full || after.Cut.Sequence != 2 || len(after.Rows) != 2 || after.Rows[1].Count != 63 || after.Rows[1].Lower != 3*time.Second || after.Rows[1].Upper != 3*time.Second {
		t.Fatal("first release/double release lost original exit", after)
	}
	_, err = s.inboundHandshakes.enter(handshakeSource{loopback: true})
	if k5RefusedBy(t, err) != refusalHandshakesLoopback || o.Snapshot().Cut.Sequence != 2 {
		t.Fatal("loopback refusal with spare node room became node fullness")
	}
	fresh := handshakeEnterFrom(t, s.inboundHandshakes, "198.18.1.1")
	fresh()
	fresh()
	if got := handshakeProjection(t, s, o); got.Cut.Full || got.Cut.Sequence != 4 {
		t.Fatal("released capacity did not admit fresh work", got)
	}
	full.Rows[0].Sequence = 999
	copyKey := o.Key()
	copyKey.Epoch[0] = 0
	if got := o.Snapshot(); got.Rows[0].Sequence != 1 || o.Key() != handshakeConstructionKey() {
		t.Fatal("exported DTO/key aliases original source")
	}
	// Source slots are distinct from the node-full source, including incomplete attempts.
	var sourceReleases []func()
	t.Cleanup(func() {
		for _, release := range sourceReleases {
			release()
		}
	})
	source := handshakeSourceOf(&net.TCPAddr{IP: net.ParseIP("198.18.2.1"), Port: 40000}, true)
	for i := 0; i < 4; i++ {
		release, e := s.inboundHandshakes.admitSource(source)
		if e != nil {
			t.Fatal(e)
		}
		sourceReleases = append(sourceReleases, release)
	}
	if _, e := s.inboundHandshakes.admitSource(source); k5RefusedBy(t, e) != refusalHandshakesPerIP || o.Snapshot().Cut.Sequence != 4 {
		t.Fatal("per-source refusal altered node-full history")
	}
}

func TestS6HandshakeConstructionKeyAndCoexistence(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	clock := &pendingConstructionClock{}
	request := HandshakeObservationRequest{Key: handshakeConstructionKey(), Capacity: 8}
	pk, tk, gk := pendingConstructionKey(), terminalConstructionKey(), guardConstructionKey()
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk, Handshake: &request}, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if r.Handshake == nil || r.Pending == nil || r.Terminal == nil || r.Guard == nil || r.Handshake.service != s || r.Pending.service != s || r.Terminal.service != s || r.Guard.service != s || r.Handshake.gate != s.inboundHandshakes {
		t.Fatal("one construction lost an actual owner")
	}
	request.Key.Anchor = "changed"
	request.Key.Epoch[0] = 0
	request.Capacity = 1
	if r.Handshake.Key() != handshakeConstructionKey() || len(s.inboundHandshakes.fullObservation.rows) != 8 || r.Handshake.Identity() != r.Pending.Identity() || r.Handshake.Identity() != r.Terminal.Identity() || r.Handshake.Identity() != r.Guard.Identity() {
		t.Fatal("caller changed frozen key/capacity/owner")
	}
	p0, t0, g0 := r.Pending.Snapshot(), r.Terminal.Snapshot(), r.Guard.Snapshot()
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	releases[0]()
	v := handshakeProjection(t, s, r.Handshake)
	if v.Cut.Sequence != 2 || r.Pending.Snapshot().Sequence != p0.Sequence || r.Terminal.Snapshot().Version != t0.Version || r.Guard.Snapshot().Sequence != g0.Sequence || v.AttributionBound {
		t.Fatal("handshake gate mutated another observation")
	}
}

func TestS6HandshakeConstructionUnavailable(t *testing.T) {
	for _, name := range []string{"capacity-zero", "capacity-negative", "capacity-arithmetic", "epoch-zero", "domain-zero", "configuration-zero", "initial-clock", "nil-clock", "later-clock", "capacity-gap"} {
		t.Run(name, func(t *testing.T) {
			key, capacity := handshakeConstructionKey(), 8
			clock := &pendingConstructionClock{}
			provider := clock.read
			want := "invalid-capacity"
			switch name {
			case "capacity-zero":
				capacity = 0
			case "capacity-negative":
				capacity = -1
			case "capacity-arithmetic":
				capacity = int(^uint(0) >> 1)
			case "epoch-zero":
				key.Epoch = [16]byte{}
				want = "invalid-source-identity"
			case "domain-zero":
				key.ClockDomain = [16]byte{}
				want = "invalid-source-identity"
			case "configuration-zero":
				key.Configuration = [16]byte{}
				want = "invalid-source-identity"
			case "initial-clock":
				clock.set(0, errors.New("missing duration"))
				want = "clock-error"
			case "nil-clock":
				provider = nil
				want = "unsupported-clock"
			case "later-clock":
				want = "clock-error"
			case "capacity-gap":
				capacity = 1
				want = "transition-capacity-gap"
			}
			s, o := constructHandshakeService(t, key, capacity, provider)
			if s.StartupRefusal() != nil || s.inboundHandshakes.bounds != productionHandshakeBounds() {
				t.Fatal("unavailable observation changed original startup/gate")
			}
			if name == "later-clock" {
				clock.set(time.Second, errors.New("lost duration"))
			}
			releases := handshakeFill(t, s.inboundHandshakes, 16)
			if s.inboundHandshakes.running() != 64 {
				t.Fatal("unavailable observation changed actual admission")
			}
			before := o.Snapshot()
			releases[0]()
			clock.set(100*time.Second, nil)
			v := handshakeProjection(t, s, o)
			if v.Status != "UNKNOWN" || v.Reason != want || !v.PastOnly || v.AttributionBound || s.inboundHandshakes.running() != 63 {
				t.Fatal("missing evidence was certified or changed gate policy", v, want)
			}
			if name == "capacity-gap" && (before.Status != "COMPLETE" || !before.Cut.Full || v.Cut != before.Cut || len(v.Rows) != 1) {
				t.Fatal("gap extended or lost sealed past prefix", before, v)
			}
		})
	}
}

func TestS6HandshakeConstructionDefaultAndValidation(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	called := 0
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{}, func() (time.Duration, error) { called++; return 0, errors.New("must not be called") })
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	if called != 0 || r.Handshake != nil || s.inboundHandshakes.fullObservation != nil || s.inboundHandshakes.bounds != productionHandshakeBounds() {
		t.Fatal("ordinary construction activated source or changed bounds")
	}
	var absent *HandshakeObservation
	if v := absent.Snapshot(); v.Available || v.Status != "INACTIVE" || v.AttributionBound || !v.PastOnly || absent.Key() != (HandshakeObservationKey{}) || absent.Identity() != (domain.PeerIdentity{}) {
		t.Fatal("nil handle fabricated evidence", v)
	}
	for _, field := range []string{"anchor", "boot", "process", "artifact", "unbounded", "control"} {
		t.Run(field, func(t *testing.T) {
			key := handshakeConstructionKey()
			switch field {
			case "anchor":
				key.Anchor = ""
			case "boot":
				key.Boot = ""
			case "process":
				key.ProcessInstance = ""
			case "artifact":
				key.ArtifactSHA256 = strings.Repeat("0", 64)
			case "unbounded":
				key.Anchor = strings.Repeat("x", 129)
			case "control":
				key.Boot = "bad\nreference"
			}
			request := HandshakeObservationRequest{Key: key, Capacity: 8}
			got, readouts, e := newServiceWithObservations(cfg, id, nil, ObservationOptions{Handshake: &request}, func() (time.Duration, error) { t.Fatal("invalid references read provider"); return 0, nil })
			if e == nil || got != nil || readouts.Handshake != nil {
				t.Fatal("malformed caller references accepted")
			}
		})
	}
}

func TestS6HandshakeConstructionProviderOutsideLeaf(t *testing.T) {
	var s *Service
	var held atomic.Bool
	clock := func() (time.Duration, error) {
		if s != nil {
			if !s.inboundHandshakes.mu.TryLock() {
				held.Store(true)
			} else {
				s.inboundHandshakes.mu.Unlock()
			}
		}
		return time.Second, nil
	}
	s, o := constructHandshakeService(t, handshakeConstructionKey(), 8, clock)
	releases := handshakeFill(t, s.inboundHandshakes, 16)
	_ = o.Snapshot()
	releases[0]()
	_ = o.Snapshot()
	if held.Load() {
		t.Fatal("constructor bridge moved provider under gate owner leaf")
	}
}

type handshakeConstructionWriteNotice struct {
	net.Conn
	once    sync.Once
	writing chan struct{}
}

func (c *handshakeConstructionWriteNotice) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}
func handshakeConstructionHello(t *testing.T, end time.Time) []byte {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tls.Client(client, &tls.Config{ServerName: "corsa.invalid", NextProtos: []string{"corsa/2"}, MinVersion: tls.VersionTLS13}).Handshake()
	}()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("ClientHello producer did not join under original deadline")
		}
	}()
	if err := server.SetReadDeadline(end); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 5)
	if _, err := io.ReadFull(server, header); err != nil {
		t.Fatal(err)
	}
	n := int(binary.BigEndian.Uint16(header[3:5]))
	if n == 0 || n > maxClientHelloRecord {
		t.Fatal("unexpected real ClientHello length", n)
	}
	record := make([]byte, 5+n)
	copy(record, header)
	if _, err := io.ReadFull(server, record[5:]); err != nil {
		t.Fatal(err)
	}
	return record
}
func TestS6HandshakeConstructionActualClientHello(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	clock := &pendingConstructionClock{}
	s, o := constructHandshakeService(t, handshakeConstructionKey(), 8, clock.read)
	_ = handshakeFill(t, s.inboundHandshakes, 15) // 48 valid external sources +15 loopback, no counter writes.
	record := handshakeConstructionHello(t, end)
	client, server := net.Pipe()
	notice := &handshakeConstructionWriteNotice{Conn: sourcedConn{Conn: server, remote: addrOf("127.0.0.1")}, writing: make(chan struct{})}
	var totals netcore.TransportTotals
	metered, err := netcore.NewMeteredConn(notice, &totals)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var acceptErr error
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("actual acceptSecureInbound did not join after owned sockets closed")
		}
	})
	clock.set(2*time.Second, nil)
	go func() {
		defer close(done)
		conn, _, err := s.acceptSecureInbound(metered, bufio.NewReader(metered))
		acceptErr = err
		if conn != nil {
			_ = conn.Close()
		}
	}()
	if err := client.SetWriteDeadline(end); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(record); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(end))
	defer timer.Stop()
	select {
	case <-notice.writing:
	case <-done:
		t.Fatal("real ClientHello returned before observed TLS write")
	case <-timer.C:
		t.Fatal("real ClientHello did not reach TLS while owning last gate slot")
	}
	full := handshakeProjection(t, s, o)
	if full.Status != "COMPLETE" || !full.Cut.Full || full.Cut.Sequence != 1 || len(full.Rows) != 1 || full.Rows[0].Count != 64 {
		t.Fatal("actual ClientHello used a different gate", full)
	}
	clock.set(3*time.Second, nil)
	_ = client.Close()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("actual failed TLS path did not return its gate slot")
	}
	if acceptErr == nil {
		t.Fatal("closed incomplete TLS unexpectedly succeeded")
	}
	after := handshakeProjection(t, s, o)
	if after.Cut.Full || after.Cut.Sequence != 2 || len(after.Rows) != 2 || after.Rows[1].Count != 63 || after.Rows[1].Lower != 3*time.Second || after.Rows[1].Upper != 3*time.Second {
		t.Fatal("actual TLS cleanup lost original gate release", after)
	}
}
