package node

import (
	"errors"
	"math"
	"math/bits"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

type loadStandRouteWireByteRate struct {
	Bytes             uint64 `json:"bytes"`
	MinBytesPerSecond uint64 `json:"min_bytes_per_second"`
	MaxBytesPerSecond uint64 `json:"max_bytes_per_second"`
}

func TestLoadStandRouteWireRateArithmetic(t *testing.T) {
	wire := loadstand.RouteWireCounts{
		Probe: loadstand.RouteWireCount{Bytes: 1}, Ack: loadstand.RouteWireCount{Bytes: 2},
		Digest: loadstand.RouteWireCount{Bytes: 3}, Summary: loadstand.RouteWireCount{Bytes: 4},
		FullV3: loadstand.RouteWireCount{Bytes: 5}, DeltaV3: loadstand.RouteWireCount{Bytes: 6},
	}
	rates, err := loadStandRouteWireBytes(wire).rates(time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i, rate := range []loadStandRouteWireByteRate{rates.Probe, rates.Ack, rates.Digest, rates.Summary, rates.FullV3, rates.DeltaV3} {
		n := uint64(i + 1)
		if rate != (loadStandRouteWireByteRate{Bytes: n, MinBytesPerSecond: n, MaxBytesPerSecond: n}) {
			t.Fatalf("class %d = %+v", i, rate)
		}
	}
	rates, err = (loadStandRouteWireClassBytes{routeWireProbe: 1}).rates(2*time.Second, 3*time.Second)
	if err != nil || rates.Probe != (loadStandRouteWireByteRate{Bytes: 1, MaxBytesPerSecond: 1}) {
		t.Fatalf("fractional rate = %+v, %v", rates.Probe, err)
	}
	bytes := loadStandRouteWireClassBytes{routeWireAck: math.MaxUint64}
	rates, err = bytes.rates(time.Second, time.Second)
	if err != nil || rates.Ack != (loadStandRouteWireByteRate{Bytes: math.MaxUint64, MinBytesPerSecond: math.MaxUint64, MaxBytesPerSecond: math.MaxUint64}) {
		t.Fatalf("largest representable rate = %+v, %v", rates.Ack, err)
	}
	if _, err := bytes.rates(time.Nanosecond, time.Second); !errors.Is(err, loadstand.ErrFinalReportInvalid) {
		t.Fatalf("rate overflow accepted: %v", err)
	}
	if _, err := bytes.rates(0, time.Second); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
		t.Fatalf("missing duration accepted: %v", err)
	}
	if bytes.add(loadStandRouteWireClassBytes{routeWireAck: 1}) {
		t.Fatal("node byte overflow accepted")
	}
}

type loadStandRouteWireClassRates struct {
	Probe   loadStandRouteWireByteRate `json:"probe"`
	Ack     loadStandRouteWireByteRate `json:"ack"`
	Digest  loadStandRouteWireByteRate `json:"digest"`
	Summary loadStandRouteWireByteRate `json:"summary"`
	FullV3  loadStandRouteWireByteRate `json:"full_v3"`
	DeltaV3 loadStandRouteWireByteRate `json:"delta_v3"`
}

type loadStandRouteWireSessionRates struct {
	SessionID        loadstand.LaunchFingerprint  `json:"session_id"`
	Dialer           loadstand.MemberID           `json:"dialer"`
	Listener         loadstand.MemberID           `json:"listener"`
	DialerToListener loadStandRouteWireClassRates `json:"dialer_to_listener"`
	ListenerToDialer loadStandRouteWireClassRates `json:"listener_to_dialer"`
}

type loadStandRouteWireNodeRates struct {
	Node     loadstand.MemberID           `json:"node"`
	Sent     loadStandRouteWireClassRates `json:"sent"`
	Received loadStandRouteWireClassRates `json:"received"`
}

type loadStandRouteWireRates struct {
	WindowNS time.Duration                    `json:"window_ns"`
	Sessions []loadStandRouteWireSessionRates `json:"sessions"`
	Nodes    []loadStandRouteWireNodeRates    `json:"nodes"`
}

type loadStandRouteWireClassBytes [routeWireClasses]uint64

func loadStandRouteWireBytes(c loadstand.RouteWireCounts) loadStandRouteWireClassBytes {
	return loadStandRouteWireClassBytes{
		routeWireProbe: c.Probe.Bytes, routeWireAck: c.Ack.Bytes, routeWireDigest: c.Digest.Bytes,
		routeWireSummary: c.Summary.Bytes, routeWireFullV3: c.FullV3.Bytes, routeWireDeltaV3: c.DeltaV3.Bytes,
	}
}

func (b *loadStandRouteWireClassBytes) add(other loadStandRouteWireClassBytes) bool {
	for i := range b {
		sum, carry := bits.Add64(b[i], other[i], 0)
		if carry != 0 {
			return false
		}
		b[i] = sum
	}
	return true
}

// Round outwards with integer arithmetic so the reported interval contains
// the exact bytes/second at every duration in the proven lifetime bracket.
func (b loadStandRouteWireClassBytes) rates(minimum, maximum time.Duration) (loadStandRouteWireClassRates, error) {
	if minimum <= 0 || maximum < minimum {
		return loadStandRouteWireClassRates{}, loadstand.ErrFinalAccountingUnavailable
	}
	var rates [routeWireClasses]loadStandRouteWireByteRate
	for i, n := range b {
		hi, lo := bits.Mul64(n, uint64(time.Second))
		if hi >= uint64(minimum) {
			return loadStandRouteWireClassRates{}, loadstand.ErrFinalReportInvalid
		}
		lower, _ := bits.Div64(hi, lo, uint64(maximum))
		upper, remainder := bits.Div64(hi, lo, uint64(minimum))
		if remainder != 0 {
			if upper == math.MaxUint64 {
				return loadStandRouteWireClassRates{}, loadstand.ErrFinalReportInvalid
			}
			upper++
		}
		rates[i] = loadStandRouteWireByteRate{Bytes: n, MinBytesPerSecond: lower, MaxBytesPerSecond: upper}
	}
	return loadStandRouteWireClassRates{
		Probe: rates[routeWireProbe], Ack: rates[routeWireAck], Digest: rates[routeWireDigest],
		Summary: rates[routeWireSummary], FullV3: rates[routeWireFullV3], DeltaV3: rates[routeWireDeltaV3],
	}, nil
}
