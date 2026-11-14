package loadstand

import (
	"errors"
	"testing"
)

func TestRouteWireCensusWorkReply(t *testing.T) {
	_, binding, expected := routeWireBoundedFixture(t, RouteWireWindowBounds{Begin: 100, End: 200})
	commitment, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
	if err != nil {
		t.Fatal(err)
	}
	plan := PlannedRouteWireObservation{Window: expected.Window, Bounds: expected.Bounds, MaxEndpoints: len(expected.Endpoints)}
	nonce := binding.launch.nonce
	clock := RouteWireClockDomain{BootID: "c481cc22-adf3-4d99-a4fe-de7f204e0ad6", Device: 4, Inode: 17}
	const requestID = 4
	for _, tc := range []struct {
		name   string
		change func(*linuxPacketKind, *LinuxRequestID, *LinuxLaunchOrdinal, *linuxRouteWireCensusReply)
		want   error
	}{
		{"bound reply", func(*linuxPacketKind, *LinuxRequestID, *LinuxLaunchOrdinal, *linuxRouteWireCensusReply) {}, nil},
		{"unavailable", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Code, r.Census, r.Clock = linuxResultUnavailable, nil, nil
		}, ErrFinalAccountingUnavailable},
		{"unavailable with data", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Code = linuxResultUnavailable
		}, ErrLinuxOwnerProtocol},
		{"ready without data", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Census = nil
		}, ErrLinuxOwnerProtocol},
		{"wrong request", func(_ *linuxPacketKind, id *LinuxRequestID, _ *LinuxLaunchOrdinal, _ *linuxRouteWireCensusReply) {
			*id++
		}, ErrLinuxOwnerProtocol},
		{"wrong ordinal", func(_ *linuxPacketKind, _ *LinuxRequestID, ordinal *LinuxLaunchOrdinal, _ *linuxRouteWireCensusReply) {
			*ordinal++
		}, ErrLinuxOwnerProtocol},
		{"wrong packet kind", func(kind *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, _ *linuxRouteWireCensusReply) {
			*kind = linuxSampleResult
		}, ErrLinuxOwnerProtocol},
		{"wrong nonce", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Nonce[0]++
		}, ErrLinuxOwnerProtocol},
		{"wrong bounds", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Census.Bounds.End++
		}, ErrLinuxOwnerProtocol},
		{"wrong clock namespace", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Clock.Inode++
		}, ErrLinuxOwnerProtocol},
		{"over budget", func(_ *linuxPacketKind, _ *LinuxRequestID, _ *LinuxLaunchOrdinal, r *linuxRouteWireCensusReply) {
			r.Census.Count++
		}, ErrLinuxOwnerProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, id, ordinal := linuxRouteWireCensusResult, LinuxRequestID(requestID), LinuxLaunchOrdinal(1)
			census := commitment
			domain := clock
			reply := linuxRouteWireCensusReply{Nonce: nonce, Code: linuxResultReady, Census: &census, Clock: &domain}
			tc.change(&kind, &id, &ordinal, &reply)
			raw, err := encodeLinuxPacket(kind, id, ordinal, reply)
			if err != nil || len(raw) > 512 {
				t.Fatalf("wire packet size %d, error %v", len(raw), err)
			}
			packet, err := decodeLinuxPacket(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeLinuxRouteWireCensusReply(packet, requestID, nonce, &plan, clock)
			want := commitment
			if tc.want != nil {
				want = RouteWireCommitment{}
			}
			if !errors.Is(err, tc.want) || got != want {
				t.Fatalf("got %+v, %v, want %+v, %v", got, err, want, tc.want)
			}
		})
	}
}
