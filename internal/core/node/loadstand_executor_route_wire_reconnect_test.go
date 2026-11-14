package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

func TestLoadStandRouteWireReconnectRequiresOrderedReplacement(t *testing.T) {
	const left, right = "full-1", "full-2"
	bounds := loadstand.RouteWireWindowBounds{Begin: 100, End: 200}
	window := loadStandRouteWireWindow{
		bounds: bounds, members: []loadStandRouteWireMember{{left, 3}, {right, 3}},
		fullReceiptRouteBudget: 1, requiredFullRoutes: 1, requiredFullSource: domain.RouteWireFullConnect, requireReconnect: true,
		disconnectAt: 125,
		dialer:       left,
	}
	life := loadstand.Incarnation{Ordinal: 1, StartedAt: time.Unix(1, 0)}
	starts := map[loadStandNodeID]loadstand.StartReceipt{
		left:  {Incarnation: life, Public: loadstand.PublicIdentity{Identity: idPeerA}},
		right: {Incarnation: life, Public: loadstand.PublicIdentity{Identity: idPeerB}},
	}
	const digest = "1b3b938e14bb444b1f626d90516df305e594206cbd5f26963393c709aab64458"
	counts := loadstand.RouteWireCounts{FullV3: loadstand.RouteWireCount{Frames: 1, Bytes: 43, SHA256: digest}}
	for _, tc := range []struct {
		name   string
		change func([]loadStandRouteWireFinal)
		want   error
	}{
		{"ordered replacement", func([]loadStandRouteWireFinal) {}, nil},
		{"replacement initiated by listener", func(r []loadStandRouteWireFinal) {
			r[0].Accounting.Endpoints[1].Direction = domain.PeerDirectionInbound
			r[1].Accounting.Endpoints[1].Direction = domain.PeerDirectionOutbound
		}, loadstand.ErrFinalAccountingUnavailable},
		{"first connection only", func(r []loadStandRouteWireFinal) {
			for i := range r {
				r[i].Accounting.Endpoints = r[i].Accounting.Endpoints[1:]
			}
		}, loadstand.ErrFinalAccountingUnavailable},
		{"missing remote retirement", func(r []loadStandRouteWireFinal) {
			p := &r[1].Accounting.Endpoints[0].Interval
			p.EndBefore, p.EndAfter = bounds.End, bounds.End
		}, loadstand.ErrFinalAccountingUnavailable},
		{"old session retired before planned disconnect", func(r []loadStandRouteWireFinal) {
			p := &r[0].Accounting.Endpoints[0].Interval
			p.EndBefore, p.EndAfter = 122, 124
		}, loadstand.ErrFinalAccountingUnavailable},
		{"uncertain order across sides", func(r []loadStandRouteWireFinal) {
			r[1].Accounting.Endpoints[0].Interval.EndAfter = 151
		}, loadstand.ErrFinalAccountingUnavailable},
		{"replacement retired before close", func(r []loadStandRouteWireFinal) {
			p := &r[1].Accounting.Endpoints[1].Interval
			p.EndBefore, p.EndAfter = 192, 193
		}, loadstand.ErrFinalAccountingUnavailable},
		{"reused local connection", func(r []loadStandRouteWireFinal) {
			r[0].Accounting.Endpoints[1].ConnID = r[0].Accounting.Endpoints[0].ConnID
		}, loadstand.ErrFinalReportInvalid},
		{"reused TLS session ID", func(r []loadStandRouteWireFinal) {
			for i := range r {
				r[i].Accounting.Endpoints[1].SessionID = r[i].Accounting.Endpoints[0].SessionID
			}
		}, loadstand.ErrFinalReportInvalid},
		{"full pass on old session", func(r []loadStandRouteWireFinal) {
			sent, received := r[0].Accounting.Endpoints, r[1].Accounting.Endpoints
			sent[0].Sent, sent[1].Sent = sent[1].Sent, loadstand.RouteWireCounts{}
			sent[0].FullSend, sent[1].FullSend = sent[1].FullSend, nil
			sent[0].SentSpan, sent[1].SentSpan = domain.RouteWireFrameSpan{First: 119, Last: 119}, domain.RouteWireFrameSpan{}
			received[0].Received, received[1].Received = received[1].Received, loadstand.RouteWireCounts{}
			received[0].FullReceive, received[1].FullReceive = received[1].FullReceive, nil
			received[0].ReceivedSpan, received[1].ReceivedSpan = domain.RouteWireFrameSpan{First: 121, Last: 121}, domain.RouteWireFrameSpan{}
		}, loadstand.ErrFinalAccountingUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second := loadstand.LaunchFingerprint{7}, loadstand.LaunchFingerprint{3}
			reports := []loadStandRouteWireFinal{
				{loadStandRouteWireLife: loadStandRouteWireLife{Node: left, Incarnation: life}, Accounting: loadstand.HeadlessRouteWireAccounting{Bounds: bounds, Endpoints: []loadstand.RouteWireEndpoint{
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 1, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: first,
						Interval: domain.RouteWireSessionInterval{BeginBefore: 100, BeginAfter: 100, EndBefore: 130, EndAfter: 132}}, Complete: true},
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 2, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: second,
						Interval: domain.RouteWireSessionInterval{BeginBefore: 150, BeginAfter: 151, EndBefore: 200, EndAfter: 200}}, Complete: true, Sent: counts,
						SentSpan: domain.RouteWireFrameSpan{First: 171, Last: 171}, FullSend: &domain.RouteWireFullSend{Destinations: 1, IdentitySHA256: digest, Epoch: 1, Passes: 1, ConnectPasses: 1}},
				}}},
				{loadStandRouteWireLife: loadStandRouteWireLife{Node: right, Incarnation: life}, Accounting: loadstand.HeadlessRouteWireAccounting{Bounds: bounds, Endpoints: []loadstand.RouteWireEndpoint{
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 10, Peer: idPeerA, Direction: domain.PeerDirectionInbound, SessionID: first,
						Interval: domain.RouteWireSessionInterval{BeginBefore: 100, BeginAfter: 100, EndBefore: 134, EndAfter: 135}}, Complete: true},
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 11, Peer: idPeerA, Direction: domain.PeerDirectionInbound, SessionID: second,
						Interval: domain.RouteWireSessionInterval{BeginBefore: 154, BeginAfter: 155, EndBefore: 200, EndAfter: 200}}, Complete: true, Received: counts,
						ReceivedSpan: domain.RouteWireFrameSpan{First: 173, Last: 173}, FullReceive: &domain.RouteWireFullReceipt{Destinations: 1, IdentitySHA256: digest, Epoch: 1, SnapshotAfterNS: 201}},
				}}},
			}
			tc.change(reports)
			got, err := window.verifyPairs(starts, reports)
			if !errors.Is(err, tc.want) || (got == nil) != (tc.want != nil) {
				t.Fatalf("reconnect evidence: %+v, %v, want %v", got, err, tc.want)
			}
			if got != nil && (len(got.rates.Sessions) != 2 || got.rates.Sessions[0].SessionID != second || got.rates.Sessions[0].DialerToListener.FullV3.Bytes != 43) {
				t.Fatalf("full bytes were not assigned to replacement session: %+v", got.rates.Sessions)
			}
		})
	}
}
