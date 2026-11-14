package loadstand

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestRouteWireCommitmentFitsControlPacketAndBindsExactCensus(t *testing.T) {
	_, binding, expected := routeWireFixture(t)
	keys := make([]RouteWireEndpointKey, 300)
	interval := domain.RouteWireSessionInterval{BeginBefore: 100, BeginAfter: 100, EndBefore: 200, EndAfter: 200}
	for i := range keys {
		keys[i] = RouteWireEndpointKey{ConnID: domain.ConnID(i + 1), Peer: domain.PeerIdentity{4}, Direction: domain.PeerDirectionInbound, SessionID: LaunchFingerprint{byte(i + 1), byte((i + 1) >> 8)}}
		keys[i].Interval = interval
	}
	expected.Endpoints = keys
	expected.Bounds = RouteWireWindowBounds{Begin: 100, End: 200}
	identity := binding.RuntimeBinding().Identity
	commitment, err := commitRouteWireWindow(expected, identity)
	if err != nil || commitment.Count != len(keys) {
		t.Fatal("could not commit census", err)
	}
	rawCensus, err := json.Marshal(expected.Endpoints)
	if err != nil || len(rawCensus) <= linuxPacketLimit {
		t.Fatal("fixture does not exceed control packet", len(rawCensus), err)
	}
	packet, err := encodeLinuxPacket(linuxStop, 2, 1, linuxStopRequest{Nonce: binding.launch.nonce, BudgetNanos: 1, RouteWire: &commitment})
	if err != nil || len(packet) > 512 {
		t.Fatal("commitment did not fit in a bounded packet", len(packet), err)
	}
	decoded, err := decodeLinuxPacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	stop, err := decodeLinuxBody[linuxStopRequest](decoded)
	if err != nil || stop.RouteWire == nil || *stop.RouteWire != commitment {
		t.Fatal("Stop lost the census commitment", err)
	}
	selection := &PlannedRouteWireObservation{Window: commitment.Window, Bounds: commitment.Bounds, MaxEndpoints: commitment.Count}
	if !acceptsRouteWireCommitment(selection, commitment) || acceptsRouteWireCommitment(nil, commitment) {
		t.Fatal("commitment did not respect the explicit FullNode selection")
	}
	selection.Bounds.End++
	if acceptsRouteWireCommitment(selection, commitment) {
		t.Fatal("commitment accepted for changed bounds")
	}
	selection.Bounds = commitment.Bounds
	selection.MaxEndpoints--
	if acceptsRouteWireCommitment(selection, commitment) {
		t.Fatal("commitment exceeded the selected endpoint budget")
	}
	selection.MaxEndpoints++
	selection.Window[0]++
	if acceptsRouteWireCommitment(selection, commitment) {
		t.Fatal("commitment accepted for the wrong window")
	}
	t.Logf("census_bytes=%d stop_packet_bytes=%d endpoints=%d", len(rawCensus), len(packet), len(keys))
	slices.Reverse(keys)
	reversed, err := commitRouteWireWindow(expected, identity)
	if err != nil || reversed != commitment {
		t.Fatal("order changed census commitment", err)
	}
	keys[0].Direction = domain.PeerDirectionOutbound
	changed, err := commitRouteWireWindow(expected, identity)
	if err != nil || changed == commitment {
		t.Fatal("direction did not bind census", err)
	}
	keys[0] = keys[1]
	if _, err := commitRouteWireWindow(expected, identity); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatal("duplicate ConnID committed", err)
	}
}

func TestHeadlessRouteWireReportBindsBootBounds(t *testing.T) {
	report, binding, expected := routeWireBoundedFixture(t, RouteWireWindowBounds{Begin: 100, End: 200})
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	prior := NewTransportHighWater(binding.RuntimeBinding())
	got, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected)
	if err != nil || got.Bounds != report.Bounds {
		t.Fatalf("bounded report rejected: %+v, %v", got, err)
	}
	for _, mutation := range []struct{ from, to string }{
		{`,"boot_window":{"begin_ns":100,"end_ns":200}`, ""},
		{`"begin_ns":100`, `"begin_ns":101`},
		{`"end_ns":200`, `"end_ns":201`},
		{`{"begin_ns":100,"end_ns":200}`, `null`},
		{`"end_ns":200`, `"end_ns":200,"unused":true`},
		{`,"session_id":"` + expected.Endpoints[0].SessionID.String() + `"`, ""},
		{`"session_id":"` + expected.Endpoints[0].SessionID.String(), `"session_id":"` + (LaunchFingerprint{9}).String()},
		{`"session_id":"` + expected.Endpoints[0].SessionID.String(), `"session_id":"` + expected.Endpoints[1].SessionID.String()},
		{`"begin_before_ns":100`, `"begin_before_ns":99`},
		{`"end_before_ns":200`, `"end_before_ns":199`},
		{`"end_after_ns":200`, `"END_AFTER_NS":200`},
		{`,"sent_span":{"first_ns":150,"last_ns":150}`, ""},
		{`"first_ns":150,"last_ns":150`, `"first_ns":99,"last_ns":150`},
		{`"first_ns":150,"last_ns":150`, `"first_ns":150,"last_ns":200`},
	} {
		modified := strings.Replace(string(raw), mutation.from, mutation.to, 1)
		if modified == string(raw) {
			t.Fatal("mutation did not change fixture", mutation)
		}
		if _, err := DecodeHeadlessRouteWireReport([]byte(modified), binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
			t.Fatal("accepted changed boot bounds", mutation, err)
		}
	}
}

func TestHeadlessRouteWireFullReceiveReceipt(t *testing.T) {
	report, binding, expected := routeWireBoundedFixture(t, RouteWireWindowBounds{Begin: 100, End: 200})
	receipt := domain.RouteWireFullReceipt{Destinations: 1, IdentitySHA256: strings.Repeat("a", 64), Epoch: 2, SnapshotAfterNS: 201}
	projection := domain.RouteWireFullSend{Destinations: 1, IdentitySHA256: receipt.IdentitySHA256, Epoch: 2, Passes: 1, ConnectPasses: 1}
	report.Endpoints[0].FullSend = &projection
	report.Endpoints[1].FullReceive = &receipt
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	prior := NewTransportHighWater(binding.RuntimeBinding())
	got, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected)
	if err != nil || len(got.Endpoints) != 2 || got.Endpoints[0].FullReceive != nil ||
		got.Endpoints[1].FullReceive == nil || *got.Endpoints[1].FullReceive != receipt ||
		got.Endpoints[0].FullSend == nil || *got.Endpoints[0].FullSend != projection || got.Endpoints[1].FullSend != nil {
		t.Fatalf("receipt did not round trip: %+v, %v", got, err)
	}
	for _, change := range []struct{ from, to string }{
		{`"snapshot_after_ns":201`, `"snapshot_after_ns":200`},
		{`"destinations":1`, `"destinations":10001`},
		{`"epoch":2`, `"epoch":0`},
		{`"passes":1`, `"passes":2`},
		{`"passes":1`, `"passes":0`},
		{`"connect_passes":1`, `"connect_passes":0`},
		{`"triggered_passes":0`, `"triggered_passes":18446744073709551615`},
		{`"periodic_passes":0,`, ``},
		{`"identity_sha256"`, `"IDENTITY_SHA256"`},
		{`"identity_sha256":"` + receipt.IdentitySHA256 + `"`, `"identity_sha256":"bad"`},
		{`"snapshot_after_ns":201`, `"snapshot_after_ns":201,"unbound":true`},
	} {
		modified := strings.Replace(string(raw), change.from, change.to, 1)
		if modified == string(raw) {
			t.Fatal("receipt mutation did not modify report", change)
		}
		if _, err := DecodeHeadlessRouteWireReport([]byte(modified), binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
			t.Fatalf("accepted mutated receipt: %+v, %v", change, err)
		}
	}
	report.Endpoints[0].FullReceive, report.Endpoints[1].FullReceive = &receipt, nil
	raw, err = report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatal("receipt accepted on send-only endpoint", err)
	}
}

func routeWireBoundedFixture(t *testing.T, bounds RouteWireWindowBounds) (HeadlessRouteWireReport, CompletedStartBinding, RouteWireExpectation) {
	t.Helper()
	report, binding, expected := routeWireFixture(t)
	report.Bounds, expected.Bounds = bounds, bounds
	for i := range report.Endpoints {
		id := LaunchFingerprint{byte(i + 1)}
		interval := domain.RouteWireSessionInterval{BeginBefore: bounds.Begin, BeginAfter: bounds.Begin, EndBefore: bounds.End, EndAfter: bounds.End}
		report.Endpoints[i].SessionID = id
		expected.Endpoints[i].SessionID = id
		report.Endpoints[i].Interval, expected.Endpoints[i].Interval = interval, interval
		mid := bounds.Begin + (bounds.End-bounds.Begin)/2
		span := domain.RouteWireFrameSpan{First: mid, Last: mid}
		if report.Endpoints[i].Sent != (RouteWireCounts{}) {
			report.Endpoints[i].SentSpan = span
		}
		if report.Endpoints[i].Received != (RouteWireCounts{}) {
			report.Endpoints[i].ReceivedSpan = span
		}
	}
	return report, binding, expected
}

func TestSealedHeadlessRouteWireReportRejectsSubstitutedBytes(t *testing.T) {
	report, binding, expected := routeWireFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	census, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
	if err != nil {
		t.Fatal(err)
	}
	seal := RouteWireFinalSeal{Census: census, Bytes: len(raw), SHA256: sha256.Sum256(raw)}
	terminal, err := encodeLinuxPacket(linuxHelperJoined, 2, 1, linuxTerminal{
		Nonce: binding.launch.nonce, Code: linuxResultReady, Exit: ProcessExitResult{Kind: WaitNormal},
		MemberFinal: &linuxMemberFinal{Receipt: binding.Receipt(), Finality: FinalExact, RouteWire: &seal},
	})
	if err != nil {
		t.Fatal("final seal did not fit on the lifecycle channel", err)
	}
	packet, err := decodeLinuxPacket(terminal)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := decodeLinuxBody[linuxTerminal](packet)
	if err != nil || joined.MemberFinal == nil || joined.MemberFinal.RouteWire == nil || *joined.MemberFinal.RouteWire != seal {
		t.Fatal("terminal packet lost the report seal", err)
	}
	if _, err := DecodeSealedHeadlessRouteWireReport(raw, binding, prior, expected, *joined.MemberFinal.RouteWire); err != nil {
		t.Fatal("original bytes rejected", err)
	}
	report.Endpoints[0].Sent.FullV3.Bytes++
	changed, err := report.Encode()
	if err != nil || len(changed) != len(raw) {
		t.Fatal("fixture must preserve report length", err)
	}
	if _, err := DecodeHeadlessRouteWireReport(changed, binding, prior, expected); err != nil {
		t.Fatal("fixture replacement must remain structurally valid", err)
	}
	if _, err := DecodeSealedHeadlessRouteWireReport(changed, binding, prior, expected, seal); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatal("same-size substituted report accepted", err)
	}
}

func routeWireFixture(t *testing.T) (HeadlessRouteWireReport, CompletedStartBinding, RouteWireExpectation) {
	t.Helper()
	binding, final := finalFixture(t)
	keys := []RouteWireEndpointKey{
		{ConnID: 1, Peer: domain.PeerIdentity{4}, Direction: domain.PeerDirectionOutbound},
		{ConnID: 2, Peer: domain.PeerIdentity{5}, Direction: domain.PeerDirectionInbound},
	}
	row := RouteWireCount{Frames: 1, Bytes: 141, SHA256: strings.Repeat("a", 64)}
	transport := HeadlessFinalReport{
		Version: 1, Nonce: final.Nonce, Fingerprint: final.Fingerprint, Identity: final.Identity,
		StartedAt: final.StartedAt, ReadAt: final.ReadAt, BytesSent: 600, BytesReceived: 500,
		Quiescent: true, Outcome: HeadlessShutdownSucceeded,
	}
	report := HeadlessRouteWireReport{
		Version: 1, Window: LaunchNonce{6}, Transport: transport,
		Endpoints: []RouteWireEndpoint{
			{RouteWireEndpointKey: keys[0], Complete: true, Sent: RouteWireCounts{FullV3: row}},
			{RouteWireEndpointKey: keys[1], Complete: true, Received: RouteWireCounts{FullV3: row}},
		},
	}
	return report, binding, RouteWireExpectation{Window: report.Window, Endpoints: keys}
}

func TestHeadlessRouteWireReportBindsWindowAndCensus(t *testing.T) {
	report, binding, expected := routeWireFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected)
	if err != nil || len(got.Endpoints) != 2 || got.Endpoints[0] != report.Endpoints[0] ||
		got.Endpoints[1] != report.Endpoints[1] || got.Traffic.BytesSent != report.Transport.BytesSent {
		t.Fatalf("valid route accounting: %+v, %v", got, err)
	}
	ordinary, err := report.Transport.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeHeadlessFinalReport(ordinary, binding, prior); err != nil {
		t.Fatalf("ordinary headless accounting changed: %v", err)
	}
	if _, err = DecodeHeadlessFinalReport(raw, binding, prior); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatalf("ordinary decoder accepted route envelope: %v", err)
	}
	if _, err = DecodeHeadlessRouteWireReport(ordinary, binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatalf("route decoder accepted ordinary report: %v", err)
	}
}

func TestHeadlessRouteWireReportRejectsInvalidEvidence(t *testing.T) {
	cases := map[string]func(*HeadlessRouteWireReport, *RouteWireExpectation, *TransportHighWater){
		"window": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) { r.Window[0]++ },
		"launch": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Transport.Nonce[0]++
		},
		"incarnation": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Transport.StartedAt = r.Transport.ReadAt
		},
		"unfinished": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Transport.Quiescent = false
		},
		"lower totals": func(_ *HeadlessRouteWireReport, _ *RouteWireExpectation, p *TransportHighWater) {
			p.seen = true
			p.sent = 601
		},
		"missing endpoint": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints = r.Endpoints[:1]
		},
		"duplicate endpoint": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[1] = r.Endpoints[0]
		},
		"duplicate census ID": func(_ *HeadlessRouteWireReport, w *RouteWireExpectation, _ *TransportHighWater) {
			w.Endpoints[1].ConnID = w.Endpoints[0].ConnID
		},
		"unsealed endpoint": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[0].Complete = false
		},
		"direction mismatch": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[0].Direction = domain.PeerDirectionInbound
		},
		"peer mismatch": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[0].Peer[0]++
		},
		"plaintext exceeds transport in aggregate": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Transport.BytesSent = 200
			r.Endpoints[1].Sent.FullV3 = r.Endpoints[0].Sent.FullV3
		},
		"digest missing": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[0].Sent.FullV3.SHA256 = ""
		},
		"bytes without frames": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Endpoints[0].Sent.FullV3.Frames = 0
		},
		"frame exceeds wire limit": func(r *HeadlessRouteWireReport, _ *RouteWireExpectation, _ *TransportHighWater) {
			r.Transport.BytesSent = 1 << 20
			r.Endpoints[0].Sent.FullV3.Bytes = 1 << 19
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			report, binding, expected := routeWireFixture(t)
			prior := NewTransportHighWater(binding.RuntimeBinding())
			mutate(&report, &expected, &prior)
			raw, err := report.Encode()
			if err != nil {
				t.Fatal(err)
			}
			if got, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) || got.Endpoints != nil {
				t.Fatalf("invalid record yielded accounting: %+v, %v", got, err)
			}
		})
	}
}

func TestHeadlessRouteWireReportRejectsMissingNullAndDuplicateCounters(t *testing.T) {
	report, binding, expected := routeWireFixture(t)
	prior := NewTransportHighWater(binding.RuntimeBinding())
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"missing class": strings.Replace(string(raw), `"probe":{"frames":0,"bytes":0,"sha256":""},`, "", 1),
		"null bytes":    strings.Replace(string(raw), `"bytes":0`, `"bytes":null`, 1),
		"duplicate":     strings.Replace(string(raw), `"bytes":0`, `"bytes":0,"bytes":0`, 1),
		"case alias":    strings.Replace(string(raw), `"bytes":0`, `"Bytes":0`, 1),
		"trailing data": string(raw) + " {}",
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if payload == string(raw) {
				t.Fatal("fixture mutation did not apply")
			}
			if _, err := DecodeHeadlessRouteWireReport([]byte(payload), binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
				t.Fatalf("invalid schema accepted: %v", err)
			}
		})
	}
	report.Endpoints = nil
	expected.Endpoints = nil
	raw, err = report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := DecodeHeadlessRouteWireReport(raw, binding, prior, expected); err != nil || len(result.Endpoints) != 0 {
		t.Fatalf("sealed zero-census window: %+v, %v", result, err)
	}
	if _, err := DecodeHeadlessRouteWireReport([]byte(strings.Repeat(" ", int(routeWireReportLimit(0))+1)), binding, prior, expected); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatalf("oversized report: %v", err)
	}
}
