package node

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

type loadStandRouteWireObservedEnd struct {
	node     loadstand.MemberID
	identity domain.PeerIdentity
	row      *loadstand.RouteWireEndpoint
}

type loadStandRouteWirePairEndpoints struct {
	outbound, inbound loadStandRouteWireObservedEnd
}

type loadStandRouteWireDuration struct {
	SessionID loadstand.LaunchFingerprint
	Minimum   time.Duration
	Maximum   time.Duration
}

type loadStandRouteWireReduction struct {
	durations []loadStandRouteWireDuration
	rates     loadStandRouteWireRates
}

// verifyPairs joins the two ends by their original TLS session ID. Frame
// totals and digests are compared after matching, never used for matching.
func (w *loadStandRouteWireWindow) verifyPairs(starts map[loadStandNodeID]loadstand.StartReceipt, reports []loadStandRouteWireFinal) (*loadStandRouteWireReduction, error) {
	if w.bounds.Begin <= 0 || w.bounds.End <= w.bounds.Begin || len(w.members) < 2 || len(reports) != len(w.members) {
		return nil, loadstand.ErrFinalAccountingUnavailable
	}
	identities := make(map[domain.PeerIdentity]bool, len(w.members))
	members := make(map[loadstand.MemberID]loadstand.StartReceipt, len(w.members))
	for _, member := range w.members {
		start := starts[member.id]
		if start.Public.Identity.IsZero() || identities[start.Public.Identity] || start.Incarnation.Ordinal == 0 || start.Incarnation.StartedAt.IsZero() {
			return nil, loadstand.ErrFinalAccountingUnavailable
		}
		identities[start.Public.Identity] = true
		members[loadstand.MemberID(member.id)] = start
	}
	pairs := make(map[loadstand.LaunchFingerprint]loadStandRouteWirePairEndpoints)
	hasFullReceipt := false
	for _, report := range reports {
		start, found := members[report.Node]
		if !found || report.Incarnation != start.Incarnation || report.Accounting.Bounds != w.bounds || len(report.Accounting.Endpoints) == 0 {
			return nil, loadstand.ErrFinalAccountingUnavailable
		}
		delete(members, report.Node)
		seen := make(map[domain.ConnID]bool, len(report.Accounting.Endpoints))
		remainingFullReceiptRows := w.fullReceiptRouteBudget
		for i := range report.Accounting.Endpoints {
			end := &report.Accounting.Endpoints[i]
			if end.ConnID == 0 || seen[end.ConnID] || end.SessionID == (loadstand.LaunchFingerprint{}) ||
				!end.Complete || !identities[end.Peer] || end.Peer == start.Public.Identity || !end.Interval.Within(w.bounds.Begin, w.bounds.End) {
				return nil, loadstand.ErrFinalReportInvalid
			}
			seen[end.ConnID] = true
			if (end.FullReceive != nil) != (w.fullReceiptRouteBudget > 0 && end.Received.FullV3.Frames > 0) {
				return nil, loadstand.ErrFinalAccountingUnavailable
			}
			if (end.FullSend != nil) != (w.fullReceiptRouteBudget > 0 && end.Sent.FullV3.Frames > 0) {
				return nil, loadstand.ErrFinalAccountingUnavailable
			}
			if full := end.FullSend; full != nil && w.requiredFullSource != "" && full.PassesFrom(w.requiredFullSource) != full.Passes {
				return nil, fmt.Errorf("%w: full pass outside the %s source profile", loadstand.ErrFinalAccountingUnavailable, w.requiredFullSource)
			}
			if receipt := end.FullReceive; receipt != nil {
				if receipt.Destinations <= 0 || receipt.Destinations > remainingFullReceiptRows {
					return nil, loadstand.ErrFinalReportInvalid
				}
				remainingFullReceiptRows -= receipt.Destinations
				hasFullReceipt = true
			}
			pair := pairs[end.SessionID]
			observed := loadStandRouteWireObservedEnd{node: report.Node, identity: start.Public.Identity, row: end}
			switch end.Direction {
			case domain.PeerDirectionOutbound:
				if pair.outbound.row != nil {
					return nil, fmt.Errorf("%w: duplicate outbound session ID", loadstand.ErrFinalReportInvalid)
				}
				pair.outbound = observed
			case domain.PeerDirectionInbound:
				if pair.inbound.row != nil {
					return nil, fmt.Errorf("%w: duplicate inbound session ID", loadstand.ErrFinalReportInvalid)
				}
				pair.inbound = observed
			default:
				return nil, loadstand.ErrFinalReportInvalid
			}
			pairs[end.SessionID] = pair
		}
	}
	if w.fullReceiptRouteBudget > 0 && !hasFullReceipt {
		return nil, loadstand.ErrFinalAccountingUnavailable
	}
	observed := false
	matchedFullSize := false
	reduction := &loadStandRouteWireReduction{rates: loadStandRouteWireRates{WindowNS: w.bounds.End - w.bounds.Begin}}
	type nodeTotals struct{ sent, received loadStandRouteWireClassBytes }
	totals := make(map[loadstand.MemberID]*nodeTotals, len(w.members))
	for _, member := range w.members {
		totals[loadstand.MemberID(member.id)] = &nodeTotals{}
	}
	ids := slices.SortedFunc(maps.Keys(pairs), func(a, b loadstand.LaunchFingerprint) int { return bytes.Compare(a[:], b[:]) })
	for _, id := range ids {
		pair := pairs[id]
		out, in := pair.outbound.row, pair.inbound.row
		if out == nil || in == nil {
			return nil, fmt.Errorf("%w: missing reciprocal endpoint", loadstand.ErrFinalAccountingUnavailable)
		}
		if out.Peer != pair.inbound.identity || in.Peer != pair.outbound.identity {
			return nil, fmt.Errorf("%w: nonreciprocal session identities", loadstand.ErrFinalReportInvalid)
		}
		if w.dialer != "" && pair.outbound.node != w.dialer {
			return nil, fmt.Errorf("%w: session originated outside the planned initiator", loadstand.ErrFinalAccountingUnavailable)
		}
		if w.dialer != "" && !w.requireReconnect && (len(pairs) != 1 ||
			out.Interval.BeginAfter != w.bounds.Begin || in.Interval.BeginAfter != w.bounds.Begin ||
			out.Interval.EndBefore != w.bounds.End || in.Interval.EndBefore != w.bounds.End) {
			return nil, fmt.Errorf("%w: periodic window needs one stable TLS session across both boundaries", loadstand.ErrFinalAccountingUnavailable)
		}
		if out.Sent != in.Received || in.Sent != out.Received {
			return nil, fmt.Errorf("%w: reciprocal frame totals or digests differ", loadstand.ErrFinalReportInvalid)
		}
		if !matchLoadStandFullProjection(out.FullSend, in.FullReceive) || !matchLoadStandFullProjection(in.FullSend, out.FullReceive) {
			return nil, fmt.Errorf("%w: full projection does not match reciprocal receipt", loadstand.ErrFinalReportInvalid)
		}
		matchedFullSize = matchedFullSize || w.matchesRequiredFull(pair.outbound.node, out.FullSend) ||
			w.matchesRequiredFull(pair.inbound.node, in.FullSend)
		minimum, maximum := out.Interval.Overlap(in.Interval)
		if minimum <= 0 || maximum < minimum {
			return nil, fmt.Errorf("%w: no proven common registered lifetime", loadstand.ErrFinalAccountingUnavailable)
		}
		begin := max(out.Interval.BeginAfter, in.Interval.BeginAfter)
		end := min(out.Interval.EndBefore, in.Interval.EndBefore)
		if !out.FramesWithin(begin, end) || !in.FramesWithin(begin, end) {
			return nil, fmt.Errorf("%w: observed frames outside common registered lifetime", loadstand.ErrFinalReportInvalid)
		}
		sent, received := loadStandRouteWireBytes(out.Sent), loadStandRouteWireBytes(in.Sent)
		dialer, listener := totals[pair.outbound.node], totals[pair.inbound.node]
		if !dialer.sent.add(sent) || !dialer.received.add(received) || !listener.sent.add(received) || !listener.received.add(sent) {
			return nil, loadstand.ErrFinalReportInvalid
		}
		forward, err := sent.rates(minimum, maximum)
		if err != nil {
			return nil, err
		}
		reverse, err := received.rates(minimum, maximum)
		if err != nil {
			return nil, err
		}
		reduction.durations = append(reduction.durations, loadStandRouteWireDuration{SessionID: id, Minimum: minimum, Maximum: maximum})
		reduction.rates.Sessions = append(reduction.rates.Sessions, loadStandRouteWireSessionRates{
			SessionID: id, Dialer: pair.outbound.node, Listener: pair.inbound.node,
			DialerToListener: forward, ListenerToDialer: reverse,
		})
		observed = observed || out.Sent != (loadstand.RouteWireCounts{}) || in.Sent != (loadstand.RouteWireCounts{})
	}
	if w.requireReconnect {
		if err := w.verifyReconnect(pairs); err != nil {
			return nil, err
		}
	}
	if !observed {
		return nil, fmt.Errorf("%w: no route frames in window", loadstand.ErrFinalAccountingUnavailable)
	}
	if w.requiredFullRoutes > 0 && !matchedFullSize {
		return nil, fmt.Errorf("%w: no reciprocal full projection matching the %d-route target", loadstand.ErrFinalAccountingUnavailable, w.requiredFullRoutes)
	}
	for _, id := range slices.Sorted(maps.Keys(totals)) {
		node := totals[id]
		sent, err := node.sent.rates(reduction.rates.WindowNS, reduction.rates.WindowNS)
		if err != nil {
			return nil, err
		}
		received, err := node.received.rates(reduction.rates.WindowNS, reduction.rates.WindowNS)
		if err != nil {
			return nil, err
		}
		reduction.rates.Nodes = append(reduction.rates.Nodes, loadStandRouteWireNodeRates{Node: id, Sent: sent, Received: received})
	}
	return reduction, nil
}

// The census clips lifetimes to the common window, so an endpoint at a
// boundary was proven registered across it. Transitions crossing either
// boundary are rejected before the census is sealed.
func (w *loadStandRouteWireWindow) verifyReconnect(pairs map[loadstand.LaunchFingerprint]loadStandRouteWirePairEndpoints) error {
	if len(w.members) != 2 || len(pairs) != 2 || w.requiredFullSource != domain.RouteWireFullConnect {
		return fmt.Errorf("%w: reconnect requires exactly two reciprocal TLS sessions between the original members", loadstand.ErrFinalAccountingUnavailable)
	}
	var previous, replacement loadStandRouteWirePairEndpoints
	for _, pair := range pairs {
		if pair.outbound.row.Interval.BeginAfter == w.bounds.Begin && pair.inbound.row.Interval.BeginAfter == w.bounds.Begin {
			previous = pair
		} else {
			replacement = pair
		}
	}
	if previous.outbound.row == nil || replacement.outbound.row == nil {
		return fmt.Errorf("%w: reconnect requires a session registered at opening and a replacement", loadstand.ErrFinalAccountingUnavailable)
	}
	oldOut, oldIn := previous.outbound.row, previous.inbound.row
	newOut, newIn := replacement.outbound.row, replacement.inbound.row
	if w.disconnectAt <= w.bounds.Begin || w.disconnectAt >= min(oldOut.Interval.EndBefore, oldIn.Interval.EndBefore) {
		return fmt.Errorf("%w: the old session did not cover the planned disconnect time", loadstand.ErrFinalAccountingUnavailable)
	}
	if max(oldOut.Interval.EndAfter, oldIn.Interval.EndAfter) >= min(newOut.Interval.BeginBefore, newIn.Interval.BeginBefore) ||
		newOut.Interval.EndBefore != w.bounds.End || newIn.Interval.EndBefore != w.bounds.End {
		return fmt.Errorf("%w: both old endpoints must retire before either replacement registers, and both new endpoints must remain at close", loadstand.ErrFinalAccountingUnavailable)
	}
	if oldOut.FullSend != nil || oldIn.FullSend != nil {
		return fmt.Errorf("%w: reconnect full pass occurred on the old TLS session", loadstand.ErrFinalAccountingUnavailable)
	}
	return nil
}

func (w *loadStandRouteWireWindow) matchesRequiredFull(sender loadstand.MemberID, proof *domain.RouteWireFullSend) bool {
	return proof != nil && proof.Destinations == w.requiredFullRoutes &&
		(w.seedSender == "" || sender == w.seedSender && proof.IdentitySHA256 == w.seedSHA256)
}

func matchLoadStandFullProjection(send *domain.RouteWireFullSend, receive *domain.RouteWireFullReceipt) bool {
	if send == nil || receive == nil {
		return send == nil && receive == nil
	}
	return send.ValidPasses() && send.Destinations == receive.Destinations && send.Epoch == receive.Epoch &&
		send.IdentitySHA256 == receive.IdentitySHA256
}

func TestLoadStandRouteWirePairsRequireReciprocalReports(t *testing.T) {
	const left, right = "full-1", "full-2"
	bounds := loadstand.RouteWireWindowBounds{Begin: 100, End: 200}
	window := loadStandRouteWireWindow{bounds: bounds, members: []loadStandRouteWireMember{{left, 3}, {right, 3}}}
	life := loadstand.Incarnation{Ordinal: 1, StartedAt: time.Unix(1, 0)}
	starts := map[loadStandNodeID]loadstand.StartReceipt{
		left:  {Incarnation: life, Public: loadstand.PublicIdentity{Identity: idPeerA}},
		right: {Incarnation: life, Public: loadstand.PublicIdentity{Identity: idPeerB}},
	}
	counts := loadstand.RouteWireCounts{FullV3: loadstand.RouteWireCount{Frames: 1, Bytes: 43, SHA256: "1b3b938e14bb444b1f626d90516df305e594206cbd5f26963393c709aab64458"}}
	for _, tc := range []struct {
		name   string
		change func([]loadStandRouteWireFinal)
		want   int
		err    error
	}{
		{"two directions", func([]loadStandRouteWireFinal) {}, 2, nil},
		{"nonzero return traffic", func(r []loadStandRouteWireFinal) {
			ack := loadstand.RouteWireCount{Frames: 1, Bytes: 9, SHA256: counts.FullV3.SHA256}
			r[0].Accounting.Endpoints[0].Received.Ack = ack
			r[1].Accounting.Endpoints[0].Sent.Ack = ack
			r[0].Accounting.Endpoints[0].ReceivedSpan = domain.RouteWireFrameSpan{First: 165, Last: 165}
			r[1].Accounting.Endpoints[0].SentSpan = domain.RouteWireFrameSpan{First: 160, Last: 160}
		}, 2, nil},
		{"same direction and identical counters in different sessions", func(r []loadStandRouteWireFinal) {
			for side := range r {
				other := r[side].Accounting.Endpoints[0]
				other.ConnID += 10
				other.SessionID = loadstand.LaunchFingerprint{3}
				r[side].Accounting.Endpoints = append(r[side].Accounting.Endpoints, other)
			}
		}, 3, nil},
		{"missing reciprocal", func(r []loadStandRouteWireFinal) { r[1].Accounting.Endpoints = r[1].Accounting.Endpoints[1:] }, 0, loadstand.ErrFinalAccountingUnavailable},
		{"wrong session", func(r []loadStandRouteWireFinal) { r[1].Accounting.Endpoints[0].SessionID[0] = 7 }, 0, loadstand.ErrFinalAccountingUnavailable},
		{"missing session", func(r []loadStandRouteWireFinal) {
			r[1].Accounting.Endpoints[0].SessionID = loadstand.LaunchFingerprint{}
		}, 0, loadstand.ErrFinalReportInvalid},
		{"wrong digest", func(r []loadStandRouteWireFinal) { r[1].Accounting.Endpoints[0].Received.FullV3.SHA256 = "different" }, 0, loadstand.ErrFinalReportInvalid},
		{"wrong bytes", func(r []loadStandRouteWireFinal) { r[1].Accounting.Endpoints[0].Received.FullV3.Bytes++ }, 0, loadstand.ErrFinalReportInvalid},
		{"duplicate session", func(r []loadStandRouteWireFinal) {
			other := r[0].Accounting.Endpoints[0]
			other.ConnID = 3
			r[0].Accounting.Endpoints = append(r[0].Accounting.Endpoints, other)
		}, 0, loadstand.ErrFinalReportInvalid},
		{"different incarnation", func(r []loadStandRouteWireFinal) { r[1].Incarnation.Ordinal++ }, 0, loadstand.ErrFinalAccountingUnavailable},
		{"wrong window", func(r []loadStandRouteWireFinal) { r[1].Accounting.Bounds.End++ }, 0, loadstand.ErrFinalAccountingUnavailable},
		{"unproven common interval", func(r []loadStandRouteWireFinal) {
			r[1].Accounting.Endpoints[0].Interval = domain.RouteWireSessionInterval{BeginBefore: 179, BeginAfter: 183, EndBefore: 190, EndAfter: 195}
		}, 0, loadstand.ErrFinalAccountingUnavailable},
		{"frame locally registered but outside common interval", func(r []loadStandRouteWireFinal) {
			r[0].Accounting.Endpoints[0].SentSpan.First = 124
		}, 0, loadstand.ErrFinalReportInvalid},
		{"zero traffic", func(r []loadStandRouteWireFinal) {
			r[0].Accounting.Endpoints[0].Sent = loadstand.RouteWireCounts{}
			r[1].Accounting.Endpoints[0].Received = loadstand.RouteWireCounts{}
			r[0].Accounting.Endpoints[0].SentSpan = domain.RouteWireFrameSpan{}
			r[1].Accounting.Endpoints[0].ReceivedSpan = domain.RouteWireFrameSpan{}
		}, 0, loadstand.ErrFinalAccountingUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reports := []loadStandRouteWireFinal{
				{loadStandRouteWireLife: loadStandRouteWireLife{Node: left, Incarnation: life}, Accounting: loadstand.HeadlessRouteWireAccounting{Bounds: bounds, Endpoints: []loadstand.RouteWireEndpoint{
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 1, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: loadstand.LaunchFingerprint{1}}, Complete: true, Sent: counts},
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 2, Peer: idPeerB, Direction: domain.PeerDirectionInbound, SessionID: loadstand.LaunchFingerprint{2}}, Complete: true},
				}}},
				{loadStandRouteWireLife: loadStandRouteWireLife{Node: right, Incarnation: life}, Accounting: loadstand.HeadlessRouteWireAccounting{Bounds: bounds, Endpoints: []loadstand.RouteWireEndpoint{
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 7, Peer: idPeerA, Direction: domain.PeerDirectionInbound, SessionID: loadstand.LaunchFingerprint{1}}, Complete: true, Received: counts},
					{RouteWireEndpointKey: loadstand.RouteWireEndpointKey{ConnID: 8, Peer: idPeerA, Direction: domain.PeerDirectionOutbound, SessionID: loadstand.LaunchFingerprint{2}}, Complete: true},
				}}},
			}
			for _, report := range reports {
				for i := range report.Accounting.Endpoints {
					report.Accounting.Endpoints[i].Interval = domain.RouteWireSessionInterval{BeginBefore: 100, BeginAfter: 100, EndBefore: 200, EndAfter: 200}
				}
			}
			reports[0].Accounting.Endpoints[0].Interval = domain.RouteWireSessionInterval{BeginBefore: 110, BeginAfter: 120, EndBefore: 180, EndAfter: 185}
			reports[1].Accounting.Endpoints[0].Interval = domain.RouteWireSessionInterval{BeginBefore: 115, BeginAfter: 125, EndBefore: 170, EndAfter: 190}
			reports[0].Accounting.Endpoints[0].SentSpan = domain.RouteWireFrameSpan{First: 145, Last: 145}
			reports[1].Accounting.Endpoints[0].ReceivedSpan = domain.RouteWireFrameSpan{First: 155, Last: 155}
			tc.change(reports)
			got, err := window.verifyPairs(starts, reports)
			if tc.err != nil {
				if !errors.Is(err, tc.err) || got != nil {
					t.Fatalf("unverified evidence produced rates: %+v, %v, want %v", got, err, tc.err)
				}
				return
			}
			if err != nil || got == nil || len(got.durations) != tc.want || len(got.rates.Sessions) != tc.want ||
				got.rates.WindowNS != 100 || len(got.rates.Nodes) != 2 {
				t.Fatalf("unexpected reduction: %+v, %v", got, err)
			}
			if got.durations[0] != (loadStandRouteWireDuration{SessionID: loadstand.LaunchFingerprint{1}, Minimum: 45, Maximum: 70}) {
				t.Fatalf("incorrect common duration: %+v", got.durations[0])
			}
			session := got.rates.Sessions[0]
			if session.SessionID != got.durations[0].SessionID || session.Dialer != left || session.Listener != right ||
				session.DialerToListener.FullV3 != (loadStandRouteWireByteRate{Bytes: 43, MinBytesPerSecond: 614285714, MaxBytesPerSecond: 955555556}) ||
				session.DialerToListener.DeltaV3 != (loadStandRouteWireByteRate{}) {
				t.Fatalf("unexpected session rates: %+v", session)
			}
			leftRates, rightRates := got.rates.Nodes[0], got.rates.Nodes[1]
			total := uint64(43) * uint64(tc.want-1) // the reciprocal idle pair carries zero
			wantTotal := loadStandRouteWireByteRate{Bytes: total, MinBytesPerSecond: total * 10_000_000, MaxBytesPerSecond: total * 10_000_000}
			if leftRates.Node != left || rightRates.Node != right || leftRates.Sent.FullV3 != wantTotal ||
				rightRates.Received.FullV3 != wantTotal || leftRates.Received.FullV3 != (loadStandRouteWireByteRate{}) {
				t.Fatalf("incorrect node aggregation: %+v", got.rates.Nodes)
			}
			if tc.name == "nonzero return traffic" &&
				(session.ListenerToDialer.Ack != (loadStandRouteWireByteRate{Bytes: 9, MinBytesPerSecond: 128571428, MaxBytesPerSecond: 200000000}) ||
					leftRates.Received.Ack != (loadStandRouteWireByteRate{Bytes: 9, MinBytesPerSecond: 90000000, MaxBytesPerSecond: 90000000}) ||
					leftRates.Received.Ack != rightRates.Sent.Ack || leftRates.Sent.Ack != (loadStandRouteWireByteRate{})) {
				t.Fatalf("incorrect reverse direction: %+v, %+v", session, got.rates.Nodes)
			}
			if tc.name == "two directions" {
				withReceipt := window
				withReceipt.fullReceiptRouteBudget = 1
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("required receipt missing: %+v, %v", got, err)
				}
				receipt := &domain.RouteWireFullReceipt{Destinations: 1, IdentitySHA256: counts.FullV3.SHA256, Epoch: 1, SnapshotAfterNS: 201}
				reports[1].Accounting.Endpoints[0].FullReceive = receipt
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("required sender projection missing: %+v, %v", got, err)
				}
				projection := &domain.RouteWireFullSend{Destinations: 1, IdentitySHA256: receipt.IdentitySHA256, Epoch: receipt.Epoch, Passes: 1, PeriodicPasses: 1}
				reports[0].Accounting.Endpoints[0].FullSend = projection
				if _, err := withReceipt.verifyPairs(starts, reports); err != nil {
					t.Fatalf("paired full receipt rejected: %v", err)
				}
				withReceipt.fullReceiptRouteBudget = 10_000
				withReceipt.requiredFullRoutes = 10_000
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("undersized full projection produced rates: %+v, %v", got, err)
				}
				projection.Destinations, receipt.Destinations = 10_000, 10_000
				if _, err := withReceipt.verifyPairs(starts, reports); err != nil {
					t.Fatalf("full projection of the required size rejected: %v", err)
				}
				withReceipt.seedSender, withReceipt.seedSHA256 = right, projection.IdentitySHA256
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("wrong fixture sender produced rates: %+v, %v", got, err)
				}
				withReceipt.seedSender, withReceipt.seedSHA256 = left, loadstandRouteWireSeedDigest(10_000)
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("wrong fixture identity set produced rates: %+v, %v", got, err)
				}
				projection.IdentitySHA256, receipt.IdentitySHA256 = withReceipt.seedSHA256, withReceipt.seedSHA256
				if _, err := withReceipt.verifyPairs(starts, reports); err != nil {
					t.Fatalf("original fixture sender and identities rejected: %v", err)
				}
				withReceipt.requiredFullSource = domain.RouteWireFullPeriodic
				if _, err := withReceipt.verifyPairs(starts, reports); err != nil {
					t.Fatalf("periodic source rejected in periodic window: %v", err)
				}
				stable := withReceipt
				stable.dialer = left
				if _, err := stable.verifyPairs(starts, reports); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("one-way periodic window accepted multiple sessions: %v", err)
				}
				stableReports := slices.Clone(reports)
				for i := range stableReports {
					stableReports[i].Accounting.Endpoints = slices.Clone(reports[i].Accounting.Endpoints[:1])
				}
				if _, err := stable.verifyPairs(starts, stableReports); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
					t.Fatalf("one-way periodic window accepted a session spanning only part of it: %v", err)
				}
				for i := range stableReports {
					stableReports[i].Accounting.Endpoints[0].Interval = domain.RouteWireSessionInterval{
						BeginBefore: bounds.Begin, BeginAfter: bounds.Begin, EndBefore: bounds.End, EndAfter: bounds.End,
					}
				}
				if _, err := stable.verifyPairs(starts, stableReports); err != nil {
					t.Fatalf("one-way periodic window rejected a stable TLS session: %v", err)
				}
				for _, other := range []struct{ triggered, connect uint64 }{{1, 0}, {0, 1}} {
					projection.PeriodicPasses, projection.TriggeredPasses, projection.ConnectPasses = 0, other.triggered, other.connect
					if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
						t.Fatalf("nonperiodic full produced periodic rates: %+v, %v", got, err)
					}
				}
				withReceipt.requiredFullSource = domain.RouteWireFullConnect
				if _, err := withReceipt.verifyPairs(starts, reports); err != nil {
					t.Fatalf("connect source rejected in connect window: %v", err)
				}
				withReceipt.requiredFullSource = ""
				withReceipt.seedSender, withReceipt.seedSHA256 = "", ""
				withReceipt.fullReceiptRouteBudget, withReceipt.requiredFullRoutes = 1, 1
				projection.Destinations, receipt.Destinations = 1, 1
				projection.Epoch++
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalReportInvalid) {
					t.Fatalf("receipt for different projection accepted: %+v, %v", got, err)
				}
				projection.Epoch--
				receipt.Destinations = 2
				if got, err := withReceipt.verifyPairs(starts, reports); got != nil || !errors.Is(err, loadstand.ErrFinalReportInvalid) {
					t.Fatalf("receipt exceeding node budget: %+v, %v", got, err)
				}
			}
		})
	}
}
