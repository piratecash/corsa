package node

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// The stand's byte figures are the node's transport counters
// (Service.TransportTrafficStats, docs/refactoring/dht/05-rollout-metrics.md
// §5.3): every byte read from or written to a peer socket, counted once, at
// the socket, for the life of the node's Service — one incarnation — never
// persisted and never decreasing. Within one incarnation a byte window is therefore always
// defined, whatever connections opened, closed or were evicted between its
// samples — the only pair rules are §5.1.1's: the same started_at, and the
// later read strictly after the earlier.

// loadStandByteWindow is what a node's sockets carried within one
// incarnation.
type loadStandByteWindow struct {
	Period   loadStandPeriod
	Sent     loadStandByteCount
	Received loadStandByteCount
}

func (r *loadStandDeltaReader) bytes(before, after domain.TransportTrafficStats) loadStandByteWindow {
	return loadStandByteWindow{
		Period:   loadStandPeriod{From: before.ReadAt, To: after.ReadAt},
		Sent:     loadStandByteCount(r.count("transport_bytes_sent", before.BytesSent, after.BytesSent)),
		Received: loadStandByteCount(r.count("transport_bytes_received", before.BytesReceived, after.BytesReceived)),
	}
}

// --- byte balance ---

// loadStandShare is a fraction of a volume, 0.1 meaning 10 %.
type loadStandShare float64

// loadStandBurstFactor scales the stand's average rate over the window to
// the rate that may hold while a sweep is being read.
type loadStandBurstFactor int

// loadStandEndedIncarnation is a node life that has finished, with the
// last reading of its transport counters. The byte balance needs it for a
// life that ended inside a span: nothing else says what that life carried.
//
// In this process the reading is taken after Run returned (exact: the
// counters are atomics that outlive Run, and no socket is left to move
// them) and TailAllowance is zero. A source in another process can only read
// before it stops the node; it states how much the stop itself may still
// carry as TailAllowance.
type loadStandEndedIncarnation struct {
	Node          loadStandNodeID
	Life          loadStandLife
	Final         domain.TransportTrafficStats
	TailAllowance loadStandByteCount
}

// loadStandBalanceTolerance is how far the stand's bytes sent and received
// may disagree over a span, and when the check is too coarse to say
// anything.
//
// Every node of the stand lives in this process and every peer socket is
// metered (all five places a peer socket is born, sync dials and notices
// included), so every byte one node wrote is a byte another node of the
// stand reads. A node's bytes over the span are summed over every life the
// span contains: the life sampled at the earlier edge from that reading, a
// life that began inside the span from zero (its counters start with it),
// a life that ended inside the span to its final reading. Σ sent and
// Σ received over all nodes then differ only by bytes on the way, and the
// allowance bounds exactly that:
//
//  1. Read skew. A sweep reads node after node; a byte written by a node
//     read early and read by a node read late (or the reverse) is on one
//     side of the edge only. Bounded by the stand's rate over the span times
//     how long each sweep took to read the counters:
//     BurstFactor × volume × (spread₁ + spread₂) / inner, where inner is the
//     interval between the earlier sweep's last read and the later sweep's
//     first. Lives that began or ended inside the span are exact at that
//     end and add no skew.
//  2. Bytes in flight at the later edge — written, not yet read, sitting in
//     a socket buffer: PerNodeInFlight per node then running.
//  3. Bytes stranded by a stop — written to a stopping node and never read:
//     PerNodeInFlight per life that ended inside the span, plus the life's
//     own TailAllowance.
//
// An allowance above MaxAllowanceShare of the volume makes the check blind —
// a defect smaller than the allowance hides in it — so such a result is
// UNDEFINED, not passed. 10 % is the default because the check guards against
// systematic accounting errors (a byte counted at two layers, a socket
// metered twice or not at all), each of which moves a large share of the
// volume; under a tenth, any such error touching at least a tenth of the
// traffic shows. A longer span between the two sweeps (they need not be
// adjacent) shrinks the skew term against the volume and buys the power
// back.
type loadStandBalanceTolerance struct {
	BurstFactor loadStandBurstFactor
	// PerNodeInFlight is PROVISIONAL until the T5 smoke run measures the
	// bytes a stand node holds in flight at an instant; the default is a
	// guess of a few frames per connection, not a measurement.
	PerNodeInFlight   loadStandByteCount
	MaxAllowanceShare loadStandShare
}

func loadStandDefaultBalanceTolerance() loadStandBalanceTolerance {
	return loadStandBalanceTolerance{BurstFactor: 2, PerNodeInFlight: 64 << 10, MaxAllowanceShare: 0.10}
}

// loadStandBalance is the outcome of one balance, carried with every result
// — passed, defect or undefined — so the power of the check is never implicit.
type loadStandBalance struct {
	Nodes int
	// Stops is how many lives ended inside the span.
	Stops    int
	Sent     loadStandByteCount
	Received loadStandByteCount
	// Volume is the larger of Sent and Received.
	Volume    loadStandByteCount
	Imbalance loadStandByteCount
	Allowance loadStandByteCount
	// AllowanceShare is Allowance / Volume; +Inf when nothing was carried.
	AllowanceShare loadStandShare
}

// checkLoadStandByteBalance checks that the stand, a closed system on
// loopback, received what it sent between two sweeps — adjacent or not, and
// whatever nodes stopped, started or restarted between them, as long as
// ended holds the final reading of every life that ended inside the span.
// It returns errLoadStandBalanceUndefined when it cannot say, a
// *loadStandDefect when the sums disagree beyond the allowance, nil
// otherwise — and the balance it computed in every case it got that far.
func checkLoadStandByteBalance(earlier, later loadStandSweep, ended []loadStandEndedIncarnation, tolerance loadStandBalanceTolerance) (loadStandBalance, error) {
	if len(earlier.Nodes) == 0 || len(later.Nodes) == 0 {
		return loadStandBalance{}, fmt.Errorf("%w: a sweep sampled no node (%d, then %d)", errLoadStandBalanceUndefined, len(earlier.Nodes), len(later.Nodes))
	}
	edges := loadStandBalanceEdges(earlier, later)
	if edges.inner <= 0 {
		return loadStandBalance{}, fmt.Errorf("%w: the later sweep began reading before the earlier one finished", errLoadStandBalanceUndefined)
	}
	spans, err := loadStandNodeSpans(earlier, later, ended)
	if err != nil {
		return loadStandBalance{}, err
	}
	balance := loadStandBalance{Nodes: len(spans)}
	var tails loadStandByteCount
	for _, span := range spans {
		carried, err := span.carried(earlier, later)
		if err != nil {
			return loadStandBalance{}, fmt.Errorf("%w: %w", errLoadStandBalanceUndefined, err)
		}
		balance.Sent += carried.sent
		balance.Received += carried.received
		balance.Stops += carried.stops
		tails += carried.tails
	}
	balance.Volume = max(balance.Sent, balance.Received)
	balance.Imbalance = balance.Volume - min(balance.Sent, balance.Received)
	allowance := float64(tolerance.BurstFactor)*float64(balance.Volume)*float64(edges.spread)/float64(edges.inner) +
		float64(tolerance.PerNodeInFlight)*float64(len(later.Nodes)+balance.Stops) + float64(tails)
	balance.Allowance = loadStandByteCount(math.Ceil(allowance))
	balance.AllowanceShare = loadStandShare(math.Inf(1))
	if balance.Volume > 0 {
		balance.AllowanceShare = loadStandShare(float64(balance.Allowance) / float64(balance.Volume))
	}
	return balance, tolerance.judge(balance)
}

// loadStandEdges are the timing of a balance: how long each sweep took to
// read every node's byte counters, added up, and the interval between the
// earlier sweep's last read and the later sweep's first.
type loadStandEdges struct {
	spread time.Duration
	inner  time.Duration
}

func loadStandBalanceEdges(earlier, later loadStandSweep) loadStandEdges {
	earliestFrom, latestFrom := loadStandTrafficReadRange(earlier)
	earliestTo, latestTo := loadStandTrafficReadRange(later)
	return loadStandEdges{
		spread: latestFrom.Sub(earliestFrom) + latestTo.Sub(earliestTo),
		inner:  earliestTo.Sub(latestFrom),
	}
}

// loadStandTrafficReadRange is the first and last transport read of a
// non-empty sweep.
func loadStandTrafficReadRange(sweep loadStandSweep) (time.Time, time.Time) {
	first, last := sweep.Nodes[0].Traffic.ReadAt, sweep.Nodes[0].Traffic.ReadAt
	for _, sample := range sweep.Nodes[1:] {
		first, last = loadStandEarlier(first, sample.Traffic.ReadAt), loadStandLater(last, sample.Traffic.ReadAt)
	}
	return first, last
}

func loadStandEarlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func loadStandLater(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (t loadStandBalanceTolerance) judge(balance loadStandBalance) error {
	switch {
	case balance.Volume == 0:
		return fmt.Errorf("%w: nothing was carried between the sweeps", errLoadStandBalanceUndefined)
	case balance.AllowanceShare > t.MaxAllowanceShare:
		return fmt.Errorf("%w: the allowance %d is %.1f%% of the volume %d, above %.1f%%: the check would be blind",
			errLoadStandBalanceUndefined, balance.Allowance, 100*float64(balance.AllowanceShare), balance.Volume, 100*float64(t.MaxAllowanceShare))
	case balance.Imbalance > balance.Allowance:
		return &loadStandDefect{
			Kind: loadStandDefectByteBalance,
			Detail: fmt.Sprintf("%d nodes (%d stops) sent %d bytes and received %d: imbalance %d over the allowance %d (%.1f%% of the volume)",
				balance.Nodes, balance.Stops, balance.Sent, balance.Received, balance.Imbalance, balance.Allowance, 100*float64(balance.AllowanceShare)),
		}
	default:
		return nil
	}
}

// loadStandNodeSpan is what the balance knows of one node over a span: its
// samples at the two edges, when it was running then, and the lives that
// ended.
type loadStandNodeSpan struct {
	node  loadStandNodeID
	from  *loadStandNodeSample
	to    *loadStandNodeSample
	ended map[loadStandLife]loadStandEndedIncarnation
}

// loadStandNodeSpans gathers every node seen at either edge or with a life
// that ended — the balance needs the whole stand.
func loadStandNodeSpans(earlier, later loadStandSweep, ended []loadStandEndedIncarnation) (map[loadStandNodeID]*loadStandNodeSpan, error) {
	spans := make(map[loadStandNodeID]*loadStandNodeSpan)
	spanOf := func(node loadStandNodeID) *loadStandNodeSpan {
		if spans[node] == nil {
			spans[node] = &loadStandNodeSpan{node: node, ended: make(map[loadStandLife]loadStandEndedIncarnation)}
		}
		return spans[node]
	}
	for i := range earlier.Nodes {
		spanOf(earlier.Nodes[i].Node).from = &earlier.Nodes[i]
	}
	for i := range later.Nodes {
		spanOf(later.Nodes[i].Node).to = &later.Nodes[i]
	}
	for _, life := range ended {
		span := spanOf(life.Node)
		if _, duplicate := span.ended[life.Life]; duplicate {
			return nil, fmt.Errorf("%w: %s life %d ended twice", errLoadStandBalanceUndefined, life.Node, life.Life)
		}
		span.ended[life.Life] = life
	}
	return spans, nil
}

// loadStandCarried is what one node's lives carried within a span.
type loadStandCarried struct {
	sent     loadStandByteCount
	received loadStandByteCount
	stops    int
	tails    loadStandByteCount
}

// carried sums the node's lives inside the span. A node that was neither
// running at an edge nor ended a life inside the span carried nothing here.
func (s *loadStandNodeSpan) carried(earlier, later loadStandSweep) (loadStandCarried, error) {
	first, last, present := s.lives(earlier, later)
	if !present {
		return loadStandCarried{}, nil
	}
	deltas := loadStandDeltaReader{node: s.node}
	var carried loadStandCarried
	for life := first; life <= last; life++ {
		start, err := s.lifeStart(life, earlier)
		if err != nil {
			return loadStandCarried{}, err
		}
		end, tail, stopped, err := s.lifeEnd(life, later)
		if err != nil {
			return loadStandCarried{}, err
		}
		if !end.StartedAt.Equal(start.StartedAt) {
			return loadStandCarried{}, fmt.Errorf("%w: %s life %d began at %s and ended as one begun at %s", errLoadStandWindowAcrossIncarnations,
				s.node, life, start.StartedAt.Format(time.RFC3339Nano), end.StartedAt.Format(time.RFC3339Nano))
		}
		bytes := deltas.bytes(start, end)
		carried.sent += bytes.Sent
		carried.received += bytes.Received
		carried.tails += tail
		if stopped {
			carried.stops++
		}
	}
	return carried, deltas.err
}

// lives is the range of lives the span holds for this node: from the one
// sampled at the earlier edge (or the first that ended inside the span)
// to the one sampled at the later edge (or the last that ended inside it).
func (s *loadStandNodeSpan) lives(earlier, later loadStandSweep) (loadStandLife, loadStandLife, bool) {
	var candidates []loadStandLife
	if s.from != nil {
		candidates = append(candidates, s.from.Incarnation.Life)
	}
	if s.to != nil {
		candidates = append(candidates, s.to.Incarnation.Life)
	}
	for life, ended := range s.ended {
		if ended.Final.ReadAt.After(earlier.End) && !ended.Final.ReadAt.After(later.Begin) {
			candidates = append(candidates, life)
		}
	}
	if len(candidates) == 0 {
		return 0, 0, false
	}
	return slices.Min(candidates), slices.Max(candidates), true
}

// lifeStart is the reading a life's bytes are counted from: the earlier
// sample for the life running at the earlier edge, zero (at the life's own
// start) for a life that began inside the span.
func (s *loadStandNodeSpan) lifeStart(life loadStandLife, earlier loadStandSweep) (domain.TransportTrafficStats, error) {
	if s.from != nil && s.from.Incarnation.Life == life {
		return s.from.Traffic, nil
	}
	startedAt, known := s.startedAt(life)
	switch {
	case !known:
		return domain.TransportTrafficStats{}, fmt.Errorf("%s life %d has neither a sample nor a final reading", s.node, life)
	case !startedAt.After(earlier.End):
		return domain.TransportTrafficStats{}, fmt.Errorf("%s life %d began at %s, before the earlier sweep ended, and was not sampled",
			s.node, life, startedAt.Format(time.RFC3339Nano))
	default:
		return domain.TransportTrafficStats{StartedAt: startedAt, ReadAt: startedAt}, nil
	}
}

// lifeEnd is the reading a life's bytes are counted to: the later sample for
// the life running at the later edge, the final reading for a life that
// ended inside the span.
func (s *loadStandNodeSpan) lifeEnd(life loadStandLife, later loadStandSweep) (domain.TransportTrafficStats, loadStandByteCount, bool, error) {
	if s.to != nil && s.to.Incarnation.Life == life {
		return s.to.Traffic, 0, false, nil
	}
	ended, known := s.ended[life]
	switch {
	case !known:
		return domain.TransportTrafficStats{}, 0, false, fmt.Errorf("%s life %d stopped without a final reading", s.node, life)
	case ended.Final.ReadAt.After(later.Begin):
		return domain.TransportTrafficStats{}, 0, false, fmt.Errorf("%s life %d ended after the later sweep began", s.node, life)
	default:
		return ended.Final, ended.TailAllowance, true, nil
	}
}

func (s *loadStandNodeSpan) startedAt(life loadStandLife) (time.Time, bool) {
	if s.to != nil && s.to.Incarnation.Life == life {
		return s.to.Traffic.StartedAt, true
	}
	if ended, known := s.ended[life]; known {
		return ended.Final.StartedAt, true
	}
	return time.Time{}, false
}
