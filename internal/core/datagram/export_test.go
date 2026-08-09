package datagram

// export_test.go is the ONLY way the external datagram_test package reaches
// internal state, and it exists so that "a test needs to see this" never turns
// into "the production API has an accessor nobody calls".
//
// Everything here is compiled into the test binary alone, so it is not API: a
// consumer of the package cannot reach it, and a reviewer counting the callers
// of an exported method is not misled into believing a rule has a production
// reader that it does not have.

// OwnerLoadForTest reports how many records the OWNER of this arrival holds in
// the base replay cache.
//
// The fairness rule of §5 charges per owner and never per channel, so this is
// what a test of it has to observe: two arrivals of one neighbour over two
// channels — a reconnect, or two parallel sessions — must answer the same
// number.
func (c *BaseReplayCache) OwnerLoadForTest(incoming IngressPeer) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ownerLoadLocked(incoming)
}

// Authority reports how much this node has been shown about the neighbour. The
// zero value is AuthorityClaimed — the weakest — so a value nobody set grants
// nothing.
//
// It is here rather than in replay_cache.go because the production readers all
// ask the QUESTION instead of the value: the fairness rule charges by owner()
// and the channel gate reads Proven() off PresentedIdentity. Only the tests of
// those two rules need the raw authority.
func (p IngressPeer) Authority() IngressAuthority { return p.authority }

// ProbeBudget returns the starting probe budget of a reverse record — the
// number a test compares ProbesLeft against, so the expectation does not
// hard-code DefaultReverseProbeBudget.
func (t *ReverseTable) ProbeBudget() int { return t.probes }

// Len returns the number of live reverse records. Production never asks: the
// table's own limits are enforced inside it, and the counters reach the
// diagnostic through ReverseEvent.
func (t *ReverseTable) Len() int {
	t.mu.Lock()
	defer t.unlockAndPublish()
	return len(t.entries)
}

// Len returns the total number of queued frames. Production reads Stats(),
// which publishes the per-class depths the diagnostic renders; this is the
// single total, which only a test asks for.
func (q *WeightedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	total := 0
	for _, target := range q.lanes {
		total += len(target.items)
	}
	return total
}

// Len returns the number of entries of the plan. Production either walks
// Entries() or asks the plan a question about a specific hop; the bare count
// is what an assertion needs and nothing else does.
func (p RoutePlan) Len() int { return len(p.entries) }

// DropCount returns the count of ONE refusal reason. Production renders the
// whole picture through Snapshot().DropsByReason — including the node package,
// which reads that map rather than this accessor — so a per-reason read has no
// caller outside an assertion.
func (m *Metrics) DropCount(reason DropReason) uint64 {
	if m == nil {
		return 0
	}
	return m.drops[reason].Load()
}

// ReverseCount is the same story for one reverse-state event: the diagnostic
// carries every event through Snapshot().ReverseEvents.
func (m *Metrics) ReverseCount(event ReverseEvent) uint64 {
	if m == nil {
		return 0
	}
	return m.reverse[event].Load()
}
