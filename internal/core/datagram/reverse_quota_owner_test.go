package datagram

import (
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// reverse_quota_owner_test.go pins the split this round is about: a reverse
// record answers TWO questions and the two are answered by DIFFERENT values —
// the CHANNEL says where the answer goes, the ADMISSION KEY says whose slot it
// is (§4.2, §5).
//
// Every fixture below therefore needs a shape none of the previous rounds' had:
// ONE neighbour on SEVERAL channels. With one channel per neighbour, "keyed on
// the channel" and "keyed on the owner" are the same observation, which is
// exactly how the previous round shipped a per-upstream quota a reconnect
// renews.

// quotaFixture is one bounded table plus the two spellings of a neighbour a
// quota test needs.
type quotaFixture struct {
	table *ReverseTable
	now   time.Time
}

func newQuotaFixture(t *testing.T, global, perUpstream int) *quotaFixture {
	t.Helper()
	fixture := &quotaFixture{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	fixture.table = NewReverseTable(ReverseTableConfig{
		Clock:  func() time.Time { return fixture.now },
		Limits: stubLimits{global: global, perUpstream: perUpstream},
	})
	return fixture
}

// upstreamLoad reports how many records the OWNER of this upstream currently
// holds — not its channel. Two values naming one owner over two channels answer
// the same number, because they are one bucket, and that is what every quota
// test in this package observes.
//
// It is a test helper and not a method of ReverseTable: the fairness cap reads
// the buckets through loadLocked on its own admission path, so an exported
// accessor beside it would be production API with no production reader — a
// reviewer counting callers would be told the rule has one.
func upstreamLoad(table *ReverseTable, upstream Upstream) int {
	table.mu.Lock()
	defer table.unlockAndPublish()
	return table.loadLocked(upstream)
}

// reserve takes one slot at an explicit instant, so "the oldest record of the
// busiest owner" is a fact of the fixture rather than of map iteration.
func (f *quotaFixture) reserve(seed string, upstream Upstream, at time.Time) ReverseReserveOutcome {
	return f.table.Reserve(ReverseReserveOpts{
		ReceivedAt: at,
		Label:      NewLabel(domaintest.ID(seed)),
		Dst:        domaintest.ID("target"),
		DType:      dtypeQuery,
		Upstream:   upstream,
	}).Outcome()
}

func (f *quotaFixture) live(seed string) bool {
	_, live := f.table.Lookup(NewLabel(domaintest.ID(seed)))
	return live
}

// provenNeighbour is one neighbour of an ACCEPTED connection as the receive path
// bills it: the identity is proven, so the SAME AdmissionKey is charged whatever
// socket the frames arrive on — which is the whole reason a quota can be keyed on
// it and survive a reconnect.
func provenNeighbour(name, session string) Upstream {
	id := domaintest.ID(name)
	return ChannelUpstream(testChannel(name+"/"+session), ProvenIdentityKey(id), id)
}

// ---------------------------------------------------------------------------
// (a) A reconnect does not renew the per-upstream quota
// ---------------------------------------------------------------------------

// TestReconnectDoesNotRenewThePerUpstreamQuota is the round's P1 on its cheapest
// observable consequence.
//
// A channel lives until its connection closes; a reverse record lives up to
// reverse_state_ttl, which is 240 s. So a quota keyed on the channel is a quota
// whose renewal the neighbour controls: fill the 64 slots, tear the session down,
// dial again, fill 64 more. The bucket has to be the thing the arrival is BILLED
// to, because that is what the receive path can defend across reconnects.
//
// The mutations this kills:
//
//   - keying upstreamKey on the channel again: the reconnected upstream then
//     opens a fresh bucket and the fourth reservation is Reserved, not Capped;
//   - keying it on the presented name: the positive control breaks instead —
//     nothing would distinguish two neighbours that both present a name;
//   - dropping the owner out of ChannelUpstream so every remote upstream keys on
//     the zero AdmissionKey: the honest neighbour then shares the flooder's
//     bucket and its own first reservation is refused.
func TestReconnectDoesNotRenewThePerUpstreamQuota(t *testing.T) {
	t.Parallel()

	fixture := newQuotaFixture(t, 64, 2)
	before := provenNeighbour("noisy", "session-1")
	after := provenNeighbour("noisy", "session-2")

	// The fixture is only a reconnect if the socket really changed. Without this
	// the whole test would pass against a channel-keyed bucket.
	channelBefore, _ := before.Channel()
	channelAfter, _ := after.Channel()
	if channelBefore == channelAfter {
		t.Fatal("the fixture reconnected onto the SAME channel: it cannot tell the two keyings apart")
	}

	if got := fixture.reserve("n1", before, fixture.now); got != ReverseSlotReserved {
		t.Fatalf("n1: %s", got)
	}
	if got := fixture.reserve("n2", before, fixture.now.Add(time.Second)); got != ReverseSlotReserved {
		t.Fatalf("n2: %s", got)
	}
	if got := fixture.reserve("n3", before, fixture.now.Add(2*time.Second)); got != ReverseSlotCapped {
		t.Fatalf("the per-upstream cap must refuse the third record of one neighbour, got %s", got)
	}

	// THE FINDING: the same neighbour on a FRESH channel, which is one tear-down
	// and one dial away.
	if got := fixture.reserve("n4", after, fixture.now.Add(3*time.Second)); got != ReverseSlotCapped {
		t.Fatalf("a reconnect bought a brand new per-upstream quota: n4 = %s, want capped", got)
	}
	if load := upstreamLoad(fixture.table, after); load != 2 {
		t.Fatalf("the reconnected upstream reads %d records, want the 2 its owner already holds: "+
			"the bucket must follow the neighbour, not the socket", load)
	}

	// POSITIVE CONTROL. Without it every assertion above is satisfied by a table
	// that refuses everything after two records: a DIFFERENT neighbour gets a
	// quota of its own.
	honest := provenNeighbour("honest", "session-1")
	if got := fixture.reserve("h1", honest, fixture.now.Add(4*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("an unrelated neighbour was refused its own quota: %s", got)
	}
	if load := upstreamLoad(fixture.table, honest); load != 1 {
		t.Fatalf("the honest neighbour holds %d records, want 1", load)
	}
	if load := upstreamLoad(fixture.table, before); load != 2 {
		t.Fatalf("the flooder's bucket moved to %d when a stranger arrived, want 2", load)
	}
}

// ---------------------------------------------------------------------------
// (b) The eviction victim is the busiest OWNER, not the busiest channel
// ---------------------------------------------------------------------------

// TestGlobalCapEvictsTheBusiestOwnerNotTheBusiestChannel is the second half of
// the same finding, on the consequence that hurts the HONEST neighbour.
//
// The fixture is deliberately lopsided in the two keyings' opposite directions:
// the honest neighbour holds TWO records on ONE channel, the flooder THREE
// spread over THREE. Counted by channel the honest neighbour is the strict
// maximum and loses its own record; counted by owner the flooder is, and loses
// one of its three. Anything more symmetric — one record per attacker channel and
// one for the honest peer — would leave every bucket tied at one and make the
// verdict depend on which tie-break fired, which proves nothing about the key.
//
// The mutations this kills:
//
//   - keying byUpstream on the channel: the busiest bucket is then the honest
//     neighbour's single channel and its oldest record is evicted;
//   - making evictOldestOfLocked match on the channel while the tally is keyed
//     on the owner: no entry matches the chosen bucket, the eviction fails, and
//     the honest neighbour's third request is Capped instead of Reserved.
func TestGlobalCapEvictsTheBusiestOwnerNotTheBusiestChannel(t *testing.T) {
	t.Parallel()

	fixture := newQuotaFixture(t, 5, 3)
	honest := provenNeighbour("honest", "only-session")

	if got := fixture.reserve("h1", honest, fixture.now); got != ReverseSlotReserved {
		t.Fatalf("h1: %s", got)
	}
	if got := fixture.reserve("h2", honest, fixture.now.Add(time.Second)); got != ReverseSlotReserved {
		t.Fatalf("h2: %s", got)
	}
	// The flooder spreads three records over three sessions of ONE neighbour.
	for i := 0; i < 3; i++ {
		seed := "a" + strconv.Itoa(i)
		upstream := provenNeighbour("flooder", "session-"+strconv.Itoa(i))
		at := fixture.now.Add(time.Duration(2+i) * time.Second)
		if got := fixture.reserve(seed, upstream, at); got != ReverseSlotReserved {
			t.Fatalf("%s: %s", seed, got)
		}
	}

	// The table is now full: 5 of 5. The honest neighbour's next request has to
	// evict somebody, and §5 says it is the busiest upstream.
	if got := fixture.reserve("h3", honest, fixture.now.Add(10*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("h3 must be admitted by evicting the busiest upstream, got %s", got)
	}

	flooder := provenNeighbour("flooder", "session-0")
	if load := upstreamLoad(fixture.table, flooder); load != 2 {
		t.Fatalf("the flooder keeps %d records, want 2: the victim is chosen by OWNER, and its "+
			"records spread over three channels are still one upstream", load)
	}
	if load := upstreamLoad(fixture.table, honest); load != 3 {
		t.Fatalf("the honest neighbour holds %d records, want 3: it lost its own slot to a "+
			"flooder that looked quiet on every single channel", load)
	}
	// The flooder's OLDEST record is the one that went, and the honest
	// neighbour's oldest is untouched.
	if fixture.live("a0") {
		t.Fatal("the evicted record must be the flooder's oldest (a0)")
	}
	if !fixture.live("h1") {
		t.Fatal("the honest neighbour's oldest record was evicted for a flooder's slot")
	}
	if !fixture.live("a1") || !fixture.live("a2") {
		t.Fatal("more than one record was evicted: the eviction takes exactly one victim")
	}
}

// ---------------------------------------------------------------------------
// The tie-break agrees with its twin in the replay cache
// ---------------------------------------------------------------------------

// TestEvictionTieSparesTheLocalUpstream states the direction of the tie-break,
// which the two fairness evictions of this package disagreed on.
//
// BaseReplayCache.noisiestOwnerLocked keeps the GREATEST key under
// ingressOwner.compare and the local bucket is its least, so a tie there never
// victimises this node's own frame. ReverseTable kept the LEAST under
// upstreamOrderLess, whose least is
// also the local marker — so a tie here victimised exactly the record the other
// one protects, and contradicted this file's own stated rule that our own
// requests are not evicted to make room for a neighbour's.
//
// Local records are the ones worth protecting: nobody else generated them, no
// attacker can multiply them, and losing one loses an exchange this node started.
//
// The mutation this kills: reading the tie in the other direction
// (upstreamOrderLess(upstream, busiest)) — our own record is then the victim.
func TestEvictionTieSparesTheLocalUpstream(t *testing.T) {
	t.Parallel()

	fixture := newQuotaFixture(t, 2, 2)
	neighbour := provenNeighbour("neighbour", "session-1")

	if got := fixture.reserve("mine", LocalUpstream(), fixture.now); got != ReverseSlotReserved {
		t.Fatalf("our own request: %s", got)
	}
	if got := fixture.reserve("theirs", neighbour, fixture.now.Add(time.Second)); got != ReverseSlotReserved {
		t.Fatalf("the neighbour's request: %s", got)
	}

	// Both buckets hold exactly one record, so the eviction below is decided by
	// the tie-break and by nothing else.
	if local, remote := upstreamLoad(fixture.table, LocalUpstream()), upstreamLoad(fixture.table, neighbour); local != 1 || remote != 1 {
		t.Fatalf("the fixture is not tied: local %d, remote %d", local, remote)
	}

	stranger := provenNeighbour("stranger", "session-1")
	if got := fixture.reserve("strangers", stranger, fixture.now.Add(2*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("the third request must be admitted by an eviction, got %s", got)
	}

	if !fixture.live("mine") {
		t.Fatal("a tie evicted OUR OWN request: the local marker must be the bucket a tie spares")
	}
	if fixture.live("theirs") {
		t.Fatal("the neighbour's record must be the victim of the tie")
	}
	if load := upstreamLoad(fixture.table, LocalUpstream()); load != 1 {
		t.Fatalf("the local bucket holds %d records, want 1", load)
	}
}

// ---------------------------------------------------------------------------
// (c) The return path is still the channel, and it is not derived from the owner
// ---------------------------------------------------------------------------

// TestTheReturnChannelIsNotDerivedFromTheQuotaOwner is the regression guard on
// the previous round: making the OWNER the bucket must not make it the address.
//
// One neighbour, two sessions, two records. They share a bucket and they do NOT
// share a return path — an answer to the question that came in on session 1 has
// to leave over session 1, whatever the other session of the same peer is doing.
//
// The mutations this kills:
//
//   - deriving Upstream.Channel from the owner, or dropping the channel field:
//     both records then name one channel and one of the two assertions fails;
//   - keying byUpstream back on the channel: the shared-bucket assertion fails,
//     which is the other half of the same statement.
func TestTheReturnChannelIsNotDerivedFromTheQuotaOwner(t *testing.T) {
	t.Parallel()

	fixture := newQuotaFixture(t, 64, 64)
	first := provenNeighbour("neighbour", "session-1")
	second := provenNeighbour("neighbour", "session-2")

	if got := fixture.reserve("q1", first, fixture.now); got != ReverseSlotReserved {
		t.Fatalf("q1: %s", got)
	}
	if got := fixture.reserve("q2", second, fixture.now.Add(time.Second)); got != ReverseSlotReserved {
		t.Fatalf("q2: %s", got)
	}

	// ONE bucket: the two sessions are one neighbour as far as §5 goes.
	if load := upstreamLoad(fixture.table, first); load != 2 {
		t.Fatalf("the neighbour holds %d records, want 2 across both of its sessions", load)
	}
	if upstreamLoad(fixture.table, first) != upstreamLoad(fixture.table, second) {
		t.Fatal("two channels of one neighbour read two different loads: they are one bucket")
	}
	if !sameUpstream(first, second) {
		t.Fatal("two sessions of one neighbour must share an accounting bucket")
	}

	// TWO return paths: each record answers over the socket its own question
	// arrived on.
	requireUpstreamChannel(t, fixture.table, "q1", first)
	requireUpstreamChannel(t, fixture.table, "q2", second)

	// And the owner is reachable as its own fact, so a log line about a capped
	// record can name the bucket rather than a socket that may already be gone.
	record, live := fixture.table.Lookup(NewLabel(domaintest.ID("q1")))
	if !live {
		t.Fatal("q1 vanished")
	}
	owner, billed := record.Upstream().Owner()
	if !billed || owner != ProvenIdentityKey(domaintest.ID("neighbour")) {
		t.Fatalf("the record is billed to %v (%t), want the neighbour's proven key", owner, billed)
	}
	if _, billed := LocalUpstream().Owner(); billed {
		t.Fatal("the local marker must be billed to nobody")
	}
}

func requireUpstreamChannel(t *testing.T, table *ReverseTable, seed string, want Upstream) {
	t.Helper()
	record, live := table.Lookup(NewLabel(domaintest.ID(seed)))
	if !live {
		t.Fatalf("the record %s vanished", seed)
	}
	wanted, addressable := want.Channel()
	if !addressable {
		t.Fatalf("the fixture's upstream %s names no channel", want)
	}
	got, ok := record.Upstream().Channel()
	if !ok || got != wanted {
		t.Fatalf("%s answers over %s (%t), want the channel its question arrived on (%s)",
			seed, got, ok, wanted)
	}
}

// TestARemoteUpstreamNeedsBothOfItsAnswers pins the closed direction of the
// split: a value that names only one of the two facts is not an upstream at all.
//
// A record with no channel has nowhere to send the answer; a record with no
// owner is charged to the zero AdmissionKey, which would be ONE bucket shared by
// every arrival the receive path could not bill — the same shape
// PeerAdmission.Admit refuses, and for the same reason.
func TestARemoteUpstreamNeedsBothOfItsAnswers(t *testing.T) {
	t.Parallel()

	name := domaintest.ID("neighbour")
	channel := testChannel("neighbour/session-1")

	if !ChannelUpstream(NoChannel(), ProvenIdentityKey(name), name).IsZero() {
		t.Fatal("an upstream with no channel has no return path and must read as unset")
	}
	if !ChannelUpstream(channel, AdmissionKey{}, name).IsZero() {
		t.Fatal("an upstream with no owner has no quota bucket and must read as unset")
	}
	if ChannelUpstream(channel, ProvenIdentityKey(name), name).IsZero() {
		t.Fatal("an upstream naming both facts must read as set")
	}
	if LocalUpstream().IsZero() {
		t.Fatal("the local marker is a set value: it needs neither of the two")
	}

	// Reserve refuses the unset value rather than opening the shared bucket.
	fixture := newQuotaFixture(t, 64, 64)
	unowned := ChannelUpstream(channel, AdmissionKey{}, name)
	if got := fixture.reserve("unowned", unowned, fixture.now); got != ReverseReserveUnset {
		t.Fatalf("an unbillable arrival took a slot: %s", got)
	}
	if fixture.table.Len() != 0 {
		t.Fatalf("the table holds %d records after a refused reservation", fixture.table.Len())
	}
}

// ---------------------------------------------------------------------------
// (c) The same statement one level up: the conveyor's answer, over the wire
// ---------------------------------------------------------------------------

// TestTransitedAnswerReturnsOnTheArrivalChannel is the pipeline-level guard on
// the previous round's correction, in the shape the owner bucket could have
// broken it: the request arrives on a SECOND session of the origin, so "the
// channel the question came in on" and "the channel this peer is normally reached
// on" are two different values, and only one of them is right.
//
// The mutations this kills:
//
//   - addressing the response with nextHopEgress(name) instead of the record's
//     channel: the hand-over then carries NO channel and the transport resolves
//     the name through the session map;
//   - re-deriving the return channel from the quota owner: the answer then leaves
//     over the peer's usual session rather than over the one that asked.
func TestTransitedAnswerReturnsOnTheArrivalChannel(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	origin, relay, target := nodes[0], nodes[1], nodes[2]

	label := newLabel(t, "return-path")
	asked := ingressOpts{
		peer:      origin.id,
		channel:   testChannel("second-session-of-" + origin.id.String()),
		authority: AuthorityProven,
	}
	if asked.channel == testChannel(origin.id.String()) {
		t.Fatal("the fixture asked over the origin's usual channel: it cannot tell the two apart")
	}
	requireOutcome(t, relay.deliverOn(t, asked, requestFrame(t, requestOpts{
		label: label, dst: target.id,
	})), InboundForwarded)

	// The slot was charged to the ORIGIN's bucket, which any channel of the
	// origin reads — that is the value that must NOT have addressed the answer.
	billed := ChannelUpstream(testChannel(origin.id.String()), ProvenIdentityKey(origin.id), origin.id)
	if load := upstreamLoad(relay.reverse, billed); load != 1 {
		t.Fatalf("the origin's bucket holds %d records, want 1: the quota follows the neighbour", load)
	}

	requireOutcome(t, relay.deliver(t, target.id, responseFrame(t, responseOpts{
		label: label, subject: target.id,
	})), InboundForwarded)

	events := net.journal()
	last := events[len(events)-1]
	if last.to != origin.id {
		t.Fatalf("the answer went to %s, want the stored upstream %s", last.to, origin.id)
	}
	if last.channel != asked.channel {
		t.Fatalf("the answer left over %s, want the channel the question arrived on (%s): "+
			"an answer belongs to the exchange's own return path", last.channel, asked.channel)
	}
	if relay.reverse.Len() != 0 {
		t.Fatal("a successfully enqueued answer frees its record")
	}
}
