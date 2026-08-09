package datagram

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_test.go holds the M6 fixtures: an in-memory topology of several
// layer instances joined by a fake transport, plus the frame builders every
// pipeline test shares.
//
// The topology is not decoration. "Ten hops there and ten back" (§9) cannot be
// checked on a single instance, and neither can "the reverse record survives a
// full round trip", "a repeated request does not re-point downstream" or "the
// answer arrives before the request has finished being published" — the last
// one only shows up because delivery here is SYNCHRONOUS and re-entrant, which
// is the harshest possible version of the race §4.2 phase 3 exists to close.

const (
	testNetwork    = domain.NetworkID("corsa-test")
	dtypeQuery     = domain.DType("get_identity")
	dtypeAnswer    = domain.DType("post_identity")
	dtypeCached    = domain.DType("cached_identity")
	dtypePush      = domain.DType("push_identity")
	dtypeUnrelated = domain.DType("file_transfer")
)

// declaredDTypes is what every fixture peer announces in its handshake, so the
// last-hop dtype gate of §4.3 never masks the behaviour under test. The gate
// itself is M7's and has its own tests.
func declaredDTypes() DeclaredDTypes {
	return NewDeclaredDTypes(domain.ParseDeclaredDTypes([]string{
		dtypeQuery.String(), dtypeAnswer.String(), dtypeCached.String(),
		dtypePush.String(), dtypeUnrelated.String(),
	}))
}

// ---------------------------------------------------------------------------
// Fake transport
// ---------------------------------------------------------------------------

// wireEvent is one frame handed to the fake transport.
type wireEvent struct {
	sendUntil time.Time
	from      domain.PeerIdentity
	to        domain.PeerIdentity
	frame     protocol.DatagramFrame
	channel   ChannelID
	bytes     int
	refused   bool
}

// fakeNetwork joins several pipelines. Delivery is synchronous: EmitTo runs
// the receiver's HandleInbound on the caller's goroutine, so a fast answer
// comes back re-entrantly, exactly as a writer that sends and receives before
// the sender's own call has returned.
type fakeNetwork struct {
	mu     sync.Mutex
	nodes  map[domain.PeerIdentity]*pipelineNode
	events []wireEvent
	refuse map[domain.PeerIdentity]bool
	drop   map[domain.PeerIdentity]bool
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{
		nodes:  make(map[domain.PeerIdentity]*pipelineNode),
		refuse: make(map[domain.PeerIdentity]bool),
		drop:   make(map[domain.PeerIdentity]bool),
	}
}

// refuseQueue makes the write queue of a peer reject frames, which is the
// local, immediate failure that sends the scheduler to the next candidate.
func (n *fakeNetwork) refuseQueue(peer domain.PeerIdentity) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.refuse[peer] = true
}

func (n *fakeNetwork) register(node *pipelineNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[node.id] = node
}

func (n *fakeNetwork) lookup(peer domain.PeerIdentity) (*pipelineNode, bool, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	node, ok := n.nodes[peer]
	return node, n.refuse[peer], n.drop[peer] || !ok
}

func (n *fakeNetwork) record(event wireEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, event)
}

// events returns the journal in order.
func (n *fakeNetwork) journal() []wireEvent {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]wireEvent(nil), n.events...)
}

// nodeEmitter is the FrameEmitter of one node: it knows WHO is sending, so the
// receiver sees a properly authenticated neighbour.
//
// It also carries the sending node itself, which is what gives a test a foothold
// BETWEEN the reservation and the commit of the routed cycle: the publish stands
// exactly there (forwardRouted), and there is no seam inside the anti-replay
// memory to stage anything in.
type nodeEmitter struct {
	net    *fakeNetwork
	from   domain.PeerIdentity
	sender *pipelineNode
}

func (e nodeEmitter) EmitTo(ctx context.Context, out OutboundFrame) bool {
	peer, frame, sendUntil := out.Peer, out.Frame, out.SendUntil
	if e.sender != nil {
		e.sender.runBeforeEmit()
	}
	target, refused, blackholed := e.net.lookup(peer)
	if refused {
		e.net.record(wireEvent{
			from: e.from, to: peer, frame: frame, sendUntil: sendUntil,
			channel: out.Channel, refused: true,
		})
		return false
	}
	// The line the layer serialized is the line that goes on the wire: the
	// emitter never re-serializes, which is the whole point of carrying it.
	if len(out.Line) == 0 {
		return false
	}
	if blackholed || target == nil {
		e.net.record(wireEvent{
			from: e.from, to: peer, frame: frame, sendUntil: sendUntil, channel: out.Channel,
		})
		return true
	}
	event := wireEvent{
		from: e.from, to: peer, frame: frame, sendUntil: sendUntil,
		channel: out.Channel, bytes: out.Bytes(),
	}
	e.net.record(event)
	// A frame arriving over the fake network models an ACCEPTED connection: the
	// sender is who it says it is, so the budget key is its proven identity and
	// the channel is the one that peer is reached on.
	result := target.pipeline.HandleInbound(ctx, InboundOpts{
		Line:      out.Line,
		Peer:      e.from,
		Channel:   testChannel(e.from.String()),
		BudgetKey: ProvenIdentityKey(e.from),
	})
	target.recordInbound(result)
	return true
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

// recordingMetrics is the metricsSink of the fixture; M8 will supply the real
// one. It also satisfies reverseMetrics, which is the point: one type serves
// both seams.
type recordingMetrics struct {
	mu       sync.Mutex
	inbound  []DropReason
	outcomes []InboundOutcome
	unknown  []domain.DType
	reverse  []ReverseEvent
	drops    []DropReason
}

// ObserveDrop counts the outbound refusals that never entered the conveyor —
// the post-dequeue drop of ClassQueueEmitter. The fixture keeps them in their
// own slice rather than folding them into `inbound`: an outbound drop is not
// an observed frame, and TestFrameRefusedAfterTheDequeueIsCounted pins that
// separation against the real Metrics.
func (m *recordingMetrics) ObserveDrop(reason DropReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops = append(m.drops, reason)
}

func (m *recordingMetrics) ObserveInbound(_ domain.DatagramMode, outcome InboundOutcome, reason DropReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outcomes = append(m.outcomes, outcome)
	m.inbound = append(m.inbound, reason)
}

func (m *recordingMetrics) ObserveUnknownDType(dtype domain.DType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unknown = append(m.unknown, dtype)
}

func (m *recordingMetrics) ObserveReverseState(event ReverseEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reverse = append(m.reverse, event)
}

// dropReasons returns the drop reasons the sink recorded, in order.
func (m *recordingMetrics) dropReasons() []DropReason {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DropReason(nil), m.inbound...)
}

// observedOutcomes returns the outcomes the sink recorded, in order.
func (m *recordingMetrics) observedOutcomes() []InboundOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]InboundOutcome(nil), m.outcomes...)
}

func (m *recordingMetrics) unknownTypes() []domain.DType {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]domain.DType(nil), m.unknown...)
}

func (m *recordingMetrics) reverseEvents() []ReverseEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ReverseEvent(nil), m.reverse...)
}

// countingCrypto is the verification-budget seam — the ONLY budget seam the
// pipeline declares. Stage one is charged by the owner of the receive path and
// therefore has no fixture here; the node package owns those tests
// (datagram_admission_order_test.go, datagram_budget_key_test.go).
//
// It ignores the key on purpose: WHICH bucket stage two lands in is a property
// of the real controller, and the test that pins it runs against
// PeerAdmission rather than against a stub that could be told to agree
// (TestVerifyBudgetIsChargedToTheKeyAndNotToTheClaimedIdentity).
type countingCrypto struct {
	mu     sync.Mutex
	charge int
	budget int
	capped bool
}

func (c *countingCrypto) ChargeVerifyFor(AdmissionKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.charge++
	if !c.capped {
		return true
	}
	return c.charge <= c.budget
}

func (c *countingCrypto) charged() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.charge
}

// pipelineNode is one layer instance plus everything it was wired from, so a
// test can reach into the anti-replay cache, the reverse table or the metrics.
//
// `replay` is the cache the layer really addresses. There is no decorator field
// beside it any more: PipelineConfig names *BaseReplayCache, so a memory that
// counts calls, blocks or refuses on command cannot be built at all — what a
// test observes instead is the cache's own counters and the state it holds.
type pipelineNode struct {
	t         *testing.T
	net       *fakeNetwork
	pipeline  *Pipeline
	types     *TypeRegistry
	replay    *BaseReplayCache
	reverse   *ReverseTable
	scheduler *Scheduler
	routes    *schedRouteResolver
	peers     *schedPeerMetadata
	direct    *schedDirectSession
	metrics   *recordingMetrics
	crypto    *countingCrypto
	id        domain.PeerIdentity

	mu      sync.Mutex
	now     time.Time
	inbound []InboundResult
	// beforeEmit runs on the publishing goroutine immediately before a frame is
	// handed to the fake wire — the one moment inside forwardRouted where the
	// reservation is held and the commit has not happened yet.
	beforeEmit func()
}

// nodeOpts are the knobs one fixture node is built with. Every field has a
// meaningful zero, so a test names only what it is actually about.
type nodeOpts struct {
	// name seeds a deterministic identity; id overrides it outright, which is
	// what a test that has to SIGN as this node needs.
	name string
	id   domain.PeerIdentity
	// now pins the fixture clock. Zero means the fixture's own epoch.
	now time.Time
	// transit makes the node advertise mesh_datagram_transit_v1, which is the
	// whole difference between a relay and an endpoint-only client.
	transit bool
	// advertised overrides the raw advertised set outright.
	advertised []string
	// cryptoBudget caps the verification budget; zero means unlimited.
	cryptoBudget int
}

func newPipelineNode(t *testing.T, net *fakeNetwork, opts nodeOpts) *pipelineNode {
	t.Helper()
	id := opts.id
	if id.IsZero() {
		id = domaintest.ID(opts.name)
	}
	now := opts.now
	if now.IsZero() {
		now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	}
	node := &pipelineNode{
		t:       t,
		net:     net,
		types:   NewTypeRegistry(),
		routes:  newSchedRouteResolver(),
		peers:   newSchedPeerMetadata(),
		direct:  newSchedDirectSession(),
		metrics: &recordingMetrics{},
		crypto:  &countingCrypto{budget: opts.cryptoBudget, capped: opts.cryptoBudget > 0},
		id:      id,
		now:     now,
	}
	node.replay = NewBaseReplayCache(BaseReplayCacheConfig{Clock: node.clock})
	node.reverse = NewReverseTable(ReverseTableConfig{Clock: node.clock, Metrics: node.metrics})

	scheduler, err := NewScheduler(SchedulerConfig{
		Routes:               node.routes,
		Peers:                node.peers,
		Direct:               node.direct,
		Secret:               schedSecret{secret: []byte("secret-" + id.String())},
		Clock:                node.clock,
		LocalID:              id,
		LocalProtocolVersion: schedLocalVersion,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	node.scheduler = scheduler

	advertised := opts.advertised
	if advertised == nil {
		advertised = []string{CapabilityDatagramV1.String()}
		if opts.transit {
			advertised = append(advertised, CapabilityDatagramTransitV1.String())
		}
	}
	pipeline, err := NewPipeline(PipelineConfig{
		Clock:       node.clock,
		Types:       node.types,
		ReplayCache: node.replay,
		Reverse:     node.reverse,
		Scheduler:   node.scheduler,
		Emitter:     nodeEmitter{net: net, from: id, sender: node},
		Crypto:      node.crypto,
		Metrics:     node.metrics,
		Advertised:  NewAdvertisedCapabilities(advertised),
		Network:     testNetwork,
		LocalID:     id,
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	node.pipeline = pipeline
	net.register(node)
	return node
}

func (n *pipelineNode) clock() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

func (n *pipelineNode) advance(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.now = n.now.Add(d)
}

// runBeforeEmit fires the hook a test staged between the reservation and the
// commit, if any.
func (n *pipelineNode) runBeforeEmit() {
	n.mu.Lock()
	hook := n.beforeEmit
	n.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// onBeforeEmit stages that hook.
func (n *pipelineNode) onBeforeEmit(hook func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.beforeEmit = hook
}

// replayCalls is what the layer did to its anti-replay memory, counted by the
// memory itself.
//
// It replaces a decorator that used to sit between the pipeline and the cache.
// With one concrete cache there is no seam to install one in — and there should
// not be, since that seam was also the way a blocking or disk-backed memory
// could reach the pipeline. The counters lose nothing that these tests assert:
// the cache increments exactly one of them per call, and they are the record an
// operator reads anyway (§5).
type replayCalls struct {
	reserves uint64
	releases uint64
	commits  uint64
}

func (n *pipelineNode) replayCalls() replayCalls { return replayCallsOf(n.replay) }

func replayCallsOf(cache *BaseReplayCache) replayCalls {
	counters := cache.Metrics()
	return replayCalls{
		// Every Reserve ends in exactly one of these: it won the key, the key was
		// taken, or there was no room for it. EvictedNoisyPeer is deliberately
		// absent — it accompanies a reservation that Reserved has already counted.
		reserves: counters.Reserved + counters.Duplicates +
			counters.RejectedCapacity + counters.RejectedNoisyPeer,
		// Every Release ends in one of the two: the record went, or the token was
		// stale and the call was the ABA no-op.
		releases: counters.Released + counters.StaleReleases,
		commits:  counters.Committed,
	}
}

// settleReplayKey puts a committed record into a cache directly, so a test can
// stage the state the early probe of step 6 meets.
func settleReplayKey(t *testing.T, cache *BaseReplayCache, key domain.ReplayKey, incoming IngressPeer, until time.Time) {
	t.Helper()
	rsv, held := cache.Reserve(context.Background(), key, incoming, until).Reservation()
	if !held {
		t.Fatalf("the fixture could not reserve %s", shortKey(key))
	}
	if applied := cache.Commit(context.Background(), rsv); !applied.IsApplied() {
		t.Fatalf("the fixture could not commit: %v", applied.Err())
	}
}

// forgetReplayRecord drops a record through the cache's OWN mutator, so every
// index stays consistent.
//
// It reproduces the one state in which the layer's Commit can answer `fail`:
// the record is gone. The cache reaches it by itself through the
// abandoned-reservation watchdog — a branch that reached neither Commit nor
// Release within replay_until plus the whole hop budget is reclaimed
// (baseHeldReservationGrace) — and a test that wants that branch cannot wait out
// the grace on a live pipeline, so it removes the record at the same point.
func forgetReplayRecord(cache *BaseReplayCache, key domain.ReplayKey) bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, held := cache.entries[key]
	if !held {
		return false
	}
	cache.removeLocked(key, entry)
	return true
}

func (n *pipelineNode) recordInbound(result InboundResult) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.inbound = append(n.inbound, result)
}

// connection is the handshake metadata of this node as its neighbours see it.
func (n *pipelineNode) connection(transit bool, extra ...string) PeerConnection {
	names := []string{CapabilityDatagramV1.String()}
	if transit {
		names = append(names, CapabilityDatagramTransitV1.String())
	}
	names = append(names, extra...)
	return PeerConnection{
		ConnectedAt: n.clock().Add(-time.Hour),
		Advertised:  NewAdvertisedCapabilities(names),
		DTypes:      declaredDTypes(),
		// The fixture's ONE channel per identity, and the same one the receiving
		// side is told about in nodeEmitter.EmitTo. The two have to agree or a
		// request's reverse record would store a channel no answer ever arrives
		// on — which is the fixture modelling a broken transport rather than a
		// network.
		Channel:                 testChannel(n.id.String()),
		ReportedProtocolVersion: schedLocalVersion,
	}
}

// link makes two nodes neighbours in both directions: a direct session and the
// per-connection metadata the ranking reads.
//
// extra are capability names both sides advertise on top of the two role ones.
// Nothing in the envelope can demand them any more — they exist so a test can
// state that an extra advertised name changes no routing decision.
func link(a, b *pipelineNode, transitA, transitB bool, extra ...string) {
	a.peers.set(b.id, b.connection(transitB, extra...))
	a.direct.set(b.id, b.connection(transitB, extra...))
	b.peers.set(a.id, a.connection(transitA, extra...))
	b.direct.set(a.id, a.connection(transitA, extra...))
}

// route teaches a node that dst is reachable through via.
func route(from *pipelineNode, dst, via domain.PeerIdentity, hops int) {
	from.routes.set(dst, RouteHint{NextHop: via, Hops: hops})
}

// ---------------------------------------------------------------------------
// Frame builders
// ---------------------------------------------------------------------------

func newSigner(t *testing.T) (ed25519.PrivateKey, domain.PeerIdentity) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	id, err := domain.ParsePeerIdentity(identity.Fingerprint(public))
	if err != nil {
		t.Fatalf("ParsePeerIdentity: %v", err)
	}
	return private, id
}

type routedOpts struct {
	now     time.Time
	private ed25519.PrivateKey
	src     domain.PeerIdentity
	dst     domain.PeerIdentity
	dtype   domain.DType
	payload []byte
	class   domain.DatagramClass
	ttl     uint8
	maxTTL  uint8
}

// signedRouted builds and signs a routed datagram.
func signedRouted(t *testing.T, opts routedOpts) protocol.DatagramFrame {
	t.Helper()
	if opts.class == "" {
		opts.class = domain.DatagramClassControl
	}
	if opts.ttl == 0 {
		opts.ttl = OriginTTL()
	}
	if opts.maxTTL == 0 {
		opts.maxTTL = OriginTTL()
	}
	if opts.dtype == "" {
		opts.dtype = dtypePush
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand: %v", err)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       opts.class,
		Src:         opts.src,
		Dst:         opts.dst,
		TTL:         opts.ttl,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       opts.dtype,
		Payload:     opts.payload,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        salt,
			MaxTTL:      opts.maxTTL,
			Time:        opts.now.Unix(),
		},
	}
	signed, err := protocol.SignDatagram(frame, testNetwork, opts.private)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return signed
}

// newLabel mints a one-shot request label. It is a random 20-byte tag and is
// deliberately not derived from any identity (§2.1.1).
func newLabel(t *testing.T, seed string) Label {
	t.Helper()
	return NewLabel(domaintest.ID("label-" + seed))
}

type requestOpts struct {
	label   Label
	dst     domain.PeerIdentity
	dtype   domain.DType
	payload []byte
	ttl     uint8
}

func requestFrame(t *testing.T, opts requestOpts) protocol.DatagramFrame {
	t.Helper()
	if opts.ttl == 0 {
		opts.ttl = OriginTTL()
	}
	if opts.dtype == "" {
		opts.dtype = dtypeQuery
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRequest,
		Class:       domain.DatagramClassControl,
		Src:         opts.label.Raw(),
		Dst:         opts.dst,
		TTL:         opts.ttl,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       opts.dtype,
		Payload:     opts.payload,
	}
	if err := frame.Validate(); err != nil {
		t.Fatalf("request frame: %v", err)
	}
	return frame
}

type responseOpts struct {
	label   Label
	subject domain.PeerIdentity
	dtype   domain.DType
	payload []byte
	ttl     uint8
}

func responseFrame(t *testing.T, opts responseOpts) protocol.DatagramFrame {
	t.Helper()
	if opts.ttl == 0 {
		opts.ttl = ResponseTTL()
	}
	if opts.dtype == "" {
		opts.dtype = dtypeAnswer
	}
	frame := protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion,
		Mode:    domain.DatagramModeResponse,
		Class:   domain.DatagramClassControl,
		Src:     opts.subject,
		Dst:     opts.label.Raw(),
		TTL:     opts.ttl,
		DType:   opts.dtype,
		Payload: opts.payload,
	}
	if err := frame.Validate(); err != nil {
		t.Fatalf("response frame: %v", err)
	}
	return frame
}

// testChannel mints a stable, distinct channel per name.
//
// Distinct is the property every fixture in this package needs and none of them
// could state before: two neighbours that share one channel are one neighbour as
// far as every channel-relative decision goes, so a test built on a single
// channel cannot tell "keyed on the channel" from "keyed on the name".
func testChannel(name string) ChannelID {
	var hash uint64 = 14695981039346656037
	for i := 0; i < len(name); i++ {
		hash ^= uint64(name[i])
		hash *= 1099511628211
	}
	if hash == 0 {
		hash = 1
	}
	return NetworkChannel(domain.ConnID(hash))
}

// testUpstream is the fixture's neighbour upstream: ONE channel and ONE budget
// bucket per identity, as an ACCEPTED connection produces them — the identity is
// proven, so it is both the name and the key the arrival is billed to.
//
// Every reverse-state test that used to distinguish two upstreams by name still
// distinguishes them — by the thing the table now keys on. The helper exists so
// that intent survives the change instead of every call site silently collapsing
// into one bucket. A test that needs the two facts APART builds the value itself
// (reverse_quota_owner_test.go), which is the only way to state "one neighbour,
// two channels" at all.
func testUpstream(id domain.PeerIdentity) Upstream {
	return ChannelUpstream(testChannel(id.String()), ProvenIdentityKey(id), id)
}

// testDownstream is the other end of the same convention: the channel a request
// to this identity would leave over, which in the fixture is the identity's own
// channel — the one PeerConnection.Channel advertises and the one an answer from
// it arrives on.
func testDownstream(id domain.PeerIdentity) Downstream {
	return ChannelDownstream(testChannel(id.String()), id)
}

// ingressOpts is one arrival's three facts as a FIXTURE states them: which name
// the neighbour presents, which channel it arrived on, and what this node has
// been shown about that name.
//
// They are a struct rather than three arguments because two of them are
// identity-shaped and the third decides how the first is read; a positional call
// site would let a test claim it was testing a borrowed name while handing over a
// proven one.
type ingressOpts struct {
	peer      domain.PeerIdentity
	channel   ChannelID
	authority IngressAuthority
}

// budgetKey is the key the ARRIVAL's authority implies, because the conveyor
// derives the authority from exactly that (inboundFrame.authority). A fixture
// that could set the two independently would be testing a receive path no node
// can produce.
func (o ingressOpts) budgetKey() AdmissionKey {
	if o.authority.Proven() {
		return ProvenIdentityKey(o.peer)
	}
	return DialedAddressKey(domain.PeerAddress("dialed:" + o.channel.String()))
}

// deliver hands a frame to a node as if it arrived from peer on an ACCEPTED
// connection: the identity is proven, so it is also the budget key, and the
// channel is that peer's own.
func (n *pipelineNode) deliver(t *testing.T, from domain.PeerIdentity, frame protocol.DatagramFrame) InboundResult {
	t.Helper()
	return n.deliverOn(t, ingressOpts{
		peer: from, channel: testChannel(from.String()), authority: AuthorityProven,
	}, frame)
}

// deliverBilledTo is deliver with the billing pulled apart from the name: WHO
// the conveyor is told the frame came from, and WHO PAYS for it.
//
// They differ on an outbound session, where the identity is the peer's own
// claim and the key is the host:port this node dialled — and a fixture that
// could not state them separately could not show a charge landing on the wrong
// bucket. The channel is derived from the key, so a dialled arrival lands on a
// channel of its own rather than on the named peer's.
func (n *pipelineNode) deliverBilledTo(
	t *testing.T,
	from domain.PeerIdentity,
	key AdmissionKey,
	frame protocol.DatagramFrame,
) InboundResult {
	t.Helper()
	channel := testChannel(from.String())
	if key != ProvenIdentityKey(from) {
		channel = testChannel("dialed:" + key.String())
	}
	return n.deliverLine(t, InboundOpts{Peer: from, Channel: channel, BudgetKey: key}, frame)
}

// deliverOn is the full three-fact arrival: the name, the channel and the level.
func (n *pipelineNode) deliverOn(t *testing.T, in ingressOpts, frame protocol.DatagramFrame) InboundResult {
	t.Helper()
	return n.deliverLine(t, InboundOpts{
		Peer: in.peer, Channel: in.channel, BudgetKey: in.budgetKey(),
	}, frame)
}

// deliverLine serializes the frame into the opts the caller has already filled
// in. It is the ONE place the fixture marshals, so a test cannot accidentally
// hand the conveyor a line no strict parser produced.
func (n *pipelineNode) deliverLine(
	t *testing.T,
	in InboundOpts,
	frame protocol.DatagramFrame,
) InboundResult {
	t.Helper()
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	in.Line = []byte(line)
	return n.pipeline.HandleInbound(context.Background(), in)
}

// ---------------------------------------------------------------------------
// Handlers used across the tests
// ---------------------------------------------------------------------------

// recordingHandler counts calls and answers with a configurable outcome.
type recordingHandler struct {
	mu       sync.Mutex
	result   func(delivery DeliveryContext, payload []byte) HandlerResult
	calls    int
	contexts []DeliveryContext
	payloads [][]byte
}

func (h *recordingHandler) Handle(_ context.Context, delivery DeliveryContext, payload []byte) HandlerResult {
	h.mu.Lock()
	h.calls++
	h.contexts = append(h.contexts, delivery)
	h.payloads = append(h.payloads, append([]byte(nil), payload...))
	result := h.result
	h.mu.Unlock()
	if result == nil {
		return AcceptDelivery()
	}
	return result(delivery, payload)
}

func (h *recordingHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *recordingHandler) lastContext() (DeliveryContext, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.contexts) == 0 {
		return DeliveryContext{}, false
	}
	return h.contexts[len(h.contexts)-1], true
}
func acceptingHandler() *recordingHandler { return &recordingHandler{} }
func answeringHandler(dtype domain.DType, payload []byte) *recordingHandler {
	return &recordingHandler{
		result: func(DeliveryContext, []byte) HandlerResult {
			return AcceptWithAnswer(dtype, payload)
		},
	}
}

var errTestRefused = errors.New("refused by the test handler")

func refusingHandler() *recordingHandler {
	return &recordingHandler{
		result: func(DeliveryContext, []byte) HandlerResult { return RejectDelivery(errTestRefused) },
	}
}

func failingHandler() *recordingHandler {
	return &recordingHandler{
		result: func(DeliveryContext, []byte) HandlerResult { return FailDelivery(errTestRefused) },
	}
}

// registerType is the shorthand every test uses for a simple registration.
func registerType(t *testing.T, node *pipelineNode, registration TypeRegistration) {
	t.Helper()
	registerTypeInto(t, node.types, registration)
}

// registerTypeInto is the same against a bare registry, for a test that reasons
// about the registration itself rather than about a node.
func registerTypeInto(t *testing.T, registry *TypeRegistry, registration TypeRegistration) {
	t.Helper()
	if err := registry.Register(registration); err != nil {
		t.Fatalf("Register(%s): %v", registration.DType.String(), err)
	}
}

func routedType(dtype domain.DType, handler Handler) TypeRegistration {
	return TypeRegistration{
		DType:   dtype,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassControl, domain.DatagramClassBulk},
		Handler: handler,
	}
}

func requestType(dtype domain.DType, handler Handler) TypeRegistration {
	return TypeRegistration{
		DType:   dtype,
		Modes:   []domain.DatagramMode{domain.DatagramModeRequest},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Handler: handler,
	}
}

func responseType(dtype domain.DType, answers domain.DType, handler Handler) TypeRegistration {
	return TypeRegistration{
		DType:     dtype,
		Modes:     []domain.DatagramMode{domain.DatagramModeResponse},
		Classes:   []domain.DatagramClass{domain.DatagramClassControl},
		AnswersTo: []domain.DType{answers},
		Handler:   handler,
	}
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

func requireDrop(t *testing.T, result InboundResult, reason DropReason) {
	t.Helper()
	if !result.Dropped() || result.Reason() != reason {
		t.Fatalf("want drop(%s), got outcome %s reason %s err %v",
			reason, result.Outcome(), result.Reason(), result.Err())
	}
}

func requireOutcome(t *testing.T, result InboundResult, outcome InboundOutcome) {
	t.Helper()
	if result.Outcome() != outcome {
		t.Fatalf("want outcome %s, got %s (reason %s, err %v)",
			outcome, result.Outcome(), result.Reason(), result.Err())
	}
}
