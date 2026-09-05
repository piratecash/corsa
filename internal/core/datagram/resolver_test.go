package datagram

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// resolver_test.go holds the scheduler fixtures shared by the M7 tests plus
// the checks of the resolver's own value types: the §6.1 dtypes contract
// and the wall-clock expiry shape of a route hint.

const schedLocalVersion = domain.ProtocolVersion(27)

// schedDType is a type no fixture declares unless it says so. Almost every
// scheduler test uses it, because the interesting gate — the last-hop dtype
// gate — has no set it waves through: a destination supports exactly the names
// it declared (§6.1), so a type has to be put there deliberately.
const schedDType = domain.DType("file_transfer")

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// schedRouteResolver serves two independent tables so a test can make the
// fresh lookup and the cached snapshot disagree — which is the only way to
// prove that a locally originated send reads the fresh one and a transit
// frame the cached one.
type schedRouteResolver struct {
	mu          sync.Mutex
	fresh       map[domain.PeerIdentity][]RouteHint
	cached      map[domain.PeerIdentity][]RouteHint
	onLookup    func()
	freshCalls  int
	cachedCalls int
}

func newSchedRouteResolver() *schedRouteResolver {
	return &schedRouteResolver{
		fresh:  make(map[domain.PeerIdentity][]RouteHint),
		cached: make(map[domain.PeerIdentity][]RouteHint),
	}
}

// set writes the same hints into both sources, the usual steady state.
func (r *schedRouteResolver) set(dst domain.PeerIdentity, hints ...RouteHint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fresh[dst] = hints
	r.cached[dst] = hints
}

func (r *schedRouteResolver) setFresh(dst domain.PeerIdentity, hints ...RouteHint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fresh[dst] = hints
}

func (r *schedRouteResolver) setCached(dst domain.PeerIdentity, hints ...RouteHint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cached[dst] = hints
}

// onEveryLookup stages a side effect that runs on every route lookup.
//
// It is the one point a test can act AFTER the early replay probe of §4.1 step 6
// and BEFORE the branch that ends the frame at step 7 — which is exactly the
// window a parallel instance of the same frame would take the reservation in.
// Nothing in production installs one.
func (r *schedRouteResolver) onEveryLookup(hook func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onLookup = hook
}

func (r *schedRouteResolver) runLookupHook() {
	r.mu.Lock()
	hook := r.onLookup
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (r *schedRouteResolver) FreshRoutes(_ context.Context, dst domain.PeerIdentity) []RouteHint {
	r.runLookupHook()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freshCalls++
	return r.fresh[dst]
}

func (r *schedRouteResolver) CachedRoutes(_ context.Context, dst domain.PeerIdentity) []RouteHint {
	r.runLookupHook()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedCalls++
	return r.cached[dst]
}

// sourceCalls reports how often each source was read, so a test can prove
// the unused one was never touched rather than merely unhelpful.
func (r *schedRouteResolver) sourceCalls() (fresh, cached int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.freshCalls, r.cachedCalls
}

// firstSendableConnection is the fake half of the PeerMetadata / DirectSession
// contract, written ONCE so both fakes answer identically — the node backs
// both with one helper and a fixture that let them drift would hide exactly
// the bug this seam exists to prevent.
//
// The contract it implements is the one resolver.go states: the first
// connection that passes the frame's gates; when none passes, the first LIVE
// one anyway, so the layer can name the refusal instead of hearing "no
// connection"; false only when the peer has no connection at all.
func firstSendableConnection(
	conns []PeerConnection,
	peer domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (PeerConnection, bool) {
	if len(conns) == 0 {
		return PeerConnection{}, false
	}
	for _, conn := range conns {
		if AdmitPeer(frame, peer, conn) {
			return conn, true
		}
	}
	return conns[0], true
}

// schedPeerMetadata answers with the connection the send path would pick for
// ONE frame.
//
// Peers may hold several connections in the fixture, in the send's own attempt
// order (outbound before inbound), exactly as the node-side helper lists them.
// A test that wants to prove "ranking follows the socket the bytes will use"
// registers an inbound connection of a newer version behind the outbound one;
// a test that wants to prove "a peer is not lost because its HEAD fails the
// gates" registers an incapable connection first and a capable one behind it.
type schedPeerMetadata struct {
	mu       sync.Mutex
	sendable map[domain.PeerIdentity][]PeerConnection
	// unusable models a stalled or dropped peer: present in the routing
	// table, but with no connection the send could use.
	unusable map[domain.PeerIdentity]bool
}

func newSchedPeerMetadata() *schedPeerMetadata {
	return &schedPeerMetadata{
		sendable: make(map[domain.PeerIdentity][]PeerConnection),
		unusable: make(map[domain.PeerIdentity]bool),
	}
}

func (p *schedPeerMetadata) set(peer domain.PeerIdentity, conn PeerConnection) {
	p.setAll(peer, conn)
}

// setAll registers every connection of a peer, in the order the send would
// try them.
func (p *schedPeerMetadata) setAll(peer domain.PeerIdentity, conns ...PeerConnection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sendable[peer] = conns
}

func (p *schedPeerMetadata) stall(peer domain.PeerIdentity) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unusable[peer] = true
}

func (p *schedPeerMetadata) SendableConnection(
	_ context.Context,
	peer domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (PeerConnection, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unusable[peer] {
		return PeerConnection{}, false
	}
	return firstSendableConnection(p.sendable[peer], peer, frame)
}

// schedDirectSession is deliberately a separate fake from schedPeerMetadata
// so a test can kill the direct path while relays stay healthy.
type schedDirectSession struct {
	mu    sync.Mutex
	conns map[domain.PeerIdentity][]PeerConnection
}

func newSchedDirectSession() *schedDirectSession {
	return &schedDirectSession{conns: make(map[domain.PeerIdentity][]PeerConnection)}
}

func (d *schedDirectSession) set(peer domain.PeerIdentity, conn PeerConnection) {
	d.setAll(peer, conn)
}

func (d *schedDirectSession) setAll(peer domain.PeerIdentity, conns ...PeerConnection) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[peer] = conns
}

func (d *schedDirectSession) LookupDirectSession(
	_ context.Context,
	dst domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (PeerConnection, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return firstSendableConnection(d.conns[dst], dst, frame)
}

// schedEmitter is the FrameEmitter of the fixture: it records every hop the
// pipeline actually published to, in order, so a test can assert both which
// hop took the frame and which hops were tried before it.
//
// It replaced a NextHopSender fake deliberately. The emitter is what the
// production send path uses, so a test that drives it exercises the same walk
// the node does — a second, test-only publication path is exactly how the
// whole §4.3 vocabulary came to live in code production never called.
type schedEmitter struct {
	mu     sync.Mutex
	refuse map[domain.PeerIdentity]bool
	// onEmit, if set, runs on the publishing goroutine as each frame is handed
	// over: the one moment inside forwardRouted where the reservation is held and
	// the commit has not happened yet. Nothing in production installs one.
	onEmit   func(protocol.DatagramFrame)
	attempts []domain.PeerIdentity
	frames   []protocol.DatagramFrame
}

func newSchedEmitter() *schedEmitter {
	return &schedEmitter{refuse: make(map[domain.PeerIdentity]bool)}
}

// refuseHop models the ordinary local refusal: the write queue would not take
// the frame and no policy verdict was made.
func (e *schedEmitter) refuseHop(peer domain.PeerIdentity) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refuse[peer] = true
}

func (e *schedEmitter) EmitTo(_ context.Context, out OutboundFrame) bool {
	e.mu.Lock()
	hook := e.onEmit
	e.mu.Unlock()
	if hook != nil {
		hook(out.Frame)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts = append(e.attempts, out.Peer)
	if e.refuse[out.Peer] {
		return false
	}
	e.frames = append(e.frames, out.Frame)
	return true
}

func (e *schedEmitter) tried() []domain.PeerIdentity {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]domain.PeerIdentity(nil), e.attempts...)
}

// failHop is refuseHop under the name a test uses when it wants to talk about
// the CAUSE rather than the fact: a write queue that would not take the frame
// is a local failure either way (§4.3).
func (e *schedEmitter) failHop(peer domain.PeerIdentity) { e.refuseHop(peer) }

func (e *schedEmitter) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts = nil
	e.frames = nil
}

type schedSecret struct{ secret []byte }

func (s schedSecret) NodeLocalSecret() []byte { return s.secret }

// schedFixtureOpts are the knobs one scheduler fixture is built with.
type schedFixtureOpts struct {
	// secret overrides node_local_secret, which seeds the explore offset.
	secret []byte
	// counters sizes the bounded explore-counter LRU.
	counters int
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

// schedFixture is a WHOLE layer instance — pipeline included — not a bare
// scheduler.
//
// That is the point of it: §4.3 has exactly two entry points into the layer,
// Pipeline.SendLocal and Pipeline.HandleInbound, and a fixture that reached
// past them into the scheduler could keep a whole vocabulary green while the
// production path answered `no_route` to everything.
type schedFixture struct {
	scheduler *Scheduler
	pipeline  *Pipeline
	routes    *schedRouteResolver
	peers     *schedPeerMetadata
	direct    *schedDirectSession
	sender    *schedEmitter
	types     *TypeRegistry
	replay    *BaseReplayCache
	reverse   *ReverseTable
	signer    ed25519.PrivateKey
	origin    ed25519.PrivateKey
	originID  domain.PeerIdentity
	local     domain.PeerIdentity
	now       time.Time
	nowMu     sync.Mutex
}

func newSchedFixture(t *testing.T, opts schedFixtureOpts) *schedFixture {
	t.Helper()
	signer, local := newSigner(t)
	origin, originID := newSigner(t)
	fixture := &schedFixture{
		routes:   newSchedRouteResolver(),
		peers:    newSchedPeerMetadata(),
		direct:   newSchedDirectSession(),
		sender:   newSchedEmitter(),
		types:    NewTypeRegistry(),
		signer:   signer,
		local:    local,
		origin:   origin,
		originID: originID,
		// Wall-clock based: the parity test in reachability_test.go runs
		// the same topology through the file router, which reads
		// time.Now() directly, so a fixed past date would make every
		// route look expired on that side only.
		now: time.Now().UTC(),
	}
	secret := opts.secret
	if len(secret) == 0 {
		secret = []byte("node-local-secret")
	}
	scheduler, err := NewScheduler(SchedulerConfig{
		Routes:               fixture.routes,
		Peers:                fixture.peers,
		Direct:               fixture.direct,
		Secret:               schedSecret{secret: secret},
		Clock:                fixture.clock,
		LocalID:              fixture.local,
		LocalProtocolVersion: schedLocalVersion,
		ExploreCounters:      opts.counters,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	fixture.scheduler = scheduler

	fixture.replay = NewBaseReplayCache(BaseReplayCacheConfig{Clock: fixture.clock})
	fixture.reverse = NewReverseTable(ReverseTableConfig{Clock: fixture.clock})
	fixture.pipeline = fixture.newPipeline(t, fixture.replay)
	return fixture
}

// newPipeline builds the conveyor over the fixture's anti-replay cache.
func (f *schedFixture) newPipeline(t *testing.T, cache *BaseReplayCache) *Pipeline {
	t.Helper()
	pipeline, err := NewPipeline(PipelineConfig{
		Clock:       f.clock,
		Types:       f.types,
		ReplayCache: cache,
		Reverse:     f.reverse,
		Scheduler:   f.scheduler,
		Emitter:     f.sender,
		Advertised:  advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		Network:     testNetwork,
		LocalID:     f.local,
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	return pipeline
}

func (f *schedFixture) clock() time.Time {
	f.nowMu.Lock()
	defer f.nowMu.Unlock()
	return f.now
}

// setNow pins the fixture clock, used by the parity test to anchor both
// routers to one instant.
func (f *schedFixture) setNow(now time.Time) {
	f.nowMu.Lock()
	defer f.nowMu.Unlock()
	f.now = now
}

func (f *schedFixture) advance(d time.Duration) {
	f.nowMu.Lock()
	defer f.nowMu.Unlock()
	f.now = f.now.Add(d)
}

// declaredDTypesOf renders a fixture's dtype list as a handshake
// declaration. No names at all is the ABSENT field — what a peer which never
// sent `dtypes` states, and §6.1 reads it as naming no type at all. A fixture
// that wants the EXPLICITLY EMPTY set builds it from
// domain.ParseDeclaredDTypes(nil) directly: the two forms name the same set,
// but they are different statements about the peer and the wire keeps them
// apart.
func declaredDTypesOf(names []string) DeclaredDTypes {
	if len(names) == 0 {
		return NewDeclaredDTypes(domain.AbsentDTypes())
	}
	return NewDeclaredDTypes(domain.ParseDeclaredDTypes(names))
}

// datagramPeer registers a peer that is a full participant: endpoint and
// transit capable, at the local protocol version, connected `age` ago, with
// the given declared dtypes.
func (f *schedFixture) datagramPeer(peer domain.PeerIdentity, age time.Duration, dtypes ...string) PeerConnection {
	conn := PeerConnection{
		ConnectedAt: f.clock().Add(-age),
		Advertised:  advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:      declaredDTypesOf(dtypes),
		// Mesh, because that is what the node reports for every connection it
		// holds (datagramConnectionPlane). A fixture that left the plane unset
		// would exercise a shape production never produces and would hide any
		// path that quietly needs an attributed connection.
		Discovery:               domain.DiscoveryPlaneMesh,
		ReportedProtocolVersion: schedLocalVersion,
	}
	f.peers.set(peer, conn)
	return conn
}

// route is the ordinary mesh hint: attributed exactly as the node's resolver
// attributes what it reads out of the routing table, so the default fixture
// route has the shape production produces rather than a bare one.
func (f *schedFixture) route(nextHop domain.PeerIdentity, hops int) RouteHint {
	return RouteHint{
		NextHop:     nextHop,
		Hops:        hops,
		ExpiresAt:   f.clock().Add(time.Hour),
		Attribution: domain.MeshRouteAttribution(domain.RouteSourceAnnouncement),
	}
}

// frame builds a signed routed datagram of schedDType, exactly as a caller of
// the layer would hand it to SendLocal.
func (f *schedFixture) frame(
	t *testing.T,
	signer ed25519.PrivateKey,
	src, dst domain.PeerIdentity,
	mutators ...func(*protocol.DatagramFrame),
) protocol.DatagramFrame {
	t.Helper()
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand: %v", err)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         src,
		Dst:         dst,
		TTL:         OriginTTL(),
		RoutePolicy: domain.RoutePolicyBest,
		DType:       schedDType,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        salt,
			MaxTTL:      OriginTTL(),
			Time:        f.clock().Unix(),
		},
	}
	for _, mutate := range mutators {
		mutate(&frame)
	}
	signed, err := protocol.SignDatagram(frame, testNetwork, signer)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return signed
}

// replayKeyOf derives the replay key of a frame exactly as the layer does, so
// a test can pre-arm state that is keyed by it.
func replayKeyOf(t *testing.T, frame protocol.DatagramFrame) domain.ReplayKey {
	t.Helper()
	transcript, err := protocol.BuildDatagramTranscript(frame, testNetwork)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript: %v", err)
	}
	return protocol.DatagramReplayKey(transcript)
}

// send is the PUBLIC local-send path: exactly what a migration adapter calls.
func (f *schedFixture) send(
	t *testing.T,
	dst domain.PeerIdentity,
	mutators ...func(*protocol.DatagramFrame),
) SendOutcome {
	t.Helper()
	return f.sendAvoiding(t, dst, NoAvoidedNextHop(), mutators...)
}

// sendAvoiding is the same public path carrying the avoid_next_hop parameter
// of §4.3 — the one a migration retry uses (§8).
func (f *schedFixture) sendAvoiding(
	t *testing.T,
	dst domain.PeerIdentity,
	avoid AvoidedNextHop,
	mutators ...func(*protocol.DatagramFrame),
) SendOutcome {
	t.Helper()
	return f.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: f.frame(t, f.signer, f.local, dst, mutators...),
		Avoid: avoid,
	})
}

// transit is the OTHER public entry: a frame somebody else signed, arriving
// from a neighbour. Its refusals are silent drops, which is the asymmetry
// §4.3 item 4 draws.
func (f *schedFixture) transit(
	t *testing.T,
	via domain.PeerIdentity,
	dst domain.PeerIdentity,
	mutators ...func(*protocol.DatagramFrame),
) InboundResult {
	t.Helper()
	frame := f.frame(t, f.origin, f.originID, dst, mutators...)
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	return f.pipeline.HandleInbound(context.Background(), InboundOpts{
		Line:       []byte(line),
		Peer:       via,
		Channel:    testChannel(via.String()),
		BudgetKey:  ProvenIdentityKey(via),
		ReceivedAt: f.clock(),
	})
}

func withExplore(frame *protocol.DatagramFrame) {
	frame.RoutePolicy = domain.RoutePolicyExplore
}

func withDType(dtype domain.DType) func(*protocol.DatagramFrame) {
	return func(frame *protocol.DatagramFrame) { frame.DType = dtype }
}

// ---------------------------------------------------------------------------
// The §6.1 dtypes contract
// ---------------------------------------------------------------------------

// TestDeclaredDTypesWithoutNamesSupportsNothing pins the rule that replaced the
// implied baseline of the withdrawn draft: the declared field IS the set,
// so a peer that listed no name is an endpoint for NO type.
//
// The four ways to arrive at "no names" are covered together because §6.1 gives
// them one meaning and the layer must not grow a second: the field was never
// sent, the session was never recorded (the zero value), the field was sent
// empty, and the field breached its bounds and was therefore ignored whole.
//
// The mutation this kills: crediting an absent field with a set of types —
// which is how every v27 peer advertising mesh_datagram_v1 came to count as an
// endpoint for four types no build implements.
func TestDeclaredDTypesWithoutNamesSupportsNothing(t *testing.T) {
	t.Parallel()

	// The names of the withdrawn baseline are written out here on purpose: they
	// are the exact set that used to be credited for free, so they are what a
	// regression would grant again.
	withdrawnBaseline := []domain.DType{"cached_identity", "get_identity", "post_identity", "push_identity"}

	tooMany := make([]string, domain.MaxDTypesPerNode+1)
	for i := range tooMany {
		tooMany[i] = "type_" + strings.Repeat("x", i%8+1)
	}
	cases := map[string]DeclaredDTypes{
		"field never sent":       NewDeclaredDTypes(domain.AbsentDTypes()),
		"handshake not recorded": {},
		"explicitly empty":       NewDeclaredDTypes(domain.ParseDeclaredDTypes(nil)),
		"breach: too many names": NewDeclaredDTypes(domain.ParseDeclaredDTypes(tooMany)),
		"breach: bad alphabet":   NewDeclaredDTypes(domain.ParseDeclaredDTypes([]string{"file_transfer", "Bad-Name"})),
		"breach: too long":       NewDeclaredDTypes(domain.ParseDeclaredDTypes([]string{strings.Repeat("a", domain.MaxDTypeLen+1)})),
		"breach: empty name":     NewDeclaredDTypes(domain.ParseDeclaredDTypes([]string{""})),
	}
	for name, declared := range cases {
		t.Run(name, func(t *testing.T) {
			for _, dtype := range withdrawnBaseline {
				if declared.Supports(dtype) {
					t.Fatalf("supports %q: no handler for it was ever promised", dtype)
				}
			}
			if declared.Supports(schedDType) {
				t.Fatalf("supports the undeclared %q", schedDType)
			}
			if declared.Supports("file_transfer") {
				t.Fatal("a breached field must not keep the valid names it carried")
			}
		})
	}
}

// TestDeclaredDTypesExplicitFieldIsTakenLiterally pins the sent field as the
// SET it is declared to be: the names it carries are supported, everything
// else is not — a name from the withdrawn baseline included. The field is the
// only statement a peer makes about its handlers (§6.1).
func TestDeclaredDTypesExplicitFieldIsTakenLiterally(t *testing.T) {
	t.Parallel()

	declared := NewDeclaredDTypes(domain.ParseDeclaredDTypes([]string{"file_transfer", "file_transfer"}))
	if len(declared.declared) != 1 {
		t.Fatalf("duplicates must collapse: len = %d", len(declared.declared))
	}
	if !declared.Supports("file_transfer") {
		t.Fatal("a declared name must be supported")
	}
	if declared.Supports("get_identity") {
		t.Fatal("a name absent from an explicit set must not be supported: the field IS the set")
	}
	if declared.Supports("dm_receipt") {
		t.Fatal("an undeclared type must not be supported")
	}
}

// TestRouteHintExpiryIsInclusive keeps the layer's expiry aligned with the
// routing table's: a route whose deadline equals now is expired, so
// "ttl_seconds = 0" and "expired" never disagree.
func TestRouteHintExpiryIsInclusive(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	if (RouteHint{}).IsExpired(now) {
		t.Fatal("a hint without a deadline must never expire")
	}
	if !(RouteHint{ExpiresAt: now}).IsExpired(now) {
		t.Fatal("expiry must be inclusive")
	}
	if (RouteHint{ExpiresAt: now.Add(time.Nanosecond)}).IsExpired(now) {
		t.Fatal("a hint one tick in the future must be alive")
	}
}

// TestNewSchedulerRefusesIncompleteWiring keeps a half-wired scheduler from
// looking like a routing bug months later: a nil dependency is refused at
// construction, not nil-checked per call.
func TestNewSchedulerRefusesIncompleteWiring(t *testing.T) {
	t.Parallel()

	complete := SchedulerConfig{
		Routes:               newSchedRouteResolver(),
		Peers:                newSchedPeerMetadata(),
		Direct:               newSchedDirectSession(),
		Secret:               schedSecret{secret: []byte("s")},
		LocalID:              domaintest.ID("local-node"),
		LocalProtocolVersion: schedLocalVersion,
	}
	if _, err := NewScheduler(complete); err != nil {
		t.Fatalf("complete config must construct: %v", err)
	}

	broken := map[string]func(*SchedulerConfig){
		"no routes":  func(c *SchedulerConfig) { c.Routes = nil },
		"no peers":   func(c *SchedulerConfig) { c.Peers = nil },
		"no direct":  func(c *SchedulerConfig) { c.Direct = nil },
		"no secret":  func(c *SchedulerConfig) { c.Secret = nil },
		"empty seed": func(c *SchedulerConfig) { c.Secret = schedSecret{} },
		"no local":   func(c *SchedulerConfig) { c.LocalID = domain.PeerIdentity{} },
		"no version": func(c *SchedulerConfig) { c.LocalProtocolVersion = 0 },
	}
	for name, breakIt := range broken {
		t.Run(name, func(t *testing.T) {
			cfg := complete
			breakIt(&cfg)
			if _, err := NewScheduler(cfg); err == nil {
				t.Fatal("expected refusal")
			}
		})
	}
}
