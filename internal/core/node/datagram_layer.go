package node

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_layer.go constructs the datagram transport layer and owns its
// schedules.
//
// The layer starts no goroutine of its own — that is its stated contract — so
// everything it needs driven is driven from here: the outbound class-queue
// pump, the queue's expiry sweep, the reverse-state sweep and the base replay
// cache's expiry sweep. All four are ordinary lifecycle loops — they stop on
// the Run context and are joined by stopRunLifecycle through goRunLoop, so Run
// cannot return while one is still inside a call into the layer — and none of
// them holds a domain mutex while calling into it.
//
// The whole file is dead weight when cfg.EnableDatagramV1 is false: nothing is
// constructed, nothing is started, and not one allocation is made (§10).
//
// Reference: docs/refactoring/datagram-transport.md §4.2, §4.3, §5, §6, §10.

// datagramPlaneParts are the components the plane is assembled from.
//
// They are a struct of their own because the assembly reads as a dependency
// graph: the registry and the anti-replay cache first, the scheduler over the
// node adapters, and the pipeline last because it needs all of them.
type datagramPlaneParts struct {
	scheduler   *datagram.Scheduler
	admission   *datagram.PeerAdmission
	queue       *datagram.WeightedQueue
	metrics     *datagram.Metrics
	types       *datagram.TypeRegistry
	replayCache *datagram.BaseReplayCache
	reverse     *datagram.ReverseTable
	network     domain.NetworkID
	localID     domain.PeerIdentity
	clock       func() time.Time
	limits      datagram.Limits

	// maintenancePace is the cadence of datagramMaintenanceLoop. Zero means
	// datagramMaintenanceInterval; it is a field rather than the bare constant
	// because the cadence is BEHAVIOUR, and the shutdown contract of that loop
	// (a pass in flight holds Run open until it returns) cannot be observed by a
	// test that has to wait out a ten-second tick first.
	maintenancePace time.Duration

	// replayCacheSweep is the maintenance PASS itself. Zero means the cache's
	// own full sweep, which is the only pass production ever runs.
	//
	// It is a seam of the LIFECYCLE and not of the memory: what a test has to be
	// able to stage here is a pass that is still executing when Run is asked to
	// stop, so the join can be observed at all — and staging that inside the
	// cache would mean a memory that can block, which is exactly what naming
	// *datagram.BaseReplayCache everywhere else makes impossible.
	replayCacheSweep func(ctx context.Context) int
}

// maintenanceInterval is the cadence this plane's maintenance loop runs at,
// with the default standing in for the zero value so the struct-literal
// fixtures get a bounded one.
func (p datagramPlaneParts) maintenanceInterval() time.Duration {
	if p.maintenancePace > 0 {
		return p.maintenancePace
	}
	return datagramMaintenanceInterval
}

// maintenancePass is the one pass datagramMaintenanceLoop performs, with the
// cache's own full sweep standing in for the zero value the same way the
// interval above does.
func (p datagramPlaneParts) maintenancePass() func(ctx context.Context) int {
	if p.replayCacheSweep != nil {
		return p.replayCacheSweep
	}
	return p.replayCache.SweepExpired
}

// datagramLayer is the assembled plane: the conveyor plus the components the
// node has to drive or observe.
//
// The handle is IMMUTABLE — no field is ever written after assembly — which is
// why it lives outside the seven-domain mutex scheme (see docs/locking.md and
// the field comment on Service.datagramPlane): a reader takes the pointer once
// and works off a consistent snapshot, and each component carries its own
// synchronisation.
type datagramLayer struct {
	datagramPlaneParts
	pipeline *datagram.Pipeline
	outbound *datagram.ClassQueueEmitter
}

// datagramQueueSweepInterval is how often expired frames are dropped from the
// class queue. A frame's deadline is its class's queue residence — 5 s for
// control, 30 s for bulk (§4.2) — so a one-second sweep resolves both by well
// under a fifth of the shorter window, while costing one pass over two bounded
// lanes.
//
// The sweep is not what enforces the deadline: the writer re-checks send_until
// immediately before the socket write, and the queue drops expired heads as it
// dispatches. It exists so a lane that stops being dequeued — no traffic, or a
// downstream that refuses everything — does not hold dead frames against its
// byte cap and starve live ones.
const datagramQueueSweepInterval = time.Second

// datagramReverseSweepInterval is how often the reverse-state table is swept.
// A record lives 240 s (§4.2) and the table is bounded, so the sweep is a
// memory hygiene pass rather than a correctness one — expiry is judged against
// the clock on every lookup. A five-second cadence keeps the pass cheap and
// the depth honest for the diagnostic.
const datagramReverseSweepInterval = 5 * time.Second

// datagramMaintenanceInterval paces the base replay cache's expiry sweep — the
// one O(n) walk over bounded state that no request path performs for itself.
// The records it frees live five minutes, so a one-second cadence would burn
// CPU to observe nothing and ten seconds is well inside the window.
const datagramMaintenanceInterval = 10 * time.Second

// datagramLayer returns the plane this Service is serving, or nil when the
// feature flag is off.
//
// The handle is immutable and is stored exactly once, during construction, so
// every caller takes the pointer and works off a consistent snapshot.
func (s *Service) datagramLayer() *datagramLayer { return s.datagramPlane.Load() }

// newDatagramLayer assembles the plane for this node. It returns nil, nil when
// the feature flag is off — the caller stores the nil and every path that
// touches the layer stays a nil check away from the pre-datagram behaviour.
func newDatagramLayer(svc *Service, metrics *datagram.Metrics) (*datagramLayer, error) {
	if !svc.cfg.EnableDatagramV1 {
		return nil, nil
	}
	parts, err := newDatagramPlaneParts(svc, metrics)
	if err != nil {
		return nil, err
	}
	return assembleDatagramLayer(svc, parts)
}

// newDatagramPlaneParts builds everything the conveyor is assembled over.
func newDatagramPlaneParts(svc *Service, metrics *datagram.Metrics) (datagramPlaneParts, error) {
	localID := domain.PeerIdentityFromWire(svc.identity.Address)
	if localID.IsZero() {
		return datagramPlaneParts{}, errors.New("datagram: the layer needs a local identity")
	}
	network, err := domain.ParseNetworkID(networkName)
	if err != nil {
		return datagramPlaneParts{}, err
	}

	limits := datagram.DefaultLimits().Normalized()
	clock := func() time.Time { return time.Now().UTC() }

	// The registry starts with the identity-discovery kit
	// (docs/protocol/identity-lookup.md): get_identity / post_identity /
	// push_identity. Registration happens HERE — before any handshake can
	// run — because §6.1 fixes the declared dtype set for the lifetime of a
	// session, and the self identity record issued later in NewService
	// declares this very set.
	types := datagram.NewTypeRegistry()
	if err := registerIdentityDiscoveryTypes(types, svc, network); err != nil {
		return datagramPlaneParts{}, err
	}
	if err := registerDMControlTypes(types, svc); err != nil {
		return datagramPlaneParts{}, err
	}

	// The poll order of the route planes is fixed HERE, by this literal, and
	// nowhere else — not by configuration (dht-dualstack-migration.md §4.2).
	// Today the mesh is the only plane, so the composite forwards its answer
	// unchanged; step 09 appends the overlay to this list and every caller
	// below stays as it is.
	routes, err := datagram.NewCompositeRouteResolver(datagramRouteResolver{service: svc})
	if err != nil {
		return datagramPlaneParts{}, err
	}

	scheduler, err := datagram.NewScheduler(datagram.SchedulerConfig{
		Routes:               routes,
		Peers:                datagramPeerMetadata{service: svc},
		Direct:               datagramDirectSession{service: svc},
		Secret:               newDatagramNodeSecret(svc.identity.PrivateKey),
		Clock:                clock,
		LocalID:              localID,
		LocalProtocolVersion: domain.ProtocolVersion(config.ProtocolVersion),
	})
	if err != nil {
		return datagramPlaneParts{}, err
	}

	return datagramPlaneParts{
		scheduler:   scheduler,
		admission:   datagram.NewPeerAdmission(datagram.AdmissionConfig{Clock: clock, Budget: limits.Peer}),
		queue:       datagram.NewWeightedQueue(datagram.WeightedQueueConfig{Clock: clock, Caps: limits.Queue}),
		metrics:     metrics,
		types:       types,
		replayCache: datagram.NewBaseReplayCache(limits.BaseReplayCacheConfig(clock)),
		reverse:     datagram.NewReverseTable(limits.ReverseTableConfig(clock, metrics)),
		network:     network,
		localID:     localID,
		clock:       clock,
		limits:      limits,
	}, nil
}

// assembleDatagramLayer builds the conveyor over the given parts.
func assembleDatagramLayer(svc *Service, parts datagramPlaneParts) (*datagramLayer, error) {
	pipeline, err := datagram.NewPipeline(datagram.PipelineConfig{
		Clock:       parts.clock,
		Types:       parts.types,
		ReplayCache: parts.replayCache,
		Reverse:     parts.reverse,
		Scheduler:   parts.scheduler,
		Emitter:     datagramFrameEmitter{service: svc},
		Queue:       parts.queue,
		// STAGE TWO ONLY, and the SAME controller stage one is charged on.
		//
		// There is no Admission field to set beside it: stage one is charged one
		// step above, in handleDatagramFrame, because it has to stand above the
		// two refusals that never reach the conveyor — a connection off the plane
		// and an oversize line. The conveyor charges stage two, on the key
		// handleDatagramFrame hands it, so both stages of §5 bill one bucket per
		// neighbour.
		Crypto:     parts.admission,
		Metrics:    parts.metrics,
		Advertised: svc.localAdvertisedCapabilities(),
		Network:    parts.network,
		LocalID:    parts.localID,
	})
	if err != nil {
		return nil, err
	}

	// The queue was supplied above, so the pipeline must have put the class
	// queue between the conveyor and the emitter. Checking the bool rather
	// than discarding it keeps the §5 promise — "bulk keeps a guaranteed
	// share under a constant control stream" — a property of the real path:
	// a silently absent queue would publish straight to the writer, first
	// come first served, and nothing else in the tree would notice.
	outbound, queued := pipeline.OutboundQueue()
	if !queued {
		return nil, errors.New("datagram: the pipeline did not install the class queue")
	}
	return &datagramLayer{
		datagramPlaneParts: parts,
		pipeline:           pipeline,
		outbound:           outbound,
	}, nil
}

// localAdvertisedCapabilities is the RAW capability set THIS node publishes,
// in the layer's own type.
//
// The pipeline reads it for the transit gate of §4.1 step 11, which must agree
// with what this node tells the network, or it would relay while claiming it
// does not. It is therefore built from exactly the function the hello/welcome
// frames are built from, localCapabilityStrings, and there is no second notion
// of "what this build could serve" beside it.
//
// It reads datagramPlaneCapability and NOT localDatagramAdvertise, and the
// difference is that this is a CONSTRUCTION-TIME snapshot taken before the
// Service has stored the layer: an advertise that answered "no plane yet" would
// be frozen in here as an empty set and the gate would refuse to relay for the
// lifetime of the process. What this set has to state is the capability the
// WIRE claims when the plane is up, and that is exactly
// datagramPlaneCapability.
//
// The snapshot is sound because both inputs of datagramPlaneCapability — the
// feature flag and the node type — are immutable after New. It used to read the
// type registry, which is filled AFTER the layer is constructed, and that was
// the one way this gate could drift from the wire.
func (s *Service) localAdvertisedCapabilities() datagram.AdvertisedCapabilities {
	return datagram.NewAdvertisedCapabilities(
		localCapabilityStrings(s.cfg.EnableMeshRoutingV3, s.datagramPlaneCapability()),
	)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// startDatagramLayer launches the layer's schedules. Every goroutine returns on
// ctx.Done, so Run's cancellation is the single stop signal — there is no second
// shutdown channel to keep in sync.
//
// No-op without the layer, which is what keeps a flag-off node at exactly its
// previous goroutine count.
func (s *Service) startDatagramLayer(ctx context.Context) {
	layer := s.datagramLayer()
	if layer == nil {
		return
	}
	s.startDatagramSchedules(ctx, layer)
}

// startDatagramSchedules launches everything the layer needs driven. The layer
// starts no goroutine of its own — that is its stated contract — so this is the
// single place they exist.
//
// They go through goRunLoop like every other loop that lives for the whole of
// Run, and they are not a subsystem of their own any more. They used to be: the
// plane owned durable stores, and Run had to join THAT set before the owner of
// those stores was stopped. The stores are gone — what these four do is an
// in-memory sweep and a queue pump — so what is left is exactly the property
// goRunLoop is for: a loop that stops on the lifecycle context and must be
// waited for rather than merely asked, because a pass in flight is a call into
// the layer while the runtime is entitled to close what that call reaches.
//
// # Bounded on cancellation, per worker
//
// Joining is only safe if every worker really ends, so each one is audited
// rather than assumed:
//
//   - outbound.Run — selects on ctx.Done between drains; a drain hands frames
//     to the writer, which is socket I/O under the frame's own send deadline;
//   - sweepQueueLoop, sweepReverseLoop — tickers over in-memory tables, no I/O
//     at all, one select on ctx.Done;
//   - datagramMaintenanceLoop — a ticker whose single pass is the base replay
//     cache's expiry sweep, which takes ctx and is in-memory arithmetic.
//
// So the one thing that could make this wait unbounded is a component that
// ignores the context it is given. That is a defect in that component, and
// blocking on it is the correct response: the alternative — a timeout here —
// hands the caller a "shutdown complete" it can act on by closing what the
// worker is still inside of.
func (s *Service) startDatagramSchedules(ctx context.Context, layer *datagramLayer) {
	// The outbound pump. It is the only consumer of the class queue, so the
	// deficit round robin of §5 decides the real send order rather than the
	// order frames happened to be produced in.
	s.goRunLoop(func() { layer.outbound.Run(ctx) })
	s.goRunLoop(func() { layer.sweepQueueLoop(ctx) })
	s.goRunLoop(func() { layer.sweepReverseLoop(ctx) })
	s.goRunLoop(func() { s.datagramMaintenanceLoop(ctx, layer) })
	// The conversation-control outbox. A pass is a ticker over an in-memory
	// map plus, at most, a few enqueues into the outbound queue above — the
	// same bounded shape as the other loops here, and it selects on ctx.Done.
	//
	// The door is opened HERE, synchronously, and not inside the goroutine.
	// A session can form the moment the handshake paths are live, and the first
	// thing one does is re-offer that conversation's reactions; if the flag were
	// still down because the goroutine had not been scheduled, the re-offer
	// would be refused and would wait for the NEXT session. The loop still
	// closes the door on its way out (stop).
	s.dmControl.setDraining(true)
	s.goRunLoop(func() { s.dmControlSendLoop(ctx) })

	log.Info().
		Bool("transit", s.localDatagramAdvertise().Transit).
		Msg("datagram_layer_started")
}

// datagramPlaneReady reports whether this node has a plane at all.
//
// It is what localDatagramAdvertise answers from, so the two capabilities of §6
// are claimed exactly while there is a conveyor behind them. A node without a
// layer answers false, and every path that touches the plane is one nil check
// away from the pre-datagram behaviour.
func (s *Service) datagramPlaneReady() bool {
	return s.datagramLayer() != nil
}

// sweepQueueLoop drops frames whose send deadline passed while they waited.
func (l *datagramLayer) sweepQueueLoop(ctx context.Context) {
	ticker := time.NewTicker(datagramQueueSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if dropped := l.queue.DropExpired(); dropped > 0 {
				log.Debug().Int("dropped", dropped).Msg("datagram_queue_expired_dropped")
			}
		}
	}
}

// sweepReverseLoop evicts reverse-state records past their 240 s window.
func (l *datagramLayer) sweepReverseLoop(ctx context.Context) {
	ticker := time.NewTicker(datagramReverseSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.pipeline.ReverseState().Sweep()
		}
	}
}

// datagramMaintenanceLoop runs the O(n) pass over bounded state that no request
// path performs for itself: the base replay cache's expiry sweep.
//
// It has a cause of its own — the anti-replay cache frees records on its own
// receive path, so a plane that stops receiving holds them until traffic returns
// — and it lives on a loop rather than inside Reserve because that is the one
// place a node with no traffic is still reached.
func (s *Service) datagramMaintenanceLoop(ctx context.Context, layer *datagramLayer) {
	ticker := time.NewTicker(layer.maintenanceInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runDatagramMaintenancePass(ctx, layer)
		}
	}
}

// runDatagramMaintenancePass is ONE tick, extracted so a test can drive the
// pass without waiting out the ticker.
func (s *Service) runDatagramMaintenancePass(ctx context.Context, layer *datagramLayer) {
	if swept := layer.sweepBaseReplayCache(ctx); swept > 0 {
		log.Debug().Int("swept", swept).Msg("datagram_base_replay_expired_swept")
	}
}

// sweepBaseReplayCache runs the base plane's full expiry pass and reports how
// many records went.
//
// It is HERE and not only inside Reserve because the cache frees records on its
// own receive path: the pass inside Reserve is bounded and runs when a frame
// arrives, so a node whose traffic stops keeps every record it holds until
// traffic returns. Five minutes of frames from every neighbour is what that
// memory costs, and nothing else in the process would ever hand it back.
//
// It goes through maintenancePass rather than calling the cache directly
// because the PASS is the plane's lifecycle seam: the join contract of
// datagramMaintenanceLoop is only observable against a pass that is still
// running when the context is cancelled.
func (l *datagramLayer) sweepBaseReplayCache(ctx context.Context) int {
	return l.maintenancePass()(ctx)
}

// forgetDatagramPeer offers a neighbour's admission buckets back when its
// session ends.
//
// It is deliberately an OFFER and not a deletion, and the honest reading is
// this: PeerAdmission.Forget drops a bucket only when dropping it forgives
// nothing — the bucket has been idle for IdleRetention AND has refilled to
// full. A session that closes while the peer was recently talking therefore
// keeps its bucket, and that is the intended answer, not a missed case:
// deleting a half-spent bucket would hand the debt straight back to a peer
// that chose the moment of the reconnect, which is the free burst the eviction
// path already treats as something with a price.
//
// So the call earns its keep on exactly one shape — a session that has been
// silent past IdleRetention and is now closing, whose bucket is released here
// instead of waiting for an eviction to notice it — and it is a no-op on every
// other. The map is bounded either way; what this avoids is reaching that
// bound by evicting somebody else.
//
// It offers back the PROVEN-identity bucket only, because an identity is all
// the close path has: onPeerSessionClosed is reached from both directions and
// is addressed by identity throughout. The dial-address bucket of an outbound
// session is therefore released by evictLocked's first pass instead — the same
// idle-and-refilled test, applied when the map next needs room. That costs
// nothing that this call would have saved: on both paths the bucket survives
// exactly as long as it still owes something.
func (s *Service) forgetDatagramPeer(peer domain.PeerIdentity) {
	layer := s.datagramLayer()
	if layer == nil {
		return
	}
	layer.admission.Forget(datagram.ProvenIdentityKey(peer))
}
