package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// identity_resolver.go is the initiator engine of the identity lookup
// (docs/protocol/identity-lookup.md, design §4.3): the two-phase retry
// machine that sends get_identity datagrams, correlates post_identity
// answers by the one-shot attempt label, verifies proof and merges the
// record.
//
// Reliability lives here and nowhere else: relays only forward, the network
// signals no refusals, silence is cured by a retry with a FRESH label and
// surrender is recorded locally. A lookup is idempotent — it is a question,
// not an artifact — so no retry is ever suppressed by dedup.

const (
	// identityLookupAttemptWindow is how long one attempt may still be
	// answered after its send.
	identityLookupAttemptWindow = 60 * time.Second

	// identityLookupMaxOpenAttempts caps live attempt records per
	// resolution: interactive sends at 60-second windows overlap.
	identityLookupMaxOpenAttempts = 5

	// identityLookupCooldown separates a terminal from the next resolution
	// of the same target.
	identityLookupCooldown = 30 * time.Second

	// identityLookupInteractiveDeadline ends phase 1: the last interactive
	// send at t=32s plus the 45-second final wait.
	identityLookupInteractiveDeadline = 77 * time.Second

	// identityLookupGateRetry is the re-check cadence while the routing
	// gate is closed; the route-event kick usually fires earlier.
	identityLookupGateRetry = 2 * time.Second

	// identityLookupTaskLifetime is the hard ceiling of one resolution,
	// background phase included.
	identityLookupTaskLifetime = 7 * 24 * time.Hour

	// identityLookupDefaultBGAttempts is the background-phase attempt cap
	// when the config does not override it.
	identityLookupDefaultBGAttempts = 20

	// identityLookupSendSlots is the global concurrent-send cap. A slot is
	// held only for the synchronous enqueue, never for the attempt window —
	// otherwise three attempts of one resolution would own the whole pool
	// for a minute.
	identityLookupSendSlots = 3
)

// identityLookupInteractiveAt are the phase-1 send offsets from resolution
// start: t = 0 / 1 / 4 / 12 / 32 s.
var identityLookupInteractiveAt = []time.Duration{
	0,
	1 * time.Second,
	4 * time.Second,
	12 * time.Second,
	32 * time.Second,
}

// identityLookupBackgroundDelays is the phase-2 exponential ladder; the
// last value is the interval ceiling.
var identityLookupBackgroundDelays = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	11 * time.Minute,
}

// identityResolutionPhase is which half of the retry machine a resolution
// is in.
type identityResolutionPhase uint8

const (
	identityPhaseInteractive identityResolutionPhase = iota
	identityPhaseBackground
)

// identityResolution is one single-flight lookup of one target.
type identityResolution struct {
	createdAt time.Time
	// nextSendAt is when the engine looks at this resolution again.
	nextSendAt time.Time
	// id is the stable operation identifier: RPC returns it, events and
	// the status poll key on it. The attempt labels are SEPARATE one-shot
	// wire tags (§4.3 "две сущности идентификации").
	id  string
	dst domain.PeerIdentity
	// prevNextHop is the first hop of the previous attempt; the retry
	// avoids it (§4.3: strict first-hop change comes from avoid_next_hop,
	// not from the explore rotation).
	prevNextHop domain.PeerIdentity
	// minSeq is the requirement checked by the INITIATOR on the received
	// record; recovery flows raise it above zero.
	minSeq domain.IdentityRecordSeq
	// lifecycle / authority / dmAvailable / usable are the §4.9 axes.
	lifecycle   domain.IdentityResolutionLifecycle
	authority   domain.IdentityRecordAuthority
	dmAvailable domain.DMAvailability
	usable      bool
	// interactiveIndex is the next phase-1 send slot.
	interactiveIndex int
	// bgAttempts counts phase-2 sends, durable via the intent store.
	bgAttempts int
	// openAttempts counts live attempt windows of this resolution.
	openAttempts int
	phase        identityResolutionPhase
	// interactiveTimeout / noRoute are progress flags, not terminals.
	interactiveTimeout bool
	noRoute            bool
	// answerAttemptGen is the generation of the attempt whose proven
	// answer terminated the resolution — see the ebus payload field.
	answerAttemptGen uint64
	// sentAnything reports whether at least one attempt reached a queue —
	// it separates "explore with avoid" retries from the first send.
	sentAnything bool
}

// stateLocked projects the resolution onto the published event shape.
// Caller holds r.mu.
func (res *identityResolution) stateLocked() ebus.IdentityResolutionState {
	return ebus.IdentityResolutionState{
		ResolutionID:       res.id,
		Target:             res.dst,
		Lifecycle:          res.lifecycle,
		Authority:          res.authority,
		DMAvailable:        res.dmAvailable,
		Usable:             res.usable,
		InteractiveTimeout: res.interactiveTimeout,
		NoRoute:            res.noRoute,
		AnswerAttemptGen:   res.answerAttemptGen,
	}
}

// identityAttemptEntry correlates one in-flight attempt with its
// resolution. The label raw bytes are the map key.
type identityAttemptEntry struct {
	sentAt  time.Time
	owner   *identityResolution
	action  identityLookupActionStamp
	queue   identityAttemptQueue
	claim   *identityLookupCompletion
	revoked bool
	// gen is the resolver-wide monotonic attempt generation: it orders
	// attempts against watermarks handed out by CurrentAttemptGen without
	// touching the wall clock (a clock step must never re-validate an old
	// question or wedge new ones).
	gen           uint64
	dst           domain.PeerIdentity
	qHash         [sha256.Size]byte
	minSeq        domain.IdentityRecordSeq
	proofRequired bool
}

// identityResolver is the engine. It owns its state under its own mutex —
// no Service domain mutex is ever held around its work; everything it
// needs from the Service goes through the svc reference on the goroutine's
// own stack, outside the lock.
type identityResolver struct {
	svc       *Service
	intents   *identityIntentStore
	network   domain.NetworkID
	clock     func() time.Time
	wake      chan struct{}
	sendSlots chan struct{}

	mu            sync.Mutex
	resolutions   map[domain.PeerIdentity]*identityResolution
	attempts      map[domain.PeerIdentity]identityAttemptEntry
	cooldownUntil map[domain.PeerIdentity]time.Time
	// attemptGen is the monotonic generation counter behind
	// identityAttemptEntry.gen and CurrentAttemptGen.
	attemptGen   uint64
	actionGen    identityLookupActionGeneration
	closed       bool
	ownerContext context.Context
	// lastStates retains the most recent published state per resolution id
	// — the lost-event insurance of §4.9: resolve_identity_status reads it.
	// Terminal states expire after identityLookupStateRetention; the map is
	// additionally bounded by identityLookupMaxRetainedStates.
	lastStates   map[string]ebus.IdentityResolutionState
	lastStatesAt map[string]time.Time

	bgAttemptsCap int
}

const (
	// identityLookupStateRetention keeps a TERMINAL state pollable after
	// the resolution is gone.
	identityLookupStateRetention = 60 * time.Second
	// identityLookupMaxRetainedStates bounds the retained-state map.
	identityLookupMaxRetainedStates = 256
)

// newIdentityResolver wires the engine; Run starts its loop.
func newIdentityResolver(svc *Service, intents *identityIntentStore, network domain.NetworkID) *identityResolver {
	cap := svc.cfg.IdentityLookupBGAttempts
	if cap <= 0 {
		cap = identityLookupDefaultBGAttempts
	}
	return &identityResolver{
		svc:           svc,
		intents:       intents,
		network:       network,
		clock:         func() time.Time { return time.Now().UTC() },
		wake:          make(chan struct{}, 1),
		sendSlots:     make(chan struct{}, identityLookupSendSlots),
		resolutions:   map[domain.PeerIdentity]*identityResolution{},
		attempts:      map[domain.PeerIdentity]identityAttemptEntry{},
		cooldownUntil: map[domain.PeerIdentity]time.Time{},
		lastStates:    map[string]ebus.IdentityResolutionState{},
		lastStatesAt:  map[string]time.Time{},
		bgAttemptsCap: cap,
	}
}

// newResolutionID mints the stable operation id.
func newResolutionID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

// emit retains the state for the status poll and publishes it. Called
// WITHOUT r.mu held — publication is a side effect and stays outside locks.
func (r *identityResolver) emit(state ebus.IdentityResolutionState, owner *identityResolution) {
	now := r.clock()
	r.mu.Lock()
	r.sweepStatesLocked(now)
	if !state.Lifecycle.Terminal() && (owner == nil || r.resolutions[state.Target] != owner) {
		r.mu.Unlock()
		return
	}
	if previous, ok := r.lastStates[state.ResolutionID]; ok && previous.Lifecycle.Terminal() {
		r.mu.Unlock()
		return
	}
	r.lastStates[state.ResolutionID] = state
	r.lastStatesAt[state.ResolutionID] = now
	r.mu.Unlock()
	ebus.PublishIdentityResolutionChanged(r.svc.eventBus, state)
}

// sweepStatesLocked expires terminal states and bounds the map. Caller
// holds r.mu.
func (r *identityResolver) sweepStatesLocked(now time.Time) {
	for id, state := range r.lastStates {
		if state.Lifecycle.Terminal() && now.Sub(r.lastStatesAt[id]) > identityLookupStateRetention {
			delete(r.lastStates, id)
			delete(r.lastStatesAt, id)
		}
	}
	if len(r.lastStates) < identityLookupMaxRetainedStates {
		return
	}
	// Over the bound with nothing expired: drop the oldest entries. Live
	// resolutions re-emit on their next transition, so a dropped row is a
	// poll miss, not a lost operation.
	for id := range r.lastStates {
		if len(r.lastStates) < identityLookupMaxRetainedStates {
			break
		}
		delete(r.lastStates, id)
		delete(r.lastStatesAt, id)
	}
}

// CurrentAttemptGen reads the monotonic attempt counter — the watermark a
// caller records BEFORE relying on a future completion: any attempt sent
// after this read carries a strictly greater generation.
func (r *identityResolver) CurrentAttemptGen() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attemptGen
}

// StateByID answers resolve_identity_status.
func (r *identityResolver) StateByID(id string) (ebus.IdentityResolutionState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.lastStates[id]
	return state, ok
}

// usableNow reads the contact plane: the keys are applicable right now
// when a box key for the target is known (any provisional source counts).
// Read OUTSIDE r.mu — it takes knowledgeMu inside the Service accessor.
func (r *identityResolver) usableNow(target domain.PeerIdentity) bool {
	_, ok := r.svc.knownBoxKey(target.String())
	return ok
}

// noteProvisionalImport reacts to an external key import for a running
// resolution: usable flips on, authority rises to provisional (never past
// authoritative), and the change is published. Not a terminal — §4.9: the
// operation continues digging for the authoritative record.
func (r *identityResolver) noteProvisionalImport(target domain.PeerIdentity) {
	usable := r.usableNow(target)
	r.mu.Lock()
	res, running := r.resolutions[target]
	if r.unavailableLocked() || !running || (res.usable == usable && res.authority != domain.IdentityAuthorityNone) {
		r.mu.Unlock()
		return
	}
	res.usable = usable
	if usable && res.authority == domain.IdentityAuthorityNone {
		res.authority = domain.IdentityAuthorityProvisional
	}
	state := res.stateLocked()
	r.mu.Unlock()
	r.emit(state, res)
}

// StartResolution registers a reason and opens (or joins) the single
// resolution of the target, returning its published state. A cooldown from
// a recent terminal delays the first ATTEMPT, never the operation itself —
// the RPC contract needs a resolution_id immediately.
func (r *identityResolver) StartResolution(target domain.PeerIdentity, reason identityIntentReason) (ebus.IdentityResolutionState, error) {
	state, _, err := r.startResolution(func() error { return nil }, target, reason, false)
	return state, err
}

func (r *identityResolver) startResolution(admissionError func() error, target domain.PeerIdentity, reason identityIntentReason, genuineUser bool) (ebus.IdentityResolutionState, identityLookupActionStamp, error) {
	if target.IsZero() || target.String() == r.svc.identity.Address {
		return ebus.IdentityResolutionState{}, identityLookupActionStamp{}, fmt.Errorf("identity lookup: invalid target")
	}
	now, usable := r.clock(), r.usableNow(target)
	r.mu.Lock()
	if r.unavailableLocked() {
		r.mu.Unlock()
		return ebus.IdentityResolutionState{}, identityLookupActionStamp{}, fmt.Errorf("identity lookup: resolver stopped")
	}
	if err := admissionError(); err != nil {
		r.mu.Unlock()
		return ebus.IdentityResolutionState{}, identityLookupActionStamp{}, err
	}
	existing, running := r.resolutions[target]
	var id string
	var err error
	if !running {
		id, err = newResolutionID()
		if err != nil {
			r.mu.Unlock()
			return ebus.IdentityResolutionState{}, identityLookupActionStamp{}, err
		}
	}
	action := identityLookupActionStamp{}
	if genuineUser {
		action = r.actionForLocked(target)
		if action.origin == identityLookupOriginNone {
			action, err = r.nextActionLocked()
			if err != nil {
				r.mu.Unlock()
				return ebus.IdentityResolutionState{}, action, err
			}
		}
	}
	snapshot, err := r.intents.addMemory(target, reason, now, action)
	if err != nil {
		r.mu.Unlock()
		return ebus.IdentityResolutionState{}, identityLookupActionStamp{}, err
	}
	if running {
		existing.usable = usable
	} else {
		firstAttemptAt := now
		if until, cooling := r.cooldownUntil[target]; cooling && until.After(firstAttemptAt) {
			firstAttemptAt = until
		}
		existing = &identityResolution{id: id, dst: target, createdAt: now, nextSendAt: firstAttemptAt, lifecycle: domain.IdentityResolutionPending, authority: domain.IdentityAuthorityNone, dmAvailable: domain.DMAvailabilityUnknown, usable: usable}
		if usable {
			existing.authority = domain.IdentityAuthorityProvisional
		}
		r.resolutions[target] = existing
		r.svc.pinRecordProtection(target)
	}
	state := existing.stateLocked()
	r.scheduleWakeLocked()
	r.mu.Unlock()
	r.persistIntent(snapshot, target)
	r.emit(state, existing)
	return state, action, nil
}

func (r *identityResolver) persistIntent(snapshot *identityIntentSnapshot, target domain.PeerIdentity) {
	if err := r.intents.persist(snapshot); err != nil {
		log.Warn().Err(err).Str("target", target.String()).Msg("identity_lookup_intent_persist_failed")
	}
}

// CancelReason is a synchronous current-reason command. Delayed work must use
// its exact attempt owner or an opaque expected-generation user action.
func (r *identityResolver) CancelReason(target domain.PeerIdentity, reason identityIntentReason) {
	r.cancelReasons(target, func(key identityIntentKey) bool { return key.reason == reason })
}
func (r *identityResolver) CancelReasonType(target domain.PeerIdentity, kind identityIntentReasonType) {
	r.cancelReasons(target, func(key identityIntentKey) bool { return key.reason.Type == kind })
}
func (r *identityResolver) cancelReasons(target domain.PeerIdentity, matches func(identityIntentKey) bool) {
	r.mu.Lock()
	var revoked identityLookupActionStamp
	remaining, snapshot, err := r.intents.removeMemory(target, func(key identityIntentKey, row identityIntentRow) bool {
		if !matches(key) {
			return false
		}
		if row.action.origin != identityLookupOriginNone {
			revoked = row.action
		}
		return true
	})
	if err != nil {
		r.mu.Unlock()
		log.Warn().Err(err).Str("target", target.String()).Msg("identity_lookup_intent_mutation_failed")
		return
	}
	if revoked.origin != identityLookupOriginNone {
		r.revokeActionLocked(target, revoked.generation)
	}
	var state ebus.IdentityResolutionState
	finished := false
	if res, ok := r.resolutions[target]; ok && remaining == 0 {
		state, finished, _ = r.finishLocked(res, domain.IdentityResolutionCancelled, nil)
	}
	r.mu.Unlock()
	if finished {
		r.emit(state, nil)
	}
	r.persistIntent(snapshot, target)
}

// run is the engine loop: a single goroutine, one timer, state under mu,
// sends outside it.
func (r *identityResolver) run(ctx context.Context) {
	defer r.stop()
	r.mu.Lock()
	r.ownerContext = ctx
	r.mu.Unlock()
	r.reseedFromIntents()
	r.svc.eventBus.Subscribe(ebus.TopicRouteTableChanged, func(ebus.RouteTableChange) {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	})
	// An external import (corsa: link, epidemic promotion into the trust
	// store) flips the usable axis mid-resolution: keys arrived through a
	// provisional source, sending unblocks, the operation keeps digging for
	// the authoritative record.
	r.svc.eventBus.Subscribe(ebus.TopicContactAdded, func(contact ebus.ContactAddedEvent) {
		r.noteProvisionalImport(contact.Address)
	})
	r.svc.eventBus.Subscribe(ebus.TopicIdentityAdded, func(identity domain.PeerIdentity) {
		r.noteProvisionalImport(identity)
	})

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		next := r.nextDeadline()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(time.Until(next))

		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-timer.C:
		}
		r.tick(ctx)
	}
}

// reseedFromIntents re-opens the background phase for every durable intent
// that survived the restart; rows past the task lifetime are dropped.
func (r *identityResolver) reseedFromIntents() {
	now := r.clock()
	for _, seed := range r.intents.seeds() {
		usable := r.usableNow(seed.Target)
		r.mu.Lock()
		if r.unavailableLocked() || r.resolutions[seed.Target] != nil {
			r.mu.Unlock()
			continue
		}
		// Re-read the CURRENT sole intent owner under resolver.mu: the seed
		// snapshot can have raced a fresh add/cancel before this lock.
		current, exists := r.intents.seedForTarget(seed.Target)
		if !exists {
			r.mu.Unlock()
			continue
		}
		if now.Sub(current.CreatedAt) > identityLookupTaskLifetime {
			_, snapshot, err := r.intents.removeMemory(seed.Target, func(identityIntentKey, identityIntentRow) bool { return true })
			r.mu.Unlock()
			if err != nil {
				log.Warn().Err(err).Msg("identity_lookup_reseed_mutation_failed")
			} else {
				r.persistIntent(snapshot, seed.Target)
			}
			continue
		}
		id, err := newResolutionID()
		if err != nil {
			r.mu.Unlock()
			log.Error().Err(err).Msg("identity_lookup_reseed_id_entropy_failed")
			continue
		}
		res := &identityResolution{id: id, dst: seed.Target, createdAt: current.CreatedAt, phase: identityPhaseBackground, bgAttempts: current.Attempts, nextSendAt: now, lifecycle: domain.IdentityResolutionActive, authority: domain.IdentityAuthorityNone, dmAvailable: domain.DMAvailabilityUnknown, usable: usable}
		if usable {
			res.authority = domain.IdentityAuthorityProvisional
		}
		r.resolutions[seed.Target] = res
		r.svc.pinRecordProtection(seed.Target)
		r.mu.Unlock()
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// nextDeadline finds the earliest time anything wants the loop.
func (r *identityResolver) nextDeadline() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.clock().Add(time.Hour)
	for _, res := range r.resolutions {
		if res.nextSendAt.Before(next) {
			next = res.nextSendAt
		}
	}
	for _, entry := range r.attempts {
		deadline := entry.sentAt.Add(identityLookupAttemptWindow + time.Nanosecond)
		if deadline.Before(next) {
			next = deadline
		}
	}
	return next
}

// tick advances every due resolution. Send work happens outside the lock:
// the loop collects due targets under mu, then processes them one by one.
func (r *identityResolver) tick(ctx context.Context) {
	now := r.clock()
	r.expireAttemptsLocked(now)

	r.mu.Lock()
	due := make([]*identityResolution, 0, len(r.resolutions))
	for _, res := range r.resolutions {
		if !now.Before(res.nextSendAt) {
			due = append(due, res)
		}
	}
	r.mu.Unlock()

	for _, res := range due {
		r.advanceOwnedResolution(ctx, res, r.clock())
	}
}

// expireAttemptsLocked sweeps attempt windows past their 60 seconds and
// cooldowns past their deadline.
func (r *identityResolver) expireAttemptsLocked(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for label, entry := range r.attempts {
		if now.Sub(entry.sentAt) <= identityLookupAttemptWindow {
			continue
		}
		delete(r.attempts, label)
		if res, ok := r.resolutions[entry.dst]; ok && res == entry.owner && entry.claim == nil && res.openAttempts > 0 {
			res.openAttempts--
		}
	}
	r.expireCooldownsLocked(now)
}

// openResolutionTargets lists the identities with a non-terminal
// resolution. Membership in r.resolutions IS that predicate: finishLocked
// deletes the entry as it records the terminal. Used by the trust store's
// live set, so an answer that is still being waited for cannot have its
// seq floor evicted out from under it.
func (r *identityResolver) openResolutionTargets() []domain.PeerIdentity {
	r.mu.Lock()
	defer r.mu.Unlock()
	targets := make([]domain.PeerIdentity, 0, len(r.resolutions))
	for target := range r.resolutions {
		targets = append(targets, target)
	}
	return targets
}

// cooldownCount reports the live cooldown map size for the resource
// breakdown.
func (r *identityResolver) cooldownCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cooldownUntil)
}

// expireCooldownsLocked drops cooldowns whose deadline has passed. A
// cooldown is consulted only by StartResolution inside its 30 s window;
// past it the entry is dead weight, and without this sweep the map kept
// one entry per target ever resolved for the life of the process. Runs
// on every tick and on every terminal, so the map is bounded by the
// number of targets that finished within the last cooldown window.
// Caller holds r.mu.
func (r *identityResolver) expireCooldownsLocked(now time.Time) {
	for target, until := range r.cooldownUntil {
		if !until.After(now) {
			delete(r.cooldownUntil, target)
		}
	}
}

// advanceResolution runs one scheduling step of one resolution: lifetime
// and phase transitions, the routing gate, then at most one send.
func (r *identityResolver) advanceOwnedResolution(ctx context.Context, owner *identityResolution, now time.Time) {
	target := owner.dst
	// Recompute the usable axis first: several provisional sources (the
	// fetch_contacts epidemic, sender keys attached to DM frames) fill the key maps
	// without any ebus event, so the tick is their only observer.
	usable := r.usableNow(target)
	r.mu.Lock()
	if res, ok := r.resolutions[target]; ok && res == owner && !r.unavailableLocked() && res.usable != usable {
		res.usable = usable
		if usable && res.authority == domain.IdentityAuthorityNone {
			res.authority = domain.IdentityAuthorityProvisional
		}
		state := res.stateLocked()
		r.mu.Unlock()
		r.emit(state, owner)
		r.mu.Lock()
	}
	res, ok := r.resolutions[target]
	if !ok || res != owner || r.unavailableLocked() || now.Before(res.nextSendAt) {
		r.mu.Unlock()
		return
	}

	if now.Sub(res.createdAt) > identityLookupTaskLifetime ||
		(res.phase == identityPhaseBackground && res.bgAttempts >= r.bgAttemptsCap) {
		// exhausted with usable=true is a valid outcome: keys exist, the
		// authoritative record was never obtained.
		state, finished, snapshot := r.finishLocked(owner, domain.IdentityResolutionExhausted, nil)
		r.mu.Unlock()
		r.persistIntent(snapshot, target)
		if finished {
			r.emit(state, owner)
		}
		return
	}
	if res.phase == identityPhaseInteractive && now.Sub(res.createdAt) >= identityLookupInteractiveDeadline {
		// A progress flag, not a terminal: the UI says "ask for a corsa:
		// link or wait", the background phase keeps working.
		res.interactiveTimeout = true
		res.phase = identityPhaseBackground
		res.nextSendAt = now.Add(identityLookupBackgroundDelays[0])
		state := res.stateLocked()
		r.mu.Unlock()
		log.Info().Str("target", target.String()).Msg("identity_lookup_interactive_timeout")
		r.emit(state, owner)
		return
	}
	if res.openAttempts >= identityLookupMaxOpenAttempts {
		res.nextSendAt = now.Add(identityLookupGateRetry)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	// The routing gate: a network attempt leaves only when the target is
	// reachable. A gated tick is not empty — it kicks the rate-limited
	// route query machinery, which today only starts on a failed forward
	// the gated lookup would never reach.
	if !r.canAttempt(target) {
		r.svc.SendRouteQuery(target)
		r.mu.Lock()
		res, ok := r.resolutions[target]
		if !ok || res != owner || r.unavailableLocked() {
			r.mu.Unlock()
			return
		}
		flipped := !res.noRoute
		res.noRoute = true
		res.nextSendAt = now.Add(identityLookupGateRetry)
		state := res.stateLocked()
		r.mu.Unlock()
		if flipped {
			r.emit(state, owner)
		}
		return
	}
	r.mu.Lock()
	res, ok = r.resolutions[target]
	if !ok || res != owner || r.unavailableLocked() {
		r.mu.Unlock()
		return
	}
	flipped := res.noRoute
	res.noRoute = false
	state := res.stateLocked()
	r.mu.Unlock()
	if flipped {
		r.emit(state, owner)
	}

	r.sendOwnedAttempt(ctx, owner, now)
}

// canAttempt reads the routing snapshot: the mesh-era resolver answer to
// "is a network attempt worth a slot". In the DHT era this becomes a
// lookup-structure question, which is why it is a method and not an inline
// condition.
func (r *identityResolver) canAttempt(target domain.PeerIdentity) bool {
	if r.svc.datagramLayer() == nil {
		return false
	}
	best := r.svc.loadRoutingSnapshot().BestRoute(target)
	return best != nil && best.Source != routing.RouteSourceLocal
}

// sendAttempt performs one send: fresh label, payload, frame, enqueue. The
// global send-slot cap is held only around the enqueue.
func (r *identityResolver) sendAttempt(ctx context.Context, target domain.PeerIdentity, now time.Time) {
	r.mu.Lock()
	owner := r.resolutions[target]
	r.mu.Unlock()
	if owner != nil {
		r.sendOwnedAttempt(ctx, owner, now)
	}
}
func (r *identityResolver) sendOwnedAttempt(ctx context.Context, owner *identityResolution, _ time.Time) {
	layer := r.svc.datagramLayer()
	if layer == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case r.sendSlots <- struct{}{}:
	}
	slotHeld := true
	defer func() {
		if slotHeld {
			<-r.sendSlots
		}
	}()
	if ctx.Err() != nil {
		return
	}
	var label domain.PeerIdentity
	if _, err := rand.Read(label[:]); err != nil {
		log.Error().Err(err).Msg("identity_lookup_attempt_id_entropy_failed")
		return
	}
	r.mu.Lock()
	if r.unavailableLocked() || r.resolutions[owner.dst] != owner || owner.openAttempts >= identityLookupMaxOpenAttempts {
		r.mu.Unlock()
		return
	}
	minSeq, firstSend, avoid := owner.minSeq, !owner.sentAnything, owner.prevNextHop
	r.mu.Unlock()
	payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: domain.IdentityLookupSchemaVersion, TargetProof: true, MinSeq: minSeq})
	if err != nil {
		log.Error().Err(err).Msg("identity_lookup_payload_build_failed")
		return
	}
	frame := protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRequest, Class: domain.DatagramClassControl, Src: label, Dst: owner.dst, TTL: domain.DatagramDefaultMaxHops, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypeGetIdentity, Payload: payload}
	avoidOpt := datagram.NoAvoidedNextHop()
	if !firstSend {
		frame.RoutePolicy = domain.RoutePolicyExplore
		if !avoid.IsZero() {
			avoidOpt = datagram.AvoidNextHop(avoid)
		}
	}
	r.mu.Lock()
	_, collision := r.attempts[label]
	if collision || r.unavailableLocked() || ctx.Err() != nil || r.resolutions[owner.dst] != owner || owner.openAttempts >= identityLookupMaxOpenAttempts || r.attemptGen == math.MaxUint64 {
		r.mu.Unlock()
		return
	}
	r.attemptGen++
	entry := identityAttemptEntry{owner: owner, dst: owner.dst, gen: r.attemptGen, qHash: sha256.Sum256(payload), proofRequired: true, minSeq: minSeq, sentAt: r.clock(), action: r.actionForLocked(owner.dst), queue: identityAttemptQueuePending}
	r.attempts[label] = entry
	owner.openAttempts++
	r.mu.Unlock()
	outcome := layer.pipeline.SendLocal(ctx, datagram.LocalSenderIdentityResolver, datagram.LocalSendOpts{Frame: frame, Avoid: avoidOpt})
	<-r.sendSlots
	slotHeld = false
	r.mu.Lock()
	if ctx.Err() != nil {
		if current, ok := r.attempts[label]; ok && current.gen == entry.gen && current.owner == owner {
			current.revoked = true
			r.attempts[label] = current
		}
	}
	completion, state, snapshot, emit, mutationErr := r.settleQueueLocked(label, entry, outcome)
	r.mu.Unlock()
	if mutationErr != nil {
		log.Warn().Err(mutationErr).Str("target", owner.dst.String()).Msg("identity_lookup_intent_mutation_failed")
	}
	r.persistIntent(snapshot, owner.dst)
	observeIdentityLookupCompletion(completion)
	if emit {
		r.emit(state, owner)
	}
}

// settleQueueLocked never consults a replacement by target to mutate it.
// Caller holds resolver.mu. Snapshot I/O and emission follow its release.
func (r *identityResolver) settleQueueLocked(label domain.PeerIdentity, expected identityAttemptEntry, outcome datagram.SendOutcome) (*identityLookupCompletion, ebus.IdentityResolutionState, *identityIntentSnapshot, bool, error) {
	entry, ok := r.attempts[label]
	if !ok || entry.gen != expected.gen || entry.owner != expected.owner || r.unavailableLocked() {
		return nil, ebus.IdentityResolutionState{}, nil, false, nil
	}
	entry.queue = identityAttemptQueueRefused
	if outcome.Kind() == datagram.SendQueued {
		entry.queue = identityAttemptQueueAccepted
	}
	r.attempts[label] = entry
	if entry.claim != nil {
		return r.settleClaimLocked(label), ebus.IdentityResolutionState{}, nil, false, nil
	}
	owner := entry.owner
	if r.resolutions[entry.dst] != owner {
		delete(r.attempts, label)
		return nil, ebus.IdentityResolutionState{}, nil, false, nil
	}
	var snapshot *identityIntentSnapshot
	var mutationErr error
	emit := false
	if outcome.Kind() == datagram.SendQueued {
		owner.sentAnything = true
		if owner.lifecycle == domain.IdentityResolutionPending {
			owner.lifecycle = domain.IdentityResolutionActive
			emit = true
		}
		if hop, ok := outcome.NextHop(); ok {
			owner.prevNextHop = hop
		}
		r.scheduleNextSendLocked(owner, r.clock())
	} else {
		delete(r.attempts, label)
		if owner.openAttempts > 0 {
			owner.openAttempts--
		}
		if outcome.Kind() == datagram.SendNoRoute {
			owner.noRoute = true
			owner.nextSendAt = r.clock().Add(identityLookupGateRetry)
		} else {
			r.scheduleNextSendLocked(owner, r.clock())
		}
	}
	if outcome.Kind() != datagram.SendNoRoute && owner.phase == identityPhaseBackground {
		snapshot, mutationErr = r.intents.recordAttemptMemory(owner.dst)
		owner.bgAttempts++
	}
	return nil, owner.stateLocked(), snapshot, emit, mutationErr
}

// scheduleNextSendLocked advances the phase schedule after a consumed
// attempt slot. Caller holds r.mu.
func (r *identityResolver) scheduleNextSendLocked(res *identityResolution, now time.Time) {
	if res.phase == identityPhaseInteractive {
		res.interactiveIndex++
		if res.interactiveIndex < len(identityLookupInteractiveAt) {
			res.nextSendAt = res.createdAt.Add(identityLookupInteractiveAt[res.interactiveIndex])
			if res.nextSendAt.Before(now) {
				res.nextSendAt = now
			}
			return
		}
		// All interactive sends are out; the next event is the phase
		// deadline at t=77s.
		res.nextSendAt = res.createdAt.Add(identityLookupInteractiveDeadline)
		return
	}
	step := res.bgAttempts
	if step >= len(identityLookupBackgroundDelays) {
		step = len(identityLookupBackgroundDelays) - 1
	}
	res.nextSendAt = now.Add(identityLookupBackgroundDelays[step])
}

// HandleAnswer is the post_identity ingest: correlate by label, verify,
// merge, decide the terminal. Returns whether the answer was consumed (for
// metrics; an unknown label is a silent drop either way).
func (r *identityResolver) HandleAnswer(label datagram.Label, payload []byte) bool {
	now := r.clock()
	r.mu.Lock()
	entry, ok := r.attempts[label.Raw()]
	if !ok || r.unavailableLocked() || entry.claim != nil || entry.owner == nil || r.resolutions[entry.dst] != entry.owner || !entry.liveAt(now) {
		r.mu.Unlock()
		log.Debug().Str("label", label.String()).Msg("identity_lookup_answer_without_live_attempt")
		return false
	}
	r.mu.Unlock()

	parsed, err := protocol.ParsePostIdentityPayload(payload)
	if err != nil {
		log.Debug().Err(err).Str("target", entry.dst.String()).Msg("identity_lookup_answer_malformed")
		return false
	}
	body, err := protocol.VerifyIdentityRecord(parsed.Record, r.network, entry.dst)
	if err != nil {
		log.Debug().Err(err).Str("target", entry.dst.String()).Msg("identity_lookup_answer_record_invalid")
		return false
	}
	if entry.proofRequired {
		if len(parsed.TargetProof) == 0 {
			log.Debug().Str("target", entry.dst.String()).Msg("identity_lookup_answer_proof_missing")
			return false
		}
		if err := protocol.VerifyTargetProof(parsed.TargetProof, body, r.network, label.Raw(), entry.qHash, parsed.Record); err != nil {
			// No ban anywhere on this path: the neighbour may be an honest
			// transit of somebody else's garbage.
			log.Debug().Err(err).Str("target", entry.dst.String()).Msg("identity_lookup_answer_proof_invalid")
			return false
		}
		// A verified proof is a verified proof, whoever asked for it: the
		// target signed this attempt's label a moment ago. A resolution of a
		// contact therefore feeds presence for free, and the prober's demand
		// mode skips the probe it would otherwise have sent.
		r.svc.notePresenceProof(entry.dst)
	}
	if body.Seq < entry.minSeq {
		// requirement-unsatisfied: the resolution stays active and keeps
		// digging for a fresh enough record.
		log.Debug().
			Str("target", entry.dst.String()).
			Uint64("seq", uint64(body.Seq)).
			Uint64("min_seq", uint64(entry.minSeq)).
			Msg("identity_lookup_answer_below_min_seq")
		return false
	}

	outcome, err := r.svc.importVerifiedIdentityRecord(r.network, parsed.Record, body)
	if err != nil {
		log.Warn().Err(err).Str("target", entry.dst.String()).Msg("identity_lookup_import_failed")
		return false
	}

	switch outcome {
	case domain.IdentityRecordMergeInserted, domain.IdentityRecordMergeReplaced, domain.IdentityRecordMergeDuplicate:
		// identical-current (duplicate) is a success: the store already
		// holds this very record.
		r.mu.Lock()
		current, live := r.attempts[label.Raw()]
		if !live || current.gen != entry.gen || current.owner != entry.owner || current.claim != nil || r.unavailableLocked() || r.resolutions[entry.dst] != entry.owner || !current.liveAt(r.clock()) {
			r.mu.Unlock()
			return true
		}
		res := entry.owner
		// Normal successful key resolution is immediate. A pending exact
		// claimed queue result alone may outlive the resolution in this row.
		var preserve *domain.PeerIdentity
		action := r.actionForLocked(entry.dst)
		if !current.revoked && current.action.origin == identityLookupOriginWindowUser && current.action == action && recordRequiresSessionV2(body.MinSession) {
			current.claim = &identityLookupCompletion{target: entry.dst, resolutionID: res.id, label: label.Raw(), attemptGeneration: entry.gen, action: current.action, sequence: body.Seq, minSession: body.MinSession, recordDigest: sha256.Sum256([]byte(parsed.Record.Body))}
			r.attempts[label.Raw()] = current
			rawLabel := label.Raw()
			preserve = &rawLabel
		}
		_, finished, snapshot := r.finishLocked(res, domain.IdentityResolutionSucceeded, preserve)
		if !finished {
			current.claim = nil
			r.attempts[label.Raw()] = current
			r.mu.Unlock()
			return true
		}
		res.authority = domain.IdentityAuthorityAuthoritative
		res.dmAvailable = domain.DMAvailabilityNo
		if body.DM {
			res.dmAvailable = domain.DMAvailabilityYes
			res.usable = true
		}
		res.answerAttemptGen = entry.gen
		state := res.stateLocked()
		completion := r.settleClaimLocked(label.Raw())
		r.mu.Unlock()
		if finished {
			r.emit(state, res)
		}
		observeIdentityLookupCompletion(completion)
		r.persistIntent(snapshot, entry.dst)
		log.Info().
			Str("target", entry.dst.String()).
			Uint64("seq", uint64(body.Seq)).
			Str("merge", outcome.String()).
			Msg("identity_lookup_succeeded")
		return true
	default:
		// stale / conflict keep the resolution active: the answer was
		// valid but not the record the operation exists to fetch.
		log.Info().
			Str("target", entry.dst.String()).
			Str("merge", outcome.String()).
			Msg("identity_lookup_answer_not_terminal")
		return true
	}
}

// finishLocked removes the resolution, its live attempts and its durable
// intents, arms the cooldown and returns the terminal state for the caller
// to emit AFTER releasing r.mu. Caller holds r.mu.
func (r *identityResolver) finishLocked(owner *identityResolution, terminal domain.IdentityResolutionLifecycle, preserve *domain.PeerIdentity) (ebus.IdentityResolutionState, bool, *identityIntentSnapshot) {
	if owner == nil || r.resolutions[owner.dst] != owner {
		return ebus.IdentityResolutionState{}, false, nil
	}
	r.intents.mu.Lock()
	hasRows := r.intents.countLocked(owner.dst) > 0
	r.intents.mu.Unlock()
	var snapshot *identityIntentSnapshot
	if hasRows {
		var err error
		_, snapshot, err = r.intents.removeMemory(owner.dst, func(identityIntentKey, identityIntentRow) bool { return true })
		if err != nil {
			return ebus.IdentityResolutionState{}, false, nil
		}
	}
	owner.lifecycle = terminal
	state := owner.stateLocked()
	delete(r.resolutions, owner.dst)
	r.svc.releaseRecordProtection(owner.dst)
	for label, entry := range r.attempts {
		if entry.owner != owner {
			continue
		}
		if terminal == domain.IdentityResolutionSucceeded && preserve != nil && label == *preserve && entry.claim != nil {
			continue
		}
		delete(r.attempts, label)
	}
	now := r.clock()
	r.expireCooldownsLocked(now)
	r.cooldownUntil[owner.dst] = now.Add(identityLookupCooldown)
	return state, true, snapshot
}

// scheduleWakeLocked nudges the loop; caller holds r.mu.
func (r *identityResolver) scheduleWakeLocked() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// notifyIdentityKeysImported is the synchronous post-key-import signal to
// the resolver: called at every site that writes key material into the
// knowledge maps, AFTER the write completed. The ebus subscriptions
// (ContactAdded / IdentityAdded) cannot carry this contract alone — the
// events are published before the box key lands and are asynchronous, and
// an already-known identity produces no event at all, so a running
// resolution could sit unusable until its next scheduled tick.
func (s *Service) notifyIdentityKeysImported(address string) {
	resolver := s.identityResolver
	if resolver == nil {
		return
	}
	target, err := domain.ParsePeerIdentity(address)
	if err != nil || target.IsZero() {
		return
	}
	resolver.noteProvisionalImport(target)
}

// importVerifiedIdentityRecord is the single verify-then-import sink for
// records that already passed VerifyIdentityRecord: the trust store keeps
// the durable record (seq-gated), and the knowledge maps receive the key
// material so the DM path can encrypt without any further lookup.
func (s *Service) importVerifiedIdentityRecord(network domain.NetworkID, record protocol.SignedIdentityRecord, body protocol.IdentityRecordBody) (domain.IdentityRecordMergeOutcome, error) {
	outcome, err := s.trust.rememberRecord(network, record, body)
	if err != nil {
		return outcome, fmt.Errorf("remember identity record: %w", err)
	}
	// Duplicate (identical-current) still refills the knowledge maps: the
	// stored record proves the keys, but the bounded LRU may have evicted
	// them — a lookup that ends in "already have it" must leave the DM path
	// able to encrypt.
	if !outcome.Accepted() && outcome != domain.IdentityRecordMergeDuplicate {
		return outcome, nil
	}
	address := body.Address.String()
	s.addKnownPubKey(address, string(body.PubKey))
	if body.DM {
		s.addKnownBoxKey(address, string(body.BoxKey))
		s.addKnownBoxSig(address, string(body.BoxSig))
	} else {
		// An authoritative dm:false is a REVOCATION, not an absence: the
		// owner's own record says it accepts no direct messages, so the
		// previously known box key must leave the live maps too — keeping
		// it would let the direct-send and fetch_contacts paths encrypt to
		// a revoked key against the opt-out.
		s.forgetKnownBoxKey(address)
	}
	s.notifyIdentityKeysImported(address)
	return outcome, nil
}
