package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// secure_session_store.go is what keeps a peer that proved v2 from being taken
// back to the relayable v1 proof — across restarts, not only for the life of
// the process (docs/protocol/session_v2.md, "Downgrade protection").
//
//   - An IDENTITY that proved itself over v2 is pinned for good: a v1
//     session naming it is refused in either direction — a v1 hello on
//     accept, a v1 welcome on dial.
//   - An ENDPOINT this node dialled and reached over v2 is bound: a v1 answer
//     from it is a downgrade, not an old node, and the dial fails. Unlike the
//     identity pin, the binding also covers somebody ELSE answering on that
//     address with an unpinned identity. A binding is ordinary and lasts 30
//     days from the last v2 session on it — an address may change hands,
//     so a binding that only records that it once answered over v2 is not
//     kept for good — unless the dial asked to protect it: a protected
//     binding guards a contact or a relation of this node, so it never
//     expires and is never lowered or removed automatically. Only a dial
//     with the intent "to X" that proved X may ask, and no dial path
//     carries such an intent yet (addressDialProtection); protected
//     bindings from earlier builds are kept as they are.
//
// The store never drops live protection to make room. At a bound a new
// protection is refused with its reason (protected bindings in pools K and
// R, 8 192 of the 32 768 places for K only, at most 4 per identity — see
// maxProtectedBindings), and what is already there stays; the endpoint may
// get an ordinary binding instead. A NEW identity at the pin bound fails its
// session with errPinStoreFull: a v2 session whose mandatory pin is not
// stored is not established, and the dialler does not fall back to v1 in
// that attempt.
//
// A change that could not be written is reported to the caller — a session
// whose protection is not on disk is not established — and stays pending:
// the next note writes it even when it changes nothing itself.
//
// Persistence is one JSON file written through secretfile (unique temp,
// owner-only at creation, fsync, rename), next to the node's peers file.

const (
	secureSessionStoreFormat = 1
	// maxPinnedIdentities bounds the pins. Every v2 handshake can pin, and a
	// fresh identity costs an attacker nothing, so the bound is a refusal of
	// NEW pins, never an eviction of old ones.
	maxPinnedIdentities = 20000
	// maxGeneralBindings bounds the ordinary endpoint bindings, apart from
	// the protected ones: a full ordinary tier never takes a protected place
	// and the other way round.
	maxGeneralBindings = 16384
	// maxProtectedBindings, protectedReserveContact and
	// maxProtectedPerIdentity bound the protected bindings
	// (docs/protocol/session_v2.md, "Bounds of the endpoint bindings").
	// Protected bindings never expire and are never lowered automatically,
	// so without a bound the tier would only grow; the per-identity cap
	// keeps one identity from filling the tier with its own endpoints.
	// The reserve is for the contact pool alone: a relation never takes it,
	// not even for a while — with nothing ever evicted, a reserve place a
	// relation took would never come back.
	maxProtectedBindings    = 32768
	protectedReserveContact = 8192
	maxProtectedPerIdentity = 4
	// endpointBindingTTL is how long an ordinary binding outlives the last
	// v2 session on its endpoint. It expires, unlike a protected one,
	// because an address may change hands and the binding only records
	// that it once answered over v2.
	endpointBindingTTL = 30 * 24 * time.Hour
	// renewalFlushEvery batches the writes of renewals for the whole store:
	// a renewal of an ordinary binding is made in memory at once and only
	// marks the store as holding unwritten renewals; the periodic flush
	// (flushRenewals, on the bootstrap tick) writes them at most this often.
	// A crash loses at most this much of a binding's 30 days, plus one tick.
	renewalFlushEvery = 60 * time.Second
)

// errLegacyRefusedPinned is a v1 session naming an identity that proved v2.
var errLegacyRefusedPinned = errors.New("secure session: v1 refused for an identity that proved v2")

// errPinStoreFull is a v2 session of an identity not pinned yet, refused
// because the store is at its bound and the mandatory pin cannot be stored.
// Its text is the diagnostic code operators search for.
var errPinStoreFull = errors.New("pin_store_full")

// endpointProtectionKind is what a dial asks of the binding it earns.
type endpointProtectionKind int

const (
	// protectionNotAsked: an ordinary binding.
	protectionNotAsked endpointProtectionKind = iota + 1
	// protectionWanted: protected if a place is free, ordinary otherwise.
	protectionWanted
	// protectionRequired: the operation needs the endpoint protected; a
	// refusal fails it instead of passing an ordinary binding off as one.
	protectionRequired
)

// endpointProtection is the protection a dial asks for, and the pool it
// would count against.
type endpointProtection struct {
	kind endpointProtectionKind
	pool domain.ProtectedBindingPool
}

func endpointProtectionNone() endpointProtection {
	return endpointProtection{kind: protectionNotAsked}
}

// endpointProtectionWanted is for a dial with the intent "to X" that would
// like X's endpoint protected but stands without it. No production path
// asks for it today: every dial is an address dial (addressDialProtection)
// and creates only ordinary bindings, so only tests use it. It stays because
// the protected tier is a store feature the dial paths do not reach yet: the
// tests pin its pools, bounds and refusals now, and the first dial "to X"
// only has to ask for it.
func endpointProtectionWanted(pool domain.ProtectedBindingPool) endpointProtection {
	return endpointProtection{kind: protectionWanted, pool: pool}
}

// endpointProtectionRequired is for an operation that is incomplete without
// the protection. No production path makes such an operation today — every
// dial is an address dial (addressDialProtection) — so only tests use it; the
// first dial with the intent "to X" that needs X's endpoint protected will.
func endpointProtectionRequired(pool domain.ProtectedBindingPool) endpointProtection {
	return endpointProtection{kind: protectionRequired, pool: pool}
}

func (p endpointProtection) asked() bool {
	return p.kind == protectionWanted || p.kind == protectionRequired
}

// endpointBindingTier is the binding an endpoint has after a note.
type endpointBindingTier int

const (
	bindingTierNone endpointBindingTier = iota + 1
	bindingTierGeneral
	bindingTierProtected
)

func (t endpointBindingTier) String() string {
	switch t {
	case bindingTierGeneral:
		return "general"
	case bindingTierProtected:
		return "protected"
	default:
		return "none"
	}
}

// endpointBindingResult is what a note did to the endpoint: its tier now,
// and the refusal when protection was asked for and not given. The three
// outcomes a caller must tell apart — protected; protection refused and an
// ordinary binding recorded; no binding — are all here, and a refused
// protection is never reported as protected.
type endpointBindingResult struct {
	tier    endpointBindingTier
	refused *domain.ProtectedBindingRefused
	// limitRefusalReported: refused is a bound that this note counted and
	// must log — the first refusal of this binding's protection.
	limitRefusalReported bool
}

// bindingProtection is what makes a binding protected: the identity whose
// standing gave the protection and the pool it counts against, both fixed
// when the protection was given. A new owner of the endpoint inherits the
// protection but not the place it takes: that stays counted for by.
type bindingProtection struct {
	by   domain.PeerIdentity
	pool domain.ProtectedBindingPool
}

type endpointBinding struct {
	// identity is the last identity proven on the endpoint over v2.
	identity domain.PeerIdentity
	lastV2   time.Time
	// protection is nil for an ordinary binding.
	protection *bindingProtection
	// limitRefusalCounted: a protection of this binding was refused at a
	// bound and counted, so renewals that meet the same bound are not
	// counted or logged again. In memory only: after a restart the first
	// refusal is counted once more.
	limitRefusalCounted bool
}

// live reports whether the binding still requires v2 at now.
func (b endpointBinding) live(now time.Time) bool {
	return b.protection != nil || now.Before(b.lastV2.Add(endpointBindingTTL))
}

func (b endpointBinding) tier() endpointBindingTier {
	if b.protection != nil {
		return bindingTierProtected
	}
	return bindingTierGeneral
}

// protectedPoolRule is how far a pool may fill the protected tier and how a
// refusal at that point is named.
type protectedPoolRule struct {
	ceiling int
	full    domain.ProtectedBindingRefusalReason
}

func protectedPoolRuleFor(pool domain.ProtectedBindingPool) protectedPoolRule {
	if pool == domain.ProtectedBindingPoolContact {
		return protectedPoolRule{ceiling: maxProtectedBindings, full: domain.ProtectedBindingRefusedStoreFull}
	}
	return protectedPoolRule{ceiling: maxProtectedBindings - protectedReserveContact, full: domain.ProtectedBindingRefusedPoolRFull}
}

// secureSessionStore is the downgrade-protection state. mu guards the maps
// and is a leaf: no I/O and no call out under it. writeMu serialises disk
// writes and is held WITHOUT mu, so a slow disk never blocks a lookup.
type secureSessionStore struct {
	dir    string // "" — memory only (a Service built without a peers path)
	name   string
	clock  func() time.Time
	logger zerolog.Logger

	mu         sync.Mutex
	identities map[domain.PeerIdentity]time.Time
	endpoints  map[domain.PeerAddress]endpointBinding
	// generalBindings, protectedByPool and protectedByGiver count endpoints
	// by tier, by pool and by protected_by, so a bound is checked without a
	// scan. Kept by insertBindingLocked, removeBindingLocked and
	// protectBindingLocked — the only writers of endpoints.
	generalBindings  int
	protectedByPool  map[domain.ProtectedBindingPool]int
	protectedByGiver map[domain.PeerIdentity]int
	// unreadable: the file exists but could not be read. Fail-closed:
	// every identity and every endpoint is treated as protected, so v1 is
	// refused while v2 keeps working, until an operator repairs the file.
	unreadable bool
	// changes counts every change, written the count the last successful
	// write covered: a gap is protection that is in memory only.
	changes, written uint64
	// renewalsUnwritten: an ordinary binding was renewed in memory since the
	// last snapshot. lastFlush is when a flush last wrote; nil until the
	// first one, which is therefore due at once.
	renewalsUnwritten bool
	lastFlush         *renewalFlush
	// pinRefusals counts sessions refused with errPinStoreFull.
	pinRefusals uint64
	// protectedRefusals counts protected bindings refused at a bound.
	protectedRefusals map[domain.ProtectedBindingRefusalReason]uint64
	// v1RefusalsIdentity counts v1 sessions refused because the identity they
	// name requires v2 (errLegacyRefusedPinned), on accept and on dial.
	v1RefusalsIdentity uint64
	// v1RefusalsEndpoint counts v1 answers refused because the endpoint that
	// gave them is bound to v2 (errLegacyRefusedV2Bound).
	v1RefusalsEndpoint uint64
	// v1DialsHeld counts v1 dials withheld by the hold that follows a
	// refused v1 answer from the same address (errLegacyRefusedBackoff).
	v1DialsHeld uint64
	// dialsHeldV2Retry counts dials refused before any socket by an
	// address's v2 retry backoff (errV2RetryHeld).
	dialsHeldV2Retry uint64

	writeMu sync.Mutex
	// writeFile puts the encoded store on disk; writeToDisk outside tests.
	writeFile func(raw []byte) error
}

// secureSessionStorePath is where the store lives: the configured path, or
// secure-sessions-<port>.json next to the peers file.
func secureSessionStorePath(cfg config.Node) string {
	if cfg.SecureSessionStorePath != "" {
		return cfg.SecureSessionStorePath
	}
	if cfg.PeersStatePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(cfg.PeersStatePath), "secure-sessions-"+config.PortSuffix(cfg.ListenAddress)+".json")
}

// loadSecureSessionStore reads the store before the node accepts or dials.
func loadSecureSessionStore(path string, clock func() time.Time) *secureSessionStore {
	store := &secureSessionStore{
		clock:             clock,
		logger:            log.Logger,
		identities:        make(map[domain.PeerIdentity]time.Time),
		endpoints:         make(map[domain.PeerAddress]endpointBinding),
		protectedByPool:   make(map[domain.ProtectedBindingPool]int),
		protectedByGiver:  make(map[domain.PeerIdentity]int),
		protectedRefusals: make(map[domain.ProtectedBindingRefusalReason]uint64),
	}
	if path == "" {
		// Memory only: nothing to write, so no snapshot is ever taken.
		return store
	}
	store.writeFile = store.writeToDisk
	store.dir, store.name = filepath.Dir(path), filepath.Base(path)
	if err := store.load(); err != nil {
		store.unreadable = true
		store.logger.Error().Err(err).Str("store", store.name).Msg("secure_session_store_unreadable_v1_refused_for_every_peer")
	}
	return store
}

// now is the store's clock in UTC, so the file does not depend on the zone
// the node runs in.
func (s *secureSessionStore) now() time.Time { return s.clock().UTC() }

type secureSessionFile struct {
	Format     int                                          `json:"format"`
	Identities map[domain.PeerIdentity]time.Time            `json:"identities"`
	Endpoints  map[domain.PeerAddress]endpointBindingRecord `json:"endpoints"`
}

// endpointBindingRecord is a binding as the file holds it. ProtectedBy and
// Pool are present exactly when Protected is true. A protected record without
// both was written before pools existed, when only a contact's binding was
// protected: it is read as given by its identity, in pool K.
type endpointBindingRecord struct {
	Identity    domain.PeerIdentity          `json:"identity"`
	LastV2      time.Time                    `json:"last_v2"`
	Protected   bool                         `json:"protected,omitempty"`
	ProtectedBy *domain.PeerIdentity         `json:"protected_by,omitempty"`
	Pool        *domain.ProtectedBindingPool `json:"pool,omitempty"`
}

func recordOf(binding endpointBinding) endpointBindingRecord {
	record := endpointBindingRecord{Identity: binding.identity, LastV2: binding.lastV2}
	if binding.protection != nil {
		by, pool := binding.protection.by, binding.protection.pool
		record.Protected, record.ProtectedBy, record.Pool = true, &by, &pool
	}
	return record
}

var (
	errIncompleteBindingRecord    = errors.New("decode: an incomplete endpoint binding")
	errContradictoryBindingRecord = errors.New("decode: a protected binding without its giver or pool, or an ordinary one with them")
	errZeroPinnedIdentity         = errors.New("decode: a zero identity")
)

// bindingFromRecord checks a record and turns it into a binding.
func bindingFromRecord(address domain.PeerAddress, record endpointBindingRecord) (endpointBinding, error) {
	if record.Identity.IsZero() || strings.TrimSpace(string(address)) == "" {
		return endpointBinding{}, errIncompleteBindingRecord
	}
	binding := endpointBinding{identity: record.Identity, lastV2: record.LastV2}
	protection, err := protectionOf(record)
	if err != nil {
		return endpointBinding{}, err
	}
	binding.protection = protection
	return binding, nil
}

// protectionOf reads the protection of a record; nil for an ordinary one.
func protectionOf(record endpointBindingRecord) (*bindingProtection, error) {
	switch {
	case !record.Protected && record.ProtectedBy == nil && record.Pool == nil:
		return nil, nil
	case record.Protected && record.ProtectedBy == nil && record.Pool == nil:
		return &bindingProtection{by: record.Identity, pool: domain.ProtectedBindingPoolContact}, nil
	case !record.Protected || record.ProtectedBy == nil || record.Pool == nil:
		return nil, errContradictoryBindingRecord
	case record.ProtectedBy.IsZero() || !record.Pool.Valid():
		return nil, errIncompleteBindingRecord
	}
	return &bindingProtection{by: *record.ProtectedBy, pool: *record.Pool}, nil
}

func (s *secureSessionStore) load() error {
	dir, err := secretfile.Open(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	// A crash between a temp and its rename leaves the temp behind, up to
	// the size of the whole store; only old ones go, since the directory is
	// shared with nodes on other ports that may be writing right now.
	sweepStaleSecretTemps(dir)
	raw, err := dir.ReadFile(s.name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read: %w", secretfile.StripPath(err))
	}
	var file secureSessionFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if file.Format != secureSessionStoreFormat {
		return fmt.Errorf("unknown format %d", file.Format)
	}
	now := s.now()
	// Nothing else holds the store yet; mu is taken for the counters'
	// writers, whose contract it is.
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, at := range file.Identities {
		if id.IsZero() {
			return errZeroPinnedIdentity
		}
		s.identities[id] = at
	}
	for address, record := range file.Endpoints {
		binding, err := bindingFromRecord(address, record)
		if err != nil {
			return err
		}
		// A last_v2 in the future (clock set back since) is clamped to now,
		// or a clock change would stretch the binding past its 30 days.
		if binding.lastV2.After(now) {
			binding.lastV2 = now
		}
		s.insertBindingLocked(address, binding)
	}
	return nil
}

// stats is the store's state for diagnostics. Takes mu, a leaf held for
// map reads only.
func (s *secureSessionStore) stats() domain.SecureSessionStoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return domain.SecureSessionStoreStats{
		ReadAt:                       s.clock(),
		PinnedIdentities:             len(s.identities),
		PinCapacity:                  maxPinnedIdentities,
		Full:                         len(s.identities) >= maxPinnedIdentities,
		PinRefusalsStoreFull:         s.pinRefusals,
		V1RefusalsIdentityPinned:     s.v1RefusalsIdentity,
		V1RefusalsEndpointBound:      s.v1RefusalsEndpoint,
		V1DialsHeldByRefusalBackoff:  s.v1DialsHeld,
		DialsHeldByV2RetryBackoff:    s.dialsHeldV2Retry,
		ProtectedBindingsContact:     s.protectedByPool[domain.ProtectedBindingPoolContact],
		ProtectedBindingsRelation:    s.protectedByPool[domain.ProtectedBindingPoolRelation],
		ProtectedCapacity:            maxProtectedBindings,
		ProtectedReserveContact:      protectedReserveContact,
		ProtectedPerIdentityCapacity: maxProtectedPerIdentity,
		GeneralBindings:              s.generalBindings,
		GeneralCapacity:              maxGeneralBindings,
		ProtectedRefusalsPerIdentity: s.protectedRefusals[domain.ProtectedBindingRefusedPerIdentity],
		ProtectedRefusalsPoolRFull:   s.protectedRefusals[domain.ProtectedBindingRefusedPoolRFull],
		ProtectedRefusalsStoreFull:   s.protectedRefusals[domain.ProtectedBindingRefusedStoreFull],
		Unreadable:                   s.unreadable,
	}
}

// noteIdentityRefusal counts one v1 session refused because the identity it
// names requires v2.
func (s *secureSessionStore) noteIdentityRefusal() {
	s.mu.Lock()
	s.v1RefusalsIdentity++
	s.mu.Unlock()
}

// noteEndpointRefusal counts one v1 answer refused because the endpoint that
// gave it is bound to v2.
func (s *secureSessionStore) noteEndpointRefusal() {
	s.mu.Lock()
	s.v1RefusalsEndpoint++
	s.mu.Unlock()
}

// noteLegacyDialHeld counts one v1 dial withheld by the hold that follows a
// refused v1 answer from the same address.
func (s *secureSessionStore) noteLegacyDialHeld() {
	s.mu.Lock()
	s.v1DialsHeld++
	s.mu.Unlock()
}

// noteDialHeldByV2Retry counts one dial refused before any socket by its
// address's v2 retry backoff.
func (s *secureSessionStore) noteDialHeldByV2Retry() {
	s.mu.Lock()
	s.dialsHeldV2Retry++
	s.mu.Unlock()
}

// identityPinned reports whether a v1 session naming id must be refused.
func (s *secureSessionStore) identityPinned(id domain.PeerIdentity) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return true
	}
	_, pinned := s.identities[id]
	return pinned
}

// endpointRequiresV2 reports whether a v1 answer from address is a downgrade.
func (s *secureSessionStore) endpointRequiresV2(address domain.PeerAddress) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return true
	}
	binding, bound := s.endpoints[address]
	return bound && binding.live(s.now())
}

// noteProvenInbound pins a peer proven on an accepted connection. An error
// means the protection is not stored — errPinStoreFull for a new identity at
// the bound, or a write failure — and the caller must not establish the
// session on it.
func (s *secureSessionStore) noteProvenInbound(id domain.PeerIdentity) error {
	var refused error
	pinned := 0
	err := s.update(func(now time.Time) bool {
		added, pinErr := s.pinLocked(id, now)
		refused, pinned = pinErr, len(s.identities)
		return added
	})
	if refused != nil {
		s.logPinRefusal(id, pinned)
		return refused
	}
	return err
}

// noteProvenOutbound pins the peer and binds the endpoint this node dialled,
// with the protection the dial asks for. The result says what the endpoint
// has now. An error means the session must not be established on it: its
// protection is not stored (errPinStoreFull, a write failure), or the dial
// required the endpoint protected and the protection was refused
// (*domain.ProtectedBindingRefused). A session refused with errPinStoreFull
// leaves no endpoint binding: it was not established, so nothing it earned is
// recorded. A required protection that is refused still leaves the ordinary
// binding the v2 session earned on the endpoint — the binding records a
// completed mutual v2 handshake on an endpoint this node dialled, which is
// what creates one, and dropping it would only weaken the endpoint; it is the
// operation that needed the protection that does not complete.
func (s *secureSessionStore) noteProvenOutbound(address domain.PeerAddress, id domain.PeerIdentity, protection endpointProtection) (endpointBindingResult, error) {
	if protection.asked() && !protection.pool.Valid() {
		s.logger.Error().Str("peer", string(address)).Str("pool", string(protection.pool)).Msg("secure_session_protection_with_unknown_pool_ignored")
		protection = endpointProtectionNone()
	}
	result := endpointBindingResult{tier: bindingTierNone}
	var refused error
	pinned := 0
	err := s.update(func(now time.Time) bool {
		added, pinErr := s.pinLocked(id, now)
		if pinErr != nil {
			refused, pinned = pinErr, len(s.identities)
			return false
		}
		var bound bool
		result, bound = s.bindLocked(address, id, protection, now)
		return added || bound
	})
	if refused != nil {
		s.logPinRefusal(id, pinned)
		return endpointBindingResult{tier: bindingTierNone}, refused
	}
	s.logBindingOutcome(address, id, result)
	if err != nil {
		return result, err
	}
	if result.refused != nil && protection.kind == protectionRequired {
		return result, result.refused
	}
	return result, nil
}

func (s *secureSessionStore) logPinRefusal(id domain.PeerIdentity, pinned int) {
	s.logger.Warn().
		Str("peer", id.String()).
		Str("reason", errPinStoreFull.Error()).
		Int("pinned", pinned).
		Msg("secure_session_refused_pin_store_full")
}

// logBindingOutcome reports what a note could not give: a protection refused
// at a bound, once per binding, and an endpoint left without any binding.
func (s *secureSessionStore) logBindingOutcome(address domain.PeerAddress, id domain.PeerIdentity, result endpointBindingResult) {
	if result.limitRefusalReported {
		s.logger.Warn().
			Str("peer", string(address)).
			Str("identity", id.String()).
			Str("reason", string(result.refused.Reason)).
			Str("recorded", result.tier.String()).
			Msg("protected_binding_limit_reached")
	}
	if result.tier == bindingTierNone {
		s.logger.Warn().Str("peer", string(address)).Msg("secure_session_endpoint_binding_refused_store_full")
	}
}

// flushRenewals writes the renewals made since the last snapshot, and retries
// a change whose write failed, at most once per renewalFlushEvery. Called on
// the bootstrap tick; nothing is written when nothing is pending.
func (s *secureSessionStore) flushRenewals() error {
	return s.flushPending(flushRenewalsWhenDue)
}

// flushRenewalsNow is flushRenewals without the wait: the last flush when the
// node stops.
func (s *secureSessionStore) flushRenewalsNow() error {
	return s.flushPending(flushRenewalsAtStop)
}

// renewalFlush is the moment a flush last wrote.
type renewalFlush struct {
	at time.Time
}

// dueAt reports whether a flush that waits for renewalFlushEvery may write at
// at; one that never wrote may.
func (f *renewalFlush) dueAt(at time.Time) bool {
	return f == nil || at.Sub(f.at) >= renewalFlushEvery
}

func (s *secureSessionStore) flushPending(moment renewalFlushMoment) error {
	s.mu.Lock()
	at := s.now()
	if s.unreadable || (moment == flushRenewalsWhenDue && !s.lastFlush.dueAt(at)) {
		s.mu.Unlock()
		return nil
	}
	if s.renewalsUnwritten {
		s.changes++
	}
	pending := s.changes != s.written
	if pending {
		s.lastFlush = &renewalFlush{at: at}
	}
	s.mu.Unlock()
	if !pending {
		return nil
	}
	return s.persist()
}

// update runs change under mu, then writes the store when the change — or an
// earlier one that never reached the disk — calls for it.
func (s *secureSessionStore) update(change func(now time.Time) bool) error {
	s.mu.Lock()
	if s.unreadable {
		// The file could not be read; writing now would replace whatever
		// protection it held with this process's partial view.
		s.mu.Unlock()
		return nil
	}
	if change(s.now()) {
		s.changes++
	}
	pending := s.changes != s.written
	s.mu.Unlock()
	if !pending {
		return nil
	}
	return s.persist()
}

// pinLocked pins id and reports whether that changed the store. An identity
// already pinned is served as before; a NEW identity at the bound is refused
// with errPinStoreFull — never by evicting an existing pin: existing
// protection is guaranteed, new pins are not promised. Caller holds mu; the
// caller logs a refusal after releasing it.
func (s *secureSessionStore) pinLocked(id domain.PeerIdentity, now time.Time) (bool, error) {
	if _, pinned := s.identities[id]; pinned {
		return false, nil
	}
	if len(s.identities) >= maxPinnedIdentities {
		s.pinRefusals++
		return false, errPinStoreFull
	}
	s.identities[id] = now
	return true, nil
}

// bindLocked binds address to id — renewing a live binding, creating a new
// one otherwise — and reports the outcome and whether the change must reach
// the disk now. Caller holds mu.
func (s *secureSessionStore) bindLocked(address domain.PeerAddress, id domain.PeerIdentity, protection endpointProtection, now time.Time) (endpointBindingResult, bool) {
	binding, bound := s.endpoints[address]
	if bound && binding.live(now) {
		return s.renewBindingLocked(address, binding, id, protection, now)
	}
	return s.createBindingLocked(address, bound, id, protection, now)
}

// renewBindingLocked records a new v2 session on a live binding. A new owner
// takes the binding over with its tier — a protected one stays protected,
// counted for whoever gave the protection — and never raises it: only the
// identity that already owned the endpoint can, so a protection asked for by
// a new owner is refused with owner_changed (not a bound, so not counted as
// one). A protected binding is never lowered and its pool never changes.
// Caller holds mu.
func (s *secureSessionStore) renewBindingLocked(address domain.PeerAddress, binding endpointBinding, id domain.PeerIdentity, protection endpointProtection, now time.Time) (endpointBindingResult, bool) {
	changedOwner := binding.identity != id
	result := endpointBindingResult{tier: binding.tier()}
	raised := false
	switch {
	case !protection.asked() || binding.protection != nil:
	case changedOwner:
		result.refused = &domain.ProtectedBindingRefused{Reason: domain.ProtectedBindingRefusedOwnerChanged}
	default:
		result.refused = s.protectedRefusalLocked(id, protection.pool)
		if result.refused == nil {
			s.protectBindingLocked(&binding, id, protection.pool)
			raised, result.tier = true, bindingTierProtected
		} else if !binding.limitRefusalCounted {
			s.countLimitRefusalLocked(result.refused.Reason)
			binding.limitRefusalCounted, result.limitRefusalReported = true, true
		}
	}
	binding.identity, binding.lastV2 = id, now
	s.endpoints[address] = binding
	due := changedOwner || raised
	if !due && binding.protection == nil {
		// A protected binding never expires, so its last_v2 changes
		// nothing that has to survive a crash; an ordinary one waits for
		// the batch.
		s.renewalsUnwritten = true
	}
	return result, due
}

// createBindingLocked binds an endpoint that has no live binding: protected
// when asked and a place is free, ordinary otherwise — or not at all when the
// ordinary tier is full too. An expired binding under the same address is
// replaced. A refusal at a bound is counted here; when no binding is left to
// remember that, the next attempt counts again. Caller holds mu.
func (s *secureSessionStore) createBindingLocked(address domain.PeerAddress, expired bool, id domain.PeerIdentity, protection endpointProtection, now time.Time) (endpointBindingResult, bool) {
	if expired {
		s.removeBindingLocked(address)
	}
	result := endpointBindingResult{tier: bindingTierNone}
	binding := endpointBinding{identity: id, lastV2: now}
	if protection.asked() {
		result.refused = s.protectedRefusalLocked(id, protection.pool)
		if result.refused == nil {
			binding.protection = &bindingProtection{by: id, pool: protection.pool}
		} else {
			s.countLimitRefusalLocked(result.refused.Reason)
			binding.limitRefusalCounted, result.limitRefusalReported = true, true
		}
	}
	if binding.protection == nil && !s.generalRoomLocked(now) {
		return result, expired
	}
	s.insertBindingLocked(address, binding)
	result.tier = binding.tier()
	return result, true
}

// protectedRefusalLocked checks the three bounds on protected bindings for
// one more given by id in pool: the identity's own bound first, then the
// pool's. The check and the insert that follows it run under the same hold
// of mu, so concurrent creations never pass a bound by even one. Caller holds
// mu.
func (s *secureSessionStore) protectedRefusalLocked(id domain.PeerIdentity, pool domain.ProtectedBindingPool) *domain.ProtectedBindingRefused {
	rule := protectedPoolRuleFor(pool)
	total := s.protectedByPool[domain.ProtectedBindingPoolContact] + s.protectedByPool[domain.ProtectedBindingPoolRelation]
	switch {
	case s.protectedByGiver[id] >= maxProtectedPerIdentity:
		return &domain.ProtectedBindingRefused{Reason: domain.ProtectedBindingRefusedPerIdentity}
	case s.protectedByPool[pool] >= rule.ceiling, total >= maxProtectedBindings:
		return &domain.ProtectedBindingRefused{Reason: rule.full}
	default:
		return nil
	}
}

// countLimitRefusalLocked counts one protection refused at a bound. Caller
// holds mu.
func (s *secureSessionStore) countLimitRefusalLocked(reason domain.ProtectedBindingRefusalReason) {
	s.protectedRefusals[reason]++
}

// generalRoomLocked reports whether one more ordinary binding fits. Only a
// full tier is scanned for expired bindings: a scan per new binding would be
// quadratic work under mu. Caller holds mu.
func (s *secureSessionStore) generalRoomLocked(now time.Time) bool {
	if s.generalBindings < maxGeneralBindings {
		return true
	}
	s.pruneExpiredLocked(now)
	return s.generalBindings < maxGeneralBindings
}

// pruneExpiredLocked drops ordinary bindings whose 30 days ran out — the one
// exception to "protection is never dropped", since an ordinary binding only
// records that an address once answered over v2. Protected ones are
// never pruned. Caller holds mu.
func (s *secureSessionStore) pruneExpiredLocked(now time.Time) {
	for address, binding := range s.endpoints {
		if !binding.live(now) {
			s.removeBindingLocked(address)
		}
	}
}

// insertBindingLocked puts binding under address and counts it. Caller holds
// mu.
func (s *secureSessionStore) insertBindingLocked(address domain.PeerAddress, binding endpointBinding) {
	if _, present := s.endpoints[address]; present {
		s.removeBindingLocked(address)
	}
	s.endpoints[address] = binding
	if binding.protection == nil {
		s.generalBindings++
		return
	}
	s.protectedByPool[binding.protection.pool]++
	s.protectedByGiver[binding.protection.by]++
}

// removeBindingLocked deletes the binding of address and uncounts it. Only
// an expired ordinary binding is ever removed. Caller holds mu.
func (s *secureSessionStore) removeBindingLocked(address domain.PeerAddress) {
	binding, present := s.endpoints[address]
	if !present {
		return
	}
	delete(s.endpoints, address)
	if binding.protection == nil {
		s.generalBindings--
		return
	}
	s.protectedByPool[binding.protection.pool]--
	if s.protectedByGiver[binding.protection.by]--; s.protectedByGiver[binding.protection.by] == 0 {
		delete(s.protectedByGiver, binding.protection.by)
	}
}

// protectBindingLocked raises an ordinary binding to protected, given by id in
// pool, and moves it between the counts. The caller writes the binding back
// to the map. Caller holds mu.
func (s *secureSessionStore) protectBindingLocked(binding *endpointBinding, id domain.PeerIdentity, pool domain.ProtectedBindingPool) {
	binding.protection = &bindingProtection{by: id, pool: pool}
	s.generalBindings--
	s.protectedByPool[pool]++
	s.protectedByGiver[id]++
}

// persist writes a snapshot of the store and reports a failure. The snapshot
// is taken under mu, the encoding and the write happen after it is released;
// on success the change count the snapshot covered is recorded as written.
func (s *secureSessionStore) persist() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.written == s.changes {
		// A save that waited for writeMu while another wrote the state it
		// was called for has nothing left to write.
		s.mu.Unlock()
		return nil
	}
	covers := s.changes
	// The snapshot carries every renewal made so far.
	s.renewalsUnwritten = false
	if s.dir == "" && s.writeFile == nil {
		s.written = covers
		s.mu.Unlock()
		return nil
	}
	file := secureSessionFile{
		Format:     secureSessionStoreFormat,
		Identities: make(map[domain.PeerIdentity]time.Time, len(s.identities)),
		Endpoints:  make(map[domain.PeerAddress]endpointBindingRecord, len(s.endpoints)),
	}
	for id, at := range s.identities {
		file.Identities[id] = at
	}
	for address, binding := range s.endpoints {
		file.Endpoints[address] = recordOf(binding)
	}
	s.mu.Unlock()

	raw, err := json.Marshal(file)
	if err == nil {
		err = s.writeFile(raw)
	}
	if err != nil {
		s.logger.Error().Err(err).Str("store", s.name).Msg("secure_session_store_write_failed")
		return fmt.Errorf("secure session store: %w", err)
	}
	s.mu.Lock()
	if covers > s.written {
		s.written = covers
	}
	s.mu.Unlock()
	return nil
}

// writeToDisk writes raw through secretfile; a memory-only store has nowhere
// to write and nothing to lose.
func (s *secureSessionStore) writeToDisk(raw []byte) error {
	if s.dir == "" {
		return nil
	}
	dir, err := secretfile.Open(s.dir)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Write(s.name, raw)
}

// renewalFlushMoment says whether a flush of the secure-session store's
// renewals waits for renewalFlushEvery.
type renewalFlushMoment int

const (
	flushRenewalsWhenDue renewalFlushMoment = iota + 1
	flushRenewalsAtStop
)

// flushSecureSessionRenewals writes the store's batched renewals: on the
// bootstrap tick once renewalFlushEvery has passed, and whatever is unwritten
// when the loop stops. Disk I/O, so the caller holds no domain mutex.
func (s *Service) flushSecureSessionRenewals(moment renewalFlushMoment) {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return
	}
	flush := s.secureSessions.store.flushRenewals
	if moment == flushRenewalsAtStop {
		flush = s.secureSessions.store.flushRenewalsNow
	}
	if err := flush(); err != nil {
		// persist has logged the failure at Error with the store's name;
		// this line only says what follows from it.
		log.Debug().Err(err).Str("store", s.secureSessions.store.name).Str("operation", "renewal_flush").
			Msg("secure_session_renewal_flush_failed_retried_on_a_later_tick")
	}
}

// SecureSessionStoreStats is the v2 downgrade-protection store as diagnostics
// see it; a node without v2 state reports the zero value.
func (s *Service) SecureSessionStoreStats() domain.SecureSessionStoreStats {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.SecureSessionStoreStats{}
	}
	return s.secureSessions.store.stats()
}
