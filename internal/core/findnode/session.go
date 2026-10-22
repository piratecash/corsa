// Package findnode contains inactive, transport-free FIND_NODE state blocks.
// Nothing here advertises capabilities, installs handlers or writes frames.
package findnode

import (
	"errors"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

const responderTimeout = Instant(6 * time.Second)
const cacheLifetime = Instant(60 * time.Second)

var (
	ErrUnauthenticated  = errors.New("find node: proven negotiated session required")
	ErrInvalid          = errors.New("find node: invalid state input")
	ErrClock            = errors.New("find node: monotonic time moved backwards")
	ErrCapacity         = errors.New("find node: bounded state full")
	ErrDuplicateLink    = errors.New("find node: duplicate challenge on session")
	ErrDuplicateExcess  = errors.New("find node: challenge already has three sessions")
	ErrDuplicateID      = errors.New("find node: duplicate pending request id")
	ErrStaleWork        = errors.New("find node: stale or released work")
	ErrUnmatchedAnswer  = errors.New("find node: unmatched session or target")
	ErrPrioritySnapshot = errors.New("find node: incomplete priority snapshot")
	ErrBudget           = errors.New("find node: no legal slot before deadline")
	ErrNotReady         = errors.New("find node: reserved slot not ready")
)

// Instant is elapsed monotonic time in one state owner's clock domain. A caller
// uses its domain elapsed sample, never a wall timestamp or a persisted value.
type Instant time.Duration

func (at Instant) plus(delta Instant) (Instant, error) {
	if at < 0 || delta < 0 || int64(at) > math.MaxInt64-int64(delta) {
		return 0, ErrInvalid
	}
	return at + delta, nil
}

type Session struct{ state *sessionState }
type sessionState struct {
	connID                          domain.ConnID
	identity                        domain.PeerIdentity
	identityProof                   sessionv2.ProvenIdentity
	group                           domain.SourceGroup
	opened                          Instant
	closed                          atomic.Bool
	sourceWindows                   atomic.Pointer[candidateSourceWindows]
	mu                              sync.Mutex
	nextID                          domain.FindNodeRequestID
	exhausted                       bool
	signer                          *SigningCalendar
	signingProjected, signingActual forwardBucket
}

// SessionSeed captures proof and negotiated capabilities at the handshake. Copies
// share a one-shot latch; neither the seed nor Session retains TLS/Conn aliases.
type SessionSeed struct{ state *sessionSeedState }
type sessionSeedState struct {
	identity domain.PeerIdentity
	proof    sessionv2.ProvenIdentity
	prepared *sessionState
	consumed atomic.Bool
}

func PrepareSession(proven sessionv2.ProvenSession, local []domain.Capability) (SessionSeed, error) {
	peer, err := proven.Peer()
	if err != nil {
		return SessionSeed{}, ErrUnauthenticated
	}
	proof, ok := peer.Proof()
	if !ok || !slices.Contains(local, domain.CapOverlayFindNodeV1) || !slices.Contains(peer.Intro.Capabilities, string(domain.CapOverlayFindNodeV1)) {
		return SessionSeed{}, ErrUnauthenticated
	}
	return SessionSeed{state: &sessionSeedState{identity: peer.Identity, proof: proof, prepared: &sessionState{}}}, nil
}
func (seed SessionSeed) Bind(connID domain.ConnID, group domain.SourceGroup, opened Instant) (Session, error) {
	if seed.state == nil || seed.state.prepared == nil {
		return Session{}, ErrUnauthenticated
	}
	if connID == 0 || group == "" || opened < 0 {
		return Session{}, ErrInvalid
	}
	if !seed.state.consumed.CompareAndSwap(false, true) {
		return Session{}, ErrInvalid
	}
	// Only the shared latch winner writes the unpublished preallocated state.
	s := seed.state.prepared
	s.connID, s.identity, s.identityProof = connID, seed.state.identity, seed.state.proof
	s.group, s.opened = group, opened
	return Session{state: s}, nil
}

// NewSession preserves proof/capability/argument validation of the original API.
func NewSession(proven sessionv2.ProvenSession, connID domain.ConnID, group domain.SourceGroup, opened Instant, local []domain.Capability) (Session, error) {
	seed, err := PrepareSession(proven, local)
	if err != nil {
		return Session{}, err
	}
	return seed.Bind(connID, group, opened)
}
func (s Session) valid() bool { return s.state != nil && !s.state.closed.Load() }
func (s Session) ConnID() domain.ConnID {
	if s.state == nil {
		return 0
	}
	return s.state.connID
}
func (s Session) Identity() domain.PeerIdentity {
	if s.state == nil {
		return domain.PeerIdentity{}
	}
	return s.state.identity
}
func (s Session) End() {
	if s.state != nil {
		s.state.closed.Store(true)
	}
}
func (s Session) close() { s.End() }
func (s Session) allocateID() (domain.FindNodeRequestID, error) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	id, err := s.peekNextIDLocked()
	if err != nil {
		return 0, err
	}
	s.commitNextIDLocked(id)
	return id, nil
}

// peekNextIDLocked requires s.state.mu, continuously through commitNextIDLocked.
func (s Session) peekNextIDLocked() (domain.FindNodeRequestID, error) {
	if !s.valid() || s.state.exhausted {
		return 0, ErrStaleWork
	}
	return s.state.nextID, nil
}

// commitNextIDLocked requires s.state.mu and its successful, unreleased peek.
func (s Session) commitNextIDLocked(id domain.FindNodeRequestID) {
	if id == math.MaxUint64 {
		s.state.exhausted = true
	} else {
		s.state.nextID++
	}
}

type SessionClass uint8

const (
	ClassOrdinary SessionClass = iota + 1
	ClassContact
	ClassSelected
	ClassIncomingPinned
)

type Priority uint8

const (
	PriorityProtected Priority = iota + 1
	PriorityAged
	PriorityNew
)

type SessionInfo struct {
	ConnID domain.ConnID
	Proof  sessionv2.ProvenIdentity
	Group  domain.SourceGroup
	Opened Instant
}

// PrioritySource is an explicit authoritative class port. It must read K/N-sel/
// N-in classification, never infer a class from the group or create pin state.
// A private node candidate may synchronously capture current strict classes for
// registered F1 members; that bounded observation is not online R/T admission.
// No production port is installed here. Calls precede every state mutex.
type PrioritySource func(SessionInfo) (SessionClass, error)
type PrioritySnapshot struct {
	target     domain.PeerIdentity
	at         Instant
	priorities map[Session]Priority
}

func NewPrioritySnapshot(target domain.PeerIdentity, sessions []Session, now Instant, classify PrioritySource) (PrioritySnapshot, error) {
	if target.IsZero() || now < 0 || classify == nil {
		return PrioritySnapshot{}, ErrInvalid
	}
	out := PrioritySnapshot{target: target, at: now, priorities: make(map[Session]Priority, len(sessions))}
	aged := make([]Session, 0, len(sessions))
	for _, s := range sessions {
		if !s.valid() || s.state.opened > now {
			return PrioritySnapshot{}, ErrUnauthenticated
		}
		if _, exists := out.priorities[s]; exists {
			continue
		}
		class, err := classify(SessionInfo{ConnID: s.ConnID(), Proof: s.state.identityProof, Group: s.state.group, Opened: s.state.opened})
		if err != nil {
			return PrioritySnapshot{}, err
		}
		switch class {
		case ClassContact, ClassSelected:
			out.priorities[s] = PriorityProtected
		case ClassIncomingPinned, ClassOrdinary:
			out.priorities[s] = PriorityNew
			if class == ClassIncomingPinned || now-s.state.opened >= Instant(10*time.Minute) {
				aged = append(aged, s)
			}
		default:
			return PrioritySnapshot{}, ErrInvalid
		}
	}
	sort.Slice(aged, func(i, j int) bool {
		a, b := aged[i], aged[j]
		if a.state.opened != b.state.opened {
			return a.state.opened < b.state.opened
		}
		return a.ConnID() < b.ConnID()
	})
	for i, s := range aged {
		if i == 4 {
			break
		}
		out.priorities[s] = PriorityAged
	}
	return out, nil
}
func (p PrioritySnapshot) Priority(s Session) (Priority, bool) {
	priority, ok := p.priorities[s]
	return priority, ok
}

type AnswerBody struct {
	Target    domain.PeerIdentity
	PublicKey identity.PublicKey
	Signature domain.FindNodeSignature
}

func (b AnswerBody) Answer(id domain.FindNodeRequestID) protocol.FindNodeAnswer {
	return protocol.FindNodeAnswer{ID: id, Target: b.Target, PublicKey: b.PublicKey, Signature: b.Signature}
}

type Delivery struct {
	Session Session
	Answer  protocol.FindNodeAnswer
}
type Counters struct{ Accepted, Refused, Expired, Cancelled, Closed, Evicted, Completed uint64 }
