// Package connbudget is the single admission authority for connection slots
// of one node: inbound sessions, outbound sessions, and the attempts that have
// not finished becoming either.
//
// It exists because a node used to have three unrelated ceilings behind three
// unrelated locks — outbound slots under the connection manager's mutex,
// inbound connections under the service's peer mutex, per-IP counters under a
// third — and the sum of independently bounded subsystems is not bounded. A
// node could hold more sockets than any single number in its configuration.
//
// Two properties drive every decision in this file:
//
//   - RESERVATION IS ADMISSION. Asking "is there room" and taking the room are
//     one operation under one mutex. A read-then-take pair leaves a window in
//     which N concurrent dials all pass the same check, which is precisely the
//     failure the ceiling exists to prevent;
//   - THE MUTEX IS A LEAF. This package never calls out — no callbacks, no
//     logging hooks, no other component's lock is ever taken while ours is
//     held. That is what lets the connection manager and the service each
//     consult the same budget without creating an edge between their locks;
//     the forbidden `cm.mu → peerMu` edge never appears, because neither of
//     them is taken here.
//
// Reference: docs/refactoring/dht/15-overlay-parameters.md §0.1.1.
package connbudget

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Direction says which half of the ceiling a reservation is drawn from. It is
// a named type rather than a bool because the two directions are not
// symmetric: the outbound reserve exists exactly so that one of them cannot
// consume what the other needs.
type Direction uint8

const (
	// DirectionOutbound is a dial this node initiates — including the dial
	// that has not connected yet.
	DirectionOutbound Direction = iota + 1
	// DirectionInbound is a socket this node accepted — including the one
	// that has not finished its handshake.
	DirectionInbound
	// DirectionAuxiliary is a short-lived outbound socket that is NOT a
	// persistent peer slot: a sender-key sync, a notice delivered to a peer
	// with no live connection.
	//
	// ⚠️ It is a third direction rather than "outbound with a different
	// comment" because review found the two must not share a limit. The
	// outbound limit sizes the node's PERSISTENT neighbourhood, and in the
	// steady state the connection manager holds every one of those positions
	// — so charging auxiliary dials to it refused them permanently, and
	// waiting did not help: the manager keeps the slots full by design. A
	// mechanism meant to bound resources had silently removed a working
	// capability.
	DirectionAuxiliary
)

// String renders the direction for diagnostics.
func (d Direction) String() string {
	switch d {
	case DirectionOutbound:
		return "outbound"
	case DirectionInbound:
		return "inbound"
	case DirectionAuxiliary:
		return "auxiliary"
	default:
		return "unknown"
	}
}

// Refusal reasons are distinguishable sentinels so a caller can act on WHY it
// was refused — and so a diagnostic can say which ceiling a node is sitting
// against — without matching error text.
var (
	// ErrTotalExhausted means the shared ceiling B has no room left.
	ErrTotalExhausted = errors.New("connbudget: total connection budget exhausted")

	// ErrOutboundReserved means the ceiling has room, but the room left is
	// the outbound reserve, which inbound connections may never take —
	// even while it sits idle. Without this a node saturated by inbound
	// connections loses the ability to dial out, which is the ability it
	// needs most in exactly that state.
	ErrOutboundReserved = errors.New("connbudget: remaining capacity is reserved for outbound")

	// ErrDirectionLimit means the per-direction ceiling that existed before
	// the shared budget still binds. Direction limits are not replaced by
	// B; they keep working inside it.
	ErrDirectionLimit = errors.New("connbudget: per-direction limit reached")

	// ErrInvalidConfig marks a configuration that cannot be honoured, such
	// as a reserve larger than the ceiling it is carved from. It is
	// returned from New and never silently corrected: a ceiling quietly
	// clamped into consistency is a ceiling nobody chose.
	ErrInvalidConfig = errors.New("connbudget: invalid configuration")

	// ErrUnknownDirection marks a reservation asked for with a direction
	// outside the closed set.
	ErrUnknownDirection = errors.New("connbudget: unknown direction")
)

// Config is the whole configuration of the budget. All fields are read once,
// in New: a ceiling that changes under a reservation is a ceiling that can be
// violated by a configuration reload.
type Config struct {
	// Total is B — the shared ceiling over inbound + outbound + attempts
	// in flight. ZERO MEANS THE SHARED CEILING IS OFF: only the
	// per-direction limits below apply, which is the behaviour a node had
	// before this package existed. It is off by default because the
	// default value of B is approved from the load bench (13b), not from a
	// single memory figure.
	Total int

	// OutboundReserve is R_out — capacity inbound connections may never
	// take. Initially equal to the effective outbound peer limit.
	OutboundReserve int

	// MaxOutbound and MaxInbound are the pre-existing per-direction
	// ceilings. They keep applying INSIDE the shared budget rather than
	// being replaced by it. Zero means "this direction has no limit of its
	// own", matching the historical meaning of both settings.
	MaxOutbound int
	MaxInbound  int

	// MaxAuxiliary bounds the short-lived outbound dials that are not peer
	// slots. Zero means they have no limit of their own — which is the
	// behaviour they had before this package existed, and therefore what
	// they must keep when the shared ceiling is OFF: a mechanism that is
	// switched off must not take a capability away.
	//
	// When the ceiling is on, a bound is required for the opposite reason:
	// auxiliary dials are bursty by nature and would otherwise be able to
	// consume the whole of B and starve the peer slots.
	MaxAuxiliary int
}

// Budget is the admission authority. The zero value is not usable; construct
// with New.
type Budget struct {
	// cfg is immutable after New — see Config.
	cfg Config

	// mu guards everything below. It is a LEAF: no call under it leaves
	// this file.
	mu        sync.Mutex
	outbound  int
	inbound   int
	auxiliary int

	// Refusal counters, kept beside the state they explain. Read only
	// through Snapshot.
	refusedTotal     uint64
	refusedReserved  uint64
	refusedDirection uint64
	refusedUnknown   uint64
}

// Used returns the total capacity in use, including attempts in flight.
// Callers that need the breakdown use Snapshot.
func (b *Budget) usedLocked() int { return b.outbound + b.inbound + b.auxiliary }

// New validates the configuration and returns a budget.
//
// It refuses rather than repairs. A reserve larger than the ceiling, or a
// negative number anywhere, is a contradiction the operator has to resolve:
// silently trimming it would leave the node running with a limit nobody
// configured and nobody could find later.
//
// R_out == B is allowed and means "this node holds nothing but peer slots":
// no inbound connections and no auxiliary dials. That is a legitimate,
// deliberately reachable mode — a client that only dials out — and Snapshot
// reports NonSlotCapacity == 0 so the state is visible rather than
// surprising.
func New(cfg Config) (*Budget, error) {
	switch {
	case cfg.Total < 0:
		return nil, fmt.Errorf("%w: total budget %d is negative", ErrInvalidConfig, cfg.Total)
	case cfg.OutboundReserve < 0:
		return nil, fmt.Errorf("%w: outbound reserve %d is negative", ErrInvalidConfig, cfg.OutboundReserve)
	case cfg.MaxOutbound < 0:
		return nil, fmt.Errorf("%w: outbound limit %d is negative", ErrInvalidConfig, cfg.MaxOutbound)
	case cfg.MaxInbound < 0:
		return nil, fmt.Errorf("%w: inbound limit %d is negative", ErrInvalidConfig, cfg.MaxInbound)
	case cfg.MaxAuxiliary < 0:
		return nil, fmt.Errorf("%w: auxiliary limit %d is negative", ErrInvalidConfig, cfg.MaxAuxiliary)
	case cfg.Total > 0 && cfg.OutboundReserve > cfg.Total:
		return nil, fmt.Errorf("%w: outbound reserve %d exceeds total budget %d",
			ErrInvalidConfig, cfg.OutboundReserve, cfg.Total)
	}
	return &Budget{cfg: cfg}, nil
}

// Enabled reports whether the shared ceiling applies. When it does not, the
// per-direction limits still do.
func (b *Budget) Enabled() bool { return b.cfg.Total > 0 }

// Reserve takes one unit of capacity for one connection attempt, atomically.
//
// The returned reservation is the caller's until it is released, and it is
// what a successful attempt hands to the connection it produced — there is no
// release-and-re-reserve step at the moment a dial succeeds, because that step
// is a window in which the ceiling can be exceeded by someone else.
//
// A nil budget reserves freely: it is the "no budget wired" case, not a
// silent zero ceiling.
func (b *Budget) Reserve(direction Direction) (*Reservation, error) {
	if b == nil {
		return &Reservation{}, nil
	}
	if direction != DirectionOutbound && direction != DirectionInbound && direction != DirectionAuxiliary {
		atomic.AddUint64(&b.refusedUnknown, 1)
		return nil, fmt.Errorf("%w: %d", ErrUnknownDirection, uint8(direction))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.admitLocked(direction); err != nil {
		return nil, err
	}

	switch direction {
	case DirectionOutbound:
		b.outbound++
	case DirectionInbound:
		b.inbound++
	case DirectionAuxiliary:
		b.auxiliary++
	}
	return &Reservation{budget: b, direction: direction}, nil
}

// admitLocked applies the three ceilings in the order that makes the refusal
// reason the most specific one true: the direction's own limit, then the
// shared ceiling, then the outbound reserve. Caller holds b.mu.
func (b *Budget) admitLocked(direction Direction) error {
	switch direction {
	case DirectionOutbound:
		if b.cfg.MaxOutbound > 0 && b.outbound >= b.cfg.MaxOutbound {
			b.refusedDirection++
			return fmt.Errorf("%w: outbound at %d", ErrDirectionLimit, b.outbound)
		}
	case DirectionInbound:
		if b.cfg.MaxInbound > 0 && b.inbound >= b.cfg.MaxInbound {
			b.refusedDirection++
			return fmt.Errorf("%w: inbound at %d", ErrDirectionLimit, b.inbound)
		}
	case DirectionAuxiliary:
		if b.cfg.MaxAuxiliary > 0 && b.auxiliary >= b.cfg.MaxAuxiliary {
			b.refusedDirection++
			return fmt.Errorf("%w: auxiliary at %d", ErrDirectionLimit, b.auxiliary)
		}
	}

	if !b.Enabled() {
		return nil
	}

	used := b.usedLocked()
	if used >= b.cfg.Total {
		b.refusedTotal++
		return fmt.Errorf("%w: %d of %d in use", ErrTotalExhausted, used, b.cfg.Total)
	}

	// The reserve protects the PERSISTENT NEIGHBOURHOOD, so it binds on every
	// direction that is not a peer slot — inbound and auxiliary alike, and
	// against their COMBINED usage.
	//
	// ⚠️ Review found checking inbound alone was not enough: with B = 12 and
	// R_out = 8, four inbound plus four auxiliary left the connection manager
	// four positions instead of eight, and a separate MaxAuxiliary did not
	// prevent it — two limits that are each satisfied can still add up past
	// the thing they were meant to protect. Counting them together also makes
	// the outcome independent of the order the connections arrive in.
	if direction != DirectionOutbound {
		nonSlot := b.inbound + b.auxiliary
		if capacity := b.cfg.Total - b.cfg.OutboundReserve; nonSlot >= capacity {
			b.refusedReserved++
			return fmt.Errorf("%w: inbound+auxiliary at %d of %d", ErrOutboundReserved, nonSlot, capacity)
		}
	}
	return nil
}

// release returns one unit. Only Reservation.Release calls it, and only once.
func (b *Budget) release(direction Direction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch direction {
	case DirectionOutbound:
		if b.outbound > 0 {
			b.outbound--
		}
	case DirectionInbound:
		if b.inbound > 0 {
			b.inbound--
		}
	case DirectionAuxiliary:
		if b.auxiliary > 0 {
			b.auxiliary--
		}
	}
}

// Stats is a point-in-time view of the budget for diagnostics. Usage INCLUDES
// attempts that have not finished: a ceiling that only counted established
// sessions would be bypassed by whatever is not counted yet, which is the
// oldest way a connection limit is defeated.
type Stats struct {
	Enabled         bool
	Total           int
	OutboundReserve int
	// NonSlotCapacity is Total − OutboundReserve: the most connections that
	// are NOT persistent peer slots — inbound plus auxiliary together — this
	// node will ever hold at once. Zero is a valid, deliberate configuration
	// (a node that dials out and accepts nothing); it is reported rather
	// than inferred.
	NonSlotCapacity int
	MaxOutbound     int
	MaxInbound      int
	MaxAuxiliary    int

	Outbound  int
	Inbound   int
	Auxiliary int
	Used      int

	RefusedTotal     uint64
	RefusedReserved  uint64
	RefusedDirection uint64
	RefusedUnknown   uint64
}

// Snapshot reads the whole state under one lock so the numbers in it are
// consistent with each other. A caller that read them one at a time could
// report a used total that never existed.
func (b *Budget) Snapshot() Stats {
	if b == nil {
		return Stats{}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	nonSlotCap := 0
	if b.cfg.Total > 0 {
		nonSlotCap = b.cfg.Total - b.cfg.OutboundReserve
	}
	return Stats{
		Enabled:          b.cfg.Total > 0,
		Total:            b.cfg.Total,
		OutboundReserve:  b.cfg.OutboundReserve,
		NonSlotCapacity:  nonSlotCap,
		MaxOutbound:      b.cfg.MaxOutbound,
		MaxInbound:       b.cfg.MaxInbound,
		MaxAuxiliary:     b.cfg.MaxAuxiliary,
		Outbound:         b.outbound,
		Inbound:          b.inbound,
		Auxiliary:        b.auxiliary,
		Used:             b.usedLocked(),
		RefusedTotal:     b.refusedTotal,
		RefusedReserved:  b.refusedReserved,
		RefusedDirection: b.refusedDirection,
		RefusedUnknown:   atomic.LoadUint64(&b.refusedUnknown),
	}
}

// Reservation is one unit of admitted capacity. It is a handle rather than a
// counter decrement so ownership is a value that can be MOVED — from the
// attempt to the connection it produced, or to whoever is left holding an
// orphaned socket — instead of a rule everyone has to remember.
//
// The zero value is a no-op reservation, which is what a nil budget hands out.
type Reservation struct {
	budget    *Budget
	direction Direction
	released  atomic.Bool
}

// Release returns the capacity. It is IDEMPOTENT by construction, not by
// convention: a connection attempt has several ends — failure, cancellation,
// a late success closing an orphaned socket — and more than one of them can
// run for the same attempt. Double-release would inflate free capacity and
// let the ceiling be exceeded, so the second call is a no-op rather than a
// second decrement.
//
// Releasing a nil reservation is legal and does nothing, so callers do not
// need a nil check on paths where a budget may not be wired.
func (r *Reservation) Release() {
	if r == nil || r.budget == nil {
		return
	}
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	r.budget.release(r.direction)
}

// Direction reports which half the reservation was drawn from. Used by
// diagnostics and by tests; the holder normally does not care.
func (r *Reservation) Direction() Direction {
	if r == nil {
		return 0
	}
	return r.direction
}
