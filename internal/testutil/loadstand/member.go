package loadstand

import (
	"context"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

type MemberID string
type MemberRole string

const (
	MemberFull MemberRole = "full"
	MemberSDK  MemberRole = "sdk-edge"
)

type IncarnationOrdinal uint64
type Incarnation struct {
	Ordinal   IncarnationOrdinal
	StartedAt time.Time
}
type PublicIdentity struct {
	Identity     domain.PeerIdentity
	PublicKey    string
	BoxPublicKey string
	BoxSignature string
}
type StartReceipt struct {
	Public      PublicIdentity
	Incarnation Incarnation
}
type ObservationState string

const (
	ObservationAvailable   ObservationState = "available"
	ObservationUnavailable ObservationState = "unavailable"
	ObservationDisabled    ObservationState = "explicitly-disabled"
)

// Observation never serializes an unavailable reading as a measured zero.
// Error text is deliberately excluded: remote responses may contain secrets.
type Observation[T any] struct {
	State ObservationState
	Value *T
}

func Observed[T any](value T, err error) Observation[T] {
	if err != nil {
		return Observation[T]{State: ObservationUnavailable}
	}
	return Observation[T]{State: ObservationAvailable, Value: &value}
}
func (o Observation[T]) Available() bool { return o.State == ObservationAvailable && o.Value != nil }

type MemberSample struct {
	// Process is original adapter custody, not an identity/role-derived label.
	// It is never reconstructed from JSON; report group ordinals are labels only.
	Process RuntimeProcess `json:"-"`
	Runtime Observation[domain.RuntimeMetrics]

	// Non-nil only for actual external Linux members; unsupported/failed reads
	// remain explicit unavailable, never in-process heap or measured zero.
	LinuxProcess *Observation[LinuxProcessSample] `json:",omitempty"`
	Incarnation  Incarnation
	Identity     domain.PeerIdentity
	ReadAt       time.Time
	Traffic      Observation[domain.TransportTrafficStats]
	Sessions     Observation[domain.SessionOutcomeStats]
	Modes        Observation[routing.ModeSelectionStats]
	Neighbours   Observation[domain.NeighbourComposition]
	RouteEntries Observation[int]
	Resources    Observation[domain.ResourceBreakdown]
	Datagram     Observation[DatagramSample]
	Bans         Observation[[]BanFinding]
}

func (s MemberSample) FullSampleAvailable() bool {
	return s.Runtime.Available() && (s.LinuxProcess == nil || s.LinuxProcess.Available()) && s.Traffic.Available() && s.Sessions.Available() && s.Modes.Available() && s.Neighbours.Available() && s.RouteEntries.Available() && s.Resources.Available() && (s.Datagram.Available() || s.Datagram.State == ObservationDisabled) && s.Bans.Available()
}

type Finality string

const (
	FinalExact       Finality = "exact-after-stop"
	FinalUnavailable Finality = "unavailable-after-stop"
)

type EndedIncarnation struct {
	Incarnation Incarnation
	Finality    Finality
	Final       Observation[domain.TransportTrafficStats]
}

// Clone prevents caller mutation of pointer-shaped observation projections.
func (e EndedIncarnation) Clone() EndedIncarnation {
	if e.Final.Value != nil {
		v := *e.Final.Value
		e.Final.Value = &v
	}
	return e
}

type StopReceipt struct {
	Terminated bool
	Ended      *EndedIncarnation
}

// Member methods are called by one executor. Implementations retain ownership
// before waiting for readiness; an error from Start does not erase that owner.
// PeerHealth is a second, global barrier after all initial launches.
type Member interface {
	ID() MemberID
	Role() MemberRole
	Endpoint() (domain.PeerAddress, bool)
	Bootstrap() []domain.PeerAddress
	Start(lifetime context.Context, readiness context.Context) (StartReceipt, error)
	Owned() bool
	CheckRunning(context.Context) error
	Done() <-chan struct{}
	PeerHealth(context.Context) error
	ImportContacts(context.Context, []PublicIdentity) error
	RuntimeProcess() RuntimeProcess
	Sample(context.Context) (MemberSample, error)
	Stop(context.Context) (StopReceipt, error)
	EndedIncarnations() []EndedIncarnation
}
type MessageBody string
type PendingAck struct {
	Recipient domain.PeerIdentity
	Queued    bool
}
type UserTraffic interface {
	PlannedRoute(context.Context, domain.PeerIdentity, []domain.PeerIdentity) error
	SendDM(context.Context, domain.PeerIdentity, MessageBody) (PendingAck, error)
	Receipts(context.Context, int) ([]Receipt, error)
}

var ErrScheduleMissed = errors.New("functional timeline deadline missed")
var ErrMemberState = errors.New("functional member lifecycle or observation invalid")
var ErrUnsupportedPlan = errors.New("functional executor does not support this plan")

// ScheduleClock measures monotonic elapsed time, independently of journal UTC.
// Offsets are absolute; WaitUntil never rebases an overloaded schedule.
type ScheduleClock interface {
	Elapsed() time.Duration
	WaitUntil(context.Context, time.Duration) error
}
type MonotonicSchedule struct{ epoch time.Time }

func NewMonotonicSchedule() *MonotonicSchedule      { return &MonotonicSchedule{epoch: time.Now()} }
func (c *MonotonicSchedule) Elapsed() time.Duration { return time.Since(c.epoch) }
func (c *MonotonicSchedule) WaitUntil(ctx context.Context, at time.Duration) error {
	delay := at - c.Elapsed()
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
