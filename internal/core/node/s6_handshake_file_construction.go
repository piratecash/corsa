package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Explicit caller budgets, not deployment defaults or a physical fit proof.
type HandshakeFileObservationLimits struct {
	RecordBytes                                       int
	StoreBytes, OrphanBytes, ScanBytes                int64
	StoreRecords, OrphanRecords, ScanBatch, ScanSteps int
}

// Directory is BORROWED. The caller keeps it open until Close returns, and
// excludes its own writes during issuer walks. Service owns issuer+lease only.
// Key refs remain unverified; Session is not an inferred boot/UTC identity.
type HandshakeFileObservationRequest struct {
	Key                     HandshakeObservationKey
	Capacity, ChunkCapacity int
	Session                 [16]byte
	Directory               *secretfile.Dir
	Limits                  HandshakeFileObservationLimits
}

// Schema2 uses exclusive row cursors and actual temporal checkpoints. It is
// not a V1 retained-suffix Snapshot. Carries preserve original entry brackets.
type HandshakeFileCheckpoint struct {
	Source                        HandshakeObservationSource
	Bounds                        HandshakeObservationBounds
	CoverageStart                 time.Duration
	Session                       [16]byte
	ID, PreviousID                uint64
	PreviousDigest                [32]byte
	FromSequence, ThroughSequence uint64
	FromAt, ThroughAt             time.Duration
	Rows                          int
	FromFull, ThroughFull         bool
	FromEntry, ThroughEntry       HandshakeObservationTransition
}

// A result concerns a finite PAST checkpoint, never UTC/benign-load/whole-run
// coverage. NO_PROGRESS means only no new source cut, not filesystem health.
// No unbounded Rows copy or caller-constructible receipt is exposed.
type HandshakeFileObservationResult struct {
	SchemaVersion                                          uint32
	Available, AttributionBound, PastOnly, Closing, Closed bool
	Status, Reason                                         string
	SourceStatus, SourceReason                             string
	Checkpoint                                             HandshakeFileCheckpoint
	Digest                                                 [32]byte
	CommitOutcome                                          secretfile.DurableCommit
	AckStatus, AckReason                                   string
}

// Immutable same-Service binding. opMu serializes only this explicit sync
// consumer, outside every Service/gate lock. Close fences with closing BEFORE
// waiting for active Capture. No timer/goroutine/cadence or hard FS latency
// bound is introduced. Do not copy this handle after first use.
type HandshakeFileObservation struct {
	service         *Service
	gate            *inboundHandshakes
	issuer          *handshakeFileIssuer
	key             HandshakeObservationKey
	identity        domain.PeerIdentity
	session         [16]byte
	opMu            sync.Mutex
	closing, closed atomic.Bool
	closeErr        error
	statusMu        sync.Mutex
	last            HandshakeFileObservationResult
	lastErr         error
}

func (o *HandshakeFileObservation) Key() HandshakeObservationKey {
	if o == nil {
		return HandshakeObservationKey{}
	}
	return o.key
}
func (o *HandshakeFileObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *HandshakeFileObservation) Status() HandshakeFileObservationResult {
	if o == nil {
		return HandshakeFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}
	}
	o.statusMu.Lock()
	result := o.last
	o.statusMu.Unlock()
	result.Closing, result.Closed = o.closing.Load(), o.closed.Load()
	if result.Closing && !result.Closed {
		result.Status, result.Reason = "CLOSING", "close_in_progress"
	}
	if o.service == nil || o.gate == nil || o.service.inboundHandshakes != o.gate {
		result.Available = false
		result.Status, result.Reason = "UNKNOWN", "file_gate_unavailable"
	}
	return result
}
func (o *HandshakeFileObservation) Err() error {
	if o == nil {
		return nil
	}
	o.statusMu.Lock()
	defer o.statusMu.Unlock()
	return o.lastErr
}
func (o *HandshakeFileObservation) publish(result HandshakeFileObservationResult, err error) {
	o.statusMu.Lock()
	o.last, o.lastErr = result, err
	o.statusMu.Unlock()
}
func (o *HandshakeFileObservation) sourceState() (string, string) {
	o.gate.mu.Lock()
	defer o.gate.mu.Unlock()
	source := o.gate.fullObservation
	if source == nil || source.segment == nil {
		return "UNKNOWN", "file_source_unavailable"
	}
	if source.invalid != "" {
		return "UNKNOWN", source.invalid
	}
	if source.reads != 0 || source.pending != 0 {
		return "IN_FLIGHT", "unvalidated_read_or_row"
	}
	return "PAST_CHECKPOINT_ONLY", ""
}
func fileCheckpoint(q handshakeSegmentDescriptor, session [16]byte) HandshakeFileCheckpoint {
	project := func(row handshakeFullTransition) HandshakeObservationTransition {
		return HandshakeObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}
	}
	return HandshakeFileCheckpoint{Source: HandshakeObservationSource{Anchor: q.key.anchor, Epoch: q.key.epoch, ClockDomain: q.key.clockDomain, Configuration: q.key.configuration, Kind: uint8(q.key.kind)}, Bounds: HandshakeObservationBounds{Node: q.bounds.node, Loopback: q.bounds.loopback, PerSource: q.bounds.perSource}, CoverageStart: q.coverageStart, Session: session, ID: q.checkpointID, PreviousID: q.previousID, PreviousDigest: q.previousDigest, FromSequence: q.fromSeq, ThroughSequence: q.throughSeq, FromAt: q.fromAt, ThroughAt: q.throughAt, Rows: q.rowCount, FromFull: q.fromCarry.full, ThroughFull: q.throughCarry.full, FromEntry: project(q.fromCarry.entry), ThroughEntry: project(q.throughCarry.entry)}
}

// Capture is a caller-driven single bounded operation: source cut -> real file
// commit -> original source ACK/DEFER. A committed past Q does not hide a later
// source gap. Files remain retained after retirement; no collector is implied.
func (o *HandshakeFileObservation) Capture() (HandshakeFileObservationResult, error) {
	result := HandshakeFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}
	if o == nil {
		return result, nil
	}
	if o.closing.Load() {
		result.Status, result.Reason = "UNKNOWN", "observation_closed"
		result.Closing, result.Closed = true, o.closed.Load()
		return result, errHandshakeStoreClosed
	}
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closing.Load() {
		result.Status, result.Reason = "UNKNOWN", "observation_closed"
		result.Closing, result.Closed = true, o.closed.Load()
		return result, errHandshakeStoreClosed
	}
	if o.service == nil || o.gate == nil || o.issuer == nil || o.service.inboundHandshakes != o.gate || o.issuer.gate != o.gate || o.issuer.owner != o.gate.fullObservation {
		result.Status, result.Reason = "UNKNOWN", "file_gate_unavailable"
		o.publish(result, errHandshakeCommitUnconfirmed)
		return result, errHandshakeCommitUnconfirmed
	}
	result.Available = true
	// Binding/readiness only, not a whole-history health refresh. This mutex
	// and directory check are outside Service/gate locks. Offered Q may still
	// strictly recover an indeterminate initialization through Commit below.
	o.issuer.mu.Lock()
	bindErr := o.issuer.boundLocked()
	identityReady := o.issuer.identityConfirmed
	o.issuer.mu.Unlock()
	if bindErr != nil {
		result.Status, result.Reason = "UNKNOWN", "file_source_unavailable"
		o.publish(result, bindErr)
		return result, bindErr
	}
	q, status := o.gate.sealHandshakeSegment()
	result.SourceStatus, result.SourceReason = o.sourceState()
	if status != "OFFERED" {
		result.Status, result.Reason = "UNKNOWN", status
		if status == "NO_PROGRESS" && !identityReady {
			result.Reason = "initial_file_confirmation_unknown"
			o.publish(result, errHandshakeCommitUnconfirmed)
			return result, errHandshakeCommitUnconfirmed
		}
		if status == "NO_PROGRESS" {
			result.Status, result.Reason = "NO_PROGRESS", "same_committed_cut"
		}
		var err error
		if status != "NO_PROGRESS" {
			err = errHandshakeCommitUnconfirmed
		}
		o.publish(result, err)
		return result, err
	}
	result.Checkpoint = fileCheckpoint(q, o.session)
	receipt, outcome, err := o.issuer.Commit(q)
	result.CommitOutcome = outcome
	if err != nil || outcome != secretfile.DurableCommitted {
		result.Status, result.Reason = "UNKNOWN", "file_commit_unconfirmed"
		if err == nil {
			err = errHandshakeCommitUnconfirmed
		}
		o.publish(result, err)
		return result, err
	}
	result.Digest = receipt.witness.digest
	result.AckStatus, result.AckReason = o.gate.acknowledgeHandshakeSegment(receipt)
	result.SourceStatus, result.SourceReason = o.sourceState()
	switch result.AckStatus {
	case "RETIRED", "ALREADY_RETIRED":
		result.Status, result.Reason = "COMMITTED_PAST", "exact_checkpoint_retired"
	case "DEFERRED":
		result.Status, result.Reason = "DEFERRED", "source_not_quiescent"
	default:
		result.Status, result.Reason = "UNKNOWN", "source_ack_refused"
		err = errHandshakeCommitUnconfirmed
	}
	o.publish(result, err)
	return result, err
}

// Close fences new Capture immediately and waits for the current synchronous
// operation OUTSIDE Service/gate locks, then closes issuer/lease only. It never
// closes the borrowed Dir or stops admission. Service.Run/App.Run call this on
// normal returns and panic unwinding; callers close an unstarted handle
// explicitly. log.Fatal/os.Exit bypass Go defers and are not covered.
// A kernel/filesystem call may block: no hard close-latency claim. Automatic
// close errors remain in Status/Err, preserving original startup/Run policy.
func (o *HandshakeFileObservation) Close() error {
	if o == nil {
		return nil
	}
	o.closing.Store(true)
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closed.Load() {
		return o.closeErr
	}
	if o.issuer != nil {
		o.closeErr = o.issuer.Close()
	}
	o.closed.Store(true)
	o.statusMu.Lock()
	o.last.SchemaVersion = 2
	o.last.PastOnly = true
	o.last.Closing, o.last.Closed = true, true
	if o.closeErr != nil {
		o.last.Status, o.last.Reason = "UNKNOWN", "file_close_failed"
		o.lastErr = o.closeErr
	} else {
		o.last.Status, o.last.Reason = "CLOSED", "observation_closed"
	}
	o.statusMu.Unlock()
	return o.closeErr
}
func (s *Service) CloseHandshakeFileObservation() error {
	if s == nil {
		return nil
	}
	return s.handshakeFileObservation.Close()
}

type handshakeFileObservationConstruction struct {
	request HandshakeFileObservationRequest
	clock   func() (time.Duration, error)
	readout *HandshakeFileObservation
	legacy  *HandshakeObservation
}

func prepareHandshakeFileObservation(request HandshakeFileObservationRequest, owner domain.PeerIdentity, clock func() (time.Duration, error)) *handshakeFileObservationConstruction {
	return &handshakeFileObservationConstruction{request: request, clock: clock, readout: &HandshakeFileObservation{key: request.Key, identity: owner, session: request.Session, last: HandshakeFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "not_constructed", PastOnly: true}}, legacy: &HandshakeObservation{key: request.Key, identity: owner}}
}
func (c *handshakeFileObservationConstruction) newFileGate(s *Service, bounds handshakeBounds) *inboundHandshakes {
	request := c.request
	key := request.Key
	caps := request.Limits
	gate, issuer, err := newFileObservedInboundHandshakes(bounds, k5SourceKey{anchor: key.Anchor, epoch: key.Epoch, clockDomain: key.ClockDomain, configuration: key.Configuration, kind: k5HandshakeNode}, request.Capacity, request.ChunkCapacity, c.clock, request.Directory, handshakeFileLimits{recordBytes: caps.RecordBytes, storeBytes: caps.StoreBytes, orphanBytes: caps.OrphanBytes, scanBytes: caps.ScanBytes, storeRecords: caps.StoreRecords, orphanRecords: caps.OrphanRecords, scanBatch: caps.ScanBatch, scanSteps: caps.ScanSteps}, request.Session)
	c.readout.service, c.readout.gate, c.readout.issuer = s, gate, issuer
	c.legacy.service, c.legacy.gate = s, gate
	s.handshakeFileObservation = c.readout
	result := HandshakeFileObservationResult{SchemaVersion: 2, Available: true, Status: "READY", Reason: "no_checkpoint_captured", PastOnly: true}
	if err != nil {
		result.Status, result.Reason = "UNKNOWN", "initial_file_source_unavailable"
	}
	c.readout.publish(result, err)
	return gate
}
func NewServiceWithHandshakeFileObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, request HandshakeFileObservationRequest) (*Service, *HandshakeFileObservation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{HandshakeFile: &request})
	return s, readouts.HandshakeFile, err
}
