package connbudget

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Explicit caller bounds; no deployment policy or physical fit is selected.
type TotalFileObservationLimits struct {
	RecordBytes                                       int
	StoreBytes, OrphanBytes, ScanBytes                int64
	StoreRecords, OrphanRecords, ScanBatch, ScanSteps int
}
type TotalFileCheckpoint struct {
	Source                        TotalObservationKey
	Config                        Config
	CoverageStart                 time.Duration
	Session                       [16]byte
	ID, PreviousID                uint64
	PreviousDigest                [32]byte
	FromSequence, ThroughSequence uint64
	FromAt, ThroughAt             time.Duration
	Rows                          int
	FromFull, ThroughFull         bool
	FromEntry, ThroughEntry       TotalObservationTransition
}

// Bounded schema2 metadata for one finite PAST checkpoint, never row export,
// UTC/load attribution, whole-run coverage or a filesystem health certificate.
type TotalFileObservationResult struct {
	SchemaVersion                                          uint32
	Available, AttributionBound, PastOnly, Closing, Closed bool
	Status, Reason, SourceStatus, SourceReason             string
	Checkpoint                                             TotalFileCheckpoint
	Digest                                                 [32]byte
	CommitOutcome                                          secretfile.DurableCommit
	AckStatus, AckReason                                   string
}

// The opaque issuer/proof never leaves connbudget. This consumer owns the
// issuer lease, NOT Directory; the caller keeps Directory open and excludes
// its own writes through Close. No worker, clock cadence or hard FS-latency
// bound. Do not copy a handle after first use.
type TotalFileObservation struct {
	budget          *Budget
	issuer          *totalFileIssuer
	legacy          *TotalObservation
	session         [16]byte
	opMu            sync.Mutex
	closing, closed atomic.Bool
	closeErr        error
	statusMu        sync.Mutex
	last            TotalFileObservationResult
	lastErr         error
}

// Selects the original Budget once before publication. Only original invalid
// Config errors are returned as constructor errors; backend errors are kept
// on the actual handle as UNKNOWN/Err, preserving original admission policy.
// All defaults/V1 constructors are unchanged. Directory is borrowed.
func NewWithTotalFileObservation(cfg Config, key TotalObservationKey, capacity, chunkCapacity int, clock func() (time.Duration, error), directory *secretfile.Dir, limits TotalFileObservationLimits, session [16]byte) (*Budget, *TotalFileObservation, error) {
	b, issuer, err := newFileSegmentedBudget(cfg, totalSourceKey{anchor: key.Anchor, epoch: key.Epoch, clockDomain: key.ClockDomain, configuration: key.Configuration}, capacity, chunkCapacity, clock, directory, totalFileLimits{recordBytes: limits.RecordBytes, storeBytes: limits.StoreBytes, orphanBytes: limits.OrphanBytes, scanBytes: limits.ScanBytes, storeRecords: limits.StoreRecords, orphanRecords: limits.OrphanRecords, scanBatch: limits.ScanBatch, scanSteps: limits.ScanSteps}, session)
	if b == nil {
		return nil, nil, err
	}
	o := &TotalFileObservation{budget: b, issuer: issuer, legacy: &TotalObservation{budget: b}, session: session, last: TotalFileObservationResult{SchemaVersion: 2, Available: true, PastOnly: true, Status: "READY", Reason: "no_checkpoint_captured"}, lastErr: err}
	if b.fullObservation.disabled {
		o.last.Status, o.last.Reason, o.last.SourceStatus, o.last.SourceReason = "DISABLED", "total-disabled", "DISABLED", "total-disabled"
	} else if err != nil {
		o.last.Status, o.last.Reason = "UNKNOWN", "initial_file_source_unavailable"
	}
	o.last.SourceStatus, o.last.SourceReason = o.sourceState()
	return b, o, nil
}

// LegacyObservation is the immutable V1 readout for this exact Budget. Enabled
// V2 refuses before prefix copy; it never certifies a recycled suffix.
func (o *TotalFileObservation) LegacyObservation() *TotalObservation {
	if o == nil {
		return nil
	}
	return o.legacy
}
func (o *TotalFileObservation) Status() TotalFileObservationResult {
	if o == nil {
		return TotalFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}
	}
	o.statusMu.Lock()
	result := o.last
	o.statusMu.Unlock()
	result.Closing, result.Closed = o.closing.Load(), o.closed.Load()
	if result.Closing && !result.Closed {
		result.Status, result.Reason = "CLOSING", "close_in_progress"
	}
	return result
}
func (o *TotalFileObservation) Err() error {
	if o == nil {
		return nil
	}
	o.statusMu.Lock()
	defer o.statusMu.Unlock()
	return o.lastErr
}
func (o *TotalFileObservation) publish(result TotalFileObservationResult, err error) {
	o.statusMu.Lock()
	o.last, o.lastErr = result, err
	o.statusMu.Unlock()
}
func (o *TotalFileObservation) sourceState() (string, string) {
	o.budget.mu.Lock()
	defer o.budget.mu.Unlock()
	s := o.budget.fullObservation
	if s == nil {
		return "UNKNOWN", "file_source_unavailable"
	}
	if s.disabled {
		return "DISABLED", "total-disabled"
	}
	if s.segment == nil {
		return "UNKNOWN", "file_source_unavailable"
	}
	if s.invalid != "" {
		return "UNKNOWN", s.invalid
	}
	if s.reads != 0 || s.pending != 0 {
		return "IN_FLIGHT", "unvalidated_read_or_row"
	}
	return "PAST_CHECKPOINT_ONLY", ""
}
func totalFileCheckpoint(q totalSegmentDescriptor, session [16]byte) TotalFileCheckpoint {
	project := func(r totalFullTransition) TotalObservationTransition {
		return TotalObservationTransition{Sequence: r.sequence, Full: r.full, Count: r.count, Lower: r.lower, Upper: r.upper, Done: r.done, Valid: r.valid}
	}
	return TotalFileCheckpoint{Source: TotalObservationKey{Anchor: q.key.anchor, Epoch: q.key.epoch, ClockDomain: q.key.clockDomain, Configuration: q.key.configuration}, Config: q.cfg, CoverageStart: q.coverageStart, Session: session, ID: q.checkpointID, PreviousID: q.previousID, PreviousDigest: q.previousDigest, FromSequence: q.fromSeq, ThroughSequence: q.throughSeq, FromAt: q.fromAt, ThroughAt: q.throughAt, Rows: q.rowCount, FromFull: q.fromCarry.full, ThroughFull: q.throughCarry.full, FromEntry: project(q.fromCarry.entry), ThroughEntry: project(q.throughCarry.entry)}
}
func (o *TotalFileObservation) finishAck(result TotalFileObservationResult, status, reason string) (TotalFileObservationResult, error) {
	result.AckStatus, result.AckReason = status, reason
	result.SourceStatus, result.SourceReason = o.sourceState()
	var err error
	switch status {
	case "RETIRED", "ALREADY_RETIRED":
		result.Status, result.Reason = "COMMITTED_PAST", "exact_checkpoint_retired"
	case "DEFERRED":
		result.Status, result.Reason = "DEFERRED", "source_not_quiescent"
	default:
		result.Status, result.Reason = "UNKNOWN", "source_ack_refused"
		err = errTotalCommitUnconfirmed
	}
	o.publish(result, err)
	return result, err
}

// Capture is caller-driven: one source checkpoint, actual store confirmation,
// and owner ACK. Once ACK installed a verified pending proof, later Capture
// uses ONLY owner retirement (no Commit/verifier/history IO), retaining its
// exact Q/digest and reporting any later source gap separately.
func (o *TotalFileObservation) Capture() (TotalFileObservationResult, error) {
	result := TotalFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}
	if o == nil {
		return result, nil
	}
	if o.closing.Load() {
		result.Status, result.Reason = "UNKNOWN", "observation_closed"
		result.Closing, result.Closed = true, o.closed.Load()
		return result, errTotalStoreClosed
	}
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closing.Load() {
		result.Status, result.Reason = "UNKNOWN", "observation_closed"
		result.Closing, result.Closed = true, o.closed.Load()
		return result, errTotalStoreClosed
	}
	if o.budget == nil || o.issuer == nil || o.issuer.gate != o.budget || o.issuer.owner != o.budget.fullObservation {
		result.Status, result.Reason = "UNKNOWN", "file_budget_unavailable"
		o.publish(result, errTotalCommitUnconfirmed)
		return result, errTotalCommitUnconfirmed
	}
	result.Available = true
	result.SourceStatus, result.SourceReason = o.sourceState()
	// Total0 has deliberately no lease, source clock, ring or storage work.
	if o.budget.fullObservation.disabled {
		result.Status, result.Reason, result.SourceStatus, result.SourceReason = "DISABLED", "total-disabled", "DISABLED", "total-disabled"
		o.publish(result, nil)
		return result, nil
	}
	o.budget.mu.Lock()
	source := o.budget.fullObservation
	segment := source.segment
	var pending bool
	var q totalSegmentDescriptor
	var digest [32]byte
	if segment != nil && segment.verifiedPending {
		pending = true
		q = segment.q
		digest = segment.qDigest
	}
	o.budget.mu.Unlock()
	if pending {
		result.Checkpoint = totalFileCheckpoint(q, o.session)
		result.Digest = digest
		result.CommitOutcome = secretfile.DurableCommitted
		status, reason := o.budget.retireVerifiedTotalSegment()
		return o.finishAck(result, status, reason)
	}
	// Binding/readiness only; NO_PROGRESS must not manufacture FS health.
	o.issuer.mu.Lock()
	bindErr := o.issuer.boundLocked()
	identityReady := o.issuer.identityConfirmed
	o.issuer.mu.Unlock()
	if bindErr != nil {
		result.Status, result.Reason = "UNKNOWN", "file_source_unavailable"
		o.publish(result, bindErr)
		return result, bindErr
	}
	q, status := o.budget.sealTotalSegment()
	result.SourceStatus, result.SourceReason = o.sourceState()
	if status != "OFFERED" {
		result.Status, result.Reason = "UNKNOWN", status
		if status == "NO_PROGRESS" && !identityReady {
			result.Reason = "initial_file_confirmation_unknown"
			o.publish(result, errTotalCommitUnconfirmed)
			return result, errTotalCommitUnconfirmed
		}
		if status == "NO_PROGRESS" {
			result.Status, result.Reason = "NO_PROGRESS", "same_committed_cut"
		}
		var err error
		if status != "NO_PROGRESS" {
			err = errTotalCommitUnconfirmed
		}
		o.publish(result, err)
		return result, err
	}
	result.Checkpoint = totalFileCheckpoint(q, o.session)
	receipt, outcome, err := o.issuer.Commit(q)
	result.CommitOutcome = outcome
	if err != nil || outcome != secretfile.DurableCommitted {
		result.Status, result.Reason = "UNKNOWN", "file_commit_unconfirmed"
		if err == nil {
			err = errTotalCommitUnconfirmed
		}
		o.publish(result, err)
		return result, err
	}
	result.Digest = receipt.witness.digest
	ack, reason := o.budget.acknowledgeTotalSegment(receipt)
	return o.finishAck(result, ack, reason)
}

// Close fences before joining active Capture, outside Budget/Service locks.
// No final Capture/GC/clock or admission stop. Borrowed Directory is not closed.
// Service/App normal return and panic unwinding can defer this; os.Exit/Fatal
// bypass Go defers. An FS call may block: no hard Close-latency guarantee.
func (o *TotalFileObservation) Close() error {
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
