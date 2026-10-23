package domain

import "time"

// PendingQueueTimeBounds brackets a real owner boundary in one validated
// monotonic duration domain. Zero values are not evidence without validity.
type PendingQueueTimeBounds struct {
	Earliest time.Duration
	Latest   time.Duration
}

// PendingQueueDurationBounds bounds a duration; the two extrema can be
// witnessed by different runs. It is not a selected acceptance threshold.
type PendingQueueDurationBounds struct {
	Lower time.Duration
	Upper time.Duration
}

// PendingQueueObservationSnapshot is an inactive candidate readout for the
// exact Service.pending frame-slot scope, never pendingKeys or transport queues.
// It covers only this source's atomic baseline through End. It cannot reconstruct
// arbitrary later subwindows. SourceID is not artifact/boot/process attribution;
// AttributionBound is false until that external provenance is actually verified.
// An unavailable or invalid interval is UNKNOWN, never an observed zero.
type PendingQueueObservationSnapshot struct {
	Available         bool
	SchemaVersion     uint32
	SourceID          [16]byte
	AttributionBound  bool
	Sequence          uint64
	Count             int64
	CountValid        bool
	CoverageValid     bool
	DurationValid     bool
	UnknownReason     string
	Baseline          PendingQueueTimeBounds
	End               PendingQueueTimeBounds
	RunStart          PendingQueueTimeBounds
	RunStartCount     int64
	RunHasIncrease    bool
	HasGrowingRun     bool
	LongestGrowingRun PendingQueueDurationBounds
}
