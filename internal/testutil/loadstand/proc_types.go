package loadstand

import (
	"errors"
	"time"
)

var ErrProcessObservation = errors.New("process observation unavailable or incarnation changed")
var ErrProcessPlatform = errors.New("linux proc observation unavailable on this platform")

type ProcessUID uint32
type ProcessTicks uint64
type ProcessTickHz uint64
type ProcessBytes uint64
type ProcessNanoseconds uint64
type ProcessFDCount uint64
type ExecutableSHA256 string

type ProcessMetricState string

const (
	ProcessMetricMeasured    ProcessMetricState = "measured"
	ProcessMetricUnavailable ProcessMetricState = "unavailable"
)

type ProcessUnavailableReason string

const (
	ProcessMissing             ProcessUnavailableReason = "missing"
	ProcessPlatformUnsupported ProcessUnavailableReason = "platform_unsupported"
	ProcessUnreadable          ProcessUnavailableReason = "unreadable"
	ProcessMalformed           ProcessUnavailableReason = "malformed"
	ProcessLimitExceeded       ProcessUnavailableReason = "limit_exceeded"
	ProcessClockUnknown        ProcessUnavailableReason = "clock_frequency_unknown"
	ProcessSchedulerDisabled   ProcessUnavailableReason = "scheduler_statistics_disabled_or_unknown"
	ProcessNotWaitOwner        ProcessUnavailableReason = "not_wait_owner"
)

// ProcessMetric distinguishes a measured zero from an unavailable observation.
// Both state and the optional value travel in the public resource DTO.
type ProcessMetric[T any] struct {
	State  ProcessMetricState       `json:"state"`
	Value  *T                       `json:"value,omitempty"`
	Reason ProcessUnavailableReason `json:"reason,omitempty"`
}

func measuredProcessMetric[T any](value T) ProcessMetric[T] {
	return ProcessMetric[T]{State: ProcessMetricMeasured, Value: &value}
}
func unavailableProcessMetric[T any](reason ProcessUnavailableReason) ProcessMetric[T] {
	return ProcessMetric[T]{State: ProcessMetricUnavailable, Reason: reason}
}
func (m ProcessMetric[T]) Get() (T, error) {
	if m.State == ProcessMetricMeasured && m.Value != nil && m.Reason == "" {
		return *m.Value, nil
	}
	var zero T
	return zero, ErrProcessObservation
}

type ProcessIncarnation struct {
	PID        ProcessID    `json:"pid"`
	StartTicks ProcessTicks `json:"kernel_start_ticks"`
}
type LinuxProcessExpectation struct {
	Incarnation        ProcessIncarnation
	UID                ProcessUID
	ExecutableSHA256   ExecutableSHA256
	MaxExecutableBytes int64
}
type ProcessCPU struct {
	UserTicks         ProcessTicks                      `json:"user_ticks"`
	SystemTicks       ProcessTicks                      `json:"system_ticks"`
	Frequency         ProcessMetric[ProcessTickHz]      `json:"ticks_per_second"`
	UserNanoseconds   ProcessMetric[ProcessNanoseconds] `json:"user_nanoseconds"`
	SystemNanoseconds ProcessMetric[ProcessNanoseconds] `json:"system_nanoseconds"`
}
type ProcessMemory struct {
	RSS          ProcessMetric[ProcessBytes] `json:"rss_bytes"`
	HighWaterRSS ProcessMetric[ProcessBytes] `json:"high_water_rss_bytes"`
	AnonymousRSS ProcessMetric[ProcessBytes] `json:"anonymous_rss_bytes"`
}
type ProcessSchedulerScope string

const ProcessSchedulerLeaderThread ProcessSchedulerScope = "leader_thread"

type ProcessScheduler struct {
	Scope      ProcessSchedulerScope             `json:"scope"`
	Runtime    ProcessMetric[ProcessNanoseconds] `json:"runtime_nanoseconds"`
	RunQueue   ProcessMetric[ProcessNanoseconds] `json:"run_queue_nanoseconds"`
	TimeSlices ProcessMetric[uint64]             `json:"time_slices"`
}
type ProcessFinalUsage struct {
	UserNanoseconds   ProcessNanoseconds `json:"user_nanoseconds"`
	SystemNanoseconds ProcessNanoseconds `json:"system_nanoseconds"`
	HighWaterRSS      ProcessBytes       `json:"high_water_rss_bytes"`
}
type LinuxProcessProvenance struct {
	Incarnation               ProcessIncarnation `json:"incarnation"`
	UID                       ProcessUID         `json:"uid"`
	RunningExecutableSHA256   ExecutableSHA256   `json:"running_executable_sha256"`
	RunningExecutableVerified bool               `json:"running_executable_verified"`
	KernelStartTimeAvailable  bool               `json:"kernel_start_time_available"`
}
type LinuxProcessSample struct {
	Provenance      LinuxProcessProvenance           `json:"provenance"`
	ReadStartedAt   time.Time                        `json:"read_started_at"`
	ReadCompletedAt time.Time                        `json:"read_completed_at"`
	CPU             ProcessCPU                       `json:"cpu"`
	Memory          ProcessMemory                    `json:"memory"`
	Descriptors     ProcessMetric[ProcessFDCount]    `json:"descriptors"`
	Scheduler       ProcessScheduler                 `json:"scheduler"`
	FinalWait4      ProcessMetric[ProcessFinalUsage] `json:"final_wait4"`
}
