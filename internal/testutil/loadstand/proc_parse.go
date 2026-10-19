package loadstand

import (
	"bytes"
	"math"
	"math/bits"
	"strconv"
	"strings"
)

type processStat struct {
	ID           ProcessIncarnation
	User, System ProcessTicks
}

func parseProcessStat(raw []byte) (processStat, error) {
	open, close := bytes.IndexByte(raw, '('), bytes.LastIndexByte(raw, ')')
	if open < 1 || close <= open || close+1 >= len(raw) || raw[close+1] != ' ' {
		return processStat{}, ErrProcessObservation
	}
	pid, err := strconv.ParseUint(strings.TrimSpace(string(raw[:open])), 10, 31)
	if err != nil || pid == 0 {
		return processStat{}, ErrProcessObservation
	}
	fields := strings.Fields(string(raw[close+1:]))
	if len(fields) < 20 || len(fields[0]) != 1 || !strings.Contains("RSDTtIWPK", fields[0]) {
		return processStat{}, ErrProcessObservation
	}
	user, e1 := strconv.ParseUint(fields[11], 10, 64)
	system, e2 := strconv.ParseUint(fields[12], 10, 64)
	start, e3 := strconv.ParseUint(fields[19], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || start == 0 {
		return processStat{}, ErrProcessObservation
	}
	return processStat{ID: ProcessIncarnation{PID: ProcessID(pid), StartTicks: ProcessTicks(start)}, User: ProcessTicks(user), System: ProcessTicks(system)}, nil
}
func parseProcessMemory(raw []byte) ProcessMemory {
	missing := unavailableProcessMetric[ProcessBytes](ProcessMissing)
	out := ProcessMemory{RSS: missing, HighWaterRSS: missing, AnonymousRSS: missing}
	targets := map[string]*ProcessMetric[ProcessBytes]{"VmRSS:": &out.RSS, "VmHWM:": &out.HighWaterRSS, "RssAnon:": &out.AnonymousRSS}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		target, ok := targets[fields[0]]
		if !ok {
			continue
		}
		if seen[fields[0]] || len(fields) != 3 || fields[2] != "kB" {
			*target = unavailableProcessMetric[ProcessBytes](ProcessMalformed)
			seen[fields[0]] = true
			continue
		}
		seen[fields[0]] = true
		count, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || count > math.MaxUint64/1024 {
			*target = unavailableProcessMetric[ProcessBytes](ProcessMalformed)
			continue
		}
		*target = measuredProcessMetric(ProcessBytes(count * 1024))
	}
	return out
}
func processTickDuration(ticks ProcessTicks, frequency ProcessMetric[ProcessTickHz]) ProcessMetric[ProcessNanoseconds] {
	hz, err := frequency.Get()
	if err != nil || hz == 0 {
		return unavailableProcessMetric[ProcessNanoseconds](ProcessClockUnknown)
	}
	whole, remainder := uint64(ticks)/uint64(hz), uint64(ticks)%uint64(hz)
	if whole > math.MaxUint64/uint64(1000000000) {
		return unavailableProcessMetric[ProcessNanoseconds](ProcessLimitExceeded)
	}
	high, low := bits.Mul64(remainder, 1000000000)
	fraction, _ := bits.Div64(high, low, uint64(hz))
	value := whole * 1000000000
	if math.MaxUint64-value < fraction {
		return unavailableProcessMetric[ProcessNanoseconds](ProcessLimitExceeded)
	}
	return measuredProcessMetric(ProcessNanoseconds(value + fraction))
}
func processUIDMatches(raw []byte, expected ProcessUID) bool {
	seen := false
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "Uid:" {
			continue
		}
		if seen || len(fields) != 5 {
			return false
		}
		seen = true
		for _, field := range fields[1:] {
			value, err := strconv.ParseUint(field, 10, 32)
			if err != nil || ProcessUID(value) != expected {
				return false
			}
		}
	}
	return seen
}
func parseProcessScheduler(raw []byte, enabled bool) ProcessScheduler {
	out := unavailableProcessScheduler(ProcessMalformed)
	fields := strings.Fields(string(raw))
	if len(fields) != 3 {
		return out
	}
	values := [3]uint64{}
	for i, field := range fields {
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return out
		}
		values[i] = n
	}
	if !enabled {
		return unavailableProcessScheduler(ProcessSchedulerDisabled)
	}
	out.Runtime = measuredProcessMetric(ProcessNanoseconds(values[0]))
	out.RunQueue = measuredProcessMetric(ProcessNanoseconds(values[1]))
	out.TimeSlices = measuredProcessMetric(values[2])
	return out
}
func unavailableProcessScheduler(reason ProcessUnavailableReason) ProcessScheduler {
	return ProcessScheduler{Scope: ProcessSchedulerLeaderThread, Runtime: unavailableProcessMetric[ProcessNanoseconds](reason), RunQueue: unavailableProcessMetric[ProcessNanoseconds](reason), TimeSlices: unavailableProcessMetric[uint64](reason)}
}
