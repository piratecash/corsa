package loadstand

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func procStatFixture(pid int, comm string, start uint64) string {
	fields := make([]string, 50)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[11] = "7"
	fields[12] = "3"
	fields[19] = fmt.Sprint(start)
	return fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " "))
}
func TestProcStatParsingBindsPIDAndLastCommDelimiter(t *testing.T) {
	raw := procStatFixture(42, "a ) strange (name)", 99)
	sample, err := parseProcessStat([]byte(raw))
	if err != nil || sample.ID.PID != 42 || sample.ID.StartTicks != 99 || sample.User != 7 || sample.System != 3 {
		t.Fatalf("valid comm/units lost: %v %+v", err, sample)
	}
	for _, raw := range []string{"42 x S", "42 (name) S 0", strings.Replace(procStatFixture(42, "ok", 99), " S ", " Z ", 1), strings.Replace(procStatFixture(42, "ok", 99), " 7 ", " -1 ", 1), procStatFixture(0, "ok", 99), procStatFixture(42, "ok", 0)} {
		if _, err := parseProcessStat([]byte(raw)); !errors.Is(err, ErrProcessObservation) {
			t.Fatalf("malformed/dead stat accepted: %v", err)
		}
	}
}
func TestProcMemoryMissingAndUnitsNeverBecomeMeasuredZero(t *testing.T) {
	sample := parseProcessMemory([]byte("VmRSS:\t0 kB\nVmHWM:\t12 kB\nRssAnon:\t3 kB\n"))
	rss, err := sample.RSS.Get()
	if err != nil || rss != 0 {
		t.Fatal("supported zero lost")
	}
	high, err := sample.HighWaterRSS.Get()
	if err != nil || high != 12288 {
		t.Fatal("kB unit conversion wrong")
	}
	for _, raw := range []string{"", "VmRSS: 7 B\n", "VmRSS: -1 kB\n", "VmRSS: 1 kB\nVmRSS: 2 kB\n", "VmRSS: 18446744073709551615 kB\n"} {
		if _, err := parseProcessMemory([]byte(raw)).RSS.Get(); err == nil {
			t.Fatal("missing/bad RSS became measured")
		}
	}
}
func TestProcessTicksNeedKnownFrequencyAndCheckedUnits(t *testing.T) {
	if _, err := processTickDuration(1, unavailableProcessMetric[ProcessTickHz](ProcessClockUnknown)).Get(); err == nil {
		t.Fatal("unknown HZ guessed")
	}
	v, err := processTickDuration(3, measuredProcessMetric(ProcessTickHz(250))).Get()
	if err != nil || v != 12000000 {
		t.Fatalf("ticks scaled incorrectly: %d %v", v, err)
	}
	if _, err := processTickDuration(^ProcessTicks(0), measuredProcessMetric(ProcessTickHz(1))).Get(); err == nil {
		t.Fatal("duration overflow accepted")
	}
}

func TestProcUIDAndSchedulerAvailabilityAreExplicit(t *testing.T) {
	if !processUIDMatches([]byte("Name: x\nUid: 10001 10001 10001 10001\n"), 10001) {
		t.Fatal("valid sameUID lost")
	}
	for _, raw := range []string{"", "Uid: 10001 10001 0 10001\n", "Uid: 10001 10001 10001\n", "Uid: 10001 10001 10001 10001\nUid: 10001 10001 10001 10001\n"} {
		if processUIDMatches([]byte(raw), 10001) {
			t.Fatal("partial/mismatched UID accepted")
		}
	}
	sample := parseProcessScheduler([]byte("0 0 0\n"), false)
	if sample.Scope != ProcessSchedulerLeaderThread {
		t.Fatal("scheduler scope lost")
	}
	if _, err := sample.Runtime.Get(); err == nil {
		t.Fatal("disabled kernel stub became measured zero")
	}
	if _, err := sample.RunQueue.Get(); err == nil {
		t.Fatal("unknown queue became zero")
	}
	sample = parseProcessScheduler([]byte("0 0 0\n"), true)
	if n, err := sample.Runtime.Get(); err != nil || n != 0 {
		t.Fatal("explicit supported zero lost")
	}
	for _, raw := range []string{"", "1 2", "1 2 3 4", "-1 2 3"} {
		if _, err := parseProcessScheduler([]byte(raw), true).Runtime.Get(); err == nil {
			t.Fatal("malformed scheduler became available")
		}
	}
	if _, err := (ProcessMetric[ProcessBytes]{}).Get(); err == nil {
		t.Fatal("uninitialised metric became measured zero")
	}
}
