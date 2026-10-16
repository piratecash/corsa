//go:build linux

package trustedclock

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// On Linux and Android the clock since boot agrees with /proc/uptime, the
// kernel's own figure of the time since boot with suspend included — the
// property the trusted time relies on.
func TestSinceBootAgreesWithProcUptime(t *testing.T) {
	t.Parallel()
	const tolerance = time.Second
	reading := sinceBootOrSkip(t)
	raw, err := os.ReadFile("/proc/uptime")
	if err != nil {
		t.Skipf("/proc/uptime cannot be read here: %v", err)
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		t.Fatalf("/proc/uptime is empty")
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		t.Fatalf("/proc/uptime %q: %v", fields[0], err)
	}
	uptime := time.Duration(seconds * float64(time.Second))
	if diff := (reading - uptime).Abs(); diff > tolerance {
		t.Fatalf("SinceBoot = %v, /proc/uptime = %v: off by %v", reading, uptime, diff)
	}
}
