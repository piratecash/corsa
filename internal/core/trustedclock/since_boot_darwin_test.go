//go:build darwin

package trustedclock

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// On darwin the clock since boot agrees with what the kernel says about the
// boot: the wall clock now minus kern.boottime, which the kernel moves along
// with every step of the wall clock, so the difference is the time since boot
// with sleep included — the property the trusted time relies on.
//
// It proves that only on a machine that has slept since boot. On one that
// never slept, CLOCK_UPTIME_RAW (which stops in sleep) would agree just as
// well, so there the test confirms the origin and the rate, and that
// CLOCK_MONOTONIC keeps running through sleep rests on Apple's documentation
// of clock_gettime alone; a sleep cannot be made from a test.
func TestSinceBootAgreesWithKernBoottime(t *testing.T) {
	t.Parallel()
	const tolerance = time.Second
	reading := sinceBootOrSkip(t)
	booted, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		t.Fatalf("kern.boottime: %v", err)
	}
	sinceBoot := time.Now().Round(0).Sub(time.Unix(booted.Unix()))
	if diff := (reading - sinceBoot).Abs(); diff > tolerance {
		t.Fatalf("SinceBoot = %v, the wall clock minus kern.boottime = %v: off by %v", reading, sinceBoot, diff)
	}
}
