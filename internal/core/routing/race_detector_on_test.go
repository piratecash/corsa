//go:build race

package routing

// raceDetectorEnabled reports whether this binary was built with -race.
//
// It exists for the heap-delta measurements in
// announce_retention_footprint_test.go, and for nothing else. The race
// detector keeps shadow state alongside every allocation and changes when
// memory is released, so a "bytes retained across a GC" reading taken under it
// measures the instrumentation as much as the structure: the same shape that
// weighs 7 KB in a normal build reads as megabytes here, and the difference
// between two shapes stops being the signal.
//
// This is a BUILD FACT, not a guess about the platform — the two files are
// selected by the toolchain from the flags it was actually given, which is why
// it is written this way rather than as a condition somebody predicted.
const raceDetectorEnabled = true
