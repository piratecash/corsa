package runjournal

import "time"

// Clock is where a record's timestamp comes from. It is injected because the
// timestamp decides which of several failed attempts is reported as the latest,
// and a test has to be able to say which one that is.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the wall clock.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }
