package desktop

import "time"

// retryWithBackoff calls fn up to maxAttempts times. If fn returns a non-nil
// result, retryWithBackoff returns it immediately. Between attempts it sleeps
// for initialBackoff, doubling the delay each time.
//
// This is used by loadPreviews to handle transient failures during startup,
// when the local node may not be fully ready yet.
func retryWithBackoff[T any](maxAttempts int, initialBackoff time.Duration, fn func() (T, bool)) (T, bool) {
	backoff := initialBackoff
	var zero T

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, ok := fn()
		if ok {
			return result, true
		}
		if attempt < maxAttempts {
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	return zero, false
}
