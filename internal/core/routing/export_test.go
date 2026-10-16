package routing

import "context"

// SetNeedsFullResyncForTest sets needsFullResync on an AnnouncePeerState.
// Test-only: provides access to the unexported field for package routing_test.
func (s *AnnouncePeerState) SetNeedsFullResyncForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
}

// AnnounceTriggeredCycleForTest runs one triggered cycle synchronously, the
// same cycle Run starts on TriggerUpdate. Test-only: a test that starts Run
// and sleeps cannot know the cycle finished, and under load it often has not.
func (a *AnnounceLoop) AnnounceTriggeredCycleForTest(ctx context.Context) {
	a.announceTriggeredCycle(ctx)
}
