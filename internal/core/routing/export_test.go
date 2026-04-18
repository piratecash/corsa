package routing

// SetNeedsFullResyncForTest sets needsFullResync on an AnnouncePeerState.
// Test-only: provides access to the unexported field for package routing_test.
func (s *AnnouncePeerState) SetNeedsFullResyncForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
}
