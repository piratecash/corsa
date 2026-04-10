package connauth

import "github.com/piratecash/corsa/internal/core/protocol"

// State holds the authentication state for a single inbound connection.
// Instances are immutable after creation — the pointer is swapped
// atomically via AuthStore.SetConnAuthState, never mutated in place.
// This enables snapshot-based concurrent reads without additional locking.
type State struct {
	Hello     protocol.Frame
	Challenge string
	Verified  bool
}
