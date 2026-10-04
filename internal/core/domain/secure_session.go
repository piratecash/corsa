package domain

import "time"

// SecureSessionStoreStats is the state of a node's v2 downgrade-protection
// store as diagnostics see it (docs/protocol/session_v2.md, "Downgrade
// protection").
type SecureSessionStoreStats struct {
	// ReadAt is when the figures were read.
	ReadAt time.Time
	// PinnedIdentities is how many identities are pinned to v2.
	PinnedIdentities int
	// PinCapacity is the bound on pinned identities.
	PinCapacity int
	// Full reports that no NEW identity can be pinned: a v2 session of an
	// identity not pinned yet is refused with pin_store_full. Existing pins
	// are kept and keep being served.
	Full bool
	// PinRefusalsStoreFull counts v2 sessions refused because their
	// mandatory pin could not be stored (pin_store_full), in either
	// direction, since the process started.
	PinRefusalsStoreFull uint64
	// Unreadable reports that the store file exists but could not be read:
	// every peer is then treated as pinned and v1 is refused.
	Unreadable bool
}
