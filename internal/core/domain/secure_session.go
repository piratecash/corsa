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
	// V1RefusalsIdentityPinned counts v1 sessions refused because the
	// identity they name requires v2 — pinned, or holding a live v2
	// connection — on accept and on dial, since the process started.
	V1RefusalsIdentityPinned uint64
	// V1RefusalsEndpointBound counts v1 answers refused on dial because the
	// endpoint that gave them is bound to v2 — a downgrade, not followed —
	// since the process started.
	V1RefusalsEndpointBound uint64
	// Unreadable reports that the store file exists but could not be read:
	// every peer is then treated as pinned and v1 is refused.
	Unreadable bool
}
