package domain

import "errors"

// ErrPinStoreRefused marks a node that refuses to start over its v2 pin store
// (docs/protocol/session_v2.md, "The pin store marker" and "Downgrade
// protection"): the store or its marker sits where someone else could change
// it, a store this node is known to have kept is missing or unreadable, or
// the node could not enforce the pins it keeps — its secure session cannot be
// built, or the build speaks v1 only.
// Starting anyway would mean starting on protection somebody else decides, or
// on none at all — every peer that proved v2 could be taken back to v1 — so
// the node does not start, and nothing is repaired, removed or replaced for
// it. Every refusal matches it through errors.Is; the reason is the
// refusal's PinStoreRefusalReason.
var ErrPinStoreRefused = errors.New("pin_store_refused")

// PinStoreRefusalReason is why the node refused to start over its pin store.
// Its text is the code operators search logs for.
type PinStoreRefusalReason string

const (
	// PinStoreRefusalIdentityPinRecovery requires trusted offline protection recovery.
	PinStoreRefusalIdentityPinRecovery PinStoreRefusalReason = "identity_pin_recovery"
	// PinStoreRefusalSupervisionRequired refuses bypass of an enrolled launcher.
	PinStoreRefusalSupervisionRequired PinStoreRefusalReason = "supervision_required"
	// PinStoreRefusalLocationUnsafe: the store, its marker, or a directory
	// on the way to either may be changed by someone other than the node's
	// user or root — a symlink in place of the file, a file that is not the
	// user's 0600, a directory others may write without the sticky bit.
	PinStoreRefusalLocationUnsafe PinStoreRefusalReason = "location_unsafe"
	// PinStoreRefusalLocationUnverifiable: the location audit could not
	// inspect a component, so the location is not known to be safe.
	PinStoreRefusalLocationUnverifiable PinStoreRefusalReason = "location_unverifiable"
	// PinStoreRefusalStoreMissing: the marker says this node kept a store,
	// and there is none where the node looks for it now — deleted, never
	// restored, or left behind by a changed port, store path or data
	// directory.
	PinStoreRefusalStoreMissing PinStoreRefusalReason = "store_missing"
	// PinStoreRefusalStoreUnreadable: the store file exists and cannot be
	// read or decoded — damaged, or written by a newer build.
	PinStoreRefusalStoreUnreadable PinStoreRefusalReason = "store_unreadable"
	// PinStoreRefusalMarkerUnwritable: a store exists and the marker that
	// would let a later start tell its loss from a first start cannot be
	// written.
	PinStoreRefusalMarkerUnwritable PinStoreRefusalReason = "marker_unwritable"
	// PinStoreRefusalTwoStores: the store loaded at the path the node uses
	// now, and the path the marker recorded still holds another store — a
	// copy, not a move. Which one is current is the operator's call.
	PinStoreRefusalTwoStores PinStoreRefusalReason = "two_stores"
	// PinStoreRefusalSecureSessionUnavailable: the build speaks the secure
	// session, and the node's v2 state cannot be built (the session mode,
	// the identity's proof or the certificate source failed). Running on v1
	// alone would not enforce a single pin.
	PinStoreRefusalSecureSessionUnavailable PinStoreRefusalReason = "secure_session_unavailable"
	// PinStoreRefusalLegacyOnlyBuild: the build speaks v1 only — a rollback
	// to a build without the secure session — and a pin store or its marker
	// is on disk: the pins it holds would not be enforced.
	PinStoreRefusalLegacyOnlyBuild PinStoreRefusalReason = "legacy_only_build"
)
