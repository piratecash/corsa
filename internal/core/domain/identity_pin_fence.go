package domain

import "time"

// IdentityPinStoreID identifies the externally enrolled pin store.
type IdentityPinStoreID string

// IdentityPinRuntimeEpoch names one supervised process, not its preview secret.
type IdentityPinRuntimeEpoch string

// IdentityPinFenceGeneration is the broker's compare-and-swap generation.
type IdentityPinFenceGeneration uint64

// IdentityPinProofRevision is the legacy wire name for accepted pin-protection
// evidence ordering in one running store, including historical reconstruction.
// It does not prove a TLS session or confer ProvenIdentity authority.
type IdentityPinProofRevision uint64

// IdentityPinSnapshotDigest identifies exactly the main-file bytes saved.
type IdentityPinSnapshotDigest string

// IdentityPinStoreIdentity binds the store to its enrolled node.
type IdentityPinStoreIdentity struct {
	Node  PeerIdentity
	Store IdentityPinStoreID
}

// IdentityPinFenceBinding is the protection the external fence must retain.
type IdentityPinFenceBinding struct {
	Scope            IdentityPinStoreIdentity
	RuntimeEpoch     IdentityPinRuntimeEpoch
	Generation       IdentityPinFenceGeneration
	Identity         PeerIdentity
	ProofRevision    IdentityPinProofRevision
	PreviousPinnedAt time.Time
	Backup           IdentityPinBackup
}

// IdentityPinRemovalPermit proves BeginRemoval fenced this exact operation.
// It grants no permission to restart or to disable any other v2 requirement.
type IdentityPinRemovalPermit struct {
	OperationID IdentityPinOperationID
	Binding     IdentityPinFenceBinding
	Generation  IdentityPinFenceGeneration
}

// IdentityPinReceiptKind describes a local persistence result to the fence.
type IdentityPinReceiptKind string

const (
	IdentityPinReceiptCleanCommitted     IdentityPinReceiptKind = "clean_committed"
	IdentityPinReceiptProtectionRestored IdentityPinReceiptKind = "protection_restored"
)

// IdentityPinReceipt records a result; recording it never opens the fence.
type IdentityPinReceipt struct {
	Permit         IdentityPinRemovalPermit
	Kind           IdentityPinReceiptKind
	SnapshotDigest IdentityPinSnapshotDigest
}
