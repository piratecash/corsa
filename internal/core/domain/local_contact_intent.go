package domain

import "errors"

// LocalContactIntent is an explicit local address-book operation, never a wire claim.
type LocalContactIntent struct {
	Network      NetworkID
	Identity     PeerIdentity
	PubKey       PeerPublicKey
	BoxKey       PeerBoxKey
	BoxSignature PeerBoxSignature
}
type ContactApplyState uint8

const (
	ContactUnchanged ContactApplyState = iota
	ContactAppliedLive
)

type LocalContactFactState uint8

const (
	LocalContactFactAbsent LocalContactFactState = iota
	LocalContactFactUnsupported
	LocalContactFactUnconfirmed
	LocalContactFactDurableConfirmed
)

type LocalContactKState uint8

const (
	LocalContactKNotEvaluated LocalContactKState = iota
	LocalContactKAwaitingFactConfirmation
	LocalContactKAwaitingFreshProof
	LocalContactKAwaitingPromotion
	LocalContactKStagedUnconfirmed
	LocalContactKConfirmed
)

// K describes classification only. AwaitingFreshProof does not assert that an
// ordinary identity pin or another downgrade protection is absent.
type LocalContactIntentResult struct {
	Identity PeerIdentity
	Contact  ContactApplyState
	Fact     LocalContactFactState
	K        LocalContactKState
}

var (
	ErrLocalContactIntent          = errors.New("invalid explicit local contact intent")
	ErrLocalContactFactUnconfirmed = errors.New("local contact provenance is not durably confirmed")
	ErrLocalContactFactStale       = errors.New("local contact intent was removed or replaced")
)
