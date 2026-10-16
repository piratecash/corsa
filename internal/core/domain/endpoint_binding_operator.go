package domain

import (
	"errors"
	"time"
)

// endpoint_binding_operator.go is the operator's side of the endpoint
// bindings (docs/protocol/session_v2.md, "Operator actions on endpoint
// bindings"): the list, the addressed removal of an ordinary binding and the
// two-step move of selected protected bindings to the ordinary tier. Nothing
// here lowers a protection by itself: a protected binding leaves its tier only
// through a demotion the operator confirmed for exactly the endpoints shown.

// EndpointBindingTier is the tier of a binding as the operator sees it.
type EndpointBindingTier string

const (
	// EndpointBindingTierProtected never expires and is never lowered
	// automatically; it counts against a pool (ProtectedBindingPool).
	EndpointBindingTierProtected EndpointBindingTier = "protected"
	// EndpointBindingTierGeneral lasts EndpointBindingGeneralTerm from the
	// last v2 session on the endpoint, or from the operator's demotion,
	// whichever is later.
	EndpointBindingTierGeneral EndpointBindingTier = "general"
)

// EndpointBindingGeneralTerm is how long an ordinary binding outlives the
// last v2 session on its endpoint — and a demoted one the demotion.
const EndpointBindingGeneralTerm = 30 * 24 * time.Hour

// EndpointBindingKeyForm is how the store holds a binding's key.
type EndpointBindingKeyForm string

const (
	// EndpointBindingKeyCanonical: the endpoint's canonical form; every
	// spelling of the endpoint names the binding.
	EndpointBindingKeyCanonical EndpointBindingKeyForm = "canonical"
	// EndpointBindingKeyNonCanonical: a record an earlier build stored under
	// a dialled address with no canonical form; only that exact address
	// names it.
	EndpointBindingKeyNonCanonical EndpointBindingKeyForm = "non_canonical"
)

// EndpointBindingView is one binding as the operator's list shows it: a
// projection of the store's record, never the record itself. Optional parts
// are absent rather than zero.
type EndpointBindingView struct {
	// Endpoint is the key the binding is held under: the canonical address,
	// or the exact stored address of a non-canonical record.
	Endpoint PeerAddress
	KeyForm  EndpointBindingKeyForm
	// Transport is absent for a non-canonical record whose transport the
	// store cannot tell.
	Transport   *EndpointTransport
	SourceGroup SourceGroup
	Tier        EndpointBindingTier
	// Pool and ProtectedBy are present exactly for a protected binding.
	Pool        *ProtectedBindingPool
	ProtectedBy *PeerIdentity
	// Identity is the last identity proven on the endpoint over v2. It is
	// what was seen there, not who is expected there.
	Identity PeerIdentity
	LastV2   time.Time
	// ExpiresAt is present exactly for an ordinary binding.
	ExpiresAt *time.Time
	// FirstAt is absent for a binding written before creation times were
	// kept.
	FirstAt *time.Time
	// DerivedFrom is the name a derived key was reached by.
	DerivedFrom *PeerAddress
	// DemotedAt is when an operator moved the binding to the ordinary tier;
	// absent for a binding never demoted, or protected again since.
	DemotedAt *time.Time
}

// MaxEndpointBindingPage bounds one page of the list: the store holds up to
// 49 152 bindings, and one answer carrying all of them would be megabytes
// built under the store's mutex.
const MaxEndpointBindingPage = 1000

// EndpointBindingQuery selects the bindings of one page. Every filter is
// optional; Limit is 1..MaxEndpointBindingPage.
type EndpointBindingQuery struct {
	// Identity keeps the bindings whose last identity, or whose protection's
	// giver, is this identity.
	Identity *PeerIdentity
	// Endpoint keeps the binding this address names.
	Endpoint *PeerAddress
	// After is the cursor: the page starts after this key, in key order.
	After *PeerAddress
	Limit int
}

// EndpointBindingPage is one page of the list, in key order.
type EndpointBindingPage struct {
	Bindings []EndpointBindingView
	// Total is how many live bindings the filters keep, over every page.
	Total int
	// NextAfter is the cursor of the next page; absent on the last one.
	NextAfter *PeerAddress
}

// EndpointBindingOperationID names one operator action in the journal. It
// does not depend on a connection: the action has none, and the id is what
// ties the lines of one action together.
type EndpointBindingOperationID string

// EndpointBindingSelectionToken is what a demotion's dry run hands back and
// its confirmation must present: a digest of the selected endpoints with the
// protection each holds. A confirmation of another selection, or of the same
// selection after its protections changed, does not match.
type EndpointBindingSelectionToken string

// MaxEndpointBindingSelection bounds the endpoints of one demotion. There is
// no "all" and no "all of an identity": every endpoint is named.
const MaxEndpointBindingSelection = 256

// EndpointBindingDemotionConsequence is what a demotion does, as the dry run
// states it before the operator confirms.
const EndpointBindingDemotionConsequence = "each endpoint becomes an ordinary binding: after 30 days without a v2 session on it, it accepts v1 again from an identity that is not pinned"

// EndpointBindingDemotionItem is one selected endpoint of a demotion.
type EndpointBindingDemotionItem struct {
	Endpoint PeerAddress
	KeyForm  EndpointBindingKeyForm
	// Pool is the pool the protection was counted against.
	Pool ProtectedBindingPool
	// ExpiresAt is the term the ordinary binding gets: the demotion plus
	// EndpointBindingGeneralTerm, extended by every later v2 session.
	ExpiresAt time.Time
	// IdentityPinRemains: the last identity proven on the endpoint is pinned,
	// so a v1 session naming THAT identity stays refused on any address
	// after the binding expires. It does not keep another identity off the
	// endpoint.
	IdentityPinRemains bool
}

// EndpointBindingDemotionPlan is the dry run of a demotion: what would
// change, what follows from it and the token that confirms exactly this.
type EndpointBindingDemotionPlan struct {
	Items       []EndpointBindingDemotionItem
	Consequence string
	Token       EndpointBindingSelectionToken
}

// EndpointBindingDemotionResult is a demotion that was written.
type EndpointBindingDemotionResult struct {
	OperationID EndpointBindingOperationID
	Items       []EndpointBindingDemotionItem
	Consequence string
}

// EndpointBindingRemoval is an ordinary binding the operator removed.
type EndpointBindingRemoval struct {
	OperationID EndpointBindingOperationID
	Endpoint    PeerAddress
	KeyForm     EndpointBindingKeyForm
}

var (
	// ErrEndpointBindingNotFound: no live binding is held under the address.
	ErrEndpointBindingNotFound = errors.New("endpoint_binding_not_found")
	// ErrEndpointBindingProtected: the addressed removal is for ordinary
	// bindings; a protected one leaves its tier only by a confirmed demotion.
	ErrEndpointBindingProtected = errors.New("endpoint_binding_protected: a protected binding is not removed, it is demoted to general by a confirmed demotion")
	// ErrEndpointBindingNotProtected: a demotion named an ordinary binding.
	ErrEndpointBindingNotProtected = errors.New("endpoint_binding_not_protected")
	// ErrEndpointBindingSelectionEmpty: a demotion named no endpoint.
	ErrEndpointBindingSelectionEmpty = errors.New("endpoint_binding_selection_empty: name every endpoint to demote")
	// ErrEndpointBindingSelectionTooLarge: a demotion named more than
	// MaxEndpointBindingSelection endpoints.
	ErrEndpointBindingSelectionTooLarge = errors.New("endpoint_binding_selection_too_large")
	// ErrEndpointBindingSelectionDuplicate: two addresses of a demotion name
	// one binding.
	ErrEndpointBindingSelectionDuplicate = errors.New("endpoint_binding_selection_duplicate")
	// ErrEndpointBindingSelectionChanged: the confirmation does not match the
	// selection as it is now — another selection, or the same one whose
	// protections changed since the dry run.
	ErrEndpointBindingSelectionChanged = errors.New("endpoint_binding_selection_changed: run the dry run again and confirm what it shows")
	// ErrEndpointBindingPageLimit: a page limit outside
	// 1..MaxEndpointBindingPage.
	ErrEndpointBindingPageLimit = errors.New("endpoint_binding_page_limit")
	// ErrEndpointBindingStoreUnavailable: the node keeps no store, or the
	// store file could not be read and nothing may be written over it.
	ErrEndpointBindingStoreUnavailable = errors.New("endpoint_binding_store_unavailable")
)
