package netcore

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
)

var ownedQueueItemBytes = domain.SizeOfAll(ownedFindNodeSend{})

func OwnedQueueItemBytes() uint64 { return ownedQueueItemBytes }

// One metric-only original-owner value, carried by the existing owned item.
// The producer owns it until a nonoffer return; after a successful offer only
// the sole consumer or a terminal producer wins the private metadata claim.
// Cancel/Finish alone do not release.
// Empty detached wrappers may remain buffered until their core is collected;
// wrapper bytes below mean graph-bearing operational wrappers, not GC reclaim.
// No work fields are cleared, and detached original cores remain accounted
// through the ORIGINAL intake owner rather than a current registry traversal.
func (item *ownedFindNodeSend) holdOwnedResources(pc *NetCore) {
	if item == nil || item.core != pc || item.resourcesHeld {
		return
	}
	if item.answer != (findnode.OwnedQueuedAnswer{}) {
		item.resourceCustody = item.answer.HoldResources()
	} else {
		item.resourceCustody = item.queued.HoldResources()
	}
	item.resourcesHeld = true // complete before channel publication
}

// No consumer reads the private wrapper payload until this successful CAS.
// This is metadata custody only, never writer-start/admission authority.
func (item *ownedFindNodeSend) claimOwnedConsumer() bool {
	return item != nil && item.custodyClaim.CompareAndSwap(0, 1)
}
func (item *ownedFindNodeSend) discardOwnedItem() {
	if !item.claimOwnedConsumer() {
		return
	}
	item.cancelOwned()
	item.releaseOwnedResources()
}
func (item *ownedFindNodeSend) retireUnclaimedOwnedItem() {
	if item == nil || !item.custodyClaim.CompareAndSwap(0, 2) {
		return
	}
	// Covers nonoffers and failed postoffers, including an offer after the
	// writer's last drain but before/after writerExited closes. If a consumer
	// already claimed this wrapper, the producer NEVER clears its fields.
	item.cancelOwned()
	item.releaseOwnedResources()
}
func (item *ownedFindNodeSend) releaseOwnedResources() {
	if item == nil {
		return
	}
	held, active := item.resourceCustody, item.resourcesHeld
	// Unique wrapper fields only. The claim prevents concurrent metadata use;
	// shared opaque work/receipt/plan fields are never mutated or cleared.
	item.queued = findnode.OwnedQueuedForward{}
	item.answer = findnode.OwnedQueuedAnswer{}
	item.resourceCustody = findnode.OwnedResourceCustody{}
	item.resourcesHeld = false // immutable core/header retained
	if active {
		held.Release()
	}
}
