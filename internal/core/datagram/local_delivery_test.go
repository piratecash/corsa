package datagram

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// local_delivery_test.go pins the outcome table of §4.1 step 10: what happens
// to the replay key for each of the three handler outcomes, and what the
// sender's repeat then meets.

// reclaimingHandler answers like any other handler, but first reclaims its own
// replay record — so the Commit that follows meets the one state in which the
// layer's memory answers `fail`: the record is gone.
//
// It is not an injection into the memory. There is no seam to inject one into,
// and that is the point; this is the state the cache reaches by itself when the
// abandoned-reservation watchdog frees a branch that outlived replay_until plus
// the whole hop budget (baseHeldReservationGrace). The handler is where the
// layer stands between Reserve and Commit on this plane, so the fixture reaches
// that state from there.
type reclaimingHandler struct {
	*recordingHandler
	reclaimed bool
}

func newReclaimingHandler(
	cache *BaseReplayCache,
	key domain.ReplayKey,
	answer func() HandlerResult,
) *reclaimingHandler {
	handler := &reclaimingHandler{recordingHandler: &recordingHandler{}}
	handler.result = func(DeliveryContext, []byte) HandlerResult {
		if !handler.reclaimed {
			handler.reclaimed = forgetReplayRecord(cache, key)
		}
		return answer()
	}
	return handler
}

// ---------------------------------------------------------------------------
// The outcome table
// ---------------------------------------------------------------------------

// TestLocalDeliveryOutcomeTable walks all three handler outcomes and pins the
// fate of the replay key for each (§4.1, §9): accepted and rejected both COMMIT
// the key, failed releases it.
//
// The three outcomes are told apart by the layer's counters (§10) and not by
// the cache record, which carries no verdict: what the record decides is only
// what the repeat below meets.
func TestLocalDeliveryOutcomeTable(t *testing.T) {
	cases := []struct {
		name        string
		handler     *recordingHandler
		outcome     InboundOutcome
		reason      DropReason
		wantCommit  uint64
		wantRelease uint64
	}{
		{"accepted", acceptingHandler(), InboundDelivered, DropReasonUnset, 1, 0},
		{"rejected", refusingHandler(), InboundDropped, DropHandlerRejected, 1, 0},
		{"failed", failingHandler(), InboundDropped, DropHandlerFailed, 0, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			net := newFakeNetwork()
			private, signer := newSigner(t)
			sender := newPipelineNode(t, net, nodeOpts{id: signer})
			receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
			registerType(t, receiver, routedType(dtypePush, testCase.handler))

			frame := signedRouted(t, routedOpts{
				private: private, src: signer, dst: receiver.id, now: sender.clock(),
			})
			result := receiver.deliver(t, sender.id, frame)
			requireOutcome(t, result, testCase.outcome)
			if testCase.reason != DropReasonUnset && result.Reason() != testCase.reason {
				t.Fatalf("reason %s, want %s", result.Reason(), testCase.reason)
			}
			// The fate of the key is read off the cache's OWN counters rather than
			// off a list of Commit calls: a Commit that was called and then refused
			// leaves the counter where it was.
			replay := receiver.replayCalls()
			if replay.commits != testCase.wantCommit {
				t.Fatalf("the key was committed %d times, want %d", replay.commits, testCase.wantCommit)
			}
			if replay.releases != testCase.wantRelease {
				t.Fatalf("Release called %d times, want %d", replay.releases, testCase.wantRelease)
			}

			// The repeat: a committed key is sieved by the early Has without a
			// second verification and without a second handler call; a released
			// one reaches the handler again.
			before := receiver.crypto.charged()
			calls := testCase.handler.callCount()
			repeat := receiver.deliver(t, sender.id, frame)
			if testCase.wantRelease == 0 {
				requireDrop(t, repeat, DropReplayDuplicate)
				if receiver.crypto.charged() != before {
					t.Fatal("a duplicate sieved by the early Has must not spend a crypto token")
				}
				if testCase.handler.callCount() != calls {
					t.Fatal("a committed key must not reach the handler twice")
				}
			} else if testCase.handler.callCount() != calls+1 {
				t.Fatal("a released key must let the repeat reach the handler")
			}
		})
	}
}

// TestPanicInHandlerIsTreatedAsFailed pins the "failed or panic" row.
func TestPanicInHandlerIsTreatedAsFailed(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	registerType(t, receiver, routedType(dtypePush, HandlerFunc(
		func(context.Context, DeliveryContext, []byte) HandlerResult {
			panic("handler exploded")
		})))

	result := receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(),
	}))
	requireDrop(t, result, DropHandlerFailed)
	if receiver.replay.Len() != 0 {
		t.Fatal("a panicking handler releases the key")
	}
}

// TestCommitRejectedFailureReleases is the third Commit.fail branch of §9.
func TestCommitRejectedFailureReleases(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(),
	})
	handler := newReclaimingHandler(receiver.replay, replayKeyOf(t, frame),
		func() HandlerResult { return RejectDelivery(errTestRefused) })
	registerType(t, receiver, routedType(dtypePush, handler))

	requireDrop(t, receiver.deliver(t, sender.id, frame), DropHandlerRejected)
	if !handler.reclaimed {
		t.Fatal("the fixture never reclaimed the reservation: the Commit below cannot have failed")
	}
	calls := receiver.replayCalls()
	if calls.commits != 0 {
		t.Fatalf("the Commit landed after all (%d): the premise of this test never armed", calls.commits)
	}
	if calls.releases != 1 {
		t.Fatalf("Commit(rejected).fail must Release, got %d", calls.releases)
	}
	// The repeat reaches the handler again and is refused again: extra work,
	// but no loss and no permanently occupied slot.
	requireDrop(t, receiver.deliver(t, sender.id, frame), DropHandlerRejected)
	if handler.callCount() != 2 {
		t.Fatalf("the handler ran %d times, want 2", handler.callCount())
	}
}

// TestHandlerReceivesIncomingPeer pins that the handler gets the same context
// as the authorization hook, incoming_peer included.
func TestHandlerReceivesIncomingPeer(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	requireOutcome(t, receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(),
	})), InboundDelivered)

	delivery, ok := handler.lastContext()
	if !ok {
		t.Fatal("the handler was not called")
	}
	peer, remote := delivery.IncomingPeer().Identity()
	if !remote || peer != sender.id {
		t.Fatalf("incoming peer %v/%v, want %v", peer, remote, sender.id)
	}
	if delivery.LocalIdentity() != receiver.id {
		t.Fatalf("local identity %v, want %v", delivery.LocalIdentity(), receiver.id)
	}
}
