package datagram

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// TestSmallOrderSignerIsDroppedWithoutBan pins the one auth refusal of §4.4
// that is NOT ban-worthy: an auth.pubkey no honest node can hold. The frame is
// self-consistent — src is the key's fingerprint and stdlib accepts the
// universal signature under it — so a relay on a build without the key check
// forwards it in good faith; the neighbour is not its author. It is refused
// before the verification token and before any replay slot, like the
// fingerprint check next to it.
func TestSmallOrderSignerIsDroppedWithoutBan(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: receiver.clock(),
	})
	forgedSrc, err := domain.ParsePeerIdentity(identity.Fingerprint(edforgery.NeutralPublicKey()))
	if err != nil {
		t.Fatalf("ParsePeerIdentity: %v", err)
	}
	frame.Src = forgedSrc
	frame.Auth.PubKey = edforgery.NeutralPublicKey()
	frame.Auth.Sig = edforgery.UniversalSignature()

	result := receiver.deliver(t, sender.id, frame)
	requireDrop(t, result, DropSignerKey)
	if result.BanWorthy() {
		t.Fatal("the neighbour was charged for the author's key")
	}
	if handler.callCount() != 0 {
		t.Fatal("a frame under the neutral key reached the handler")
	}
	if receiver.replay.Len() != 0 {
		t.Fatal("the refusal precedes the reservation")
	}
	if receiver.crypto.charged() != 0 {
		t.Fatal("the key check is comparisons only and runs before the verification token")
	}
}
