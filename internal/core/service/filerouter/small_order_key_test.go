package filerouter

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// forgedFileCommand is a file_command "from" the neutral key's fingerprint:
// the nonce binds the forged SRC, src_pubkey fingerprints to it, and the
// signature is the one stdlib accepts under that key for any nonce.
func forgedFileCommand(dst domain.PeerIdentity) protocol.FileCommandFrame {
	src := domain.PeerIdentityFromWire(identity.Fingerprint(edforgery.NeutralPublicKey()))
	const ttl = 5
	now := time.Now().Unix()
	payload := "forged-payload"
	return protocol.FileCommandFrame{
		Type:      protocol.FileCommandFrameType,
		SRC:       src,
		SrcPubKey: base64.StdEncoding.EncodeToString(edforgery.NeutralPublicKey()),
		DST:       dst,
		TTL:       ttl,
		MaxTTL:    ttl,
		Time:      now,
		Nonce:     protocol.ComputeNonce(src, dst, ttl, now, payload),
		Signature: hex.EncodeToString(edforgery.UniversalSignature()),
		Payload:   payload,
	}
}

// TestFileRouterDropsSmallOrderSourceKey covers both branches the forgery
// would otherwise take: local delivery to a node that trusts the forged
// address, and relay towards somebody else.
func TestFileRouterDropsSmallOrderSourceKey(t *testing.T) {
	t.Parallel()
	localID := domaintest.ID("local-node-identity-1234567890ab")
	forged := forgedFileCommand(localID)

	t.Run("local_delivery", func(t *testing.T) {
		t.Parallel()
		trusted := map[domain.PeerIdentity]ed25519.PublicKey{forged.SRC: edforgery.NeutralPublicKey()}
		tr := newTestFileRouter(localID, true, routing.Snapshot{TakenAt: time.Now()}, trusted, nil)
		raw, err := protocol.MarshalFileCommandFrame(forged)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})
		if got := len(tr.localDeliveries()); got != 0 {
			t.Fatalf("a frame signed under a small-order key was delivered %d times", got)
		}
	})

	t.Run("relay", func(t *testing.T) {
		t.Parallel()
		dst := domaintest.ID("far-destination-identity-12345678")
		nextHop := domaintest.ID("next-hop-identity-1234567890abcd")
		relayed := forgedFileCommand(dst)
		snap := routing.Snapshot{
			TakenAt: time.Now(),
			Routes: map[domain.PeerIdentity][]routing.RouteEntry{
				dst: {{Identity: dst, NextHop: nextHop, Hops: 2, ExpiresAt: time.Now().Add(time.Minute)}},
			},
		}
		tr := newTestFileRouter(localID, true, snap, nil, nil)
		raw, err := protocol.MarshalFileCommandFrame(relayed)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})
		if got := len(tr.sentTo(nextHop)); got != 0 {
			t.Fatalf("a frame signed under a small-order key was relayed %d times", got)
		}
	})
}
