package protocol

import (
	"bytes"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// livenessTestNetwork is the network every test below derives against, unless
// it is deliberately testing the network binding.
func livenessTestNetwork() domain.NetworkID { return domain.NetworkID("corsa-testnet") }

// livenessTestLabel is a stand-in for the per-attempt label the prober
// generates. Tests that care about the binding use their own.
func livenessTestLabel() domain.PeerIdentity {
	return domain.PeerIdentityFromWire("abcdef0123456789abcdef0123456789abcdef01")
}

func livenessTestIdentity(t *testing.T) (*identity.Identity, domain.PeerIdentity, string) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id,
		domain.PeerIdentityFromWire(id.Address),
		base64.StdEncoding.EncodeToString(id.BoxPublicKey)
}

// TestBothSidesDeriveTheSameToken is the property the gate rests on: the asker
// builds the token from their own private key and the target's public one, and
// the target recomputes it from the mirror pair. Neither needs anything the
// other sent, so the token cannot be replayed by a party holding neither key.
func TestBothSidesDeriveTheSameToken(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	token, err := BuildLivenessToken(askerID.BoxPrivateKey, livenessTestNetwork(), targetBox, asker, target, livenessTestLabel(), epoch)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	if len(token) != livenessTokenBytes {
		t.Fatalf("token width: got %d, want %d", len(token), livenessTokenBytes)
	}
	if !VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, asker, target, livenessTestLabel(), epoch, token) {
		t.Fatal("the target could not recompute the asker's token")
	}
}

// TestATokenIsUselessToAThirdParty: somebody who holds neither private key
// cannot produce a token, which is the entire gate. A stranger who has both
// PUBLIC keys — they are in the identity records, so assume they do — still
// cannot.
func TestATokenIsUselessToAThirdParty(t *testing.T) {
	askerID, asker, _ := livenessTestIdentity(t)
	targetID, target, _ := livenessTestIdentity(t)
	strangerID, _, strangerBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	_, _ = askerID, targetID

	// The stranger claims to be the asker and signs with their own key.
	forged, err := BuildLivenessToken(strangerID.BoxPrivateKey, livenessTestNetwork(), strangerBox, asker, target, livenessTestLabel(), epoch)
	if err != nil {
		t.Fatalf("build forged token: %v", err)
	}
	if VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), base64.StdEncoding.EncodeToString(askerID.BoxPublicKey), asker, target, livenessTestLabel(), epoch, forged) {
		t.Fatal("a token built by a third party verified as the asker's")
	}
}

// TestATokenCannotBeReflected: the token for A→B must not verify as B→A. The
// ECDH output is symmetric, so without direction in the derivation a target
// could take the token addressed to them and use it to probe the asker.
func TestATokenCannotBeReflected(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	token, err := BuildLivenessToken(askerID.BoxPrivateKey, livenessTestNetwork(), targetBox, asker, target, livenessTestLabel(), epoch)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	// Same two parties, roles swapped.
	if VerifyLivenessToken(askerID.BoxPrivateKey, livenessTestNetwork(), targetBox, target, asker, livenessTestLabel(), epoch, token) {
		t.Fatal("the token reflected: A→B verified as B→A")
	}
	_ = askerBox
	_ = targetID
}

// TestATokenExpiresWithItsEpoch, and TestNeighbouringEpochsAreAccepted below,
// are the two halves of the time rule: a captured token stops working, and
// ordinary clock skew does not break an honest one.
func TestATokenExpiresWithItsEpoch(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)

	now := time.Now()
	old := LivenessTokenEpochAt(now.Add(-10 * LivenessTokenEpoch))
	token, err := BuildLivenessToken(askerID.BoxPrivateKey, livenessTestNetwork(), targetBox, asker, target, livenessTestLabel(), old)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	current := LivenessTokenEpochAt(now)
	if VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, asker, target, livenessTestLabel(), current, token) {
		t.Fatal("a token from ten epochs ago still verifies in the current one")
	}
}

func TestNeighbouringEpochsAreAccepted(t *testing.T) {
	now := time.Unix(int64(5*LivenessTokenEpoch.Seconds()), 0).UTC()
	accepted := LivenessTokenEpochsAccepted(now)
	if len(accepted) != 3 {
		t.Fatalf("accepted epochs: got %d, want 3 (current and both neighbours)", len(accepted))
	}
	current := LivenessTokenEpochAt(now)
	seen := map[uint64]bool{}
	for _, epoch := range accepted {
		seen[epoch] = true
	}
	for _, want := range []uint64{current - 1, current, current + 1} {
		if !seen[want] {
			t.Fatalf("epoch %d is not accepted: an honest token built either side of a "+
				"window boundary would be refused on ordinary clock skew", want)
		}
	}
}

// TestAMalformedTokenIsRefusedNotPanicking: everything here parses attacker
// input. Wrong widths and unparsable keys must be refusals, never panics.
func TestAMalformedTokenIsRefusedNotPanicking(t *testing.T) {
	targetID, target, _ := livenessTestIdentity(t)
	_, asker, askerBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	cases := map[string][]byte{
		"empty":     {},
		"one byte":  {0x01},
		"too long":  make([]byte, livenessTokenBytes+1),
		"too short": make([]byte, livenessTokenBytes-1),
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, asker, target, livenessTestLabel(), epoch, token) {
				t.Fatal("a malformed token verified")
			}
		})
	}

	if _, err := BuildLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), "not base64!", asker, target, livenessTestLabel(), epoch); err == nil {
		t.Fatal("a non-base64 box key was accepted")
	}
	if _, err := BuildLivenessToken(nil, livenessTestNetwork(), askerBox, asker, target, livenessTestLabel(), epoch); err == nil {
		t.Fatal("a nil private key was accepted")
	}
}

// TestSealedProbeRoundTrip: the asker seals, the target opens, and the claim
// survives intact.
func TestSealedProbeRoundTrip(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	now := time.Now()

	sealed, err := SealLivenessProbe(askerID.BoxPrivateKey, livenessTestNetwork(), asker, target, targetBox, livenessTestLabel(), now)
	if err != nil {
		t.Fatalf("seal probe: %v", err)
	}

	claim, err := OpenLivenessProbe(targetID, target, sealed)
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	if claim.Asker != asker {
		t.Fatalf("claim names %s, want %s", claim.Asker, asker)
	}
	if !VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, claim.Asker, target, livenessTestLabel(), claim.Epoch, claim.Token) {
		t.Fatal("the token inside a freshly sealed probe did not verify")
	}
}

// TestASealedProbeIsOpaqueToEverybodyElse: a relay holding the ciphertext
// learns nothing, and cannot even tell who is asking. That is the reason the
// asker's name is inside rather than in the plaintext `requester` triple.
func TestASealedProbeIsOpaqueToEverybodyElse(t *testing.T) {
	askerID, asker, _ := livenessTestIdentity(t)
	_, target, targetBox := livenessTestIdentity(t)
	relayID, _, _ := livenessTestIdentity(t)

	sealed, err := SealLivenessProbe(askerID.BoxPrivateKey, livenessTestNetwork(), asker, target, targetBox, livenessTestLabel(), time.Now())
	if err != nil {
		t.Fatalf("seal probe: %v", err)
	}

	if _, err := OpenLivenessProbe(relayID, target, sealed); err == nil {
		t.Fatal("a relay opened a probe sealed to somebody else")
	}
	// The asker's fingerprint must not be recoverable from the bytes.
	if idx := indexOfBytes(sealed, []byte(asker.String())); idx >= 0 {
		t.Fatalf("the asker's fingerprint appears in the sealed bytes at %d", idx)
	}
}

// TestASealedProbeIsBoundToItsTarget: the label carries the target, so a probe
// captured on the way to B cannot be replayed at C even by a party who could
// otherwise decrypt it.
func TestASealedProbeIsBoundToItsTarget(t *testing.T) {
	askerID, asker, _ := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	_, other, _ := livenessTestIdentity(t)

	sealed, err := SealLivenessProbe(askerID.BoxPrivateKey, livenessTestNetwork(), asker, target, targetBox, livenessTestLabel(), time.Now())
	if err != nil {
		t.Fatalf("seal probe: %v", err)
	}
	if _, err := OpenLivenessProbe(targetID, other, sealed); err == nil {
		t.Fatal("a probe sealed for one target opened under another target's label")
	}
}

func indexOfBytes(haystack, needle []byte) int {
	if len(needle) == 0 || len(needle) > len(haystack) {
		return -1
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// TestATokenIsBoundToItsNetwork closes a replay the first version left open.
//
// The two identities are the same principals on every network, so the ECDH
// output is identical there. Without the network in the derivation, one sealed
// claim captured on network A would verify on network B inside the accepted
// epochs — the token would say "these two know each other" when what it has to
// say is "these two know each other HERE".
func TestATokenIsBoundToItsNetwork(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	here := domain.NetworkID("corsa-mainnet")
	elsewhere := domain.NetworkID("corsa-devnet")

	token, err := BuildLivenessToken(askerID.BoxPrivateKey, here, targetBox, asker, target, livenessTestLabel(), epoch)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	if !VerifyLivenessToken(targetID.BoxPrivateKey, here, askerBox, asker, target, livenessTestLabel(), epoch, token) {
		t.Fatal("a token did not verify on its own network")
	}
	if VerifyLivenessToken(targetID.BoxPrivateKey, elsewhere, askerBox, asker, target, livenessTestLabel(), epoch, token) {
		t.Fatal("a token minted on one network verified on another: a captured claim " +
			"is portable between networks")
	}
}

// TestLivenessTokenByteVector pins the derivation.
//
// A token is a wire contract between two independently built nodes: if either
// side changes the label, the salt, the field order or the width, every probe
// starts failing and the symptom is "presence stopped working", nowhere near
// the cause. Fixed keys and a fixed epoch make that a compile-time-visible
// change instead.
func TestLivenessTokenByteVector(t *testing.T) {
	askerPriv, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x11}, 32))
	if err != nil {
		t.Fatalf("asker key: %v", err)
	}
	targetPriv, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x22}, 32))
	if err != nil {
		t.Fatalf("target key: %v", err)
	}
	asker := domain.PeerIdentityFromWire("1111111111111111111111111111111111111111")
	target := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")
	network := domain.NetworkID("corsa-vector-net")
	label := domain.PeerIdentityFromWire("3333333333333333333333333333333333333333")

	token, err := BuildLivenessToken(
		askerPriv, network,
		base64.StdEncoding.EncodeToString(targetPriv.PublicKey().Bytes()),
		asker, target, label, 42,
	)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}

	const want = "55b6dc2b3f074f72c032d1a714c478fe"
	if got := hex.EncodeToString(token); got != want {
		t.Fatalf("token vector changed:\n got  %s\n want %s\n\n"+
			"The derivation is a contract between two independently built nodes. "+
			"If this change is intended, both ends must ship it together and the "+
			"vector updated in the same commit; if it is not, presence probes are "+
			"about to start failing everywhere with no message that says why.", got, want)
	}

	// The mirror side must land on the same bytes.
	if !VerifyLivenessToken(
		targetPriv, network,
		base64.StdEncoding.EncodeToString(askerPriv.PublicKey().Bytes()),
		asker, target, label, 42, token,
	) {
		t.Fatal("the vector token does not verify from the target side")
	}
}

// TestATokenIsBoundToItsAttempt closes the replay the review found.
//
// The sealed blob travels in the clear as far as any hop on the path is
// concerned: it cannot be READ without the target's key, but it can be COPIED.
// Before the token covered the attempt label, a transit could lift the
// ciphertext into a get_identity of its own, with a fresh label of its own, and
// harvest proofs for the rest of the epoch window — the target verified the
// claim, saw a contact, and answered. The claim is now good for exactly one
// question.
func TestATokenIsBoundToItsAttempt(t *testing.T) {
	askerID, asker, askerBox := livenessTestIdentity(t)
	targetID, target, targetBox := livenessTestIdentity(t)
	epoch := LivenessTokenEpochAt(time.Now())

	ours := domain.PeerIdentityFromWire("aaaa111111111111111111111111111111111111")
	theirs := domain.PeerIdentityFromWire("bbbb222222222222222222222222222222222222")

	token, err := BuildLivenessToken(askerID.BoxPrivateKey, livenessTestNetwork(), targetBox, asker, target, ours, epoch)
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	if !VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, asker, target, ours, epoch, token) {
		t.Fatal("a token did not verify against its own attempt label")
	}
	if VerifyLivenessToken(targetID.BoxPrivateKey, livenessTestNetwork(), askerBox, asker, target, theirs, epoch, token) {
		t.Fatal("a token verified under somebody else's attempt label: a transit can " +
			"copy the sealed claim into its own request and harvest proofs")
	}
}

// TestSealingRefusesAnUnboundClaim: a claim with no attempt label is exactly
// the bearer credential the binding removes, so it must not be constructible.
func TestSealingRefusesAnUnboundClaim(t *testing.T) {
	askerID, asker, _ := livenessTestIdentity(t)
	_, target, targetBox := livenessTestIdentity(t)

	if _, err := SealLivenessProbe(
		askerID.BoxPrivateKey, livenessTestNetwork(), asker, target, targetBox,
		domain.PeerIdentity{}, time.Now(),
	); err == nil {
		t.Fatal("a claim with no attempt label was sealed: it would be replayable " +
			"into any request for the rest of the epoch window")
	}
}
