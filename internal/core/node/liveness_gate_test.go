package node

import (
	"encoding/base64"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// liveness_gate_test.go covers the contact gate on a liveness probe (PR B).
//
// The rule under test, and the reason each half of it exists:
//
//   - a probe carrying a VALID reciprocity token is answered — the target
//     knows it is talking to a contact;
//   - a probe carrying a token from anyone else is met with SILENCE, not a
//     refusal: a refusal would confirm the identity exists;
//   - a request carrying NO sealed claim is the public lookup and is answered
//     exactly as before, because first contact by 40-hex address and by
//     corsa:-link depends on it, and the identity resolver refuses an answer
//     without a proof.

func livenessGateNode(t *testing.T) (*Service, *identity.Identity, domain.PeerIdentity) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	store, err := loadTrustStore(filepath.Join(t.TempDir(), "trust.json"), trustedContact{
		Address: id.Address,
		PubKey:  identity.PublicKeyBase64(id.PublicKey),
		BoxKey:  base64.StdEncoding.EncodeToString(id.BoxPublicKey),
	})
	if err != nil {
		t.Fatalf("load trust store: %v", err)
	}
	svc := &Service{
		identity:      id,
		trust:         store,
		presenceClock: time.Now,
	}
	return svc, id, domain.PeerIdentityFromWire(id.Address)
}

// rememberLivenessContact puts a contact into the target's trust store with the
// box key the gate recomputes the token from.
func rememberLivenessContact(t *testing.T, svc *Service, contact *identity.Identity) {
	t.Helper()
	if _, err := svc.trust.remember(trustedContact{
		Address:      contact.Address,
		PubKey:       identity.PublicKeyBase64(contact.PublicKey),
		BoxKey:       base64.StdEncoding.EncodeToString(contact.BoxPublicKey),
		BoxSignature: identity.SignBoxKeyBinding(contact),
	}); err != nil {
		t.Fatalf("remember contact: %v", err)
	}
}

// livenessGateNetwork is the network both sides of these tests derive against.
func livenessGateNetwork() domain.NetworkID { return domain.NetworkID("corsa-gate-testnet") }

// livenessGateLabel stands in for the one-time attempt label the frame carries.
func livenessGateLabel() domain.PeerIdentity {
	return domain.PeerIdentityFromWire("0123456789abcdef0123456789abcdef01234567")
}

func livenessGateHandler(svc *Service) *getIdentityHandler {
	return newGetIdentityHandler(svc, livenessGateNetwork(), nil)
}

// TestGateAdmitsAContact: the everyday case. The asker seals a claim with
// their own box key; the target finds them in its contacts and recomputes the
// token from the mirror key pair.
func TestGateAdmitsAContact(t *testing.T) {
	target, targetID, targetIdentity := livenessGateNode(t)
	askerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate asker: %v", err)
	}
	asker := domain.PeerIdentityFromWire(askerID.Address)
	rememberLivenessContact(t, target, askerID)

	sealed, err := protocol.SealLivenessProbe(
		askerID.BoxPrivateKey, livenessGateNetwork(), asker, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), livenessGateLabel(), time.Now(),
	)
	if err != nil {
		t.Fatalf("seal claim: %v", err)
	}

	if !livenessGateHandler(target).acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("a contact's own sealed claim was rejected: presence probes from " +
			"contacts would be answered with silence and every contact would go grey")
	}
}

// TestGateRefusesAStranger is the oracle this PR closes on the probe path. The
// stranger holds both public keys — they are in the identity records, so
// assume they do — and still cannot produce the token.
func TestGateRefusesAStranger(t *testing.T) {
	target, targetID, targetIdentity := livenessGateNode(t)
	strangerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate stranger: %v", err)
	}
	stranger := domain.PeerIdentityFromWire(strangerID.Address)

	sealed, err := protocol.SealLivenessProbe(
		strangerID.BoxPrivateKey, livenessGateNetwork(), stranger, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), livenessGateLabel(), time.Now(),
	)
	if err != nil {
		t.Fatalf("seal claim: %v", err)
	}

	if livenessGateHandler(target).acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("a probe from somebody who is not a contact was admitted: the " +
			"presence oracle is open on the probe path")
	}
}

// TestGateRefusesABorrowedName: a real contact cannot probe under ANOTHER
// contact's name. The token is derived from the asker's own private key, so
// claiming a name they do not hold produces a MAC that does not verify against
// that name's public key.
func TestGateRefusesABorrowedName(t *testing.T) {
	target, targetID, targetIdentity := livenessGateNode(t)

	victimID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate victim: %v", err)
	}
	impostorID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate impostor: %v", err)
	}
	// BOTH are contacts of the target, so membership alone would admit this.
	rememberLivenessContact(t, target, victimID)
	rememberLivenessContact(t, target, impostorID)

	victim := domain.PeerIdentityFromWire(victimID.Address)
	// The impostor seals a claim naming the victim, signed with its own key.
	sealed, err := protocol.SealLivenessProbe(
		impostorID.BoxPrivateKey, livenessGateNetwork(), victim, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), livenessGateLabel(), time.Now(),
	)
	if err != nil {
		t.Fatalf("seal claim: %v", err)
	}

	if livenessGateHandler(target).acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("one contact probed under another contact's name: the token is " +
			"not being checked against the claimed identity's key")
	}
}

// TestGateRefusesAStaleToken: a captured claim stops working. Without this the
// token would be a permanent credential rather than a time-bound one.
func TestGateRefusesAStaleToken(t *testing.T) {
	target, targetID, targetIdentity := livenessGateNode(t)
	askerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate asker: %v", err)
	}
	asker := domain.PeerIdentityFromWire(askerID.Address)
	rememberLivenessContact(t, target, askerID)

	stale := time.Now().Add(-10 * protocol.LivenessTokenEpoch)
	sealed, err := protocol.SealLivenessProbe(
		askerID.BoxPrivateKey, livenessGateNetwork(), asker, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), livenessGateLabel(), stale,
	)
	if err != nil {
		t.Fatalf("seal claim: %v", err)
	}

	if livenessGateHandler(target).acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("a claim from ten epochs ago was admitted: a captured token never expires")
	}
}

// TestGateAnswersOneQuestionOnce is the anti-replay half of the gate.
//
// The token binds a claim to its attempt label, so the sealed blob cannot be
// moved into a different question. It can still be presented AGAIN as the same
// question, and that is what a hop on the path holds: the whole request, valid
// for the rest of its epoch window. The second presentation must be silence.
func TestGateAnswersOneQuestionOnce(t *testing.T) {
	target, targetID, targetIdentity := livenessGateNode(t)
	askerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate asker: %v", err)
	}
	asker := domain.PeerIdentityFromWire(askerID.Address)
	rememberLivenessContact(t, target, askerID)

	sealed, err := protocol.SealLivenessProbe(
		askerID.BoxPrivateKey, livenessGateNetwork(), asker, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), livenessGateLabel(), time.Now(),
	)
	if err != nil {
		t.Fatalf("seal claim: %v", err)
	}

	// ONE handler across both calls: the replay set belongs to the node, and
	// a per-call handler would make this test pass without the rule existing.
	handler := livenessGateHandler(target)
	if !handler.acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("the first presentation of an honest claim was rejected")
	}
	if handler.acceptLivenessClaim(sealed, livenessGateLabel()) {
		t.Fatal("the SAME sealed request was admitted twice: a hop that captured " +
			"one probe can harvest proofs from it for the rest of the token's " +
			"epoch window")
	}

	// A different attempt from the same asker is a different question and must
	// still be answered — otherwise the anti-replay would silence the cadence.
	second := domain.PeerIdentityFromWire("fedcba9876543210fedcba9876543210fedcba98")
	fresh, err := protocol.SealLivenessProbe(
		askerID.BoxPrivateKey, livenessGateNetwork(), asker, targetIdentity,
		base64.StdEncoding.EncodeToString(targetID.BoxPublicKey), second, time.Now(),
	)
	if err != nil {
		t.Fatalf("seal second claim: %v", err)
	}
	if !handler.acceptLivenessClaim(fresh, second) {
		t.Fatal("the asker's NEXT probe was refused: the anti-replay is keyed on " +
			"the contact rather than on the attempt, and presence would stop " +
			"renewing after one probe")
	}
}

// TestGateRefusesGarbage: everything here is attacker input. Nonsense must be a
// refusal, never a panic.
func TestGateRefusesGarbage(t *testing.T) {
	target, _, _ := livenessGateNode(t)
	handler := livenessGateHandler(target)

	for name, sealed := range map[string][]byte{
		"empty":        {},
		"short":        make([]byte, 8),
		"prefix only":  make([]byte, 44),
		"random bytes": []byte("this is not a sealed liveness claim at all"),
	} {
		t.Run(name, func(t *testing.T) {
			if handler.acceptLivenessClaim(sealed, livenessGateLabel()) {
				t.Fatal("garbage was admitted as a reciprocity claim")
			}
		})
	}
}

// TestPublicLookupCarriesNoSealedClaim pins the compatibility half of the
// decision.
//
// The identity resolver — the thing that resolves an identity you have only an
// address for — must keep sending a request with NO sealed claim, so the target
// answers it on the public path. If a claim ever appeared here, first contact
// would silently stop working for everyone who is not already a contact, which
// is the population the lookup exists for.
func TestPublicLookupCarriesNoSealedClaim(t *testing.T) {
	payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V:           domain.IdentityLookupSchemaVersion,
		TargetProof: true,
	})
	if err != nil {
		t.Fatalf("build lookup payload: %v", err)
	}
	parsed, err := protocol.ParseGetIdentityPayload(payload)
	if err != nil {
		t.Fatalf("parse lookup payload: %v", err)
	}
	if len(parsed.Sealed) != 0 {
		t.Fatal("a plain lookup request carries a sealed claim: the target would " +
			"gate it, and resolving an identity you are not yet in contact with " +
			"would stop working")
	}
}

// TestSealedClaimSurvivesTheWire: the field has to round-trip byte for byte,
// or the gate rejects every honest probe.
func TestSealedClaimSurvivesTheWire(t *testing.T) {
	claim := []byte("sealed-claim-bytes-not-really-encrypted-but-opaque-here")
	payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V:           domain.IdentityLookupSchemaVersion,
		TargetProof: true,
		Sealed:      claim,
	})
	if err != nil {
		t.Fatalf("build probe payload: %v", err)
	}
	parsed, err := protocol.ParseGetIdentityPayload(payload)
	if err != nil {
		t.Fatalf("parse probe payload: %v", err)
	}
	if string(parsed.Sealed) != string(claim) {
		t.Fatalf("sealed claim round trip: got %q, want %q", parsed.Sealed, claim)
	}
}
