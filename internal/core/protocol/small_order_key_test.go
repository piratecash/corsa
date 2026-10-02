package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// small_order_key_test.go drives every signature check of this package with
// the universal forgery (testutil/edforgery): the neutral key, an address that
// IS its fingerprint, and the signature stdlib accepts for any message. Each
// site must refuse it as a bad KEY — distinguishable from a bad signature —
// because the callers decide penalties on that distinction.

// mustSigningKey parses an honest key for the tests that verify with one.
func mustSigningKey(t *testing.T, raw []byte) identity.PublicKey {
	t.Helper()
	key, err := identity.ParsePublicKey(raw)
	if err != nil {
		t.Fatalf("ParsePublicKey(honest): %v", err)
	}
	return key
}

func forgedAddress(t *testing.T) domain.PeerIdentity {
	t.Helper()
	return mustIdentity(t, identity.Fingerprint(edforgery.NeutralPublicKey()))
}

func forgedKeyBase64() string {
	return base64.StdEncoding.EncodeToString(edforgery.NeutralPublicKey())
}

func forgedDatagram(t *testing.T) DatagramFrame {
	t.Helper()
	forged := newSignedDatagram(t).Clone()
	forged.Src = forgedAddress(t)
	forged.Auth.PubKey = edforgery.NeutralPublicKey()
	forged.Auth.Sig = edforgery.UniversalSignature()
	return forged
}

func TestVerifyDatagramSignatureRefusesSmallOrderSigner(t *testing.T) {
	forged := forgedDatagram(t)
	if !DatagramSignerMatchesSrc(forged) {
		t.Fatal("the forgery must be self-consistent: its key fingerprints to its src")
	}
	err := VerifyDatagramSignature(forged, testDatagramNetwork)
	if !errors.Is(err, ErrDatagramSignerKey) || !errors.Is(err, identity.ErrInvalidPublicKey) {
		t.Fatalf("VerifyDatagramSignature(forged) = %v, want ErrDatagramSignerKey", err)
	}
	if errors.Is(err, ErrDatagramSignature) {
		t.Fatal("a bad key was reported as a bad signature; the pipeline would charge the relay for it")
	}
	if _, err := ParseDatagramSignerKey(forged); !errors.Is(err, ErrDatagramSignerKey) {
		t.Fatalf("ParseDatagramSignerKey(forged) = %v, want ErrDatagramSignerKey", err)
	}
	if _, err := ParseDatagramSignerKey(newSignedDatagram(t)); err != nil {
		t.Fatalf("ParseDatagramSignerKey(genuine) = %v", err)
	}
}

func TestVerifyIdentityRecordRefusesSmallOrderKey(t *testing.T) {
	address := forgedAddress(t)
	body, err := json.Marshal(identityRecordBodyWire{
		Address:  address.String(),
		PubKey:   forgedKeyBase64(),
		IssuedAt: 1780000000,
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	record := SignedIdentityRecord{
		Version: domain.IdentityRecordVersion,
		Body:    body,
		Sig:     edforgery.UniversalSignature(),
	}
	_, err = VerifyIdentityRecord(record, testRecordNetwork, address)
	if !errors.Is(err, identity.ErrInvalidPublicKey) || !errors.Is(err, ErrIdentityRecordMalformed) {
		t.Fatalf("VerifyIdentityRecord(forged) = %v, want a malformed-record error carrying ErrInvalidPublicKey", err)
	}
}

func TestVerifyLookupRequesterRefusesSmallOrderKey(t *testing.T) {
	requester := forgedAddress(t)
	dst := mustIdentity(t, vecDstAddress)
	payload := GetIdentityPayload{
		V:                 1,
		Requester:         requester,
		RequesterIssuedAt: vecIssuedAt,
		RequesterSig:      edforgery.UniversalSignature(),
	}
	err := VerifyLookupRequester(forgedKeyBase64(), testRecordNetwork, vectorAttemptID(), payload, dst)
	if !errors.Is(err, ErrLookupProofInvalid) || !errors.Is(err, identity.ErrInvalidPublicKey) {
		t.Fatalf("VerifyLookupRequester(forged) = %v, want ErrLookupProofInvalid carrying ErrInvalidPublicKey", err)
	}
}

func TestVerifyTargetProofRefusesSmallOrderKey(t *testing.T) {
	body := IdentityRecordBody{
		PubKey:  domain.PeerPublicKey(forgedKeyBase64()),
		Address: forgedAddress(t),
	}
	record := SignedIdentityRecord{Version: domain.IdentityRecordVersion, Body: []byte("{}"), Sig: edforgery.UniversalSignature()}
	err := VerifyTargetProof(edforgery.UniversalSignature(), body, testRecordNetwork, vectorAttemptID(),
		sha256.Sum256([]byte(vecRequestPayload)), record)
	if !errors.Is(err, ErrLookupProofInvalid) || !errors.Is(err, identity.ErrInvalidPublicKey) {
		t.Fatalf("VerifyTargetProof(forged) = %v, want ErrLookupProofInvalid carrying ErrInvalidPublicKey", err)
	}
}
