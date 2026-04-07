package gazeta

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
)

func TestEncryptDecryptForRecipient(t *testing.T) {
	t.Parallel()

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}

	encoded, err := EncryptForRecipient(
		identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		"dm",
		sender.Address,
		"secret-text",
	)
	if err != nil {
		t.Fatalf("EncryptForRecipient failed: %v", err)
	}

	plain, err := DecryptForIdentity(recipient, encoded)
	if err != nil {
		t.Fatalf("DecryptForIdentity failed: %v", err)
	}

	if plain.From != sender.Address {
		t.Fatalf("unexpected sender: got %q want %q", plain.From, sender.Address)
	}
	if plain.Body != "secret-text" {
		t.Fatalf("unexpected body: got %q", plain.Body)
	}

	other, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate other identity: %v", err)
	}

	if _, err := DecryptForIdentity(other, encoded); err == nil {
		t.Fatal("expected decryption to fail for non-recipient")
	}
}

func TestNoticeIDIsStable(t *testing.T) {
	t.Parallel()

	if got, want := ID("abc"), ID("abc"); got != want {
		t.Fatalf("expected stable id, got %q and %q", got, want)
	}
	if strings.TrimSpace(ID("abc")) == "" {
		t.Fatal("expected non-empty notice id")
	}
}
