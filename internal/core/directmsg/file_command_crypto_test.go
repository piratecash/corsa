package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/crypto/ecdhgcm"
)

func TestEncryptDecryptFileCommandPayload(t *testing.T) {
	t.Parallel()

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	boxKeyBase64 := identity.BoxPublicKeyBase64(recipient.BoxPublicKey)

	inner, _ := json.Marshal(domain.ChunkRequestPayload{
		FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
		Offset: 32768,
		Size:   16384,
	})

	original := domain.FileCommandPayload{
		Command: domain.FileActionChunkReq,
		Data:    inner,
	}

	encrypted, err := EncryptFileCommandPayload(boxKeyBase64, original)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	if encrypted == "" {
		t.Fatal("encrypted payload is empty")
	}

	decrypted, err := DecryptFileCommandPayload(recipient, encrypted)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}

	if decrypted.Command != original.Command {
		t.Errorf("Command = %q, want %q", decrypted.Command, original.Command)
	}

	var req domain.ChunkRequestPayload
	if err := json.Unmarshal(decrypted.Data, &req); err != nil {
		t.Fatalf("unmarshal inner: %v", err)
	}

	if req.FileID != "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" {
		t.Errorf("FileID = %q", req.FileID)
	}
	if req.Offset != 32768 {
		t.Errorf("Offset = %d, want 32768", req.Offset)
	}
	if req.Size != 16384 {
		t.Errorf("Size = %d, want 16384", req.Size)
	}
}

func TestDecryptFileCommandPayloadWrongKey(t *testing.T) {
	t.Parallel()

	recipient, _ := identity.Generate()
	wrongRecipient, _ := identity.Generate()

	boxKeyBase64 := identity.BoxPublicKeyBase64(recipient.BoxPublicKey)

	inner, _ := json.Marshal(domain.FileDownloadedPayload{
		FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	})

	payload := domain.FileCommandPayload{
		Command: domain.FileActionDownloaded,
		Data:    inner,
	}

	encrypted, err := EncryptFileCommandPayload(boxKeyBase64, payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Decrypting with the wrong key should fail.
	_, err = DecryptFileCommandPayload(wrongRecipient, encrypted)
	if err == nil {
		t.Fatal("decrypt with wrong key should fail")
	}
}

func TestEncryptDecryptAllCommandTypes(t *testing.T) {
	t.Parallel()

	recipient, _ := identity.Generate()
	boxKey := identity.BoxPublicKeyBase64(recipient.BoxPublicKey)

	commands := []struct {
		name    string
		command domain.FileAction
		data    interface{}
	}{
		{
			"chunk_request",
			domain.FileActionChunkReq,
			domain.ChunkRequestPayload{
				FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
				Offset: 0,
				Size:   16384,
			},
		},
		{
			"chunk_response",
			domain.FileActionChunkResp,
			domain.ChunkResponsePayload{
				FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
				Offset: 0,
				Data:   "dGVzdC1jaHVuay1kYXRh",
			},
		},
		{
			"file_downloaded",
			domain.FileActionDownloaded,
			domain.FileDownloadedPayload{
				FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
			},
		},
		{
			"file_downloaded_ack",
			domain.FileActionDownloadedAck,
			domain.FileDownloadedAckPayload{
				FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
			},
		},
	}

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			inner, _ := json.Marshal(tc.data)
			payload := domain.FileCommandPayload{
				Command: tc.command,
				Data:    inner,
			}

			encrypted, err := EncryptFileCommandPayload(boxKey, payload)
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}

			decrypted, err := DecryptFileCommandPayload(recipient, encrypted)
			if err != nil {
				t.Fatalf("decrypt: %v", err)
			}

			if decrypted.Command != tc.command {
				t.Errorf("Command = %q, want %q", decrypted.Command, tc.command)
			}
		})
	}
}

func TestFileCommandKeyDomainSeparation(t *testing.T) {
	t.Parallel()

	// Verify domain separation: a payload encrypted with file-command label
	// cannot be decrypted by DM label (and vice versa). We test this
	// end-to-end by encrypting a file command, then attempting to decrypt
	// the raw sealed box with the DM label via ecdhgcm.Open.
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	boxKeyBase64 := identity.BoxPublicKeyBase64(recipient.BoxPublicKey)

	inner, _ := json.Marshal(domain.FileDownloadedPayload{
		FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	})
	payload := domain.FileCommandPayload{
		Command: domain.FileActionDownloaded,
		Data:    inner,
	}

	encrypted, err := EncryptFileCommandPayload(boxKeyBase64, payload)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Decrypting with the correct label must succeed.
	_, err = DecryptFileCommandPayload(recipient, encrypted)
	if err != nil {
		t.Fatalf("decrypt with correct label failed: %v", err)
	}

	// Manually reconstruct the sealed box and try to open with the DM label.
	combined, _ := base64.RawURLEncoding.DecodeString(encrypted)
	box := &ecdhgcm.SealedBox{
		EphemeralPub: combined[:32],
		Nonce:        combined[32:44],
		Ciphertext:   combined[44:],
	}

	_, err = ecdhgcm.Open(recipient.BoxPrivateKey, box, dmKeyLabel)
	if err == nil {
		t.Error("decrypting file command payload with DM label must fail (domain separation)")
	}
}
