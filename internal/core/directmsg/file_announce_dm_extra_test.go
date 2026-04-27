package directmsg

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// TestFileAnnounceDMWithUserDescription verifies that a PlainMessage carrying
// a file_announce command with Body set to user-provided description text
// (not the "[file]" sentinel) survives DM encryption round-trip.
func TestFileAnnounceDMWithUserDescription(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient: %v", err)
	}

	announce := domain.FileAnnouncePayload{
		FileName:    "quarterly-report.pdf",
		FileSize:    2_097_152,
		ContentType: "application/pdf",
		FileHash:    "abc123def456789012345678901234567890abcdef1234567890123456789012",
	}
	commandData, err := json.Marshal(announce)
	if err != nil {
		t.Fatalf("marshal announce: %v", err)
	}

	userDescription := "Here's the Q4 quarterly report for your review"

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{
			Body:        userDescription,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: string(commandData),
		},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants: %v", err)
	}

	got, err := DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}

	// Body must be the user description text, not the sentinel.
	if got.Body != userDescription {
		t.Errorf("Body = %q, want %q", got.Body, userDescription)
	}

	if got.Command != string(domain.DMCommandFileAnnounce) {
		t.Errorf("Command = %q, want %q", got.Command, domain.DMCommandFileAnnounce)
	}

	if got.CommandData == "" {
		t.Fatal("CommandData is empty after decrypt")
	}

	var decoded domain.FileAnnouncePayload
	if err := json.Unmarshal([]byte(got.CommandData), &decoded); err != nil {
		t.Fatalf("unmarshal CommandData: %v", err)
	}

	if decoded.FileName != announce.FileName {
		t.Errorf("FileName = %q, want %q", decoded.FileName, announce.FileName)
	}
	if decoded.FileSize != announce.FileSize {
		t.Errorf("FileSize = %d, want %d", decoded.FileSize, announce.FileSize)
	}
}

// TestFileCommandEncryptedPayloadRoundTrip verifies that a FileCommandPayload
// (chunk_request) encrypted with EncryptFileCommandPayload and decrypted with
// DecryptFileCommandPayload preserves all fields through the full
// encrypt → wire-encode → wire-decode → decrypt cycle.
func TestFileCommandEncryptedPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	boxKeyBase64 := identity.BoxPublicKeyBase64(recipient.BoxPublicKey)

	// Build a chunk_request with specific values.
	inner, _ := json.Marshal(domain.ChunkRequestPayload{
		FileID: "roundtrip-file-id-test",
		Offset: 65536,
		Size:   16384,
	})

	original := domain.FileCommandPayload{
		Command: domain.FileActionChunkReq,
		Data:    inner,
	}

	// Encrypt.
	encrypted, err := EncryptFileCommandPayload(boxKeyBase64, original)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Decrypt.
	decrypted, err := DecryptFileCommandPayload(recipient, encrypted)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}

	// Verify command type preserved.
	if decrypted.Command != original.Command {
		t.Errorf("Command = %q, want %q", decrypted.Command, original.Command)
	}

	// Verify inner payload preserved.
	var req domain.ChunkRequestPayload
	if err := json.Unmarshal(decrypted.Data, &req); err != nil {
		t.Fatalf("unmarshal inner payload: %v", err)
	}

	if req.FileID != "roundtrip-file-id-test" {
		t.Errorf("FileID = %q, want roundtrip-file-id-test", req.FileID)
	}
	if req.Offset != 65536 {
		t.Errorf("Offset = %d, want 65536", req.Offset)
	}
	if req.Size != 16384 {
		t.Errorf("Size = %d, want 16384", req.Size)
	}
}
