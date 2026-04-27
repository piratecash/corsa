package directmsg

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// TestFileAnnounceDMRoundTrip verifies that a PlainMessage carrying a
// file_announce command (with Command and CommandData fields) survives
// encryption and decryption via the DM pipeline. This is the end-to-end
// test for the file_announce DM flow: Body is set to the "[file]" sentinel,
// Command is "file_announce", and CommandData holds the JSON-encoded
// FileAnnouncePayload.
func TestFileAnnounceDMRoundTrip(t *testing.T) {
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
		FileName:    "document.pdf",
		FileSize:    1048576,
		ContentType: "application/pdf",
		FileHash:    "abc123def456789012345678901234567890abcdef1234567890123456789012",
	}
	commandData, err := json.Marshal(announce)
	if err != nil {
		t.Fatalf("marshal announce: %v", err)
	}

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{
			Body:        domain.FileDMBodySentinel,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: string(commandData),
		},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants: %v", err)
	}

	// Decrypt as recipient.
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

	// Body must be the sentinel "[file]".
	if got.Body != domain.FileDMBodySentinel {
		t.Errorf("Body = %q, want %q", got.Body, domain.FileDMBodySentinel)
	}

	// Command must be "file_announce".
	if got.Command != string(domain.DMCommandFileAnnounce) {
		t.Errorf("Command = %q, want %q", got.Command, domain.DMCommandFileAnnounce)
	}

	// CommandData must round-trip to the original payload.
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
	if decoded.ContentType != announce.ContentType {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, announce.ContentType)
	}
	if decoded.FileHash != announce.FileHash {
		t.Errorf("FileHash = %q, want %q", decoded.FileHash, announce.FileHash)
	}
}

// TestFileAnnounceDMBothPartiesCanDecrypt verifies that both sender and
// recipient can decrypt a file_announce DM (dual-sealed envelope property).
func TestFileAnnounceDMBothPartiesCanDecrypt(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient: %v", err)
	}

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{
			Body:        domain.FileDMBodySentinel,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: `{"file_name":"test.txt","file_size":100,"content_type":"text/plain","file_hash":"deadbeef"}`,
		},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Recipient decrypts.
	gotRecipient, err := DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("recipient decrypt: %v", err)
	}
	if gotRecipient.Command != string(domain.DMCommandFileAnnounce) {
		t.Errorf("recipient Command = %q, want file_announce", gotRecipient.Command)
	}

	// Sender decrypts own copy.
	gotSender, err := DecryptForIdentity(
		sender,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("sender decrypt: %v", err)
	}
	if gotSender.Command != string(domain.DMCommandFileAnnounce) {
		t.Errorf("sender Command = %q, want file_announce", gotSender.Command)
	}
	if gotSender.Body != domain.FileDMBodySentinel {
		t.Errorf("sender Body = %q, want %q", gotSender.Body, domain.FileDMBodySentinel)
	}
}
