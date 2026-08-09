package protocol

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
)

// TestMaxFileChunkFitsBulkDatagram is the size boundary §2.3 requires to be
// measured on real bytes rather than on filler: the largest chunk the file
// transport produces today, through the real encryption path, with every
// optional header field populated. It answers the only two questions the
// layer owes the migration — does the decoded payload fit the class cap,
// and does the whole line fit MaxFrameLine.
//
// The double-encoding trap of §2.3 is deliberately NOT tested here:
// EncryptFileCommandPayload returns base64 text, and whether the adapter
// decodes it once before building the datagram is the adapter's contract.
// The layer cannot tell "arbitrary bytes" from "bytes that happen to be
// base64 text", and pretending otherwise would be a test of nothing.
func TestMaxFileChunkFitsBulkDatagram(t *testing.T) {
	chunk := make([]byte, domain.DefaultChunkSize)
	if _, err := rand.Read(chunk); err != nil {
		t.Fatalf("random chunk: %v", err)
	}
	// Random bytes on purpose: the ciphertext of a compressible chunk is the
	// same size, but the JSON/base64 stages in between are not obliged to be.
	response, err := json.Marshal(domain.ChunkResponsePayload{
		FileID: domain.FileID("0123456789abcdef0123456789abcdef01234567"),
		Offset: 1 << 40,
		Data:   base64.StdEncoding.EncodeToString(chunk),
		Epoch:  1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("marshal chunk response: %v", err)
	}

	boxKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate box key: %v", err)
	}
	sealed, err := directmsg.EncryptFileCommandPayload(
		base64.StdEncoding.EncodeToString(boxKey.PublicKey().Bytes()),
		domain.FileCommandPayload{Command: domain.FileActionChunkResp, Data: response},
	)
	if err != nil {
		t.Fatalf("EncryptFileCommandPayload: %v", err)
	}
	// The adapter's obligation, exercised here so the sizes below are the
	// real ciphertext and not its base64 text.
	ciphertext, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil {
		t.Fatalf("decode sealed payload: %v", err)
	}

	frame := newSignedDatagram(t).Clone()
	frame.Class = domain.DatagramClassBulk
	frame.DType = domain.DType("chunk_response")
	frame.TTL = domain.DatagramDefaultMaxHops
	frame.Payload = ciphertext
	signed, err := SignDatagram(frame, testDatagramNetwork, testDatagramKey(t))
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}

	bulkCap, err := domain.DatagramPayloadCap(domain.DatagramClassBulk)
	if err != nil {
		t.Fatalf("DatagramPayloadCap: %v", err)
	}
	if signed.DecodedPayloadLen() > bulkCap {
		t.Fatalf("decoded ciphertext = %d bytes, exceeds the bulk cap %d", signed.DecodedPayloadLen(), bulkCap)
	}

	line, err := MarshalDatagramFrameLine(signed)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	if len(line) > MaxFrameLine {
		t.Fatalf("frame line = %d bytes, exceeds MaxFrameLine %d", len(line), MaxFrameLine)
	}
	parsed, err := ParseDatagramFrameLine(line)
	if err != nil {
		t.Fatalf("ParseDatagramFrameLine: %v", err)
	}
	if parsed.DecodedPayloadLen() != signed.DecodedPayloadLen() {
		t.Fatalf("round trip changed the payload size: %d, want %d",
			parsed.DecodedPayloadLen(), signed.DecodedPayloadLen())
	}
	if err := VerifyDatagramSignature(parsed, testDatagramNetwork); err != nil {
		t.Fatalf("VerifyDatagramSignature after round trip: %v", err)
	}

	// §2.3 sizes the maximum chunk at ≈ 22 046 decoded bytes — three times
	// under the bulk cap. Pin the order of magnitude so a change in the
	// chunk or command format that eats the headroom is visible here.
	if signed.DecodedPayloadLen() > bulkCap/2 {
		t.Fatalf("the largest chunk now takes %d of the %d byte bulk cap, headroom is gone",
			signed.DecodedPayloadLen(), bulkCap)
	}
}
