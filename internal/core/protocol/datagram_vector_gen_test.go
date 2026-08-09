package protocol

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// updateGoldenVector is the standard golden-file switch: a flag of the TEST
// binary rather than an environment variable, so it cannot be exported once in
// a shell and then silently rewrite the vector on every later run, and so
// `go test -h` lists it where somebody looking for it would look.
var updateGoldenVector = flag.Bool("update-golden", false,
	"rewrite testdata/"+datagramGoldenVectorFile+" from the reference frame")

// datagramGoldenVectorFile is the artefact §3.2 ships with the spec. The name
// carries the header version, so a bump means a NEW file rather than an edited
// one: an implementation that still speaks the old version keeps a vector to
// check itself against.
const datagramGoldenVectorFile = "datagram_vector_v2.json"

// TestWriteDatagramGoldenVector regenerates the golden vector from the same
// reference frame the rest of this package signs.
//
// It is a test and not a standalone generator so the bytes it writes come from
// the production encoder through the very helper the assertions use — a
// separate program would be a second implementation of the transcript, and a
// golden vector produced by a second implementation proves nothing about the
// first.
//
// It is SKIPPED unless asked for, because a generator that runs on every `go
// test` cannot fail: it would rewrite the file to match whatever the code
// currently does, which is the opposite of a golden vector.
//
//	go test ./internal/core/protocol -run TestWriteDatagramGoldenVector -update-golden
//
// Then read the diff before committing: a change here is a WIRE change, and the
// diff is the only place it is visible as one.
func TestWriteDatagramGoldenVector(t *testing.T) {
	if !*updateGoldenVector {
		t.Skip("pass -update-golden to rewrite testdata/" + datagramGoldenVectorFile)
	}

	key := testDatagramKey(t)
	frame := newSignedDatagram(t)
	line, err := MarshalDatagramFrame(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrame: %v", err)
	}
	transcript, err := BuildDatagramTranscript(frame, testDatagramNetwork)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript: %v", err)
	}

	vector := datagramGoldenVector{
		Description: "corsa datagram transport golden vector, header v2, base auth profile (av=1). " +
			"The v1 envelope carried req_caps and ext; both are gone, and with them four transcript " +
			"segments. Reference: docs/refactoring/datagram-transport.md 3.2.",
		Frame:         string(line),
		Network:       testDatagramNetwork.String(),
		PubKeyHex:     hex.EncodeToString(key.Public().(ed25519.PublicKey)),
		ReplayKeyHex:  DatagramReplayKey(transcript).String(),
		SeedHex:       hex.EncodeToString(key.Seed()),
		SigB64URL:     base64.RawURLEncoding.EncodeToString(frame.Auth.Sig),
		TranscriptHex: hex.EncodeToString(transcript),
	}

	encoded, err := json.MarshalIndent(vector, "", "  ")
	if err != nil {
		t.Fatalf("marshal vector: %v", err)
	}
	path := filepath.Join("testdata", datagramGoldenVectorFile)
	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s — review the diff, it is a wire change", path)
}
