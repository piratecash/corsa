package protocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// datagramGoldenVector mirrors testdata/datagram_vector_v2.json field for
// field; the file name lives in datagramGoldenVectorFile. The file is the artefact §3.2 requires to ship with the spec, so
// it is kept as data rather than as Go literals: it can be copied into the
// document and re-used by an independent implementation unchanged.
type datagramGoldenVector struct {
	Description   string `json:"description"`
	Frame         string `json:"frame"`
	Network       string `json:"network"`
	PubKeyHex     string `json:"pubkey_hex"`
	ReplayKeyHex  string `json:"replay_key_hex"`
	SeedHex       string `json:"seed_hex"`
	SigB64URL     string `json:"sig_b64url"`
	TranscriptHex string `json:"transcript_hex"`
}

func loadDatagramGoldenVector(t *testing.T) datagramGoldenVector {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", datagramGoldenVectorFile))
	if err != nil {
		t.Fatalf("read golden vector: %v", err)
	}
	var vector datagramGoldenVector
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatalf("decode golden vector: %v", err)
	}
	return vector
}

// TestDatagramGoldenVector is the mandatory cross-implementation test
// vector of §3.2: frame JSON, transcript, sha256(transcript), private key
// and signature all compared BYTE for byte. Without it two independent
// implementations diverge on the first ambiguity.
func TestDatagramGoldenVector(t *testing.T) {
	vector := loadDatagramGoldenVector(t)

	seed, err := hex.DecodeString(vector.SeedHex)
	if err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	key := ed25519.NewKeyFromSeed(seed)
	publicKey := key.Public().(ed25519.PublicKey)
	if got := hex.EncodeToString(publicKey); got != vector.PubKeyHex {
		t.Fatalf("public key = %s, want %s", got, vector.PubKeyHex)
	}

	// The vector's own JSON is the input: parsing it proves the strict
	// parser accepts exactly the bytes the spec publishes.
	frame, err := ParseDatagramFrame([]byte(vector.Frame))
	if err != nil {
		t.Fatalf("ParseDatagramFrame(vector frame): %v", err)
	}
	reserialized, err := MarshalDatagramFrame(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrame: %v", err)
	}
	if string(reserialized) != vector.Frame {
		t.Fatalf("re-serialized frame differs from the vector:\n got %s\nwant %s", reserialized, vector.Frame)
	}

	network := domain.NetworkID(vector.Network)
	transcript, err := BuildDatagramTranscript(frame, network)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript: %v", err)
	}
	if got := hex.EncodeToString(transcript); got != vector.TranscriptHex {
		t.Fatalf("transcript mismatch:\n got %s\nwant %s", got, vector.TranscriptHex)
	}
	if got := DatagramReplayKey(transcript).String(); got != vector.ReplayKeyHex {
		t.Fatalf("replay key = %s, want %s", got, vector.ReplayKeyHex)
	}
	if got := base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, transcript)); got != vector.SigB64URL {
		t.Fatalf("signature = %s, want %s", got, vector.SigB64URL)
	}
	if got := base64.RawURLEncoding.EncodeToString(frame.Auth.Sig); got != vector.SigB64URL {
		t.Fatalf("frame signature = %s, want %s", got, vector.SigB64URL)
	}
	if err := VerifyDatagramSignature(frame, network); err != nil {
		t.Fatalf("VerifyDatagramSignature: %v", err)
	}
	if !DatagramSignerMatchesSrc(frame) {
		t.Fatal("Fingerprint(pubkey) does not match src in the golden vector")
	}

	// Signing the parsed frame again must reproduce the vector exactly:
	// Ed25519 is deterministic, so a differing byte means a differing
	// transcript, not a differing nonce.
	unsigned := frame.Clone()
	unsigned.Auth.Sig = nil
	resigned, err := SignDatagram(unsigned, network, key)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	if !bytes.Equal(resigned.Auth.Sig, frame.Auth.Sig) {
		t.Fatal("re-signing the vector frame produced a different signature")
	}
}

// TestDatagramTranscriptRejectsEmptyNetwork pins that the network segment
// can never be empty: a zero-length segment would make every network sign
// identically, which is exactly the re-binding attack the field prevents.
func TestDatagramTranscriptRejectsEmptyNetwork(t *testing.T) {
	frame := newSignedDatagram(t)
	if _, err := BuildDatagramTranscript(frame, ""); !errors.Is(err, domain.ErrInvalidNetworkID) {
		t.Fatalf("BuildDatagramTranscript with empty network: error = %v, want ErrInvalidNetworkID", err)
	}
}

// TestDatagramTranscriptCoversMiddlemanMutations walks the exact attack
// list of §3.2: every field a relay could rewrite to change routing,
// authority or meaning is inside the transcript, so each rewrite breaks the
// signature. `ttl` is the deliberate exception — it changes every hop.
func TestDatagramTranscriptCoversMiddlemanMutations(t *testing.T) {
	network := testDatagramNetwork
	mutations := map[string]func(*DatagramFrame){
		"class": func(d *DatagramFrame) { d.Class = domain.DatagramClassBulk },
		"route_policy": func(d *DatagramFrame) {
			d.RoutePolicy = domain.RoutePolicyExplore
		},
		"max_ttl": func(d *DatagramFrame) { d.Auth.MaxTTL = 20 },
		"dtype":   func(d *DatagramFrame) { d.DType = domain.DType("chunk_request") },
		"src":     func(d *DatagramFrame) { d.Src = mustIdentity(t, testDatagramDstHex) },
		"dst":     func(d *DatagramFrame) { d.Dst = mustIdentity(t, testDatagramSrcHex) },
		"payload": func(d *DatagramFrame) { d.Payload[0] ^= 0xff },
		"salt":    func(d *DatagramFrame) { d.Auth.Salt[0] ^= 0xff },
		"time":    func(d *DatagramFrame) { d.Auth.Time++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			mutated := newSignedDatagram(t).Clone()
			mutate(&mutated)
			if err := VerifyDatagramSignature(mutated, network); !errors.Is(err, ErrDatagramSignature) {
				t.Fatalf("mutating %s: error = %v, want ErrDatagramSignature", name, err)
			}
		})
	}

	// Re-binding the frame to another network is a mutation of the signing
	// context rather than of the frame, so it is exercised on the verifier.
	t.Run("network", func(t *testing.T) {
		if err := VerifyDatagramSignature(newSignedDatagram(t), "another-network"); !errors.Is(err, ErrDatagramSignature) {
			t.Fatalf("verifying under another network: error = %v, want ErrDatagramSignature", err)
		}
	})

	// Downgrading the mode is caught even earlier: routed without auth and
	// request with auth are both outside the §2.1 matrix, so the frame never
	// reaches the signature check at all.
	t.Run("mode", func(t *testing.T) {
		mutated := newSignedDatagram(t).Clone()
		mutated.Mode = domain.DatagramModeRequest
		if err := VerifyDatagramSignature(mutated, network); err == nil {
			t.Fatal("mode downgrade verified successfully")
		}
		signedRequest := mutated
		signedRequest.Auth = nil
		if _, err := BuildDatagramTranscript(signedRequest, network); err == nil {
			t.Fatal("an unsigned request built a transcript")
		}
	})

	// `av` is covered by the transcript like every field above, but it is the
	// one a middleman can no longer test AGAINST THE SIGNATURE: this build
	// implements exactly one auth version, so a rewritten `av` is refused as an
	// unknown version before any Ed25519 work. That is the stronger refusal —
	// it costs no verification and, unlike a broken signature, carries no ban
	// for the neighbour that merely relayed the frame.
	//
	// Coverage is therefore pinned on the transcript itself rather than through
	// a mutation: `av` is its first length-prefixed segment, so on the day a
	// second version exists a rewrite breaks the signature exactly as the
	// mutations above do.
	t.Run("av", func(t *testing.T) {
		mutated := newSignedDatagram(t).Clone()
		mutated.Auth.AuthVersion = domain.AuthVersionBase + 1
		if err := VerifyDatagramSignature(mutated, network); !errors.Is(err, ErrDatagramUnknownVersion) {
			t.Fatalf("rewriting av: error = %v, want ErrDatagramUnknownVersion", err)
		}
		// Nobody can sign a frame under an av this build does not implement
		// either, so there is no transcript for a forged profile to exist over.
		if _, err := BuildDatagramTranscript(mutated, network); !errors.Is(err, ErrDatagramUnknownVersion) {
			t.Fatalf("transcript over an unimplemented av: error = %v, want ErrDatagramUnknownVersion", err)
		}

		transcript, err := BuildDatagramTranscript(newSignedDatagram(t), network)
		if err != nil {
			t.Fatalf("BuildDatagramTranscript: %v", err)
		}
		// domain tag, separator, then lp(av): a 4-byte big-endian length of 1
		// followed by the version byte.
		want := append([]byte(datagramTranscriptDomain), 0x00, 0x00, 0x00, 0x00, 0x01, byte(domain.AuthVersionBase))
		if !bytes.HasPrefix(transcript, want) {
			t.Fatalf("av is not the first signed segment:\n got %x\nwant prefix %x", transcript, want)
		}
	})

	// ttl is NOT covered: it changes on every hop, so the same frame must
	// still verify after a legal decrement.
	t.Run("ttl is excluded", func(t *testing.T) {
		decremented := newSignedDatagram(t).Clone()
		decremented.TTL--
		if err := VerifyDatagramSignature(decremented, network); err != nil {
			t.Fatalf("a decremented ttl broke the signature: %v", err)
		}
	})
}

// TestDatagramTranscriptIsUnambiguous pins the reason every segment is
// length-prefixed: without lp() a longer dtype and a shorter payload would
// concatenate into the same byte string, and a second field combination
// with the same signature would be a matter of technique (§3.2).
func TestDatagramTranscriptIsUnambiguous(t *testing.T) {
	network := testDatagramNetwork
	base := newSignedDatagram(t).Clone()

	first := base.Clone()
	first.DType = domain.DType("ab")
	first.Payload = []byte("c")

	second := base.Clone()
	second.DType = domain.DType("a")
	second.Payload = []byte("bc")

	firstTranscript, err := BuildDatagramTranscript(first, network)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript(first): %v", err)
	}
	secondTranscript, err := BuildDatagramTranscript(second, network)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript(second): %v", err)
	}
	if bytes.Equal(firstTranscript, secondTranscript) {
		t.Fatal("two different dtype/payload combinations produced the same transcript")
	}
	if DatagramReplayKey(firstTranscript) == DatagramReplayKey(secondTranscript) {
		t.Fatal("two different frames produced the same replay key")
	}
}

// TestDatagramTranscriptEndsWithThePayload pins that the payload is the LAST
// segment of the v2 transcript: the four segments that used to follow the
// pubkey — req_caps, ext.cap, ext.v and ext.data — are gone from the envelope,
// and a transcript still reserving empty slots for them would sign a shape no
// parser can produce.
func TestDatagramTranscriptEndsWithThePayload(t *testing.T) {
	frame := newSignedDatagram(t).Clone()
	transcript, err := BuildDatagramTranscript(frame, testDatagramNetwork)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript: %v", err)
	}

	payloadSegment := make([]byte, 0, 4+len(frame.Payload))
	payloadSegment = append(payloadSegment, 0x00, 0x00, 0x00, byte(len(frame.Payload)))
	payloadSegment = append(payloadSegment, frame.Payload...)
	if !bytes.HasSuffix(transcript, payloadSegment) {
		t.Fatalf("transcript tail = %x, want suffix %x", transcript, payloadSegment)
	}
	// The pubkey segment immediately precedes it: nothing sits between them.
	pubKeySegment := make([]byte, 0, 4+len(frame.Auth.PubKey))
	pubKeySegment = append(pubKeySegment, 0x00, 0x00, 0x00, byte(len(frame.Auth.PubKey)))
	pubKeySegment = append(pubKeySegment, frame.Auth.PubKey...)
	if !bytes.HasSuffix(transcript, append(pubKeySegment, payloadSegment...)) {
		t.Fatalf("transcript tail = %x, want the pubkey segment directly before the payload", transcript)
	}
}

// TestDatagramReplayKeyIsSHA256OfTranscript pins the derivation itself: the
// key is never carried on the wire, so it has to be reproducible from the
// frame alone by any node that holds it (§3.1).
func TestDatagramReplayKeyIsSHA256OfTranscript(t *testing.T) {
	transcript, err := BuildDatagramTranscript(newSignedDatagram(t), testDatagramNetwork)
	if err != nil {
		t.Fatalf("BuildDatagramTranscript: %v", err)
	}
	want := sha256.Sum256(transcript)
	if got := DatagramReplayKey(transcript); got != domain.ReplayKey(want) {
		t.Fatalf("replay key = %s, want %s", got, hex.EncodeToString(want[:]))
	}
}

// TestDatagramSignerMatchesSrc covers the fingerprint gate of §3.1, kept
// separate from signature verification: a frame may carry a perfectly valid
// signature over a key that is not the one src names.
func TestDatagramSignerMatchesSrc(t *testing.T) {
	signed := newSignedDatagram(t)
	if !DatagramSignerMatchesSrc(signed) {
		t.Fatal("a correctly signed frame failed the fingerprint gate")
	}

	foreignSrc := signed.Clone()
	foreignSrc.Src = mustIdentity(t, testDatagramDstHex)
	if DatagramSignerMatchesSrc(foreignSrc) {
		t.Fatal("a frame carrying a stranger's src passed the fingerprint gate")
	}

	// A key of the wrong length is refused before any crypto is attempted:
	// there is nothing to fingerprint and nothing to verify.
	shortKey := signed.Clone()
	shortKey.Auth.PubKey = shortKey.Auth.PubKey[:16]
	if DatagramSignerMatchesSrc(shortKey) {
		t.Fatal("a truncated public key passed the fingerprint gate")
	}
	if err := VerifyDatagramSignature(shortKey, testDatagramNetwork); !errors.Is(err, ErrDatagramEncoding) {
		t.Fatalf("VerifyDatagramSignature with a truncated key: error = %v, want ErrDatagramEncoding", err)
	}

	noAuth := signed.Clone()
	noAuth.Auth = nil
	if DatagramSignerMatchesSrc(noAuth) {
		t.Fatal("a frame without auth passed the fingerprint gate")
	}
}

// TestSignDatagramRefusesAForeignPubKey pins that signing cannot silently
// overwrite a public key the caller already placed in the frame: doing so
// would turn "this frame claims key K" into "this frame is signed by
// whoever called us".
func TestSignDatagramRefusesAForeignPubKey(t *testing.T) {
	frame := newSignedDatagram(t).Clone()
	frame.Auth.PubKey = bytes.Repeat([]byte{0x01}, domain.DatagramPubKeyBytes)
	if _, err := SignDatagram(frame, testDatagramNetwork, testDatagramKey(t)); !errors.Is(err, ErrDatagramAuth) {
		t.Fatalf("SignDatagram with a foreign pubkey: error = %v, want ErrDatagramAuth", err)
	}
}
