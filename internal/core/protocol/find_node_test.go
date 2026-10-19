package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const findNodeRequestVector = `{"type":"find_node","v":1,"id":1,"target":"ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee","challenge":"MDEyMzQ1Njc4OTo7PD0-P0BBQkNERUZHSElKS0xNTk8"}`
const findNodeAnswerVector = `{"type":"find_node_answer","v":1,"id":1,"target":"ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee","pubkey":"rcFAEfgtHFbZVqpPnXPYhYNhpgYEhSXg0Ixjjcdd2Mc=","sig":"FOwSiGy2LQxHWggQwEqFkt6EnJpSi-qzRTeDXFGFdvOfAQ30SWrkuJQSESSP7v36aykP2C1qckZhfrI8uIydCQ"}`

func TestFindNodePublishedVectors(t *testing.T) {
	request, err := ParseFindNodeRequest([]byte(findNodeRequestVector))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := ParseFindNodeAnswer([]byte(findNodeAnswerVector))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		encoded  []byte
		expected string
	}{
		{mustFindNodeRequest(t, request), findNodeRequestVector},
		{mustFindNodeAnswer(t, answer), findNodeAnswerVector},
	} {
		if string(item.encoded) != item.expected {
			t.Fatalf("wire mismatch: %s", item.encoded)
		}
	}
	transcript, err := BuildFindNodeAnswerTranscript(domain.NetworkID("gazeta-devnet"), request.Target, request.Challenge)
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript) != 93 {
		t.Fatalf("transcript length: %d", len(transcript))
	}
	hash := sha256.Sum256(transcript)
	if hex.EncodeToString(hash[:]) != "5cb7e64704431c96f0eb7883b6b6909a26bf115f2b02f48878c6e8852c6c0dfe" {
		t.Fatalf("transcript hash: %x", hash)
	}
	if err := VerifyFindNodeAnswer(answer, request, domain.NetworkID("gazeta-devnet")); err != nil {
		t.Fatal(err)
	}
}

func TestFindNodeStrictParsing(t *testing.T) {
	for name, raw := range map[string]string{
		"duplicate":           strings.Replace(findNodeRequestVector, `"id":1`, `"id":1,"id":2`, 1),
		"duplicate escaped":   strings.Replace(findNodeRequestVector, `"id":1`, `"id":1,"\u0069d":2`, 1),
		"ambiguous type":      strings.Replace(findNodeRequestVector, `"type":"find_node"`, `"type":"find_node","type":"find_node_answer"`, 1),
		"uppercase":           strings.Replace(findNodeRequestVector, `"type"`, `"TYPE"`, 1),
		"unknown":             strings.TrimSuffix(findNodeRequestVector, "}") + `,"unknown":{}}`,
		"null id":             strings.Replace(findNodeRequestVector, `"id":1`, `"id":null`, 1),
		"missing id":          strings.Replace(findNodeRequestVector, `"id":1,`, ``, 1),
		"overflow":            strings.Replace(findNodeRequestVector, `"id":1`, `"id":18446744073709551616`, 1),
		"negative":            strings.Replace(findNodeRequestVector, `"id":1`, `"id":-1`, 1),
		"fraction":            strings.Replace(findNodeRequestVector, `"id":1`, `"id":1.0`, 1),
		"string id":           strings.Replace(findNodeRequestVector, `"id":1`, `"id":"1"`, 1),
		"null target":         strings.Replace(findNodeRequestVector, `"target":"ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee"`, `"target":null`, 1),
		"empty challenge":     strings.Replace(findNodeRequestVector, `MDEyMzQ1Njc4OTo7PD0-P0BBQkNERUZHSElKS0xNTk8`, ``, 1),
		"noncanonical base64": strings.Replace(findNodeRequestVector, `NTk8`, `NTk9`, 1),
		"trailing":            findNodeRequestVector + `{}`,
		"size":                findNodeRequestVector + strings.Repeat(" ", 1024),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseFindNodeRequest([]byte(raw)); !errors.Is(err, ErrFindNodeMalformed) {
				t.Fatalf("error: %v", err)
			}
		})
	}
	for _, raw := range []string{
		strings.TrimSuffix(findNodeAnswerVector, "}") + `,"issued_at":1}`,
		strings.Replace(findNodeAnswerVector, `"sig":`, `"SIG":`, 1),
		strings.Replace(findNodeAnswerVector, `rcFAEfgtHFbZVqpPnXPYhYNhpgYEhSXg0Ixjjcdd2Mc=`, `AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=`, 1),
		strings.Replace(findNodeAnswerVector, `"id":1`, `"id":null`, 1),
	} {
		if _, err := ParseFindNodeAnswer([]byte(raw)); err == nil {
			t.Fatalf("invalid answer accepted: %s", raw)
		}
	}
	if _, err := ParseFindNodeRequest([]byte(strings.Replace(findNodeRequestVector, `"v":1`, `"v":2`, 1))); !errors.Is(err, ErrFindNodeVersionUnsupported) {
		t.Fatalf("version error: %v", err)
	}
	for _, id := range []string{"0", "18446744073709551615"} {
		if _, err := ParseFindNodeRequest([]byte(strings.Replace(findNodeRequestVector, `"id":1`, `"id":`+id, 1))); err != nil {
			t.Fatalf("valid id %s: %v", id, err)
		}
	}
}

func TestFindNodeAnswerBindings(t *testing.T) {
	request, err := ParseFindNodeRequest([]byte(findNodeRequestVector))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := ParseFindNodeAnswer([]byte(findNodeAnswerVector))
	if err != nil {
		t.Fatal(err)
	}
	type bindingCase struct {
		name    string
		request FindNodeRequest
		answer  FindNodeAnswer
		network domain.NetworkID
	}
	tests := []bindingCase{
		{"network", request, answer, "other"},
		{"empty network", request, answer, ""},
		{"long network", request, answer, domain.NetworkID(strings.Repeat("a", 65536))},
	}
	changed := request
	changed.Challenge[0]++
	tests = append(tests, bindingCase{"challenge", changed, answer, "gazeta-devnet"})
	changed = request
	changed.ID++
	tests = append(tests, bindingCase{"id", changed, answer, "gazeta-devnet"})
	changed = request
	changed.Target[0]++
	tests = append(tests, bindingCase{"target", changed, answer, "gazeta-devnet"})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := VerifyFindNodeAnswer(test.answer, test.request, test.network); err == nil {
				t.Fatal("altered binding accepted")
			}
		})
	}
}

func TestFindNodeZeroKeyCannotBeMarshaledAsItsEmptyHash(t *testing.T) {
	var key identity.PublicKey
	target, err := domain.ParsePeerIdentity(key.Fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MarshalFindNodeAnswer(FindNodeAnswer{Target: target, PublicKey: key}); !errors.Is(err, ErrFindNodeKey) {
		t.Fatalf("zero key marshal: %v", err)
	}
}

func TestFindNodeJSONMethodsKeepStrictCodec(t *testing.T) {
	var request FindNodeRequest
	if err := json.Unmarshal([]byte(findNodeRequestVector), &request); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil || string(raw) != findNodeRequestVector {
		t.Fatalf("request JSON: %s %v", raw, err)
	}
	if err := json.Unmarshal([]byte(strings.Replace(findNodeRequestVector, `"id":1`, `"id":null`, 1)), &request); err == nil {
		t.Fatal("JSON bypass accepted null")
	}
	if string(mustFindNodeRequest(t, request)) != findNodeRequestVector {
		t.Fatal("failed parse mutated receiver")
	}
	var answer FindNodeAnswer
	if err := json.Unmarshal([]byte(findNodeAnswerVector), &answer); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(answer)
	if err != nil || string(raw) != findNodeAnswerVector {
		t.Fatalf("answer JSON: %s %v", raw, err)
	}
}

func TestFindNodeRejectsSmallOrderAnswerKeys(t *testing.T) {
	// Neutral keys and aliases cannot become proofs for their own fingerprint.
	for _, encoded := range []string{
		"0100000000000000000000000000000000000000000000000000000000000000",
		"0000000000000000000000000000000000000000000000000000000000000000",
		"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	} {
		key, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		raw := strings.Replace(findNodeAnswerVector, "ba8112fa4ba3d6f934b2ad2aa06966023d56e1ee", identity.Fingerprint(key), 1)
		raw = strings.Replace(raw, "rcFAEfgtHFbZVqpPnXPYhYNhpgYEhSXg0Ixjjcdd2Mc=", base64.StdEncoding.EncodeToString(key), 1)
		if _, err := ParseFindNodeAnswer([]byte(raw)); !errors.Is(err, ErrFindNodeKey) {
			t.Fatalf("key %s: %v", encoded, err)
		}
	}
}

func mustFindNodeRequest(t *testing.T, request FindNodeRequest) []byte {
	t.Helper()
	raw, err := MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func mustFindNodeAnswer(t *testing.T, answer FindNodeAnswer) []byte {
	t.Helper()
	raw, err := MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
