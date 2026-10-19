package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const (
	FindNodeFrameType        = "find_node"
	FindNodeAnswerFrameType  = "find_node_answer"
	MaxFindNodeFrameBytes    = 1024
	findNodeAnswerSigningTag = "corsa-find-node-answer-v1"
)

var (
	ErrFindNodeMalformed          = errors.New("find node: malformed frame")
	ErrFindNodeVersionUnsupported = errors.New("find node: unsupported version")
	ErrFindNodeKey                = errors.New("find node: unacceptable target key")
	ErrFindNodeAnswerBinding      = errors.New("find node: answer binding mismatch")
	ErrFindNodeSignature          = errors.New("find node: invalid answer signature")
)

// These codecs do not install handlers or grant overlay rights. The caller
// must establish the proven v2 session, negotiated capability and admission.
type FindNodeRequest struct {
	ID        domain.FindNodeRequestID
	Target    domain.PeerIdentity
	Challenge domain.FindNodeChallenge
}

type FindNodeAnswer struct {
	ID        domain.FindNodeRequestID
	Target    domain.PeerIdentity
	PublicKey identity.PublicKey
	Signature domain.FindNodeSignature
}

type findNodeRequestWire struct {
	Type      string                   `json:"type"`
	Version   uint8                    `json:"v"`
	ID        domain.FindNodeRequestID `json:"id"`
	Target    domain.PeerIdentity      `json:"target"`
	Challenge string                   `json:"challenge"`
}

type findNodeAnswerWire struct {
	Type      string                   `json:"type"`
	Version   uint8                    `json:"v"`
	ID        domain.FindNodeRequestID `json:"id"`
	Target    domain.PeerIdentity      `json:"target"`
	PublicKey string                   `json:"pubkey"`
	Signature string                   `json:"sig"`
}

func parseFindNodeFields(raw []byte, frameType string, names ...string) (map[string]json.RawMessage, error) {
	if len(raw) > MaxFindNodeFrameBytes {
		return nil, fmt.Errorf("%w: size cap exceeded", ErrFindNodeMalformed)
	}
	if err := scanDuplicateJSONKeys(raw, 4); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFindNodeMalformed, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFindNodeMalformed, err)
	}
	if len(fields) != len(names) {
		return nil, fmt.Errorf("%w: closed field set", ErrFindNodeMalformed)
	}
	for _, name := range names {
		value, exists := fields[name]
		if !exists || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: required field %s", ErrFindNodeMalformed, name)
		}
	}
	typeName, err := findNodeField[string](fields, "type")
	if err != nil {
		return nil, err
	}
	if typeName != frameType {
		return nil, fmt.Errorf("%w: frame type", ErrFindNodeMalformed)
	}
	version, err := findNodeField[uint8](fields, "v")
	if err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, ErrFindNodeVersionUnsupported
	}
	return fields, nil
}

func findNodeField[T any](fields map[string]json.RawMessage, name string) (T, error) {
	var value T
	if err := json.Unmarshal(fields[name], &value); err != nil {
		return value, fmt.Errorf("%w: %s: %w", ErrFindNodeMalformed, name, err)
	}
	return value, nil
}

func findNodeCorrelation(fields map[string]json.RawMessage) (domain.FindNodeRequestID, domain.PeerIdentity, error) {
	id, err := findNodeField[domain.FindNodeRequestID](fields, "id")
	if err != nil {
		return 0, domain.PeerIdentity{}, err
	}
	target, err := findNodeField[domain.PeerIdentity](fields, "target")
	if err != nil {
		return 0, domain.PeerIdentity{}, err
	}
	if target.IsZero() {
		return 0, domain.PeerIdentity{}, fmt.Errorf("%w: absent target", ErrFindNodeMalformed)
	}
	return id, target, nil
}

func decodeFindNodeBytes(fields map[string]json.RawMessage, name string, encoding *base64.Encoding, size int) ([]byte, error) {
	encoded, err := findNodeField[string](fields, name)
	if err != nil {
		return nil, err
	}
	raw, err := encoding.Strict().DecodeString(encoded)
	if err != nil || len(raw) != size || encoding.EncodeToString(raw) != encoded {
		return nil, fmt.Errorf("%w: noncanonical %s", ErrFindNodeMalformed, name)
	}
	return raw, nil
}

func ParseFindNodeRequest(raw []byte) (FindNodeRequest, error) {
	fields, err := parseFindNodeFields(raw, FindNodeFrameType, "type", "v", "id", "target", "challenge")
	if err != nil {
		return FindNodeRequest{}, err
	}
	id, target, err := findNodeCorrelation(fields)
	if err != nil {
		return FindNodeRequest{}, err
	}
	challenge, err := decodeFindNodeBytes(fields, "challenge", base64.RawURLEncoding, 32)
	if err != nil {
		return FindNodeRequest{}, err
	}
	return FindNodeRequest{ID: id, Target: target, Challenge: domain.FindNodeChallenge(challenge)}, nil
}

func ParseFindNodeAnswer(raw []byte) (FindNodeAnswer, error) {
	fields, err := parseFindNodeFields(raw, FindNodeAnswerFrameType, "type", "v", "id", "target", "pubkey", "sig")
	if err != nil {
		return FindNodeAnswer{}, err
	}
	id, target, err := findNodeCorrelation(fields)
	if err != nil {
		return FindNodeAnswer{}, err
	}
	keyBytes, err := decodeFindNodeBytes(fields, "pubkey", base64.StdEncoding, 32)
	if err != nil {
		return FindNodeAnswer{}, err
	}
	key, err := identity.ParsePublicKey(keyBytes)
	if err != nil {
		return FindNodeAnswer{}, fmt.Errorf("%w: %w", ErrFindNodeKey, err)
	}
	if key.Fingerprint() != target.String() {
		return FindNodeAnswer{}, ErrFindNodeKey
	}
	signature, err := decodeFindNodeBytes(fields, "sig", base64.RawURLEncoding, 64)
	if err != nil {
		return FindNodeAnswer{}, err
	}
	return FindNodeAnswer{ID: id, Target: target, PublicKey: key, Signature: domain.FindNodeSignature(signature)}, nil
}

func MarshalFindNodeRequest(request FindNodeRequest) ([]byte, error) {
	if request.Target.IsZero() {
		return nil, fmt.Errorf("%w: absent target", ErrFindNodeMalformed)
	}
	return json.Marshal(findNodeRequestWire{Type: FindNodeFrameType, Version: 1, ID: request.ID, Target: request.Target, Challenge: base64.RawURLEncoding.EncodeToString(request.Challenge[:])})
}

func MarshalFindNodeAnswer(answer FindNodeAnswer) ([]byte, error) {
	if !validFindNodeAnswerKey(answer) {
		return nil, ErrFindNodeKey
	}
	return json.Marshal(findNodeAnswerWire{Type: FindNodeAnswerFrameType, Version: 1, ID: answer.ID, Target: answer.Target, PublicKey: base64.StdEncoding.EncodeToString(answer.PublicKey.Bytes()), Signature: base64.RawURLEncoding.EncodeToString(answer.Signature[:])})
}

func validFindNodeAnswerKey(answer FindNodeAnswer) bool {
	return !answer.Target.IsZero() && len(answer.PublicKey.Bytes()) == 32 && answer.PublicKey.Fingerprint() == answer.Target.String()
}

func (request FindNodeRequest) MarshalJSON() ([]byte, error) { return MarshalFindNodeRequest(request) }
func (request *FindNodeRequest) UnmarshalJSON(raw []byte) error {
	parsed, err := ParseFindNodeRequest(raw)
	if err == nil {
		*request = parsed
	}
	return err
}
func (answer FindNodeAnswer) MarshalJSON() ([]byte, error) { return MarshalFindNodeAnswer(answer) }
func (answer *FindNodeAnswer) UnmarshalJSON(raw []byte) error {
	parsed, err := ParseFindNodeAnswer(raw)
	if err == nil {
		*answer = parsed
	}
	return err
}

func BuildFindNodeAnswerTranscript(network domain.NetworkID, target domain.PeerIdentity, challenge domain.FindNodeChallenge) ([]byte, error) {
	if _, err := domain.ParseNetworkID(network.String()); err != nil {
		return nil, err
	}
	if len(network.String()) > 65535 {
		return nil, fmt.Errorf("%w: network exceeds uint16 framing", domain.ErrInvalidNetworkID)
	}
	if target.IsZero() {
		return nil, fmt.Errorf("%w: absent target", ErrFindNodeMalformed)
	}
	transcript := identityLookupSigningDomain(findNodeAnswerSigningTag, network)
	transcript = append(transcript, target[:]...)
	return append(transcript, challenge[:]...), nil
}

// VerifyFindNodeAnswer binds the answer to the retained request, not to a
// challenge supplied by the reply. The caller also binds its original session.
func VerifyFindNodeAnswer(answer FindNodeAnswer, request FindNodeRequest, network domain.NetworkID) error {
	if answer.ID != request.ID || answer.Target != request.Target {
		return ErrFindNodeAnswerBinding
	}
	if !validFindNodeAnswerKey(answer) {
		return ErrFindNodeKey
	}
	transcript, err := BuildFindNodeAnswerTranscript(network, request.Target, request.Challenge)
	if err != nil {
		return err
	}
	if !answer.PublicKey.Verify(transcript, answer.Signature[:]) {
		return ErrFindNodeSignature
	}
	return nil
}
