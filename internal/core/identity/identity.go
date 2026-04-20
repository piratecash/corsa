package identity

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// AddressLength is the fixed length of an identity address in hex characters.
// Derived from SHA-256(Ed25519 public key), truncated to 20 bytes = 40 hex chars.
const AddressLength = 40

// validAddressRe matches exactly 40 lowercase hex characters.
var validAddressRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

// ValidateAddress checks that addr is a well-formed identity address:
// exactly 40 lowercase hexadecimal characters. Returns nil if valid.
func ValidateAddress(addr string) error {
	if len(addr) != AddressLength {
		return fmt.Errorf("identity address must be %d hex chars, got %d", AddressLength, len(addr))
	}
	if !validAddressRe.MatchString(addr) {
		return fmt.Errorf("identity address must be lowercase hex, got %q", addr)
	}
	return nil
}

// IsValidAddress returns true if addr is a well-formed identity address.
func IsValidAddress(addr string) bool {
	return ValidateAddress(addr) == nil
}

type Identity struct {
	PrivateKey    ed25519.PrivateKey
	PublicKey     ed25519.PublicKey
	BoxPrivateKey *ecdh.PrivateKey
	BoxPublicKey  []byte
	Address       string
}

type storedIdentity struct {
	PrivateKey    string `json:"private_key"`
	BoxPrivateKey string `json:"box_private_key,omitempty"`
}

// Load reads an existing identity file at path. Unlike LoadOrCreate, it
// never generates a new key pair — returns an error when the file is missing.
func Load(path string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read identity %s: %w", path, err)
	}
	id, err := decodeIdentity(data)
	if err != nil {
		return nil, err
	}
	// Re-save to upgrade the format (e.g. add box key if missing).
	if err := save(path, id); err != nil {
		return nil, err
	}
	return id, nil
}

// FromPrivateKeyBase64 restores an Identity from a base64-encoded Ed25519
// private key string. The X25519 box key pair is derived deterministically
// from the Ed25519 seed using HMAC-SHA256 with domain separation — the same
// input always produces the same Identity. This is the primary way for SDK
// consumers to supply identity without relying on file-based storage.
func FromPrivateKeyBase64(privKeyBase64 string) (*Identity, error) {
	privBytes, err := base64.StdEncoding.DecodeString(privKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("decode private key base64: %w", err)
	}
	if len(privBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: got %d, want %d", len(privBytes), ed25519.PrivateKeySize)
	}
	privateKey := ed25519.PrivateKey(privBytes)
	publicKey := privateKey.Public().(ed25519.PublicKey)

	boxPrivate, boxPublic, err := deriveBoxKeyPair(privateKey.Seed())
	if err != nil {
		return nil, err
	}

	return &Identity{
		PrivateKey:    privateKey,
		PublicKey:     publicKey,
		BoxPrivateKey: boxPrivate,
		BoxPublicKey:  boxPublic,
		Address:       Fingerprint(publicKey),
	}, nil
}

// deriveBoxKeyPair deterministically derives an X25519 key pair from an
// Ed25519 seed. Uses HMAC-SHA256 with domain-separated key to produce
// 32 bytes suitable for X25519 private key material.
func deriveBoxKeyPair(seed []byte) (*ecdh.PrivateKey, []byte, error) {
	mac := hmac.New(sha256.New, []byte("corsa-box-key-derivation-v1"))
	mac.Write(seed)
	derived := mac.Sum(nil) // 32 bytes

	curve := ecdh.X25519()
	boxPrivate, err := curve.NewPrivateKey(derived)
	if err != nil {
		return nil, nil, fmt.Errorf("derive box private key: %w", err)
	}
	return boxPrivate, boxPrivate.PublicKey().Bytes(), nil
}

func LoadOrCreate(path string) (*Identity, error) {
	if data, err := os.ReadFile(path); err == nil {
		id, err := decodeIdentity(data)
		if err != nil {
			return nil, err
		}
		if err := save(path, id); err != nil {
			return nil, err
		}
		return id, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read identity %s: %w", path, err)
	}

	id, err := Generate()
	if err != nil {
		return nil, err
	}

	if err := save(path, id); err != nil {
		return nil, err
	}

	return id, nil
}

func Generate() (*Identity, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}

	boxPrivateKey, boxPublicKey, err := generateBoxKeyPair()
	if err != nil {
		return nil, err
	}

	return &Identity{
		PrivateKey:    privateKey,
		PublicKey:     publicKey,
		BoxPrivateKey: boxPrivateKey,
		BoxPublicKey:  boxPublicKey,
		Address:       Fingerprint(publicKey),
	}, nil
}

func Fingerprint(publicKey ed25519.PublicKey) string {
	sum := sha256.Sum256(publicKey)
	return hex.EncodeToString(sum[:20])
}

func PublicKeyBase64(publicKey ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(publicKey)
}

func BoxPublicKeyBase64(publicKey []byte) string {
	return base64.StdEncoding.EncodeToString(publicKey)
}

func SignBoxKeyBinding(id *Identity) string {
	signature := ed25519.Sign(id.PrivateKey, boxKeyBindingPayload(id.Address, BoxPublicKeyBase64(id.BoxPublicKey)))
	return base64.RawURLEncoding.EncodeToString(signature)
}

func SignPayload(id *Identity, payload []byte) string {
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(id.PrivateKey, payload))
}

func VerifyBoxKeyBinding(address, publicKeyBase64, boxKeyBase64, signatureBase64 string) error {
	publicKeyBytes, err := base64.StdEncoding.DecodeString(publicKeyBase64)
	if err != nil {
		return fmt.Errorf("decode public key: %w", err)
	}
	if len(publicKeyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key size: %d", len(publicKeyBytes))
	}

	publicKey := ed25519.PublicKey(publicKeyBytes)
	if Fingerprint(publicKey) != address {
		return fmt.Errorf("public key fingerprint mismatch")
	}

	signature, err := base64.RawURLEncoding.DecodeString(signatureBase64)
	if err != nil {
		return fmt.Errorf("decode box key signature: %w", err)
	}

	if !ed25519.Verify(publicKey, boxKeyBindingPayload(address, boxKeyBase64), signature) {
		return fmt.Errorf("invalid box key signature")
	}

	return nil
}

func VerifyPayload(address, publicKeyBase64 string, payload []byte, signatureBase64 string) error {
	publicKeyBytes, err := base64.StdEncoding.DecodeString(publicKeyBase64)
	if err != nil {
		return fmt.Errorf("decode public key: %w", err)
	}
	if len(publicKeyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key size: %d", len(publicKeyBytes))
	}

	publicKey := ed25519.PublicKey(publicKeyBytes)
	if Fingerprint(publicKey) != address {
		return fmt.Errorf("public key fingerprint mismatch")
	}

	signature, err := base64.RawURLEncoding.DecodeString(signatureBase64)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

func boxKeyBindingPayload(address, boxKeyBase64 string) []byte {
	return []byte("corsa-boxkey-v1|" + address + "|" + boxKeyBase64)
}

// Save persists the identity to the given file path. The parent directory
// is created automatically if it does not exist.
func Save(path string, id *Identity) error {
	return save(path, id)
}

func save(path string, id *Identity) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create identity directory: %w", err)
	}

	payload, err := json.MarshalIndent(storedIdentity{
		PrivateKey:    base64.StdEncoding.EncodeToString(id.PrivateKey),
		BoxPrivateKey: base64.StdEncoding.EncodeToString(id.BoxPrivateKey.Bytes()),
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal identity: %w", err)
	}

	if err := os.WriteFile(path, payload, 0o600); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}

	return nil
}

func decodeIdentity(data []byte) (*Identity, error) {
	var stored storedIdentity
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("decode identity json: %w", err)
	}

	privateKey, err := base64.StdEncoding.DecodeString(stored.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("decode private key: %w", err)
	}

	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: %d", len(privateKey))
	}

	pub := ed25519.PrivateKey(privateKey).Public().(ed25519.PublicKey)

	var boxPrivate *ecdh.PrivateKey
	var boxPublic []byte
	if stored.BoxPrivateKey != "" {
		boxBytes, err := base64.StdEncoding.DecodeString(stored.BoxPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("decode box private key: %w", err)
		}

		curve := ecdh.X25519()
		boxPrivate, err = curve.NewPrivateKey(boxBytes)
		if err != nil {
			return nil, fmt.Errorf("restore box private key: %w", err)
		}
		boxPublic = boxPrivate.PublicKey().Bytes()
	} else {
		var err error
		boxPrivate, boxPublic, err = generateBoxKeyPair()
		if err != nil {
			return nil, err
		}
	}

	return &Identity{
		PrivateKey:    ed25519.PrivateKey(privateKey),
		PublicKey:     pub,
		BoxPrivateKey: boxPrivate,
		BoxPublicKey:  boxPublic,
		Address:       Fingerprint(pub),
	}, nil
}

func generateBoxKeyPair() (*ecdh.PrivateKey, []byte, error) {
	curve := ecdh.X25519()
	privateKey, err := curve.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("generate x25519 key: %w", err)
	}

	return privateKey, privateKey.PublicKey().Bytes(), nil
}
