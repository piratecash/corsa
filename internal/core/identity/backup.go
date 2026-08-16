package identity

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// backup.go implements the versioned full identity backup of the identity
// discovery layer (docs/protocol/identity-lookup.md): both private keys plus
// the identity-record seq counter, so a restored node keeps its address, its
// DM encryption key AND the monotonicity of its signed record.
//
// The legacy single-Ed25519-key import stays available as a separate,
// explicitly marked branch: it preserves the address but derives the box key
// from the Ed25519 seed, so an identity whose box key was generated randomly
// comes back with a DIFFERENT encryption key. Callers must surface that
// warning; the restored value carries the flag they need.

// IdentityBackupVersion is the only backup format version this build writes
// and understands. An unknown version is a reject, never a guess.
const IdentityBackupVersion = 1

// Backup failures are distinguishable sentinels so UI can tell "wrong file"
// from "future format" without matching error text.
var (
	// ErrBackupMalformed covers structural problems: not JSON, missing or
	// undecodable fields, key size violations, address mismatch.
	ErrBackupMalformed = errors.New("identity backup: malformed")

	// ErrBackupVersionUnsupported marks a backup written by a future build.
	ErrBackupVersionUnsupported = errors.New("identity backup: unsupported version")
)

// identityBackupFile is the on-disk shape of a versioned full backup.
// Address duplicates what the private key derives; it is stored so a human
// can tell backups apart and the import can detect a corrupted key early.
type identityBackupFile struct {
	PrivateKey    string `json:"private_key"`
	BoxPrivateKey string `json:"box_private_key"`
	Address       string `json:"address"`
	RecordSeq     uint64 `json:"record_seq"`
	Version       int    `json:"version"`
}

// RestoredIdentity is the result of any import branch.
type RestoredIdentity struct {
	Identity *Identity
	// RecordSeq is the identity-record seq counter at backup time. Zero for
	// the legacy branch, which predates records. The node must treat it as
	// a FLOOR: the first record issued after a restore starts above it.
	RecordSeq uint64
	// BoxKeyDerived is true when the box key was NOT restored but derived
	// from the Ed25519 seed (legacy branch). The address is preserved, the
	// encryption key may differ from the one peers knew — the caller is
	// obliged to warn the user.
	BoxKeyDerived bool
}

// ExportBackup renders the versioned full backup: both private keys and the
// current record seq.
func ExportBackup(id *Identity, recordSeq uint64) ([]byte, error) {
	if id == nil || id.BoxPrivateKey == nil {
		return nil, fmt.Errorf("%w: identity has no box private key", ErrBackupMalformed)
	}
	payload, err := json.MarshalIndent(identityBackupFile{
		Version:       IdentityBackupVersion,
		PrivateKey:    base64.StdEncoding.EncodeToString(id.PrivateKey),
		BoxPrivateKey: base64.StdEncoding.EncodeToString(id.BoxPrivateKey.Bytes()),
		Address:       id.Address,
		RecordSeq:     recordSeq,
	}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal identity backup: %w", err)
	}
	return payload, nil
}

// ImportBackup restores a versioned full backup: both keys come back
// exactly as exported, the address is re-derived and cross-checked against
// the stored one.
func ImportBackup(data []byte) (RestoredIdentity, error) {
	var stored identityBackupFile
	if err := json.Unmarshal(data, &stored); err != nil {
		return RestoredIdentity{}, fmt.Errorf("%w: %v", ErrBackupMalformed, err)
	}
	if stored.Version != IdentityBackupVersion {
		return RestoredIdentity{}, fmt.Errorf("%w: version %d", ErrBackupVersionUnsupported, stored.Version)
	}

	privBytes, err := base64.StdEncoding.DecodeString(stored.PrivateKey)
	if err != nil {
		return RestoredIdentity{}, fmt.Errorf("%w: decode private key: %v", ErrBackupMalformed, err)
	}
	if len(privBytes) != ed25519.PrivateKeySize {
		return RestoredIdentity{}, fmt.Errorf("%w: private key size %d", ErrBackupMalformed, len(privBytes))
	}
	boxBytes, err := base64.StdEncoding.DecodeString(stored.BoxPrivateKey)
	if err != nil {
		return RestoredIdentity{}, fmt.Errorf("%w: decode box private key: %v", ErrBackupMalformed, err)
	}
	boxPrivate, err := ecdh.X25519().NewPrivateKey(boxBytes)
	if err != nil {
		return RestoredIdentity{}, fmt.Errorf("%w: restore box private key: %v", ErrBackupMalformed, err)
	}

	privateKey := ed25519.PrivateKey(privBytes)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	address := Fingerprint(publicKey)
	if stored.Address != "" && stored.Address != address {
		return RestoredIdentity{}, fmt.Errorf("%w: address %q does not match key fingerprint %q",
			ErrBackupMalformed, stored.Address, address)
	}

	return RestoredIdentity{
		Identity: &Identity{
			PrivateKey:    privateKey,
			PublicKey:     publicKey,
			BoxPrivateKey: boxPrivate,
			BoxPublicKey:  boxPrivate.PublicKey().Bytes(),
			Address:       address,
			// The floor travels ON the identity so saving the restored
			// identity file persists it and the self-record issue path
			// starts above it with no extra plumbing.
			RecordSeqFloor: stored.RecordSeq,
		},
		RecordSeq: stored.RecordSeq,
	}, nil
}

// ImportLegacyEd25519 is the preserved legacy branch: restore from a bare
// base64 Ed25519 private key. The box key pair is derived deterministically
// from the seed, which is NOT the randomly generated box key a file identity
// originally had — the address survives, the encryption key may not, and
// BoxKeyDerived says so.
func ImportLegacyEd25519(privKeyBase64 string) (RestoredIdentity, error) {
	id, err := FromPrivateKeyBase64(privKeyBase64)
	if err != nil {
		return RestoredIdentity{}, err
	}
	return RestoredIdentity{Identity: id, BoxKeyDerived: true}, nil
}
