package identity

import (
	"crypto/ed25519"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"io"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

// StoredRecordSeqFloor reads the floor for owner from the installed identity
// file. An absent, unsafe, damaged, or unrelated file supplies no floor.
// The ordinary restore path still decides whether the file can be replaced.
// This read never upgrades the file or exposes key material.
func StoredRecordSeqFloor(path string, owner *Identity) uint64 {
	if owner == nil || len(owner.PrivateKey) != ed25519.PrivateKeySize {
		return 0
	}
	// Identity files written by Save contain two fixed-length keys and a
	// counter. Bound this optional read even when recovering a damaged file.
	const maxIdentityBytes = 4096
	dir, file, err := secretfile.OpenAuditedRead(path)
	if err != nil {
		return 0
	}
	defer func() { _ = dir.Close() }()
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxIdentityBytes+1))
	if err != nil || len(data) > maxIdentityBytes {
		return 0
	}
	var stored storedIdentity
	if err := json.Unmarshal(data, &stored); err != nil {
		return 0
	}
	key, err := base64.StdEncoding.DecodeString(stored.PrivateKey)
	if err != nil || !hmac.Equal(key, owner.PrivateKey) {
		return 0
	}
	return stored.RecordSeqFloor
}
