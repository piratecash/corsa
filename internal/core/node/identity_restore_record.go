package node

import (
	"encoding/base64"
	"errors"
	"io"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// applyPriorIdentityRecord uses a signed prior self-record only as a floor.
// No keys or policy fields from the record become part of the restored identity.
func applyPriorIdentityRecord(dir *secretfile.Dir, name string, restored *identity.Identity) error {
	if err := validateIdentityBackupName(name); err != nil {
		return errors.New("invalid prior record name")
	}
	file, err := dir.OpenFileForRead(name)
	if err != nil {
		return errors.New("cannot open prior record")
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, domain.MaxIdentityRecordBytes+1))
	if err != nil || len(raw) > domain.MaxIdentityRecordBytes {
		return errors.New("cannot read prior record within its size limit")
	}
	prior, err := protocol.ParseSignedIdentityRecord(raw)
	if err != nil {
		return errors.New("invalid prior record")
	}
	network, err := domain.ParseNetworkID(networkName)
	if err != nil {
		return errors.New("invalid local identity record network")
	}
	owner, err := domain.ParsePeerIdentity(restored.Address)
	if err != nil {
		return errors.New("invalid restored identity")
	}
	body, err := protocol.VerifyIdentityRecord(prior, network, owner)
	if err != nil || string(body.PubKey) != base64.StdEncoding.EncodeToString(restored.PublicKey) {
		return errors.New("prior record does not verify for the restored identity and network")
	}
	restored.RecordSeqFloor = max(restored.RecordSeqFloor, uint64(body.Seq))
	return nil
}
