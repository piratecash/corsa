package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/piratecash/corsa/internal/core/domain"
)

// IdentityMetadata is a derived snapshot companion, never a second live owner.
// Only format 1 may omit it; that format knows no class provenance.
func decodeSecureSessionFile(raw []byte) (secureSessionFile, error) {
	var file secureSessionFile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return file, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return file, domain.ErrIdentityPinMetadata
	}
	if err := validateNinObjectKeys(raw, file.Format == secureSessionJournalFormat); err != nil {
		return file, err
	}
	if err := validateSecureSessionPinClasses(file); err != nil {
		return file, err
	}
	if file.Format == 1 {
		file.Format = secureSessionStoreFormat
		file.IdentityMetadata = make(map[domain.PeerIdentity]domain.IdentityPinMetadata, len(file.Identities))
		for id := range file.Identities {
			file.IdentityMetadata[id] = domain.IdentityPinMetadata{}
		}
	}
	return file, nil
}
func validateSecureSessionPinClasses(file secureSessionFile) error {
	switch file.Format {
	case 1:
		if file.InboundQuota != nil {
			return errNinJournal
		}
		if len(file.IdentityMetadata) != 0 {
			return domain.ErrIdentityPinMetadata
		}
	case secureSessionStoreFormat, secureSessionJournalFormat:
		if file.Format == secureSessionStoreFormat && file.InboundQuota != nil {
			return errNinJournal
		}
		if file.Format == secureSessionJournalFormat {
			if err := validateNinImage(file.InboundQuota); err != nil {
				return err
			}
		}
		if len(file.IdentityMetadata) != len(file.Identities) {
			return domain.ErrIdentityPinMetadata
		}
		for id, metadata := range file.IdentityMetadata {
			if _, found := file.Identities[id]; !found {
				return domain.ErrIdentityPinMetadata
			}
			if err := metadata.Validate(); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown secure session store format %d", file.Format)
	}
	return nil
}
