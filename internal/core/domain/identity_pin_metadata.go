package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"time"
	"unicode/utf8"
)

var ErrIdentityPinMetadata = errors.New("identity pin metadata invalid or unsupported")

// IdentityPinAllocationState describes storage allocation, never current trust
// or a running session's D1 priority. Its zero is explicitly absent.
type IdentityPinAllocationState uint8

const (
	IdentityPinAllocationAbsent IdentityPinAllocationState = iota
	IdentityPinAllocationPending
	IdentityPinAllocationEligible
)

// IdentityPinOrigin names a verified local fact; received claims are not origins.
type IdentityPinOrigin uint8

const (
	IdentityPinOriginAbsent IdentityPinOrigin = iota
	IdentityPinOriginLocalContact
	IdentityPinOriginSelectedSession
	IdentityPinOriginInboundSession
	IdentityPinOriginUserLookup
	IdentityPinOriginOutgoingSend
	IdentityPinOriginProvenNextHop
)

type IdentityPinEvidenceReference string

// IdentityPinAllocation holds one bounded fact for a class. Pending neighbour
// allocations reserve capacity but do not assert ten minutes of a live session.
type IdentityPinAllocation struct {
	State       IdentityPinAllocationState   `json:"state"`
	Origin      IdentityPinOrigin            `json:"origin"`
	At          time.Time                    `json:"at"`
	SourceGroup SourceGroup                  `json:"source_group,omitempty"`
	Reference   IdentityPinEvidenceReference `json:"reference,omitempty"`
}

// IdentityPinMetadata has at most four allocations and one global pin owner.
// Contact has no own class quota; it does not erase another allocation's count
// or prove current contact trust.
// The zero value is explicit unclassified protection and invents no provenance.
type IdentityPinMetadata struct {
	Contact  IdentityPinAllocation
	Selected IdentityPinAllocation
	Inbound  IdentityPinAllocation
	Relation IdentityPinAllocation
}
type identityPinMetadataWire struct {
	Version        uint8                  `json:"version"`
	Classification string                 `json:"classification"`
	Contact        *IdentityPinAllocation `json:"contact,omitempty"`
	Selected       *IdentityPinAllocation `json:"selected,omitempty"`
	Inbound        *IdentityPinAllocation `json:"inbound,omitempty"`
	Relation       *IdentityPinAllocation `json:"relation,omitempty"`
}

func validPinGroup(group SourceGroup) bool {
	switch group {
	case SourceGroupLoopback, SourceGroupTor, SourceGroupI2P, SourceGroupUnresolvedName:
		return true
	}
	prefix, err := netip.ParsePrefix(string(group))
	return err == nil && prefix == prefix.Masked() && prefix.String() == string(group) && ((prefix.Addr().Is4() && prefix.Bits() == 16) || (prefix.Addr().Is6() && prefix.Bits() == 32))
}
func (m IdentityPinMetadata) Validate() error {
	for index, allocation := range []IdentityPinAllocation{m.Contact, m.Selected, m.Inbound, m.Relation} {
		if allocation.State == IdentityPinAllocationAbsent {
			if allocation != (IdentityPinAllocation{}) {
				return ErrIdentityPinMetadata
			}
			continue
		}
		if allocation.State != IdentityPinAllocationPending && allocation.State != IdentityPinAllocationEligible {
			return ErrIdentityPinMetadata
		}
		if allocation.At.IsZero() || len(allocation.Reference) > 128 || !utf8.ValidString(string(allocation.Reference)) || len(allocation.SourceGroup) > 64 {
			return ErrIdentityPinMetadata
		}
		if allocation.SourceGroup != "" && !validPinGroup(allocation.SourceGroup) {
			return ErrIdentityPinMetadata
		}
		switch index {
		case 0:
			if allocation.State != IdentityPinAllocationEligible || allocation.Origin != IdentityPinOriginLocalContact {
				return ErrIdentityPinMetadata
			}
		case 1:
			if allocation.Origin != IdentityPinOriginSelectedSession || !validPinGroup(allocation.SourceGroup) {
				return ErrIdentityPinMetadata
			}
		case 2:
			if allocation.Origin != IdentityPinOriginInboundSession || !validPinGroup(allocation.SourceGroup) {
				return ErrIdentityPinMetadata
			}
		case 3:
			if allocation.State != IdentityPinAllocationEligible || (allocation.Origin != IdentityPinOriginUserLookup && allocation.Origin != IdentityPinOriginOutgoingSend && allocation.Origin != IdentityPinOriginProvenNextHop) {
				return ErrIdentityPinMetadata
			}
		}
	}
	return nil
}
func (m IdentityPinMetadata) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	wire := identityPinMetadataWire{Version: 1, Classification: "unclassified"}
	for _, pair := range []struct {
		allocation IdentityPinAllocation
		target     **IdentityPinAllocation
	}{{m.Contact, &wire.Contact}, {m.Selected, &wire.Selected}, {m.Inbound, &wire.Inbound}, {m.Relation, &wire.Relation}} {
		if pair.allocation.State == IdentityPinAllocationAbsent {
			continue
		}
		copy := pair.allocation
		copy.At = copy.At.UTC()
		*pair.target = &copy
		wire.Classification = "classified"
	}
	return json.Marshal(wire)
}
func (m *IdentityPinMetadata) UnmarshalJSON(raw []byte) error {
	var wire identityPinMetadataWire
	if err := decodeStrictPinJSON(raw, &wire); err != nil {
		return err
	}
	if wire.Version != 1 {
		return ErrIdentityPinMetadata
	}
	var result IdentityPinMetadata
	for _, pair := range []struct {
		source *IdentityPinAllocation
		target *IdentityPinAllocation
	}{{wire.Contact, &result.Contact}, {wire.Selected, &result.Selected}, {wire.Inbound, &result.Inbound}, {wire.Relation, &result.Relation}} {
		if pair.source != nil {
			if pair.source.State == IdentityPinAllocationAbsent {
				return ErrIdentityPinMetadata
			}
			*pair.target = *pair.source
		}
	}
	if err := result.Validate(); err != nil {
		return err
	}
	expected := "classified"
	if result == (IdentityPinMetadata{}) {
		expected = "unclassified"
	}
	if wire.Classification != expected {
		return ErrIdentityPinMetadata
	}
	*m = result
	return nil
}
func decodeStrictPinJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.Join(ErrIdentityPinMetadata, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrIdentityPinMetadata
	}
	return nil
}

// IdentityPinBackup is an independent, bounded, canonical public protection
// record retained by the external broker. It contains no identity private keys,
// runtime proof counter, confirmation secret or current-trust assertion.
type IdentityPinBackup struct {
	Version uint8                   `json:"version"`
	Payload string                  `json:"payload"`
	Digest  IdentityPinBackupDigest `json:"digest"`
}
type IdentityPinBackupDigest string

const MaxIdentityPinBackupBytes = 2048

type IdentityPinProtectionRecord struct {
	Identity PeerIdentity        `json:"identity"`
	PinnedAt time.Time           `json:"pinned_at"`
	Metadata IdentityPinMetadata `json:"metadata"`
}

func NewIdentityPinBackup(id PeerIdentity, at time.Time, metadata IdentityPinMetadata) (IdentityPinBackup, error) {
	if id.IsZero() || at.IsZero() {
		return IdentityPinBackup{}, ErrIdentityPinMetadata
	}
	record := IdentityPinProtectionRecord{Identity: id, PinnedAt: at.UTC(), Metadata: metadata}
	raw, err := json.Marshal(record)
	if err != nil {
		return IdentityPinBackup{}, err
	}
	if len(raw) > MaxIdentityPinBackupBytes {
		return IdentityPinBackup{}, ErrIdentityPinMetadata
	}
	digest := sha256.Sum256(append([]byte("corsa-identity-pin-backup-v1\x00"), raw...))
	return IdentityPinBackup{Version: 1, Payload: string(raw), Digest: IdentityPinBackupDigest(hex.EncodeToString(digest[:]))}, nil
}
func (b IdentityPinBackup) Restore(id PeerIdentity, at time.Time) (IdentityPinProtectionRecord, error) {
	if b.Version != 1 || len(b.Payload) == 0 || len(b.Payload) > MaxIdentityPinBackupBytes {
		return IdentityPinProtectionRecord{}, ErrIdentityPinMetadata
	}
	var record IdentityPinProtectionRecord
	if err := decodeStrictPinJSON([]byte(b.Payload), &record); err != nil {
		return IdentityPinProtectionRecord{}, err
	}
	if record.Identity != id || !record.PinnedAt.Equal(at) {
		return IdentityPinProtectionRecord{}, ErrIdentityPinMetadata
	}
	canonical, err := NewIdentityPinBackup(record.Identity, record.PinnedAt, record.Metadata)
	if err != nil || b != canonical {
		return IdentityPinProtectionRecord{}, ErrIdentityPinMetadata
	}
	return record, nil
}
