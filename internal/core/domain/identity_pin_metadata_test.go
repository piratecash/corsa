package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPinBackupCanonicalAndBounded(t *testing.T) {
	id := PeerIdentity{4}
	at := time.Unix(100, 99).UTC()
	metadata := IdentityPinMetadata{Contact: IdentityPinAllocation{State: IdentityPinAllocationEligible, Origin: IdentityPinOriginLocalContact, At: at, Reference: IdentityPinEvidenceReference(strings.Repeat("c", 128))}, Selected: IdentityPinAllocation{State: IdentityPinAllocationPending, Origin: IdentityPinOriginSelectedSession, At: at, SourceGroup: "192.0.0.0/16", Reference: IdentityPinEvidenceReference(strings.Repeat("s", 128))}, Inbound: IdentityPinAllocation{State: IdentityPinAllocationEligible, Origin: IdentityPinOriginInboundSession, At: at, SourceGroup: SourceGroupLoopback, Reference: IdentityPinEvidenceReference(strings.Repeat("i", 128))}, Relation: IdentityPinAllocation{State: IdentityPinAllocationEligible, Origin: IdentityPinOriginProvenNextHop, At: at, Reference: IdentityPinEvidenceReference(strings.Repeat("r", 128))}}
	backup, err := NewIdentityPinBackup(id, at, metadata)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := backup.Restore(id, at)
	if err != nil || restored.Metadata != metadata {
		t.Fatalf("canonical full record: %v", err)
	}
	if len(backup.Payload) > MaxIdentityPinBackupBytes {
		t.Fatal("unbounded backup")
	}
	raw, err := json.Marshal(backup)
	if err != nil {
		t.Fatal(err)
	}
	var decoded IdentityPinBackup
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != backup {
		t.Fatal("backup wire changed record")
	}
	for _, bad := range []IdentityPinBackup{{}, {Version: 2, Payload: backup.Payload, Digest: backup.Digest}, {Version: 1, Payload: backup.Payload + " ", Digest: backup.Digest}, {Version: 1, Payload: backup.Payload, Digest: "broken"}, {Version: 1, Payload: strings.Repeat("x", MaxIdentityPinBackupBytes+1)}} {
		if _, err := bad.Restore(id, at); !errors.Is(err, ErrIdentityPinMetadata) {
			t.Fatalf("invalid backup accepted: %v", err)
		}
	}
	if _, err := backup.Restore(PeerIdentity{5}, at); err == nil {
		t.Fatal("backup changed identity")
	}
	if _, err := backup.Restore(id, at.Add(time.Second)); err == nil {
		t.Fatal("backup changed timestamp")
	}
}
func TestPinMetadataRefusesUnknownOrInventedFacts(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"version":2,"classification":"unclassified"}`, `{"version":1,"classification":"unclassified","surprise":true}`, `{"version":1,"classification":"classified"}`, `{"version":1,"classification":"unclassified","contact":{"state":0}}`, `{"version":1,"classification":"classified","relation":{"state":2,"origin":3,"at":"2026-10-18T00:00:00Z"}}`} {
		var metadata IdentityPinMetadata
		if err := json.Unmarshal([]byte(raw), &metadata); err == nil {
			t.Fatalf("invalid metadata accepted: %s", raw)
		}
	}
	raw, err := json.Marshal(IdentityPinMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"version":1,"classification":"unclassified"}` {
		t.Fatalf("migration has implicit provenance: %s", raw)
	}
}
