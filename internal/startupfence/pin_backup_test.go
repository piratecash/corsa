package startupfence

import (
	"encoding/json"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func pinBindingBackup(t testing.TB, b Binding) Binding {
	t.Helper()
	backup, err := domain.NewIdentityPinBackup(b.Identity, b.PreviousPinnedAt, domain.IdentityPinMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	b.Backup = backup
	return b
}

func TestBrokerRetainsFullBackupAndRefusesLegacyActivePermit(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	enrollment := testEnrollment()
	if err := ledger.Enroll(ctx, enrollment); err != nil {
		t.Fatal(err)
	}
	grant, err := ledger.PrepareStartup(ctx, enrollment.Node, "backup-test")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(30, 40).UTC()
	metadata := domain.IdentityPinMetadata{Relation: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginOutgoingSend, At: at, Reference: "sender-owned-message"}}
	backup, err := domain.NewIdentityPinBackup(enrollment.Node, at, metadata)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{Node: enrollment.Node, Store: enrollment.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: enrollment.Node, PinRevision: 1, PreviousPinnedAt: at, Backup: backup}
	permit, err := ledger.BeginRemoval(ctx, lease, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = OpenLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ledger.Close(); err != nil {
			t.Error(err)
		}
	}()
	status, err := ledger.Status(ctx, enrollment.Node)
	if err != nil {
		t.Fatal(err)
	}
	if status.Permit == nil || !samePermit(*status.Permit, permit) {
		t.Fatal("independent broker restart lost full protection backup")
	}
	restored, err := status.Permit.Binding.Backup.Restore(enrollment.Node, at)
	if err != nil || restored.Metadata != metadata {
		t.Fatalf("broker metadata: %v", err)
	}
	var raw []byte
	if err := ledger.db.QueryRowContext(ctx, "SELECT record FROM nodes WHERE node=?", enrollment.Node.String()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var record nodeRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	record.Status.Permit.Binding.Backup = domain.IdentityPinBackup{}
	legacy, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.db.ExecContext(ctx, "UPDATE nodes SET record=? WHERE node=?", legacy, enrollment.Node.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Status(ctx, enrollment.Node); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy active permit inferred unclassified: %v", err)
	}
	if _, err := ledger.PrepareRecovery(ctx, enrollment.Node, "unsafe-upgrade"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy permit admitted recovery: %v", err)
	}
	var after []byte
	if err := ledger.db.QueryRowContext(ctx, "SELECT record FROM nodes WHERE node=?", enrollment.Node.String()).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if string(after) != string(legacy) {
		t.Fatal("legacy permit was rewritten instead of refusing")
	}
}
