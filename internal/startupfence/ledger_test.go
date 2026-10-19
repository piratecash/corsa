package startupfence

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestFencedStateSurvivesRestartAndOnlyMatchingReceiptCanFinalize(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
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
	grant, err := ledger.PrepareStartup(ctx, enrollment.Node, "boot-one")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	binding := pinBindingBackup(t, Binding{Node: enrollment.Node, Store: enrollment.Store, RuntimeEpoch: "runtime", Generation: lease.Generation, Identity: domain.PeerIdentityFromWire("1111111111111111111111111111111111111111"), PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()})
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
	if _, err := ledger.PrepareStartup(ctx, enrollment.Node, "boot-two"); !errors.Is(err, ErrFenced) {
		t.Fatalf("startup after crash = %v, want fenced", err)
	}
	receipt := Receipt{Permit: permit, Kind: ReceiptCleanCommitted, SnapshotDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if _, err := ledger.Finalize(ctx, receipt); !errors.Is(err, ErrReceiptRequired) {
		t.Fatalf("finalize without receipt = %v", err)
	}
	if err := ledger.RecordReceipt(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if err := ledger.SealWriter(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.PrepareStartup(ctx, enrollment.Node, "boot-three"); !errors.Is(err, ErrFenced) {
		t.Fatalf("receipt unfenced: %v", err)
	}
	changed := receipt
	changed.SnapshotDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := ledger.Finalize(ctx, changed); !errors.Is(err, ErrChanged) {
		t.Fatalf("changed receipt = %v", err)
	}
	if _, err := ledger.Finalize(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.PrepareStartup(ctx, enrollment.Node, "boot-four"); err != nil {
		t.Fatal(err)
	}
}

func TestMissingPriorPinTimeCannotCreateFence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()
	e := testEnrollment()
	if err := ledger.Enroll(ctx, e); err != nil {
		t.Fatal(err)
	}
	grant, err := ledger.PrepareStartup(ctx, e.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	b := Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1}
	if _, err := ledger.BeginRemoval(ctx, lease, b); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing prior timestamp: %v", err)
	}
	status, err := ledger.Status(ctx, e.Node)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateRunning || status.Generation != lease.Generation {
		t.Fatal("invalid recovery data mutated broker")
	}
}

func TestReplacementConnectionRetainsExtraDurability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()
	conn, err := ledger.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("discard: %v", err)
	}
	_ = conn.Close()
	var synchronous int
	var journal string
	if err := ledger.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if err := ledger.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if synchronous != 3 || journal != "delete" {
		t.Fatalf("replacement durability: synchronous=%d journal=%s", synchronous, journal)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ledger mode = %o", info.Mode().Perm())
	}
}

func testEnrollment() Enrollment {
	return Enrollment{Node: domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Store: "store-a", LauncherFingerprint: "launcher", FaultDomain: "operator-attested:separate-host-b", LauncherProfile: "linux-systemd-v1"}
}

func TestStartupGrantCannotCrossRemovalOrBeReplayed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ledger.Close(); err != nil {
			t.Error(err)
		}
	}()
	e := testEnrollment()
	if err := ledger.Enroll(ctx, e); err != nil {
		t.Fatal(err)
	}
	old, err := ledger.PrepareStartup(ctx, e.Node, "old")
	if err != nil {
		t.Fatal(err)
	}
	current, err := ledger.PrepareStartup(ctx, e.Node, "current")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeStartup(ctx, old); !errors.Is(err, ErrChanged) {
		t.Fatalf("old grant = %v", err)
	}
	lease, err := ledger.ConsumeStartup(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeStartup(ctx, current); !errors.Is(err, ErrChanged) {
		t.Fatalf("replay = %v", err)
	}
	b := pinBindingBackup(t, Binding{Node: e.Node, Store: e.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: e.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()})
	if _, err := ledger.BeginRemoval(ctx, lease, b); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeStartup(ctx, current); !errors.Is(err, ErrFenced) {
		t.Fatalf("fenced grant = %v", err)
	}
}

func TestFreshRecoveryGrantCanReplaceRestoredSnapshotAndRejectStaleSelections(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenLedger(ctx, filepath.Join(directory, "ledger.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()
	enrollment := testEnrollment()
	if err := ledger.Enroll(ctx, enrollment); err != nil {
		t.Fatal(err)
	}
	startup, err := ledger.PrepareStartup(ctx, enrollment.Node, "boot")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := ledger.ConsumeStartup(ctx, startup)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := ledger.BeginRemoval(ctx, lease, pinBindingBackup(t, Binding{Node: enrollment.Node, Store: enrollment.Store, RuntimeEpoch: "epoch", Generation: lease.Generation, Identity: enrollment.Node, PinRevision: 1, PreviousPinnedAt: time.Unix(1, 0).UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	first, err := ledger.PrepareRecovery(ctx, enrollment.Node, "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeRecovery(ctx, first); err != nil {
		t.Fatal(err)
	}
	previous := Receipt{Permit: permit, Kind: ReceiptProtectionRestored, SnapshotDigest: strings.Repeat("a", 64)}
	if err := ledger.RecordRecoveryReceipt(ctx, first, previous); err != nil {
		t.Fatal(err)
	}
	next, err := ledger.PrepareRecovery(ctx, enrollment.Node, "retry")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConsumeRecovery(ctx, next); err != nil {
		t.Fatal(err)
	}
	updated := previous
	updated.SnapshotDigest = strings.Repeat("b", 64)
	if err := ledger.RecordReceipt(ctx, updated); !errors.Is(err, ErrChanged) {
		t.Fatalf("generic receipt replaced restored snapshot: %v", err)
	}
	if err := ledger.RecordRecoveryReceipt(ctx, first, updated); !errors.Is(err, ErrChanged) {
		t.Fatalf("stale grant replaced restored snapshot: %v", err)
	}
	clean := updated
	clean.Kind = ReceiptCleanCommitted
	if err := ledger.RecordRecoveryReceipt(ctx, next, clean); !errors.Is(err, ErrInvalid) {
		t.Fatalf("recovery could select clean: %v", err)
	}
	if err := ledger.RecordRecoveryReceipt(ctx, next, updated); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finalize(ctx, updated); !errors.Is(err, ErrFenced) {
		t.Fatalf("active recovery finalized: %v", err)
	}
	if err := ledger.CompleteRecovery(ctx, first, updated); !errors.Is(err, ErrChanged) {
		t.Fatalf("stale completion succeeded: %v", err)
	}
	if err := ledger.CompleteRecovery(ctx, next, previous); !errors.Is(err, ErrReceiptRequired) {
		t.Fatalf("old snapshot completed new recovery: %v", err)
	}
	if err := ledger.CompleteRecovery(ctx, next, updated); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RecordRecoveryReceipt(ctx, next, previous); !errors.Is(err, ErrChanged) {
		t.Fatalf("sealed recovery mutated snapshot: %v", err)
	}
	if _, err := ledger.Finalize(ctx, previous); !errors.Is(err, ErrChanged) {
		t.Fatalf("stale selection finalized: %v", err)
	}
	if _, err := ledger.Finalize(ctx, updated); err != nil {
		t.Fatal(err)
	}
}
