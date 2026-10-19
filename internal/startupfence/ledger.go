package startupfence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	_ "modernc.org/sqlite" // The broker's durable ledger, separate from node storage.
)

// Ledger serialises CAS operations, and acknowledges them only after SQLite's
// FULL-synchronous rollback-journal transaction commits. The broker storage is
// a provisioned, independent fault domain; loss of it is outside this model.
type Ledger struct {
	mu  sync.Mutex
	db  *sql.DB
	now func() time.Time
}
type nodeRecord struct {
	Status         Status        `json:"status"`
	Grant          *StartupGrant `json:"grant,omitempty"`
	Lease          *Lease        `json:"lease,omitempty"`
	RecoveryGrant  *StartupGrant `json:"recovery_grant,omitempty"`
	RecoveryActive *StartupGrant `json:"recovery_active,omitempty"`
}

func OpenLedger(ctx context.Context, path string) (*Ledger, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("ledger path: %w", ErrInvalid)
	}
	// Enrollment is administrative and this database contains no private keys.
	// A private directory also protects WAL/journal sidecars from other users.
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("ledger directory must be private: %w", ErrInvalid)
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(filepath.Base(path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if closeErr := file.Close(); closeErr != nil {
			return nil, closeErr
		}
	} else if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if err := secretfile.AuditLocation(path); err != nil {
		return nil, fmt.Errorf("ledger location: %w", err)
	}
	// PRAGMAs belong to every connection, including a replacement after an
	// interrupted HTTP request made database/sql discard the previous one.
	query := url.Values{}
	query.Add("_pragma", "journal_mode(DELETE)")
	query.Add("_pragma", "synchronous(EXTRA)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Set("mode", "rw")
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	fail := func(err error) (*Ledger, error) { _ = db.Close(); return nil, err }
	for _, stmt := range []string{"CREATE TABLE IF NOT EXISTS nodes (node TEXT PRIMARY KEY, record BLOB NOT NULL)"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fail(err)
		}
	}
	return &Ledger{db: db, now: time.Now}, nil
}
func (l *Ledger) Close() error { return l.db.Close() }

func (l *Ledger) Enroll(ctx context.Context, e Enrollment) error {
	if e.Node.IsZero() || e.Store == "" || e.LauncherFingerprint == "" || e.FaultDomain == "" || e.LauncherProfile != "linux-systemd-v1" {
		return ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	raw, err := json.Marshal(nodeRecord{Status: Status{Enrollment: e, State: StateAllowed}})
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, "INSERT INTO nodes(node,record) VALUES(?,?)", e.Node.String(), raw)
	return err
}

func (l *Ledger) read(ctx context.Context, node domain.PeerIdentity) (nodeRecord, error) {
	var raw []byte
	if err := l.db.QueryRowContext(ctx, "SELECT record FROM nodes WHERE node=?", node.String()).Scan(&raw); err != nil {
		return nodeRecord{}, err
	}
	var r nodeRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return nodeRecord{}, err
	}
	if err := validateRecord(r, node); err != nil {
		return nodeRecord{}, err
	}
	return r, nil
}

// change retains the mutex through commit. A timeout or lost reply is an
// unknown outcome to the caller; it must not advance a local destructive step.
func (l *Ledger) change(ctx context.Context, node domain.PeerIdentity, fn func(*nodeRecord) error) (nodeRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nodeRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM nodes WHERE node=?", node.String()).Scan(&raw); err != nil {
		return nodeRecord{}, err
	}
	var r nodeRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return nodeRecord{}, err
	}
	if err := validateRecord(r, node); err != nil {
		return nodeRecord{}, err
	}
	if err := fn(&r); err != nil {
		return nodeRecord{}, err
	}
	raw, err = json.Marshal(r)
	if err != nil {
		return nodeRecord{}, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE nodes SET record=? WHERE node=?", raw, node.String()); err != nil {
		return nodeRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodeRecord{}, err
	}
	return r, nil
}

func (l *Ledger) Status(ctx context.Context, node domain.PeerIdentity) (Status, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, err := l.read(ctx, node)
	return r.Status, err
}

func (l *Ledger) PrepareStartup(ctx context.Context, node domain.PeerIdentity, nonce string) (StartupGrant, error) {
	if len(nonce) == 0 || len(nonce) > 128 {
		return StartupGrant{}, ErrInvalid
	}
	token, err := randomToken()
	if err != nil {
		return StartupGrant{}, err
	}
	r, err := l.change(ctx, node, func(r *nodeRecord) error {
		if r.Status.State == StateFenced {
			return ErrFenced
		}
		// A new supervisor can replace an abandoned grant/lease only because the
		// enrolled launcher is trusted to contain the old child before starting.
		r.Status.Generation++
		r.Status.State = StateStarting
		r.Lease = nil
		r.RecoveryGrant = nil
		r.Grant = &StartupGrant{Node: node, Store: r.Status.Enrollment.Store, Generation: r.Status.Generation, BootNonce: nonce, Token: CredentialToken(token), ExpiresAt: l.now().UTC().Add(30 * time.Second)}
		return nil
	})
	if err != nil {
		return StartupGrant{}, err
	}
	return *r.Grant, nil
}

func (l *Ledger) ConsumeStartup(ctx context.Context, g StartupGrant) (Lease, error) {
	token, err := randomToken()
	if err != nil {
		return Lease{}, err
	}
	r, err := l.change(ctx, g.Node, func(r *nodeRecord) error {
		if r.Status.State == StateFenced {
			return ErrFenced
		}
		if r.Status.State != StateStarting || r.Grant == nil || *r.Grant != g || !l.now().Before(g.ExpiresAt) {
			return ErrChanged
		}
		r.Lease = &Lease{Node: g.Node, Store: g.Store, Generation: g.Generation, Token: CredentialToken(token)}
		r.Grant = nil
		r.Status.State = StateRunning
		return nil
	})
	if err != nil {
		return Lease{}, err
	}
	return *r.Lease, nil
}

func (l *Ledger) BeginRemoval(ctx context.Context, lease Lease, b Binding) (RemovalPermit, error) {
	if _, err := b.Backup.Restore(b.Identity, b.PreviousPinnedAt); err != nil {
		return RemovalPermit{}, ErrInvalid
	}
	if b.Identity.IsZero() || b.RuntimeEpoch == "" || len(b.RuntimeEpoch) > 128 || len(b.Store) > 128 || b.PinRevision == 0 || b.PreviousPinnedAt.IsZero() || b.Node != lease.Node || b.Store != lease.Store || b.Generation != lease.Generation {
		return RemovalPermit{}, ErrInvalid
	}
	op, err := randomToken()
	if err != nil {
		return RemovalPermit{}, err
	}
	r, err := l.change(ctx, b.Node, func(r *nodeRecord) error {
		if r.Status.State == StateFenced {
			return ErrFenced
		}
		if r.Status.State != StateRunning || r.Lease == nil || *r.Lease != lease || r.Status.Generation != b.Generation {
			return ErrChanged
		}
		r.Status.Generation++
		r.Status.State = StateFenced
		r.Grant = nil
		r.RecoveryGrant = nil
		r.Status.WriterActive = true
		r.Status.Permit = &RemovalPermit{Operation: OperationID(op), Binding: b, Generation: r.Status.Generation}
		r.Status.Receipt = nil
		return nil
	})
	if err != nil {
		return RemovalPermit{}, err
	}
	return *r.Status.Permit, nil
}

func (l *Ledger) RecordReceipt(ctx context.Context, receipt Receipt) error {
	if !validDigest(receipt.SnapshotDigest) || (receipt.Kind != ReceiptCleanCommitted && receipt.Kind != ReceiptProtectionRestored) {
		return ErrInvalid
	}
	_, err := l.change(ctx, receipt.Permit.Binding.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.Status.Permit == nil || !samePermit(*r.Status.Permit, receipt.Permit) {
			return ErrChanged
		}
		if r.Status.Receipt != nil && !receiptMayReplace(*r.Status.Receipt, receipt) {
			return ErrChanged
		}
		copied := receipt
		r.Status.Receipt = &copied
		return nil
	})
	return err
}

// Only the exact current recovery grant can replace an earlier restored
// snapshot. A restarted recovery may legitimately change its complete main.
func (l *Ledger) RecordRecoveryReceipt(ctx context.Context, grant StartupGrant, receipt Receipt) error {
	if receipt.Kind != ReceiptProtectionRestored || !validDigest(receipt.SnapshotDigest) {
		return ErrInvalid
	}
	_, err := l.change(ctx, grant.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || !r.Status.WriterActive || r.RecoveryActive == nil || *r.RecoveryActive != grant || r.Status.Permit == nil || !samePermit(*r.Status.Permit, receipt.Permit) {
			return ErrChanged
		}
		copied := receipt
		r.Status.Receipt = &copied
		return nil
	})
	return err
}

func receiptMayReplace(current, next Receipt) bool {
	if sameReceipt(current, next) {
		return true
	}
	return current.Kind == ReceiptCleanCommitted && next.Kind == ReceiptProtectionRestored
}

// PrepareRecovery grants no network startup and leaves StateFenced unchanged.
func (l *Ledger) PrepareRecovery(ctx context.Context, node domain.PeerIdentity, nonce string) (StartupGrant, error) {
	if len(nonce) == 0 || len(nonce) > 128 {
		return StartupGrant{}, ErrInvalid
	}
	token, err := randomToken()
	if err != nil {
		return StartupGrant{}, err
	}
	r, err := l.change(ctx, node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.Status.Permit == nil {
			return ErrChanged
		}
		r.RecoveryGrant = &StartupGrant{Node: node, Store: r.Status.Enrollment.Store, Generation: r.Status.Generation, BootNonce: nonce, Token: CredentialToken(token), ExpiresAt: l.now().UTC().Add(30 * time.Second)}
		return nil
	})
	if err != nil {
		return StartupGrant{}, err
	}
	return *r.RecoveryGrant, nil
}

func (l *Ledger) ConsumeRecovery(ctx context.Context, g StartupGrant) (RemovalPermit, error) {
	r, err := l.change(ctx, g.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.Status.Permit == nil || r.RecoveryGrant == nil || *r.RecoveryGrant != g || g.Generation != r.Status.Generation || !l.now().Before(g.ExpiresAt) {
			return ErrChanged
		}
		copied := g
		r.RecoveryActive = &copied
		r.RecoveryGrant = nil
		r.Status.WriterActive = true
		return nil
	})
	if err != nil {
		return RemovalPermit{}, err
	}
	return *r.Status.Permit, nil
}

func (l *Ledger) CompleteRecovery(ctx context.Context, g StartupGrant, receipt Receipt) error {
	_, err := l.change(ctx, g.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.Status.Permit == nil || !samePermit(*r.Status.Permit, receipt.Permit) || r.RecoveryActive == nil || *r.RecoveryActive != g {
			return ErrChanged
		}
		if receipt.Kind != ReceiptProtectionRestored || r.Status.Receipt == nil || !sameReceipt(*r.Status.Receipt, receipt) {
			return ErrReceiptRequired
		}
		r.RecoveryActive = nil
		r.Status.WriterActive = false
		return nil
	})
	return err
}

// Finalize is a separate, operator-authenticated action. It is never called by
// the core or by RecordReceipt. The exact receipt is the operator's selection.
func (l *Ledger) Finalize(ctx context.Context, receipt Receipt) (Status, error) {
	r, err := l.change(ctx, receipt.Permit.Binding.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.Status.Permit == nil || !samePermit(*r.Status.Permit, receipt.Permit) {
			return ErrChanged
		}
		if r.Status.Receipt == nil {
			return ErrReceiptRequired
		}
		if r.RecoveryActive != nil || r.Status.WriterActive {
			return ErrFenced
		}
		if !sameReceipt(*r.Status.Receipt, receipt) {
			return ErrChanged
		}
		copied := receipt
		at := l.now().UTC()
		r.Status.LastFinalizedReceipt = &copied
		r.Status.FinalizedAt = &at
		r.Status.State = StateAllowed
		r.Status.Generation++
		r.Grant = nil
		r.Lease = nil
		r.RecoveryGrant = nil
		r.Status.Permit = nil
		r.Status.Receipt = nil
		return nil
	})
	return r.Status, err
}

// SealWriter is called by the trusted launcher only after actual child exit0,
// never by the live node. An abnormal exit leaves the external fence intact.
func (l *Ledger) SealWriter(ctx context.Context, receipt Receipt) error {
	_, err := l.change(ctx, receipt.Permit.Binding.Node, func(r *nodeRecord) error {
		if r.Status.State != StateFenced || r.RecoveryActive != nil || r.Status.Permit == nil || !samePermit(*r.Status.Permit, receipt.Permit) {
			return ErrChanged
		}
		if r.Status.Receipt == nil || !sameReceipt(*r.Status.Receipt, receipt) {
			return ErrReceiptRequired
		}
		r.Status.WriterActive = false
		return nil
	})
	return err
}

func validateRecord(r nodeRecord, node domain.PeerIdentity) error {
	if r.Status.Permit != nil {
		b := r.Status.Permit.Binding
		if _, err := b.Backup.Restore(b.Identity, b.PreviousPinnedAt); err != nil {
			return ErrInvalid
		}
	}
	if node.IsZero() || r.Status.Enrollment.Node != node || r.Status.Enrollment.Store == "" {
		return ErrInvalid
	}
	switch r.Status.State {
	case StateAllowed:
		return nil
	case StateStarting:
		if r.Grant != nil && r.Grant.Node == node && r.Grant.Generation == r.Status.Generation {
			return nil
		}
	case StateRunning:
		if r.Lease != nil && r.Lease.Node == node && r.Lease.Generation == r.Status.Generation {
			return nil
		}
	case StateFenced:
		if r.Status.Permit != nil && r.Status.Permit.Binding.Node == node && r.Status.Permit.Generation == r.Status.Generation {
			return nil
		}
	}
	return ErrInvalid
}

func protocolError(err error) error {
	for _, known := range []error{ErrFenced, ErrChanged, ErrReceiptRequired, ErrUnauthorized, ErrInvalid, ErrUnsupported} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrUnavailable
}
