// Package startupfence implements the external restart fence. It never enables
// identity-pin removal by its presence: enrollment and a supervised startup are
// separate prerequisites, and the production node does not yet install them.
package startupfence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type StoreID string
type RuntimeEpoch string
type Generation uint64
type PinRevision uint64
type OperationID = domain.IdentityPinOperationID

// CredentialToken is transported in the authenticated protocol only; every
// formatting verb redacts it, including diagnostics such as %#v and %d.
type CredentialToken string

func (CredentialToken) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("[redacted]")) }

var (
	ErrFenced          = errors.New("startup_fenced")
	ErrChanged         = errors.New("fence_generation_changed")
	ErrReceiptRequired = errors.New("fence_receipt_required")
	ErrUnauthorized    = errors.New("fence_unauthorized")
	ErrInvalid         = errors.New("fence_invalid_request")
	ErrUnavailable     = errors.New("fence_unavailable_or_outcome_unknown")
	ErrUnsupported     = errors.New("fence_supervision_unsupported")
)

// Enrollment is an administrator's attestation, not a topology inference from
// a URL. Launcher configuration and credentials must be outside the node volume.
type Enrollment struct {
	Node                domain.PeerIdentity `json:"node"`
	Store               StoreID             `json:"store"`
	LauncherFingerprint string              `json:"launcher_fingerprint"`
	FaultDomain         string              `json:"fault_domain"`
	LauncherProfile     string              `json:"launcher_profile"`
}

type Binding struct {
	Node             domain.PeerIdentity      `json:"node"`
	Store            StoreID                  `json:"store"`
	RuntimeEpoch     RuntimeEpoch             `json:"runtime_epoch"`
	Generation       Generation               `json:"generation"`
	Identity         domain.PeerIdentity      `json:"identity"`
	PinRevision      PinRevision              `json:"pin_revision"`
	PreviousPinnedAt time.Time                `json:"previous_pinned_at"`
	Backup           domain.IdentityPinBackup `json:"backup"`
}

type StartupGrant struct {
	Node       domain.PeerIdentity `json:"node"`
	Store      StoreID             `json:"store"`
	Generation Generation          `json:"generation"`
	BootNonce  string              `json:"boot_nonce"`
	Token      CredentialToken     `json:"token"`
	ExpiresAt  time.Time           `json:"expires_at"`
}

type Lease struct {
	Node       domain.PeerIdentity `json:"node"`
	Store      StoreID             `json:"store"`
	Generation Generation          `json:"generation"`
	Token      CredentialToken     `json:"token"`
}

type RemovalPermit struct {
	Operation  OperationID `json:"operation"`
	Binding    Binding     `json:"binding"`
	Generation Generation  `json:"generation"`
}

type ReceiptKind string

const (
	ReceiptCleanCommitted     ReceiptKind = "clean_committed"
	ReceiptProtectionRestored ReceiptKind = "protection_restored"
)

type Receipt struct {
	Permit         RemovalPermit `json:"permit"`
	Kind           ReceiptKind   `json:"kind"`
	SnapshotDigest string        `json:"snapshot_digest"`
}

// RemovalFence is the core-facing contract. An error from BeginRemoval never
// authorises a local write. Recording a receipt NEVER grants another startup.
type RemovalFence interface {
	BeginRemoval(context.Context, Binding) (RemovalPermit, error)
	RecordReceipt(context.Context, Receipt) error
}

type State string

const (
	StateAllowed  State = "allowed"
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateFenced   State = "fenced"
)

type RecoveryAuthorization struct {
	Permit RemovalPermit `json:"permit"`
	Grant  StartupGrant  `json:"grant"`
}

type Status struct {
	Enrollment           Enrollment     `json:"enrollment"`
	State                State          `json:"state"`
	Generation           Generation     `json:"generation"`
	Permit               *RemovalPermit `json:"permit,omitempty"`
	Receipt              *Receipt       `json:"receipt,omitempty"`
	WriterActive         bool           `json:"writer_active"`
	LastFinalizedReceipt *Receipt       `json:"last_finalized_receipt,omitempty"`
	FinalizedAt          *time.Time     `json:"finalized_at,omitempty"`
}

func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func validDigest(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32 && value == hex.EncodeToString(raw)
}

func sameBinding(a, b Binding) bool {
	at, bt := a.PreviousPinnedAt, b.PreviousPinnedAt
	a.PreviousPinnedAt, b.PreviousPinnedAt = time.Time{}, time.Time{}
	return a == b && at.Equal(bt)
}
func samePermit(a, b RemovalPermit) bool {
	return a.Operation == b.Operation && a.Generation == b.Generation && sameBinding(a.Binding, b.Binding)
}
func sameReceipt(a, b Receipt) bool {
	return a.Kind == b.Kind && a.SnapshotDigest == b.SnapshotDigest && samePermit(a.Permit, b.Permit)
}
