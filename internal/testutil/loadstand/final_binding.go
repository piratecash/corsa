package loadstand

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrFinalAccountingUnavailable = errors.New("final process accounting unavailable")
var ErrFinalReportInvalid = errors.New("invalid final process report")

type LaunchNonce [32]byte
type LaunchFingerprint [32]byte

func canonicalDigest(text string, out []byte) error {
	if len(text) != 64 || text != strings.ToLower(text) {
		return ErrInvalidSample
	}
	raw, err := hex.DecodeString(text)
	if err != nil || len(raw) != 32 {
		return ErrInvalidSample
	}
	copy(out, raw)
	nonzero := false
	for _, b := range raw {
		nonzero = nonzero || b != 0
	}
	if !nonzero {
		return ErrInvalidSample
	}
	return nil
}
func (v LaunchNonce) String() string                     { return hex.EncodeToString(v[:]) }
func (v LaunchFingerprint) String() string               { return hex.EncodeToString(v[:]) }
func (v LaunchNonce) MarshalText() ([]byte, error)       { return []byte(v.String()), nil }
func (v LaunchFingerprint) MarshalText() ([]byte, error) { return []byte(v.String()), nil }
func (v *LaunchNonce) UnmarshalText(raw []byte) error    { return canonicalDigest(string(raw), v[:]) }
func (v *LaunchFingerprint) UnmarshalText(raw []byte) error {
	return canonicalDigest(string(raw), v[:])
}

type launchBinding struct {
	nonce       LaunchNonce
	fingerprint LaunchFingerprint
}

func newLaunchBinding(config ChildConfig, launchFileSHA string) (launchBinding, error) {
	var out launchBinding
	if _, err := rand.Read(out.nonce[:]); err != nil {
		return out, err
	}
	// This is parent-selected PUBLIC launch configuration correlation, not an
	// attestation of effective child configuration or a verified executable.
	raw, err := json.Marshal(struct {
		Version       uint8
		Config        ChildConfig
		LaunchFileSHA string
	}{1, config, launchFileSHA})
	if err != nil {
		return out, err
	}
	out.fingerprint = sha256.Sum256(raw)
	return out, nil
}

// CompletedStartBinding copies the existing member-owned successful receipt.
// Constructing this value does not itself establish successful member Start.
// Its constructor never obtains expected identity from a final/state report.
type CompletedStartBinding struct {
	launch  launchBinding
	receipt StartReceipt
}

func completedStartBinding(launch launchBinding, receipt StartReceipt) (CompletedStartBinding, error) {
	if launch.nonce == (LaunchNonce{}) || launch.fingerprint == (LaunchFingerprint{}) || receipt.Public.Identity.IsZero() || receipt.Incarnation.Ordinal == 0 || receipt.Incarnation.StartedAt.IsZero() || receipt.Public.PublicKey == "" || receipt.Public.BoxPublicKey == "" || receipt.Public.BoxSignature == "" {
		return CompletedStartBinding{}, ErrFinalAccountingUnavailable
	}
	return CompletedStartBinding{launch: launch, receipt: receipt}, nil
}
func (b CompletedStartBinding) RuntimeBinding() RuntimeBinding {
	return RuntimeBinding{Identity: b.receipt.Public.Identity, StartedAt: b.receipt.Incarnation.StartedAt}
}
func (b CompletedStartBinding) Receipt() StartReceipt { return b.receipt }

// TransportHighWater stores constant-size evidence for ONE immutable period.
// Invalid is permanent; it is never cleared by a later successful observation.
type TransportHighWater struct {
	binding  RuntimeBinding
	seen     bool
	invalid  bool
	sent     uint64
	received uint64
}

func NewTransportHighWater(binding RuntimeBinding) TransportHighWater {
	return TransportHighWater{binding: binding, invalid: binding.Identity.IsZero() || binding.StartedAt.IsZero()}
}
func (h *TransportHighWater) Observe(stats domain.TransportTrafficStats, err error) {
	if err != nil || !stats.StartedAt.Equal(h.binding.StartedAt) || stats.ReadAt.Before(stats.StartedAt) || (h.seen && (stats.BytesSent < h.sent || stats.BytesReceived < h.received)) {
		h.invalid = true
		return
	}
	h.seen = true
	h.sent = max(h.sent, stats.BytesSent)
	h.received = max(h.received, stats.BytesReceived)
}

func (v LaunchNonce) Format(s fmt.State, _ rune)       { _, _ = io.WriteString(s, v.String()) }
func (v LaunchFingerprint) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, v.String()) }
