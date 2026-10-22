package loadstand

import (
	"encoding/json"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// HeadlessFinalReport keeps the existing exact launch-owned FD/binding and
// ten mandatory fields, without mislabelling a Full shutdown as an SDK result.
type HeadlessFinalReport struct {
	Version       uint8                   `json:"version"`
	Nonce         LaunchNonce             `json:"launch_nonce"`
	Fingerprint   LaunchFingerprint       `json:"plan_sha256"`
	Identity      domain.PeerIdentity     `json:"identity"`
	StartedAt     time.Time               `json:"started_at"`
	ReadAt        time.Time               `json:"read_at"`
	BytesSent     uint64                  `json:"bytes_sent"`
	BytesReceived uint64                  `json:"bytes_received"`
	Quiescent     bool                    `json:"transport_quiescent"`
	Outcome       HeadlessShutdownOutcome `json:"headless_shutdown_outcome"`
}
type HeadlessShutdownOutcome string

const HeadlessShutdownSucceeded HeadlessShutdownOutcome = "success"

func DecodeHeadlessFinalReport(raw []byte, binding CompletedStartBinding, prior TransportHighWater) (domain.TransportTrafficStats, error) {
	invalid := func() (domain.TransportTrafficStats, error) {
		return domain.TransportTrafficStats{}, ErrFinalReportInvalid
	}
	if len(raw) == 0 || int64(len(raw)) > int64(FinalReportLimit) {
		return invalid()
	}
	names := [...]string{"version", "launch_nonce", "plan_sha256", "identity", "started_at", "read_at", "bytes_sent", "bytes_received", "transport_quiescent", "headless_shutdown_outcome"}
	fields, err := object(raw, names[:]...)
	if err != nil || len(fields) != len(names) {
		return invalid()
	}
	var report HeadlessFinalReport
	if err := json.Unmarshal(raw, &report); err != nil {
		return invalid()
	}
	expected := binding.RuntimeBinding()
	if binding.launch.nonce == (LaunchNonce{}) || binding.launch.fingerprint == (LaunchFingerprint{}) || expected.Identity.IsZero() || expected.StartedAt.IsZero() || prior.invalid || prior.binding.Identity != expected.Identity || !prior.binding.StartedAt.Equal(expected.StartedAt) {
		return invalid()
	}
	if report.Version != 1 || report.Nonce != binding.launch.nonce || report.Fingerprint != binding.launch.fingerprint || report.Identity != expected.Identity || !report.StartedAt.Equal(expected.StartedAt) || report.ReadAt.IsZero() || report.ReadAt.Before(report.StartedAt) || !report.Quiescent || report.Outcome != HeadlessShutdownSucceeded {
		return invalid()
	}
	if prior.seen && (report.BytesSent < prior.sent || report.BytesReceived < prior.received) {
		return invalid()
	}
	return domain.TransportTrafficStats{StartedAt: report.StartedAt, ReadAt: report.ReadAt, BytesSent: report.BytesSent, BytesReceived: report.BytesReceived}, nil
}
func (r HeadlessFinalReport) Encode() ([]byte, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > int64(FinalReportLimit) {
		return nil, ErrArtifactTooLarge
	}
	return raw, nil
}
