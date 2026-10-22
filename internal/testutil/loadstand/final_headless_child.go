package loadstand

import (
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
)

func (s *ChildFinalSlot) PublishHeadlessAndScan(ctx context.Context, identity domain.PeerIdentity, stats domain.TransportTrafficStats, secrets [][]byte, credentials *Credentials) error {
	if s == nil || s.writer == nil || s.scan == nil {
		return ErrFinalAccountingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := auditPrivateFinalFD(s.writer, false); err != nil {
		return err
	}
	report := HeadlessFinalReport{Version: 1, Nonce: s.binding.nonce, Fingerprint: s.binding.fingerprint, Identity: identity, StartedAt: stats.StartedAt, ReadAt: stats.ReadAt, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, Quiescent: true, Outcome: HeadlessShutdownSucceeded}
	raw, err := report.Encode()
	if err != nil {
		return err
	}
	if _, err := s.writer.WriteAt(raw, 0); err != nil {
		return err
	}
	if err := s.writer.Truncate(int64(len(raw))); err != nil {
		return err
	}
	if err := s.writer.Sync(); err != nil {
		return err
	}
	if err := s.writer.Close(); err != nil {
		s.writer = nil
		return err
	}
	s.writer = nil
	return errors.Join(ScanPublicArtifactFD(ctx, secrets, s.scan, FinalReportLimit), credentials.ScanPublicFD(ctx, s.scan, FinalReportLimit))
}
