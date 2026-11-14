package loadstand

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

func (s *ChildFinalSlot) PublishHeadlessAndScan(ctx context.Context, identity domain.PeerIdentity, stats domain.TransportTrafficStats, secrets [][]byte, credentials *Credentials) error {
	if err := s.checkHeadlessWriter(ctx); err != nil {
		return err
	}
	raw, err := s.headlessReport(identity, stats).Encode()
	if err != nil {
		return err
	}
	return s.publishHeadless(ctx, raw, FinalReportLimit, secrets, credentials)
}

// PublishHeadlessRouteWireAndScan writes the sealed route counters after the
// same Service shutdown as the ordinary headless report.
func (s *ChildFinalSlot) PublishHeadlessRouteWireAndScan(ctx context.Context, identity domain.PeerIdentity, stats domain.TransportTrafficStats, window LaunchNonce, bounds RouteWireWindowBounds, endpoints []RouteWireEndpoint, secrets [][]byte, credentials *Credentials) error {
	if err := s.checkHeadlessWriter(ctx); err != nil {
		return err
	}
	if window == (LaunchNonce{}) {
		return ErrFinalReportInvalid
	}
	report := HeadlessRouteWireReport{Version: 1, Window: window, Bounds: bounds, Transport: s.headlessReport(identity, stats), Endpoints: endpoints}
	raw, err := report.Encode()
	if err != nil {
		return err
	}
	return s.publishHeadless(ctx, raw, routeWireReportLimit(len(endpoints)), secrets, credentials)
}

func (s *ChildFinalSlot) headlessReport(identity domain.PeerIdentity, stats domain.TransportTrafficStats) HeadlessFinalReport {
	return HeadlessFinalReport{Version: 1, Nonce: s.binding.nonce, Fingerprint: s.binding.fingerprint, Identity: identity, StartedAt: stats.StartedAt, ReadAt: stats.ReadAt, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, Quiescent: true, Outcome: HeadlessShutdownSucceeded}
}

func (s *ChildFinalSlot) checkHeadlessWriter(ctx context.Context) error {
	if s == nil || s.writer == nil || s.scan == nil {
		return ErrFinalAccountingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return auditPrivateFinalFD(s.writer, false)
}

func (s *ChildFinalSlot) publishHeadless(ctx context.Context, raw []byte, limit ArtifactReadLimit, secrets [][]byte, credentials *Credentials) error {
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
	return errors.Join(ScanPublicArtifactFD(ctx, secrets, s.scan, limit), credentials.ScanPublicFD(ctx, s.scan, limit))
}
