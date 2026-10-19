package loadstand

import (
	"context"
	"errors"
	"os"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ChildFinalSlot owns the inherited writer and one temporary same-inode scan
// duplicate. No pathname or expected identity is learned from artifact bytes.
type ChildFinalSlot struct {
	writer  *os.File
	scan    *os.File
	binding launchBinding
}

func ReadChildFinalSlot(getenv func(string) string) (*ChildFinalSlot, error) {
	nonce, fingerprint, fd := getenv("LOADSTAND_FINAL_NONCE"), getenv("LOADSTAND_FINAL_PLAN"), getenv("LOADSTAND_FINAL_FD")
	if nonce == "" && fingerprint == "" && fd == "" {
		return nil, nil
	}
	var binding launchBinding
	if fd != "3" || binding.nonce.UnmarshalText([]byte(nonce)) != nil || binding.fingerprint.UnmarshalText([]byte(fingerprint)) != nil {
		return nil, ErrInvalidConfiguration
	}
	file := os.NewFile(3, "loadstand-final-writer")
	if file == nil {
		return nil, ErrUnsupportedArtifact
	}
	if err := auditPrivateFinalFD(file, false); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	info, err := file.Stat()
	if err != nil || info.Size() != 0 {
		return nil, errors.Join(ErrUnsupportedArtifact, err, file.Close())
	}
	scan, err := duplicateFinalFD(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return &ChildFinalSlot{writer: file, scan: scan, binding: binding}, nil
}
func (s *ChildFinalSlot) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.writer != nil {
		err = s.writer.Close()
		s.writer = nil
	}
	if s.scan != nil {
		err = errors.Join(err, s.scan.Close())
		s.scan = nil
	}
	return err
}
func (s *ChildFinalSlot) PublishAndScan(ctx context.Context, identity domain.PeerIdentity, stats domain.TransportTrafficStats, secrets [][]byte, credentials *Credentials) error {
	if s == nil || s.writer == nil || s.scan == nil {
		return ErrFinalAccountingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := auditPrivateFinalFD(s.writer, false); err != nil {
		return err
	}
	report := FinalReport{Version: 1, Nonce: s.binding.nonce, Fingerprint: s.binding.fingerprint, Identity: identity, StartedAt: stats.StartedAt, ReadAt: stats.ReadAt, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, Quiescent: true, Outcome: SDKShutdownSucceeded}
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
