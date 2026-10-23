package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// This bridge exists only in the existing test harness. Production measurement
// wiring and the Linux process sampler are deliberately absent.
type loadStandProcessSource struct{ source *loadstand.ProcessSource }

var _ loadStandNodeSource = loadStandProcessSource{}

func (s loadStandProcessSource) TransportTraffic(ctx context.Context) (domain.TransportTrafficStats, error) {
	return s.source.TransportTraffic(ctx)
}
func (s loadStandProcessSource) SessionOutcomes(ctx context.Context) (domain.SessionOutcomeStats, error) {
	return s.source.SessionOutcomes(ctx)
}
func (s loadStandProcessSource) ModeSelection(ctx context.Context) (routing.ModeSelectionStats, error) {
	return s.source.ModeSelection(ctx)
}
func (s loadStandProcessSource) Neighbours(ctx context.Context) (domain.NeighbourComposition, error) {
	return s.source.Neighbours(ctx)
}
func (s loadStandProcessSource) RouteEntries(ctx context.Context) (int, error) {
	return s.source.RouteEntries(ctx)
}
func (s loadStandProcessSource) Resources(ctx context.Context) (domain.ResourceBreakdown, error) {
	return s.source.Resources(ctx)
}
func (s loadStandProcessSource) DatagramSummary(ctx context.Context) (loadStandReading[loadStandDatagramSample], error) {
	reading, err := s.source.DatagramSummary(ctx)
	if err != nil {
		return loadStandReading[loadStandDatagramSample]{}, err
	}
	sample, err := reading.Get()
	if errors.Is(err, loadstand.ErrDatagramDisabled) {
		return loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled), nil
	}
	if err != nil {
		return loadStandReading[loadStandDatagramSample]{}, err
	}
	return loadStandReadingOf(loadStandDatagramSample{Metrics: sample.Metrics, Reverse: sample.Reverse, LocalSenders: loadstand.CloneLocalSendersSnapshot(sample.LocalSenders)}), nil
}
func (s loadStandProcessSource) Bans(ctx context.Context, now time.Time, policy loadStandBanPolicy) ([]loadStandBanFinding, error) {
	readings, err := s.source.Bans(ctx, now, loadstand.BanPolicy{SelfIdentityCooldownInvalidates: policy.SelfIdentityCooldownInvalidates})
	if err != nil {
		return nil, err
	}
	out := make([]loadStandBanFinding, 0, len(readings))
	for _, finding := range readings {
		kind, ok := processBanKinds[finding.Kind]
		if !ok {
			return nil, loadstand.ErrInvalidSample
		}
		key := finding.Key
		if finding.Count > 0 {
			key = fmt.Sprintf("%s count=%d", key, finding.Count)
		}
		out = append(out, loadStandBanFinding{Kind: kind, Key: key, Until: finding.Until})
	}
	return out, nil
}

var processBanKinds = map[loadstand.BanKind]loadStandBanKind{
	loadstand.BanScore: loadStandBanScore, loadstand.BanBlacklist: loadStandBanBlacklist, loadstand.BanIPWide: loadStandBanIPWide, loadstand.BanRemoteIPWide: loadStandBanRemoteIPWide,
	loadstand.BanPeer: loadStandBanPeer, loadstand.BanRemotePeer: loadStandBanRemotePeer, loadstand.BanSelfIdentityCooldown: loadStandSelfIdentityCooldown,
	loadstand.BanProviderIP: "provider_ip_ban", loadstand.BanLoopbackScore: "loopback_identity_score", loadstand.BanLoopbackBanned: "loopback_identity_ban",
}

func NewLoadStandProcessFixtureForTest(t *testing.T) *Service {
	svc := newDatagramLayerService(t, true)
	svc.refreshNeighbourComposition()
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() { defer close(stopped); svc.runBanDiagnosticsRefresh(ctx) }()
	awaitBanDiagnosticsWorker(t, svc)
	t.Cleanup(func() { cancel(); <-stopped })
	return svc
}
func CheckLoadStandProcessSampleForTest(ctx context.Context, source *loadstand.ProcessSource) error {
	sample, err := takeLoadStandNodeSample(ctx, runjournal.SystemClock{}, loadStandSampleSubject{}, loadStandProcessSource{source: source}, loadStandBanPolicy{})
	if err != nil {
		return err
	}
	if !sample.Neighbours.Ready || len(sample.Resources.Subsystems()) != len(domain.ResourceSubsystems()) || len(sample.Bans) != 0 {
		return errors.New("process source lost required sample family")
	}
	if _, err := sample.Datagram.Get(); err != nil {
		return err
	}
	if !sample.Traffic.StartedAt.Equal(sample.Modes.StartedAt) || !sample.Traffic.StartedAt.Equal(sample.Sessions.StartedAt) {
		return errors.New("process source mixed counter incarnations")
	}
	return nil
}
