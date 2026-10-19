package node

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

type ninFixtureClock struct {
	tick   atomic.Int64
	failed atomic.Bool
	reads  atomic.Int64
}

func (c *ninFixtureClock) read() (time.Duration, error) {
	c.reads.Add(1)
	if c.failed.Load() {
		return 0, errors.New("elapsed unavailable")
	}
	return time.Duration(c.tick.Load()), nil
}
func (c *ninFixtureClock) advance(d time.Duration) { c.tick.Add(int64(d)) }
func ninOpenFixture(t *testing.T, path string, c *ninFixtureClock) *secureSessionStore {
	t.Helper()
	s := openSecureSessionStore(secureSessionStoreOptions{path: path, clock: newStoreClock().trusted(), logger: zerolog.Nop(), audit: secretfile.AuditLocation, ninElapsed: c.read})
	if err := s.startupRefusal(); err != nil {
		t.Fatal(err)
	}
	return s
}
func ninFilesFixture(t *testing.T, s *secureSessionStore) identityPinDurableFiles {
	t.Helper()
	f, err := s.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if err = f.Preflight(t.Context()); errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Skip("strict durable backend unavailable")
	} else if err != nil {
		t.Fatal(err)
	}
	return f
}

// Installation exists ONLY in test sources. There is no compiled production
// installer or caller. Virgin authority is actual audited absence plus pristine
// owner state under the exclusive gate/writer, never a caller KnownEmpty flag.
func ninInstallVirginFixture(t *testing.T, s *secureSessionStore, f identityPinDurableFiles) {
	t.Helper()
	ninInstallFixture(t, s, f, func() (uint8, error) {
		if s.pinMainLoaded || len(s.identities) != 0 || len(s.endpoints) != 0 || len(s.nonCanonical) != 0 || len(s.pinRemovalAudit) != 0 || s.pinRecovery != nil || s.changes != 0 {
			return 0, errNinJournal
		}
		if err := secretfile.AuditLocation(filepath.Join(s.dir, s.name)); err != nil {
			return 0, err
		}
		dir, err := secretfile.Open(s.dir)
		if err != nil {
			return 0, err
		}
		defer func() { _ = dir.Close() }()
		if _, err = dir.Lstat(s.name); !errors.Is(err, os.ErrNotExist) {
			return 0, errNinJournal
		}
		if readPinStoreMarker(s.markerPath).state != markerNotKept {
			return 0, errNinJournal
		}
		return ninKnown, nil
	})
}
func ninInstallUnknownFixture(t *testing.T, s *secureSessionStore, f identityPinDurableFiles) {
	t.Helper()
	ninInstallFixture(t, s, f, func() (uint8, error) { return ninUnknown, nil })
}
func ninInstallFixture(t *testing.T, s *secureSessionStore, f identityPinDurableFiles, observe func() (uint8, error)) {
	t.Helper()
	ctx := t.Context()
	if err := f.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	s.pinMutationGate.Lock()
	defer s.pinMutationGate.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.memoryOnlyLocked() || s.ninFormat3 || s.unreadable || s.changes == ^uint64(0) {
		s.mu.Unlock()
		t.Fatal("fixture installation refused")
	}
	// Namespace I/O is deliberately outside mu; exclusive mutation/writer gates
	// retain pristine-owner ordering. This callback is TEST-ONLY, not a port.
	s.mu.Unlock()
	phase, err := observe()
	if err != nil {
		t.Fatal(err)
	}
	var domainID [16]byte
	if _, err = rand.Read(domainID[:]); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.ninFormat3 = true
	s.ninJournal = &ninJournal{domain: domainID, generation: 1, phase: phase, rows: 1}
	s.sampleNinQuotaLocked()
	s.changes++
	file, covers := s.snapshotLocked(), s.changes
	quota := file.InboundQuota.stamp()
	s.mu.Unlock()
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.WriteMain(ctx, raw)
	if err != nil || result != secretfile.DurableCommitted {
		t.Fatalf("fixture install remains attached/unconfirmed: %v %v", result, err)
	}
	s.mu.Lock()
	s.recordNinSnapshotWrittenLocked(quota, true)
	s.ackPinMetadataLocked(nil, covers, digest)
	s.mu.Unlock()
}
func ninUnknownFixture(t *testing.T) (*secureSessionStore, *ninFixtureClock, identityPinDurableFiles) {
	t.Helper()
	c := new(ninFixtureClock)
	s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
	f := ninFilesFixture(t, s)
	ninInstallUnknownFixture(t, s, f)
	return s, c, f
}
func ninReadFileFixture(t *testing.T, s *secureSessionStore) secureSessionFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.dir, s.name))
	if err != nil {
		t.Fatal(err)
	}
	f, err := decodeSecureSessionFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestInboundN0DefaultLegacyNeverInstallsOrSamples(t *testing.T) {
	c := new(ninFixtureClock)
	c.failed.Store(true)
	s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
	if err := s.noteProvenInbound(domain.PeerIdentity{91}); err != nil {
		t.Fatal(err)
	}
	before := ninReadFileFixture(t, s)
	c.advance(48 * time.Hour)
	if err := s.flushRenewals(); err != nil {
		t.Fatal(err)
	}
	if err := s.flushRenewalsNow(); err != nil {
		t.Fatal(err)
	}
	after := ninReadFileFixture(t, s)
	if c.reads.Load() != 0 || before.Format != 2 || after.Format != 2 || after.InboundQuota != nil || s.ninJournal != nil {
		t.Fatal("ordinary legacy auto-installed journal")
	}
	if _, err := s.stageInboundPin(t.Context(), nil, nil); !errors.Is(err, domain.InboundQuotaNotInstalled) {
		t.Fatal(err)
	}
	memory := ninOpenFixture(t, "", c)
	if memory.ninJournal != nil || memory.snapshotLocked().Format != 2 {
		t.Fatal("memory default changed")
	}
}
func TestInboundN0ObservedUnknownSurvivesFourNormalRestarts(t *testing.T) {
	for _, periodic := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown_only", true: "periodic_only"}[periodic], func(t *testing.T) {
			s, c, _ := ninUnknownFixture(t)
			path := filepath.Join(s.dir, s.name)
			for n := 1; n <= 4; n++ {
				// NO diagnostic call or pin/endpoint mutation makes progress dirty.
				c.advance(6 * time.Hour)
				var err error
				if periodic {
					err = s.flushRenewals()
				} else {
					err = s.flushRenewalsNow()
				}
				if err != nil {
					t.Fatal(err)
				}
				file := ninReadFileFixture(t, s)
				if file.InboundQuota.tick != uint64(n)*uint64(6*time.Hour) {
					t.Fatalf("time-only checkpoint stalled/reset: segment %d tick %d", n, file.InboundQuota.tick)
				}
				c = &ninFixtureClock{}
				c.advance(time.Duration(n*1000) * time.Hour)
				s = ninOpenFixture(t, path, c)
				if s.ninJournal.tick != uint64(n)*uint64(6*time.Hour) {
					t.Fatal("restart credited downtime")
				}
			}
			if s.ninJournal.phase != ninKnown || s.ninJournal.strictConfirmed() {
				t.Fatal("phase not persisted or ordinary write laundered strict ACK")
			}
		})
	}
}
func TestInboundN0ParkedWriterRetainsSuffixAndPhase(t *testing.T) {
	for _, phaseChange := range []bool{false, true} {
		t.Run(map[bool]string{false: "suffix", true: "phase"}[phaseChange], func(t *testing.T) {
			s, c, _ := ninUnknownFixture(t)
			first := 6 * time.Hour
			later := time.Hour
			if phaseChange {
				first = 24*time.Hour - time.Minute
				later = time.Minute
			}
			c.advance(first)
			actual := s.writeFile
			entered, release := make(chan struct{}), make(chan struct{})
			s.writeFile = func(raw []byte) error { close(entered); <-release; return actual(raw) }
			done := make(chan error, 1)
			go func() { done <- s.flushRenewalsNow() }()
			<-entered
			c.advance(later)
			s.mu.Lock()
			s.sampleNinQuotaLocked()
			s.mu.Unlock()
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			s.writeFile = actual
			s.mu.Lock()
			pending := s.ninProgressPendingLocked()
			tick := s.ninJournal.tick
			s.mu.Unlock()
			if !pending || tick != uint64(first+later) {
				t.Fatal("old writer cleared/rewound newer progress")
			}
			if err := s.flushRenewalsNow(); err != nil {
				t.Fatal(err)
			}
			if ninReadFileFixture(t, s).InboundQuota.tick != tick {
				t.Fatal("suffix not checkpointed")
			}
		})
	}
}
func TestInboundN0PersistSamplesAfterWriterWait(t *testing.T) {
	s, c, _ := ninUnknownFixture(t)
	s.writeMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(started); done <- s.persist() }()
	<-started
	c.advance(time.Hour)
	s.writeMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ninReadFileFixture(t, s).InboundQuota.tick != uint64(time.Hour) {
		t.Fatal("persist old early return lost time-only state")
	}
}
func TestInboundN0StrictContentIndependentOfSamplingAndOrdinaryWrite(t *testing.T) {
	s, c, f := ninUnknownFixture(t)
	c.advance(24 * time.Hour)
	if err := s.flushRenewalsNow(); err != nil {
		t.Fatal(err)
	}
	if s.ninJournal.strictConfirmed() {
		t.Fatal("ordinary save granted strict content ACK")
	}
	mockFiles := NewMockidentityPinDurableFiles(t)
	mockFiles.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	mockFiles.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		c.advance(time.Hour)
		s.mu.Lock()
		s.sampleNinQuotaLocked()
		s.mu.Unlock()
		return f.WriteMain(ctx, raw)
	}).Once()
	if _, err := s.confirmPinMetadata(t.Context(), mockFiles); err != nil {
		t.Fatal(err)
	}
	if !s.ninJournal.strictConfirmed() || !s.ninJournal.pending() {
		t.Fatal("time sampling starved content ACK or erased suffix")
	}
}
func TestInboundN0ClockFaultsAndNoEagerArena(t *testing.T) {
	s, c, _ := ninUnknownFixture(t)
	if s.ninJournal.arena != nil || unsafe.Sizeof(ninRow{}) != 520 {
		t.Fatal("journal not lazy/fixed520")
	}
	c.failed.Store(true)
	status := s.inboundQuotaStatus(domain.SourceGroupLoopback)
	if status.Reason != domain.InboundQuotaClockUnavailable || status.RemainingObserved != nil {
		t.Fatal("clock failure fabricated time", status)
	}
	c.failed.Store(false)
	c.advance(time.Hour)
	s.inboundQuotaStatus(domain.SourceGroupLoopback)
	c.advance(-2 * time.Hour)
	status = s.inboundQuotaStatus(domain.SourceGroupLoopback)
	if status.Reason != domain.InboundQuotaClockFault || status.RemainingObserved != nil {
		t.Fatal("backwards clock granted progress", status)
	}
}
func TestInboundN0RequiredFormatThreeJournal(t *testing.T) {
	s, _, _ := ninUnknownFixture(t)
	file := ninReadFileFixture(t, s)
	raw, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`{"format":3,"identities":{},"identity_metadata":{},"endpoints":{}}`, `{"format":3,"inbound_pin_quota":null}`, `{"format":3,"format":3}`, `{"format":2,"FORMAT":2}`} {
		if _, err := decodeSecureSessionFile([]byte(bad)); err == nil {
			t.Fatalf("malformed format/journal accepted %s", bad)
		}
	}
	file.Format = 2
	if _, _, err := encodeIdentityPinSnapshot(file); err == nil {
		t.Fatal("legacy encoder stripped journal")
	}
	if _, err := decodeSecureSessionFile(raw); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.ninJournal = nil
	broken := s.snapshotLocked()
	s.mu.Unlock()
	if _, _, err := encodeIdentityPinSnapshot(broken); err == nil {
		t.Fatal("missing attached owner downgraded")
	}
}

func TestInboundN0PeriodicProgressUsesElapsedNotWallCadence(t *testing.T) {
	s, c, _ := ninUnknownFixture(t)
	for n := 1; n <= 2; n++ {
		c.advance(6 * time.Hour)
		if err := s.flushRenewals(); err != nil {
			t.Fatal(err)
		}
		if ninReadFileFixture(t, s).InboundQuota.tick != uint64(n)*uint64(6*time.Hour) {
			t.Fatal("wall-only due predicate stalled elapsed checkpoint", n)
		}
	}
}
