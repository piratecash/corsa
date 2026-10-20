package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func n2OwnedServiceCleanup(t *testing.T, s *Service) {
	t.Helper()
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
}

func n2FreshCandidate(t *testing.T) (n1ServiceFixture, config.Node) {
	t.Helper()
	c := new(ninFixtureClock)
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	// The actual immutable strict backend is admitted before calling the factory.
	seed := ninOpenFixture(t, cfg.SecureSessionStorePath, c)
	_ = ninFilesFixture(t, seed)
	s, result, err := newUnknownInboundPinCandidateService(cfg, id, nil, inboundPinClock{elapsed: c.read})
	n2OwnedServiceCleanup(t, s)
	if err != nil || result.state != ninInstallReady || !result.created || result.owner != s.localContactPinStore() || s.inboundPins == nil || s.inboundPins.service != s || s.inboundPins.store != result.owner {
		t.Fatal("Unknown factory did not install/attach on its one actual Service/store", result.state, err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	dialID, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dial := NewService(configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, Type: domain.NodeTypeFull}), dialID, nil)
	n2OwnedServiceCleanup(t, dial)
	return n1ServiceFixture{service: s, dialer: dial, clock: c}, cfg
}

func TestInboundN2UnknownInstallPreservesLegacyWholeSnapshot(t *testing.T) {
	for _, format := range []int{1, 2} {
		t.Run(strconv.Itoa(format), func(t *testing.T) {
			c := new(ninFixtureClock)
			path := filepath.Join(t.TempDir(), "pins.json")
			s := ninOpenFixture(t, path, c)
			id := domain.PeerIdentity{71}
			if _, err := s.noteProvenOutbound(dialledAt("192.0.2.71:64646"), id, endpointProtectionNone()); err != nil {
				t.Fatal(err)
			}
			if err := s.noteProvenInbound(domain.PeerIdentity{72}); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			legacy := s.snapshotLocked()
			s.mu.Unlock()
			legacy.Format = format
			if format == 1 {
				legacy.IdentityMetadata = nil
			} else {
				// Valid historical serialized classes are compatibility input only;
				// they do not mint N2/N1 witness, quota debit or runtime strict ACK.
				at := legacy.Identities[id]
				legacy.IdentityMetadata[id] = domain.IdentityPinMetadata{
					Contact:  domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: at, Reference: "legacy-contact"},
					Inbound:  domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginInboundSession, At: at, SourceGroup: domain.SourceGroupLoopback},
					Relation: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: at, Reference: "legacy-relation"},
				}
			}
			for _, binding := range legacy.Endpoints {
				legacy.Endpoints["legacy-noncanonical"] = binding
				break
			}
			raw, err := json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			s = ninOpenFixture(t, path, c)
			s.mu.Lock()
			before := s.snapshotLocked()
			s.mu.Unlock()
			files := ninFilesFixture(t, s)
			result, err := s.installUnknownInboundPinJournal(t.Context(), files)
			if err != nil || result.state != ninInstallReady || !result.created || result.strict.digest == "" {
				t.Fatal("legacy install not strictly confirmed", result.state, err)
			}
			after := ninReadFileFixture(t, s)
			quota := after.InboundQuota
			if after.Format != 3 || quota == nil || quota.phase != ninUnknown || quota.tick != 0 || quota.generation != 1 || len(quota.rows) != 1 || quota.rows[0].count != 0 {
				t.Fatal("legacy falsely became Known or invented history")
			}
			after.Format, after.InboundQuota = before.Format, nil
			if !reflect.DeepEqual(before, after) {
				t.Fatal("installation lost legacy class/pin/endpoint/scope/audit data")
			}
			reopened := ninOpenFixture(t, path, &ninFixtureClock{})
			if reopened.ninJournal.domain != quota.domain || reopened.ninJournal.strictConfirmed() || reopened.ninJournal.tick != 0 || !reopened.identityPinned(id) || !reopened.endpointRequiresV2("192.0.2.71:64646") {
				t.Fatal("reopen lost protection/history or inferred strict ACK")
			}
		})
	}
}

func TestInboundN2InitialElapsedIsAnchorAndArenaStaysLazy(t *testing.T) {
	c := new(ninFixtureClock)
	c.advance(10000 * time.Hour)
	s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
	result, err := s.installUnknownInboundPinJournal(t.Context(), ninFilesFixture(t, s))
	if err != nil || result.state != ninInstallReady {
		t.Fatal(result.state, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.ninJournal
	if j == nil || j.domain == ([16]byte{}) || j.tick != 0 || j.phase != ninUnknown || j.generation != 1 || j.rows != 1 || j.arena != nil || !j.anchored || j.last != 10000*time.Hour {
		t.Fatal("large boot reading earned history or eagerly allocated arena")
	}
}

func TestInboundN2StrictOutcomesRetainAttachedJournalAndOrdinaryService(t *testing.T) {
	for _, outcome := range []string{"not-committed", "indeterminate", "committed-error", "late-cancel"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			c := new(ninFixtureClock)
			svc := newServiceWithDatagramConstructor(cfg, id, nil, nil, productionDatagramConstructor{}, inboundPinClock{elapsed: c.read})
			n2OwnedServiceCleanup(t, svc)
			s := svc.localContactPinStore()
			real := ninFilesFixture(t, s)
			peer := domain.PeerIdentity{73}
			if err := s.noteProvenInbound(peer); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
			files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
				image, err := decodeSecureSessionFile(raw)
				if err != nil || image.Format != 3 {
					t.Error("strict attempt escaped format3", err)
				}
				if svc.inboundPins != nil {
					t.Error("producer published before strict callback returned")
				}
				if outcome == "not-committed" {
					return secretfile.DurableNotCommitted, nil
				}
				commit, err := real.WriteMain(ctx, raw)
				if err != nil || commit != secretfile.DurableCommitted {
					return commit, err
				}
				if outcome == "indeterminate" {
					return secretfile.DurableIndeterminate, errors.New("post-write unknown")
				}
				if outcome == "committed-error" {
					return commit, errors.New("post-commit error")
				}
				cancel()
				return commit, nil
			}).Once()
			result, err := s.installUnknownInboundPinJournal(ctx, files)
			if !s.ninFormat3 || s.ninJournal == nil {
				t.Fatal("uncertain installation detached attached journal")
			}
			if outcome == "late-cancel" {
				if err != nil || result.state != ninInstallReady || result.strict.digest == "" {
					t.Fatal("late cancellation erased committed history", result.state, err)
				}
			} else {
				if !errors.Is(err, errPinMetadataCommitUnconfirmed) || result.state != ninInstallAttachedUnconfirmed || result.strict.digest != "" || s.ninJournal.strictConfirmed() {
					t.Fatal("uncertain outcome became ACK/readiness", result.state, err)
				}
				if err := attachInboundPinCandidate(svc, &result); err == nil || svc.inboundPins != nil {
					t.Fatal("unconfirmed install enabled N1")
				}
			}
			if !s.ninFormat3 || s.ninJournal == nil || !s.identityPinned(peer) || svc.inboundPins != nil || s.pinRecovery != nil || s.startupRefusal() != nil {
				t.Fatal("unconfirmed installation detached protection or stopped ordinary Service")
			}
			if err := s.noteProvenInbound(domain.PeerIdentity{74}); err != nil {
				t.Fatal("ordinary protection unavailable after installer outcome", err)
			}
			if outcome != "late-cancel" && s.ninJournal.strictConfirmed() {
				t.Fatal("ordinary save laundered strict install ACK")
			}
			if saved := ninReadFileFixture(t, s); saved.Format != 3 || saved.InboundQuota.domain != s.ninJournal.domain {
				t.Fatal("ordinary save fell back to format2 after attachment")
			}
			domainBefore := s.ninJournal.domain
			retry, err := s.installUnknownInboundPinJournal(t.Context(), real)
			if err != nil || retry.created || retry.state != ninInstallReady || s.ninJournal.domain != domainBefore {
				t.Fatal("explicit same-owner retry reset journal", retry.state, err)
			}
		})
	}
}

func TestInboundN2UnsupportedAndPreattachmentRefusalsHaveNoEffects(t *testing.T) {
	for _, kind := range []string{"unsupported", "cancelled", "memory", "no-clock", "unreadable", "fatal", "network-started", "generation-full", "inconsistent"} {
		t.Run(kind, func(t *testing.T) {
			c := new(ninFixtureClock)
			path := filepath.Join(t.TempDir(), "pins.json")
			if kind == "memory" {
				path = ""
			}
			s := ninOpenFixture(t, path, c)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			files := NewMockidentityPinDurableFiles(t)
			switch kind {
			case "cancelled":
				cancel()
			case "unsupported":
				files.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
			default:
				files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			}
			s.mu.Lock()
			switch kind {
			case "no-clock":
				s.ninElapsed = nil
			case "unreadable":
				s.unreadable = true
			case "fatal":
				s.pinRecovery = &identityPinRecoveryState{fatal: true, cause: domain.ErrIdentityPinRecoveryRequired}
			case "network-started":
				s.pinNetworkStarted = true
			case "generation-full":
				s.changes = ^uint64(0)
			case "inconsistent":
				s.ninFormat3 = true
			}
			changes, format, recovery := s.changes, s.ninFormat3, s.pinRecovery
			s.mu.Unlock()
			result, err := s.installUnknownInboundPinJournal(ctx, files)
			if err == nil || result.state != ninInstallUnavailable || result.created || s.ninJournal != nil || s.ninFormat3 != format || s.changes != changes || s.pinRecovery != recovery || c.reads.Load() != 0 {
				t.Fatal("pre-attachment refusal changed owner", kind, result.state, err)
			}
		})
	}
}

func TestInboundN2CancelledLockWaitCannotAttach(t *testing.T) {
	for _, lock := range []string{"gate", "writer", "mu"} {
		t.Run(lock, func(t *testing.T) {
			s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), new(ninFixtureClock))
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if lock == "gate" {
				s.pinMutationGate.Lock()
			} else {
				s.writeMu.Lock()
			}
			heldGate, heldWriter, heldMu := lock == "gate", lock != "gate", false
			done := make(chan error, 1)
			joined := make(chan struct{})
			entry := make(chan uint64, 1)
			t.Cleanup(func() {
				cancel()
				if heldMu {
					s.mu.Unlock()
				}
				if heldWriter {
					s.writeMu.Unlock()
				}
				if heldGate {
					s.pinMutationGate.Unlock()
				}
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Error("installer caller not joined")
				}
			})
			go func() {
				defer close(joined)
				entry <- relationGateCurrentGID()
				_, err := s.installUnknownInboundPinJournal(ctx, files)
				done <- err
			}()
			gid := <-entry
			n1WaitMutexFrame(t, gid, "installUnknownInboundPinJournal.func1")
			if lock == "mu" {
				s.mu.Lock()
				heldMu = true
				s.writeMu.Unlock()
				heldWriter = false
				deadline := time.Now().Add(2 * time.Second)
				observedWriterHeld := false
				for time.Now().Before(deadline) {
					if !s.writeMu.TryLock() {
						observedWriterHeld = true
						break
					}
					s.writeMu.Unlock()
					runtime.Gosched()
				}
				if !observedWriterHeld {
					t.Fatal("installer did not acquire released writer before final mu wait")
				}
				n1WaitMutexFrame(t, gid, "installUnknownInboundPinJournal.func1")
			}
			cancel()
			if heldMu {
				s.mu.Unlock()
				heldMu = false
			}
			if heldWriter {
				s.writeMu.Unlock()
				heldWriter = false
			}
			if heldGate {
				s.pinMutationGate.Unlock()
				heldGate = false
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancelled installer failed to return after dependency release")
			}
			<-joined
			if s.ninFormat3 || s.ninJournal != nil || s.changes != 0 {
				t.Fatal("cancel after lock wait attached journal")
			}
		})
	}
}

func TestInboundN2ObservedUnknownReopenThenActualIncomingAllocation(t *testing.T) {
	first, cfg := n2FreshCandidate(t)
	s := first.service.localContactPinStore()
	files := ninFilesFixture(t, s)
	first.clock.advance(6 * time.Hour)
	if result, err := s.installUnknownInboundPinJournal(t.Context(), files); err != nil || result.created {
		t.Fatal("explicit progress confirmation reinstalled journal", err)
	}
	domainBefore := s.ninJournal.domain
	first.service.runCancel()
	first.service.closeBackgroundAdmission()
	first.service.WaitBackground()
	c := new(ninFixtureClock)
	c.advance(10000 * time.Hour)
	reopened, result, err := newUnknownInboundPinCandidateService(cfg, first.service.identity, nil, inboundPinClock{elapsed: c.read})
	n2OwnedServiceCleanup(t, reopened)
	if err != nil || result.created || result.state != ninInstallReady || reopened.localContactPinStore().ninJournal.domain != domainBefore || reopened.localContactPinStore().ninJournal.tick != uint64(6*time.Hour) {
		t.Fatal("reopen credited downtime or reset cumulative progress", result.state, err)
	}
	remaining := reopened.localContactPinStore().inboundQuotaStatus(domain.SourceGroupLoopback)
	if remaining.Reason != domain.InboundQuotaHistoryUnknown || remaining.RemainingObserved == nil || *remaining.RemainingObserved != 18*time.Hour {
		t.Fatal("Unknown remaining uptime incorrect", remaining)
	}
	reopened.routeWithdrawalGracePeriodTest = -1
	incoming := n1AcceptedOnService(t, n1ServiceFixture{service: reopened, dialer: first.dialer, clock: c})
	if !reopened.publishInboundLifetime(incoming.owner) {
		t.Fatal("actual original incoming lifetime not published")
	}
	c.advance(18*time.Hour - time.Nanosecond)
	if err := reopened.inboundPins.pass(t.Context()); !errors.Is(err, domain.InboundQuotaHistoryUnknown) {
		t.Fatal("Unknown ended early", err)
	}
	c.advance(time.Nanosecond)
	store := reopened.localContactPinStore()
	real := ninFilesFixture(t, store)
	if _, err := store.installUnknownInboundPinJournal(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	if err := reopened.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := reopened.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	before := store.ninJournal.image()
	count, confirmed := store.inboundAllocations, store.inboundConfirmed
	store.mu.Unlock()
	if count != 1 || confirmed != 1 || before.phase != ninKnown || before.domain != domainBefore || before.rows[0].count != 1 {
		t.Fatal("real incoming witness did not allocate/debit exactly once after cumulative wait")
	}
	if result, err := store.installUnknownInboundPinJournal(t.Context(), real); err != nil || result.created {
		t.Fatal("existing format3 reinstall", err)
	}
	store.mu.Lock()
	after := store.ninJournal.image()
	store.mu.Unlock()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("idempotent installation replaced existing domain/history/progress")
	}
}

func TestInboundN2OlderStrictImagePreservesSuffixAndCurrentReadiness(t *testing.T) {
	for _, kind := range []string{"suffix", "phase", "metadata"} {
		t.Run(kind, func(t *testing.T) {
			c := new(ninFixtureClock)
			s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
			real := ninFilesFixture(t, s)
			peer := domain.PeerIdentity{75}
			if err := s.noteProvenInbound(peer); err != nil {
				t.Fatal(err)
			}
			if _, err := s.installUnknownInboundPinJournal(t.Context(), real); err != nil {
				t.Fatal(err)
			}
			first, later := 6*time.Hour, time.Hour
			if kind == "phase" {
				first, later = 24*time.Hour-time.Minute, time.Minute
			}
			c.advance(first)
			original := metadataSnapshot(t, s, peer)
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
			files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
				if !s.mu.TryLock() {
					t.Error("strict installer I/O retained store.mu")
					return secretfile.DurableNotCommitted, errors.New("mu held")
				}
				if kind == "metadata" {
					next := pinClassMetadataFixture(t)
					if _, err := s.stagePinMetadataLocked(original, next); err != nil {
						t.Error(err)
					}
				} else {
					c.advance(later)
					s.sampleNinQuotaLocked()
				}
				s.mu.Unlock()
				return real.WriteMain(ctx, raw)
			}).Once()
			result, err := s.installUnknownInboundPinJournal(t.Context(), files)
			if result.strict.digest == "" {
				t.Fatal("historical committed image evidence lost")
			}
			s.mu.Lock()
			j := s.ninJournal
			pending, currentStrict := j.pending(), j.strictConfirmed()
			tick, writtenTick := j.tick, j.written.tick
			dirty := s.changes != s.written
			entry := s.identities[peer]
			s.mu.Unlock()
			if kind == "phase" {
				if !errors.Is(err, errPinMetadataCommitUnconfirmed) || result.state != ninInstallAttachedUnconfirmed || currentStrict || !pending || tick != ninWindow {
					t.Fatal("nil confirmer error laundered changed current content", result.state, err)
				}
			} else if err != nil || result.state != ninInstallReady || !currentStrict {
				t.Fatal("current unchanged content not confirmed", result.state, err)
			}
			if kind == "suffix" && (!pending || tick != uint64(first+later) || writtenTick != uint64(first) || ninReadFileFixture(t, s).InboundQuota.tick != uint64(first)) {
				t.Fatal("older image ACK erased unsaved elapsed suffix")
			}
			if kind == "metadata" && (!dirty || entry.metadata == nil || entry.metadata.confirmedRevision == entry.protectionRevision) {
				t.Fatal("old full image ACKed later metadata/revision")
			}
			domainBefore := j.domain
			if result, err := s.installUnknownInboundPinJournal(t.Context(), real); err != nil || result.created || result.state != ninInstallReady || s.ninJournal.domain != domainBefore || s.ninJournal.pending() {
				t.Fatal("exact current retry failed or reset quota", result.state, err)
			}
		})
	}
}

func TestInboundN2AttachmentDrainsOldWriterAndC3KeepsFormatThree(t *testing.T) {
	c := new(ninFixtureClock)
	s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
	real := ninFilesFixture(t, s)
	peer := domain.PeerIdentity{76}
	if err := s.noteProvenInbound(peer); err != nil {
		t.Fatal(err)
	}
	if err := s.noteProvenInbound(domain.PeerIdentity{77}); err != nil {
		t.Fatal(err)
	}
	originalWrite := s.writeFile
	entered, release := make(chan struct{}), make(chan struct{})
	formats := make(chan int, 4)
	s.writeFile = func(raw []byte) error {
		image, err := decodeSecureSessionFile(raw)
		if err != nil {
			return err
		}
		formats <- image.Format
		close(entered)
		<-release
		return originalWrite(raw)
	}
	s.mu.Lock()
	s.changes++
	s.mu.Unlock()
	writerDone := make(chan error, 1)
	writerClosed := make(chan struct{})
	go func() { defer close(writerClosed); writerDone <- s.persist() }()
	<-entered
	installDone := make(chan error, 1)
	installClosed := make(chan struct{})
	gid := make(chan uint64, 1)
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		select {
		case <-writerClosed:
		case <-time.After(3 * time.Second):
			t.Error("old writer not joined")
		}
		select {
		case <-installClosed:
		case <-time.After(3 * time.Second):
			t.Error("installer not joined")
		}
		s.writeFile = originalWrite
	})
	go func() {
		defer close(installClosed)
		gid <- relationGateCurrentGID()
		_, err := s.installUnknownInboundPinJournal(t.Context(), real)
		installDone <- err
	}()
	n1WaitMutexFrame(t, <-gid, "installUnknownInboundPinJournal.func1")
	s.mu.Lock()
	attachedEarly := s.ninFormat3 || s.ninJournal != nil
	s.mu.Unlock()
	if attachedEarly {
		t.Fatal("installer overtook earlier format2 writer")
	}
	if s.pinMutationGate.TryLock() {
		s.pinMutationGate.Unlock()
		t.Fatal("attachment barrier failed to exclude C3")
	}
	close(release)
	released = true
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	<-writerClosed
	if err := <-installDone; err != nil {
		t.Fatal(err)
	}
	<-installClosed
	s.writeFile = originalWrite
	if got := <-formats; got != 2 {
		t.Fatal("ordinary pre-attachment image was not legacy", got)
	}
	if saved := ninReadFileFixture(t, s); saved.Format != 3 {
		t.Fatal("late format2 image replaced attached format3")
	}
	domainBefore := s.ninJournal.domain
	preview := pinRemovalPreview(t, s, peer)
	s.mu.Lock()
	entry := s.identities[peer]
	s.mu.Unlock()
	binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{78}, Store: "n2-c3"}, RuntimeEpoch: "n2-process", Generation: 1, Identity: peer, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
	permit := domain.IdentityPinRemovalPermit{OperationID: "n2-remove", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := s.removeIdentityPin(t.Context(), binding, preview.Token, fence, real); err != nil {
		t.Fatal(err)
	}
	saved := ninReadFileFixture(t, s)
	if saved.Format != 3 || saved.InboundQuota.domain != domainBefore || len(saved.PinRemovalAudit) != 1 || !s.identityPinned(domain.PeerIdentity{77}) {
		t.Fatal("C3 lost attached domain, audit or unrelated protection")
	}
	if _, err := s.installUnknownInboundPinJournal(t.Context(), real); err == nil {
		t.Fatal("installer bypassed existing C3 recovery/startup refusal")
	}
}

func TestInboundN2HelperRejectsStaleResultAndLateRunAttachment(t *testing.T) {
	f, _ := n2FreshCandidate(t)
	s := f.service.localContactPinStore()
	if err := attachInboundPinCandidate(f.service, nil); err == nil {
		t.Fatal("helper constructed a second producer")
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	c := new(ninFixtureClock)
	ordinary := newServiceWithDatagramConstructor(cfg, id, nil, nil, productionDatagramConstructor{}, inboundPinClock{elapsed: c.read})
	n2OwnedServiceCleanup(t, ordinary)
	owner := ordinary.localContactPinStore()
	result, err := owner.installUnknownInboundPinJournal(t.Context(), ninFilesFixture(t, owner))
	if err != nil {
		t.Fatal(err)
	}
	foreign := result
	foreign.owner = s
	if err := attachInboundPinCandidate(ordinary, &foreign); err == nil || ordinary.inboundPins != nil {
		t.Fatal("foreign installation result attached producer")
	}
	c.advance(24 * time.Hour)
	owner.mu.Lock()
	owner.sampleNinQuotaLocked()
	owner.mu.Unlock()
	if err := attachInboundPinCandidate(ordinary, &result); err == nil || ordinary.inboundPins != nil {
		t.Fatal("historical Ready result bypassed current strict content check")
	}
	if _, err := owner.installUnknownInboundPinJournal(t.Context(), ninFilesFixture(t, owner)); err != nil {
		t.Fatal(err)
	}
	if err := ordinary.beginIdentityPinNetworkRun(); err != nil {
		t.Fatal(err)
	}
	if err := attachInboundPinCandidate(ordinary, &result); err == nil || ordinary.inboundPins != nil {
		t.Fatal("returned running ordinary Service gained late producer")
	}
}

func TestInboundN2ActualRunOwnsInstalledProducerAndJoinsWriterWait(t *testing.T) {
	f, _ := n2FreshCandidate(t)
	store := f.service.localContactPinStore()
	f.service.WaitBackground()
	f.service.bootstrapTickOverride = make(chan time.Time)
	f.clock.advance(24 * time.Hour)
	store.mu.Lock()
	store.sampleNinQuotaLocked()
	store.mu.Unlock()
	store.writeMu.Lock()
	held := true
	ctx, cancel := context.WithCancel(t.Context())
	runID := make(chan uint64, 1)
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); runID <- relationGateCurrentGID(); done <- f.service.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if held {
			store.writeMu.Unlock()
		}
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("N2 actual Run not joined")
		}
	})
	id := <-runID
	var worker uint64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for gid, stack := range n1CompleteStacks(t) {
			if strings.Contains(stack, "(*inboundPinProducer).runWithFiles(") && strings.Contains(stack, "confirmPinMetadata(") && strings.Contains(stack, "sync.") {
				if worker != 0 && worker != gid {
					t.Fatal("multiple installed N1 workers")
				}
				worker = gid
				if !strings.Contains(stack, "(*Service).goRunLoop.func1(") || !strings.Contains(stack, "in goroutine "+strconv.FormatUint(id, 10)) {
					t.Fatal("N2 installed producer detached from its actual Service.Run")
				}
			}
		}
		if worker != 0 {
			break
		}
		runtime.Gosched()
	}
	if worker == 0 {
		t.Fatal("actual installed producer never entered strict writer wait")
	}
	cancel()
	observedJoin := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stacks := n1CompleteStacks(t)
		if strings.Contains(stacks[id], "stopRunLifecycle(") && strings.Contains(stacks[id], "sync.") {
			observedJoin = true
			break
		}
		select {
		case <-joined:
			t.Fatal("Run returned before installed worker dependency")
		default:
		}
		runtime.Gosched()
	}
	if !observedJoin {
		t.Fatal("Run not observed at actual lifecycle join")
	}
	store.writeMu.Unlock()
	held = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join released worker")
	}
	<-joined
	if _, alive := n1CompleteStacks(t)[worker]; alive {
		t.Fatal("installed producer survived actual Run join")
	}
	// This is the actual shared writer-lock dependency, not a blocked-kernel-I/O claim.
}

func TestInboundN2ParkedStrictReturnNeverPublishesProducer(t *testing.T) {
	cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	c := new(ninFixtureClock)
	svc := newServiceWithDatagramConstructor(cfg, id, nil, nil, productionDatagramConstructor{}, inboundPinClock{elapsed: c.read})
	n2OwnedServiceCleanup(t, svc)
	s := svc.localContactPinStore()
	real := ninFilesFixture(t, s)
	entered, release := make(chan struct{}), make(chan struct{})
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		close(entered)
		<-release
		return real.WriteMain(ctx, raw)
	}).Once()
	type answer struct {
		result ninInstallResult
		err    error
	}
	done := make(chan answer, 1)
	joined := make(chan struct{})
	held := true
	go func() {
		defer close(joined)
		r, err := s.installUnknownInboundPinJournal(t.Context(), files)
		done <- answer{r, err}
	}()
	t.Cleanup(func() {
		if held {
			close(release)
		}
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("parked installer callback not joined")
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual typed strict callback not reached")
	}
	s.mu.Lock()
	attached, unconfirmed := s.ninFormat3 && s.ninJournal != nil, !s.ninJournal.strictConfirmed()
	s.mu.Unlock()
	if !attached || !unconfirmed || svc.inboundPins != nil {
		t.Fatal("parked strict callback enabled producer or detached journal")
	}
	// A forged internal result alone cannot grant authority; current strict
	// state is independently required. Production has only two exact factories.
	forged := ninInstallResult{owner: s, state: ninInstallReady}
	if err := attachInboundPinCandidate(svc, &forged); err == nil || svc.inboundPins != nil {
		t.Fatal("result boolean substituted for actual current strict ACK")
	}
	select {
	case <-joined:
		t.Fatal("installer detached before actual backend callback return")
	default:
	}
	close(release)
	held = false
	var got answer
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("installer not returned after real strict backend completion")
	}
	<-joined
	if got.err != nil || got.result.state != ninInstallReady || got.result.strict.digest == "" {
		t.Fatal("real strict completion not observed", got.err)
	}
	if err := attachInboundPinCandidate(svc, &got.result); err != nil || svc.inboundPins == nil || svc.inboundPins.service != svc || svc.inboundPins.store != s {
		t.Fatal("confirmed same-Service producer not published", err)
	}
}

func TestInboundN2PublicDefaultsAndUnavailableFactoryRemainOrdinary(t *testing.T) {
	for _, constructor := range []string{"NewService", "NewServiceWithNetwork"} {
		t.Run(constructor, func(t *testing.T) {
			cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			for n := 0; n < 2; n++ {
				var svc *Service
				if constructor == "NewService" {
					svc = NewService(cfg, id, nil)
				} else {
					backend := netcoretest.New()
					t.Cleanup(backend.Shutdown)
					svc = NewServiceWithNetwork(cfg, id, backend)
				}
				n2OwnedServiceCleanup(t, svc)
				s := svc.localContactPinStore()
				if svc.inboundPins != nil || s.ninFormat3 || s.ninJournal != nil {
					t.Fatal("public constructor installed Unknown journal or N1")
				}
				if err := s.noteProvenInbound(domain.PeerIdentity{79}); err != nil {
					t.Fatal("ordinary mandatory protection unavailable", err)
				}
				if saved := ninReadFileFixture(t, s); saved.Format != 2 || saved.InboundQuota != nil {
					t.Fatal("ordinary restart auto-migrated journal")
				}
				svc.runCancel()
				svc.closeBackgroundAdmission()
				svc.WaitBackground()
			}
		})
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull})
	cfg.PeersStatePath = ""
	cfg.SecureSessionStorePath = ""
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc, result, err := newUnknownInboundPinCandidateService(cfg, id, nil, inboundPinClock{elapsed: new(ninFixtureClock).read})
	n2OwnedServiceCleanup(t, svc)
	if err == nil || result.state != ninInstallUnavailable || svc.inboundPins != nil || svc.localContactPinStore().ninJournal != nil || svc.StartupRefusal() != nil {
		t.Fatal("unavailable optional factory lost ordinary Service ownership", result.state, err)
	}
	if err := svc.localContactPinStore().noteProvenInbound(domain.PeerIdentity{80}); err != nil {
		t.Fatal("unavailable N2 disabled ordinary memory pinning", err)
	}
}
