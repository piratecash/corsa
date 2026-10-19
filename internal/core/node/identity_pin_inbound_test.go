package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/stretchr/testify/mock"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestInboundN0GenericCASCannotInventJournalDebit(t *testing.T) {
	store, binding, _ := selectedFixture(t)
	expected, err := store.pinMetadataSnapshot(binding.identity)
	if err != nil {
		t.Fatal(err)
	}
	next := expected.metadata
	next.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginInboundSession, SourceGroup: domain.SourceGroupLoopback, At: time.Now()}
	if _, err := store.stagePinMetadata(expected, next); err == nil {
		t.Fatal("generic CAS created Inbound allocation without quota journal/debit")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.identities[binding.identity].classMetadata().Inbound.State != domain.IdentityPinAllocationAbsent {
		t.Fatal("refused generic CAS changed Inbound allocation")
	}
}

// ninProofFixture runs a real mutual TLS exporter proof, then the ORIGINAL
// mandatory pin publication. The fixture alone owns the connection lifetime;
// no production registry/ten-minute producer is installed by N0.
func ninProofFixture(t *testing.T, s *secureSessionStore, c *ninFixtureClock) *matureInboundWitness {
	t.Helper()
	return ninProofForFixture(t, s, c, nil)
}
func ninProofForFixture(t *testing.T, s *secureSessionStore, c *ninFixtureClock, dialer *identity.Identity) *matureInboundWitness {
	t.Helper()
	listener, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if dialer == nil {
		dialer, err = identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
	}
	serverLocal, err := sessionv2.NewLocal(listener, "nin-fixture")
	if err != nil {
		t.Fatal(err)
	}
	clientLocal, err := sessionv2.NewLocal(dialer, "nin-fixture")
	if err != nil {
		t.Fatal(err)
	}
	certs, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	server, client := tcpPair(t)
	addr, err := netip.ParseAddrPort(server.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		session sessionv2.ProvenSession
		err     error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		session, err := sessionv2.Accept(ctx, server, serverLocal, protocol.Frame{Version: 31}, certs, sessionv2.DefaultTimeouts)
		done <- result{session, err}
	}()
	dialed, err := sessionv2.Dial(ctx, client, clientLocal, protocol.Frame{Version: 31}, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	accepted := <-done
	if accepted.err != nil {
		t.Fatal(accepted.err)
	}
	peer, err := accepted.session.Peer()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := s.noteProvenInboundBinding(peer.Identity)
	if err != nil {
		t.Fatal(err)
	}
	w := &matureInboundWitness{binding: binding, session: accepted.session, connID: 1, group: sourceGroup(addr.Addr()), ready: time.Duration(c.tick.Load())}
	t.Cleanup(func() {
		w.mu.Lock()
		w.ended = true
		w.mu.Unlock()
		_ = server.Close()
		_ = client.Close()
		wire, _ := dialed.Conn()
		if wire != nil {
			_ = wire.Close()
		}
	})
	return w
}
func ninKnownFixture(t *testing.T) (*secureSessionStore, *ninFixtureClock, identityPinDurableFiles) {
	t.Helper()
	c := new(ninFixtureClock)
	s := ninOpenFixture(t, filepath.Join(t.TempDir(), "pins.json"), c)
	f := ninFilesFixture(t, s)
	ninInstallVirginFixture(t, s, f)
	return s, c, f
}
func ninMatureFixture(t *testing.T) (*secureSessionStore, *ninFixtureClock, identityPinDurableFiles, *matureInboundWitness) {
	t.Helper()
	s, c, f := ninKnownFixture(t)
	w := ninProofFixture(t, s, c)
	c.advance(10 * time.Minute)
	return s, c, f, w
}
func TestInboundN0RealProofAtomicStageAndStrictOutcomes(t *testing.T) {
	for _, outcome := range []secretfile.DurableCommit{secretfile.DurableNotCommitted, secretfile.DurableIndeterminate, secretfile.DurableCommitted} {
		t.Run(fmt.Sprint(outcome), func(t *testing.T) {
			s, c, files, w := ninMatureFixture(t)
			before := metadataSnapshot(t, s, w.binding.identity)
			stage, err := s.stageInboundPin(t.Context(), w, files)
			if err != nil {
				t.Fatal(err)
			}
			if stage.binding != w.binding || s.ninJournal.generation != 2 || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 {
				t.Fatal("allocation/debit not paired")
			}
			oldImage := ninReadFileFixture(t, s)
			if oldImage.InboundQuota.rows[0].count != 0 || oldImage.IdentityMetadata[w.binding.identity].Inbound.State != domain.IdentityPinAllocationAbsent {
				t.Fatal("stage wrote a partial/new crash image")
			}
			after := metadataSnapshot(t, s, w.binding.identity)
			if s.inboundConfirmed != 0 || after.protectionRevision <= before.protectionRevision || after.metadata.Inbound.State != domain.IdentityPinAllocationEligible || after.evidenceRevision != before.evidenceRevision {
				t.Fatal("metadata publication changed proof/failed allocation")
			}
			if err := s.persist(); err != nil {
				t.Fatal(err)
			}
			if s.ninJournal.strictConfirmed() {
				t.Fatal("ordinary write ACKed debit")
			}
			injected := errors.New("strict result injected")
			f := NewMockidentityPinDurableFiles(t)
			f.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			f.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
				// If a complete new image reached the namespace it must contain BOTH sides.
				if outcome != secretfile.DurableNotCommitted {
					if _, err := files.WriteMain(ctx, raw); err != nil {
						t.Fatal(err)
					}
				}
				c.advance(time.Second)
				s.mu.Lock()
				s.sampleNinQuotaLocked()
				s.mu.Unlock()
				return outcome, injected
			}).Once()
			if _, err := s.confirmPinMetadata(t.Context(), f); !errors.Is(err, injected) || s.ninJournal.strictConfirmed() {
				t.Fatal("failed/ambiguous strict write granted ACK", err)
			}
			nextWitness := ninProofFixture(t, s, c)
			c.advance(10 * time.Minute)
			if _, err := s.stageInboundPin(t.Context(), nextWitness, files); !errors.Is(err, domain.InboundQuotaUnconfirmed) {
				t.Fatal("uncertain content allowed next allocation", err)
			}
			if _, err := s.stageInboundPin(t.Context(), w, files); err != nil {
				t.Fatal("existing eligible recheck should not debit", err)
			}
			if _, err := s.confirmPinMetadata(t.Context(), files); err != nil {
				t.Fatal(err)
			}
			if !s.ninJournal.strictConfirmed() || s.inboundConfirmed != 1 || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 {
				t.Fatal("retry changed debit")
			}
			restarted := ninOpenFixture(t, filepath.Join(s.dir, s.name), c)
			if restarted.inboundAllocations != 1 || restarted.inboundConfirmed != 0 || restarted.ninJournal.strictConfirmed() || restarted.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 {
				t.Fatal("restart lost paired image or invented ACK")
			}
		})
	}
}
func TestInboundN0RealWitnessMaturityTerminalABAAndPreflight(t *testing.T) {
	s, c, f := ninKnownFixture(t)
	w := ninProofFixture(t, s, c)
	if _, err := s.stageInboundPin(t.Context(), w, f); !errors.Is(err, errNinWitness) {
		t.Fatal("early session earned class", err)
	}
	c.advance(10 * time.Minute)
	unsupported := NewMockidentityPinDurableFiles(t)
	unsupported.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
	before := metadataSnapshot(t, s, w.binding.identity)
	if _, err := s.stageInboundPin(t.Context(), w, unsupported); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatal(err)
	}
	if metadataSnapshot(t, s, w.binding.identity) != before || s.ninJournal.generation != 1 {
		t.Fatal("preflight refusal had effects")
	}
	w.mu.Lock()
	w.ended = true
	w.mu.Unlock()
	if _, err := s.stageInboundPin(t.Context(), w, f); !errors.Is(err, errNinWitness) {
		t.Fatal("ended session earned class", err)
	}
	wire, wireErr := w.session.Conn()
	if wireErr != nil {
		t.Fatal(wireErr)
	}
	if err := wire.Close(); err != nil {
		t.Fatal(err)
	}
	w = ninProofFixture(t, s, c)
	c.advance(10 * time.Minute)
	s.mu.Lock()
	s.deletePinEntryLocked(w.binding.identity)
	s.changes++
	s.mu.Unlock()
	if err := s.noteProvenInbound(w.binding.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := s.stageInboundPin(t.Context(), w, f); !errors.Is(err, errNinWitness) {
		t.Fatal("old witness reattached recreated entry", err)
	}
	if s.ninJournal.generation != 1 || s.inboundAllocations != 0 {
		t.Fatal("stale witness consumed quota")
	}
}
func TestInboundN0CancellationAfterWaitHasNoPairedEffects(t *testing.T) {
	s, _, files, w := ninMatureFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	s.pinMutationGate.Lock()
	entered := make(chan struct{})
	f := NewMockidentityPinDurableFiles(t)
	f.EXPECT().Preflight(mock.Anything).RunAndReturn(func(context.Context) error { close(entered); return nil }).Once()
	done := make(chan error, 1)
	go func() { _, err := s.stageInboundPin(ctx, w, f); done <- err }()
	<-entered
	cancel()
	s.pinMutationGate.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.ninJournal.generation != 1 || s.inboundAllocations != 0 {
		t.Fatal("cancelled waiter mutated")
	}
	// A completed durable commit wins cancellation arriving after the syscall.
	if _, err := s.stageInboundPin(t.Context(), w, files); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	f = NewMockidentityPinDurableFiles(t)
	f.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	f.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		out, err := files.WriteMain(ctx, raw)
		cancel()
		return out, err
	}).Once()
	if _, err := s.confirmPinMetadata(ctx, f); err != nil || !s.ninJournal.strictConfirmed() {
		t.Fatal("late cancellation erased durable ACK", err)
	}
}
func TestInboundN0Group64BoundaryAndRemovalDoesNotRefund(t *testing.T) {
	s, c, f, w := ninMatureFixture(t)
	// A real qualifying witness meets a legitimately loaded complete 63-event
	// history. Equal timestamps remain distinct events (no timestamp set).
	s.mu.Lock()
	j := s.ninJournal
	j.arena = new([ninGroupCapacity]ninRow)
	j.arena[0].count = 63
	for k := 0; k < 63; k++ {
		j.arena[0].ticks[k] = j.tick
	}
	s.mu.Unlock()
	if _, err := s.stageInboundPin(t.Context(), w, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.confirmPinMetadata(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	w2 := ninProofFixture(t, s, c)
	c.advance(10 * time.Minute)
	if _, err := s.stageInboundPin(t.Context(), w2, f); !errors.Is(err, domain.InboundQuotaGroupFull) {
		t.Fatal("65th event admitted", err)
	}
	s.mu.Lock()
	s.deletePinEntryLocked(w.binding.identity)
	s.changes++
	s.mu.Unlock()
	if got := s.inboundQuotaStatus(domain.SourceGroupLoopback); got.Events != 64 {
		t.Fatal("removal refunded debit", got)
	}
	c.advance(24*time.Hour - 10*time.Minute)
	if _, err := s.stageInboundPin(t.Context(), w2, f); err != nil {
		t.Fatal("exact24h events did not expire", err)
	}
	if s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 {
		t.Fatal("expired history was retained")
	}
}
func TestInboundN0RawCapacityIncludesKAndAbsentC3Reservation(t *testing.T) {
	s, _, f, w := ninMatureFixture(t)
	allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginInboundSession, At: s.now(), SourceGroup: domain.SourceGroupLoopback}
	s.mu.Lock()
	for k := 1; k <= maxInboundPins; k++ {
		id := domain.PeerIdentity{88, byte(k), byte(k >> 8)}
		if err := s.recordPinEvidenceLocked(id, s.now()); err != nil {
			t.Fatal(err)
		}
		entry := s.identities[id]
		m := entry.classMetadata()
		m.Inbound = allocation
		m.Contact = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: s.now()}
		if err := entry.setUnpublishedMetadata(m); err != nil {
			t.Fatal(err)
		}
		s.putPinEntryLocked(id, entry)
	}
	id := domain.PeerIdentity{88, 1}
	entry := s.identities[id]
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	s.deletePinEntryLocked(id)
	s.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: id, PreviousPinnedAt: entry.pinnedAt, Backup: backup}}}
	s.mu.Unlock()
	if _, err := s.stageInboundPin(t.Context(), w, f); !errors.Is(err, domain.InboundQuotaClassFull) {
		t.Fatal("K/absent reservation discounted raw class", err)
	}
	if got := s.inboundQuotaStatus(domain.SourceGroupLoopback); got.Allocations != 4096 || s.inboundAllocations != 4095 || got.Events != 0 {
		t.Fatal(got)
	}
}

func TestInboundN0CounterExhaustionHasNoAllocationOrDebit(t *testing.T) {
	for _, counter := range []string{"protection", "dirty", "content"} {
		t.Run(counter, func(t *testing.T) {
			s, _, f, w := ninMatureFixture(t)
			s.mu.Lock()
			switch counter {
			case "protection":
				s.protectionRevision = ^identityPinProtectionRevision(0)
			case "dirty":
				s.changes = ^uint64(0)
			case "content":
				s.ninJournal.generation = ^uint64(0)
				s.ninJournal.confirmed = s.ninJournal.stamp()
			}
			s.mu.Unlock()
			if _, err := s.stageInboundPin(t.Context(), w, f); !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
				t.Fatal("overflow not refused", err)
			}
			if metadataSnapshot(t, s, w.binding.identity).metadata.Inbound.State != domain.IdentityPinAllocationAbsent || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 0 {
				t.Fatal("overflow had paired effects")
			}
		})
	}
}
func TestInboundN0ConcurrentLastEventHasOneOwner(t *testing.T) {
	s, c, f, w1 := ninMatureFixture(t)
	w2 := ninProofFixture(t, s, c)
	c.advance(10 * time.Minute)
	s.mu.Lock()
	j := s.ninJournal
	j.arena = new([ninGroupCapacity]ninRow)
	j.arena[0].count = 63
	s.mu.Unlock()
	results := make(chan error, 2)
	for _, w := range []*matureInboundWitness{w1, w2} {
		go func(w *matureInboundWitness) { _, err := s.stageInboundPin(t.Context(), w, f); results <- err }(w)
	}
	admitted := 0
	for range 2 {
		if err := <-results; err == nil {
			admitted++
		} else if !errors.Is(err, domain.InboundQuotaUnconfirmed) && !errors.Is(err, domain.InboundQuotaGroupFull) {
			t.Fatal(err)
		}
	}
	if admitted != 1 || s.inboundAllocations != 1 || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 64 {
		t.Fatal("last event overshot or allocation/debit split", admitted)
	}
}
func TestInboundN0ExistingClassificationAtGlobal20000(t *testing.T) {
	s, _, f, w := ninMatureFixture(t)
	s.mu.Lock()
	for k := 1; len(s.identities) < maxPinnedIdentities; k++ {
		id := domain.PeerIdentity{89, byte(k), byte(k >> 8)}
		if err := s.recordPinEvidenceLocked(id, s.now()); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Unlock()
	if _, err := s.stageInboundPin(t.Context(), w, f); err != nil {
		t.Fatal("existing classification consumed global place", err)
	}
	if err := s.noteProvenInbound(domain.PeerIdentity{90}); !errors.Is(err, errPinStoreFull) {
		t.Fatal("new identity bypassed global20k", err)
	}
	if len(s.identities) != 20000 || s.inboundAllocations != 1 {
		t.Fatal("global/raw tally mismatch")
	}
}

func TestInboundN0SameIdentityRemovalReearningChargesAgain(t *testing.T) {
	s, c, f := ninKnownFixture(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	old := ninProofForFixture(t, s, c, peer)
	c.advance(10 * time.Minute)
	if _, err := s.stageInboundPin(t.Context(), old, f); err != nil {
		t.Fatal(err)
	}
	id := old.binding.identity
	preview := pinRemovalPreview(t, s, id)
	s.mu.Lock()
	entry := s.identities[id]
	s.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{22}, Store: "nin-repromotion"}, RuntimeEpoch: "nin-process", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	permit := domain.IdentityPinRemovalPermit{OperationID: "nin-repromotion", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := s.removeIdentityPin(t.Context(), binding, preview.Token, fence, f); err != nil {
		t.Fatal(err)
	}
	fresh := ninProofForFixture(t, s, c, peer)
	c.advance(10 * time.Minute)
	if _, err := s.stageInboundPin(t.Context(), old, f); !errors.Is(err, errNinWitness) {
		t.Fatal("old incarnation earned new class", err)
	}
	if _, err := s.stageInboundPin(t.Context(), fresh, f); err != nil {
		t.Fatal(err)
	}
	if got := s.inboundQuotaStatus(domain.SourceGroupLoopback); got.Events != 2 || got.Allocations != 1 {
		t.Fatal("same identity reearning refunded/deduped old debit", got)
	}
}
func ninFillOldInboundFixture(t *testing.T, s *secureSessionStore, count int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := 1; k <= count; k++ {
		id := domain.PeerIdentity{86, byte(k), byte(k >> 8)}
		if err := s.recordPinEvidenceLocked(id, s.now()); err != nil {
			t.Fatal(err)
		}
		entry := s.identities[id]
		m := entry.classMetadata()
		m.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginInboundSession, At: s.now(), SourceGroup: domain.SourceGroupLoopback}
		if err := entry.setUnpublishedMetadata(m); err != nil {
			t.Fatal(err)
		}
		s.putPinEntryLocked(id, entry)
	}
}
func TestInboundN0ConcurrentLastRawClassAndExistingPending(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "last_class", true: "pending_owns_its_place"}[pending], func(t *testing.T) {
			s, c, f, w1 := ninMatureFixture(t)
			ninFillOldInboundFixture(t, s, 4095)
			if pending {
				s.mu.Lock()
				entry := s.identities[w1.binding.identity]
				m := entry.classMetadata()
				m.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginInboundSession, At: s.now(), SourceGroup: domain.SourceGroupLoopback}
				if err := entry.setUnpublishedMetadata(m); err != nil {
					t.Fatal(err)
				}
				s.putPinEntryLocked(w1.binding.identity, entry)
				s.mu.Unlock()
				if _, err := s.stageInboundPin(t.Context(), w1, f); err != nil {
					t.Fatal("existing Pending needed additional class place", err)
				}
			} else {
				w2 := ninProofFixture(t, s, c)
				c.advance(10 * time.Minute)
				done := make(chan error, 2)
				for _, w := range []*matureInboundWitness{w1, w2} {
					go func(w *matureInboundWitness) { _, err := s.stageInboundPin(t.Context(), w, f); done <- err }(w)
				}
				admitted := 0
				for range 2 {
					if err := <-done; err == nil {
						admitted++
					} else if !errors.Is(err, domain.InboundQuotaUnconfirmed) && !errors.Is(err, domain.InboundQuotaClassFull) {
						t.Fatal(err)
					}
				}
				if admitted != 1 {
					t.Fatal("last class was not exclusive", admitted)
				}
			}
			if s.inboundAllocations != 4096 || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 {
				t.Fatal("raw class/debit mismatch")
			}
		})
	}
}

func TestInboundN0NegativeMaturityAnchorCannotEarn(t *testing.T) {
	s, _, files, w := ninMatureFixture(t)
	w.mu.Lock()
	w.ready = -1
	w.mu.Unlock()
	if _, err := s.stageInboundPin(t.Context(), w, files); !errors.Is(err, errNinWitness) {
		t.Fatal("negative maturity anchor admitted", err)
	}
	if s.inboundAllocations != 0 || s.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 0 {
		t.Fatal("invalid anchor had effects")
	}
}
