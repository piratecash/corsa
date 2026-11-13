//go:build !windows

package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

func TestPinWriteObservationStartupBaseline(t *testing.T) {
	dir := t.TempDir()
	request := pinMembershipTestRequest(newK3ActualClock(), 8, 4)
	request.durableWrites = true
	opts := secureSessionStoreOptions{
		path: filepath.Join(dir, "pins.json"), markerPath: filepath.Join(dir, "marker.json"),
		clock: newStoreClock().trusted(), logger: zerolog.Nop(), audit: secretfile.AuditLocation,
		pinMembership: &request,
	}
	check := func(t *testing.T, s *secureSessionStore, confirmed, recovery bool, count int) {
		t.Helper()
		got := s.pinMembershipSnapshot()
		writes := got.DurableWrites
		if got.Status != "PAST" || got.PointOnly.RecoveryPending != recovery ||
			writes == nil || writes.BaselineConfirmed != confirmed || writes.Pending != 0 ||
			len(writes.Events) != 0 || got.Sequence != 0 || len(got.BaselineIdentities) != count {
			t.Fatal("unexpected startup evidence", got)
		}
	}
	t.Run("memory-only", func(t *testing.T) {
		memory := opts
		memory.path, memory.markerPath = "", ""
		check(t, openSecureSessionStore(memory), false, false, 0)
	})
	store := openSecureSessionStore(opts)
	check(t, store, false, false, 0)
	id := domain.PeerIdentity{42}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	t.Run("kept-main-and-marker", func(t *testing.T) {
		restarted := openSecureSessionStore(opts)
		check(t, restarted, true, false, 1)
		if restarted.pinMembershipSnapshot().BaselineIdentities[0] != id {
			t.Fatal("confirmed baseline did not bind the loaded identity")
		}
	})
	t.Run("recreated-marker", func(t *testing.T) {
		if err := os.Remove(opts.markerPath); err != nil {
			t.Fatal(err)
		}
		check(t, openSecureSessionStore(opts), true, false, 1)
		if _, err := os.Stat(opts.markerPath); err != nil {
			t.Fatal("marker was not recreated", err)
		}
	})
	t.Run("recovery-pending", func(t *testing.T) {
		guardPath := filepath.Join(dir, identityPinGuardName(filepath.Base(opts.path)))
		if err := os.WriteFile(guardPath, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		check(t, openSecureSessionStore(opts), false, true, 1)
		if err := os.Remove(guardPath); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unreadable-main", func(t *testing.T) {
		if err := os.WriteFile(opts.path, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := openSecureSessionStore(opts).pinMembershipSnapshot()
		if got.Status != "UNKNOWN" || got.DurableWrites != nil {
			t.Fatal("refused startup produced durable evidence", got)
		}
	})
}

func TestPinWriteObservationTracksMainAndMarker(t *testing.T) {
	dir := t.TempDir()
	request := pinMembershipTestRequest(newK3ActualClock(), 8, 4)
	request.durableWrites = true
	store := openSecureSessionStore(secureSessionStoreOptions{
		path: filepath.Join(dir, "pins.json"), markerPath: filepath.Join(dir, "marker.json"),
		clock: newStoreClock().trusted(), logger: zerolog.Nop(), audit: secretfile.AuditLocation,
		pinMembership: &request,
	})
	writeMarker := store.writeMarker
	markerError := errors.New("marker writer unavailable")
	store.writeMarker = func([]byte) error {
		got := store.pinMembershipSnapshot().DurableWrites
		if got == nil || len(got.Events) != 1 || got.Events[0].Kind != "begin" || got.Pending != got.Events[0].Operation {
			t.Error("marker in flight did not leave an open write in the captured prefix", got)
		}
		return markerError
	}
	first := domain.PeerIdentity{1}
	if err := store.noteProvenInbound(first); !errors.Is(err, markerError) {
		t.Fatalf("expected original marker error, got %v", err)
	}
	partial := store.pinMembershipSnapshot()
	writes := partial.DurableWrites
	if writes == nil || writes.Pending != 0 || len(writes.Events) != 2 ||
		writes.Events[1].Kind != "indeterminate" || writes.Events[1].Operation != writes.Events[0].Sequence {
		t.Fatal("main without confirmed marker did not produce an indeterminate pair", writes)
	}

	store.writeMarker = writeMarker
	second := domain.PeerIdentity{2}
	if err := store.noteProvenInbound(second); err != nil {
		t.Fatal(err)
	}
	current := store.pinMembershipSnapshot()
	writes = current.DurableWrites
	if writes == nil || writes.Status != "PAST" || writes.Pending != 0 || len(writes.Events) != 4 {
		t.Fatal("missing completed observation", writes)
	}
	begin, end := writes.Events[2], writes.Events[3]
	if begin.Kind != "begin" || begin.Operation != begin.Sequence || end.Kind != "confirmed" ||
		end.Operation != begin.Operation || end.Elapsed < begin.Elapsed ||
		begin.MembershipSequence != 2 || end.MembershipSequence != 2 ||
		len(begin.Identities) != 2 || !pinMembershipTestSet(begin.Identities)[first] || !pinMembershipTestSet(begin.Identities)[second] {
		t.Fatal("write pair did not bind the exact image to membership order", begin, end)
	}
	begin.Identities[0] = domain.PeerIdentity{}
	if store.pinMembershipSnapshot().DurableWrites.Events[2].Identities[0].IsZero() {
		t.Fatal("readout leaked mutable source storage")
	}
	if err := store.noteProvenInbound(domain.PeerIdentity{3}); err != nil {
		t.Fatal(err)
	}
	exhausted := store.pinMembershipSnapshot()
	if exhausted.DurableWrites.Status != "UNKNOWN" || exhausted.DurableWrites.Reason != "pin-write-history-capacity" ||
		len(exhausted.DurableWrites.Events) != 4 || exhausted.CountAtCut != 3 {
		t.Fatal("capacity fault changed store membership or presented a complete write history", exhausted)
	}
}

func TestPinWriteObservationTracksMainBeforeRemovalReceipt(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	request := pinMembershipTestRequest(newK3ActualClock(), 8, 4)
	request.durableWrites = true
	store.initializePinMembership(request, false)
	fence := NewMockIdentityPinRemovalFence(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "observed-removal", Binding: binding, Generation: 2}
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, domain.IdentityPinReceipt) error {
		got := store.pinMembershipSnapshot()
		writes := got.DurableWrites
		if writes == nil || len(writes.Events) != 2 || writes.Events[0].Kind != "begin" ||
			len(writes.Events[0].Identities) != 0 || writes.Events[1].Kind != "confirmed" ||
			writes.Events[1].MembershipSequence != 0 || got.CountAtCut != 1 {
			t.Error("durable main changed before receipt but observer did not preserve memory order", got)
		}
		return nil
	}).Once()
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); err != nil {
		t.Fatal(err)
	}
	got := store.pinMembershipSnapshot()
	if got.Sequence != 1 || got.CountAtCut != 0 || len(got.Transitions) != 1 || got.Transitions[0].Added {
		t.Fatal("missing post-receipt membership edge", got)
	}
}
