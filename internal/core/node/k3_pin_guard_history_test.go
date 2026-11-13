//go:build !windows

package node

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

func guardObservationRequest() pinMembershipRequest {
	request := pinMembershipTestRequest(newK3ActualClock(), 8, 16)
	request.durableWrites = true
	return request
}

func TestK3PinGuardMarksBeforeBrokerWithoutLocalRecovery(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	store.initializePinMembership(guardObservationRequest(), false)
	before := store.pinMembershipSnapshot()
	if before.DurableWrites.GuardCoverage == nil || before.DurableWrites.GuardCoverage.Gap {
		t.Fatal("expected a clean starting epoch")
	}
	fence := NewMockIdentityPinRemovalFence(t)
	injected := errors.New("broker result unknown")
	fence.EXPECT().BeginRemoval(mock.Anything, binding).RunAndReturn(
		func(context.Context, domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error) {
			during := store.pinMembershipSnapshot()
			guard := during.DurableWrites.GuardCoverage
			if guard == nil || !guard.Gap || guard.From < before.Cut.After ||
				guard.From > during.Cut.Before || during.PointOnly.RecoveryPending {
				t.Fatal("guard gap was not published before the broker call", during)
			}
			return domain.IdentityPinRemovalPermit{}, injected
		}).Once()
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	after := store.pinMembershipSnapshot()
	guard := after.DurableWrites.GuardCoverage
	if guard == nil || !guard.Gap || after.PointOnly.RecoveryPending || len(after.DurableWrites.Events) != 0 {
		t.Fatal("point-only recovery and writer history did not retain the guard gap", after)
	}
	from := guard.From
	guard.Gap = false
	again := store.pinMembershipSnapshot().DurableWrites.GuardCoverage
	if again == nil || !again.Gap || again.From != from {
		t.Fatal("snapshot alias rewrote a recorded gap", again)
	}
}

func TestK3PinGuardAtStartupAndOfflineRecovery(t *testing.T) {
	original, binding, _, files := pinTransactionFixture(t)
	request := guardObservationRequest()
	opts := secureSessionStoreOptions{
		path: filepath.Join(original.dir, original.name), clock: newStoreClock().trusted(),
		logger: zerolog.Nop(), audit: secretfile.AuditLocation, pinMembership: &request,
	}
	permit := domain.IdentityPinRemovalPermit{OperationID: "test-guard-epoch", Binding: binding, Generation: 2}
	t.Run("offline-recovery-marks-clean-store", func(t *testing.T) {
		store := openSecureSessionStore(opts)
		before := store.pinMembershipSnapshot()
		if !before.DurableWrites.BaselineConfirmed || before.DurableWrites.GuardCoverage.Gap {
			t.Fatal("expected a confirmed start before offline recovery", before)
		}
		fence := NewMockIdentityPinRemovalFence(t)
		fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(
			func(context.Context, domain.IdentityPinReceipt) error {
				got := store.pinMembershipSnapshot()
				guard := got.DurableWrites.GuardCoverage
				if !guard.Gap || guard.From < before.Cut.After || guard.From > got.DurableWrites.Events[0].Elapsed {
					t.Fatal("offline recovery was not marked before write", got)
				}
				return nil
			}).Once()
		if _, err := store.recoverIdentityPinProtection(context.Background(), permit, fence, files); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("restart-with-guard-starts-uncovered", func(t *testing.T) {
		raw, err := json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit})
		if err != nil {
			t.Fatal(err)
		}
		if commit, err := files.WriteGuard(context.Background(), raw); err != nil || commit != secretfile.DurableCommitted {
			t.Fatal(commit, err)
		}
		got := openSecureSessionStore(opts).pinMembershipSnapshot()
		guard := got.DurableWrites.GuardCoverage
		if guard == nil || !guard.Gap || guard.From != got.Baseline.After ||
			got.DurableWrites.BaselineConfirmed || !got.PointOnly.RecoveryPending {
			t.Fatal("startup guard incorrectly supplied a covered prefix", got)
		}
	})
}
