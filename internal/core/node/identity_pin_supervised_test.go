package node

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

func TestIdentityPinAdapterPreservesExactPermitAndEpoch(t *testing.T) {
	binding := supervisorBindingBackup(t, startupfence.Binding{Node: domain.PeerIdentity{1}, Store: "store", RuntimeEpoch: "supervisor-epoch", Generation: 9, Identity: domain.PeerIdentity{2}, PinRevision: 17, PreviousPinnedAt: time.Unix(44, 55).UTC()})
	permit := startupfence.RemovalPermit{Operation: "one-operation", Binding: binding, Generation: 10}
	converted := identityPinPermitFromSupervisor(permit)
	if converted.OperationID != permit.Operation || converted.Binding.RuntimeEpoch != "supervisor-epoch" || !reflect.DeepEqual(permit, identityPinPermitToSupervisor(converted)) {
		t.Fatal("adapter lost or invented permit fields")
	}
}

func TestIdentityPinUnverifiedConcreteSessionCannotAdmitOrMutate(t *testing.T) {
	store, _, token, _ := pinTransactionFixture(t)
	svc := &Service{secureSessions: &secureSessions{store: store}}
	for _, session := range []*startupfence.SupervisedSession{nil, {}} {
		if err := svc.AdmitSupervisedIdentityPinStartup(session); !errors.Is(err, domain.ErrIdentityPinSupervisionRequired) {
			t.Fatalf("unverified startup: %v", err)
		}
		if err := svc.RecoverSupervisedIdentityPinProtection(context.Background(), session); !errors.Is(err, domain.ErrIdentityPinSupervisionRequired) {
			t.Fatalf("unverified recovery: %v", err)
		}
		if _, err := svc.removeSupervisedIdentityPin(context.Background(), session, domain.PeerIdentity{21}, token); !errors.Is(err, domain.ErrIdentityPinSupervisionRequired) {
			t.Fatalf("unverified removal: %v", err)
		}
	}
	if !store.identityPinned(domain.PeerIdentity{21}) {
		t.Fatal("unverified adapter removed pin")
	}
	store.mu.Lock()
	admitted, offline := store.pinSupervisedStartup, store.pinOfflineRecovery
	store.mu.Unlock()
	if admitted || offline {
		t.Fatal("unverified adapter mutated startup flags")
	}
}
