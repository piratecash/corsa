package node

import (
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func TestFirstPinPersistenceOutcomesAcrossProofPaths(t *testing.T) {
	paths := []struct {
		name string
		note func(*secureSessionStore, domain.PeerIdentity) error
	}{
		{"inbound", func(store *secureSessionStore, id domain.PeerIdentity) error {
			_, err := store.noteProvenInboundBinding(id)
			return err
		}},
		{"outbound-canonical", func(store *secureSessionStore, id domain.PeerIdentity) error {
			_, err := store.noteProvenOutbound(dialledAt("192.0.2.1:64646"), id, endpointProtectionNone())
			return err
		}},
		{"outbound-without-binding", func(store *secureSessionStore, id domain.PeerIdentity) error {
			_, err := store.noteProvenOutbound(dialledAt("no-port"), id, endpointProtectionNone())
			return err
		}},
	}
	outcomes := []struct {
		name    string
		commit  secretfile.DurableCommit
		outcome domain.EndpointBindingActionOutcome
		visible bool
	}{
		{"not-published", secretfile.DurableNotCommitted, domain.EndpointBindingActionNotApplied, false},
		{"visible-unconfirmed", secretfile.DurableIndeterminate, domain.EndpointBindingActionIndeterminate, true},
		{"committed-cleanup-error", secretfile.DurableCommitted, domain.EndpointBindingActionAppliedCleanupError, true},
	}
	for _, path := range paths {
		for _, expected := range outcomes {
			t.Run(path.name+"/"+expected.name, func(t *testing.T) {
				clock := newStoreClock()
				locations := newPinStorePaths(t)
				store, _ := openPinStore(t, locations, clock)
				requireStarts(t, store)
				old, candidate := domain.PeerIdentity{4}, domain.PeerIdentity{5}
				if err := store.noteProvenInbound(old); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected pin write outcome")
				write := store.writeFile
				writes := 0
				store.writeFile = func(raw []byte) error {
					writes++
					if expected.visible {
						// Model publication with the actual writer. Only the
						// reported confirmation outcome is injected.
						if err := write(raw); err != nil {
							return err
						}
					}
					return &pinFileWriteError{commit: expected.commit, cause: injected}
				}

				for range 2 {
					err := path.note(store, candidate)
					var failure *pinSnapshotWriteError
					if !errors.Is(err, injected) || !errors.As(err, &failure) || failure.outcome != expected.outcome {
						t.Fatalf("pin write returned %v, want %s and the original error", err, expected.outcome)
					}
				}
				if writes != 2 || store.stats().PinnedIdentities != 2 {
					t.Fatalf("same proof: writes=%d pins=%d, want two writes and two identities",
						writes, store.stats().PinnedIdentities)
				}
				reopened, _ := openPinStore(t, locations, clock)
				requireStarts(t, reopened)
				if !reopened.identityPinned(old) || reopened.identityPinned(candidate) != expected.visible {
					t.Fatal("reopened store differs from the injected visibility boundary")
				}

				store.writeFile = write
				if err := path.note(store, candidate); err != nil {
					t.Fatalf("retry after restoring writes: %v", err)
				}
				reopened, _ = openPinStore(t, locations, clock)
				requireStarts(t, reopened)
				if !reopened.identityPinned(old) || !reopened.identityPinned(candidate) || reopened.stats().PinnedIdentities != 2 {
					t.Fatal("confirmed retry did not retain exactly the two identities")
				}
			})
		}
	}
}
