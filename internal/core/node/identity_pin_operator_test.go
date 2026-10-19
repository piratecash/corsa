package node

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func pinRemovalPreview(t *testing.T, store *secureSessionStore, id domain.PeerIdentity) domain.IdentityPinRemovalPreview {
	t.Helper()
	preview, err := store.previewIdentityPinRemoval(id)
	if err != nil {
		t.Fatal(err)
	}
	return preview
}

func pinRemovalTokenError(store *secureSessionStore, id domain.PeerIdentity, token domain.IdentityPinConfirmationToken) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.validateIdentityPinTokenLocked(id, token)
}

func TestIdentityPinPreviewTracksOnlyItsOwnNewProof(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	target, other := domain.PeerIdentity{1}, domain.PeerIdentity{2}
	if err := store.noteProvenInbound(target); err != nil {
		t.Fatal(err)
	}
	first := pinRemovalPreview(t, store, target)
	if err := pinRemovalTokenError(store, target, first.Token); err != nil {
		t.Fatal(err)
	}
	if err := store.noteProvenInbound(other); err != nil {
		t.Fatal(err)
	}
	if err := pinRemovalTokenError(store, target, first.Token); err != nil {
		t.Fatalf("other identity invalidated preview: %v", err)
	}
	clock.now = clock.now.Add(time.Hour)
	if err := store.noteProvenInbound(target); err != nil {
		t.Fatal(err)
	}
	if err := pinRemovalTokenError(store, target, first.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatalf("new proof reused old preview: %v", err)
	}
	fresh := pinRemovalPreview(t, store, target)
	if fresh.PinnedAt != first.PinnedAt {
		t.Fatal("a new proof changed the original pin timestamp")
	}
	if fresh.Token == first.Token {
		t.Fatal("new proof did not change confirmation")
	}
	if err := pinRemovalTokenError(store, other, fresh.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatalf("token accepted for another identity: %v", err)
	}
}

func TestIdentityPinPreviewRejectsABAAndRestart(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "pins.json")
	store := loadSecureSessionStore(path, clock.trusted())
	id := domain.PeerIdentity{3}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, id)
	// Model an already committed removal, without enabling any operator path.
	store.mu.Lock()
	delete(store.identities, id)
	store.mu.Unlock()
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	if err := pinRemovalTokenError(store, id, preview.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Errorf("remove/re-pin revived preview: %v", err)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	if err := pinRemovalTokenError(restarted, id, preview.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Errorf("restart revived preview: %v", err)
	}
}

func TestIdentityPinPreviewIsReadOnlyAndRemovalRequiresFence(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{4}
	endpoint := domain.PeerAddress("192.0.2.4:64646")
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), id, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	before, changes, written := store.snapshotLocked(), store.changes, store.written
	svc := &Service{secureSessions: &secureSessions{store: store}}
	preview, err := svc.PreviewIdentityPinRemoval(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Identity != id || preview.Consequence != domain.IdentityPinRemovalConsequence || preview.RemovalAvailable {
		t.Fatalf("wrong preview: %+v", preview)
	}
	if _, err := svc.ConfirmIdentityPinRemoval(context.Background(), id, preview.Token); !errors.Is(err, domain.ErrIdentityPinRemovalUnavailable) {
		t.Fatalf("unfenced removal: %v", err)
	}
	if !reflect.DeepEqual(before, store.snapshotLocked()) || changes != store.changes || written != store.written {
		t.Fatal("preview/unavailable confirm mutated store")
	}
	if !store.identityPinned(id) || !store.endpointRequiresV2(endpoint) {
		t.Fatal("unavailable operation weakened protection")
	}
	if _, err := svc.PreviewIdentityPinRemoval(context.Background(), domain.PeerIdentity{}); !errors.Is(err, domain.ErrIdentityPinIdentityRequired) {
		t.Fatalf("zero identity: %v", err)
	}
	if _, err := svc.PreviewIdentityPinRemoval(context.Background(), domain.PeerIdentity{5}); !errors.Is(err, domain.ErrIdentityPinNotFound) {
		t.Fatalf("absent identity: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.PreviewIdentityPinRemoval(canceled, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preview: %v", err)
	}
}

func TestIdentityPinPreviewInvalidationCoversEveryProofPath(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"inbound", "outbound", "noncanonical"} {
		t.Run(path, func(t *testing.T) {
			store := loadSecureSessionStore("", newStoreClock().trusted())
			id := domain.PeerIdentity{10}
			if err := store.noteProvenInbound(id); err != nil {
				t.Fatal(err)
			}
			preview := pinRemovalPreview(t, store, id)
			switch path {
			case "inbound":
				if err := store.noteProvenInbound(id); err != nil {
					t.Fatal(err)
				}
			case "outbound":
				if _, err := store.noteProvenOutbound(dialledAt("192.0.2.10:64646"), id, endpointProtectionNone()); err != nil {
					t.Fatal(err)
				}
			case "noncanonical":
				if _, err := store.noteProvenOutbound(dialledAt("not-an-endpoint"), id, endpointProtectionNone()); err != nil {
					t.Fatal(err)
				}
			}
			if err := pinRemovalTokenError(store, id, preview.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
				t.Fatalf("%s proof left old preview valid: %v", path, err)
			}
		})
	}
}

func TestIdentityPinRevisionDoesNotCreateDiskWritesOrWrap(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{11}
	writes := 0
	store.writeFile = func([]byte) error { writes++; return nil }
	store.markerWritten = true
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	before := pinRemovalPreview(t, store, id)
	changes, written := store.changes, store.written
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	if writes != 1 || store.changes != changes || store.written != written {
		t.Fatal("runtime proof revision caused disk changes")
	}
	current := pinRemovalPreview(t, store, id)
	if current.Token == before.Token {
		t.Fatal("existing pin proof did not invalidate preview")
	}
	store.mu.Lock()
	store.evidenceRevision = identityPinEvidenceRevision(^uint64(0))
	store.mu.Unlock()
	if err := store.noteProvenInbound(id); !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
		t.Fatalf("counter wrapped: %v", err)
	}
	if err := pinRemovalTokenError(store, id, current.Token); err != nil {
		t.Fatalf("rejected proof changed owner entry: %v", err)
	}
	if !store.identityPinned(id) {
		t.Fatal("revision exhaustion lost pin")
	}
}

func TestIdentityPinPreviewRefusesUntrustedStoreAndMalformedTokens(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{12}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, id)
	for _, token := range []domain.IdentityPinConfirmationToken{"", "invalid", domain.IdentityPinConfirmationToken(string(preview.Token)[1:]), domain.IdentityPinConfirmationToken("ff" + string(preview.Token)[2:])} {
		// Use a guaranteed different token, independent of random MAC prefix.
		if token == preview.Token {
			continue
		}
		if err := pinRemovalTokenError(store, id, token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
			t.Fatalf("malformed/forged token accepted: %v", err)
		}
	}
	store.mu.Lock()
	store.unreadable = true
	store.mu.Unlock()
	if _, err := store.previewIdentityPinRemoval(id); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatalf("preview trusted unreadable store: %v", err)
	}
	if _, err := (&Service{}).PreviewIdentityPinRemoval(context.Background(), id); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatalf("missing store: %v", err)
	}
}

func TestIdentityPinConcurrentProofAndPreviewShareOneOwner(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{13}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	old := pinRemovalPreview(t, store, id)
	const rounds = 32
	outcomes := make(chan error, rounds*2)
	for range rounds {
		go func() { outcomes <- store.noteProvenInbound(id) }()
		go func() { _, err := store.previewIdentityPinRemoval(id); outcomes <- err }()
	}
	for range rounds * 2 {
		if err := <-outcomes; err != nil {
			t.Fatal(err)
		}
	}
	current := pinRemovalPreview(t, store, id)
	if err := pinRemovalTokenError(store, id, current.Token); err != nil {
		t.Fatal(err)
	}
	if err := pinRemovalTokenError(store, id, old.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatalf("concurrent proofs left old preview valid: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.identities) != 1 || store.evidenceRevision != rounds+1 {
		t.Fatalf("lost proofs: entries=%d revision=%d", len(store.identities), store.evidenceRevision)
	}
}

func TestIdentityPinPreviewIncludesInactiveCandidateRestartConsequence(t *testing.T) {
	store := loadSecureSessionStore("", newStoreClock().trusted())
	id := domain.PeerIdentity{23}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, id)
	for _, part := range []string{"inactive candidate", "stop", "one removal", "external operator finalize", "network restart"} {
		if !strings.Contains(preview.Consequence, part) {
			t.Errorf("preview omitted %q: %s", part, preview.Consequence)
		}
	}
	if preview.ConsequenceVersion < 2 || preview.RemovalAvailable {
		t.Fatalf("candidate consequence was not versioned and gated: %+v", preview)
	}
}
