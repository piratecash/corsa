//go:build !windows

package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func TestOrdinaryPinWriterKeepsExactPrivateBytesAndMemoryOnlyPolicy(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "new", "nested")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "pins.json")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	raw := []byte("exact pin bytes\x00\n")
	if err := writeOrdinaryPinFile(path, raw); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("ordinary writer changed bytes: %q %v", got, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stamp, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || stamp.Uid != uint32(os.Geteuid()) || stamp.Nlink != 1 {
		t.Fatalf("ordinary replacement not private: %v", info)
	}
	if err := secretfile.AuditLocation(path); err != nil {
		t.Fatal(err)
	}
	store := loadSecureSessionStore("", newStoreClock().trusted())
	if err := store.noteProvenInbound(domain.PeerIdentity{41}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	memoryOnly := store.memoryOnlyLocked()
	acknowledged := store.changes == store.written
	store.mu.Unlock()
	if !memoryOnly || !acknowledged || !store.markerWritten {
		t.Fatal("explicit memory-only policy acquired a persistent marker requirement")
	}
}

// Per-store seams test propagation/order; a visible-store failure below is
// modeled after a completed real write, not a physical fsync or powercut.
func TestOrdinaryPinAcknowledgementWaitsForStoreAndMarker(t *testing.T) {
	for _, failure := range []string{"store-before-visible", "store-after-visible", "marker", "none"} {
		t.Run(failure, func(t *testing.T) {
			store, _ := openPinStore(t, newPinStorePaths(t), newStoreClock())
			requireStarts(t, store)
			originalWrite, originalMarker := store.writeFile, store.writeMarker
			injected := errors.New("ordinary persistence refused")
			var order []string
			fail := true
			store.writeFile = func(raw []byte) error {
				order = append(order, "store")
				if fail && failure == "store-before-visible" {
					return injected
				}
				if err := originalWrite(raw); err != nil {
					return err
				}
				if fail && failure == "store-after-visible" {
					return injected
				}
				return nil
			}
			store.writeMarker = func(raw []byte) error {
				order = append(order, "marker")
				if fail && failure == "marker" {
					return injected
				}
				return originalMarker(raw)
			}
			identity := domain.PeerIdentity{42}
			err := store.noteProvenInbound(identity)
			if failure == "none" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, injected) {
				t.Fatalf("local error lost: %v", err)
			}
			store.mu.Lock()
			pending := store.changes != store.written
			store.mu.Unlock()
			if !store.identityPinned(identity) || pending != (failure != "none") || store.markerWritten != (failure == "none") {
				t.Fatal("failed save discarded memory protection or acknowledged durable success")
			}
			want := []string{"store"}
			if failure == "marker" || failure == "none" {
				want = append(want, "marker")
			}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("store/marker order=%v want=%v", order, want)
			}
			if failure != "none" {
				fail = false
				order = nil
				if err := store.noteProvenInbound(identity); err != nil {
					t.Fatalf("same-pin pending retry failed: %v", err)
				}
				store.mu.Lock()
				pending = store.changes != store.written
				store.mu.Unlock()
				if pending || !store.markerWritten || !reflect.DeepEqual(order, []string{"store", "marker"}) {
					t.Fatal("same-pin retry acknowledged before both stages landed")
				}
			}
		})
	}
}

func TestOrdinaryPinSnapshotDoesNotAcknowledgeLaterMutation(t *testing.T) {
	store, _ := openPinStore(t, newPinStorePaths(t), newStoreClock())
	requireStarts(t, store)
	actual := store.writeFile
	firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	finishFirst := sync.OnceFunc(func() { close(releaseFirst) })
	finishSecond := sync.OnceFunc(func() { close(releaseSecond) })
	defer finishFirst()
	defer finishSecond()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writes := 0
	store.writeFile = func(raw []byte) error {
		writes++
		wait := releaseFirst
		switch writes {
		case 1:
			close(firstEntered)
		case 2:
			close(secondEntered)
			wait = releaseSecond
		default:
			return fmt.Errorf("unexpected extra write %d", writes)
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		return actual(raw)
	}
	a, b := domain.PeerIdentity{43}, domain.PeerIdentity{44}
	aDone, bDone := make(chan error, 1), make(chan error, 1)
	var aExited, bExited chan struct{}
	t.Cleanup(func() {
		cancel()
		finishFirst()
		finishSecond()
		for _, exited := range []chan struct{}{aExited, bExited} {
			if exited == nil {
				continue
			}
			select {
			case <-exited:
			case <-time.After(2 * time.Second):
				t.Error("owned pin writer did not join before temporary-file cleanup")
			}
		}
	})
	aExited = make(chan struct{})
	go func() { defer close(aExited); aDone <- store.noteProvenInbound(a) }()
	select {
	case <-firstEntered:
	case <-ctx.Done():
		t.Fatal("first save never reached held writer")
	}
	bExited = make(chan struct{})
	go func() { defer close(bExited); bDone <- store.noteProvenInbound(b) }()
	waitForConditionMsg(t, 2*time.Second, "later pin did not enter memory before first save", func() bool { return store.identityPinned(b) })
	store.mu.Lock()
	beforeWritten, beforeChanges := store.written, store.changes
	store.mu.Unlock()
	if beforeWritten >= beforeChanges {
		t.Fatal("held first save acknowledged a later mutation")
	}
	finishFirst()
	select {
	case <-secondEntered:
	case <-ctx.Done():
		t.Fatal("second save never reached held writer")
	}
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first save did not finish")
	}
	store.mu.Lock()
	covered, changed := store.written, store.changes
	store.mu.Unlock()
	if covered <= beforeWritten || covered >= changed {
		t.Fatalf("first snapshot covered later mutation: written=%d changes=%d", covered, changed)
	}
	select {
	case <-bDone:
		t.Fatal("later save returned before its own writer completed")
	default:
	}
	finishSecond()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("second save did not finish")
	}
	store.mu.Lock()
	complete := store.written == store.changes
	store.mu.Unlock()
	if !complete || !store.identityPinned(a) || !store.identityPinned(b) {
		t.Fatal("completed saves lost protection or left an unacknowledged change")
	}
}

// Calls the real startup state machine with its existing per-store writer
// seams, initialized as the constructor does. This is function-level fault
// propagation, not an injected kernel crash or a second application process.
func TestOrdinaryPinStartupConfirmsValidatedRawBeforeMarker(t *testing.T) {
	for _, situation := range []string{"kept", "missing-marker", "store-error", "marker-error", "two-stores"} {
		t.Run(situation, func(t *testing.T) {
			paths := newPinStorePaths(t)
			clock := newStoreClock()
			seed, _ := openPinStore(t, paths, clock)
			requireStarts(t, seed)
			identity := domain.PeerIdentity{45}
			if err := seed.noteProvenInbound(identity); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(paths.store)
			if err != nil {
				t.Fatal(err)
			}
			validated := append([]byte(" \n\t"), original...)
			validated = append(validated, '\n')
			if err := os.WriteFile(paths.store, validated, 0600); err != nil {
				t.Fatal(err)
			}
			if situation == "missing-marker" {
				if err := os.Remove(paths.marker); err != nil {
					t.Fatal(err)
				}
			}
			if situation == "two-stores" {
				other := filepath.Join(filepath.Dir(paths.store), "copied-pins.json")
				if err := os.WriteFile(other, validated, 0600); err != nil {
					t.Fatal(err)
				}
				paths.store = other
			}
			store := newSecureSessionStore(clock.trusted(), zerolog.Nop())
			store.dir, store.name = filepath.Dir(paths.store), filepath.Base(paths.store)
			store.markerPath = paths.marker
			var order []string
			injected := errors.New("startup confirmation refused")
			store.writeFile = func(raw []byte) error {
				order = append(order, "store")
				if !bytes.Equal(raw, validated) {
					return errors.New("startup recomputed validated bytes")
				}
				if situation == "store-error" {
					return injected
				}
				return writeOrdinaryPinFile(paths.store, raw)
			}
			store.writeMarker = func(raw []byte) error {
				order = append(order, "marker")
				if situation == "marker-error" {
					return injected
				}
				return writePinStoreMarker(paths.marker, raw, secretfile.AuditLocation)
			}
			refusal := store.startFromDisk(secureSessionStoreOptions{path: paths.store, markerPath: paths.marker, audit: secretfile.AuditLocation})
			switch situation {
			case "store-error":
				if refusal == nil || !errors.Is(refusal, injected) || !errors.Is(refusal, domain.ErrPinStoreRefused) || store.markerWritten || !reflect.DeepEqual(order, []string{"store"}) {
					t.Fatalf("visible main bypassed failed confirmation: refusal=%v order=%v", refusal, order)
				}
			case "marker-error":
				if refusal == nil || !errors.Is(refusal, injected) || refusal.Reason != domain.PinStoreRefusalMarkerUnwritable || store.markerWritten || !reflect.DeepEqual(order, []string{"store", "marker"}) {
					t.Fatalf("visible marker bypassed failed reconfirmation: refusal=%v order=%v", refusal, order)
				}
			case "two-stores":
				if refusal == nil || refusal.Reason != domain.PinStoreRefusalTwoStores || len(order) != 0 {
					t.Fatalf("conflicting stores were rewritten before refusal: refusal=%v order=%v", refusal, order)
				}
			default:
				if refusal != nil || !store.markerWritten || !reflect.DeepEqual(order, []string{"store", "marker"}) || !store.identityPinned(identity) {
					t.Fatalf("accepted restart did not confirm main then marker: refusal=%v order=%v", refusal, order)
				}
			}
			retained, err := os.ReadFile(paths.store)
			if err != nil || !bytes.Equal(retained, validated) {
				t.Fatal("startup changed or discarded validated visible evidence")
			}
		})
	}
	unsafe := &secretfile.UnsafeLocationError{Violation: secretfile.ViolationFileNotOwnerOnly}
	refusal := refuseUnsafeLocation(pinStoreRoleStore, "private-pins.json", unsafe)
	actual, found := refusal.unsafeLocation()
	if !found || actual != unsafe || !errors.Is(refusal, secretfile.ErrUnsafeLocation) {
		t.Fatal("location refusal lost its exact underlying unsafe error")
	}
}
