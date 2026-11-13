package node

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Hold a legacy cache publication after it reads the authoritative record.
// The next record must not be merged until that publication can finish;
// otherwise its older key could be written after the newer record's key.
func TestRecordReplacementOrdersWithLegacyBoxPublication(t *testing.T) {
	for _, dm := range []bool{true, false} {
		name := "rotation"
		if !dm {
			name = "revocation"
		}
		t.Run(name, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			owner, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			rotatedBox, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			rotated := *owner
			rotated.BoxPrivateKey, rotated.BoxPublicKey = rotatedBox.BoxPrivateKey, rotatedBox.BoxPublicKey
			trustOwner(t, svc.trust, owner)
			initial, initialBody := issueTestRecord(t, owner, 3, true)
			if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, initial, initialBody); err != nil {
				t.Fatal(err)
			}
			replacement, replacementBody := issueTestRecord(t, &rotated, 4, dm)

			var jobs []<-chan struct{}
			svc.knowledgeMu.Lock()
			locked := true
			t.Cleanup(func() {
				if locked {
					svc.knowledgeMu.Unlock()
				}
				for _, done := range jobs {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("key publication did not finish cleanup")
					}
				}
			})
			waitFor := func(label string, ready func() bool) {
				t.Helper()
				deadline := time.NewTimer(3 * time.Second)
				defer deadline.Stop()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for !ready() {
					select {
					case <-deadline.C:
						t.Fatal(label)
					case <-tick.C:
					}
				}
			}

			legacyDone := make(chan struct{})
			jobs = append(jobs, legacyDone)
			go func() {
				defer close(legacyDone)
				svc.addKnownBoxPair(owner.Address, identity.BoxPublicKeyBase64(owner.BoxPublicKey), identity.SignBoxKeyBinding(owner))
			}()
			waitFor("legacy publication released record protection before its cache write", func() bool {
				if svc.trust.mu.TryLock() {
					svc.trust.mu.Unlock()
					return false
				}
				return true
			})

			mergeDone := make(chan struct{})
			jobs = append(jobs, mergeDone)
			var mergeErr error
			var outcome domain.IdentityRecordMergeOutcome
			go func() {
				defer close(mergeDone)
				outcome, mergeErr = svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replacement, replacementBody)
			}()
			// An RWMutex reader can coexist with the legacy reader; it fails
			// here once the replacement writer is actually queued behind it.
			waitFor("record replacement never reached the publication barrier", func() bool {
				if svc.trust.mu.TryRLock() {
					svc.trust.mu.RUnlock()
					return false
				}
				return true
			})
			select {
			case <-mergeDone:
				t.Fatal("record replacement passed a still-pending legacy cache write")
			default:
			}
			svc.knowledgeMu.Unlock()
			locked = false
			waitFor("legacy publication did not finish", func() bool {
				select {
				case <-legacyDone:
					return true
				default:
					return false
				}
			})
			waitFor("record replacement did not finish", func() bool {
				select {
				case <-mergeDone:
					return true
				default:
					return false
				}
			})
			if mergeErr != nil || outcome != domain.IdentityRecordMergeReplaced {
				t.Fatalf("record replacement: outcome=%v err=%v", outcome, mergeErr)
			}
			svc.knowledgeMu.RLock()
			key, sig := svc.boxKeys[owner.Address], svc.boxSigs[owner.Address]
			svc.knowledgeMu.RUnlock()
			if key != string(replacementBody.BoxKey) || sig != string(replacementBody.BoxSig) {
				t.Fatalf("legacy publication survived the replacement: key=%q sig=%q", key, sig)
			}
			// A failed older save can return after this newer publication.
			// Its cache cleanup must preserve the now-consistent pair.
			svc.discardSupersededRecordBoxPair(owner.Address)
			svc.knowledgeMu.RLock()
			preserved := svc.boxKeys[owner.Address] == key && svc.boxSigs[owner.Address] == sig
			svc.knowledgeMu.RUnlock()
			if !preserved {
				t.Fatal("late failure cleanup removed the newer published pair")
			}
			for _, row := range svc.trustedContactsFrame().Contacts {
				if row.Address == owner.Address {
					if row.BoxKey != key || row.BoxSig != sig {
						t.Fatal("trusted contact and cache disagree after publication")
					}
					return
				}
			}
			t.Fatal("trusted contact missing after record replacement")
		})
	}
}

func TestRecordWriteFailureCannotKeepSupersededBoxPair(t *testing.T) {
	for _, dm := range []bool{true, false} {
		name := "rotation"
		if !dm {
			name = "revocation"
		}
		t.Run(name, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			owner, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			rotatedBox, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			rotated := *owner
			rotated.BoxPrivateKey, rotated.BoxPublicKey = rotatedBox.BoxPrivateKey, rotatedBox.BoxPublicKey
			trustOwner(t, svc.trust, owner)
			initial, initialBody := issueTestRecord(t, owner, 3, true)
			if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, initial, initialBody); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(svc.trust.path)
			if err != nil {
				t.Fatal(err)
			}
			// A directory at this fixture's own temporary-file path makes
			// WriteFile fail portably, without relying on uid or chmod.
			blocker := svc.trust.path + ".tmp"
			if err := os.Mkdir(blocker, 0o700); err != nil {
				t.Fatal(err)
			}
			replacement, replacementBody := issueTestRecord(t, &rotated, 4, dm)
			if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replacement, replacementBody); err == nil {
				t.Fatal("record import concealed the failed write")
			}
			after, err := os.ReadFile(svc.trust.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed temporary write changed the durable snapshot: %v", err)
			}
			if key, ok := svc.knownBoxKey(owner.Address); ok && key == string(initialBody.BoxKey) {
				t.Error("failed record write left the superseded box key usable")
			}
			if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replacement, replacementBody); err == nil {
				t.Error("duplicate import concealed the still-unconfirmed record write")
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replacement, replacementBody); err != nil {
				t.Fatalf("retry after repairing the write path: %v", err)
			}
			reloaded, err := loadTrustStore(svc.trust.path, trustedContact{})
			if err != nil {
				t.Fatal(err)
			}
			_, durable, found := reloaded.recordFor(testRecordStoreNetwork, replacementBody.Address)
			if !found || durable.Seq != replacementBody.Seq || durable.DM != dm {
				t.Errorf("successful retry did not persist the replacement: found=%v seq=%v dm=%v", found, durable.Seq, durable.DM)
			}
			// The now-durable duplicate must remain a no-write operation.
			if err := os.Mkdir(blocker, 0o700); err != nil {
				t.Fatal(err)
			}
			if outcome, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replacement, replacementBody); err != nil || outcome != domain.IdentityRecordMergeDuplicate {
				t.Fatalf("durable duplicate touched the blocked write path: outcome=%v err=%v", outcome, err)
			}
		})
	}
}

func TestSessionRecordDuplicateDoesNotFlushPendingPersistentRecord(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	trustOwner(t, svc.trust, owner)
	initial, initialBody := issueTestRecord(t, owner, 3, true)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, initial, initialBody); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(svc.trust.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	pending, pendingBody := issueTestRecord(t, owner, 4, false)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, pending, pendingBody); err == nil {
		t.Fatal("persistent import unexpectedly passed the blocked write path")
	}
	outsider, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cached, cachedBody := issueTestRecord(t, outsider, 1, true)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, cached, cachedBody); err != nil {
		t.Fatalf("session-cache import attempted a disk flush: %v", err)
	}
	// A session-cache duplicate must also stay independent of a stalled
	// persistent writer, rather than merely avoiding its eventual WriteFile.
	svc.trust.saveMu.Lock()
	type importResult struct {
		outcome domain.IdentityRecordMergeOutcome
		err     error
	}
	done := make(chan importResult, 1)
	go func() {
		outcome, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, cached, cachedBody)
		done <- importResult{outcome: outcome, err: err}
	}()
	var result importResult
	var blocked bool
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		blocked = true
	}
	svc.trust.saveMu.Unlock()
	if blocked {
		select {
		case result = <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("session-cache duplicate did not finish after releasing the writer")
		}
		t.Error("session-cache duplicate waited for an unrelated persistent writer")
	}
	if result.err != nil || result.outcome != domain.IdentityRecordMergeDuplicate {
		t.Fatalf("session-cache duplicate attempted a disk flush: outcome=%v err=%v", result.outcome, result.err)
	}
	// The loader saves its own startup snapshot. End the write obstruction
	// after the operation under test and before checking the disk on restart.
	if err := os.Remove(svc.trust.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := loadTrustStore(svc.trust.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	_, durable, found := reloaded.recordFor(testRecordStoreNetwork, initialBody.Address)
	if !found || durable.Seq != initialBody.Seq {
		t.Fatal("session-cache duplicate changed the pending persistent record")
	}
	if _, _, found := reloaded.recordFor(testRecordStoreNetwork, cachedBody.Address); found {
		t.Fatal("session-cache duplicate persisted an untrusted session record")
	}
}
