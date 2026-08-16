package node

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const testRecordStoreNetwork = domain.NetworkID("gazeta-devnet")

// issueTestRecord builds and self-verifies a record for owner, returning
// the pair rememberRecord expects from its verify-then-import caller.
func issueTestRecord(t *testing.T, owner *identity.Identity, seq domain.IdentityRecordSeq, dm bool) (protocol.SignedIdentityRecord, protocol.IdentityRecordBody) {
	t.Helper()
	record, err := protocol.BuildSignedIdentityRecord(owner, protocol.IdentityRecordSpec{
		Network:  testRecordStoreNetwork,
		DM:       dm,
		DTypes:   domain.ExplicitDTypes([]domain.DType{"get_identity"}),
		IssuedAt: 1780000000,
		Seq:      seq,
	})
	if err != nil {
		t.Fatalf("build record seq %d: %v", seq, err)
	}
	ownerID, err := domain.ParsePeerIdentity(owner.Address)
	if err != nil {
		t.Fatalf("parse owner address: %v", err)
	}
	body, err := protocol.VerifyIdentityRecord(record, testRecordStoreNetwork, ownerID)
	if err != nil {
		t.Fatalf("verify record seq %d: %v", seq, err)
	}
	return record, body
}

func newRecordTestStore(t *testing.T) (*trustStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trust.json")
	store, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}
	return store, path
}

// TestTrustStoreRecordPersistsAcrossReload: an accepted record survives a
// reload byte-identically — body and signature verbatim.
func TestTrustStoreRecordPersistsAcrossReload(t *testing.T) {
	store, path := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	record, body := issueTestRecord(t, owner, 3, true)

	outcome, err := store.rememberRecord(testRecordStoreNetwork, record, body)
	if err != nil {
		t.Fatalf("rememberRecord: %v", err)
	}
	if outcome != domain.IdentityRecordMergeInserted {
		t.Fatalf("outcome = %s, want inserted", outcome)
	}

	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	stored, storedBody, ok := reloaded.recordFor(testRecordStoreNetwork, body.Address)
	if !ok {
		t.Fatal("record lost across reload")
	}
	if string(stored.Body) != string(record.Body) || string(stored.Sig) != string(record.Sig) {
		t.Error("record bytes not preserved verbatim across reload")
	}
	if storedBody.Seq != 3 {
		t.Errorf("seq = %d, want 3", storedBody.Seq)
	}

	// The record must still verify from the reloaded bytes.
	if _, err := protocol.VerifyIdentityRecord(stored, testRecordStoreNetwork, body.Address); err != nil {
		t.Errorf("reloaded record fails verification: %v", err)
	}
}

// TestTrustStoreRecordSeqGate pins the four non-insert outcomes and their
// side effects on the stored record.
func TestTrustStoreRecordSeqGate(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	recordSeq2, bodySeq2 := issueTestRecord(t, owner, 2, true)
	if outcome, err := store.rememberRecord(testRecordStoreNetwork, recordSeq2, bodySeq2); err != nil || outcome != domain.IdentityRecordMergeInserted {
		t.Fatalf("seed: outcome=%v err=%v", outcome, err)
	}

	t.Run("stale is a silent no-op", func(t *testing.T) {
		recordSeq1, bodySeq1 := issueTestRecord(t, owner, 1, true)
		outcome, err := store.rememberRecord(testRecordStoreNetwork, recordSeq1, bodySeq1)
		if err != nil || outcome != domain.IdentityRecordMergeStale {
			t.Fatalf("outcome=%v err=%v, want stale", outcome, err)
		}
		if _, body, _ := store.recordFor(testRecordStoreNetwork, bodySeq1.Address); body.Seq != 2 {
			t.Errorf("stored seq = %d, want 2 untouched", body.Seq)
		}
	})

	t.Run("duplicate is a silent no-op", func(t *testing.T) {
		outcome, err := store.rememberRecord(testRecordStoreNetwork, recordSeq2, bodySeq2)
		if err != nil || outcome != domain.IdentityRecordMergeDuplicate {
			t.Fatalf("outcome=%v err=%v, want duplicate", outcome, err)
		}
	})

	t.Run("same seq different bytes is a conflict, store keeps its record", func(t *testing.T) {
		conflicting, conflictingBody := issueTestRecord(t, owner, 2, false)
		outcome, err := store.rememberRecord(testRecordStoreNetwork, conflicting, conflictingBody)
		if err != nil || outcome != domain.IdentityRecordMergeConflict {
			t.Fatalf("outcome=%v err=%v, want conflict", outcome, err)
		}
		if _, body, _ := store.recordFor(testRecordStoreNetwork, conflictingBody.Address); !body.DM {
			t.Error("conflict replaced the stored record")
		}
	})

	t.Run("higher seq replaces", func(t *testing.T) {
		recordSeq5, bodySeq5 := issueTestRecord(t, owner, 5, true)
		outcome, err := store.rememberRecord(testRecordStoreNetwork, recordSeq5, bodySeq5)
		if err != nil || outcome != domain.IdentityRecordMergeReplaced {
			t.Fatalf("outcome=%v err=%v, want replaced", outcome, err)
		}
		if _, body, _ := store.recordFor(testRecordStoreNetwork, bodySeq5.Address); body.Seq != 5 {
			t.Errorf("stored seq = %d, want 5", body.Seq)
		}
	})
}

// TestTrustStoreRecordReplacesPinnedContactKeys: the seq-gated replacement
// path — a verified higher-seq record MAY change pinned contact keys and
// clears the conflict marker, the one thing TOFU remember() refuses.
func TestTrustStoreRecordReplacesPinnedContactKeys(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	// Pin the contact with pre-record epidemic keys.
	if stored, err := store.remember(trustedContact{
		Address: owner.Address, PubKey: "old-pk", BoxKey: "old-bk", BoxSignature: "old-sig", Source: "test",
	}); err != nil || !stored {
		t.Fatalf("remember: stored=%v err=%v", stored, err)
	}
	// A conflicting epidemic triple marks a conflict and is refused.
	if _, err := store.remember(trustedContact{
		Address: owner.Address, PubKey: "other-pk", BoxKey: "other-bk", BoxSignature: "other-sig", Source: "test",
	}); !errors.Is(err, errTrustConflict) {
		t.Fatalf("expected errTrustConflict, got %v", err)
	}

	record, body := issueTestRecord(t, owner, 1, true)
	if outcome, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil || !outcome.Accepted() {
		t.Fatalf("rememberRecord: outcome=%v err=%v", outcome, err)
	}

	contact := store.trustedContacts()[owner.Address]
	if contact.PubKey != string(body.PubKey) || contact.BoxKey != string(body.BoxKey) || contact.BoxSignature != string(body.BoxSig) {
		t.Error("record did not replace pinned contact keys")
	}
	if contact.Source != trustContactSourceRecord {
		t.Errorf("contact source = %q, want %q", contact.Source, trustContactSourceRecord)
	}
	store.mu.RLock()
	_, conflictKept := store.conflicts[owner.Address]
	store.mu.RUnlock()
	if conflictKept {
		t.Error("accepted record must clear the conflict marker")
	}
}

// TestTrustStoreKeylessRecordClearsContactBoxKeys: a dm=false record is the
// owner's withdrawal of the box key; the contact must not keep encrypting
// to a tombstone.
func TestTrustStoreKeylessRecordClearsContactBoxKeys(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if stored, err := store.remember(trustedContact{
		Address: owner.Address, PubKey: "old-pk", BoxKey: "old-bk", BoxSignature: "old-sig", Source: "test",
	}); err != nil || !stored {
		t.Fatalf("remember: stored=%v err=%v", stored, err)
	}

	record, body := issueTestRecord(t, owner, 1, false)
	if outcome, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil || !outcome.Accepted() {
		t.Fatalf("rememberRecord: outcome=%v err=%v", outcome, err)
	}

	contact := store.trustedContacts()[owner.Address]
	if contact.BoxKey != "" || contact.BoxSignature != "" {
		t.Error("keyless record must clear contact box fields")
	}
	if contact.PubKey != string(body.PubKey) {
		t.Error("keyless record must still update the signing key")
	}
}

// TestTrustStoreRecordKeyIncludesNetwork: records of one address on two
// networks occupy separate slots, and a lookup on the wrong network answers
// nothing.
func TestTrustStoreRecordKeyIncludesNetwork(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	record, body := issueTestRecord(t, owner, 1, true)

	if outcome, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil || !outcome.Accepted() {
		t.Fatalf("rememberRecord: outcome=%v err=%v", outcome, err)
	}

	if _, _, ok := store.recordFor("other-net", body.Address); ok {
		t.Error("record leaked across networks")
	}

	otherRecord, otherBody := issueTestRecord(t, owner, 7, true)
	if outcome, err := store.rememberRecord("other-net", otherRecord, otherBody); err != nil || outcome != domain.IdentityRecordMergeInserted {
		t.Fatalf("other-net insert: outcome=%v err=%v — the composite key must give it its own slot", outcome, err)
	}
	if _, body1, _ := store.recordFor(testRecordStoreNetwork, body.Address); body1.Seq != 1 {
		t.Errorf("first network seq = %d, want 1 untouched", body1.Seq)
	}
}

// TestTrustStoreForgetDropsRecord: deleting a contact takes its stored
// record along.
func TestTrustStoreForgetDropsRecord(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	record, body := issueTestRecord(t, owner, 1, true)
	if stored, err := store.remember(trustedContact{Address: owner.Address, PubKey: "pk", Source: "test"}); err != nil || !stored {
		t.Fatalf("remember: stored=%v err=%v", stored, err)
	}
	if _, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("rememberRecord: %v", err)
	}

	if removed, err := store.forget(body.Address); err != nil || !removed {
		t.Fatalf("forget: removed=%v err=%v", removed, err)
	}
	if _, _, ok := store.recordFor(testRecordStoreNetwork, body.Address); ok {
		t.Error("record survived the contact's deletion")
	}
}

// TestTrustStoreLegacyMigration: a pre-version contacts-only file loads
// with every contact intact and is rewritten as the current schema on the
// first save.
func TestTrustStoreLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")
	legacy := `{"contacts":{"aabbccddeeff00112233445566778899aabbccdd":{` +
		`"address":"aabbccddeeff00112233445566778899aabbccdd","pub_key":"pk","box_key":"bk",` +
		`"box_signature":"sig","first_seen_at":"2024-01-01T00:00:00Z","last_seen_at":"2024-01-01T00:00:00Z",` +
		`"source":"legacy"}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	store, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("load legacy store: %v", err)
	}
	contact, ok := store.trustedContacts()["aabbccddeeff00112233445566778899aabbccdd"]
	if !ok || contact.PubKey != "pk" {
		t.Fatal("legacy contact lost in migration")
	}

	// loadTrustStore always saves; the file must now carry the current
	// version and an intact contact set.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migrated file: %v", err)
	}
	var file trustFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("unmarshal migrated file: %v", err)
	}
	if file.Version != trustFileVersion {
		t.Errorf("version = %d, want %d", file.Version, trustFileVersion)
	}
	if _, ok := file.Contacts["aabbccddeeff00112233445566778899aabbccdd"]; !ok {
		t.Error("contact missing from migrated file")
	}

	// And the migrated file loads again cleanly.
	if _, err := loadTrustStore(path, trustedContact{}); err != nil {
		t.Errorf("reload migrated store: %v", err)
	}
}

// TestTrustStoreSkipsTornRecordRow: one broken record row is skipped with a
// log, never fatal for the store or the contacts.
func TestTrustStoreSkipsTornRecordRow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")
	corrupt := `{"version":2,"contacts":{},"records":[{"network":"gazeta-devnet",` +
		`"address":"aabbccddeeff00112233445566778899aabbccdd","v":1,"body":"%%%not-base64%%%","sig":"","stored_at":"2024-01-01T00:00:00Z"}]}`
	if err := os.WriteFile(path, []byte(corrupt), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	store, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("load with torn row: %v", err)
	}
	addr, _ := domain.ParsePeerIdentity("aabbccddeeff00112233445566778899aabbccdd")
	if _, _, ok := store.recordFor(testRecordStoreNetwork, addr); ok {
		t.Error("torn row must be skipped, not restored")
	}
}

// TestEnsureSelfIdentityRecordLifecycle: first start issues seq 1 and
// persists BEFORE returning; an identical spec re-issues nothing; any
// content change — dtypes upgrade or rollback, dm flip — bumps seq.
func TestEnsureSelfIdentityRecordLifecycle(t *testing.T) {
	store, path := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	now := time.Unix(1780000000, 0)

	spec := selfRecordSpec{
		network: testRecordStoreNetwork,
		dm:      true,
		dtypes:  domain.ExplicitDTypes([]domain.DType{"get_identity", "post_identity"}),
	}

	first, firstBody, err := ensureSelfIdentityRecord(store, owner, spec, now)
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if firstBody.Seq != 1 {
		t.Errorf("first seq = %d, want 1", firstBody.Seq)
	}

	// Publish-after-persist: the returned record is already on disk.
	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if stored, _, ok := reloaded.recordFor(spec.network, firstBody.Address); !ok || string(stored.Body) != string(first.Body) {
		t.Fatal("returned record not persisted before publish")
	}

	// Same spec on restart: no re-issue, no seq churn.
	second, secondBody, err := ensureSelfIdentityRecord(reloaded, owner, spec, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if secondBody.Seq != 1 || string(second.Body) != string(first.Body) {
		t.Errorf("identical spec re-issued: seq %d, body changed=%v", secondBody.Seq, string(second.Body) != string(first.Body))
	}

	// A dtypes change (upgrade) bumps the seq.
	upgraded := spec
	upgraded.dtypes = domain.ExplicitDTypes([]domain.DType{"get_identity", "post_identity", "push_identity"})
	_, thirdBody, err := ensureSelfIdentityRecord(reloaded, owner, upgraded, now.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("third ensure: %v", err)
	}
	if thirdBody.Seq != 2 {
		t.Errorf("dtypes change seq = %d, want 2", thirdBody.Seq)
	}

	// Rolling BACK to the old set is also a change and must bump again —
	// a rollback that reused seq 1 would conflict at every receiver.
	_, fourthBody, err := ensureSelfIdentityRecord(reloaded, owner, spec, now.Add(3*time.Hour))
	if err != nil {
		t.Fatalf("fourth ensure: %v", err)
	}
	if fourthBody.Seq != 3 {
		t.Errorf("rollback seq = %d, want 3", fourthBody.Seq)
	}

	// A dm flip issues a keyless record under the next seq.
	keyless := spec
	keyless.dm = false
	_, fifthBody, err := ensureSelfIdentityRecord(reloaded, owner, keyless, now.Add(4*time.Hour))
	if err != nil {
		t.Fatalf("fifth ensure: %v", err)
	}
	if fifthBody.Seq != 4 || fifthBody.DM {
		t.Errorf("dm flip: seq=%d dm=%v, want 4/false", fifthBody.Seq, fifthBody.DM)
	}

	// Absent vs explicitly-empty dtypes are different statements; moving
	// between them is a content change.
	absent := keyless
	absent.dtypes = domain.AbsentDTypes()
	emptySet := keyless
	emptySet.dtypes = domain.ExplicitDTypes(nil)
	_, absentBody, err := ensureSelfIdentityRecord(reloaded, owner, absent, now.Add(5*time.Hour))
	if err != nil {
		t.Fatalf("absent ensure: %v", err)
	}
	_, emptyBody, err := ensureSelfIdentityRecord(reloaded, owner, emptySet, now.Add(6*time.Hour))
	if err != nil {
		t.Fatalf("empty-set ensure: %v", err)
	}
	if emptyBody.Seq != absentBody.Seq+1 {
		t.Errorf("absent→empty did not bump: %d → %d", absentBody.Seq, emptyBody.Seq)
	}
}

// TestEnsureSelfIdentityRecordHonoursBackupFloor: a restored backup
// carries the pre-backup seq; the first record issued after the restore
// must start ABOVE it, or peers holding the old record reject the new one
// as stale.
func TestEnsureSelfIdentityRecordHonoursBackupFloor(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	owner.RecordSeqFloor = 41

	_, body, err := ensureSelfIdentityRecord(store, owner, selfRecordSpec{
		network: testRecordStoreNetwork,
		dm:      true,
		dtypes:  domain.AbsentDTypes(),
	}, time.Unix(1780000000, 0))
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if body.Seq != 42 {
		t.Fatalf("seq = %d, want floor+1 = 42", body.Seq)
	}
}

// TestTrustStoreRejectsTamperedRecordRow: a persisted record whose
// signature no longer verifies is dropped at load — a corrupted row must
// not survive a restart and keep re-publishing via push_identity.
func TestTrustStoreRejectsTamperedRecordRow(t *testing.T) {
	store, path := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	record, body := issueTestRecord(t, owner, 3, true)
	if _, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("remember: %v", err)
	}

	// Corrupt the signature on disk.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var payload trustFile
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("records = %d", len(payload.Records))
	}
	sig, err := base64.RawURLEncoding.DecodeString(payload.Records[0].Sig)
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	sig[0] ^= 0xFF
	payload.Records[0].Sig = base64.RawURLEncoding.EncodeToString(sig)
	tampered, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, _, ok := reloaded.recordFor(testRecordStoreNetwork, body.Address); ok {
		t.Fatal("a tampered record survived the reload")
	}
}

// TestEnsureSelfIdentityRecordFailedPersistIsNotPublished: when the store
// cannot persist, ensure returns an error and no record — the reserve →
// persist → publish order means a caller never observes an unpersisted seq.
func TestEnsureSelfIdentityRecordFailedPersistIsNotPublished(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "trust.json")
	store, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}
	// Make the store directory read-only so the save's temp-file write fails.
	if err := os.Chmod(filepath.Join(dir, "sub"), 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "sub"), 0o700) })

	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	_, _, err = ensureSelfIdentityRecord(store, owner, selfRecordSpec{
		network: testRecordStoreNetwork,
		dm:      true,
		dtypes:  domain.AbsentDTypes(),
	}, time.Unix(1780000000, 0))
	if err == nil {
		t.Fatal("ensure succeeded with an unwritable store — an unpersisted seq was published")
	}
}

// TestTrustStoreStaleSnapshotCannotClobberNewer: mutators persist outside
// the state mutex, so two saves can reach the disk in either order — the
// generation stamp must keep an older snapshot from overwriting a newer
// file (the durable-record loss the race would cause is silent).
func TestTrustStoreStaleSnapshotCannotClobberNewer(t *testing.T) {
	store, path := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	// The OLD (empty) snapshot, taken before the record lands.
	store.mu.Lock()
	stale := store.snapshotLocked()
	store.mu.Unlock()

	record, body := issueTestRecord(t, owner, 3, true)
	if _, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("remember: %v", err)
	}

	// The stale save arrives LAST — the losing side of the race.
	if err := store.saveSnapshot(stale); err != nil {
		t.Fatalf("stale save: %v", err)
	}

	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, _, ok := reloaded.recordFor(testRecordStoreNetwork, body.Address); !ok {
		t.Fatal("a stale snapshot overwrote the newer trust store on disk")
	}
}
