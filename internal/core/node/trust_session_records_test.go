package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
)

func newSessionRecordTestStore(t *testing.T) (*trustStore, string, *time.Time) {
	t.Helper()
	store, path := newRecordTestStore(t)
	now := time.Unix(1_780_000_000, 0).UTC()
	store.clock = func() time.Time { return now }
	return store, path, &now
}

func mustRememberRecord(t *testing.T, store *trustStore, owner *identity.Identity, seq domain.IdentityRecordSeq, dm bool) domain.IdentityRecordMergeOutcome {
	t.Helper()
	record, body := issueTestRecord(t, owner, seq, dm)
	outcome, err := store.rememberRecord(testRecordStoreNetwork, record, body)
	if err != nil {
		t.Fatalf("rememberRecord seq %d: %v", seq, err)
	}
	return outcome
}

func mustGenerate(t *testing.T) *identity.Identity {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return owner
}

func recordRowsOnDisk(t *testing.T, path string) []trustRecordRow {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trust file: %v", err)
	}
	var payload trustFile
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("decode trust file: %v", err)
	}
	return payload.Records
}

// A session peer's record is session memory (identity-lookup.md §4): it is
// readable while cached, and it never reaches the disk.
func TestSessionPeerRecordIsCachedNotPersisted(t *testing.T) {
	t.Parallel()
	store, path, _ := newSessionRecordTestStore(t)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)

	if outcome := mustRememberRecord(t, store, peer, 2, true); outcome != domain.IdentityRecordMergeInserted {
		t.Fatalf("outcome = %s, want inserted", outcome)
	}
	if _, body, ok := store.recordFor(testRecordStoreNetwork, peerID); !ok || body.Seq != 2 {
		t.Fatalf("cached record not readable: ok=%v", ok)
	}
	if rows := recordRowsOnDisk(t, path); len(rows) != 0 {
		t.Fatalf("session peer record written to disk: %+v", rows)
	}
	usage := store.sessionRecordUsage()
	if usage.count != 1 || usage.bytes <= int(sessionRecordStructBytes) {
		t.Fatalf("cache usage = %+v, want one entry priced above its struct", usage)
	}

	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, _, ok := reloaded.recordFor(testRecordStoreNetwork, peerID); ok {
		t.Fatal("session peer record survived a restart")
	}
}

// The seq gate holds while the record is cached: stale is refused, a
// duplicate is a no-op, a higher seq replaces, and dm:false lands as the
// stored truth.
func TestSessionPeerRecordKeepsSeqGate(t *testing.T) {
	t.Parallel()
	store, _, _ := newSessionRecordTestStore(t)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)

	mustRememberRecord(t, store, peer, 3, true)
	if outcome := mustRememberRecord(t, store, peer, 2, true); outcome != domain.IdentityRecordMergeStale {
		t.Fatalf("lower seq outcome = %s, want stale", outcome)
	}
	if outcome := mustRememberRecord(t, store, peer, 3, true); outcome != domain.IdentityRecordMergeDuplicate {
		t.Fatalf("same record outcome = %s, want duplicate", outcome)
	}
	if outcome := mustRememberRecord(t, store, peer, 4, false); outcome != domain.IdentityRecordMergeReplaced {
		t.Fatalf("higher seq outcome = %s, want replaced", outcome)
	}
	_, body, ok := store.recordFor(testRecordStoreNetwork, peerID)
	if !ok || body.Seq != 4 || body.DM {
		t.Fatalf("stored body = %+v ok=%v, want seq 4 with dm:false", body, ok)
	}
	if usage := store.sessionRecordUsage(); usage.count != 1 {
		t.Fatalf("a replacement must not add an entry: count=%d", usage.count)
	}
}

// Ever-new session peers with a constant number of live sessions must
// leave the cache at its budget, never above it — memory plateaus.
func TestSessionPeerRecordsPlateauUnderChurn(t *testing.T) {
	t.Parallel()
	store, _, _ := newSessionRecordTestStore(t)
	store.sessionRecords = newSessionRecordCache(16, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)

	for i := 0; i < 100; i++ {
		mustRememberRecord(t, store, mustGenerate(t), 1, true)
		if usage := store.sessionRecordUsage(); usage.count > 16 {
			t.Fatalf("round %d: cache holds %d records over a budget of 16", i, usage.count)
		}
	}
	usage := store.sessionRecordUsage()
	if usage.count != 16 || usage.evicted != 84 || usage.evictedBytes == 0 {
		t.Fatalf("usage after churn = %+v, want 16 live and 84 evicted", usage)
	}

	// The byte budget is enforced independently of the count.
	store.sessionRecords = newSessionRecordCache(maxSessionIdentityRecords, 3000, sessionIdentityRecordTTL)
	for i := 0; i < 20; i++ {
		mustRememberRecord(t, store, mustGenerate(t), 1, true)
	}
	usage = store.sessionRecordUsage()
	if usage.bytes > 3000 || usage.count == 0 || usage.count >= 20 {
		t.Fatalf("byte budget not enforced: %+v", usage)
	}
}

// Records age out on the maintenance sweep, oldest first, and a record
// that is still inside its TTL stays.
func TestSessionPeerRecordsExpire(t *testing.T) {
	t.Parallel()
	store, _, now := newSessionRecordTestStore(t)
	old := mustGenerate(t)
	oldID, _ := domain.ParsePeerIdentity(old.Address)
	mustRememberRecord(t, store, old, 1, true)

	*now = now.Add(sessionIdentityRecordTTL / 2)
	fresh := mustGenerate(t)
	freshID, _ := domain.ParsePeerIdentity(fresh.Address)
	mustRememberRecord(t, store, fresh, 1, true)

	*now = now.Add(sessionIdentityRecordTTL/2 + time.Second)
	if swept := store.sweepSessionRecords(*now); swept != 1 {
		t.Fatalf("swept %d records, want 1", swept)
	}
	if _, _, ok := store.recordFor(testRecordStoreNetwork, oldID); ok {
		t.Fatal("record past its TTL survived the sweep")
	}
	if _, _, ok := store.recordFor(testRecordStoreNetwork, freshID); !ok {
		t.Fatal("record inside its TTL was swept")
	}
	if usage := store.sessionRecordUsage(); usage.expired != 1 || usage.count != 1 {
		t.Fatalf("usage after sweep = %+v", usage)
	}
}

// Trusting an identity promotes its cached record to the persistent set:
// it is now the interlocutor's record and belongs on disk, with the seq
// gate no longer subject to the cache budget.
func TestTrustingAPeerPromotesItsRecordToDisk(t *testing.T) {
	t.Parallel()
	store, path, _ := newSessionRecordTestStore(t)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	mustRememberRecord(t, store, peer, 5, true)

	if stored, err := store.remember(trustedContact{Address: peer.Address, PubKey: "pk", Source: "test"}); err != nil || !stored {
		t.Fatalf("remember: stored=%v err=%v", stored, err)
	}
	if usage := store.sessionRecordUsage(); usage.count != 0 {
		t.Fatalf("record still cached after promotion: %+v", usage)
	}
	rows := recordRowsOnDisk(t, path)
	if len(rows) != 1 || rows[0].Address != peer.Address {
		t.Fatalf("disk rows after promotion = %+v, want the contact's record", rows)
	}
	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, body, ok := reloaded.recordFor(testRecordStoreNetwork, peerID); !ok || body.Seq != 5 {
		t.Fatalf("promoted record lost across reload: ok=%v body=%+v", ok, body)
	}
	// From now on the record is persistent: a newer seq is written to disk.
	if outcome := mustRememberRecord(t, reloaded, peer, 6, true); outcome != domain.IdentityRecordMergeReplaced {
		t.Fatalf("outcome = %s, want replaced", outcome)
	}
	if rows := recordRowsOnDisk(t, path); len(rows) != 1 {
		t.Fatalf("disk rows = %+v", rows)
	}
}

// Forgetting a non-contact drops its cached record even though there is
// no contact to remove.
func TestForgetNonContactDropsCachedRecord(t *testing.T) {
	t.Parallel()
	store, _, _ := newSessionRecordTestStore(t)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	mustRememberRecord(t, store, peer, 1, true)

	if removed, err := store.forget(peerID); err != nil || removed {
		t.Fatalf("forget of a non-contact: removed=%v err=%v, want false and nil", removed, err)
	}
	if _, _, ok := store.recordFor(testRecordStoreNetwork, peerID); ok {
		t.Fatal("cached record survived forget")
	}
}

// Migration: a file written by a build that persisted every session peer's
// record loads with its contacts and their records intact, drops the rows
// of non-contacts, and rewrites the file without them.
func TestLoadDropsPersistedSessionPeerRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	// Write the legacy shape through the store itself: a contact with a
	// record, plus a non-contact record injected directly into the
	// persistent set the way the old build stored it.
	store, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}
	contact := mustGenerate(t)
	contactID, _ := domain.ParsePeerIdentity(contact.Address)
	if stored, err := store.remember(trustedContact{Address: contact.Address, PubKey: "pk", Source: "test"}); err != nil || !stored {
		t.Fatalf("remember: stored=%v err=%v", stored, err)
	}
	mustRememberRecord(t, store, contact, 1, true)
	stranger := mustGenerate(t)
	strangerID, _ := domain.ParsePeerIdentity(stranger.Address)
	strangerRecord, strangerBody := issueTestRecord(t, stranger, 7, true)
	store.mu.Lock()
	store.records[trustRecordKey{network: testRecordStoreNetwork.String(), address: stranger.Address}] = trustedIdentityRecord{record: strangerRecord, body: strangerBody, storedAt: time.Now().UTC()}
	store.mu.Unlock()
	if err := store.save(); err != nil {
		t.Fatalf("save legacy shape: %v", err)
	}
	if rows := recordRowsOnDisk(t, path); len(rows) != 2 {
		t.Fatalf("legacy file rows = %d, want 2", len(rows))
	}

	reloaded, err := loadTrustStore(path, trustedContact{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reloaded.isTrustedContact(contactID) {
		t.Fatal("contact lost in migration")
	}
	if _, _, ok := reloaded.recordFor(testRecordStoreNetwork, contactID); !ok {
		t.Fatal("contact's record lost in migration")
	}
	if _, _, ok := reloaded.recordFor(testRecordStoreNetwork, strangerID); ok {
		t.Fatal("non-contact record restored from disk")
	}
	rows := recordRowsOnDisk(t, path)
	if len(rows) != 1 || rows[0].Address != contact.Address {
		t.Fatalf("rows after migration = %+v, want only the contact's", rows)
	}
}

// The node's own record is persistent even on a store whose only contact
// is the self row.
func TestOwnRecordStaysPersistent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "trust.json")
	self := mustGenerate(t)
	store, err := loadTrustStore(path, trustedContact{Address: self.Address, PubKey: "pk", Source: "self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}
	mustRememberRecord(t, store, self, 1, true)
	if rows := recordRowsOnDisk(t, path); len(rows) != 1 {
		t.Fatalf("own record rows on disk = %d, want 1", len(rows))
	}
	if usage := store.sessionRecordUsage(); usage.count != 0 {
		t.Fatalf("own record landed in the session cache: %+v", usage)
	}
}

// The seq gate is what the cache exists to keep. While the node is still
// talking to an identity, neither the TTL nor the budget may drop its
// record: an entry that vanishes takes the floor with it, and the owner's
// own earlier record — a revocation predecessor — would then merge as
// `inserted` and bring a withdrawn box key back.
func TestSeqGateSurvivesExpiryWhileTheSessionIsLive(t *testing.T) {
	t.Parallel()
	store, _, now := newSessionRecordTestStore(t)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	protection := newRecordProtection()
	protection.pin(peer.Address)
	store.setRecordProtection(protection)

	// seq 4 revokes the box key.
	mustRememberRecord(t, store, peer, 4, false)

	*now = now.Add(sessionIdentityRecordTTL + time.Hour)
	if swept := store.sweepSessionRecords(*now); swept != 0 {
		t.Fatalf("swept %d records of a peer we are still talking to, want 0", swept)
	}

	if outcome := mustRememberRecord(t, store, peer, 3, true); outcome != domain.IdentityRecordMergeStale {
		t.Fatalf("replayed seq 3 outcome = %s, want stale", outcome)
	}
	_, body, ok := store.recordFor(testRecordStoreNetwork, peerID)
	if !ok || body.Seq != 4 || body.DM {
		t.Fatalf("stored body = %+v ok=%v, want the seq-4 revocation intact", body, ok)
	}

	// Once the peer is gone, the entry ages out as any other.
	protection.unpin(peer.Address)
	if swept := store.sweepSessionRecords(*now); swept != 1 {
		t.Fatalf("swept %d records after the session went away, want 1", swept)
	}
}

// Budget eviction obeys the same protection, and the unprotected entries
// around it are still evicted — protection must not turn the budget off.
func TestSeqGateSurvivesBudgetEvictionWhileTheSessionIsLive(t *testing.T) {
	t.Parallel()
	store, _, _ := newSessionRecordTestStore(t)
	store.sessionRecords = newSessionRecordCache(4, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)
	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	protection := newRecordProtection()
	protection.pin(peer.Address)
	store.setRecordProtection(protection)

	// The protected record is accepted FIRST, so it sits at the front of
	// the eviction order — the position the budget would take next.
	mustRememberRecord(t, store, peer, 4, false)

	for range 40 {
		mustRememberRecord(t, store, mustGenerate(t), 1, true)
	}

	usage := store.sessionRecordUsage()
	if usage.count > 5 || usage.evicted == 0 {
		t.Fatalf("budget not enforced around the protected entry: %+v", usage)
	}
	if usage.protected != 1 {
		t.Fatalf("protected count = %d, want 1", usage.protected)
	}
	if _, body, ok := store.recordFor(testRecordStoreNetwork, peerID); !ok || body.Seq != 4 {
		t.Fatalf("protected record evicted by the budget: ok=%v body=%+v", ok, body)
	}
	if outcome := mustRememberRecord(t, store, peer, 3, true); outcome != domain.IdentityRecordMergeStale {
		t.Fatalf("replayed seq 3 outcome = %s, want stale", outcome)
	}
}

// The window a periodic snapshot leaves open, driven through the real
// paths: maintenance runs BEFORE the session exists and does not run again.
// A session that comes up after it must protect its own record from the
// moment it exists, or one import into a full cache drops the floor and the
// peer's own earlier record walks a revoked key back in.
func TestNewSessionProtectsItsRecordWithoutWaitingForMaintenance(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	svc.trust.sessionRecords = newSessionRecordCache(2, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)
	svc.trust.setRecordProtection(svc.recordProtection)

	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)

	// 1. The maintenance pass samples the live set — the session does not
	//    exist yet, so nothing about this peer is in it.
	svc.reconcileRecordProtection()

	// 2. The session comes up and the peer pushes its record: seq 4
	//    withdraws its box key.
	svc.onPeerSessionEstablished(peerID, nil)
	record, body := issueTestRecord(t, peer, 4, false)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("import seq 4: %v", err)
	}

	// 3. Other identities fill the cache past its budget. No maintenance
	//    pass runs in between — this is the whole point.
	for range 20 {
		other := mustGenerate(t)
		otherRecord, otherBody := issueTestRecord(t, other, 1, true)
		if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, otherRecord, otherBody); err != nil {
			t.Fatalf("import filler: %v", err)
		}
	}

	if _, storedBody, ok := svc.trust.recordFor(testRecordStoreNetwork, peerID); !ok || storedBody.Seq != 4 {
		t.Fatalf("the live session's record was evicted: ok=%v body=%+v", ok, storedBody)
	}

	// 4. The replay of the peer's own earlier record must still be refused,
	//    and the box key must stay revoked.
	replay, replayBody := issueTestRecord(t, peer, 3, true)
	outcome, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replay, replayBody)
	if err != nil {
		t.Fatalf("import seq 3: %v", err)
	}
	if outcome != domain.IdentityRecordMergeStale {
		t.Fatalf("replayed seq 3 outcome = %s, want stale", outcome)
	}
	if key, ok := svc.knownBoxKey(peer.Address); ok && key != "" {
		t.Fatal("the replay restored the revoked box key")
	}
}

// The same for a lookup: a resolution opened between maintenance passes
// holds the floor its own answer will be checked against.
func TestOpenLookupProtectsItsTargetWithoutWaitingForMaintenance(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	svc.trust.sessionRecords = newSessionRecordCache(2, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)
	svc.trust.setRecordProtection(svc.recordProtection)

	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	svc.reconcileRecordProtection()

	record, body := issueTestRecord(t, peer, 4, false)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("import seq 4: %v", err)
	}
	if _, err := svc.identityResolver.StartResolution(peerID, identityIntentReason{Type: identityIntentReasonUIChat}); err != nil {
		t.Fatalf("start resolution: %v", err)
	}

	for range 20 {
		other := mustGenerate(t)
		otherRecord, otherBody := issueTestRecord(t, other, 1, true)
		if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, otherRecord, otherBody); err != nil {
			t.Fatalf("import filler: %v", err)
		}
	}

	if _, storedBody, ok := svc.trust.recordFor(testRecordStoreNetwork, peerID); !ok || storedBody.Seq != 4 {
		t.Fatalf("the open lookup's floor was evicted: ok=%v body=%+v", ok, storedBody)
	}
}

// The live set is held by the session and lookup paths themselves, and
// released by them: it tracks the working set rather than the history.
func TestRecordProtectionFollowsSessionsAndLookups(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	session := domaintest.ID("protected-session")
	lookup := domaintest.ID("protected-lookup")

	svc.onPeerSessionEstablished(session, nil)
	if !svc.recordProtection.contains(session.String()) {
		t.Error("an established session does not protect its identity")
	}
	if _, err := svc.identityResolver.StartResolution(lookup, identityIntentReason{Type: identityIntentReasonUIChat}); err != nil {
		t.Fatalf("start resolution: %v", err)
	}
	if !svc.recordProtection.contains(lookup.String()) {
		t.Error("an open lookup does not protect its target")
	}

	// A second session for the same identity is a second holder: the first
	// one closing must not release the protection.
	svc.onPeerSessionEstablished(session, nil)
	svc.onPeerSessionClosed(session, nil)
	if !svc.recordProtection.contains(session.String()) {
		t.Error("protection released while another session for the identity is live")
	}
	svc.onPeerSessionClosed(session, nil)
	if svc.recordProtection.contains(session.String()) {
		t.Error("protection outlived the last session")
	}

	svc.identityResolver.mu.Lock()
	svc.identityResolver.finishLocked(lookup, domain.IdentityResolutionSucceeded)
	svc.identityResolver.mu.Unlock()
	if svc.recordProtection.contains(lookup.String()) {
		t.Error("a finished lookup still protects its target")
	}
}

// The maintenance pass is the self-heal: a pin whose release was missed,
// or a release that ran twice, survives at most one pass.
func TestRecordProtectionReconcileRepairsDrift(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	leaked := domaintest.ID("leaked-pin")
	live := domaintest.ID("live-session")

	svc.recordProtection.pin(leaked.String())
	svc.peerMu.Lock()
	svc.identitySessions[live] = 1
	svc.peerMu.Unlock()

	svc.reconcileRecordProtection()

	if svc.recordProtection.contains(leaked.String()) {
		t.Error("a pin with nothing behind it survived the reconcile")
	}
	if !svc.recordProtection.contains(live.String()) {
		t.Error("a live session missing its pin was not repaired")
	}
}

// The eviction must decide and delete as one step against a pin. A
// candidate chosen while unprotected and deleted after the pin landed is
// the same lost floor as never checking at all, so the primitive the cache
// evicts through refuses the drop for an address pinned at that instant and
// performs it for one that is not.
func TestDropIfUnprotectedIsAtomicAgainstPin(t *testing.T) {
	t.Parallel()
	protection := newRecordProtection()
	protection.pin("pinned")

	dropped := 0
	if protection.dropIfUnprotected("pinned", func() { dropped++ }) {
		t.Error("dropIfUnprotected reported a drop for a pinned address")
	}
	if dropped != 0 {
		t.Error("the drop ran for a pinned address")
	}
	if !protection.dropIfUnprotected("free", func() { dropped++ }) || dropped != 1 {
		t.Errorf("dropIfUnprotected on a free address: dropped=%d", dropped)
	}

	// A pin taken from inside the drop of ANOTHER address cannot interleave
	// with it: the mutex the drop runs under is the one the pin needs. What
	// this asserts is the ordering the cache depends on — the pin observes
	// the removal as already complete.
	protection.unpin("pinned")
	if !protection.dropIfUnprotected("free-2", func() {
		dropped++
	}) {
		t.Error("second free drop refused")
	}
	if dropped != 2 {
		t.Errorf("dropped = %d, want 2", dropped)
	}
}

// A session that comes up WHILE the maintenance pass is gathering its
// counts must not be erased by that pass: the gather began before the
// session existed, so committing it would hand the cache a live session it
// believes is not there — and the next import takes the floor with it.
func TestReconcileRefusesToCommitAStaleGather(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	peer := domaintest.ID("late-session")

	// The pass reads the generation and gathers — nothing is live yet.
	gen := svc.recordProtection.generation()
	live := svc.liveRecordProtectionCounts()

	// The session opens in the gap, and pins.
	svc.onPeerSessionEstablished(peer, nil)

	if svc.recordProtection.reconcile(gen, live) {
		t.Fatal("a gather that predates the session was committed over it")
	}
	if !svc.recordProtection.contains(peer.String()) {
		t.Fatal("the live session lost its protection to a stale reconcile")
	}

	// The next pass sees the session and keeps it.
	svc.reconcileRecordProtection()
	if !svc.recordProtection.contains(peer.String()) {
		t.Fatal("the repaired pass dropped a live session")
	}
}

// End to end, through the real paths and with no hand-placed protection:
// the maintenance gather straddles the session, and the peer's own earlier
// record must still be refused afterwards.
func TestReconcileRaceDoesNotCostTheSeqFloor(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	svc.trust.sessionRecords = newSessionRecordCache(2, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)
	svc.trust.setRecordProtection(svc.recordProtection)

	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)

	gen := svc.recordProtection.generation()
	live := svc.liveRecordProtectionCounts()

	svc.onPeerSessionEstablished(peerID, nil)
	record, body := issueTestRecord(t, peer, 4, false)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("import seq 4: %v", err)
	}

	// The maintenance pass lands with counts gathered before the session.
	svc.recordProtection.reconcile(gen, live)

	for range 20 {
		other := mustGenerate(t)
		otherRecord, otherBody := issueTestRecord(t, other, 1, true)
		if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, otherRecord, otherBody); err != nil {
			t.Fatalf("import filler: %v", err)
		}
	}

	replay, replayBody := issueTestRecord(t, peer, 3, true)
	outcome, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, replay, replayBody)
	if err != nil {
		t.Fatalf("import seq 3: %v", err)
	}
	if outcome != domain.IdentityRecordMergeStale {
		t.Fatalf("replayed seq 3 outcome = %s, want stale", outcome)
	}
	if _, storedBody, ok := svc.trust.recordFor(testRecordStoreNetwork, peerID); !ok || storedBody.Seq != 4 {
		t.Fatalf("floor lost to the stale reconcile: ok=%v body=%+v", ok, storedBody)
	}
}

// Under concurrency the invariant is the one the whole design exists for:
// once an address is pinned, a record that was present stays present until
// the pin goes. Imports hammer the budget from one goroutine while another
// pins, observes and unpins.
func TestProtectedRecordSurvivesConcurrentEviction(t *testing.T) {
	t.Parallel()
	store, _, _ := newSessionRecordTestStore(t)
	store.sessionRecords = newSessionRecordCache(4, maxSessionIdentityRecordBytes, sessionIdentityRecordTTL)
	protection := newRecordProtection()
	store.setRecordProtection(protection)

	peer := mustGenerate(t)
	peerID, _ := domain.ParsePeerIdentity(peer.Address)
	record, body := issueTestRecord(t, peer, 4, false)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 300 {
			other := mustGenerate(t)
			otherRecord, otherBody := issueTestRecord(t, other, 1, true)
			if _, err := store.rememberRecord(testRecordStoreNetwork, otherRecord, otherBody); err != nil {
				return
			}
		}
	}()

	for range 300 {
		// Re-seat the record, then pin: from the pin onwards the entry may
		// not be evicted, whatever the churn does to the rest of the cache.
		if _, err := store.rememberRecord(testRecordStoreNetwork, record, body); err != nil {
			t.Fatalf("rememberRecord: %v", err)
		}
		protection.pin(peer.Address)
		present := func() bool {
			_, _, ok := store.recordFor(testRecordStoreNetwork, peerID)
			return ok
		}
		if present() {
			for range 5 {
				if !present() {
					protection.unpin(peer.Address)
					t.Fatal("a pinned record was evicted while its session was live")
				}
			}
		}
		protection.unpin(peer.Address)
	}
	<-done
}

// The atomicity above cannot be proved by a concurrent test — the losing
// interleaving is a few instructions wide and the scheduler will not
// reliably land in it — so it is pinned where it is decided instead: every
// removal the TTL and the budget perform must go through the protection's
// own critical section, and neither path may make the decision for itself
// from a lock-free read. A future edit that puts `contains` back into an
// eviction path fails here rather than in production.
func TestEvictionPathsDecideInsideTheProtection(t *testing.T) {
	t.Parallel()

	if calls := callsInsideFunctionBody(t, "trust_session_records.go", "dropLocked"); !slices.Contains(calls, "dropIfUnprotected") {
		t.Errorf("dropLocked does not delegate to dropIfUnprotected; calls: %v", calls)
	}
	for _, path := range []string{"sweepLocked", "evictOneLocked"} {
		calls := callsInsideFunctionBody(t, "trust_session_records.go", path)
		if !slices.Contains(calls, "dropLocked") {
			t.Errorf("%s does not remove through dropLocked; calls: %v", path, calls)
		}
		if slices.Contains(calls, "removeLocked") {
			t.Errorf("%s removes an entry directly: the check and the removal must be one step", path)
		}
		if slices.Contains(calls, "contains") {
			t.Errorf("%s decides from a lock-free read; a pin can land between that answer and the removal", path)
		}
	}
}
