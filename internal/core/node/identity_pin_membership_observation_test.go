package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

func pinMembershipTestRequest(c *k3ActualClock, baseline, history int) pinMembershipRequest {
	return pinMembershipRequest{Instance: [16]byte{211}, BaselineCapacity: baseline, HistoryCapacity: history, elapsed: c.read}
}
func pinMembershipTestOpen(path string, r *pinMembershipRequest, p findnodebridge.Publisher) *secureSessionStore {
	return openSecureSessionStore(secureSessionStoreOptions{path: path, clock: newStoreClock().trusted(), logger: zerolog.Nop(), audit: secretfile.AuditLocation, bridgePublisher: p, pinMembership: r})
}
func pinMembershipTestLabels(t *testing.T, v pinMembershipSnapshot) {
	t.Helper()
	if v.Population != "RawPinMembership" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("raw membership acquired qualification", v)
	}
}
func pinMembershipTestSame(t *testing.T, a, b pinMembershipSnapshot) {
	t.Helper()
	pinMembershipTestLabels(t, b)
	if a.Instance != b.Instance || a.Baseline != b.Baseline || a.Cut != b.Cut || a.Sequence != b.Sequence || a.CountAtCut != b.CountAtCut || !reflect.DeepEqual(a.BaselineIdentities, b.BaselineIdentities) || !reflect.DeepEqual(a.Transitions, b.Transitions) {
		t.Fatal("last coherent membership prefix advanced across gap", a, b)
	}
}
func pinMembershipTestSet(ids []domain.PeerIdentity) map[domain.PeerIdentity]bool {
	out := make(map[domain.PeerIdentity]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}
func pinMembershipTestSeedDisk(t *testing.T, path string, ids ...domain.PeerIdentity) {
	t.Helper()
	s := loadSecureSessionStore(path, newStoreClock().trusted())
	for _, id := range ids {
		if err := s.noteProvenInbound(id); err != nil {
			t.Fatal(err)
		}
	}
}
func pinMembershipTestPanic(t *testing.T, fn func()) {
	t.Helper()
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		fn()
	}()
	if !panicked {
		t.Fatal("observation swallowed natural panic")
	}
}

func TestRawPinMembershipActualStore(t *testing.T) {
	t.Run("loaded-complete-baseline-copy-before-audit-capacity-and-default", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pins.json")
		ids := []domain.PeerIdentity{{1}, {2}}
		pinMembershipTestSeedDisk(t, path, ids...)
		clock := newK3ActualClock()
		r := pinMembershipTestRequest(clock, 2, 8)
		original := r.Instance
		s := openSecureSessionStore(secureSessionStoreOptions{path: path, clock: newStoreClock().trusted(), logger: zerolog.Nop(), pinMembership: &r, audit: func(p string) error {
			r.Instance = [16]byte{}
			r.BaselineCapacity = 0
			r.HistoryCapacity = 0
			return secretfile.AuditLocation(p)
		}})
		v := s.pinMembershipSnapshot()
		pinMembershipTestLabels(t, v)
		if v.Status != "PAST" || v.Instance != original || v.CountAtCut != 2 || v.Sequence != 0 || len(v.Transitions) != 0 || !reflect.DeepEqual(pinMembershipTestSet(v.BaselineIdentities), pinMembershipTestSet(ids)) || v.PointOnly.MemoryOnly || !v.PointOnly.Readable || v.Baseline.Before < 0 || v.Baseline.After < v.Baseline.Before {
			t.Fatal("loaded pins were truncated, replayed or backdated", v)
		}
		c := newK3ActualClock()
		small := pinMembershipTestRequest(c, 1, 8)
		limited := pinMembershipTestOpen(path, &small, findnodebridge.Publisher{})
		unknown := limited.pinMembershipSnapshot()
		if unknown.Status != "UNKNOWN" || unknown.Reason != "initial-baseline-capacity" || len(unknown.BaselineIdentities) != 0 || unknown.CountAtCut != 0 || c.count() != 0 || !limited.identityPinned(ids[0]) || !limited.identityPinned(ids[1]) {
			t.Fatal("baseline overflow truncated or changed original store", unknown)
		}
		for _, bad := range []pinMembershipRequest{{Instance: [16]byte{1}, BaselineCapacity: 0, HistoryCapacity: 1}, {Instance: [16]byte{1}, BaselineCapacity: maxPinnedIdentities + 1, HistoryCapacity: 1}, {Instance: [16]byte{1}, BaselineCapacity: 1, HistoryCapacity: pinMembershipMaxHistory + 1}, {BaselineCapacity: 1, HistoryCapacity: 1}} {
			calls := 0
			bad.elapsed = func() (time.Duration, error) { calls++; return 1, nil }
			invalid := pinMembershipTestOpen("", &bad, findnodebridge.Publisher{})
			u := invalid.pinMembershipSnapshot()
			if u.Status != "UNKNOWN" || len(u.BaselineIdentities) != 0 || invalid.pinMembership.history != nil || calls != 0 {
				t.Fatal("invalid request performed source work", u)
			}
		}
		ordinary := pinMembershipTestOpen("", nil, findnodebridge.Publisher{})
		if ordinary.pinMembership != nil || ordinary.pinMembershipSnapshot().Status != "OFF" {
			t.Fatal("ordinary store attached observer")
		}
		if err := ordinary.noteProvenInbound(domain.PeerIdentity{3}); err != nil || !ordinary.identityPinned(domain.PeerIdentity{3}) {
			t.Fatal("nil observer skipped original pin", err)
		}
	})
	t.Run("failed-write-is-raw-memory-edge-duplicate-and-metadata-not-new-membership", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pins.json")
		c := newK3ActualClock()
		r := pinMembershipTestRequest(c, 8, 16)
		s := pinMembershipTestOpen(path, &r, findnodebridge.Publisher{})
		id := domain.PeerIdentity{4}
		diskFull := errors.New("existing writeFile returned failure")
		s.writeFile = func([]byte) error { return diskFull }
		if err := s.noteProvenInbound(id); !errors.Is(err, diskFull) || !s.identityPinned(id) {
			t.Fatal("failed persistence erased actual memory pin", err)
		}
		v := s.pinMembershipSnapshot()
		if v.Status != "PAST" || v.CountAtCut != 1 || v.Sequence != 1 || len(v.Transitions) != 1 || !v.Transitions[0].Added || v.Transitions[0].Identity != id {
			t.Fatal("raw-memory add was mistaken for durable failure", v)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("failed first writer unexpectedly created durable image", err)
		}
		s.writeFile = s.writeToDisk
		if err := s.noteProvenInbound(id); err != nil {
			t.Fatal(err)
		}
		if !loadSecureSessionStore(path, newStoreClock().trusted()).identityPinned(id) || s.pinMembershipSnapshot().Sequence != 1 {
			t.Fatal("successful duplicate retry lost disk pin or minted a second edge")
		}
		s.mu.Lock()
		binding := s.pinEntryBindingLocked(id)
		s.mu.Unlock()
		if _, err := s.stageSelectedPin(t.Context(), binding, selectedAllocation(t)); err != nil {
			t.Fatal(err)
		}
		after := s.pinMembershipSnapshot()
		if after.Sequence != 1 || after.CountAtCut != 1 {
			t.Fatal("metadata replacement manufactured membership gap", after)
		}
		s.mu.Lock()
		s.deletePinEntryLocked(domain.PeerIdentity{99})
		s.mu.Unlock()
		if s.pinMembershipSnapshot().Sequence != 1 {
			t.Fatal("absent deletion manufactured membership")
		}
	})
	t.Run("actual-removal-receipt-disk-memory-interval-and-recovery-add", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pins.json")
		id := domain.PeerIdentity{21}
		pinMembershipTestSeedDisk(t, path, id)
		c := newK3ActualClock()
		r := pinMembershipTestRequest(c, 8, 16)
		s := pinMembershipTestOpen(path, &r, findnodebridge.Publisher{})
		before := s.pinMembershipSnapshot()
		preview := pinRemovalPreview(t, s, id)
		if before.Sequence != 0 || before.CountAtCut != 1 || len(before.BaselineIdentities) != 1 || before.BaselineIdentities[0] != id {
			t.Fatal("recovery fixture lacks actual loaded baseline", before)
		}
		s.mu.Lock()
		entry := s.identities[id]
		s.mu.Unlock()
		binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{22}, Store: "membership-store"}, RuntimeEpoch: "membership-fixture", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
		files, err := s.openIdentityPinFiles()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := files.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := files.Preflight(t.Context()); err != nil {
			t.Fatal("actual strict filesystem prerequisite unavailable", err)
		}
		permit := domain.IdentityPinRemovalPermit{OperationID: "membership-removal", Binding: binding, Generation: 2}
		fence := NewMockIdentityPinRemovalFence(t)
		fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
		fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
			if receipt.Kind != domain.IdentityPinReceiptCleanCommitted || !s.identityPinned(id) {
				t.Error("local pin removed before external receipt")
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var disk secureSessionFile
			if err := json.Unmarshal(raw, &disk); err != nil {
				return err
			}
			if _, present := disk.Identities[id]; present {
				t.Error("durable image still contains locally retained pin at receipt")
			}
			during := s.pinMembershipSnapshot()
			if during.Sequence != before.Sequence || during.CountAtCut != 1 {
				t.Error("file visibility was relabelled as memory deletion", during)
			}
			return nil
		}).Once()
		result, err := s.removeIdentityPin(t.Context(), binding, preview.Token, fence, files)
		if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
			t.Fatal(result, err)
		}
		removed := s.pinMembershipSnapshot()
		if removed.Sequence != 1 || removed.CountAtCut != 0 || removed.Transitions[0].Added || removed.Transitions[0].Identity != id {
			t.Fatal("original local deletion missing", removed)
		}
		// A real guard plus a main image without the ID reserves capacity but is
		// not an enumerable member. Reopen through the original guard inspector.
		guard, err := json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit})
		if err != nil {
			t.Fatal(err)
		}
		if err := confirmedIdentityPinWrite(files.WriteGuard(t.Context(), guard)); err != nil {
			t.Fatal(err)
		}
		pendingClock := newK3ActualClock()
		pendingReq := pinMembershipTestRequest(pendingClock, 8, 8)
		pending := pinMembershipTestOpen(path, &pendingReq, findnodebridge.Publisher{})
		point := pending.pinMembershipSnapshot()
		if point.Status == "UNKNOWN" || point.CountAtCut != 0 || len(point.BaselineIdentities) != 0 || !point.PointOnly.RecoveryPending || point.PointOnly.CapacityReservationCount != 1 {
			t.Fatal("recovery reservation became member or erased raw baseline", point)
		}
		fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
			if receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
				t.Error("wrong recovery receipt", receipt)
			}
			return nil
		}).Once()
		receipt, err := s.recoverIdentityPinProtection(t.Context(), permit, fence, files)
		if err != nil || receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
			t.Fatal(receipt, err)
		}
		restored := s.pinMembershipSnapshot()
		if restored.Sequence != 2 || restored.CountAtCut != 1 || !restored.Transitions[1].Added || restored.Transitions[1].Identity != id || !s.identityPinned(id) {
			t.Fatal("between-read remove/recovery-add lost original edge", restored)
		}
	})
	t.Run("real-publication-before-after-outside-W-and-joined-owner-snapshot", func(t *testing.T) {
		d, err := findnodebridge.NewFixtureDomain(make(chan struct{}), new(atomic.Int64))
		if err != nil {
			t.Fatal(err)
		}
		c := newK3ActualClock()
		r := pinMembershipTestRequest(c, 8, 16)
		s := pinMembershipTestOpen("", &r, d.Publisher())
		if err := d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
		var probeMu sync.Mutex
		var failures []error
		probes := 0
		c.setHook(func() {
			stamp, err := d.Observer().BeginCapture()
			if err == nil {
				read, readErr := d.Consumer().TryReadClock(stamp)
				err = readErr
				if err == nil {
					read.Release()
				}
			}
			probeMu.Lock()
			defer probeMu.Unlock()
			probes++
			if err != nil {
				failures = append(failures, err)
			}
		})
		if err := s.noteProvenInbound(domain.PeerIdentity{31}); err != nil {
			t.Fatal(err)
		}
		c.setHook(nil)
		if probes != 2 || len(failures) != 0 {
			t.Fatal("observer Before/After entered original publication W", probes, failures)
		}
		entered, release, writerDone, readerDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		t.Cleanup(unblock)
		c.setHook(func() { once.Do(func() { close(entered); <-release }) })
		deadline := time.Now().Add(2 * time.Second)
		var writeErr error
		go func() { defer close(writerDone); writeErr = s.noteProvenInbound(domain.PeerIdentity{32}) }()
		t.Cleanup(func() { unblock(); k3Wait(t, writerDone, time.Now().Add(2*time.Second)) })
		k3Wait(t, entered, deadline)
		if s.mu.TryLock() {
			s.mu.Unlock()
			t.Fatal("source clock escaped original store owner lock")
		}
		var v pinMembershipSnapshot
		go func() { defer close(readerDone); v = s.pinMembershipSnapshot() }()
		t.Cleanup(func() {
			unblock()
			end := time.Now().Add(2 * time.Second)
			k3Wait(t, writerDone, end)
			k3Wait(t, readerDone, end)
		})
		select {
		case <-readerDone:
			t.Fatal("snapshot crossed paused original mutation")
		default:
		}
		unblock()
		k3Wait(t, writerDone, deadline)
		k3Wait(t, readerDone, deadline)
		c.setHook(nil)
		if writeErr != nil || v.Sequence != 2 || v.CountAtCut != 2 {
			t.Fatal("joined reader saw partial owner mutation", v, writeErr)
		}
	})
	t.Run("history-clock-overflow-and-panic-freeze-last-coherent-prefix", func(t *testing.T) {
		for _, mode := range []string{"before-error", "after-error", "regression", "history", "sequence", "panic-after", "panic-snapshot"} {
			c := newK3ActualClock()
			capacity := 8
			if mode == "history" {
				capacity = 1
			}
			r := pinMembershipTestRequest(c, 8, capacity)
			s := pinMembershipTestOpen("", &r, findnodebridge.Publisher{})
			id := domain.PeerIdentity{41}
			if err := s.noteProvenInbound(id); err != nil {
				t.Fatal(err)
			}
			if mode == "sequence" {
				s.mu.Lock()
				s.pinMembership.sequence = ^uint64(0)
				s.mu.Unlock()
			}
			before := s.pinMembershipSnapshot()
			switch mode {
			case "before-error":
				c.configure(time.Hour, 0, errors.New("Before unavailable"))
			case "after-error":
				c.setHook(func() { c.mu.Lock(); c.err = errors.New("After unavailable"); c.hook = nil; c.mu.Unlock() })
			case "regression":
				c.configure(0, 0, nil)
			case "panic-after":
				c.setHook(func() { c.setHook(func() { panic("natural After callback panic") }) })
			case "panic-snapshot":
				c.setHook(func() { panic("natural Snapshot callback panic") })
			}
			expectedMapCount := 2
			if mode == "panic-snapshot" {
				pinMembershipTestPanic(t, func() { s.pinMembershipSnapshot() })
				expectedMapCount = 1
				if !s.mu.TryLock() {
					t.Fatal("Snapshot panic retained original mutex")
				}
				s.mu.Unlock()
			} else if mode == "panic-after" {
				// A negative raw-leaf call under its real owner lock: reuse an actual
				// entry solely to exercise unwind, not to mint a positive session proof.
				pinMembershipTestPanic(t, func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					s.putPinEntryLocked(domain.PeerIdentity{42}, s.identities[id])
				})
			} else if err := s.noteProvenInbound(domain.PeerIdentity{42}); err != nil {
				t.Fatal("observation gap became admission error", mode, err)
			}
			after := s.pinMembershipSnapshot()
			if after.Status != "UNKNOWN" || after.Reason == "" {
				t.Fatal("observation gap certified membership", mode, after)
			}
			pinMembershipTestSame(t, before, after)
			s.mu.Lock()
			rawCount := len(s.identities)
			noClock := s.pinMembership.clock == nil
			s.mu.Unlock()
			if rawCount != expectedMapCount || !noClock {
				t.Fatal("gap changed raw map mutation or retained callback", mode, rawCount)
			}
			calls := c.count()
			c.setHook(nil)
			c.configure(2*time.Hour, 0, nil)
			pinMembershipTestSame(t, after, s.pinMembershipSnapshot())
			if c.count() != calls {
				t.Fatal("faulted source polled clock", mode)
			}
		}
	})
	t.Run("snapshot-value-copies-and-unreadable-failclosed-is-not-enumerable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pins.json")
		ids := []domain.PeerIdentity{{51}, {52}}
		pinMembershipTestSeedDisk(t, path, ids...)
		c := newK3ActualClock()
		r := pinMembershipTestRequest(c, 8, 16)
		s := pinMembershipTestOpen(path, &r, findnodebridge.Publisher{})
		old := s.pinMembershipSnapshot()
		old.BaselineIdentities[0] = domain.PeerIdentity{99}
		if err := s.noteProvenInbound(domain.PeerIdentity{53}); err != nil {
			t.Fatal(err)
		}
		one := s.pinMembershipSnapshot()
		if !reflect.DeepEqual(pinMembershipTestSet(one.BaselineIdentities), pinMembershipTestSet(ids)) || len(old.Transitions) != 0 {
			t.Fatal("baseline alias or later append changed returned prefix")
		}
		one.Transitions[0].Identity = domain.PeerIdentity{99}
		if err := s.noteProvenInbound(domain.PeerIdentity{54}); err != nil {
			t.Fatal(err)
		}
		two := s.pinMembershipSnapshot()
		if len(one.Transitions) != 1 || two.Transitions[0].Identity != (domain.PeerIdentity{53}) || two.Sequence != 2 {
			t.Fatal("transition alias or append overwrote immutable prefix", two)
		}
		unreadablePath := filepath.Join(t.TempDir(), "pins.json")
		if err := os.WriteFile(unreadablePath, []byte("not json"), 0600); err != nil {
			t.Fatal(err)
		}
		badClock := newK3ActualClock()
		badReq := pinMembershipTestRequest(badClock, 8, 8)
		bad := pinMembershipTestOpen(unreadablePath, &badReq, findnodebridge.Publisher{})
		u := bad.pinMembershipSnapshot()
		pinMembershipTestLabels(t, u)
		if u.Status != "UNKNOWN" || len(u.BaselineIdentities) != 0 || u.CountAtCut != 0 || u.PointOnly.Readable || u.PointOnly.InitialStoreRefusal == "" || badClock.count() != 0 || !bad.identityPinned(domain.PeerIdentity{98}) {
			t.Fatal("failclosed predicate became an enumerated set", u)
		}
		_ = bad.noteProvenInbound(domain.PeerIdentity{98})
		raw, err := os.ReadFile(unreadablePath)
		if err != nil || string(raw) != "not json" {
			t.Fatal("observer changed original unreadable-store preservation", err)
		}
		var absent *secureSessionStore
		missing := absent.pinMembershipSnapshot()
		pinMembershipTestLabels(t, missing)
		if missing.Status != "UNKNOWN" || missing.Reason == "" {
			t.Fatal("nil store certified empty baseline")
		}
	})
}
