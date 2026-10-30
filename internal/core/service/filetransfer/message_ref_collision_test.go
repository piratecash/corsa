package filetransfer

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
)

func refCollisionManager(t *testing.T) (*Manager, domain.PeerIdentity, domain.PeerIdentity, domain.PeerIdentity) {
	t.Helper()
	self, a, b := domaintest.ID("file-ref-self"), domaintest.ID("file-ref-a"), domaintest.ID("file-ref-b")
	m := NewFileTransferManager(Config{DownloadDir: t.TempDir(), LocalID: &identity.Identity{Address: self.String()}})
	return m, self, a, b
}

// This uses the legacy registration surface, whose sender argument already
// carries enough context to keep two same-ID incoming announcements apart.
func TestFileMessageIDCollisionRegistersBothPeers(t *testing.T) {
	m, _, a, b := refCollisionManager(t)
	for _, peer := range []domain.PeerIdentity{a, b} {
		if err := m.RegisterFileReceive("shared", strings.Repeat("a", 64), "image.png", "image/png", 8, peer); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(m.AllTransfersSnapshot()); got != 2 {
		t.Fatalf("same wire ID hid a peer's announce: got %d transfers", got)
	}
	if err := m.StartDownload("shared"); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("ID-only start: %v", err)
	}
	for _, row := range m.AllTransfersSnapshot() {
		if row.State != "available" {
			t.Fatalf("ambiguous start mutated %v", row)
		}
	}
	if _, _, _, found := m.ReceiverProgress("shared"); found {
		t.Fatal("ambiguous progress selected one peer")
	}
	m.CleanupTransferByMessageID("shared")
	if got := len(m.AllTransfersSnapshot()); got != 2 {
		t.Fatalf("ID-only cleanup erased %d colliding transfers", 2-got)
	}
}

func TestFileMessageRefCleanupPreservesOtherPeerAndDirection(t *testing.T) {
	m, _, a, b := refCollisionManager(t)
	inA, inB, outA := m.receiverRef("shared", a), m.receiverRef("shared", b), m.senderRef("shared", a)
	for _, peer := range []domain.PeerIdentity{a, b} {
		if err := m.RegisterFileReceive("shared", strings.Repeat("a", 64), "image.png", "image/png", 8, peer); err != nil {
			t.Fatal(err)
		}
	}
	m.senderMaps[outA] = &senderFileMapping{Ref: outA, FileID: "shared", Recipient: a, State: senderTombstone}
	m.CleanupTransferByMessageRef(inA)
	if _, ok := m.receiverMaps[inA]; ok {
		t.Fatal("selected receiver remains")
	}
	if _, ok := m.receiverMaps[inB]; !ok {
		t.Fatal("other peer's receiver was erased")
	}
	if _, ok := m.senderMaps[outA]; !ok {
		t.Fatal("opposite direction was erased")
	}
	if err := m.DeleteLocalCopyRef(outA); !errors.Is(err, ErrOutgoingCopy) {
		t.Fatalf("outgoing exact copy deletion: %v", err)
	}
}

func TestFileMessageRefWireCommandsKeepOriginalID(t *testing.T) {
	m, _, a, b := refCollisionManager(t)
	var peers []domain.PeerIdentity
	m.sendCommand = func(peer domain.PeerIdentity, payload domain.FileCommandPayload) error {
		var req domain.ChunkRequestPayload
		if err := json.Unmarshal(payload.Data, &req); err != nil {
			return err
		}
		if req.FileID != "shared" {
			t.Fatalf("local reference leaked to wire: %q", req.FileID)
		}
		peers = append(peers, peer)
		return nil
	}
	for _, peer := range []domain.PeerIdentity{a, b} {
		if err := m.RegisterFileReceive("shared", strings.Repeat("a", 64), "image.png", "image/png", 8, peer); err != nil {
			t.Fatal(err)
		}
		if err := m.StartDownloadRef(m.receiverRef("shared", peer)); err != nil {
			t.Fatal(err)
		}
	}
	if len(peers) != 2 || peers[0] != a || peers[1] != b {
		t.Fatalf("wrong logical destinations: %v", peers)
	}
	if m.partialPath(m.receiverRef("shared", a)) == m.partialPath(m.receiverRef("shared", b)) {
		t.Fatal("same-ID peers share partial path")
	}
}

func TestFileMessageRefMigratesLegacyPartialWithoutMovingBytes(t *testing.T) {
	dir := t.TempDir()
	self, a, b := domaintest.ID("legacy-self"), domaintest.ID("legacy-a"), domaintest.ID("legacy-b")
	oldPath := partialDownloadPath(dir, "shared")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldPath, []byte("ab"), 0600); err != nil {
		t.Fatal(err)
	}
	mappingPath := filepath.Join(dir, "transfers.json")
	pf := persistedTransferFile{Version: 1, Cleanups: []persistedCleanup{{FileID: "shared", Partials: []string{oldPath}}}, Transfers: []persistedTransferEntry{{FileID: "shared", FileHash: strings.Repeat("a", 64), FileName: "image.png", FileSize: 8, ContentType: "image/png", Peer: a, Role: "receiver", State: string(receiverWaitingRoute), NextOffset: 2, BytesReceived: 2}}}
	if err := atomicWriteJSON(mappingPath, pf); err != nil {
		t.Fatal(err)
	}
	cfg := Config{DownloadDir: dir, MappingsPath: mappingPath, LocalID: &identity.Identity{Address: self.String()}}
	m := NewFileTransferManager(cfg)
	if err := m.Err(); err != nil {
		t.Fatal(err)
	}
	refA := m.receiverRef("shared", a)
	if got := m.receiverMaps[refA]; got.PartialPath != canonicalPartialPath(oldPath) || got.NextOffset != 2 {
		t.Fatalf("legacy ownership/progress lost: %+v", got)
	}
	if err := m.RegisterFileReceive("shared", strings.Repeat("b", 64), "other.png", "image/png", 8, b); err != nil {
		t.Fatal(err)
	}
	if m.partialPath(m.receiverRef("shared", b)) == oldPath {
		t.Fatal("new peer claimed legacy partial")
	}
	after := NewFileTransferManager(cfg)
	if err := after.Err(); err != nil {
		t.Fatal(err)
	}
	if len(after.receiverMaps) != 2 || after.receiverMaps[refA].PartialPath != canonicalPartialPath(oldPath) {
		t.Fatal("version 2 roundtrip lost owners")
	}
	if data, err := os.ReadFile(oldPath); err != nil || string(data) != "ab" {
		t.Fatalf("migration rewrote partial bytes: %q %v", data, err)
	}
	legacyErasure := domain.MessageRef{Topic: "file-cleanup-legacy", ID: "shared"}
	cleanup, owed := after.pendingCleanups[legacyErasure]
	if !owed || len(cleanup.Partials) != 1 || cleanup.Partials[0] != canonicalPartialPath(oldPath) {
		t.Fatalf("legacy cleanup path ownership was not imported: owed=%v cleanup=%+v", owed, cleanup)
	}
	if remaining := after.erasePartial(legacyErasure, cleanup.Partials[0]); remaining != cleanup.Partials[0] {
		t.Fatal("legacy erasure forgot a currently owned partial")
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("legacy cleanup erased live partial: %v", err)
	}
	if data, err := os.ReadFile(oldPath); err != nil || string(data) != "ab" {
		t.Fatalf("legacy cleanup changed live partial bytes: %q %v", data, err)
	}
}

func TestFileMessageRefRefusesUnknownVersionAndDuplicateOwners(t *testing.T) {
	for _, kind := range []string{"unknown", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "transfers.json")
			self, a := domaintest.ID("refuse-self"), domaintest.ID("refuse-a")
			row := persistedTransferEntry{FileID: "shared", FileHash: strings.Repeat("a", 64), FileName: "image.png", FileSize: 8, Peer: a, Role: "receiver", State: string(receiverAvailable)}
			pf := persistedTransferFile{Version: 1, Transfers: []persistedTransferEntry{row, row}}
			if kind == "unknown" {
				pf.Version = 999
				pf.Transfers = nil
			}
			if err := atomicWriteJSON(path, pf); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			m := NewFileTransferManager(Config{DownloadDir: dir, MappingsPath: path, LocalID: &identity.Identity{Address: self.String()}})
			if m.Err() == nil {
				t.Fatal("invalid persistent owners were accepted")
			}
			if len(m.senderMaps)+len(m.receiverMaps) != 0 {
				t.Fatal("refusal restored a partial map")
			}
			if err := m.RegisterFileReceive("new", strings.Repeat("a", 64), "new.png", "image/png", 8, a); err == nil {
				t.Fatal("disabled manager accepted write")
			}
			m.Stop()
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("refused mapping file was overwritten")
			}
		})
	}
}

func TestFileMessageRefSelfTransferPersistsBothRoles(t *testing.T) {
	dir := t.TempDir()
	self := domaintest.ID("self-role-owner")
	cfg := Config{DownloadDir: dir, MappingsPath: filepath.Join(dir, "transfers.json"), LocalID: &identity.Identity{Address: self.String()}}
	m := NewFileTransferManager(cfg)
	if err := m.RegisterFileReceive("self", strings.Repeat("a", 64), "self.png", "image/png", 8, self); err != nil {
		t.Fatal(err)
	}
	ref := m.senderRef("self", self)
	m.senderMaps[ref] = &senderFileMapping{Ref: ref, FileID: "self", FileHash: strings.Repeat("a", 64), FileName: "self.png", FileSize: 8, Recipient: self, State: senderTombstone}
	if err := m.DeleteLocalCopyRef(ref); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("self roles must not guess receiver: %v", err)
	}
	m.mu.Lock()
	err := m.saveMappingsLockedErr()
	m.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewFileTransferManager(cfg)
	if err := restored.Err(); err != nil {
		t.Fatalf("valid self roles rejected on reload: %v", err)
	}
	if len(restored.senderMaps) != 1 || len(restored.receiverMaps) != 1 {
		t.Fatal("self role reload collapsed a role")
	}
	if got := restored.receiverMaps[ref].PartialPath; filepath.Dir(got) != canonicalPartialPath(filepath.Join(dir, "partial", "by-ref")) {
		t.Fatalf("new reference did not use distinct path namespace: %s", got)
	}
}
