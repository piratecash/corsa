package filetransfer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrSenderMappingPersist = errors.New("sender mapping committed but persistence failed")

// Err reports a startup refusal without replacing an unreadable mapping file.
func (m *Manager) Err() error { return m.startupErr }

func (m *Manager) ownerAddress() string {
	if m.localID == nil {
		return ""
	}
	return m.localID.Address
}

func (m *Manager) senderRef(id domain.FileID, peer domain.PeerIdentity) domain.MessageRef {
	return domain.MessageRef{Topic: "dm", ID: string(id), Sender: m.ownerAddress(), Recipient: peer.String()}
}
func (m *Manager) receiverRef(id domain.FileID, peer domain.PeerIdentity) domain.MessageRef {
	return domain.MessageRef{Topic: "dm", ID: string(id), Sender: peer.String(), Recipient: m.ownerAddress()}
}
func (m *Manager) validateRef(ref domain.MessageRef, sender bool) error {
	if m.startupErr != nil {
		return m.startupErr
	}
	if m.ownerAddress() == "" || ref.Topic != "dm" || ref.ID == "" || ref.Sender == "" || ref.Recipient == "" {
		return fmt.Errorf("invalid file message reference")
	}
	owner := m.ownerAddress()
	if sender && ref.Sender != owner || !sender && ref.Recipient != owner {
		return fmt.Errorf("file message reference has a different local owner")
	}
	return nil
}

// partialPath assigns a new transfer a path unique to its full local owner.
// Restored mappings keep their explicit PartialPath through every deferred step.
func (m *Manager) partialPath(ref domain.MessageRef) string {
	sum := sha256.Sum256([]byte(ref.Key()))
	return canonicalPartialPath(filepath.Join(m.downloadDir, "partial", "by-ref", fmt.Sprintf("%x.part", sum)))
}

// resolveLegacyRef refuses an unsigned ID naming more than one local record.
// Role filters preserve existing sender/receiver read contracts.
func (m *Manager) resolveLegacyRef(id domain.FileID, sender, receiver bool) (domain.MessageRef, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startupErr != nil {
		return domain.MessageRef{}, false, m.startupErr
	}
	var result domain.MessageRef
	found := false
	accept := func(ref domain.MessageRef) error {
		if ref.ID != string(id) {
			return nil
		}
		if found && result != ref {
			return domain.ErrAmbiguousMessage
		}
		result, found = ref, true
		return nil
	}
	if sender {
		for ref := range m.senderMaps {
			if err := accept(ref); err != nil {
				return domain.MessageRef{}, false, err
			}
		}
	}
	if receiver {
		for ref := range m.receiverMaps {
			if err := accept(ref); err != nil {
				return domain.MessageRef{}, false, err
			}
		}
	}
	return result, found, nil
}

func (t *SenderAnnounceToken) Commit(id domain.FileID, peer domain.PeerIdentity) error {
	return t.CommitRef(t.manager.senderRef(id, peer), peer)
}
func (m *Manager) RegisterFileReceive(id domain.FileID, hash, name, contentType string, size uint64, peer domain.PeerIdentity) error {
	return m.RegisterFileReceiveRef(m.receiverRef(id, peer), hash, name, contentType, size, peer)
}
func (m *Manager) legacyReceiver(id domain.FileID, action func(domain.MessageRef) error) error {
	ref, found, err := m.resolveLegacyRef(id, false, true)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("no receiver mapping for %s", id)
	}
	return action(ref)
}
func (m *Manager) StartDownload(id domain.FileID) error {
	return m.legacyReceiver(id, m.StartDownloadRef)
}
func (m *Manager) CancelDownload(id domain.FileID) error {
	return m.legacyReceiver(id, m.CancelDownloadRef)
}
func (m *Manager) RestartDownload(id domain.FileID) error {
	return m.legacyReceiver(id, m.RestartDownloadRef)
}
func (m *Manager) ForceRetryChunk(id domain.FileID) error {
	return m.legacyReceiver(id, m.ForceRetryChunkRef)
}
func (m *Manager) DeleteLocalCopy(id domain.FileID) error {
	ref, found, err := m.resolveLegacyRef(id, true, true)
	if err != nil {
		return err
	}
	if !found {
		return ErrNoLocalCopy
	}
	return m.DeleteLocalCopyRef(ref)
}
func (m *Manager) RemoveSenderMapping(id domain.FileID) bool {
	ref, found, err := m.resolveLegacyRef(id, true, false)
	return err == nil && found && m.RemoveSenderMappingRef(ref)
}
func (m *Manager) CleanupTransferByMessageID(id domain.FileID) {
	ref, found, err := m.resolveLegacyRef(id, true, true)
	if err == nil && found {
		m.CleanupTransferByMessageRef(ref)
	}
}
func (m *Manager) SenderProgress(id domain.FileID) (uint64, uint64, string, bool) {
	ref, found, err := m.resolveLegacyRef(id, true, false)
	if err != nil || !found {
		return 0, 0, "", false
	}
	return m.SenderProgressRef(ref)
}
func (m *Manager) ReceiverProgress(id domain.FileID) (uint64, uint64, string, bool) {
	ref, found, err := m.resolveLegacyRef(id, false, true)
	if err != nil || !found {
		return 0, 0, "", false
	}
	return m.ReceiverProgressRef(ref)
}
func (m *Manager) SenderFilePath(id domain.FileID) string {
	ref, found, err := m.resolveLegacyRef(id, true, false)
	if err != nil || !found {
		return ""
	}
	return m.SenderFilePathRef(ref)
}
func (m *Manager) ReceiverFilePath(id domain.FileID) string {
	ref, found, err := m.resolveLegacyRef(id, false, true)
	if err != nil || !found {
		return ""
	}
	return m.ReceiverFilePathRef(ref)
}

// canonicalPartialPath gives loaded legacy paths and new reference paths the
// same spelling for ownership comparisons and physical-path stripes.
func canonicalPartialPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(evalSymlinksPartial(absolute))
}
