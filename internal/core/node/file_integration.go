package node

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// initFileTransfer initializes the file transfer subsystem and wires it into
// the Service. Called once during Service startup when the node supports
// file_transfer_v1.
//
// Components created:
//   - fileStore: content-addressed file storage in <dataDir>/transmit
//   - FileTransferManager: sender/receiver state machines
//   - fileRouter: routing file commands through the mesh
func (s *Service) initFileTransfer() {
	dataDir := s.cfg.EffectiveDataDir()
	transmitDir := filepath.Join(dataDir, domain.TransmitSubdir)
	store, err := filetransfer.NewFileStore(transmitDir)
	if err != nil {
		log.Error().Err(err).Msg("file_transfer: failed to initialize file store")
		return
	}

	downloadDir := s.cfg.EffectiveDownloadDir()

	manager := filetransfer.NewFileTransferManager(filetransfer.Config{
		Store:        store,
		DownloadDir:  downloadDir,
		MappingsPath: filetransfer.TransfersMappingsPath(dataDir, domain.PeerIdentity(s.identity.Address), domain.ListenAddress(s.cfg.ListenAddress)),
		LocalID:      s.identity,
		SendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return s.sendFileCommandToPeer(dst, payload)
		},
		PeerBoxKey: func(peer domain.PeerIdentity) (string, bool) {
			return s.peerBoxKeyBase64(peer)
		},
		PeerReachable: func(peer domain.PeerIdentity) bool {
			return s.isPeerReachable(peer)
		},
	})

	router := filerouter.NewRouter(filerouter.RouterConfig{
		NonceCache: newDefaultNonceCache(),
		LocalID:    domain.PeerIdentity(s.identity.Address),
		IsFullNode: func() bool { return s.CanForward() },
		RouteSnap: func() routing.Snapshot {
			if s.routingTable == nil {
				return routing.Snapshot{}
			}
			return s.routingTable.Snapshot()
		},
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			return s.fileTransferPeerUsableAt(id)
		},
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			return peerPubKeyFromTrust(s, id)
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			return s.sendFileRawToPeer(dst, data)
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			s.handleLocalFileCommand(frame)
		},
	})

	s.mu.Lock()
	s.fileStore = store
	s.fileTransfer = manager
	s.fileRouter = router
	s.mu.Unlock()

	manager.Start()
	log.Info().Msg("file_transfer: subsystem initialized")
}

// stopFileTransfer shuts down the file transfer subsystem.
func (s *Service) stopFileTransfer() {
	s.mu.RLock()
	manager := s.fileTransfer
	s.mu.RUnlock()

	if manager != nil {
		manager.Stop()
	}
}

// handleFileCommandFrame is the entry point for inbound file_command frames
// received from the network. Called from the main frame dispatcher.
//
// incomingPeer is the identity of the immediate neighbor the frame arrived
// from. It is propagated into the router so transit forwarding applies
// split-horizon and never reflects the frame back to that neighbor. An
// empty identity disables split-horizon (used for locally-injected frames
// and tests).
func (s *Service) handleFileCommandFrame(raw json.RawMessage, incomingPeer domain.PeerIdentity) {
	s.mu.RLock()
	router := s.fileRouter
	s.mu.RUnlock()

	if router == nil {
		log.Debug().Msg("file_transfer: received file_command but subsystem not initialized")
		return
	}

	router.HandleInbound(raw, incomingPeer)
}

// handleLocalFileCommand decrypts and dispatches a file command that arrived
// at its final destination (DST == local identity).
func (s *Service) handleLocalFileCommand(frame protocol.FileCommandFrame) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return
	}

	// Decrypt payload using local identity's box key.
	payload, err := directmsg.DecryptFileCommandPayload(s.identity, frame.Payload)
	if err != nil {
		log.Debug().Err(err).Str("src", string(frame.SRC)).Msg("file_transfer: decrypt payload failed")
		return
	}

	manager.HandleLocalFileCommand(frame.SRC, payload)
}

// sendFileCommandToPeer encrypts and sends a file command payload to a peer.
// Used by FileTransferManager to dispatch commands.
func (s *Service) sendFileCommandToPeer(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
	boxKey, ok := s.peerBoxKeyBase64(dst)
	if !ok {
		return errPeerBoxKeyNotFound
	}

	s.mu.RLock()
	router := s.fileRouter
	s.mu.RUnlock()
	if router == nil {
		return errFileTransferNotInitialized
	}

	return router.SendFileCommand(
		dst,
		boxKey,
		payload,
		s.identity.PrivateKey,
		directmsg.EncryptFileCommandPayload,
	)
}

// peerBoxKeyBase64 returns the base64-encoded box key for a trusted peer.
func (s *Service) peerBoxKeyBase64(peer domain.PeerIdentity) (string, bool) {
	trusted := s.trust.trustedContacts()
	contact, ok := trusted[string(peer)]
	if !ok {
		return "", false
	}
	if contact.BoxKey == "" {
		return "", false
	}
	return contact.BoxKey, true
}

// hasCapability returns true if the slice contains the target capability.
func hasCapability(caps []domain.Capability, target domain.Capability) bool {
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

// fileTransferPeerUsableAt reports whether peer currently has at least one
// usable file-capable connection and, when known, when that peer most
// recently connected. Stalled peers are treated as unusable so file datagrams
// do not route into dead next-hops. When multiple live connections exist for
// the same identity, the oldest connected one wins as the stability tie-break.
func (s *Service) fileTransferPeerUsableAt(peer domain.PeerIdentity) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fileTransferPeerUsableAtLocked(peer, time.Now().UTC())
}

func (s *Service) forEachUsableFileTransferPeerLocked(now time.Time, visit func(domain.PeerIdentity, time.Time)) {
	consider := func(id domain.PeerIdentity, address domain.PeerAddress) {
		health := s.health[s.resolveHealthAddress(address)]
		if health == nil || !health.Connected || s.computePeerStateAtLocked(health, now) == peerStateStalled {
			return
		}
		visit(id, health.LastConnectedAt)
	}

	for _, sess := range s.sessions {
		if sess == nil || !hasCapability(sess.capabilities, domain.CapFileTransferV1) {
			continue
		}
		consider(sess.peerIdentity, sess.address)
	}

	for _, pc := range s.inboundNetCores {
		if pc == nil || !pc.HasCapability(domain.CapFileTransferV1) {
			continue
		}
		consider(pc.Identity(), pc.Address())
	}
}

func (s *Service) usableFileTransferPeersLocked(now time.Time) map[domain.PeerIdentity]struct{} {
	fileCapable := make(map[domain.PeerIdentity]struct{})
	s.forEachUsableFileTransferPeerLocked(now, func(id domain.PeerIdentity, _ time.Time) {
		fileCapable[id] = struct{}{}
	})
	return fileCapable
}

func (s *Service) fileTransferPeerUsableAtLocked(peer domain.PeerIdentity, now time.Time) (time.Time, bool) {
	var oldest time.Time
	found := false

	consider := func(address domain.PeerAddress) {
		health := s.health[s.resolveHealthAddress(address)]
		if health == nil || !health.Connected || s.computePeerStateAtLocked(health, now) == peerStateStalled {
			return
		}
		if !health.LastConnectedAt.IsZero() && (oldest.IsZero() || health.LastConnectedAt.Before(oldest)) {
			oldest = health.LastConnectedAt
		}
		found = true
	}

	for _, sess := range s.sessions {
		if sess == nil || sess.peerIdentity != peer || !hasCapability(sess.capabilities, domain.CapFileTransferV1) {
			continue
		}
		consider(sess.address)
	}

	for _, pc := range s.inboundNetCores {
		if pc == nil || pc.Identity() != peer || !pc.HasCapability(domain.CapFileTransferV1) {
			continue
		}
		consider(pc.Address())
	}

	return oldest, found
}

// isPeerReachable returns true if the peer has a direct session (outbound
// or inbound) with file_transfer_v1 capability, or an active route whose
// next-hop has file_transfer_v1 capability. Routes through next-hops without
// file transfer support are ignored — they cannot carry file commands and
// would cause pointless retries.
func (s *Service) isPeerReachable(peer domain.PeerIdentity) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()

	// Reuse the same liveness rules as the actual file-router path so
	// FileTransferManager does not get an optimistic "reachable" result
	// for peers that are connected on paper but already stalled. The same
	// now timestamp is used for both the direct-peer and routed-peer checks
	// to avoid threshold drift around the healthy/degraded/stalled boundary.
	if _, ok := s.fileTransferPeerUsableAtLocked(peer, now); ok {
		return true
	}

	// Build the set of peers that currently have at least one usable
	// file-capable connection. Used to filter route next-hops.
	fileCapable := s.usableFileTransferPeersLocked(now)
	localID := domain.PeerIdentity(s.identity.Address)
	rt := s.routingTable

	// Check routing table — skip self-routes and next-hops without
	// file_transfer_v1 capability.
	if rt != nil {
		snap := rt.Snapshot()
		if routes, ok := snap.Routes[peer]; ok {
			for i := range routes {
				if routes[i].IsWithdrawn() || routes[i].IsExpired(snap.TakenAt) {
					continue
				}
				if routes[i].NextHop == localID {
					continue
				}
				if _, capable := fileCapable[routes[i].NextHop]; !capable {
					continue
				}
				return true
			}
		}
	}

	return false
}

// sendFileRawToPeer sends pre-serialized file command bytes to a peer.
// Delegates to the unified sendFrameToIdentity, which routes through the
// per-connection write queue (servePeerSession for outbound, connWriter for
// inbound) — preventing byte interleaving on shared sockets.
func (s *Service) sendFileRawToPeer(dst domain.PeerIdentity, data []byte) bool {
	data = append(data, '\n')
	frame := protocol.Frame{
		Type:    protocol.FileCommandFrameType,
		RawLine: string(data),
	}
	return s.sendFrameToIdentity(dst, frame, domain.CapFileTransferV1)
}

// StoreFileForTransmit delegates to FileTransferManager.StoreFileForTransmit
// to copy the source file into the transmit directory with content-addressed
// dedup. Returns the SHA-256 hash of the file content.
func (s *Service) StoreFileForTransmit(sourcePath string) (string, error) {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return "", err
	}
	return manager.StoreFileForTransmit(sourcePath)
}

// TransmitFileSize returns the byte size of the stored transmit blob.
// The size comes from the persisted copy, not from the original source file.
func (s *Service) TransmitFileSize(fileHash string) (uint64, error) {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return 0, err
	}
	return manager.TransmitFileSize(fileHash)
}

// RemoveUnreferencedTransmitFile delegates to FileTransferManager to delete
// the transmit blob if no active ref or pending reservation protects it.
func (s *Service) RemoveUnreferencedTransmitFile(fileHash string) {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return
	}
	manager.RemoveUnreferencedTransmitFile(fileHash)
}

// PrepareFileAnnounce atomically validates transmit file availability and
// reserves a sender quota slot. Returns a token that the caller uses to
// either Commit (after the DM is sent successfully) or Rollback (on any
// failure). This encapsulates the entire transmit-file and sender-mapping
// lifecycle so callers (dm_router) only deal with sending the DM.
func (s *Service) PrepareFileAnnounce(
	fileHash, fileName, contentType string,
	fileSize uint64,
) (*filetransfer.SenderAnnounceToken, error) {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return nil, err
	}
	return manager.PrepareFileAnnounce(fileHash, fileName, contentType, fileSize)
}

// FileTransferProgress returns the transfer progress for a given FileID.
// The caller indicates whether it is the sender (isSender=true) or receiver.
func (s *Service) FileTransferProgress(fileID domain.FileID, isSender bool) (bytesTransferred, totalSize uint64, state string, found bool) {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return 0, 0, "", false
	}
	if isSender {
		return manager.SenderProgress(fileID)
	}
	return manager.ReceiverProgress(fileID)
}

// FileTransferFilePath returns the on-disk path for a transferred file.
// For the sender it resolves to the content-addressed blob in the transmit
// directory; for the receiver it returns the CompletedPath of the download.
// Returns empty string if the file is not found or not yet available.
func (s *Service) FileTransferFilePath(fileID domain.FileID, isSender bool) string {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return ""
	}
	if isSender {
		return manager.SenderFilePath(fileID)
	}
	return manager.ReceiverFilePath(fileID)
}

// RegisterIncomingFileTransfer registers a receiver-side file mapping after
// a file_announce DM has been received and decrypted. Does not start
// downloading — call StartFileDownload when the user accepts. Returns an
// error if the announce metadata is invalid or the subsystem is not ready.
func (s *Service) RegisterIncomingFileTransfer(
	fileID domain.FileID,
	fileHash, fileName, contentType string,
	fileSize uint64,
	sender domain.PeerIdentity,
) error {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return err
	}
	return manager.RegisterFileReceive(fileID, fileHash, fileName, contentType, fileSize, sender)
}

// CancelFileDownload aborts an active download, deletes the partial file,
// and resets the receiver mapping to available state.
func (s *Service) CancelFileDownload(fileID domain.FileID) error {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return err
	}
	return manager.CancelDownload(fileID)
}

// StartFileDownload begins downloading a file that was previously registered
// via RegisterIncomingFileTransfer. Sends the first chunk_request to the sender.
func (s *Service) StartFileDownload(fileID domain.FileID) error {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return err
	}
	return manager.StartDownload(fileID)
}

// CleanupPeerTransfers removes all file transfer mappings associated with the
// given peer identity. Releases transmit file refs and deletes downloaded files.
func (s *Service) CleanupPeerTransfers(peer domain.PeerIdentity) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return
	}
	manager.CleanupPeerTransfers(peer)
}

// RemoveSenderMapping removes a single sender mapping by fileID, releasing
// the transmit file ref if the mapping is not in a terminal state. Returns
// true if the mapping existed and was removed.
func (s *Service) RemoveSenderMapping(fileID domain.FileID) bool {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return false
	}
	return manager.RemoveSenderMapping(fileID)
}

// FetchFileTransfers returns a JSON-encoded list of active and pending
// sender/receiver transfers. Terminal states (completed, tombstone,
// failed) are excluded. Returns empty array when the subsystem is not
// initialized rather than an error — no transfers is a valid state.
func (s *Service) FetchFileTransfers() (json.RawMessage, error) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return json.RawMessage("[]"), nil
	}
	entries := manager.TransfersSnapshot()
	data, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("marshal file transfers: %w", err)
	}
	return data, nil
}

// FetchFileMappings returns a JSON-encoded list of sender file mappings.
// TransmitPath is excluded from the output.
func (s *Service) FetchFileMappings() (json.RawMessage, error) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return json.RawMessage("[]"), nil
	}
	entries := manager.MappingsSnapshot()
	data, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("marshal file mappings: %w", err)
	}
	return data, nil
}

// RestartFileDownload resets a failed download back to available so the
// user can re-initiate the download.
func (s *Service) RestartFileDownload(fileID domain.FileID) error {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return err
	}
	return manager.RestartDownload(fileID)
}

// RetryFileChunk forces an immediate retry of a stalled chunk request.
func (s *Service) RetryFileChunk(fileID domain.FileID) error {
	manager, err := s.getFileTransferManager()
	if err != nil {
		return err
	}
	return manager.ForceRetryChunk(fileID)
}

// getFileTransferManager returns the FileTransferManager or
// errFileTransferNotInitialized if the subsystem is not ready.
func (s *Service) getFileTransferManager() (*filetransfer.Manager, error) {
	s.mu.RLock()
	manager := s.fileTransfer
	s.mu.RUnlock()

	if manager == nil {
		return nil, errFileTransferNotInitialized
	}
	return manager, nil
}

// Sentinel errors for file transfer.
var (
	errPeerBoxKeyNotFound         = &fileTransferError{"peer box key not found"}
	errFileTransferNotInitialized = &fileTransferError{"file transfer subsystem not initialized"}
)

type fileTransferError struct {
	msg string
}

func (e *fileTransferError) Error() string {
	return "file_transfer: " + e.msg
}

// peerPubKeyFromTrust resolves a PeerIdentity to an Ed25519 public key
// using the trust store. This is a helper for filerouter.RouterConfig.PeerPubKey.
func peerPubKeyFromTrust(s *Service, identity domain.PeerIdentity) (ed25519.PublicKey, bool) {
	trusted := s.trust.trustedContacts()
	contact, ok := trusted[string(identity)]
	if !ok {
		return nil, false
	}

	pubBytes, err := base64.StdEncoding.DecodeString(contact.PubKey)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		return nil, false
	}

	return pubBytes, true
}
