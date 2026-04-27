package node

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
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
		PeerRouteMeta: func(id domain.PeerIdentity) (filerouter.PeerRouteMeta, bool) {
			return s.fileTransferPeerRouteMeta(id)
		},
		// Authorization boundary for local delivery: only sources the
		// user has explicitly trusted may deposit files into the local
		// inbox. Authenticity is checked by the router itself from the
		// self-contained SrcPubKey field on the wire frame, so the
		// node only needs to express the trust policy here.
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := s.trust.trustedContacts()[string(id)]
			return ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			return s.sendFileRawToPeer(dst, data)
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			s.handleLocalFileCommand(frame)
		},
	})

	// fileMu instead of s.peerMu: the file subsystem handles are a standalone
	// domain (see docs/locking.md). Writing them under s.peerMu would block
	// every unrelated peer-domain reader (network_stats, peer_health) on
	// every startup/teardown cycle.
	log.Trace().Str("site", "initFileTransfer").Str("phase", "lock_wait").Msg("file_mu_writer")
	s.fileMu.Lock()
	log.Trace().Str("site", "initFileTransfer").Str("phase", "lock_held").Msg("file_mu_writer")
	s.fileStore = store
	s.fileTransfer = manager
	s.fileRouter = router
	s.fileMu.Unlock()
	log.Trace().Str("site", "initFileTransfer").Str("phase", "lock_released").Msg("file_mu_writer")

	manager.Start()
	log.Info().Msg("file_transfer: subsystem initialized")
}

// stopFileTransfer shuts down the file transfer subsystem.
//
// Reads the manager handle under fileMu — the handle itself lives in the file
// domain (see docs/locking.md).  Manager.Stop() runs outside the lock because
// teardown may block on in-flight transfers and we do not want to hold fileMu
// while waiting on the file subsystem's own internal synchronisation.
func (s *Service) stopFileTransfer() {
	s.fileMu.RLock()
	manager := s.fileTransfer
	s.fileMu.RUnlock()

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
	// fileMu, not s.peerMu: fileRouter lives in the file domain.
	s.fileMu.RLock()
	router := s.fileRouter
	s.fileMu.RUnlock()

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

	// fileMu, not s.peerMu: fileRouter lives in the file domain.
	s.fileMu.RLock()
	router := s.fileRouter
	s.fileMu.RUnlock()
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

// fileTransferPeerRouteMeta reports whether peer currently has at least one
// usable file-capable connection and, when usable, returns the route-meta
// snapshot the file router needs to rank candidates.
//
// The returned ConnectedAt and ProtocolVersion describe a *single*
// connection — the one sendFrameToIdentity would actually try first
// (outbound preferred, inbound as fall-back, both filtered through the
// shared peerSendableConnectionsLocked policy). Aggregating across all
// eligible sessions (max version, oldest connectedAt) would let the file
// router prefer this peer based on a version no packet would ever
// traverse, because the live send path does not pick by version. Stalled
// peers are treated as unusable so file datagrams do not route into dead
// next-hops.
func (s *Service) fileTransferPeerRouteMeta(peer domain.PeerIdentity) (filerouter.PeerRouteMeta, bool) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.fileTransferPeerRouteMetaLocked(peer, time.Now().UTC())
}

// forEachUsableFileTransferPeerLocked walks every peer that this node
// could legitimately use for file-transfer traffic and invokes visit for
// each one. Eligibility mirrors the live send path
// (peerSendableConnectionsLocked / fileTransferPeerRouteMetaLocked):
//
//   - file_transfer_v1 capability is negotiated;
//   - health entry is Connected and not stalled;
//   - raw negotiated protocol_version is at or above
//     domain.FileCommandMinPeerProtocolVersion.
//
// The version gate is the critical part. Without it,
// isPeerReachable / file-transfer scheduling would happily promise a
// route through a v11 next-hop that Router.collectRouteCandidates and
// peerSendableConnectionsLocked later reject — reachability lying, with
// downloads waking up onto a route the actual file router is forbidden
// to use. The cutover regression for that lie lives in the
// TestIsPeerReachable_RouteVia(BelowCutover|UnknownVersion)NextHop pair
// in file_integration_test.go.
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
		// Cutover gate, raw negotiated version. peerSession.version is
		// pre-clamp (the route-meta layer is where inflated-version
		// clamp lives), so a value below the cutover here is always a
		// genuinely-pre-cutover or unknown peer; admitting it would
		// re-open the v11 black hole. Comparing on
		// domain.ProtocolVersion avoids the uint8-narrow wrap.
		if domain.ProtocolVersion(sess.version) < domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion) {
			continue
		}
		consider(sess.peerIdentity, sess.address)
	}

	// Outbound NetCores surface through s.sessions above; skip them
	// here so pre-activation outbound entries do not leak into the
	// file-transfer peer set before the session is established.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if !info.HasCapability(domain.CapFileTransferV1) {
			return true
		}
		// Same cutover gate on the inbound tier; info.protocolVersion
		// is the raw negotiated value captured from the inbound
		// NetCore.
		if info.protocolVersion < domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion) {
			return true
		}
		consider(info.identity, info.address)
		return true
	})
}

func (s *Service) usableFileTransferPeersLocked(now time.Time) map[domain.PeerIdentity]struct{} {
	fileCapable := make(map[domain.PeerIdentity]struct{})
	s.forEachUsableFileTransferPeerLocked(now, func(id domain.PeerIdentity, _ time.Time) {
		fileCapable[id] = struct{}{}
	})
	return fileCapable
}

func (s *Service) fileTransferPeerRouteMetaLocked(peer domain.PeerIdentity, now time.Time) (filerouter.PeerRouteMeta, bool) {
	// Single source of truth: ask the same helper that backs
	// sendFrameToIdentity for the ordered candidate list. Picking the
	// head of that list guarantees the meta describes the connection
	// the live send path would actually try first — the previous
	// implementation aggregated max(version) and oldest(connectedAt)
	// across *all* candidates and could promise an inbound v9 path
	// while bytes were going out over an outbound v5 session.
	//
	// The slice is sorted outbound-first by the helper, so the head
	// is the outbound session when one exists, falling back to an
	// inbound conn otherwise — exactly the live send-path policy.
	candidates := s.peerSendableConnectionsLocked(peer, domain.CapFileTransferV1, now)
	if len(candidates) == 0 {
		return filerouter.PeerRouteMeta{}, false
	}
	picked := candidates[0]
	return filerouter.PeerRouteMeta{
		ConnectedAt:        picked.connectedAt,
		ProtocolVersion:    trustedFileRouteVersion(peer, picked.protocolVersion),
		RawProtocolVersion: picked.protocolVersion,
	}, true
}

// trustedFileRouteVersion returns the protocol version the file router
// should use for ranking decisions, applying the inflated-version
// defence: a peer claiming a higher protocol version than this build
// implements cannot actually be speaking the protocol we just
// enumerated, so the claim is either a misconfig or a deliberate
// traffic-capture attack — lie about being "newer" to win the
// protocolVersion DESC primary key and pull all file routes through
// the attacker's next-hop.
//
// The defence is a soft demote, not a hard reject: ranking version is
// clamped to zero, which sorts below every legit (peer ≤ local)
// version under DESC ordering and pushes the candidate to the bottom
// of the plan. The peer remains usable as a last-resort hop — we want
// the option to deliver bytes when no trusted peer is reachable, just
// not to *prefer* the suspicious one.
//
// Logging is at Warn level so operators can correlate ranking
// surprises in the diagnostic with security signal in the journal.
func trustedFileRouteVersion(peer domain.PeerIdentity, reported domain.ProtocolVersion) domain.ProtocolVersion {
	const localVersion = domain.ProtocolVersion(config.ProtocolVersion)
	if reported <= localVersion {
		return reported
	}
	log.Warn().
		Str("peer", string(peer)).
		Int("reported_version", int(reported)).
		Int("local_version", int(localVersion)).
		Msg("file_router: peer reports inflated protocol version, demoted in ranking")
	return 0
}

// isPeerReachable returns true if the peer has a direct session (outbound
// or inbound) with file_transfer_v1 capability, or an active route whose
// next-hop has file_transfer_v1 capability. Routes through next-hops without
// file transfer support are ignored — they cannot carry file commands and
// would cause pointless retries.
func (s *Service) isPeerReachable(peer domain.PeerIdentity) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	now := time.Now().UTC()

	// Reuse the same liveness rules as the actual file-router path so
	// FileTransferManager does not get an optimistic "reachable" result
	// for peers that are connected on paper but already stalled. The same
	// now timestamp is used for both the direct-peer and routed-peer checks
	// to avoid threshold drift around the healthy/degraded/stalled boundary.
	// Reachability only cares about the boolean result; the meta payload
	// is discarded here on purpose.
	if _, ok := s.fileTransferPeerRouteMetaLocked(peer, now); ok {
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

// CleanupTransferByMessageID releases sender/receiver mappings,
// transmit-blob refs, and partial/completed downloaded files attached
// to a single DM. Called by the DM-router delete hook after a chatlog
// row has been removed (locally or via inbound message_delete). No-op
// when no file-transfer state is associated with the message ID.
func (s *Service) CleanupTransferByMessageID(fileID domain.FileID) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return
	}
	manager.CleanupTransferByMessageID(fileID)
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

// ExplainFileRoute is the diagnostic counterpart of sendFileCommandToPeer:
// it returns the ranked list of next-hops the file router would try
// when sending a file command to dst, marking the first one as best.
// excludeVia is intentionally empty — callers are asking "which peer
// would I actually use?", not "which peer would I use as a transit
// node for an inbound frame", so split-horizon does not apply here.
//
// Returns errFileTransferNotInitialized when the file subsystem has
// not been wired yet (matches the rest of the file-domain API). An
// empty array is returned when the subsystem is up but no usable
// next-hop currently exists for dst — that is a valid state, not an
// error.
//
// Wire schema (one entry per next-hop, in selection order):
//
//	[
//	  {
//	    "next_hop": "<peer identity>",
//	    "hops": 1,
//	    "protocol_version": 12,                  // normalized ranking key for this next-hop —
//	                                             // equal to the raw negotiated version for legit
//	                                             // peers, but clamped to 0 by the inflated-version
//	                                             // defence (trustedFileRouteVersion) when the peer
//	                                             // reported v > config.ProtocolVersion. The raw
//	                                             // negotiated value is not serialized; it lives in
//	                                             // PeerRouteMeta.RawProtocolVersion as the
//	                                             // eligibility key only. Pre-cutover and unknown
//	                                             // peers are filtered out before the plan, so the
//	                                             // only way 0 appears here is the clamped-inflated
//	                                             // case.
//	    "connected_at": "2025-01-01T12:34:56Z",  // omitted when unknown
//	    "uptime_seconds": 3600.5,                // 0 when connected_at omitted
//	    "best": true                             // true only for index 0
//	  },
//	  ...
//	]
func (s *Service) ExplainFileRoute(dst domain.PeerIdentity) (json.RawMessage, error) {
	s.fileMu.RLock()
	router := s.fileRouter
	s.fileMu.RUnlock()
	if router == nil {
		return nil, errFileTransferNotInitialized
	}

	plan := router.ExplainRoute(dst)
	now := time.Now().UTC()

	type wireEntry struct {
		NextHop         string  `json:"next_hop"`
		Hops            int     `json:"hops"`
		ProtocolVersion int     `json:"protocol_version"`
		ConnectedAt     string  `json:"connected_at,omitempty"`
		UptimeSeconds   float64 `json:"uptime_seconds"`
		Best            bool    `json:"best"`
	}

	out := make([]wireEntry, len(plan))
	for i, e := range plan {
		entry := wireEntry{
			NextHop:         string(e.NextHop),
			Hops:            e.Hops,
			ProtocolVersion: int(e.ProtocolVersion),
			Best:            i == 0,
		}
		if !e.ConnectedAt.IsZero() {
			// connected_at is rendered in RFC3339 UTC so it round-trips
			// through any JSON consumer (CLI table, desktop console, SDK).
			// uptime_seconds is the derived view used by the peers console:
			// (now - connected_at) clamped at zero so a future-dated
			// connectedAt (clock-skew bug) never produces negative uptime.
			entry.ConnectedAt = e.ConnectedAt.UTC().Format(time.RFC3339)
			uptime := now.Sub(e.ConnectedAt).Seconds()
			if uptime < 0 {
				uptime = 0
			}
			entry.UptimeSeconds = uptime
		}
		out[i] = entry
	}

	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal file route plan: %w", err)
	}
	return data, nil
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
//
// Read from fileMu (not s.peerMu): the manager handle is a file-domain field.
// Callers that combine this with peer-state reads (e.g. sendFileCommandToPeer)
// release fileMu before acquiring s.peerMu so the two domains never nest —
// the helper returns by value and the caller decides when to inspect peer
// state.
func (s *Service) getFileTransferManager() (*filetransfer.Manager, error) {
	s.fileMu.RLock()
	manager := s.fileTransfer
	s.fileMu.RUnlock()

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

// (peerPubKeyFromTrust / peerPubKeyFromKnowledge removed: the file
// router now verifies authenticity self-contained from the wire frame
// via FileCommandFrame.SrcPubKey + identity fingerprint check. Local-
// delivery authorization is expressed directly via the trust-store
// lookup inlined into the IsAuthorizedForLocalDelivery callback in
// initFileTransfer. See docs/protocol/file_transfer.md for the
// authenticity-vs-authorization split.)
