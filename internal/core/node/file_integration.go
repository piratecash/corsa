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
	"github.com/piratecash/corsa/internal/core/ebus"
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
		MappingsPath: filetransfer.TransfersMappingsPath(dataDir, domain.PeerIdentityFromWire(s.identity.Address), domain.ListenAddress(s.cfg.ListenAddress)),
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
		// Bridge receiver-side download completion to the ebus so the
		// desktop UI can play download-done.mp3 without coupling the
		// filetransfer package to ebus directly. The manager invokes
		// this callback outside its own mutex; PublishFileDownloadCompleted
		// is no-op-on-nil so test fixtures that omit eventBus stay safe.
		OnReceiverDownloadComplete: func(ev filetransfer.ReceiverDownloadCompletedEvent) {
			ebus.PublishFileDownloadCompleted(s.eventBus, ebus.FileDownloadCompletedResult{
				From:        ev.Sender,
				FileID:      ev.FileID,
				FileName:    ev.FileName,
				FileSize:    ev.FileSize,
				ContentType: ev.ContentType,
			})
		},
	})

	router := filerouter.NewRouter(filerouter.RouterConfig{
		NonceCache: newDefaultNonceCache(),
		LocalID:    domain.PeerIdentityFromWire(s.identity.Address),
		IsFullNode: func() bool { return s.CanForward() },
		RouteSnap: func() routing.Snapshot {
			// Reads the cached routing snapshot maintained by the
			// hot-reads refresher (routing_snapshot.go). Bounded
			// staleness ≤ routingSnapshotMinInterval (1 s) + one
			// refresh tick (~1–1.5 s) is acceptable for the transit path
			// inside HandleInbound: the in-flight frame has its own
			// metadata and an ~1–1.5 s-stale view at worst delays the
			// failover by ~1–1.5 s. Each file-routing decision is then a
			// cheap atomic.Pointer load of the cached snapshot — strictly
			// better than taking a fresh routing-table snapshot (t.mu.Lock
			// + deep copy) per decision, which would block the routing
			// writers (announce loop, TickTTL, hop_ack confirmation).
			// Locally-originated
			// paths (SendFileCommand, ExplainRoute) read through
			// RouteLookup below instead, which goes straight to the
			// table and so sees a route the moment it lands.
			return s.loadRoutingSnapshot()
		},
		RouteLookup: func(dst domain.PeerIdentity) []routing.RouteEntry {
			// Per-destination fresh read for SendFileCommand and
			// ExplainRoute. routing.Table.Lookup takes t.mu.RLock,
			// filters withdrawn/expired and pre-sorts at O(K). It is
			// the same fresh oracle isPeerReachable uses — without
			// it, the user-initiated send racing the cached
			// snapshot's republish window would observe
			// isPeerReachable=true followed immediately by SendFile
			// returning "no route to <dst>". Direct Table access is
			// safe here because the lock is per-table (not the
			// Service domain mutex), so contention is bounded by
			// concurrent UpdateRoute / TickTTL writers, which is far
			// shallower than the Service-wide footprint.
			return s.routingTable.Lookup(dst)
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
			_, ok := s.trust.trustedContacts()[id.String()]
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
		log.Debug().Err(err).Str("src", frame.SRC.String()).Msg("file_transfer: decrypt payload failed")
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
	contact, ok := trusted[peer.String()]
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
//   - health entry is Connected and not stalled.
//
// Authenticity of every file-command frame is enforced at the wire layer
// (`FileCommandFrame.SrcPubKey` + identity-fingerprint check inside
// `Router.HandleInbound`); reachability does not need to mirror that check.
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

	// Outbound NetCores surface through s.sessions above; skip them
	// here so pre-activation outbound entries do not leak into the
	// file-transfer peer set before the session is established.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if !info.HasCapability(domain.CapFileTransferV1) {
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

// inflationWarnGap is the cutoff above which trustedFileRouteVersion
// raises the cap log to WARN. A reported version up to local+gap is
// treated as a normal staged rollout (one node a few releases ahead
// of the fleet, possibly skipping versions) and logged at DEBUG to
// avoid swamping the journal: trustedFileRouteVersion is invoked from
// every PeerRouteMeta lookup — once per route candidate per file
// command — so a single v=local+1 neighbour would otherwise emit a
// WARN on every chunk request through it. A reported version above
// local+gap is more likely a misconfig or an inflation attack
// (claiming an impossibly-new build to win the protocolVersion DESC
// primary key) and stays at WARN so operators can correlate the
// ranking surprise in explainFileRoute with a security signal in the
// journal.
//
// 4 is chosen empirically: alpha releases bump ProtocolVersion every
// few weeks, so a 4-version gap covers ~half a year of staged
// rollout drift. Anything beyond that is far enough out of band to
// warrant operator attention regardless of whether it is benign or
// hostile.
const inflationWarnGap = 4

// trustedFileRouteVersion returns the protocol version the file router
// should use for ranking decisions, applying the inflated-version
// defence: a peer claiming a higher protocol version than this build
// implements is either a benign staged-rollout case (operator upgraded
// the peer ahead of this node) or a deliberate traffic-capture attack
// — lie about being "newer" to win the protocolVersion DESC primary
// key and pull all file routes through the attacker's next-hop.
//
// The defence caps the ranking value at the local protocol version
// instead of zeroing it. The cap neutralises the inflation attack on
// the primary key (an attacker reporting v=local+100 cannot WIN over
// a legitimate v=local peer because both collapse to the same ranking
// value), but it does NOT push the upgraded peer to last-resort —
// that earlier "clamp to 0" behaviour broke staged rollouts: a single
// node ahead of the fleet was permanently starved of file traffic
// because every legacy v=local peer outranked it on the primary key
// regardless of hops or uptime. The cap leaves both peers in the same
// equal-version tier, where the secondary keys (hops ASC,
// connectedAt ASC) decide.
//
// RawProtocolVersion (set separately by the caller) keeps the
// actually-reported value for audit logging — only the ranking
// projection is capped here.
//
// Logging level is gated by inflationWarnGap (see above): a small
// gap is the normal staged-rollout shape and lands at DEBUG; a large
// gap is suspicious and stays at WARN. The cap itself fires
// regardless of log level — the log is purely informational.
func trustedFileRouteVersion(peer domain.PeerIdentity, reported domain.ProtocolVersion) domain.ProtocolVersion {
	const localVersion = domain.ProtocolVersion(config.ProtocolVersion)
	if reported <= localVersion {
		return reported
	}
	event := log.Debug()
	if int(reported)-int(localVersion) > inflationWarnGap {
		event = log.Warn()
	}
	event.
		Str("peer", peer.String()).
		Int("reported_version", int(reported)).
		Int("local_version", int(localVersion)).
		Msg("file_router: peer reports newer protocol version, capping at local for ranking")
	return localVersion
}

// isPeerReachable returns true if the peer has a direct session (outbound
// or inbound) with file_transfer_v1 capability, or an active route whose
// next-hop has file_transfer_v1 capability. Routes through next-hops without
// file transfer support are ignored — they cannot carry file commands and
// would cause pointless retries.
//
// Freshness contract: file-transfer entry points (UI "send file", incoming
// file_announce) AND the FileTransferManager periodic ticker call this.
// The user-initiated entry points must observe routes the routing table
// accepted up to the moment of the call — using the cached routing
// snapshot here would drop a route that arrived between two refresh
// ticks (≤ 500ms gap) and turn an immediate user action into a "peer
// not reachable" error. The periodic ticker iterates O(transfers) under
// FileTransferManager.mu, so a full table deep copy on every iteration
// would be O(transfers × routes) inside the file-transfer lock — fatal
// at scale. To satisfy both call sites the function uses
// routing.Table.Lookup(peer): a per-destination read that takes
// t.mu.RLock for an O(K) scan where K is the number of routes for this
// one identity (typically 1–10), not for the whole table.
//
// Lock discipline: the peer-domain probe (direct-session check,
// file-capable peer set, localID copy) runs under a short s.peerMu.RLock
// that is released BEFORE routing.Table.Lookup is called. The two reads
// are therefore sequential, not nested — a routing-table writer storm
// cannot extend a peer-domain hold and vice versa. The previous shape
// of this function nested t.mu.RLock inside s.peerMu.RLock and coupled
// the domains' contention together; this two-phase pattern eliminates
// that coupling and the per-call full-table deep copy in one move.
//
// The probe is best-effort by design (peer state may change microseconds
// after we observed it), so the small staleness gap between the peerMu
// read and the routing-table read is harmless: the file router runs its
// own candidate collection (with TTL/split-horizon/capability filters)
// before any send attempt, so a route that disappears between this
// probe and the actual send is filtered out at that later stage.
func (s *Service) isPeerReachable(peer domain.PeerIdentity) bool {
	now := time.Now().UTC()

	// Phase 1: peer-domain probe under a short RLock. Capture everything
	// we will need from peer state into local copies so the routing
	// read in Phase 2 does not need s.peerMu held.
	s.peerMu.RLock()
	if _, ok := s.fileTransferPeerRouteMetaLocked(peer, now); ok {
		s.peerMu.RUnlock()
		return true
	}
	// Reuse the same liveness rules as the actual file-router path so
	// FileTransferManager does not get an optimistic "reachable" result
	// for peers that are connected on paper but already stalled. The same
	// now timestamp is used for both the direct-peer and routed-peer checks
	// to avoid threshold drift around the healthy/degraded/stalled boundary.
	fileCapable := s.usableFileTransferPeersLocked(now)
	localID := domain.PeerIdentityFromWire(s.identity.Address)
	s.peerMu.RUnlock()

	// Phase 2: per-destination routing read, lock-sequential with Phase 1.
	// Lookup filters withdrawn and expired entries against the table's
	// own clock (time.Now by default), so a finite-TTL route that aged
	// out since the last TickTTL pass is correctly rejected without
	// waiting for the 10s TickTTL cycle. The full-table deep copy that
	// Snapshot() performs is replaced with an O(K) slice scan over only
	// this destination's routes — important because the periodic
	// receiver tick can call this for every active transfer under
	// FileTransferManager.mu.
	if s.routingTable == nil {
		return false
	}
	for _, route := range s.routingTable.Lookup(peer) {
		if route.NextHop == localID {
			continue
		}
		if _, capable := fileCapable[route.NextHop]; !capable {
			continue
		}
		return true
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

// AllFileTransfersSnapshot returns a typed list of every sender/receiver
// mapping (active AND terminal) for in-process callers — primarily the
// desktop UI's file tab. The JSON-encoded variant is FetchAllFileTransfers
// (used by the RPC layer); this method skips the marshal/unmarshal round
// trip when the consumer is in the same process.
//
// Returns an empty (non-nil) slice when the file-transfer subsystem is
// not initialized. Mirrors the contract of FetchAllFileTransfers.
func (s *Service) AllFileTransfersSnapshot() []filetransfer.TransferSnapshot {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return []filetransfer.TransferSnapshot{}
	}
	return manager.AllTransfersSnapshot()
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
//	    "protocol_version": 14,                  // normalized ranking key for this next-hop —
//	                                             // equal to the raw negotiated version for peers
//	                                             // at or below local; capped at config.ProtocolVersion
//	                                             // by the inflated-version defence (trustedFileRouteVersion)
//	                                             // when the peer reported v > config.ProtocolVersion.
//	                                             // The raw negotiated value is not serialized;
//	                                             // it lives in PeerRouteMeta.RawProtocolVersion.
//	                                             // Bounded above by config.ProtocolVersion;
//	                                             // peers below the global MinimumProtocolVersion
//	                                             // are rejected at handshake and never reach the plan.
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
			NextHop:         e.NextHop.String(),
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

// FetchAllFileTransfers returns a JSON-encoded list of ALL
// sender/receiver transfers, including terminal states (completed,
// failed, tombstone). Used by the desktop UI's file tab to show
// transfer history. Use FetchFileTransfers when only active/pending
// entries should be returned (existing observability RPC).
//
// Returns empty array when the subsystem is not initialized rather
// than an error — no transfers is a valid state.
func (s *Service) FetchAllFileTransfers() (json.RawMessage, error) {
	manager, _ := s.getFileTransferManager()
	if manager == nil {
		return json.RawMessage("[]"), nil
	}
	entries := manager.AllTransfersSnapshot()
	data, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("marshal all file transfers: %w", err)
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
