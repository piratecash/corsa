package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// FileTransferBridge owns file transfer orchestration: preparing sender
// announcements, committing/rolling back sender mappings, registering
// incoming transfers, and proxying download control. DMRouter delegates
// all file-transfer concerns here so it can remain a pure message router.
type FileTransferBridge struct {
	client *DesktopClient
	// registerIncomingFn is the receiver-side registration call. In
	// production it points at client.RegisterIncomingFileTransfer
	// (which delegates through the embedded node.Service to
	// FileTransferManager.RegisterFileReceive). Tests can override
	// via setRegisterIncomingForTest to drive RegisterIncoming
	// against an in-process Manager without standing up a full
	// node.Service.
	registerIncomingFn func(fileID domain.FileID, fileHash, fileName, contentType string, fileSize uint64, sender domain.PeerIdentity) error
}

// NewFileTransferBridge creates a bridge backed by the given client.
// The default registrar dispatches through client.RegisterIncomingFileTransfer
// (errNoLocalNode in standalone-RPC mode).
func NewFileTransferBridge(client *DesktopClient) *FileTransferBridge {
	b := &FileTransferBridge{client: client}
	b.registerIncomingFn = b.defaultRegisterIncoming
	return b
}

// defaultRegisterIncoming routes the registration through the embedded
// DesktopClient — extracted as a method so the constructor's bound
// method value remains valid even after tests override
// registerIncomingFn (the override does not modify the default).
func (b *FileTransferBridge) defaultRegisterIncoming(fileID domain.FileID, fileHash, fileName, contentType string, fileSize uint64, sender domain.PeerIdentity) error {
	return b.client.RegisterIncomingFileTransfer(fileID, fileHash, fileName, contentType, fileSize, sender)
}

// AnnounceResult is returned by PrepareAndSend on success. It contains
// everything the caller (DMRouter) needs to update its UI state.
type AnnounceResult struct {
	Sent   *DirectMessage
	FileID domain.FileID
}

// PrepareAndSend validates the transmit file, reserves a sender quota slot,
// sends the file_announce DM, and commits the sender mapping. On any
// failure the reservation is rolled back automatically.
//
// The method is synchronous and blocking (up to sendTimeout). The caller
// is responsible for running it in a goroutine if async behavior is needed.
func (b *FileTransferBridge) PrepareAndSend(
	ctx context.Context,
	to domain.PeerIdentity,
	msg domain.OutgoingDM,
	meta domain.FileAnnouncePayload,
) (*AnnounceResult, error) {
	token, err := b.client.PrepareFileAnnounce(
		meta.FileHash, meta.FileName, meta.ContentType, meta.FileSize,
	)
	if err != nil {
		// EnsureStored placed the blob on disk without a ref or pending
		// reservation. If PrepareFileAnnounce fails before creating a
		// token (e.g. maxFileMappings), no Rollback exists to clean up.
		// RemoveUnreferencedTransmitFile deletes the blob only when
		// refs=0 AND pending=0, so concurrent senders of the same
		// content are safe.
		b.client.RemoveUnreferencedTransmitFile(meta.FileHash)

		log.Warn().Err(err).
			Str("file_hash", meta.FileHash).
			Str("file_name", meta.FileName).
			Msg("file_transfer_bridge: preparation failed")
		return nil, fmt.Errorf("prepare file announce: %w", err)
	}
	defer token.Rollback() // no-op after Commit

	sent, err := b.client.SendDirectMessage(ctx, to, msg)
	if err != nil {
		return nil, fmt.Errorf("send file announce DM: %w", err)
	}
	if sent == nil {
		return nil, fmt.Errorf("send file announce DM: no message returned")
	}

	fileID := domain.FileID(sent.ID)
	if err := token.Commit(fileID, to); err != nil {
		// Commit marks committed=true and keeps the in-memory mapping
		// even when persist fails, so defer Rollback is a no-op and the
		// mapping remains live for the current session. The DM has
		// already reached the recipient — they see a file card and can
		// request chunks immediately. Persist failure means the mapping
		// is lost on restart, but that is strictly better than dropping
		// it now while the peer is waiting.
		log.Error().Err(err).
			Str("file_id", sent.ID).
			Msg("file_transfer_bridge: commit persist failed — mapping live in memory, transfer may not survive restart")
	}

	return &AnnounceResult{Sent: sent, FileID: fileID}, nil
}

// RollbackMapping removes a single committed sender mapping that became
// orphaned (e.g. peer removed after Commit). Targeted cleanup — does not
// affect other mappings for the same peer.
func (b *FileTransferBridge) RollbackMapping(fileID domain.FileID) {
	b.client.RemoveSenderMapping(fileID)
}

// RegisterIncoming registers a receiver-side mapping for a file_announce DM.
// Parses the announce payload from the message and delegates to the
// FileTransferManager.
//
// Returns nil on successful (or idempotent) registration, a non-nil
// error on every failure path so the caller can decide whether to
// publish downstream events. Failure modes:
//
//   - msg is not a file_announce or has empty CommandData → ignored
//     and returns nil; this is the legitimate non-file-announce
//     pass-through, not an error.
//   - JSON parse failure → returns the unmarshal error; no mapping
//     was created.
//   - DesktopClient.RegisterIncomingFileTransfer error (errNoLocalNode
//     in standalone-RPC mode, manager-side validation rejection,
//     etc.) → returns the underlying error verbatim.
//
// The caller (DMRouter.tryRegisterFileReceive) gates TopicFileReceived
// publication on a nil return so subscribers never see "registered"
// events for transfers that AllTransfersSnapshot will never list.
func (b *FileTransferBridge) RegisterIncoming(msg DirectMessage) error {
	if msg.Command != domain.DMCommandFileAnnounce || msg.CommandData == "" {
		return nil
	}

	var payload domain.FileAnnouncePayload
	if err := json.Unmarshal([]byte(msg.CommandData), &payload); err != nil {
		log.Warn().Err(err).Str("msg_id", msg.ID).
			Msg("file_transfer_bridge: failed to parse file_announce payload")
		return fmt.Errorf("parse file_announce payload: %w", err)
	}

	fileID := domain.FileID(msg.ID)
	if err := b.registerIncomingFn(
		fileID, payload.FileHash, payload.FileName, payload.ContentType,
		payload.FileSize, msg.Sender,
	); err != nil {
		log.Warn().Err(err).
			Str("file_id", string(fileID)).
			Str("sender", string(msg.Sender)).
			Msg("file_transfer_bridge: rejected file_announce with invalid metadata")
		return fmt.Errorf("register receiver mapping: %w", err)
	}
	return nil
}

// StartDownload begins downloading a previously announced file.
func (b *FileTransferBridge) StartDownload(fileID domain.FileID) error {
	return b.client.StartFileDownload(fileID)
}

// CancelDownload aborts an active download.
func (b *FileTransferBridge) CancelDownload(fileID domain.FileID) error {
	return b.client.CancelFileDownload(fileID)
}

// RestartDownload resets a failed download back to available for re-download.
func (b *FileTransferBridge) RestartDownload(fileID domain.FileID) error {
	return b.client.RestartFileDownload(fileID)
}

// Progress returns the transfer progress for a file.
func (b *FileTransferBridge) Progress(fileID domain.FileID, isSender bool) (bytesTransferred, totalSize uint64, state string, found bool) {
	return b.client.FileTransferProgress(fileID, isSender)
}

// AllTransfers returns every sender/receiver mapping (active and
// terminal) as typed snapshots. The desktop file tab uses this to
// render the full transfer history. Order is undefined — callers are
// expected to sort by created_at or any other field they need.
//
// Returns an empty non-nil slice when the file-transfer subsystem is
// not available (standalone-RPC desktop, embedded node not started).
func (b *FileTransferBridge) AllTransfers() []filetransfer.TransferSnapshot {
	if b.client == nil {
		return []filetransfer.TransferSnapshot{}
	}
	return b.client.AllFileTransfers()
}

// FilePath returns the on-disk path for a transferred file. For the sender
// it points to the transmit blob; for the receiver to the completed download.
// Returns empty string if the file is not available on disk.
func (b *FileTransferBridge) FilePath(fileID domain.FileID, isSender bool) string {
	return b.client.FileTransferFilePath(fileID, isSender)
}

// CleanupPeer removes all file transfer mappings for the given peer.
// Called when a peer is removed from the sidebar.
func (b *FileTransferBridge) CleanupPeer(peer domain.PeerIdentity) {
	b.client.CleanupPeerTransfers(peer)
}

// OnMessageDeleted is the cleanup hook invoked by DMRouter when a DM
// row has been removed from chatlog (locally on the sender side via
// SendMessageDelete, or remotely on the recipient side via
// handleInboundMessageDelete). For file_announce messages this drops
// the matching sender or receiver mapping, releases the transmit-blob
// ref count (sender side), and deletes partial / completed files in
// the download directory (receiver side).
//
// Idempotency contract: a MessageID with no associated mapping is a
// silent no-op inside FileTransferManager. The hook is safe to call
// for every deleted message regardless of whether it was a
// file_announce.
//
// FileID == MessageID by construction (see domain/file_transfer.go).
// The conversion is a typed-string alias change; no runtime cost.
func (b *FileTransferBridge) OnMessageDeleted(messageID domain.MessageID) {
	if b.client == nil {
		return
	}
	b.client.CleanupTransferByMessageID(domain.FileID(messageID))
}

// sendTimeout is the default timeout for sending a file announce DM.
const sendTimeout = 3 * time.Second
