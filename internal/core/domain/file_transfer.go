package domain

import (
	"encoding/json"
	"time"
)

// FileID identifies a file transfer event. It equals the MessageID of the
// file_announce DM that initiated the transfer. Two sends of the same file
// produce two different FileIDs (but the same FileHash).
type FileID = MessageID

// FileDMBodySentinel is the placeholder body text used in file_announce DMs
// when the sender provides no caption. It satisfies the non-empty body
// validation without carrying user-visible content.
const FileDMBodySentinel = "[file]"

// FileTransferTopic is the logical topic for file command frames.
// File commands are never stored in chatlog and never enter the DM pipeline.
const FileTransferTopic = "file"

// TransmitSubdir is the subdirectory name under the data dir where files
// awaiting transfer are stored. Content-addressed by SHA-256 hash.
const TransmitSubdir = "transmit"

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

const (
	// FileCommandMaxTTL is the upper bound for TTL in a FileCommandFrame.
	// Larger values are clamped or rejected.
	FileCommandMaxTTL uint8 = 255

	// FileCommandMaxClockDrift is the maximum acceptable difference (seconds)
	// between the frame timestamp and the receiver's local clock. Frames
	// outside this window are dropped as stale or future-dated.
	FileCommandMaxClockDrift = 300 // 5 minutes

	// FileCommandMinPeerProtocolVersion is the lowest protocol_version a
	// peer must advertise for this node to participate in file_command
	// traffic with it on either side. The wire frame carries SrcPubKey
	// for self-contained authenticity (see docs/protocol/file_transfer.md
	// "Authenticity vs authorization"); a peer reporting below this floor
	// predates the SrcPubKey field and either emits a frame the v2 router
	// drops on missing-pubkey, or — if it is the relay between two v2
	// endpoints — drops the v2 frame on its own trust-store-only pubkey
	// lookup, which is the original production failure that motivated the
	// cutover.
	//
	// Both directions are gated:
	//   - send: peerSendableConnectionsLocked filters direct sessions on
	//     the raw peerSession.version, and Router.collectRouteCandidates
	//     filters route-table next-hops on PeerRouteMeta.RawProtocolVersion;
	//   - receive: dispatchNetworkFrame rejects file_command from inbound
	//     connections whose negotiated protocol_version is below the floor.
	//
	// Both sides reject a raw/unknown version of 0 — there is no positive
	// evidence the peer speaks the v2 SrcPubKey wire format, and admitting
	// such a frame re-opens the same v11-relay attack surface the cutover
	// exists to close.
	//
	// The only soft-demote case is "peer reported v > config.ProtocolVersion
	// (newer than this build — either a staged-rollout upgrade or an
	// inflation attack)": the route-meta helper caps
	// PeerRouteMeta.ProtocolVersion at config.ProtocolVersion but keeps
	// RawProtocolVersion at the actual reported value. Eligibility is
	// then decided by RawProtocolVersion (>= 12 → admit), and ranking
	// is decided by ProtocolVersion (capped at local, so the inflation
	// lie cannot WIN the primary key over a legitimate v=local peer).
	// The cap leaves the upgraded peer in the equal-version tier where
	// hops/uptime decide — earlier behaviour clamped to 0 instead and
	// starved every legitimate upgraded peer of file traffic.
	//
	// This value is the temporary bridge until MinimumProtocolVersion is
	// officially raised to 12 globally; see
	// docs/file-transfer-pubkey-gate-minproto12-cleanup.md for the removal
	// plan.
	FileCommandMinPeerProtocolVersion = 12

	// FileCommandNonceCacheSize is the capacity of the anti-replay nonce
	// cache. When full, the oldest entries are evicted (LRU).
	FileCommandNonceCacheSize = 10_000

	// FileCommandNonceTTL is the duration a nonce stays in the anti-replay
	// cache before automatic expiry.
	FileCommandNonceTTL = 300 * time.Second

	// DefaultChunkSize is the default requested chunk size in bytes.
	// Derived from wire budget (65536) minus header/encryption/base64 overhead.
	DefaultChunkSize uint32 = 16_384 // 16 KB
)

// ---------------------------------------------------------------------------
// File actions (command types inside encrypted payload)
// ---------------------------------------------------------------------------

// FileAction identifies the command type inside the encrypted payload of a
// FileCommandFrame. Transit nodes never see this value — only cleartext
// SRC/DST/TTL/Time/Nonce/Signature are visible.
//
// FileAction now narrowly identifies file-transfer protocol frames that
// travel inside FileCommandFrame. The file announcement that opens a
// transfer is *not* a FileAction — it is a DM and uses
// DMCommandFileAnnounce. See docs/dm-commands.md.
type FileAction string

const (
	// FileActionChunkReq is sent by the receiver to request a chunk.
	FileActionChunkReq FileAction = "chunk_request"

	// FileActionChunkResp is sent by the sender with chunk data.
	FileActionChunkResp FileAction = "chunk_response"

	// FileActionDownloaded is sent by the receiver after successful
	// SHA-256 verification of the complete file.
	FileActionDownloaded FileAction = "file_downloaded"

	// FileActionDownloadedAck is the sender's acknowledgement of
	// file_downloaded, allowing the receiver to stop resending.
	FileActionDownloadedAck FileAction = "file_downloaded_ack"
)

// Valid returns true if the action is a recognised file command.
func (a FileAction) Valid() bool {
	switch a {
	case FileActionChunkReq, FileActionChunkResp,
		FileActionDownloaded, FileActionDownloadedAck:
		return true
	default:
		return false
	}
}

// IsProtocolCommand returns true if the action uses the FileCommandFrame
// wire format. Every FileAction now identifies a protocol command — the
// historical file_announce action lived in this enum and was *not* a
// protocol command, but it has migrated to DMCommandFileAnnounce.
func (a FileAction) IsProtocolCommand() bool {
	return a.Valid()
}

// ---------------------------------------------------------------------------
// file_announce payload (carried inside DM PlainMessage.CommandData)
// ---------------------------------------------------------------------------

// FileAnnouncePayload is the metadata attached to a file_announce DM.
// It is stored in the PlainMessage.CommandData field inside the encrypted
// envelope, so transit nodes never see it.
type FileAnnouncePayload struct {
	FileName    string `json:"file_name"`
	FileSize    uint64 `json:"file_size"`
	ContentType string `json:"content_type"`
	FileHash    string `json:"file_hash"` // hex-encoded SHA-256 of file content
}

// ---------------------------------------------------------------------------
// File command payloads (inside FileCommandFrame encrypted Payload)
// ---------------------------------------------------------------------------

// FileCommandPayload is the top-level structure inside the encrypted payload
// of a FileCommandFrame. The Command field identifies the action; Data holds
// the action-specific JSON (ChunkRequestPayload, ChunkResponsePayload, etc.).
type FileCommandPayload struct {
	Command FileAction      `json:"command"`
	Data    json.RawMessage `json:"data"`
}

// ChunkRequestPayload is sent by the receiver to request a specific chunk.
type ChunkRequestPayload struct {
	FileID FileID `json:"file_id"`
	Offset uint64 `json:"offset"`
	Size   uint32 `json:"size"`
}

// ChunkResponsePayload is sent by the sender with the requested chunk data.
//
// Epoch carries the sender's current serving epoch — a monotonic counter on
// the sender's mapping that is bumped on every genuine transition from a
// non-serving state into senderServing. Re-downloads (senderCompleted →
// senderServing for the same FileID) bump the epoch so the receiver can
// distinguish the new serving run from stale traffic belonging to a prior
// completed run. A zero Epoch means the sender is pre-epoch (legacy) and
// the receiver should not gate on it.
type ChunkResponsePayload struct {
	FileID FileID `json:"file_id"`
	Offset uint64 `json:"offset"`
	Data   string `json:"data"`            // base64-encoded chunk (max DefaultChunkSize raw bytes)
	Epoch  uint64 `json:"epoch,omitempty"` // sender serving epoch; 0 means legacy/unset
}

// FileDownloadedPayload is sent by the receiver after SHA-256 verification.
//
// Epoch echoes back the most recent sender serving epoch observed in a
// chunk_response for this FileID. The sender uses it to reject stale replays
// from a previous completed serving run of the same FileID: if a delayed
// file_downloaded carries an epoch that no longer matches the sender's
// current ServingEpoch, the message is dropped. A zero Epoch is treated as
// legacy (pre-upgrade receiver) and skips the epoch check for backwards
// compatibility during rolling upgrades.
type FileDownloadedPayload struct {
	FileID FileID `json:"file_id"`
	Epoch  uint64 `json:"epoch,omitempty"` // echoed sender serving epoch; 0 means legacy/unset
}

// FileDownloadedAckPayload is the sender's acknowledgement of file_downloaded.
//
// Epoch echoes back the epoch from the file_downloaded that this ack
// completes. A receiver that has already advanced to a newer serving epoch
// (via a cancel + restart cycle) can use this to ignore a late ack belonging
// to a prior run, preventing a stale ack from flipping an active download to
// completed. The struct is layout-compatible with FileDownloadedPayload so
// the sender can construct the ack via type conversion.
type FileDownloadedAckPayload struct {
	FileID FileID `json:"file_id"`
	Epoch  uint64 `json:"epoch,omitempty"` // echoed sender serving epoch; 0 means legacy/unset
}
