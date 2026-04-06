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
type FileAction string

const (
	// FileActionAnnounce is sent as a DM (not a FileCommandFrame).
	// Listed here for reference completeness; it uses the DM wire format.
	FileActionAnnounce FileAction = "file_announce"

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
	case FileActionAnnounce, FileActionChunkReq, FileActionChunkResp,
		FileActionDownloaded, FileActionDownloadedAck:
		return true
	default:
		return false
	}
}

// IsProtocolCommand returns true if the action uses the FileCommandFrame
// wire format (not the DM pipeline). file_announce is a DM, everything
// else is a protocol command.
func (a FileAction) IsProtocolCommand() bool {
	switch a {
	case FileActionChunkReq, FileActionChunkResp,
		FileActionDownloaded, FileActionDownloadedAck:
		return true
	default:
		return false
	}
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
type ChunkResponsePayload struct {
	FileID FileID `json:"file_id"`
	Offset uint64 `json:"offset"`
	Data   string `json:"data"` // base64-encoded chunk (max DefaultChunkSize raw bytes)
}

// FileDownloadedPayload is sent by the receiver after SHA-256 verification.
type FileDownloadedPayload struct {
	FileID FileID `json:"file_id"`
}

// FileDownloadedAckPayload is the sender's acknowledgement of file_downloaded.
type FileDownloadedAckPayload struct {
	FileID FileID `json:"file_id"`
}
