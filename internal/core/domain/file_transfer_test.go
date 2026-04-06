package domain

import (
	"encoding/json"
	"testing"
)

func TestFileActionValid(t *testing.T) {
	t.Parallel()

	validActions := []FileAction{
		FileActionAnnounce,
		FileActionChunkReq,
		FileActionChunkResp,
		FileActionDownloaded,
		FileActionDownloadedAck,
	}
	for _, a := range validActions {
		if !a.Valid() {
			t.Errorf("FileAction(%q).Valid() = false, want true", a)
		}
	}

	invalidActions := []FileAction{"", "unknown", "file_cancel", "CHUNK_REQUEST"}
	for _, a := range invalidActions {
		if a.Valid() {
			t.Errorf("FileAction(%q).Valid() = true, want false", a)
		}
	}
}

func TestFileActionIsProtocolCommand(t *testing.T) {
	t.Parallel()

	protocolCommands := []FileAction{
		FileActionChunkReq,
		FileActionChunkResp,
		FileActionDownloaded,
		FileActionDownloadedAck,
	}
	for _, a := range protocolCommands {
		if !a.IsProtocolCommand() {
			t.Errorf("FileAction(%q).IsProtocolCommand() = false, want true", a)
		}
	}

	// file_announce is NOT a protocol command — it uses the DM pipeline.
	if FileActionAnnounce.IsProtocolCommand() {
		t.Error("FileActionAnnounce.IsProtocolCommand() = true, want false")
	}

	// Unknown actions are not protocol commands.
	if FileAction("unknown").IsProtocolCommand() {
		t.Error("unknown action should not be a protocol command")
	}
}

func TestFileAnnouncePayloadJSON(t *testing.T) {
	t.Parallel()

	payload := FileAnnouncePayload{
		FileName:    "document.pdf",
		FileSize:    1048576,
		ContentType: "application/pdf",
		FileHash:    "abc123def456",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FileAnnouncePayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.FileName != payload.FileName {
		t.Errorf("FileName = %q, want %q", decoded.FileName, payload.FileName)
	}
	if decoded.FileSize != payload.FileSize {
		t.Errorf("FileSize = %d, want %d", decoded.FileSize, payload.FileSize)
	}
	if decoded.ContentType != payload.ContentType {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, payload.ContentType)
	}
	if decoded.FileHash != payload.FileHash {
		t.Errorf("FileHash = %q, want %q", decoded.FileHash, payload.FileHash)
	}
}

func TestFileCommandPayloadJSON(t *testing.T) {
	t.Parallel()

	inner, _ := json.Marshal(ChunkRequestPayload{
		FileID: "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
		Offset: 16384,
		Size:   16384,
	})

	payload := FileCommandPayload{
		Command: FileActionChunkReq,
		Data:    inner,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded FileCommandPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Command != FileActionChunkReq {
		t.Errorf("Command = %q, want %q", decoded.Command, FileActionChunkReq)
	}

	var req ChunkRequestPayload
	if err := json.Unmarshal(decoded.Data, &req); err != nil {
		t.Fatalf("unmarshal chunk request: %v", err)
	}
	if req.Offset != 16384 {
		t.Errorf("Offset = %d, want 16384", req.Offset)
	}
}
