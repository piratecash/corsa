package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/protocol"
)

func TestLoadQueueStateReturnsEmptyForMissingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing-queue.json")
	state, err := loadQueueState(path)
	if err != nil {
		t.Fatalf("loadQueueState missing file: %v", err)
	}
	if len(state.Pending) != 0 {
		t.Fatalf("expected empty pending map, got %#v", state.Pending)
	}
	if len(state.RelayRetry) != 0 {
		t.Fatalf("expected empty relay retry map, got %#v", state.RelayRetry)
	}
}

func TestSaveAndLoadQueueStateRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "queue.json")
	now := time.Now().UTC().Truncate(time.Second)
	want := queueStateFile{
		Pending: map[string][]pendingFrame{
			"127.0.0.1:64646": {
				{
					Frame: protocol.Frame{
						Type:      "send_message",
						Topic:     "dm",
						ID:        "msg-1",
						Recipient: "recipient-1",
						Body:      "ciphertext",
					},
					QueuedAt: now,
					Retries:  2,
				},
			},
		},
		RelayRetry: map[string]relayAttempt{
			relayMessageKey(protocol.MessageID("msg-1")): {
				FirstSeen:   now,
				LastAttempt: now.Add(5 * time.Second),
				Attempts:    3,
			},
		},
	}

	if err := saveQueueState(path, want); err != nil {
		t.Fatalf("saveQueueState: %v", err)
	}

	got, err := loadQueueState(path)
	if err != nil {
		t.Fatalf("loadQueueState: %v", err)
	}
	if len(got.Pending) != 1 {
		t.Fatalf("expected 1 pending entry, got %#v", got.Pending)
	}
	items := got.Pending["127.0.0.1:64646"]
	if len(items) != 1 {
		t.Fatalf("expected 1 pending frame, got %#v", items)
	}
	if items[0].Frame.ID != "msg-1" || items[0].Retries != 2 || !items[0].QueuedAt.Equal(now) {
		t.Fatalf("unexpected pending frame: %#v", items[0])
	}

	state, ok := got.RelayRetry[relayMessageKey(protocol.MessageID("msg-1"))]
	if !ok {
		t.Fatalf("missing relay retry state: %#v", got.RelayRetry)
	}
	if state.Attempts != 3 || !state.FirstSeen.Equal(now) {
		t.Fatalf("unexpected relay retry state: %#v", state)
	}
}

func TestLoadQueueStateRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "queue.json")
	if err := os.WriteFile(path, []byte("{broken"), 0o600); err != nil {
		t.Fatalf("write invalid queue file: %v", err)
	}

	if _, err := loadQueueState(path); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}
