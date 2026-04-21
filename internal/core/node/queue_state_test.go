package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
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
		RelayMessages: []protocol.Envelope{
			{
				ID:         protocol.MessageID("msg-1"),
				Topic:      "dm",
				Sender:     "sender-1",
				Recipient:  "recipient-1",
				Flag:       protocol.MessageFlag("sender-delete"),
				TTLSeconds: 0,
				Payload:    []byte("ciphertext"),
				CreatedAt:  now,
			},
		},
		RelayReceipts: []protocol.DeliveryReceipt{
			{
				MessageID:   protocol.MessageID("msg-2"),
				Sender:      "sender-2",
				Recipient:   "recipient-2",
				Status:      protocol.ReceiptStatusDelivered,
				DeliveredAt: now,
			},
		},
		OutboundState: map[string]outboundDelivery{
			"msg-1": {
				MessageID:     "msg-1",
				Recipient:     "recipient-1",
				Status:        "retrying",
				QueuedAt:      now,
				LastAttemptAt: now.Add(5 * time.Second),
				Retries:       3,
				Error:         "retry queued delivery",
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
	outbound, ok := got.OutboundState["msg-1"]
	if !ok {
		t.Fatalf("missing outbound state: %#v", got.OutboundState)
	}
	if outbound.Status != "retrying" || outbound.Retries != 3 {
		t.Fatalf("unexpected outbound state: %#v", outbound)
	}
	if len(got.RelayMessages) != 1 || got.RelayMessages[0].ID != protocol.MessageID("msg-1") {
		t.Fatalf("unexpected relay messages: %#v", got.RelayMessages)
	}
	if len(got.RelayReceipts) != 1 || got.RelayReceipts[0].MessageID != protocol.MessageID("msg-2") {
		t.Fatalf("unexpected relay receipts: %#v", got.RelayReceipts)
	}
}

// TestConcurrentSaveQueueStateProducesValidJSON verifies that overlapping
// saveQueueState calls to the same path never produce corrupted JSON.
// Before the atomic-rename fix, concurrent os.WriteFile calls could
// interleave, producing malformed output like two concatenated JSON objects.
func TestConcurrentSaveQueueStateProducesValidJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "queue.json")
	const goroutines = 10
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				state := queueStateFile{
					Version: queueStateVersion,
					Pending: map[string][]pendingFrame{
						fmt.Sprintf("10.0.0.%d:%d", id, i): {
							{
								Frame: protocol.Frame{
									Type: "send_message",
									ID:   fmt.Sprintf("msg-%d-%d", id, i),
								},
							},
						},
					},
					RelayRetry:    map[string]relayAttempt{},
					OutboundState: map[string]outboundDelivery{},
				}
				if err := saveQueueState(path, state); err != nil {
					t.Errorf("saveQueueState goroutine %d iter %d: %v", id, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// After all writes complete, the file must contain valid JSON.
	state, err := loadQueueState(path)
	if err != nil {
		t.Fatalf("loadQueueState after concurrent writes: %v", err)
	}
	if state.Version != queueStateVersion {
		t.Fatalf("expected version %d, got %d", queueStateVersion, state.Version)
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

// TestSaveQueueStateWrapsErrQueuePersistFailed verifies the sentinel contract:
// every non-nil error returned from saveQueueState must satisfy
// errors.Is(err, ErrQueuePersistFailed) so callers (the persister retry path,
// the graceful-shutdown error log, future telemetry) can branch on failure
// type without matching substrings in the message.  Driven by a read-only
// directory so the MkdirAll step fails deterministically without depending on
// disk-space or permission tricks that vary by OS.
func TestSaveQueueStateWrapsErrQueuePersistFailed(t *testing.T) {
	t.Parallel()

	readOnlyDir := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(readOnlyDir, 0o500); err != nil {
		t.Fatalf("mkdir read-only parent: %v", err)
	}
	// Attempt to create a queue file beneath the read-only parent.  MkdirAll
	// for the nested directory must fail, producing a wrapped sentinel.
	target := filepath.Join(readOnlyDir, "nested", "queue.json")

	err := saveQueueState(target, queueStateFile{Version: queueStateVersion})
	if err == nil {
		t.Fatal("expected error when creating queue file under read-only directory")
	}
	if !errors.Is(err, ErrQueuePersistFailed) {
		t.Fatalf("expected errors.Is(err, ErrQueuePersistFailed) to be true, got err=%v", err)
	}
}
