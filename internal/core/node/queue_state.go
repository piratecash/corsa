package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"corsa/internal/core/protocol"
)

type queueStateFile struct {
	Pending       map[string][]pendingFrame   `json:"pending,omitempty"`
	RelayRetry    map[string]relayAttempt     `json:"relay_retry,omitempty"`
	RelayMessages []protocol.Envelope         `json:"relay_messages,omitempty"`
	RelayReceipts []protocol.DeliveryReceipt  `json:"relay_receipts,omitempty"`
	OutboundState map[string]outboundDelivery `json:"outbound_state,omitempty"`
}

func loadQueueState(path string) (queueStateFile, error) {
	state := queueStateFile{
		Pending:       map[string][]pendingFrame{},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	if path == "" {
		return state, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read queue state %s: %w", path, err)
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode queue state %s: %w", path, err)
	}
	if state.Pending == nil {
		state.Pending = map[string][]pendingFrame{}
	}
	if state.RelayRetry == nil {
		state.RelayRetry = map[string]relayAttempt{}
	}
	if state.OutboundState == nil {
		state.OutboundState = map[string]outboundDelivery{}
	}
	return state, nil
}

func saveQueueState(path string, state queueStateFile) error {
	if path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create queue state directory: %w", err)
	}

	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal queue state: %w", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		return fmt.Errorf("write queue state: %w", err)
	}
	return nil
}
