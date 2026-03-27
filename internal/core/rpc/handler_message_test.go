package rpc_test

import (
	"corsa/internal/core/protocol"
	"testing"
)

func TestMessageFetchMessagesValidTopic(t *testing.T) {
	messages := []protocol.MessageFrame{
		{
			ID:        "msg-1",
			Sender:    "sender-1",
			Recipient: "recipient-1",
			Body:      "test message 1",
			CreatedAt: "2026-03-26T10:00:00Z",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_messages" && frame.Topic != "" {
				return protocol.Frame{
					Type:     "messages_response",
					Topic:    frame.Topic,
					Messages: messages,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{
		"topic": "global",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "messages_response")
	expectField(t, result, "topic", "global")

	msgs, ok := result["messages"].([]interface{})
	if !ok {
		t.Errorf("expected messages to be array, got %T", result["messages"])
		return
	}
	if len(msgs) != len(messages) {
		t.Errorf("expected %d messages, got %d", len(messages), len(msgs))
	}
}

func TestMessageFetchMessagesMissingTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessagesEmptyTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{
		"topic": "",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessagesWithPagination(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_messages" {
				return protocol.Frame{
					Type:     "messages_response",
					Topic:    frame.Topic,
					Limit:    frame.Limit,
					Count:    frame.Count,
					Messages: []protocol.MessageFrame{},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{
		"topic":  "dm",
		"limit":  10,
		"offset": 5,
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "messages_response")
}

func TestMessageFetchMessageIDsValidTopic(t *testing.T) {
	ids := []string{"msg-1", "msg-2", "msg-3"}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_message_ids" && frame.Topic != "" {
				return protocol.Frame{
					Type:  "message_ids_response",
					Topic: frame.Topic,
					IDs:   ids,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/ids", map[string]interface{}{
		"topic": "global",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "message_ids_response")

	idsResult, ok := result["ids"].([]interface{})
	if !ok {
		t.Errorf("expected ids to be array, got %T", result["ids"])
		return
	}
	if len(idsResult) != len(ids) {
		t.Errorf("expected %d ids, got %d", len(ids), len(idsResult))
	}
}

func TestMessageFetchMessageIDsMissingTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/ids", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessage(t *testing.T) {
	msgData := &protocol.MessageFrame{
		ID:        "msg-abc123",
		Sender:    "sender-addr",
		Recipient: "recipient-addr",
		Body:      "test message content",
		CreatedAt: "2026-03-26T10:00:00Z",
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_message" && frame.Topic != "" && frame.ID != "" {
				return protocol.Frame{
					Type:  "message_response",
					Topic: frame.Topic,
					ID:    frame.ID,
					Item:  msgData,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/get", map[string]interface{}{
		"topic": "dm",
		"id":    "msg-abc123",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "message_response")
	expectField(t, result, "topic", "dm")
	expectField(t, result, "id", "msg-abc123")
	expectFieldExists(t, result, "item")
}

func TestMessageFetchMessageMissingTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/get", map[string]interface{}{
		"id": "msg-123",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessageMissingID(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/get", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchInboxValidRequest(t *testing.T) {
	messages := []protocol.MessageFrame{
		{
			ID:        "msg-1",
			Sender:    "sender-addr",
			Recipient: "recipient-addr",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_inbox" {
				return protocol.Frame{
					Type:      "inbox_response",
					Topic:     frame.Topic,
					Recipient: frame.Recipient,
					Messages:  messages,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/inbox", map[string]interface{}{
		"topic":     "dm",
		"recipient": "recipient-addr",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "inbox_response")
	expectField(t, result, "recipient", "recipient-addr")
}

func TestMessageFetchInboxMissingTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/inbox", map[string]interface{}{
		"recipient": "recipient-addr",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchInboxDefaultRecipient(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_inbox" {
				return protocol.Frame{
					Type:      "inbox_response",
					Topic:     frame.Topic,
					Recipient: frame.Recipient,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/inbox", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "inbox_response")
	expectField(t, result, "recipient", "test-address-abc123")
}

func TestMessageFetchPendingMessages(t *testing.T) {
	pending := []protocol.PendingMessageFrame{
		{
			ID:        "pending-1",
			Recipient: "recipient-addr",
			Status:    "queued",
			QueuedAt:  "2026-03-26T10:00:00Z",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_pending_messages" {
				return protocol.Frame{
					Type:            "pending_messages_response",
					Topic:           frame.Topic,
					PendingMessages: pending,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/pending", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "pending_messages_response")
	expectFieldExists(t, result, "pending_messages")
}

func TestMessageFetchPendingMessagesMissingTopic(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/pending", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchDeliveryReceipts(t *testing.T) {
	receipts := []protocol.ReceiptFrame{
		{
			MessageID:   "msg-1",
			Recipient:   "recipient-addr",
			DeliveredAt: "2026-03-26T10:00:00Z",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_delivery_receipts" {
				return protocol.Frame{
					Type:      "receipts_response",
					Recipient: frame.Recipient,
					Receipts:  receipts,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/receipts", map[string]interface{}{
		"recipient": "recipient-addr",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "receipts_response")
	expectFieldExists(t, result, "receipts")
}

func TestMessageFetchDeliveryReceiptsDefaultRecipient(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_delivery_receipts" {
				return protocol.Frame{
					Type:      "receipts_response",
					Recipient: frame.Recipient,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/receipts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "receipts_response")
	expectField(t, result, "recipient", "test-address-abc123")
}

func TestMessageFetchMessagesNodeError(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_messages" {
				return protocol.Frame{
					Type:  "error",
					Error: "topic not found",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{
		"topic": "nonexistent",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
}

func TestMessageFetchDMHeaders(t *testing.T) {
	headers := []protocol.DMHeaderFrame{
		{
			ID:        "dm-1",
			Sender:    "sender-addr",
			Recipient: "recipient-addr",
			CreatedAt: "2026-03-26T10:00:00Z",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_dm_headers" {
				return protocol.Frame{
					Type:      "dm_headers_response",
					DMHeaders: headers,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/dm_headers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "dm_headers_response")
	expectFieldExists(t, result, "dm_headers")

	dmHeaders, ok := result["dm_headers"].([]interface{})
	if !ok {
		t.Fatalf("expected dm_headers to be array, got %T", result["dm_headers"])
	}
	if len(dmHeaders) != 1 {
		t.Errorf("expected 1 dm header, got %d", len(dmHeaders))
	}
}

func TestMessageFetchDMHeadersEmpty(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_dm_headers" {
				return protocol.Frame{
					Type: "dm_headers_response",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/dm_headers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "dm_headers_response")
}

func TestMessageSendDMNilRouter(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   "recipient-addr",
		"body": "hello",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMMissingTo(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"body": "hello",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMMissingBody(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to": "recipient-addr",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMSuccess(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   "peer-addr",
		"body": "hello world",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "queued")
	expectField(t, result, "to", "peer-addr")

	if dmRouter.lastTo != "peer-addr" {
		t.Errorf("expected dmRouter.lastTo = %q, got %q", "peer-addr", dmRouter.lastTo)
	}
	if dmRouter.lastBody != "hello world" {
		t.Errorf("expected dmRouter.lastBody = %q, got %q", "hello world", dmRouter.lastBody)
	}
}

func TestMessageCommandsHiddenWithoutDMRouter(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil) // dmRouter=nil

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array, got %T", result["commands"])
	}

	foundDMHeaders := false
	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := cmdMap["name"].(string)
		// send_dm requires DMRouter — should be hidden
		if name == "send_dm" {
			t.Errorf("command %q should be hidden when dmRouter is nil", name)
		}
		// fetch_dm_headers uses only NodeProvider — should be visible
		if name == "fetch_dm_headers" {
			foundDMHeaders = true
		}
	}
	if !foundDMHeaders {
		t.Error("fetch_dm_headers should be visible even without dmRouter (uses only NodeProvider)")
	}
}

func TestMessageCommandsVisibleWithDMRouter(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array, got %T", result["commands"])
	}

	found := map[string]bool{"send_dm": false, "fetch_dm_headers": false}
	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := cmdMap["name"].(string)
		if _, exists := found[name]; exists {
			found[name] = true
		}
	}

	for name, visible := range found {
		if !visible {
			t.Errorf("command %q should be visible when dmRouter is present", name)
		}
	}
}
