package rpc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/piratecash/corsa/internal/core/service"
)

// validTo is a well-formed 40-char lowercase-hex peer identity. The send_dm /
// send_file_announce RPC boundary now parses the recipient with
// domain.ParsePeerIdentity, so tests that expect to reach past that boundary
// (success, 503, reply_to validation) must supply a parseable address.
const validTo = "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"

// zeroIdentity is the all-zero 40-char lowercase-hex peer identity. It parses
// with a NIL error through domain.ParsePeerIdentity (only the empty string is
// the no-op case), so the identity-taking RPC boundaries must reject it with an
// explicit IsZero gate rather than letting the "absent" sentinel through as a
// required peer.
const zeroIdentity = "0000000000000000000000000000000000000000"

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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_messages" && frame.Topic != "" {
			return protocol.Frame{
				Type:     "messages_response",
				Topic:    frame.Topic,
				Messages: messages,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessagesEmptyTopic(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/list", map[string]interface{}{
		"topic": "",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessagesWithPagination(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
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
	})
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_message_ids" && frame.Topic != "" {
			return protocol.Frame{
				Type:  "message_ids_response",
				Topic: frame.Topic,
				IDs:   ids,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newDefaultNodeProvider(t)
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_message" && frame.Topic != "" && frame.ID != "" {
			return protocol.Frame{
				Type:  "message_response",
				Topic: frame.Topic,
				ID:    frame.ID,
				Item:  msgData,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/get", map[string]interface{}{
		"id": "msg-123",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchMessageMissingID(t *testing.T) {
	node := newDefaultNodeProvider(t)
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_inbox" {
			return protocol.Frame{
				Type:      "inbox_response",
				Topic:     frame.Topic,
				Recipient: frame.Recipient,
				Messages:  messages,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/inbox", map[string]interface{}{
		"recipient": "recipient-addr",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageFetchInboxDefaultRecipient(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_inbox" {
			return protocol.Frame{
				Type:      "inbox_response",
				Topic:     frame.Topic,
				Recipient: frame.Recipient,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_pending_messages" {
			return protocol.Frame{
				Type:            "pending_messages_response",
				Topic:           frame.Topic,
				PendingMessages: pending,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/pending", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "pending_messages_response")
	expectFieldExists(t, result, "pending_messages")
}

func TestMessageFetchPendingMessagesMissingTopic(t *testing.T) {
	node := newDefaultNodeProvider(t)
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_delivery_receipts" {
			return protocol.Frame{
				Type:      "receipts_response",
				Recipient: frame.Recipient,
				Receipts:  receipts,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/receipts", map[string]interface{}{
		"recipient": "recipient-addr",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "receipts_response")
	expectFieldExists(t, result, "receipts")
}

func TestMessageFetchDeliveryReceiptsDefaultRecipient(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_delivery_receipts" {
			return protocol.Frame{
				Type:      "receipts_response",
				Recipient: frame.Recipient,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/receipts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "receipts_response")
	expectField(t, result, "recipient", "test-address-abc123")
}

func TestMessageFetchMessagesNodeError(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_messages" {
			return protocol.Frame{
				Type:  "error",
				Error: "topic not found",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_dm_headers" {
			return protocol.Frame{
				Type:      "dm_headers_response",
				DMHeaders: headers,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
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
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_dm_headers" {
			return protocol.Frame{
				Type: "dm_headers_response",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/dm_headers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "dm_headers_response")
}

func TestMessageSendDMNilRouter(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   "recipient-addr",
		"body": "hello",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMMissingTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"body": "hello",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

// TestMessageSendDMRejectsMalformedTo pins the RPC boundary: a non-empty but
// non-hex recipient must be rejected synchronously with 400 BEFORE the message
// is enqueued. Previously such an address was coerced to the zero identity via
// PeerIdentityFromWire and the RPC returned "pending" while the real error
// surfaced asynchronously.
func TestMessageSendDMRejectsMalformedTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   "not-hex",
		"body": "hello",
	})

	expectStatusCode(t, code, 400)
	if errMsg, _ := result["error"].(string); !strings.Contains(errMsg, "valid peer identity") {
		t.Errorf("expected peer-identity validation error, got %q", errMsg)
	}

	// The malformed address must not be enqueued.
	dmRouter.AssertNotCalled(t, "SendMessage")
}

// TestMessageSendDMRejectsZeroIdentityTo pins the second half of the boundary:
// the all-zero 40-hex address parses with a nil error through
// domain.ParsePeerIdentity, so without the explicit IsZero gate the absent
// sentinel would slip through as a real recipient. It must be rejected with 400
// BEFORE the message is enqueued.
func TestMessageSendDMRejectsZeroIdentityTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   zeroIdentity,
		"body": "hello",
	})

	expectStatusCode(t, code, 400)
	if errMsg, _ := result["error"].(string); !strings.Contains(errMsg, "zero peer identity") {
		t.Errorf("expected zero-identity validation error, got %q", errMsg)
	}

	// The zero identity must not be enqueued.
	dmRouter.AssertNotCalled(t, "SendMessage")
}

func TestMessageSendDMMissingBody(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to": validTo,
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMSuccess(t *testing.T) {
	node := newDefaultNodeProvider(t)

	var capturedTo domain.PeerIdentity
	var capturedMsg domain.OutgoingDM
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedTo = args.Get(0).(domain.PeerIdentity)
			capturedMsg = args.Get(1).(domain.OutgoingDM)
		}).Return(nil)
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   validTo,
		"body": "hello world",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "pending")
	expectField(t, result, "to", validTo)

	if capturedTo.String() != validTo {
		t.Errorf("expected capturedTo = %q, got %q", validTo, capturedTo)
	}
	if string(capturedMsg.Body) != "hello world" {
		t.Errorf("expected capturedMsg.Body = %q, got %q", "hello world", capturedMsg.Body)
	}
}

// TestMessageSendDMReturns503WhenWipePending pins the
// outgoing-barrier mapping: when SendMessage rejects with
// service.ErrConversationDeleteInflight (a wipe is in flight for
// the peer), the RPC must surface 503 Service Unavailable rather
// than 400 Bad Request — the input is well-formed, the server is
// just temporarily refusing the send. RPC clients use the status
// code to decide whether to retry vs. fix the request.
func TestMessageSendDMReturns503WhenWipePending(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).
		Return(service.ErrConversationDeleteInflight)
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   validTo,
		"body": "hello",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestMessageSendDMRejectsNonStringReplyTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	// reply_to as number — must be rejected, not silently dropped.
	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": 123,
	})

	expectStatusCode(t, code, 400)
	if errMsg, _ := result["error"].(string); !strings.Contains(errMsg, "reply_to must be a string") {
		t.Errorf("expected validation error about reply_to, got %q", errMsg)
	}

	// reply_to as bool — same rejection.
	code2, result2 := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": true,
	})

	expectStatusCode(t, code2, 400)
	if errMsg, _ := result2["error"].(string); !strings.Contains(errMsg, "reply_to must be a string") {
		t.Errorf("expected validation error about reply_to, got %q", errMsg)
	}
}

func TestMessageSendDMRejectsInvalidUUIDReplyTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	// reply_to as string but not UUID v4 — must be rejected synchronously.
	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": "not-a-uuid",
	})

	expectStatusCode(t, code, 400)
	if errMsg, _ := result["error"].(string); !strings.Contains(errMsg, "valid message ID") {
		t.Errorf("expected UUID validation error, got %q", errMsg)
	}

	// Verify dmRouter was never called — the request should not be enqueued.
	dmRouter.AssertNotCalled(t, "SendMessage")
}

func TestMessageSendDMAcceptsValidUUIDReplyTo(t *testing.T) {
	node := newDefaultNodeProvider(t)

	var capturedMsg domain.OutgoingDM
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedMsg = args.Get(1).(domain.OutgoingDM)
		}).Return(nil)
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Without chatlog, existence check is skipped — format-valid UUID is accepted.
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "pending")
	if string(capturedMsg.ReplyTo) != "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" {
		t.Errorf("expected reply_to forwarded, got %q", capturedMsg.ReplyTo)
	}
}

func TestMessageSendDMRejectsDanglingReplyToWithChatlog(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)

	knownEntries := map[string]bool{} // empty — no messages exist
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.EXPECT().HasEntryInConversation(mock.Anything, mock.Anything).
		RunAndReturn(func(peerAddress, messageID string) bool {
			return knownEntries[peerAddress+":"+messageID]
		})

	server := setupTestServerWithDMRouterAndChatlog(t, node, chatlog, dmRouter)

	// Valid UUID format but does not exist in conversation — must be rejected.
	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	})

	expectStatusCode(t, code, 400)
	if errMsg, _ := result["error"].(string); !strings.Contains(errMsg, "does not exist") {
		t.Errorf("expected existence validation error, got %q", errMsg)
	}
	dmRouter.AssertNotCalled(t, "SendMessage")
}

func TestMessageSendDMAcceptsExistingReplyToWithChatlog(t *testing.T) {
	node := newDefaultNodeProvider(t)

	var capturedMsg domain.OutgoingDM
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedMsg = args.Get(1).(domain.OutgoingDM)
		}).Return(nil)
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	knownEntries := map[string]bool{
		validTo + ":a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5": true,
	}
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.EXPECT().HasEntryInConversation(mock.Anything, mock.Anything).
		RunAndReturn(func(peerAddress, messageID string) bool {
			return knownEntries[peerAddress+":"+messageID]
		})

	server := setupTestServerWithDMRouterAndChatlog(t, node, chatlog, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":       validTo,
		"body":     "hello",
		"reply_to": "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "pending")
	if string(capturedMsg.ReplyTo) != "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" {
		t.Errorf("expected reply_to forwarded, got %q", capturedMsg.ReplyTo)
	}
}

func TestMessageSendDMAcceptsEmptyReplyTo(t *testing.T) {
	node := newDefaultNodeProvider(t)

	var capturedMsg domain.OutgoingDM
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedMsg = args.Get(1).(domain.OutgoingDM)
		}).Return(nil)
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	// Omitted reply_to — should succeed normally.
	code, result := postJSON(t, server, "/rpc/v1/message/send_dm", map[string]interface{}{
		"to":   validTo,
		"body": "hello",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "pending")
	if string(capturedMsg.ReplyTo) != "" {
		t.Errorf("expected empty reply_to, got %q", capturedMsg.ReplyTo)
	}
}

// validMessageID is a well-formed UUID v4 used by deleteDm tests that need to
// reach past the message_id validation to assert peer-identity handling.
const validMessageID = "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"

// TestMessageDeleteDMRejectsMalformedPeer pins the deleteDm boundary: a
// non-empty but non-hex peer must be rejected synchronously with
// ErrValidation BEFORE SendMessageDelete runs, instead of being coerced to the
// zero identity.
func TestMessageDeleteDMRejectsMalformedPeer(t *testing.T) {
	dmRouter := newDefaultDMRouterProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterMessageCommands(table, newDefaultNodeProvider(t), dmRouter, nil)

	resp := table.Execute(rpc.CommandRequest{
		Name: "deleteDm",
		Args: map[string]interface{}{
			"peer":       "not-hex",
			"message_id": validMessageID,
		},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Errorf("expected ErrValidation for malformed peer, got %v", resp.ErrorKind)
	}
	if resp.Error == nil || !strings.Contains(resp.Error.Error(), "valid peer identity") {
		t.Errorf("expected peer-identity validation error, got %v", resp.Error)
	}
	dmRouter.AssertNotCalled(t, "SendMessageDelete")
}

// TestMessageDeleteDMRejectsZeroIdentityPeer pins the second half of the
// deleteDm boundary: the all-zero 40-hex peer parses with a nil error through
// domain.ParsePeerIdentity, so without the explicit IsZero gate the absent
// sentinel would slip through. It must be rejected with ErrValidation BEFORE
// SendMessageDelete runs.
func TestMessageDeleteDMRejectsZeroIdentityPeer(t *testing.T) {
	dmRouter := newDefaultDMRouterProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterMessageCommands(table, newDefaultNodeProvider(t), dmRouter, nil)

	resp := table.Execute(rpc.CommandRequest{
		Name: "deleteDm",
		Args: map[string]interface{}{
			"peer":       zeroIdentity,
			"message_id": validMessageID,
		},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Errorf("expected ErrValidation for zero identity, got %v", resp.ErrorKind)
	}
	if resp.Error == nil || !strings.Contains(resp.Error.Error(), "zero peer identity") {
		t.Errorf("expected zero-identity validation error, got %v", resp.Error)
	}
	dmRouter.AssertNotCalled(t, "SendMessageDelete")
}

func TestMessageCommandsHiddenWithoutDMRouter(t *testing.T) {
	node := newDefaultNodeProvider(t)
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
		if name == "sendDm" {
			t.Errorf("command %q should be hidden when dmRouter is nil", name)
		}
		// fetchDmHeaders uses only NodeProvider — should be visible
		if name == "fetchDmHeaders" {
			foundDMHeaders = true
		}
	}
	if !foundDMHeaders {
		t.Error("fetchDmHeaders should be visible even without dmRouter (uses only NodeProvider)")
	}
}

func TestMessageCommandsVisibleWithDMRouter(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array, got %T", result["commands"])
	}

	found := map[string]bool{"sendDm": false, "fetchDmHeaders": false}
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
