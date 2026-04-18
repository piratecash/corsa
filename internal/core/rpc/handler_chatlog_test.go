package rpc_test

import (
	"testing"

	"github.com/stretchr/testify/mock"

	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

func TestChatlogFetchEntriesValidRequest(t *testing.T) {
	chatlogJSON := `{"entries":[{"id":"1","text":"hello"}]}`
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return(chatlogJSON, nil)
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic":        "dm",
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 200)
	if raw, ok := result["_raw"]; ok {
		if raw != chatlogJSON {
			t.Errorf("expected raw JSON %s, got %s", chatlogJSON, raw)
		}
	}
}

func TestChatlogFetchEntriesDefaultsTopic(t *testing.T) {
	// When topic is omitted, it defaults to "dm" (backward-compatible with
	// the old desktop console path where consoleFetchChatlog defaulted topic).
	chatlogJSON := `{"entries":[]}`

	var capturedTopic, capturedPeerAddress string
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedTopic = args.String(0)
			capturedPeerAddress = args.String(1)
		}).
		Return(chatlogJSON, nil)
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, _ := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 200)

	if capturedTopic != "dm" {
		t.Errorf("expected default topic 'dm', got %q", capturedTopic)
	}
	_ = capturedPeerAddress
}

func TestChatlogFetchEntriesOptionalPeerAddress(t *testing.T) {
	// peer_address is optional: empty string returns all entries for the topic,
	// matching the old desktop console behavior (consoleFetchChatlog).
	chatlogJSON := `{"entries":[]}`
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return(chatlogJSON, nil)
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, _ := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 200)
}

// When chatlog provider is nil, commands are not registered.
// Legacy handler returns 503 for unregistered commands.
func TestChatlogFetchEntriesNilProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic":        "dm",
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchEntriesProviderError(t *testing.T) {
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("", errorWithMessage("failed to fetch chatlog"))
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic":        "dm",
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 500)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchPreviews(t *testing.T) {
	previewsJSON := `{"previews":[{"id":"1","name":"chat1"}]}`
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return(previewsJSON, nil)
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	if raw, ok := result["_raw"]; ok {
		if raw != previewsJSON {
			t.Errorf("expected raw JSON %s, got %s", previewsJSON, raw)
		}
	}
}

func TestChatlogFetchPreviewsNilProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchPreviewsProviderError(t *testing.T) {
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return("", errorWithMessage("failed to fetch previews"))
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	expectStatusCode(t, code, 500)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchConversations(t *testing.T) {
	conversationsJSON := `{"conversations":[{"id":"1","peer":"addr"}]}`
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return(conversationsJSON, nil)
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/conversations", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	if raw, ok := result["_raw"]; ok {
		if raw != conversationsJSON {
			t.Errorf("expected raw JSON %s, got %s", conversationsJSON, raw)
		}
	}
}

func TestChatlogFetchConversationsNilProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/conversations", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchConversationsProviderError(t *testing.T) {
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("", errorWithMessage("failed to fetch conversations"))
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/conversations", map[string]interface{}{})

	expectStatusCode(t, code, 500)
	expectFieldExists(t, result, "error")
}

func TestChatlogCommandsHiddenWithNilProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array, got %T", result["commands"])
	}

	chatlogCommands := map[string]bool{
		"fetchChatlog":         true,
		"fetchChatlogPreviews": true,
		"fetchConversations":   true,
	}

	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := cmdMap["name"].(string)
		if chatlogCommands[name] {
			t.Errorf("command %q should be hidden when chatlog provider is nil", name)
		}
	}
}

func TestChatlogCommandsVisibleWithProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	chatlog := newDefaultChatlogProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array, got %T", result["commands"])
	}

	found := map[string]bool{
		"fetchChatlog":         false,
		"fetchChatlogPreviews": false,
		"fetchConversations":   false,
	}

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
			t.Errorf("command %q should be visible when chatlog provider is present", name)
		}
	}
}
