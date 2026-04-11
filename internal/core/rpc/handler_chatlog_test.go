package rpc_test

import (
	"testing"
)

func TestChatlogFetchEntriesValidRequest(t *testing.T) {
	chatlogJSON := `{"entries":[{"id":"1","text":"hello"}]}`
	chatlog := &mockChatlogProvider{
		chatlogResult: chatlogJSON,
	}
	node := &mockNodeProvider{}
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
	chatlog := &mockChatlogProvider{
		chatlogResult: chatlogJSON,
	}
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, chatlog)

	code, _ := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 200)

	if chatlog.chatlogTopic != "dm" {
		t.Errorf("expected default topic 'dm', got %q", chatlog.chatlogTopic)
	}
}

func TestChatlogFetchEntriesOptionalPeerAddress(t *testing.T) {
	// peer_address is optional: empty string returns all entries for the topic,
	// matching the old desktop console behavior (consoleFetchChatlog).
	chatlogJSON := `{"entries":[]}`
	chatlog := &mockChatlogProvider{
		chatlogResult: chatlogJSON,
	}
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, chatlog)

	code, _ := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic": "dm",
	})

	expectStatusCode(t, code, 200)
}

// When chatlog provider is nil, commands are not registered.
// Legacy handler returns 503 for unregistered commands.
func TestChatlogFetchEntriesNilProvider(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/entries", map[string]interface{}{
		"topic":        "dm",
		"peer_address": "peer-addr-123",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchEntriesProviderError(t *testing.T) {
	chatlog := &mockChatlogProvider{
		chatlogErr: errorWithMessage("failed to fetch chatlog"),
	}
	node := &mockNodeProvider{}
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
	chatlog := &mockChatlogProvider{
		previewsResult: previewsJSON,
	}
	node := &mockNodeProvider{}
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
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchPreviewsProviderError(t *testing.T) {
	chatlog := &mockChatlogProvider{
		previewsErr: errorWithMessage("failed to fetch previews"),
	}
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	expectStatusCode(t, code, 500)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchConversations(t *testing.T) {
	conversationsJSON := `{"conversations":[{"id":"1","peer":"addr"}]}`
	chatlog := &mockChatlogProvider{
		conversationsResult: conversationsJSON,
	}
	node := &mockNodeProvider{}
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
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil) // chatlog=nil

	code, result := postJSON(t, server, "/rpc/v1/chatlog/conversations", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestChatlogFetchConversationsProviderError(t *testing.T) {
	chatlog := &mockChatlogProvider{
		conversationsErr: errorWithMessage("failed to fetch conversations"),
	}
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/chatlog/conversations", map[string]interface{}{})

	expectStatusCode(t, code, 500)
	expectFieldExists(t, result, "error")
}

func TestChatlogCommandsHiddenWithNilProvider(t *testing.T) {
	node := &mockNodeProvider{}
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
	node := &mockNodeProvider{}
	chatlog := &mockChatlogProvider{}
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
