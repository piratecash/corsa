package rpc_test

import (
	"fmt"
	"strings"
	"testing"
)

func TestFileSendAnnounceSuccess(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "status", "pending")
	expectField(t, result, "to", "peer-addr")
	expectField(t, result, "file_name", "document.pdf")
	expectField(t, result, "file_hash", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")

	if dmRouter.lastTo != "peer-addr" {
		t.Errorf("expected dmRouter.lastTo = %q, got %q", "peer-addr", dmRouter.lastTo)
	}
}

func TestFileSendAnnounceValidationFailure(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{
		fileAnnounceError: fmt.Errorf("transmit file not found for hash abc123hash"),
	}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 500)
	errMsg, _ := result["error"].(string)
	if !strings.Contains(errMsg, "file announce failed") {
		t.Errorf("expected error containing 'file announce failed', got %q", errMsg)
	}
	if !strings.Contains(errMsg, "transmit file not found") {
		t.Errorf("expected error containing 'transmit file not found', got %q", errMsg)
	}
}

func TestFileSendAnnounceMissingTo(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceMissingFileName(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceMissingFileSize(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceZeroFileSize(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 0,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceMissingFileHash(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceNilDMRouter(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil) // dmRouter=nil

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceDefaultContentType(t *testing.T) {
	node := &mockNodeProvider{}
	dmRouter := &mockDMRouterProvider{}
	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	// Omit content_type — should default to "application/octet-stream"
	code, _ := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 200)

	// Verify the DM was dispatched (dmRouter was called).
	if dmRouter.lastTo != "peer-addr" {
		t.Errorf("expected dmRouter.lastTo = %q, got %q", "peer-addr", dmRouter.lastTo)
	}
}
