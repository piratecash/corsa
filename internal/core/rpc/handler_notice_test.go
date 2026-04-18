package rpc_test

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestNoticeFetchNotices(t *testing.T) {
	notices := []protocol.NoticeFrame{
		{
			ID:         "notice-1",
			ExpiresAt:  1711454400,
			Ciphertext: "encrypted-data-1",
		},
		{
			ID:         "notice-2",
			ExpiresAt:  1711540800,
			Ciphertext: "encrypted-data-2",
		},
	}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_notices" {
			return protocol.Frame{
				Type:    "notices_response",
				Notices: notices,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/notice/list", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "notices_response")

	noticesResult, ok := result["notices"].([]interface{})
	if !ok {
		t.Errorf("expected notices to be array, got %T", result["notices"])
		return
	}
	if len(noticesResult) != len(notices) {
		t.Errorf("expected %d notices, got %d", len(notices), len(noticesResult))
	}

	// Verify first notice structure
	if len(noticesResult) > 0 {
		notice, ok := noticesResult[0].(map[string]interface{})
		if !ok {
			t.Errorf("expected notice to be map, got %T", noticesResult[0])
			return
		}
		if _, hasID := notice["id"]; !hasID {
			t.Error("notice missing 'id' field")
		}
		if _, hasExpires := notice["expires_at"]; !hasExpires {
			t.Error("notice missing 'expires_at' field")
		}
		if _, hasCiphertext := notice["ciphertext"]; !hasCiphertext {
			t.Error("notice missing 'ciphertext' field")
		}
	}
}

func TestNoticeFetchNoticesEmpty(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_notices" {
			return protocol.Frame{
				Type:    "notices_response",
				Notices: []protocol.NoticeFrame{},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/notice/list", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "notices_response")

	// Empty slice may be omitted (null) due to omitempty on the Frame field
	if notices, exists := result["notices"]; exists && notices != nil {
		noticesResult, ok := notices.([]interface{})
		if !ok {
			t.Errorf("expected notices to be array or nil, got %T", notices)
			return
		}
		if len(noticesResult) != 0 {
			t.Errorf("expected 0 notices, got %d", len(noticesResult))
		}
	}
}

func TestNoticeFetchNoticesNodeError(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_notices" {
			return protocol.Frame{
				Type:  "error",
				Error: "notices not available",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/notice/list", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
}

func TestNoticeFetchNoticesSingleNotice(t *testing.T) {
	notices := []protocol.NoticeFrame{
		{
			ID:         "notice-single",
			ExpiresAt:  1711454400,
			Ciphertext: "encrypted-single",
		},
	}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_notices" {
			return protocol.Frame{
				Type:    "notices_response",
				Notices: notices,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/notice/list", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "notices_response")

	noticesResult, ok := result["notices"].([]interface{})
	if !ok {
		t.Fatalf("expected notices to be array")
	}
	if len(noticesResult) != 1 {
		t.Fatalf("expected 1 notice, got %d", len(noticesResult))
	}

	notice, ok := noticesResult[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected notice to be map")
	}
	if id, ok := notice["id"].(string); !ok || id != "notice-single" {
		t.Errorf("expected notice id 'notice-single', got %v", notice["id"])
	}
}

func TestNoticeFetchNoticesMultipleCiphertexts(t *testing.T) {
	notices := []protocol.NoticeFrame{
		{ID: "notice-1", ExpiresAt: 1711454400, Ciphertext: "cipher-1"},
		{ID: "notice-2", ExpiresAt: 1711540800, Ciphertext: "cipher-2"},
		{ID: "notice-3", ExpiresAt: 1711627200, Ciphertext: "cipher-3"},
		{ID: "notice-4", ExpiresAt: 1711713600, Ciphertext: "cipher-4"},
	}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_notices" {
			return protocol.Frame{
				Type:    "notices_response",
				Notices: notices,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/notice/list", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	noticesResult, ok := result["notices"].([]interface{})
	if !ok {
		t.Fatalf("expected notices to be array")
	}
	if len(noticesResult) != len(notices) {
		t.Errorf("expected %d notices, got %d", len(notices), len(noticesResult))
	}
}
