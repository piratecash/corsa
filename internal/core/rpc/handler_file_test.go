package rpc_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestFileSendAnnounceSuccess(t *testing.T) {
	node := newDefaultNodeProvider(t)

	var capturedTo domain.PeerIdentity
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).Return(nil).Maybe()
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedTo = args.Get(0).(domain.PeerIdentity)
		}).
		Return(nil)

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

	if capturedTo != "peer-addr" {
		t.Errorf("expected capturedTo = %q, got %q", "peer-addr", capturedTo)
	}
}

func TestFileSendAnnounceValidationFailure(t *testing.T) {
	node := newDefaultNodeProvider(t)

	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).Return(nil).Maybe()
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("transmit file not found for hash abc123hash"))

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

// TestFileSendAnnounceReturns503WhenWipePending pins the same
// outgoing-barrier mapping as TestMessageSendDMReturns503WhenWipePending
// but on the file_announce path: when SendFileAnnounce rejects with
// service.ErrConversationDeleteInflight, the RPC must surface 503
// rather than 500. The input is well-formed and the file is valid;
// the server is just temporarily refusing the send because a
// conversation_delete is in flight for the peer.
func TestFileSendAnnounceReturns503WhenWipePending(t *testing.T) {
	node := newDefaultNodeProvider(t)

	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).Return(nil).Maybe()
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(service.ErrConversationDeleteInflight)

	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	code, result := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestFileSendAnnounceMissingTo(t *testing.T) {
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
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
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
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
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
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
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
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
	node := newDefaultNodeProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
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
	node := newDefaultNodeProvider(t)
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
	node := newDefaultNodeProvider(t)

	var capturedTo domain.PeerIdentity
	dmRouter := rpcmocks.NewMockDMRouterProvider(t)
	dmRouter.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	dmRouter.On("SendMessage", mock.Anything, mock.Anything).Return(nil).Maybe()
	dmRouter.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedTo = args.Get(0).(domain.PeerIdentity)
		}).
		Return(nil)

	server := setupTestServerWithDMRouter(t, node, nil, dmRouter)

	// Omit content_type — should default to "application/octet-stream"
	code, _ := postJSON(t, server, "/rpc/v1/file/send_file_announce", map[string]interface{}{
		"to":        "peer-addr",
		"file_name": "document.pdf",
		"file_size": 1024,
		"file_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	})

	expectStatusCode(t, code, 200)

	if capturedTo != "peer-addr" {
		t.Errorf("expected capturedTo = %q, got %q", "peer-addr", capturedTo)
	}
}

// TestExplainFileRouteRequiresIdentity confirms the RPC handler rejects a
// call with no identity argument and reports it as a validation error
// (HTTP 400 in the wire layer). The lower-level ranking and JSON shape
// are exercised in node and filerouter package tests; this test only
// owns the RPC contract.
func TestExplainFileRouteRequiresIdentity(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterFileCommands(table, newDefaultNodeProvider(t), newDefaultDMRouterProvider(t))

	resp := table.Execute(rpc.CommandRequest{
		Name: "explainFileRoute",
		Args: map[string]interface{}{},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Errorf("expected ErrValidation when identity is missing, got %v", resp.ErrorKind)
	}
}

// TestExplainFileRouteRejectsMalformedIdentity confirms the same address
// validation gate as fetchRouteLookup runs here, so the file router never
// receives an obviously broken identity from the RPC layer.
func TestExplainFileRouteRejectsMalformedIdentity(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterFileCommands(table, newDefaultNodeProvider(t), newDefaultDMRouterProvider(t))

	for _, id := range []string{"too-short", "../../../etc/passwd", "GGHHIIJJ00112233445566778899aabbccddeeff"} {
		resp := table.Execute(rpc.CommandRequest{
			Name: "explainFileRoute",
			Args: map[string]interface{}{"identity": id},
		})
		if resp.ErrorKind != rpc.ErrValidation {
			t.Errorf("identity %q: expected ErrValidation, got %v", id, resp.ErrorKind)
		}
	}
}

// TestExplainFileRouteForwardsToNodeProvider walks the happy path: a valid
// identity reaches NodeProvider.ExplainFileRoute and the JSON it returns
// is propagated verbatim back to the caller. This is the contract the
// console / CLI / SDK rely on — the RPC layer must not reshape the
// payload, otherwise the diagnostic output drifts away from what the
// node-level explainer actually produced.
func TestExplainFileRouteForwardsToNodeProvider(t *testing.T) {
	node := rpcmocks.NewMockNodeProvider(t)
	node.On("Address").Return("test-address-abc123").Maybe()
	node.On("ClientVersion").Return("0.16-alpha").Maybe()

	wantPayload := json.RawMessage(`[{"next_hop":"aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44","hops":1,"protocol_version":7,"connected_at":"2025-01-01T12:34:56Z","uptime_seconds":42,"best":true}]`)
	node.On("ExplainFileRoute", domain.PeerIdentity("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")).
		Return(wantPayload, nil).Once()

	table := rpc.NewCommandTable()
	rpc.RegisterFileCommands(table, node, newDefaultDMRouterProvider(t))

	resp := table.Execute(rpc.CommandRequest{
		Name: "explainFileRoute",
		Args: map[string]interface{}{"identity": "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"},
	})
	if resp.Error != nil {
		t.Fatalf("explainFileRoute: unexpected error: %v", resp.Error)
	}
	if string(resp.Data) != string(wantPayload) {
		t.Fatalf("explainFileRoute: payload mismatch\n got=%s\nwant=%s", string(resp.Data), string(wantPayload))
	}
}
