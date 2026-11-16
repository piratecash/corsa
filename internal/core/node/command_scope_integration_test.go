package node

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// authenticatedConn opens an ordinary secure session with no optional
// capabilities and returns the reader for its application frames.
func authenticatedConn(t *testing.T, svc *Service) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn := dialTestTLSClient(t, svc.externalListenAddress())
	return conn, bufio.NewReader(conn)
}

func newScopedTestService(t *testing.T) (*Service, func()) {
	t.Helper()

	addr := freeAddress(t)
	tempDir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(configtest.WithTempDataDirs(t, config.Node{
		ListenAddress:  addr,
		TrustStorePath: filepath.Join(tempDir, "trust.json"),
		Type:           domain.NodeTypeFull,
	}), id, nil)
	svc.disableRateLimiting = true

	ctx, cancel := context.WithCancel(context.Background())
	svc, stop := startTestService(t, ctx, cancel, svc)
	return svc, stop
}

// =============================================================================
// Data commands via TCP → unknown_command (snake_case)
// =============================================================================

// TestDataCommandViaTCP_UnknownCommand_SnakeCase verifies that data-only
// commands sent over TCP data port are rejected with unknown_command.
// These commands live exclusively in handleLocalFrameDispatch / RPC HTTP.
// Test covers snake_case variants (the standard wire format).
//
// Each data command requires its own TCP connection because the server
// closes the read loop after unknown_command (return false from
// dispatchNetworkFrame). Rate limiting is disabled via
// newScopedTestService to prevent conn-rate-limit flakiness.
func TestDataCommandViaTCP_UnknownCommand_SnakeCase(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	dataSnakeCase := []struct {
		name  string
		frame protocol.Frame
	}{
		{"fetch_identities", protocol.Frame{Type: "fetch_identities"}},
		{"search_identities", protocol.Frame{Type: "search_identities", Query: "aa"}},
		{"fetch_trusted_contacts", protocol.Frame{Type: "fetch_trusted_contacts"}},
		{"fetch_peer_health", protocol.Frame{Type: "fetch_peer_health"}},
		{"fetch_network_stats", protocol.Frame{Type: "fetch_network_stats"}},
		{"fetch_pending_messages", protocol.Frame{Type: "fetch_pending_messages", Topic: "global"}},
		{"fetch_messages", protocol.Frame{Type: "fetch_messages", Topic: "global"}},
		{"fetch_message_ids", protocol.Frame{Type: "fetch_message_ids", Topic: "global"}},
		{"fetch_message", protocol.Frame{Type: "fetch_message", Topic: "global", ID: "msg-1"}},
		{"fetch_inbox", protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: "addr"}},
		{"fetch_delivery_receipts", protocol.Frame{Type: "fetch_delivery_receipts", Recipient: "addr"}},
		{"fetch_notices", protocol.Frame{Type: "fetch_notices"}},
		{"fetch_reachable_ids", protocol.Frame{Type: "fetch_reachable_ids"}},
		{"connect_only", protocol.Frame{Type: "connect_only", Peers: []string{"addr:64646"}}},
		{"delete_trusted_contact", protocol.Frame{Type: "delete_trusted_contact", Address: "addr"}},
		{"import_contacts", protocol.Frame{Type: "import_contacts"}},
		{"send_message", protocol.Frame{Type: "send_message", Topic: "global", ID: "msg-1", Address: "addr", Recipient: "*", Body: "test"}},
		{"import_message", protocol.Frame{Type: "import_message", Topic: "global", ID: "msg-2", Address: "addr", Recipient: "*", Body: "test"}},
		{"publish_notice", protocol.Frame{Type: "publish_notice", TTLSeconds: 30, Ciphertext: "test"}},
		{"send_delivery_receipt", protocol.Frame{Type: "send_delivery_receipt", ID: "msg-1", Address: "addr", Recipient: "rcpt", Status: "delivered", DeliveredAt: "2026-01-01T00:00:00Z"}},
	}

	for _, tc := range dataSnakeCase {
		t.Run(tc.name, func(t *testing.T) {
			conn, reader := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, tc.frame)
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: data command %q should return unknown_command for auth peer, got type=%q code=%s",
					tc.name, resp.Type, resp.Code)
			}
		})

	}
}

// =============================================================================
// Data commands via TCP → unknown_command (camelCase)
// =============================================================================

// TestDataCommandViaTCP_UnknownCommand_CamelCase verifies that camelCase
// variants of data commands are also rejected with unknown_command on the
// data port. Even after RPC camelCase migration (Stage 6), TCP data port
// must never accept these commands in any casing.
//
// Each data command requires its own TCP connection because the server
// closes the read loop after unknown_command. Rate limiting is disabled
// via newScopedTestService to prevent conn-rate-limit flakiness.
func TestDataCommandViaTCP_UnknownCommand_CamelCase(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	dataCamelCase := []string{
		"fetchIdentities",
		"fetchTrustedContacts",
		"fetchPeerHealth",
		"fetchNetworkStats",
		"fetchPendingMessages",
		"fetchMessages",
		"fetchMessageIds",
		"fetchMessage",
		"fetchInbox",
		"fetchDeliveryReceipts",
		"fetchNotices",
		"fetchReachableIds",
		"deleteTrustedContact",
		"importContacts",
		"sendMessage",
		"importMessage",
		"publishNotice",
		"connectOnly",
	}

	for _, cmd := range dataCamelCase {
		t.Run(cmd, func(t *testing.T) {
			conn, reader := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: camelCase data command %q should return unknown_command for auth peer, got type=%q code=%s",
					cmd, resp.Type, resp.Code)
			}
		})

	}
}

// =============================================================================
// Data commands via TCP → unknown_command (kebab-case)
// =============================================================================

// TestDataCommandViaTCP_UnknownCommand_KebabCase verifies that kebab-case
// (hyphenated) variants of data commands are rejected with unknown_command.
// This prevents potential confusion between wire format conventions.
//
// Each data command requires its own TCP connection because the server
// closes the read loop after unknown_command. Rate limiting is disabled
// via newScopedTestService to prevent conn-rate-limit flakiness.
func TestDataCommandViaTCP_UnknownCommand_KebabCase(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	dataKebabCase := []string{
		"fetch-identities",
		"fetch-trusted-contacts",
		"fetch-peer-health",
		"fetch-network-stats",
		"fetch-pending-messages",
		"fetch-messages",
		"fetch-message-ids",
		"fetch-message",
		"fetch-inbox",
		"fetch-delivery-receipts",
		"fetch-notices",
		"fetch-reachable-ids",
		"delete-trusted-contact",
		"import-contacts",
		"send-message",
		"import-message",
		"publish-notice",
		"connect-only",
	}

	for _, cmd := range dataKebabCase {
		t.Run(cmd, func(t *testing.T) {
			conn, reader := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: kebab-case data command %q should return unknown_command for auth peer, got type=%q code=%s",
					cmd, resp.Type, resp.Code)
			}
		})

	}
}

// =============================================================================
// P2P commands: unauthenticated peer → auth_required
// =============================================================================

// TestAuthPeerCanGetPeers verifies that an authenticated peer can request
// the peer list over the TCP data port — required for syncPeer/syncPeerSession.
func TestAuthPeerCanGetPeers(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "peers" {
		t.Fatalf("authenticated get_peers should return peers, got type=%q code=%s error=%q",
			resp.Type, resp.Code, resp.Error)
	}
}

// TestAuthPeerCanFetchContacts verifies that an authenticated peer can
// fetch contacts over the TCP data port — required for syncPeer/syncContactsViaSession.
func TestAuthPeerCanFetchContacts(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "fetch_contacts"})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "contacts" {
		t.Fatalf("authenticated fetch_contacts should return contacts, got type=%q code=%s error=%q",
			resp.Type, resp.Code, resp.Error)
	}
}

// The authenticated session owns its identity for its lifetime.
func TestTLSConnectionRejectsASecondHandshake(t *testing.T) {
	t.Parallel()
	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()
	for _, command := range []string{"hello", "auth_session"} {
		writeJSONFrame(t, conn, protocol.Frame{Type: command})
		resp := readJSONTestFrame(t, reader)
		if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
			t.Fatalf("post-handshake %s: got %#v", command, resp)
		}
	}
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	if reply := readJSONTestFrame(t, reader); reply.Type != "pong" {
		t.Fatalf("authenticated connection did not remain usable: %#v", reply)
	}
}
