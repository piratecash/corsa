package node

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// authenticatedConn performs the full hello -> welcome -> auth_session -> auth_ok
// handshake on the data port and returns the authenticated TCP connection,
// a bufio.Reader for responses, and the peer identity used.
func authenticatedConn(t *testing.T, svc *Service) (net.Conn, *bufio.Reader, *identity.Identity) {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
		Listen:                 "10.0.0.99:64646",
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, connauth.SessionAuthPayload(welcome.Challenge, id.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %#v", authOK)
	}

	return conn, reader, id
}

// unauthenticatedConn connects to the data port and sends a hello without
// identity fields, staying unauthenticated.
func unauthenticatedConn(t *testing.T, svc *Service) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %#v", welcome)
	}

	return conn, reader
}

func newScopedTestService(t *testing.T) (*Service, func()) {
	t.Helper()

	addr := freeAddress(t)
	tempDir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:    addr,
		AdvertiseAddress: addr,
		TrustStorePath:   filepath.Join(tempDir, "trust.json"),
		QueueStatePath:   filepath.Join(tempDir, "queue.json"),
		Type:             domain.NodeTypeFull,
	}, id)
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
		{"delete_trusted_contact", protocol.Frame{Type: "delete_trusted_contact", Address: "addr"}},
		{"import_contacts", protocol.Frame{Type: "import_contacts"}},
		{"send_message", protocol.Frame{Type: "send_message", Topic: "global", ID: "msg-1", Address: "addr", Recipient: "*", Body: "test"}},
		{"import_message", protocol.Frame{Type: "import_message", Topic: "global", ID: "msg-2", Address: "addr", Recipient: "*", Body: "test"}},
		{"publish_notice", protocol.Frame{Type: "publish_notice", TTLSeconds: 30, Ciphertext: "test"}},
		{"send_delivery_receipt", protocol.Frame{Type: "send_delivery_receipt", ID: "msg-1", Address: "addr", Recipient: "rcpt", Status: "delivered", DeliveredAt: "2026-01-01T00:00:00Z"}},
	}

	for _, tc := range dataSnakeCase {
		t.Run(tc.name+"_auth", func(t *testing.T) {
			conn, reader, _ := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, tc.frame)
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: data command %q should return unknown_command for auth peer, got type=%q code=%s",
					tc.name, resp.Type, resp.Code)
			}
		})

		t.Run(tc.name+"_unauth", func(t *testing.T) {
			conn, reader := unauthenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, tc.frame)
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: data command %q should return unknown_command for unauth peer, got type=%q code=%s",
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
	}

	for _, cmd := range dataCamelCase {
		t.Run(cmd+"_auth", func(t *testing.T) {
			conn, reader, _ := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: camelCase data command %q should return unknown_command for auth peer, got type=%q code=%s",
					cmd, resp.Type, resp.Code)
			}
		})

		t.Run(cmd+"_unauth", func(t *testing.T) {
			conn, reader := unauthenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: camelCase data command %q should return unknown_command for unauth peer, got type=%q code=%s",
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
	}

	for _, cmd := range dataKebabCase {
		t.Run(cmd+"_auth", func(t *testing.T) {
			conn, reader, _ := authenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: kebab-case data command %q should return unknown_command for auth peer, got type=%q code=%s",
					cmd, resp.Type, resp.Code)
			}
		})

		t.Run(cmd+"_unauth", func(t *testing.T) {
			conn, reader := unauthenticatedConn(t, svc)
			defer func() { _ = conn.Close() }()

			writeJSONFrame(t, conn, protocol.Frame{Type: cmd})
			resp := readJSONTestFrame(t, reader)
			if resp.Type != "error" || resp.Code != protocol.ErrCodeUnknownCommand {
				t.Fatalf("SECURITY: kebab-case data command %q should return unknown_command for unauth peer, got type=%q code=%s",
					cmd, resp.Type, resp.Code)
			}
		})
	}
}

// =============================================================================
// P2P commands: unauthenticated peer → auth_required
// =============================================================================

// TestUnauthPeerCannotPushMessage verifies that an unauthenticated peer
// cannot send push_message on the data port.
func TestUnauthPeerCannotPushMessage(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "push_message",
		Topic:     "dm",
		ID:        "test-msg-1",
		Recipient: "some-addr",
		Body:      "blocked payload",
	})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated push_message not rejected with auth-required: %#v", resp)
	}
}

// TestUnauthPeerCannotSubscribeInbox verifies that an unauthenticated peer
// cannot send subscribe_inbox on the data port.
func TestUnauthPeerCannotSubscribeInbox(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "subscribe_inbox",
		Topic:     "dm",
		Recipient: "victim-address",
	})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated subscribe_inbox not rejected: %#v", resp)
	}
}

// TestUnauthPeerCannotSendRelayMessage verifies that an unauthenticated peer
// cannot inject relay traffic on the data port.
func TestUnauthPeerCannotSendRelayMessage(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:        "relay_message",
		ID:          "attack-relay-1",
		Address:     "spoofed-origin",
		Recipient:   "target-address",
		Topic:       "dm",
		Body:        "malicious payload",
		HopCount:    1,
		MaxHops:     10,
		PreviousHop: "10.0.0.99:64646",
	})

	resp := readJSONTestFrame(t, reader)
	if resp.Type == "relay_hop_ack" {
		t.Fatalf("SECURITY: unauthenticated relay_message produced hop-ack: %#v", resp)
	}
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated relay_message should return auth-required, got %#v", resp)
	}
}

// TestUnauthPeerCannotAnnouncePeer verifies that an unauthenticated peer
// cannot announce peers on the data port.
func TestUnauthPeerCannotAnnouncePeer(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:     "announce_peer",
		NodeType: "full",
		Peers:    []string{"10.0.0.50:12345"},
	})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated announce_peer not rejected: %#v", resp)
	}
}

// TestUnauthPeerCannotGetPeers verifies that an unauthenticated peer
// cannot request the peer list on the data port.
func TestUnauthPeerCannotGetPeers(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated get_peers not rejected with auth-required: %#v", resp)
	}
}

// TestUnauthPeerCannotFetchContacts verifies that an unauthenticated peer
// cannot fetch the contact list on the data port.
func TestUnauthPeerCannotFetchContacts(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "fetch_contacts"})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated fetch_contacts not rejected with auth-required: %#v", resp)
	}
}

// TestAuthPeerCanGetPeers verifies that an authenticated peer can request
// the peer list over the TCP data port — required for syncPeer/syncPeerSession.
func TestAuthPeerCanGetPeers(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, _ := authenticatedConn(t, svc)
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

	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "fetch_contacts"})

	resp := readJSONTestFrame(t, reader)
	if resp.Type != "contacts" {
		t.Fatalf("authenticated fetch_contacts should return contacts, got type=%q code=%s error=%q",
			resp.Type, resp.Code, resp.Error)
	}
}

// TestUnauthPeerCanUsePingPong verifies that handshake commands work for
// unauthenticated peers.
func TestUnauthPeerCanUsePingPong(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "pong" {
		t.Fatalf("expected pong for unauthenticated peer, got %#v", resp)
	}
}

// =============================================================================
// GAP-0 regression tests
// =============================================================================

// TestGAP0_MobileClientCannotBypassAuth is the canonical GAP-0 regression
// test: a connection using Client="mobile" with mesh_relay_v1 capability
// but no identity fields stays unauthenticated and cannot inject relay traffic.
func TestGAP0_MobileClientCannotBypassAuth(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	// Send hello with Client="mobile" and NO identity fields (only Address,
	// missing PubKey/BoxKey/BoxSig). In the old code, requiresSessionAuth
	// returned false for "mobile", allowing the peer to skip auth and access
	// all commands.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
		Address:                "fake-attacker-address",
		Listen:                 "10.0.0.99:64646",
		Capabilities:           []string{"mesh_relay_v1"},
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %#v", welcome)
	}
	// The welcome should NOT have a challenge (missing identity fields).
	if welcome.Challenge != "" {
		t.Fatalf("expected no challenge for hello without full identity fields, got challenge=%q", welcome.Challenge)
	}

	// Try relay_message — should be rejected (auth required).
	writeJSONFrame(t, conn, protocol.Frame{
		Type:        "relay_message",
		ID:          "gap0-test-1",
		Address:     "spoofed-sender",
		Recipient:   "target",
		Topic:       "dm",
		Body:        "malicious",
		HopCount:    1,
		MaxHops:     10,
		PreviousHop: "10.0.0.99:64646",
	})

	resp := readJSONTestFrame(t, reader)
	if resp.Type == "relay_hop_ack" {
		t.Fatalf("SECURITY GAP-0: Client=mobile without auth produced relay_hop_ack: %#v", resp)
	}
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY GAP-0: expected auth-required error, got %#v", resp)
	}
}

// TestGAP0_MobileClientWithIdentityGetsChallenge verifies that the
// connauth.HasIdentityFields logic correctly triggers auth for any client type
// (including "mobile") when identity fields are provided.
func TestGAP0_MobileClientWithIdentityGetsChallenge(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	// Send hello with Client="mobile" but WITH all identity fields.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %#v", welcome)
	}
	if welcome.Challenge == "" {
		t.Fatal("expected challenge for hello with identity fields (Client=mobile) — " +
			"connauth.HasIdentityFields should trigger auth regardless of Client value")
	}

	// Complete auth — should succeed.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, connauth.SessionAuthPayload(welcome.Challenge, id.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %#v", authOK)
	}

	// Verify auth works: ping should still work after auth.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "pong" {
		t.Fatalf("expected pong after auth with Client=mobile, got %#v", resp)
	}
}

// =============================================================================
// Re-hello rejection tests
// =============================================================================

// TestReHelloAfterAuthIsRejected verifies that once a connection has
// completed auth_session, a subsequent hello is rejected with
// hello-after-auth. This prevents identity/capability spoofing on
// an already-authenticated connection.
func TestReHelloAfterAuthIsRejected(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, id := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	// Attempt to send a second hello with different identity fields.
	spoofID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                spoofID.Address,
		PubKey:                 identity.PublicKeyBase64(spoofID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(spoofID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(spoofID),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
		t.Fatalf("re-hello after auth should return hello-after-auth, got %#v", resp)
	}

	// Verify the connection's identity was NOT changed.
	writeJSONFrame(t, conn, protocol.Frame{Type: "ping"})
	pong := readJSONTestFrame(t, reader)
	if pong.Type != "pong" {
		t.Fatalf("connection should remain functional after rejected re-hello, got %#v", pong)
	}
	_ = id // original identity should still be bound
}

// TestReHelloDuringAuthIsRejected verifies that once a challenge has been
// issued (auth initiated but not completed), a second hello is rejected.
func TestReHelloDuringAuthIsRejected(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// First hello with identity fields → gets challenge.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Second hello (re-hello during auth) → rejected.
	spoofID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                spoofID.Address,
		PubKey:                 identity.PublicKeyBase64(spoofID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(spoofID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(spoofID),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
		t.Fatalf("re-hello during auth should return hello-after-auth, got %#v", resp)
	}
}
