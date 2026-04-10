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
// identity fields, staying RoleUnauthPeer.
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

	ctx, cancel := context.WithCancel(context.Background())
	svc, stop := startTestService(t, ctx, cancel, svc)
	return svc, stop
}

// --- Authenticated peer tests ---

// TestAuthPeerCanFetchContacts verifies that an authenticated peer can
// successfully execute fetch_contacts on the data port.
func TestAuthPeerCanFetchContacts(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "fetch_contacts"})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "contacts" {
		t.Fatalf("expected contacts response, got type=%q code=%s error=%s", resp.Type, resp.Code, resp.Error)
	}
	if resp.Count < 0 {
		t.Fatalf("contacts response has negative count: %d", resp.Count)
	}
}

// TestAuthPeerCanGetPeers verifies that an authenticated peer can execute
// get_peers on the data port.
func TestAuthPeerCanGetPeers(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "peers" {
		t.Fatalf("expected peers, got %#v", resp)
	}
}

// TestAuthPeerCanSendMessage verifies that an authenticated peer can
// send messages on the data port — a command now restricted to RoleAuthPeer.
func TestAuthPeerCanSendMessage(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, id := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	ts := time.Now().UTC().Format(time.RFC3339)
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "send_message",
		Topic:     "global",
		ID:        "auth-msg-1",
		Address:   id.Address,
		Recipient: "*",
		Flag:      "immutable",
		CreatedAt: ts,
		Body:      "auth-only message",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type == "error" && resp.Code == protocol.ErrCodeAuthRequired {
		t.Fatalf("auth peer send_message rejected with auth-required")
	}
	if resp.Type != "message_stored" && resp.Type != "message_known" {
		t.Fatalf("expected message_stored or message_known, got %#v", resp)
	}
}

// TestAuthPeerCanFetchInbox verifies that an authenticated peer can
// fetch inbox on the data port — a command now restricted to RoleAuthPeer.
func TestAuthPeerCanFetchInbox(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, id := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: id.Address,
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type == "error" && resp.Code == protocol.ErrCodeAuthRequired {
		t.Fatalf("auth peer fetch_inbox rejected with auth-required")
	}
	if resp.Type != "inbox" {
		t.Fatalf("expected inbox, got %#v", resp)
	}
}

// TestAuthPeerCanPublishNotice verifies that an authenticated peer can
// successfully publish a notice on the data port.
func TestAuthPeerCanPublishNotice(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:       "publish_notice",
		TTLSeconds: 60,
		Ciphertext: "dGVzdC1ub3RpY2UtY2lwaGVydGV4dA==",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type == "error" {
		t.Fatalf("auth peer publish_notice rejected: code=%s error=%s", resp.Code, resp.Error)
	}
	if resp.Type != "notice_stored" {
		t.Fatalf("expected notice_stored, got %q", resp.Type)
	}
}

// --- Unauthenticated peer tests ---

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

// TestGAP0_MobileClientCannotBypassAuth is the canonical GAP-0 regression
// test: a connection using Client="mobile" with mesh_relay_v1 capability
// but no identity fields stays RoleUnauthPeer and cannot inject relay traffic.
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

	// Try relay_message — should be rejected by connauth.Commands.
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

// TestGAP0_MobileClientWithIdentityGetsChallenge verifies that the new
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

	// Verify the connection was actually promoted to RoleAuthPeer by using
	// an auth-only command. get_peers is allowed for both roles and would
	// not catch a regression where Client="mobile" stays unauthenticated.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: id.Address,
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "inbox" {
		t.Fatalf("auth-only fetch_inbox should work after auth with Client=mobile, got %#v", resp)
	}
}

// --- Unauth regression tests for newly restricted commands ---
// These commands were recently moved from both-roles to RoleAuthPeer-only.
// Each test verifies the security boundary independently.

func TestUnauthPeerCannotSendMessage(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "send_message",
		Topic:     "global",
		ID:        "unauth-msg-1",
		Address:   "fake-sender",
		Recipient: "*",
		Flag:      "immutable",
		Body:      "blocked",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated send_message not rejected: %#v", resp)
	}

	// Verify the message was not persisted despite the rejection.
	stored := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if stored.Count != 0 {
		t.Fatalf("SECURITY: rejected send_message left %d messages in topic global", stored.Count)
	}
}

func TestUnauthPeerCannotImportMessage(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "import_message",
		Topic:     "global",
		ID:        "unauth-import-1",
		Address:   "fake-sender",
		Recipient: "*",
		Flag:      "immutable",
		Body:      "blocked",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated import_message not rejected: %#v", resp)
	}

	// Verify the message was not persisted despite the rejection.
	stored := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if stored.Count != 0 {
		t.Fatalf("SECURITY: rejected import_message left %d messages in topic global", stored.Count)
	}
}

func TestUnauthPeerCannotFetchInbox(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: svc.Address(),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated fetch_inbox not rejected: %#v", resp)
	}
}

func TestUnauthPeerCannotFetchDeliveryReceipts(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_delivery_receipts",
		Recipient: svc.Address(),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated fetch_delivery_receipts not rejected: %#v", resp)
	}
}

func TestUnauthPeerCannotPublishNotice(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:       "publish_notice",
		TTLSeconds: 30,
		Ciphertext: "fake-ciphertext",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated publish_notice not rejected: %#v", resp)
	}

	// Verify the notice was not persisted despite the rejection.
	notices := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_notices"})
	if notices.Count != 0 {
		t.Fatalf("SECURITY: rejected publish_notice left %d notices in store", notices.Count)
	}
}

func TestUnauthPeerCannotDeleteTrustedContact(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	// Snapshot trusted contacts count before the attack attempt.
	before := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})

	writeJSONFrame(t, conn, protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: svc.Address(),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated delete_trusted_contact not rejected: %#v", resp)
	}

	// Verify the trust store was not modified despite the rejection.
	after := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if after.Count != before.Count {
		t.Fatalf("SECURITY: rejected delete_trusted_contact changed trust store count from %d to %d",
			before.Count, after.Count)
	}
}

func TestUnauthPeerCannotSendDeliveryReceipt(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader := unauthenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "send_delivery_receipt",
		ID:        "msg-1",
		Recipient: "some-addr",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("SECURITY: unauthenticated send_delivery_receipt not rejected: %#v", resp)
	}

	// Verify the receipt was not persisted despite the rejection.
	receipts := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_delivery_receipts", Recipient: "some-addr"})
	if receipts.Count != 0 {
		t.Fatalf("SECURITY: rejected send_delivery_receipt left %d receipts for some-addr", receipts.Count)
	}
}

// --- Re-hello rejection tests ---

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
		t.Fatalf("SECURITY: re-hello after auth should be rejected with %s, got %#v",
			protocol.ErrCodeHelloAfterAuth, resp)
	}

	// Verify the connection retains the original auth context by using an
	// auth-sensitive, identity-bound command. get_peers is allowed for both
	// roles and doesn't depend on identity, so it would mask a silent auth
	// state reset. fetch_inbox requires RoleAuthPeer AND is filtered by
	// Recipient, proving both auth and identity survived the rejected re-hello.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: id.Address,
	})
	inbox := readJSONTestFrame(t, reader)
	if inbox.Type != "inbox" {
		t.Fatalf("auth-sensitive fetch_inbox failed after rejected re-hello "+
			"(original identity may have been damaged): got %#v", inbox)
	}
}

// TestReHelloWithoutIdentityAfterAuthIsRejected verifies that even a
// simple hello (without identity fields) is rejected on an authenticated
// connection.
func TestReHelloWithoutIdentityAfterAuthIsRejected(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	conn, reader, id := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
		t.Fatalf("SECURITY: re-hello (no identity) after auth should be rejected, got %#v", resp)
	}

	// Verify the auth context survived the rejected re-hello by using an
	// auth-only, identity-bound command.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: id.Address,
	})
	inbox := readJSONTestFrame(t, reader)
	if inbox.Type != "inbox" {
		t.Fatalf("auth-sensitive fetch_inbox failed after rejected re-hello without identity "+
			"(auth state may have been damaged): got %#v", inbox)
	}
}

// --- Pending-auth re-hello tests ---

// TestReHelloDuringPendingAuthIsRejected verifies that once a hello with
// identity fields has been sent (challenge issued, auth pending), a second
// hello is rejected. Without this guard, the second hello would overwrite
// NetCore address/identity via rememberConnPeerAddr while
// handleAuthSession still verifies against the original state.Hello,
// allowing an attacker to authenticate as identity A but bind the
// connection to an unverified address B.
func TestReHelloDuringPendingAuthIsRejected(t *testing.T) {
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

	// Step 1: hello with identity → challenge issued.
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
		Listen:                 "10.0.0.1:64646",
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Step 2: second hello WITHOUT identity fields but with a different
	// Listen address — attempts to overwrite NetCore.Address.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
		Listen:                 "evil-address:9999",
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
		t.Fatalf("SECURITY: re-hello during pending auth should be rejected with %s, got %#v",
			protocol.ErrCodeHelloAfterAuth, resp)
	}

	// Step 3: original auth_session should still work — the pending state
	// was not corrupted by the rejected second hello.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, connauth.SessionAuthPayload(welcome.Challenge, id.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("auth_session after rejected re-hello should succeed, got %#v", authOK)
	}
}

// TestReHelloDuringPendingAuthWithIdentityIsRejected is the variant where
// the second hello also carries identity fields (for a different identity),
// attempting to overwrite both connauth.PrepareAuth state and NetCore.
func TestReHelloDuringPendingAuthWithIdentityIsRejected(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	idA, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	idB, err := identity.Generate()
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

	// hello with identity A → challenge issued.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                idA.Address,
		PubKey:                 identity.PublicKeyBase64(idA.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(idA.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(idA),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	// Second hello with identity B — should be rejected.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaWireVersion,
		Address:                idB.Address,
		PubKey:                 identity.PublicKeyBase64(idB.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(idB.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(idB),
	})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "error" || resp.Code != protocol.ErrCodeHelloAfterAuth {
		t.Fatalf("SECURITY: re-hello with different identity during pending auth should be rejected, got %#v", resp)
	}

	// Complete auth_session for the original identity A using the original
	// challenge. If the rejected re-hello with identity B corrupted the
	// pending auth state (challenge, recorded identity), this will fail.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   idA.Address,
		Signature: identity.SignPayload(idA, connauth.SessionAuthPayload(welcome.Challenge, idA.Address)),
	})
	authOK := readJSONTestFrame(t, reader)
	if authOK.Type != "auth_ok" {
		t.Fatalf("auth_session for original identity A after rejected re-hello should succeed, got %#v", authOK)
	}

	// Use an auth-sensitive, identity-bound command to prove the connection
	// is fully functional with identity A (not B).
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "fetch_inbox",
		Topic:     "dm",
		Recipient: idA.Address,
	})
	inbox := readJSONTestFrame(t, reader)
	if inbox.Type != "inbox" {
		t.Fatalf("auth-sensitive fetch_inbox failed after completing auth for identity A: got %#v", inbox)
	}
}

// --- get_peers reachability tests ---

// TestUnauthPeerGetPeersIgnoresDeclaredNetworks verifies that for an
// unauthenticated connection, connPeerReachableGroups classifies by the
// actual TCP remote address, not by hello-declared networks. An attacker
// claiming .onion reachability must not cause the server to filter the
// response to overlay-only peers.
//
// Setup: bootstrap the service with both a clearnet (IPv4) peer and a
// Tor v3 (.onion) peer. The unauth peer claims networks=["onion"] in
// its hello. If the code trusts the declared network, the response would
// contain only the .onion peer. The correct behavior is to classify by
// conn.RemoteAddr() (localhost → local → nil groups → all routable),
// so BOTH peers must appear.
func TestUnauthPeerGetPeersIgnoresDeclaredNetworks(t *testing.T) {
	t.Parallel()

	// Valid Tor v3 address: 56 base32 chars + ".onion"
	const torV3Addr = "2gzyxa5ihm7nsggfxnu52rck2vv4rvmdlkiu3zzui5du4xyclen53wid.onion:8333"
	const clearnetAddr = "198.51.100.1:8333" // RFC 5737 TEST-NET-2

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
		BootstrapPeers:   []string{clearnetAddr, torV3Addr},
	}, id)

	ctx, cancel := context.WithCancel(context.Background())
	svc, stop := startTestService(t, ctx, cancel, svc)
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// Send hello claiming .onion network — without identity fields
	// (attacker scenario: pretend to be Tor-only to harvest overlay addrs).
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "mobile",
		ClientVersion:          "1.0",
		Address:                "fakeaddr.onion:8333",
		Networks:               []string{"onion"},
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" {
		t.Fatalf("expected welcome, got %#v", welcome)
	}

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	resp := readJSONTestFrame(t, reader)
	if resp.Type != "peers" {
		t.Fatalf("expected peers, got %#v", resp)
	}

	// Verify: both clearnet and .onion peers must be present.
	// If the code were trusting hello-declared networks=["onion"],
	// it would filter out the clearnet peer — that's the regression
	// this test catches.
	hasClearnet := false
	hasTor := false
	for _, p := range resp.Peers {
		if p == clearnetAddr {
			hasClearnet = true
		}
		if p == torV3Addr {
			hasTor = true
		}
	}
	if !hasClearnet {
		t.Errorf("SECURITY: clearnet peer %q missing from get_peers response — "+
			"server may be trusting hello-declared networks for unauth peer; got peers: %v",
			clearnetAddr, resp.Peers)
	}
	if !hasTor {
		t.Errorf("overlay peer %q missing from get_peers response — "+
			"expected all routable peers for localhost connection; got peers: %v",
			torV3Addr, resp.Peers)
	}
}
