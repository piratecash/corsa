package node

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// small_order_key_test.go pins the penalty rule for keys no honest node can
// hold (identity.ParsePublicKey): a ban is charged ONLY for the immediate
// peer's own handshake key; the same key inside content the peer merely
// forwarded is refused in silence, because the forwarder is not its author
// and may run a build that never refused it.

// mustParseSigningKey parses an honest key for tests that verify with one.
func mustParseSigningKey(t *testing.T, raw []byte) identity.PublicKey {
	t.Helper()
	key, err := identity.ParsePublicKey(raw)
	if err != nil {
		t.Fatalf("ParsePublicKey(honest): %v", err)
	}
	return key
}

// forgedIdentityAddress is the address the neutral key certifies.
func forgedIdentityAddress() string {
	return identity.Fingerprint(edforgery.NeutralPublicKey())
}

func forgedKeyBase64() string {
	return base64.StdEncoding.EncodeToString(edforgery.NeutralPublicKey())
}

// ---------------------------------------------------------------------------
// The peer's own key: refused with a ban
// ---------------------------------------------------------------------------

// TestHelloWithSmallOrderKeyIsRefusedWithBan is the new wire behaviour: a
// hello whose identity fields are the self-consistent universal forgery used
// to pass PrepareAuth (stdlib verified the box binding) and then auth_session
// (stdlib verified the challenge), i.e. the connection authenticated as an
// identity nobody holds a key for. It is now refused at hello with the same
// error and the same 100 ban points as any other invalid binding.
func TestHelloWithSmallOrderKeyIsRefusedWithBan(t *testing.T) {
	t.Parallel()
	svc, stop := startTestNodeWithoutDials(t, config.Node{ListenAddress: freeAddress(t)})
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaVersion,
		Address:       forgedIdentityAddress(),
		PubKey:        forgedKeyBase64(),
		BoxKey:        base64.StdEncoding.EncodeToString(make([]byte, 32)),
		BoxSig:        base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature()),
	})
	reply := readJSONTestFrame(t, reader)
	if reply.Type != "error" || reply.Code != protocol.ErrCodeInvalidAuthSignature {
		t.Fatalf("hello with a small-order key: got %#v, want error %s", reply, protocol.ErrCodeInvalidAuthSignature)
	}
	if got := banScoreForIP(svc, "127.0.0.1"); got != banIncrementInvalidSig {
		t.Fatalf("ban score = %d, want %d: the key is the peer's own", got, banIncrementInvalidSig)
	}
}

// TestHelloWithHonestKeyIsUnaffected is the compatibility control on the same
// path: an honest identity of any build still gets its welcome, authenticates
// and is charged nothing.
func TestHelloWithHonestKeyIsUnaffected(t *testing.T) {
	t.Parallel()
	svc, stop := startTestNodeWithoutDials(t, config.Node{ListenAddress: freeAddress(t)})
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

	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaVersion,
		Address:       id.Address,
		PubKey:        identity.PublicKeyBase64(id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(id),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+id.Address)),
	})
	if authOK := readJSONTestFrame(t, reader); authOK.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %#v", authOK)
	}
	if got := banScoreForIP(svc, "127.0.0.1"); got != 0 {
		t.Fatalf("ban score = %d for an honest handshake", got)
	}
}

// ---------------------------------------------------------------------------
// Forwarded content: refused in silence
// ---------------------------------------------------------------------------

// forgedLocalDatagram is a routed datagram addressed to svc whose author is
// the neutral key: src fingerprints to it, the signature is the universal one.
func forgedLocalDatagram(t *testing.T, svc *Service) protocol.DatagramFrame {
	t.Helper()
	honest := localDatagram(t, svc)
	forged := honest.Clone()
	forged.Src = domain.PeerIdentityFromWire(forgedIdentityAddress())
	forged.Auth.PubKey = edforgery.NeutralPublicKey()
	forged.Auth.Sig = edforgery.UniversalSignature()
	return forged
}

// localDatagram is a genuinely signed, fresh push_identity datagram for svc.
func localDatagram(t *testing.T, svc *Service) protocol.DatagramFrame {
	t.Helper()
	return newNodeDatagram(t, func(frame *protocol.DatagramFrame) {
		frame.Dst = domain.PeerIdentityFromWire(svc.identity.Address)
		frame.TTL = 1
		frame.Auth.MaxTTL = 1
		frame.Auth.Time = time.Now().UTC().Unix()
	})
}

// TestForwardedDatagramWithSmallOrderSignerCostsTheNeighbourNothing: the
// neighbour on this connection relayed a datagram whose AUTHOR holds a
// small-order key. The frame is refused under its own reason, and the
// neighbour's ban score stays at zero. The control on a second fixture shows
// the ban surface is live: a forged signature under an honest key — which
// every build refuses, so relaying it IS the neighbour's own violation — is
// still charged.
func TestForwardedDatagramWithSmallOrderSignerCostsTheNeighbourNothing(t *testing.T) {
	t.Parallel()

	svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	if svc.dispatchInboundDatagramLine(connID, mustDatagramLine(t, forgedLocalDatagram(t, svc))) {
		t.Fatal("a datagram signed under the neutral key was accepted")
	}
	if got := datagramDropCount(svc, datagram.DropSignerKey); got != 1 {
		t.Fatalf("invalid_signer_key drops = %d, want 1", got)
	}
	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d: the relay is not the author of the key it forwarded", got)
	}

	control, _, controlID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	forgedSig := localDatagram(t, control).Clone()
	forgedSig.Auth.Sig[0] ^= 0xff
	if control.dispatchInboundDatagramLine(controlID, mustDatagramLine(t, forgedSig)) {
		t.Fatal("a datagram with a forged signature was accepted")
	}
	if got := datagramDropCount(control, datagram.DropSignature); got != 1 {
		t.Fatalf("invalid_signature drops = %d, want 1: the control never reached the signature check", got)
	}
	if banScoreForIP(control, datagramTestPeerIP) == 0 {
		t.Fatal("the ban surface is unreachable on this fixture, so the zero above proves nothing")
	}
}

// forgedDMEnvelope is a dm-v1 envelope "from" the neutral key's address,
// signed with the universal signature.
func forgedDMEnvelope(t *testing.T, recipient string) string {
	t.Helper()
	part := map[string]string{"ephemeral": "e", "nonce": "n", "data": "d"}
	encoded, err := json.Marshal(map[string]any{
		"version":   "dm-v1",
		"from":      forgedIdentityAddress(),
		"to":        recipient,
		"recipient": part,
		"sender":    part,
		"signature": base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature()),
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded)
}

// TestRelayedDMWithSmallOrderSenderKeyIsRefusedWithoutBan: a relay hands
// over a DM whose attached sender key is the neutral key. Before the key
// check the envelope verified and the message was stored as coming from that
// address. It is now refused, the key is not cached, and the relay is not
// charged.
func TestRelayedDMWithSmallOrderSenderKeyIsRefusedWithoutBan(t *testing.T) {
	t.Parallel()
	svc, _, connID := newRoutableDatagramInboundFixture(t)

	const messageID = "forged-dm-small-order"
	svc.handleInboundPushMessage(connID, protocol.Frame{
		Type:   "push_message",
		Topic:  "dm",
		PubKey: forgedKeyBase64(),
		Item: &protocol.MessageFrame{
			ID:         messageID,
			Sender:     forgedIdentityAddress(),
			Recipient:  svc.identity.Address,
			Flag:       string(protocol.MessageFlagSenderDelete),
			CreatedAt:  time.Now().UTC().Format(time.RFC3339),
			TTLSeconds: 0,
			Body:       forgedDMEnvelope(t, svc.identity.Address),
		},
	})

	svc.gossipMu.RLock()
	stored := svc.seen.Has(messageID)
	svc.gossipMu.RUnlock()
	if stored {
		t.Fatal("a DM signed under the neutral key was stored")
	}
	svc.knowledgeMu.RLock()
	_, cached := svc.pubKeys[forgedIdentityAddress()]
	svc.knowledgeMu.RUnlock()
	if cached {
		t.Fatal("the attached small-order key entered the knowledge store")
	}
	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d: the relay is not the author of the DM it forwarded", got)
	}
}

// TestPushIdentityWithSmallOrderRecordKeyKeepsTheSession: a record carrying
// a small-order key is refused without closing the session it arrived on;
// the control — a record that fails for a reason of its own making, a bad
// signature under an honest key — still closes it, so the observable is live.
func TestPushIdentityWithSmallOrderRecordKeyKeepsTheSession(t *testing.T) {
	t.Parallel()
	const sessionConn = domain.ConnID(9)

	newHandler := func(t *testing.T) (*pushIdentityHandler, *Service, *netcoretest.Backend) {
		t.Helper()
		backend := netcoretest.New()
		t.Cleanup(backend.Shutdown)
		backend.Register(sessionConn, netcore.Inbound, "10.0.0.91:64646")
		svc := NewServiceWithNetwork(config.Node{
			ListenAddress:    "127.0.0.1:0",
			Type:             config.NodeTypeFull,
			TrustStorePath:   t.TempDir() + "/trust.json",
			EnableDatagramV1: true,
		}, testIdentityForNetworkConsumerTest(t), backend)
		t.Cleanup(svc.WaitBackground)
		return &pushIdentityHandler{
			svc: svc, network: testRecordStoreNetwork,
			clock:          func() time.Time { return time.Now().UTC() },
			lastAcceptedAt: map[pushSessionKey]time.Time{},
			violationAt:    map[pushSessionKey]time.Time{},
		}, svc, backend
	}

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	t.Run("small_order_record_key", func(t *testing.T) {
		handler, svc, backend := newHandler(t)
		body, err := json.Marshal(map[string]any{
			"address":   forgedIdentityAddress(),
			"pubkey":    forgedKeyBase64(),
			"dm":        false,
			"issued_at": uint64(1780000000),
			"seq":       uint64(1),
		})
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		record := protocol.SignedIdentityRecord{
			Version: domain.IdentityRecordVersion,
			Body:    body,
			Sig:     edforgery.UniversalSignature(),
		}
		delivery, payload := newPushDelivery(t, svc, peer, peerID, record)
		if result := handler.Handle(context.Background(), delivery, payload); result.Outcome() == datagram.HandlerAccepted {
			t.Fatal("a record under a small-order key was accepted")
		}
		if backend.RemoteAddr(sessionConn) == "" {
			t.Fatal("the session was closed for a key that is the record's, not the peer's")
		}
	})

	t.Run("control_bad_signature", func(t *testing.T) {
		handler, svc, backend := newHandler(t)
		record, _ := issueTestRecord(t, peer, 1, true)
		record.Sig = append([]byte(nil), record.Sig...)
		record.Sig[0] ^= 0xff
		delivery, payload := newPushDelivery(t, svc, peer, peerID, record)
		if result := handler.Handle(context.Background(), delivery, payload); result.Outcome() == datagram.HandlerAccepted {
			t.Fatal("a record with a forged signature was accepted")
		}
		if backend.RemoteAddr(sessionConn) != "" {
			t.Fatal("the session survived its own invalid record; the observable above is vacuous")
		}
	})
}

// TestStoredSmallOrderKeyNeverVerifiesARouteClaim: a small-order key that sits
// in the knowledge store (written before the ingest paths refused it) reads as
// an unknown key. Before, the universal signature under it earned the
// attested-route trust bonus.
func TestStoredSmallOrderKeyNeverVerifiesARouteClaim(t *testing.T) {
	t.Parallel()
	svc, _ := newTestServiceWithIdentity(t)
	registerKnownPubKey(t, svc, forgedIdentityAddress(), edforgery.NeutralPublicKey())

	if _, ok := svc.publicKeyForIdentity(domain.PeerIdentityFromWire(forgedIdentityAddress())); ok {
		t.Fatal("publicKeyForIdentity returned a small-order key")
	}

	frames := []protocol.AnnounceRouteFrame{{Identity: forgedIdentityAddress(), Origin: idOriginC.String(), Hops: 1, SeqNo: 1}}
	sigs := [][]byte{edforgery.UniversalSignature()}
	_, _, verified := svc.verifyRouteAnnounceV3Sigs(idOriginC, frames, sigs, 1)
	if len(verified) != 1 || verified[0] {
		t.Fatalf("verified = %v: a claim signed under the neutral key earned the trust bonus", verified)
	}
}
