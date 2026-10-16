package node

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// An old node of each released version (28, 29, 30) dials the node under
// test with a hello naming an identity that proved v2 there. The node refuses
// it at the hello, as before, and now says so in a frame the old node already
// understands — connection_notice{code=peer-banned, reason=peer-ban} — before
// it closes. The old node records the refusal against the address it dialled,
// and its dial gate stops offering that address, so the refusals do not turn
// into a storm of redials.
func TestAnOldDiallerNamingAPinnedIdentityIsToldItIsRefused(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			pinned := domain.PeerIdentity{0x65, 0x01}
			newAddress := freeAddress(t)
			oldDialsNew := startImpersonatingProxy(t, newAddress, oldPeerDials, version, pinned)

			newNode, stopNew := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  newAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(func(svc *Service) {
				if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
					t.Fatalf("pin: %v", err)
				}
			}))
			defer stopNew()

			old, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  freeAddress(t),
				BootstrapPeers: []string{oldDialsNew.address()},
				Type:           config.NodeTypeFull,
			}, asOldNode)
			defer stopOld()
			dialled := domain.PeerAddress(normalizeAddress(oldDialsNew.address()))

			waitForConditionMsg(t, 20*time.Second, "the old node never recorded the refusal as a peer-ban notice", func() bool {
				return remoteBanReason(old, dialled) == string(protocol.PeerBannedReasonPeerBan)
			})
			if newNode.SecureSessionStoreStats().V1RefusalsIdentityPinned == 0 {
				t.Fatalf("v%d: the old node was banned by something other than the pin refusal", version)
			}
			if !oldDialsNew.nodeSentFrame(protocol.FrameTypeConnectionNotice) {
				t.Fatalf("v%d: the node under test sent no connection_notice", version)
			}
			if oldDialsNew.nodeSentFrame("welcome") {
				t.Fatalf("v%d: the node sent a welcome to a v1 hello naming a pinned identity", version)
			}
			for _, candidate := range old.peerProvider.Candidates() {
				if candidate.Address == dialled {
					t.Fatalf("v%d: the old node still offers %s for a dial after the refusal notice", version, dialled)
				}
			}
		})
	}
}

// remoteBanReason is the reason svc recorded for a peer-banned notice from
// address, "" when it recorded none.
func remoteBanReason(svc *Service, address domain.PeerAddress) string {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	entry := svc.persistedMeta[address]
	if entry == nil || entry.RemoteBannedUntil == nil {
		return ""
	}
	return entry.RemoteBanReason
}

// An identity that proves itself over v2 between a v1 hello naming it and
// that connection's auth_session is refused at the auth_session, and the
// dialler is told so the same way as a refusal at the hello: a peer-banned
// notice with reason peer-ban comes before the error.
func TestAV1DiallerRefusedAtAuthSessionIsToldItIsRefused(t *testing.T) {
	t.Parallel()
	svc, stop := startQuietNode(t, nil)
	defer stop()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersionSecureSession - 1,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          config.CorsaVersion,
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected a welcome with a challenge, got %#v", welcome)
	}
	if err := svc.secureSessions.store.noteProvenInbound(domain.PeerIdentityFromWire(id.Address)); err != nil {
		t.Fatalf("pin: %v", err)
	}
	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, protocol.SessionAuthPayload(welcome.Challenge, id.Address)),
	})

	notice := readJSONTestFrame(t, reader)
	if notice.Type != protocol.FrameTypeConnectionNotice || notice.Code != protocol.ErrCodePeerBanned {
		t.Fatalf("the first answer to the refused auth_session is %#v, want connection_notice peer-banned", notice)
	}
	details, until, err := protocol.ParsePeerBannedDetails(notice.Details)
	if err != nil || details.Reason != protocol.PeerBannedReasonPeerBan || !until.IsZero() {
		t.Fatalf("notice details %+v, until %v, err %v; want reason peer-ban without until", details, until, err)
	}
	if refusal := readJSONTestFrame(t, reader); refusal.Type != "error" || refusal.Code != protocol.ErrCodeAuthRequired {
		t.Fatalf("after the notice: %#v, want the auth-required error", refusal)
	}
	if nodeAuthenticatedIdentity(svc, domain.PeerIdentityFromWire(id.Address)) {
		t.Fatal("the refused identity was authenticated over v1")
	}
}
