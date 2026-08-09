package node

import (
	"bufio"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_wire_test.go covers the wire-level obligations of
// docs/refactoring/datagram-transport.md §9:
//
//   - line 1073: a raw line reaches the STRICT parser on BOTH directions;
//     duplicate `type`, `ttl` and `auth` are rejected; not one datagram field
//     is lost in the outbound-session read loop, and a datagram sent over an
//     outbound session does arrive and parse (the one-directional
//     disappearance regression);
//   - line 1074: a frame above 128 KiB is refused on inbound TCP AND in the
//     peer-session read loop, which itself accepts up to 8 MiB, with the
//     budget counted on len(line) including the newline;
//   - line 1091: the size boundary measured on REAL encryption of the largest
//     chunk, on both receive paths.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const datagramTestDstHex = "00f39d89f345eb1613bb2fa02ee883a214a6a697"

// datagramTestKey is the deterministic signing key of these tests. A fixed
// seed keeps `src` stable, which matters: src is Fingerprint(pubkey) and the
// parser checks the canonical form of both.
func datagramTestKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 7)
	}
	return ed25519.NewKeyFromSeed(seed)
}

// newNodeDatagram builds a fully populated, signed routed datagram. Every
// optional header field is present on purpose — route_policy and the whole
// auth block are exactly what a lossy read loop would drop silently.
func newNodeDatagram(t *testing.T, mutate func(*protocol.DatagramFrame)) protocol.DatagramFrame {
	t.Helper()
	key := datagramTestKey(t)
	src, err := domain.ParsePeerIdentity(identity.Fingerprint(key.Public().(ed25519.PublicKey)))
	if err != nil {
		t.Fatalf("ParsePeerIdentity(src): %v", err)
	}
	dst, err := domain.ParsePeerIdentity(datagramTestDstHex)
	if err != nil {
		t.Fatalf("ParsePeerIdentity(dst): %v", err)
	}
	payload := make([]byte, 24)
	for i := range payload {
		payload[i] = byte(0x30 + i)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	for i := range salt {
		salt[i] = byte(0xb0 + i)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         src,
		Dst:         dst,
		TTL:         9,
		RoutePolicy: domain.RoutePolicyExplore,
		DType:       domain.DType("push_identity"),
		Payload:     payload,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        salt,
			MaxTTL:      10,
			Time:        1780000000,
		},
	}
	if mutate != nil {
		mutate(&frame)
	}
	signed, err := protocol.SignDatagram(frame, datagramTestNetwork(), key)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return signed
}

// datagramTestNetwork is the network id bound into the transcript. Only the
// signature depends on it, and none of these tests verifies a signature —
// they test the wire path, which is deliberately network-agnostic.
func datagramTestNetwork() domain.NetworkID { return domain.NetworkID("gazeta-devnet") }

func mustDatagramLine(t *testing.T, frame protocol.DatagramFrame) string {
	t.Helper()
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	return line
}

// newDatagramInboundFixture builds an authenticated inbound connection that
// has negotiated the capabilities passed in, wired the way production wires
// one: NetCore in the registry, auth state verified, Network surface backed
// by netcoretest.
func newDatagramInboundFixture(t *testing.T, caps ...domain.Capability) (*Service, *netcoretest.Backend, domain.ConnID) {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7700)
	backend.Register(connID, netcore.Inbound, "10.0.0.77:64646")

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(connID, serverPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)
	// The VERIFIED hello is what production leaves behind on an accepted
	// connection, and it is where the proof of identity lives: handleAuthSession
	// stores the state connauth.VerifyAuthSession returned, whose Hello.Address
	// the signature was checked against. The NetCore identity mirror is set from
	// the same value on one branch of that function, so the fixture sets both —
	// a fixture that only mirrored would let the ingress key a budget on
	// something no peer ever proved.
	pc.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: datagramTestDstHex}})
	pc.SetCapabilities(caps)
	pc.SetIdentity(domain.PeerIdentityFromWire(datagramTestDstHex))

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
	svc.peerMu.Unlock()

	return svc, backend, connID
}

// newLayeredDatagramInboundFixture is newDatagramInboundFixture with the
// CONVEYOR wired — the production shape of the ingress.
//
// The distinction is not cosmetic. Without a layer the ingress stops at the
// strict parser and every test of §9 lines 1073-1074 exercises the fall-back
// branch, not the `layer != nil → pipeline.HandleInbound` path a running node
// takes. The baseline kit is registered too, because §6.1 makes advertising
// mesh_datagram_v1 a promise to handle it.
func newLayeredDatagramInboundFixture(
	t *testing.T,
	caps ...domain.Capability,
) (*Service, *netcoretest.Backend, domain.ConnID) {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		EnableDatagramV1: true,
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must build the conveyor: the ingress path under test only exists with one")
	}
	requireDatagramPlane(t, svc)
	registerFixtureDatagramTypes(t, svc)

	connID := netcore.ConnID(7701)
	backend.Register(connID, netcore.Inbound, "10.0.0.78:64646")

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(connID, serverPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)
	// The VERIFIED hello is what production leaves behind on an accepted
	// connection, and it is where the proof of identity lives: handleAuthSession
	// stores the state connauth.VerifyAuthSession returned, whose Hello.Address
	// the signature was checked against. The NetCore identity mirror is set from
	// the same value on one branch of that function, so the fixture sets both —
	// a fixture that only mirrored would let the ingress key a budget on
	// something no peer ever proved.
	pc.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: datagramTestDstHex}})
	pc.SetCapabilities(caps)
	pc.SetIdentity(domain.PeerIdentityFromWire(datagramTestDstHex))

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
	svc.peerMu.Unlock()

	return svc, backend, connID
}

// newDatagramOutboundFixture builds an outbound peer session that negotiated
// the given capabilities — the other half of the wire.
func newDatagramOutboundFixture(t *testing.T, caps ...domain.Capability) (*Service, domain.PeerAddress, *peerSession) {
	t.Helper()
	svc, _ := newTestServiceWithIdentity(t)
	// The struct-literal fixture skips NewService, so the counter set the
	// ingress reports through has to be wired explicitly. Every method is
	// nil-safe, which would make a missing set silently unobservable rather
	// than a panic — hence the explicit assignment here.
	svc.datagramMetrics = datagram.NewMetrics()
	address := domain.PeerAddress("addr-datagram-peer")
	session := &peerSession{
		address: address,
		// A struct-literal session still has to carry the connection id the real
		// one is built with: it is the datagram plane's CHANNEL, and the ingress
		// refuses an arrival that names none (errDatagramNoChannel).
		connID:       domain.ConnID(7101),
		peerIdentity: domain.PeerIdentityFromWire(datagramTestDstHex),
		capabilities: caps,
		sendCh:       make(chan peerSendItem, 4),
		authOK:       true,
	}
	svc.peerMu.Lock()
	svc.sessions[address] = session
	svc.health = map[domain.PeerAddress]*peerHealth{address: {Connected: true}}
	svc.peerMu.Unlock()
	return svc, address, session
}

// provenDatagramNeighbour is the ingress view of a neighbour whose identity
// this node PROVED — the accepted-connection shape — for the tests that drive
// handleDatagramFrame directly instead of through a dispatcher.
func provenDatagramNeighbour(peer domain.PeerIdentity, direction datagramDirection) datagramNeighbour {
	return datagramNeighbour{
		direction: direction,
		budgetKey: datagram.ProvenIdentityKey(peer),
		// One channel per fixture neighbour: the ingress requires one, and a
		// shared constant would make two neighbours indistinguishable to every
		// channel-relative decision behind it.
		channel:     datagram.NetworkChannel(domain.ConnID(7201)),
		identity:    peer,
		label:       peer.String(),
		speaksPlane: true,
	}
}

// deliverOutboundSessionLine feeds ONE wire line to the outbound-session
// receive path in the order readPeerSession applies it: admission first
// (§4.1 step 1), then the classification-driven diversion to the ingress.
//
// It exists because a datagram no longer travels through the universal Frame at
// all, so a test can no longer reach the ingress by handing
// dispatchPeerSessionFrame a synthetic protocol.Frame — that route now lands on
// the unreachable-residue guard, which is the whole point of finding 1. Tests
// that need the real socket loop use newReadPeerSessionFixture; this is for the
// struct-literal fixtures, and it deliberately mirrors the loop rather than the
// dispatcher.
func deliverOutboundSessionLine(t *testing.T, svc *Service, session *peerSession, line string) {
	t.Helper()
	if dropped, _ := svc.refuseUnadmissibleFrameLine(session, line); dropped {
		return
	}
	if !isDatagramWireLine(line) {
		t.Fatalf("the fixture line is not classified as a datagram, so the diversion under test never fires: %.64q", line)
	}
	svc.dispatchSessionDatagramLine(session, line)
}

// datagramDropCount returns how many refusals of one reason the ingress has
// counted. The metric is the only observable the wire layer has — every
// refusal is silent on the wire by contract (§2).
func datagramDropCount(svc *Service, reason datagram.DropReason) uint64 {
	return svc.datagramMetrics.Snapshot().DropsByReason[reason.String()]
}

func datagramObservedCount(svc *Service) uint64 {
	return svc.datagramMetrics.Snapshot().Observed
}

// newTestBufioReader feeds a wire line to the production readFrameLine so the
// size gates are exercised through the real reader rather than a paraphrase.
func newTestBufioReader(line string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(line))
}

// ---------------------------------------------------------------------------
// Dispatch surface (§3.4)
// ---------------------------------------------------------------------------

// TestDatagramIsRawLineBacked pins the §3.4 requirement that dispatch keys on
// the TOP-LEVEL type and that the type uses the RawLine bypass. Without the
// entry the outbound-session dispatcher re-parses an empty string and the
// frame disappears in exactly one direction of the wire — the bug
// route_announce_v3 and route_poison_v1 were both fixed for.
func TestDatagramIsRawLineBacked(t *testing.T) {
	t.Parallel()

	if !isRawLineBackedFrameType(protocol.DatagramFrameType) {
		t.Fatal("isRawLineBackedFrameType(datagram) = false: the strict parser of §3.4 would never see the original bytes on the outbound path")
	}
	// The inner dtype must NOT be a dispatch key: it is a datagram-internal
	// field the universal Frame does not even carry.
	if isRawLineBackedFrameType("push_identity") {
		t.Fatal("isRawLineBackedFrameType(push_identity) = true: dispatch must key on the top-level type, never on dtype")
	}
}

// TestDatagramIsP2PWireCommand pins the auth-gate classification: an
// unauthenticated peer must be told auth_required, not unknown_command.
func TestDatagramIsP2PWireCommand(t *testing.T) {
	t.Parallel()

	if !isP2PWireCommand(protocol.DatagramFrameType) {
		t.Fatal("datagram missing from p2pWireCommands: an unauthenticated peer would get unknown_command instead of auth_required")
	}
	if !isFireAndForgetFrame(protocol.DatagramFrameType) {
		t.Fatal("isFireAndForgetFrame(datagram) = false: the session would block waiting for a reply a datagram never sends")
	}
	// A datagram may land on an outbound session at any moment, including
	// while a synchronous request is waiting for its reply. Without this
	// classification the request loop would take the datagram AS the reply
	// and drop it — the same swallow bug the route_sync pair was fixed for.
	if !isUnsolicitedSessionFrame(protocol.DatagramFrameType) {
		t.Fatal("isUnsolicitedSessionFrame(datagram) = false: a datagram arriving mid-request would be consumed as the reply")
	}
}

// TestWideFrameLineBudgetIsAClosedAllowlist pins the inversion of finding 2:
// the set that BUYS the 8 MiB response budget is the enumerated one, and
// everything else — the datagram and announce planes, the response types that
// batch nothing, and every type this build has never heard of — is capped at
// MaxFrameLine.
//
// The mutation this kills: restating the rule as "the complement of the strict
// set", under which `some_future_reply_v9` below silently regains 8 MiB.
func TestWideFrameLineBudgetIsAClosedAllowlist(t *testing.T) {
	t.Parallel()

	// Each member is here because this node's own limits let it past 128 KiB;
	// see hasWideFrameLineBudget for the derivation of each.
	for _, frameType := range []string{"contacts", "push_message"} {
		if !hasWideFrameLineBudget(frameType) {
			t.Errorf("hasWideFrameLineBudget(%q) = false: a reply this node's own limits let past 128 KiB would be dropped", frameType)
		}
	}

	capped := []string{
		protocol.DatagramFrameType,
		"announce_routes",
		"routes_update",
		"request_resync",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
		protocol.RoutePoisonV2FrameType,
		protocol.RouteSyncDigestFrameType,
		protocol.FileCommandFrameType,
		"relay_message",
		"peers",
		// Not reachable on this reader at all: fetch_messages / fetch_inbox are
		// not P2P wire commands, so their replies never travel here. Granting
		// them the wide budget would buy an attacker one 8 MiB decode per frame
		// for a reply the dispatcher then drops unhandled.
		"messages",
		"inbox",
		// The closure itself.
		"some_future_reply_v9",
		"",
	}
	for _, frameType := range capped {
		if hasWideFrameLineBudget(frameType) {
			t.Errorf("hasWideFrameLineBudget(%q) = true: the entitled set is not closed", frameType)
		}
	}

	if isAnnouncePlaneFrameType(protocol.DatagramFrameType) {
		t.Error("isAnnouncePlaneFrameType(datagram) = true: a datagram is not an announce frame, and every reader of that predicate would now be wrong")
	}
}

// ---------------------------------------------------------------------------
// End to end, both directions (§9 line 1073)
// ---------------------------------------------------------------------------

// TestDatagramReachesStrictParser_InboundTCP walks the inbound path: a raw
// wire line handed to dispatchNetworkFrame must arrive at the strict parser
// byte for byte.
func TestDatagramReachesStrictParser_InboundTCP(t *testing.T) {
	svc, _, connID := newDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))

	if !svc.dispatchNetworkFrame(connID, strings.TrimSuffix(line, "\n")) {
		t.Fatal("dispatchNetworkFrame(datagram) returned false: a datagram must never tear the connection down")
	}
	if got := datagramObservedCount(svc); got != 1 {
		t.Fatalf("ingress observed %d frames, want 1 — the raw line did not reach the strict parser", got)
	}
	if got := datagramDropCount(svc, datagram.DropMalformed); got != 0 {
		t.Fatalf("a well-formed datagram was counted malformed %d times", got)
	}
}

// TestDatagramReachesStrictParser_OutboundSession is the same assertion on
// the other direction, through the order readPeerSession applies: admission,
// then the pre-parse diversion straight to the ingress.
func TestDatagramReachesStrictParser_OutboundSession(t *testing.T) {
	svc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))

	deliverOutboundSessionLine(t, svc, session, line)

	if got := datagramObservedCount(svc); got != 1 {
		t.Fatalf("ingress observed %d frames, want 1 — the datagram disappeared in the outbound direction of the wire", got)
	}
	if got := datagramDropCount(svc, datagram.DropMalformed); got != 0 {
		t.Fatalf("a well-formed datagram was counted malformed %d times", got)
	}
}

// TestDatagramOutboundSessionLosesNoField is the field-preservation half of
// §9 line 1073. The universal Frame cannot carry mode, class, src, dst, ttl,
// route_policy, dtype, payload or auth, so a read loop that forwarded the
// PARSED frame instead of the raw line would silently drop all of them.
// Re-parsing what the loop hands to dispatch must reproduce the original frame
// field for field.
func TestDatagramOutboundSessionLosesNoField(t *testing.T) {
	t.Parallel()

	original := newNodeDatagram(t, nil)
	line := mustDatagramLine(t, original)
	trimmed := strings.TrimSpace(line)

	frame, err := protocol.ParseFrameLine(trimmed)
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if isRawLineBackedFrameType(frame.Type) {
		frame.RawLine = trimmed
	}

	// What the dispatcher hands the strict parser.
	roundTripped, err := protocol.ParseDatagramFrameLine(frame.RawLine)
	if err != nil {
		t.Fatalf("ParseDatagramFrameLine(RawLine): %v", err)
	}

	if roundTripped.Version != original.Version ||
		roundTripped.Mode != original.Mode ||
		roundTripped.Class != original.Class ||
		roundTripped.Src != original.Src ||
		roundTripped.Dst != original.Dst ||
		roundTripped.TTL != original.TTL ||
		roundTripped.RoutePolicy != original.RoutePolicy ||
		roundTripped.DType != original.DType {
		t.Fatalf("header lost fields through the outbound read loop:\n got %+v\nwant %+v", roundTripped, original)
	}
	if string(roundTripped.Payload) != string(original.Payload) {
		t.Fatalf("payload lost: got %x, want %x", roundTripped.Payload, original.Payload)
	}
	if roundTripped.Auth == nil {
		t.Fatal("auth block lost through the outbound read loop")
	}
	if roundTripped.Auth.AuthVersion != original.Auth.AuthVersion ||
		roundTripped.Auth.MaxTTL != original.Auth.MaxTTL ||
		roundTripped.Auth.Time != original.Auth.Time ||
		string(roundTripped.Auth.Salt) != string(original.Auth.Salt) ||
		string(roundTripped.Auth.PubKey) != string(original.Auth.PubKey) ||
		string(roundTripped.Auth.Sig) != string(original.Auth.Sig) {
		t.Fatalf("auth lost fields: got %+v, want %+v", roundTripped.Auth, original.Auth)
	}
	// The signature survives the whole path, which is the strongest single
	// statement of "no field changed": the transcript covers every one of
	// them.
	if err := protocol.VerifyDatagramSignature(roundTripped, datagramTestNetwork()); err != nil {
		t.Fatalf("signature broke through the outbound read loop: %v", err)
	}
}

// TestDatagramOutboundSessionDropsEmptyRawLine proves the failure mode the
// RawLine population prevents: with the pre-fix reader output the dispatcher
// has nothing to parse and the frame is dropped. Without this assertion the
// test above could pass even if dispatch fell back to universal Frame fields.
func TestDatagramOutboundSessionDropsEmptyRawLine(t *testing.T) {
	svc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)

	svc.dispatchSessionDatagramLine(session, "")

	if got := datagramDropCount(svc, datagram.DropMalformed); got != 1 {
		t.Fatalf("an empty line must be refused by the strict parser; malformed drops = %d, want 1", got)
	}
}

// TestDatagramDuplicateKeysRejected is the strict-parsing half of §9 line
// 1073, exercised through the REAL dispatchers on both directions: a repeated
// `type`, `ttl` or `auth` must be refused. encoding/json collapses duplicate
// keys silently, so a datagram that reached dispatch as a universal Frame
// would accept every one of these.
func TestDatagramDuplicateKeysRejected(t *testing.T) {
	valid := mustDatagramLine(t, newNodeDatagram(t, nil))
	valid = strings.TrimSuffix(valid, "\n")

	cases := map[string]string{
		"duplicate_type": injectDuplicateJSONKey(t, valid, `"type":"datagram"`),
		"duplicate_ttl":  injectDuplicateJSONKey(t, valid, `"ttl":9`),
		"duplicate_auth": injectDuplicateJSONKey(t, valid, `"auth":{"av":1,"pubkey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","salt":"AAAAAAAAAAAAAAAAAAAAAA","max_ttl":10,"time":1780000000,"sig":"`+strings.Repeat("A", 86)+`"}`),
	}

	for name, line := range cases {
		t.Run(name+"_inbound", func(t *testing.T) {
			svc, _, connID := newDatagramInboundFixture(t, domain.CapMeshDatagramV1)
			if !svc.dispatchNetworkFrame(connID, line) {
				t.Fatal("a refused datagram must not tear the connection down")
			}
			if got := datagramDropCount(svc, datagram.DropMalformed); got != 1 {
				t.Fatalf("malformed drops = %d, want 1 — the duplicate key was not rejected", got)
			}
		})
		t.Run(name+"_outbound", func(t *testing.T) {
			svc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
			svc.dispatchSessionDatagramLine(session, line)
			if got := datagramDropCount(svc, datagram.DropMalformed); got != 1 {
				t.Fatalf("malformed drops = %d, want 1 — the duplicate key was not rejected", got)
			}
		})
	}
}

// injectDuplicateJSONKey appends a second copy of a key to an otherwise valid
// datagram line. Producing the shape by string surgery is deliberate: no
// marshaller in the tree can emit a duplicate key, and that is exactly why
// the parser has to scan for one.
func injectDuplicateJSONKey(t *testing.T, line string, fragment string) string {
	t.Helper()
	if !strings.HasSuffix(line, "}") {
		t.Fatalf("expected a JSON object line, got %q", line)
	}
	return line[:len(line)-1] + "," + fragment + "}"
}

// TestDatagramWithoutCapabilityIsDroppedSilently is the mixed-network half:
// a node that never negotiated mesh_datagram_v1 must drop the frame without
// reaching the parser, without an error frame and without closing — the
// existing rules, applied without a crash.
func TestDatagramWithoutCapabilityIsDroppedSilently(t *testing.T) {
	svc, backend, connID := newDatagramInboundFixture(t, domain.CapMeshRelayV1)
	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

	if !svc.dispatchNetworkFrame(connID, line) {
		t.Fatal("a capability-gated drop must keep the connection alive")
	}
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("the ingress observed %d frames on a node without the capability, want 0 — the gate must stand before the parser", got)
	}
	select {
	case data := <-backend.Outbound(connID):
		t.Fatalf("a capability-gated drop must be silent on the wire; got %q", data)
	default:
	}

	// The outbound direction refuses the same way.
	outSvc, _, session := newDatagramOutboundFixture(t, domain.CapMeshRelayV1)
	deliverOutboundSessionLine(t, outSvc, session, line)
	if got := datagramObservedCount(outSvc); got != 0 {
		t.Fatalf("outbound session without the capability observed %d frames, want 0", got)
	}
}

// TestDatagramUnauthenticatedPeerGetsAuthRequired pins the classification a
// legacy or hostile peer sees before auth: the command exists on this port,
// so the answer is auth_required, never unknown_command.
func TestDatagramUnauthenticatedPeerGetsAuthRequired(t *testing.T) {
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7799)
	backend.Register(connID, netcore.Inbound, "10.0.0.79:64646")

	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")
	if svc.dispatchNetworkFrame(connID, line) {
		t.Fatal("an unauthenticated P2P command must be refused")
	}

	select {
	case data := <-backend.Outbound(connID):
		frame, err := parseFrameLineForTest(data)
		if err != nil {
			t.Fatalf("parse reply: %v (raw=%q)", err, data)
		}
		if frame.Code != protocol.ErrCodeAuthRequired {
			t.Fatalf("reply code = %q, want %q — datagram is missing from p2pWireCommands", frame.Code, protocol.ErrCodeAuthRequired)
		}
	default:
		t.Fatal("no reply frame: the auth gate must answer auth_required")
	}
}

// ---------------------------------------------------------------------------
// MaxFrameLine on receive (§9 line 1074)
// ---------------------------------------------------------------------------

// TestWireLineBudgetCountsTheNewline pins the accounting rule of §2.3:
// the budget is len(line) INCLUDING the terminating newline, whether or not
// the caller has already stripped it. The peer-session reader hands over a
// trimmed line and the inbound reader hands over the line as read, so a
// helper that disagreed by one byte would move the boundary between the two
// paths.
func TestWireLineBudgetCountsTheNewline(t *testing.T) {
	t.Parallel()

	if got := wireLineBudget("{}"); got != 3 {
		t.Fatalf("wireLineBudget(trimmed) = %d, want 3 (2 bytes + the newline the sender wrote)", got)
	}
	if got := wireLineBudget("{}\n"); got != 3 {
		t.Fatalf("wireLineBudget(with newline) = %d, want 3", got)
	}

	exact := strings.Repeat("x", protocol.MaxFrameLine-1)
	if exceedsDatagramFrameLine(exact) {
		t.Fatalf("a line of exactly MaxFrameLine bytes including the newline must pass; budget = %d", wireLineBudget(exact))
	}
	if !exceedsDatagramFrameLine(exact + "x") {
		t.Fatal("one byte past MaxFrameLine must be refused")
	}
}

// TestDatagramOversizeRefusedOnInboundTCP covers the inbound half of §9 line
// 1074 at both of its gates: the command reader itself (bound by
// maxCommandLineBytes) and the ingress, which repeats the check because the
// peer-session path reaches it through a much wider reader.
func TestDatagramOversizeRefusedOnInboundTCP(t *testing.T) {
	oversize := oversizeDatagramLine(t)

	// Gate 1: the inbound command reader never even hands it to dispatch.
	if _, err := readFrameLine(newTestBufioReader(oversize), maxCommandLineBytes); err == nil {
		t.Fatal("the inbound command reader accepted a datagram above maxCommandLineBytes")
	}

	// Gate 2: the ingress refuses it in its own right — on the PRODUCTION
	// path, with the conveyor wired, because that is the branch a running
	// node takes — and the connection survives: a size violation is a silent
	// drop, not a tear-down.
	//
	// The assertion is on a reason ONLY THIS GATE produces. While the gate
	// shared `malformed` with the strict parser, deleting the gate left the
	// whole package green: the oversize line simply fell through to the
	// parser, which rejected it and incremented the very counter the test
	// was reading.
	svc, _, connID := newLayeredDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	if !svc.dispatchNetworkFrame(connID, strings.TrimSuffix(oversize, "\n")) {
		t.Fatal("an oversize datagram must not tear the connection down")
	}
	if got := datagramDropCount(svc, datagram.DropFrameTooLarge); got != 1 {
		t.Fatalf("frame_too_large drops = %d, want 1", got)
	}
	if got := datagramDropCount(svc, datagram.DropMalformed); got != 0 {
		t.Fatalf("the gate counted %d malformed drops: it must be distinguishable from a parser refusal", got)
	}

	// A line INSIDE the budget on the same fixture reaches the conveyor, so
	// the test above cannot pass by refusing everything.
	inside := mustDatagramLine(t, newNodeDatagram(t, nil))
	if !svc.dispatchNetworkFrame(connID, strings.TrimSuffix(inside, "\n")) {
		t.Fatal("a well-formed datagram must not tear the connection down")
	}
	if got := datagramDropCount(svc, datagram.DropFrameTooLarge); got != 1 {
		t.Fatalf("a legal frame was counted as oversize: %d", got)
	}
	if datagramObservedCount(svc) == 0 {
		t.Fatal("the legal frame never reached the conveyor: §9 lines 1073-1074 would be checked on the fall-back path only")
	}
}

// TestDatagramOversizeRefusedInPeerSessionLoop is the half that matters most:
// the peer-session reader accepts up to maxResponseLineBytes (8 MiB), so
// without the strict predicate a hostile peer pushes a multi-megabyte frame
// through the wide response reader and the "smaller than 128 KiB" invariant
// stops being a property of reception.
func TestDatagramOversizeRefusedInPeerSessionLoop(t *testing.T) {
	oversize := oversizeDatagramLine(t)

	// The wide reader really does accept it — that is the hole being closed.
	line, err := readFrameLine(newTestBufioReader(oversize), maxResponseLineBytes)
	if err != nil {
		t.Fatalf("precondition: the peer-session reader accepts up to 8 MiB; got %v", err)
	}
	if len(line) <= protocol.MaxFrameLine {
		t.Fatalf("precondition: the fixture must exceed MaxFrameLine; got %d bytes", len(line))
	}

	// The read loop's admission is what drops it, before any dispatch.
	if _, verdict := admitFrameLinePreParse(oversize); verdict != preParseRefuseOverBudget {
		t.Fatalf("admission verdict on an oversize datagram = %v: the peer-session read loop would let it through", verdict)
	}

	// And the ingress refuses it independently, so a future caller that
	// forgets the loop guard still cannot get an oversize frame parsed.
	svc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
	svc.handleDatagramFrame(context.Background(), oversize, svc.sessionDatagramNeighbour(session))
	if got := datagramDropCount(svc, datagram.DropFrameTooLarge); got != 1 {
		t.Fatalf("oversize drops on the outbound path = %d, want 1", got)
	}
}

// TestOversizeDatagramIsNeverBanWorthy pins the PRICE of the size gate, which
// is the one thing the two tests above do not state.
//
// §2.3 is a rule about the LINE, and the neighbour that handed the line over is
// not the author of the frame inside it: nothing in the envelope obliges a relay
// to have measured the frame the way this node does, and §4.4 reserves
// punishment for what every datagram transit IS obliged to check — the
// stable header and auth. So an oversize datagram is counted and forgotten: no ban score, no
// error frame, no tear-down.
//
// The mutation this kills: `ban: true` on the exceedsDatagramFrameLine branch of
// handleDatagramFrame, which charged banIncrementInvalidSig on every accepted
// connection that relayed one.
func TestOversizeDatagramIsNeverBanWorthy(t *testing.T) {
	oversize := oversizeDatagramLine(t)

	t.Run("outbound_session", func(t *testing.T) {
		svc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
		result := svc.handleDatagramFrame(context.Background(), oversize, svc.sessionDatagramNeighbour(session))
		if result.BanWorthy() {
			t.Fatal("an oversize datagram was charged ban points on the outbound session")
		}
		if got := datagramDropCount(svc, datagram.DropFrameTooLarge); got != 1 {
			t.Fatalf("frame_too_large drops = %d, want 1: the limit itself must still refuse the line", got)
		}
	})

	t.Run("inbound_connection", func(t *testing.T) {
		svc, backend, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		if !svc.dispatchInboundDatagramLine(connID, oversize) {
			// The refusal itself is expected; what must not happen is the ban.
			t.Log("the oversize line was refused, as it must be")
		}
		if score := banScoreForIP(svc, datagramTestPeerIP); score != 0 {
			t.Fatalf("ban score = %d for an oversize datagram: the neighbour that relayed a frame is not its author (§4.4)", score)
		}
		select {
		case data := <-backend.Outbound(connID):
			t.Fatalf("the refusal must be silent on the wire; got %q", data)
		default:
		}

		// The ban observable is LIVE on this fixture, or the assertion above
		// would hold whatever the code did.
		banned, _, bannedID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		decoy := injectDuplicateJSONKey(t, strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n"), `"ttl":9`)
		if !banned.dispatchNetworkFrame(bannedID, decoy) {
			t.Fatal("a header violation must not tear the connection down")
		}
		if score := banScoreForIP(banned, datagramTestPeerIP); score == 0 {
			t.Fatal("the ban surface is unreachable on this fixture, so the assertion above is vacuous")
		}
	})
}

// oversizeDatagramLine builds a syntactically plausible datagram line one
// byte past MaxFrameLine. The payload is filler: the size gate stands BEFORE
// the parser, so the frame never has to be valid.
func oversizeDatagramLine(t *testing.T) string {
	t.Helper()
	frame := newNodeDatagram(t, func(frame *protocol.DatagramFrame) {
		frame.Class = domain.DatagramClassBulk
		frame.Payload = make([]byte, domain.DatagramBulkPayloadCap)
	})
	line, err := protocol.MarshalDatagramFrameLineWithLimit(frame, 8*1024*1024)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLineWithLimit: %v", err)
	}

	// Even a class-capped payload base64-expands to only ~86 KiB, so the
	// oversize case has to be built by string surgery: no marshaller in the
	// tree will emit a frame this big, which is the whole point — the shape
	// exists only on the wire, produced by a peer that ignores the budget,
	// and the gate must refuse it BEFORE the parser has a chance to reject
	// it for something else.
	const marker = `"payload":"`
	at := strings.Index(line, marker)
	if at < 0 {
		t.Fatalf("marshalled datagram has no payload field: %q", line[:64])
	}
	at += len(marker)
	line = line[:at] + strings.Repeat("A", protocol.MaxFrameLine) + line[at:]
	if len(line) <= protocol.MaxFrameLine {
		t.Fatalf("fixture is only %d bytes; it must exceed MaxFrameLine %d", len(line), protocol.MaxFrameLine)
	}
	return line
}

// ---------------------------------------------------------------------------
// Size boundary on real encryption, both receive paths (§9 line 1091)
// ---------------------------------------------------------------------------

// TestMaxEncryptedChunkPassesBothReceivePaths is the boundary §2.3 demands be
// measured on real bytes: the largest chunk the file transport produces,
// through the real encryption path, with every optional header field filled,
// must be accepted on BOTH receive paths. A regression that shrinks the
// budget on either side shows up here as a refusal, not as a silent
// production drop of the largest legitimate frame.
func TestMaxEncryptedChunkPassesBothReceivePaths(t *testing.T) {
	line := maxEncryptedChunkDatagramLine(t)
	if len(line) > protocol.MaxFrameLine {
		t.Fatalf("the largest real chunk no longer fits MaxFrameLine: %d > %d", len(line), protocol.MaxFrameLine)
	}
	trimmed := strings.TrimSuffix(line, "\n")

	// Inbound TCP: through the real command reader, then the real dispatcher.
	read, err := readFrameLine(newTestBufioReader(line), maxCommandLineBytes)
	if err != nil {
		t.Fatalf("the inbound command reader refused the largest legitimate datagram: %v", err)
	}
	if len(read) != len(line) {
		t.Fatalf("the reader returned %d bytes, want %d", len(read), len(line))
	}
	inSvc, _, connID := newDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	if !inSvc.dispatchNetworkFrame(connID, trimmed) {
		t.Fatal("dispatchNetworkFrame refused the largest legitimate datagram")
	}
	if got := datagramDropCount(inSvc, datagram.DropMalformed); got != 0 {
		t.Fatalf("inbound refused the boundary frame %d times", got)
	}
	if got := datagramObservedCount(inSvc); got != 1 {
		t.Fatalf("inbound observed %d frames, want 1", got)
	}

	// Outbound session: same frame, the other direction.
	outSvc, _, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
	deliverOutboundSessionLine(t, outSvc, session, trimmed)
	if got := datagramDropCount(outSvc, datagram.DropMalformed); got != 0 {
		t.Fatalf("the outbound session refused the boundary frame %d times", got)
	}
	if got := datagramObservedCount(outSvc); got != 1 {
		t.Fatalf("outbound observed %d frames, want 1", got)
	}
}

// maxEncryptedChunkDatagramLine builds the boundary frame: a real
// EncryptFileCommandPayload ciphertext of a full-size chunk, decoded ONCE
// (the adapter's obligation, §2.3), inside a bulk datagram.
func maxEncryptedChunkDatagramLine(t *testing.T) string {
	t.Helper()
	chunk := make([]byte, domain.DefaultChunkSize)
	if _, err := rand.Read(chunk); err != nil {
		t.Fatalf("random chunk: %v", err)
	}
	response, err := json.Marshal(domain.ChunkResponsePayload{
		FileID: domain.FileID("0123456789abcdef0123456789abcdef01234567"),
		Offset: 1 << 40,
		Data:   base64.StdEncoding.EncodeToString(chunk),
		Epoch:  1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("marshal chunk response: %v", err)
	}
	boxKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate box key: %v", err)
	}
	sealed, err := directmsg.EncryptFileCommandPayload(
		base64.StdEncoding.EncodeToString(boxKey.PublicKey().Bytes()),
		domain.FileCommandPayload{Command: domain.FileActionChunkResp, Data: response},
	)
	if err != nil {
		t.Fatalf("EncryptFileCommandPayload: %v", err)
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil {
		t.Fatalf("decode sealed payload: %v", err)
	}

	frame := newNodeDatagram(t, func(frame *protocol.DatagramFrame) {
		frame.Class = domain.DatagramClassBulk
		frame.DType = domain.DType("chunk_response")
		frame.TTL = domain.DatagramDefaultMaxHops
		frame.Payload = ciphertext
	})
	return mustDatagramLine(t, frame)
}
