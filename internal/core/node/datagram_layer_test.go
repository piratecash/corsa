package node

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_layer_test.go covers the wiring of the layer into node.Service:
// the feature flag, the resolver adapters and the frame emitter.
//
// The two obligations of §9 that live here are line 1088 — the metadata a
// candidate is ranked by must describe the connection the send would really
// use — and line 1089 — the reachability probe must agree with the send over
// an unchanged routing table.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// newDatagramLayerService builds a real Service through NewService, so the
// layer is constructed exactly as production constructs it.
func newDatagramLayerService(t *testing.T, enabled bool) *Service {
	t.Helper()
	return newDatagramLayerServiceWith(t, enabled, config.NodeTypeFull)
}

// newDatagramLayerServiceOfType builds a node of the given type with the plane
// enabled and asserted present. The type registry stays empty, which is the
// PR-0 state: a caller that needs handlers registers them itself
// (registerFixtureDatagramTypes).
func newDatagramLayerServiceOfType(t *testing.T, nodeType domain.NodeType) *Service {
	t.Helper()
	svc := newDatagramLayerServiceWith(t, true, nodeType)
	requireDatagramPlane(t, svc)
	return svc
}

func newDatagramLayerServiceWith(t *testing.T, enabled bool, nodeType domain.NodeType) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64650",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              nodeType,
		AllowPrivatePeers: true,
		EnableDatagramV1:  enabled,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	return svc
}

// requireDatagramPlane asserts the fixture node really has a conveyor.
//
// It used to bring the attempt machine up — the one thing a locally originated
// datagram was gated on — and there is no longer anything to bring up: the
// layer's whole memory is an in-memory cache that starts empty and is correct
// from the first frame. What is left is the assertion, kept at every call site
// because a fixture whose flag silently failed to take effect would otherwise
// pass every test about a plane it does not have.
func requireDatagramPlane(t *testing.T, svc *Service) {
	t.Helper()
	if svc.datagramLayer() == nil {
		t.Fatal("the node has no datagram layer")
	}
}

// datagramEmitterOf returns the emitter exactly as newDatagramLayer builds it.
func datagramEmitterOf(svc *Service) datagramFrameEmitter {
	return datagramFrameEmitter{service: svc}
}

// installDatagramPeer wires one peer with the connections the test describes
// and returns its identity. Outbound sessions and inbound connections are
// registered exactly where the production send path looks for them, so the
// helper cannot accidentally make a connection visible to the ranking that
// the emitter would not find.
type datagramPeerConn struct {
	// version is what the peer CLAIMS on this connection.
	version domain.ProtocolVersion
	// connectedAt drives the uptime tie-break.
	connectedAt time.Time
	// declared overrides the RAW handshake declarations of THIS connection.
	// The zero value means the default below — both role names, no dtypes
	// field. A test that needs two connections of one peer to disagree about
	// what they advertise sets it, which is the shape §4.3 line 574 is about:
	// the declarations belong to a connection, not to a peer.
	declared *netcore.HandshakeDeclarations
	// inbound picks the tier.
	inbound bool
}

// datagramPeerDeclarations is the default self-description of a fixture
// connection.
//
// The layer gates candidates on the RAW advertised set, not on the typed one,
// so a fixture that set only the typed capabilities would be refused by
// admitPeer for a reason no production session can hit: applyWelcomeMetadata
// writes both halves from the same welcome frame.
//
// The `dtypes` half is stated for the same reason. An endpoint declares its
// set explicitly (§6.1) and nothing is implied by its absence, so a fixture
// that left the field out would be a peer that promised no handler — refused
// by the LAST-HOP gate the moment it is the destination, again for a reason no
// production endpoint can hit.
func datagramPeerDeclarations() netcore.HandshakeDeclarations {
	return netcore.HandshakeDeclarations{
		AdvertisedNames: []domain.CapabilityName{
			datagram.CapabilityDatagramV1,
			datagram.CapabilityDatagramTransitV1,
		},
		DeclaredDTypes: domain.ExplicitDTypes(fixtureDatagramTypes()),
	}
}

func installDatagramPeer(
	t *testing.T,
	svc *Service,
	peer domain.PeerIdentity,
	conns ...datagramPeerConn,
) map[bool]domain.PeerAddress {
	t.Helper()
	addresses := make(map[bool]domain.PeerAddress, len(conns))
	now := time.Now().UTC()

	for i, conn := range conns {
		declarations := datagramPeerDeclarations()
		if conn.declared != nil {
			declarations = conn.declared.Clone()
		}
		health := &peerHealth{
			Connected:           true,
			LastConnectedAt:     conn.connectedAt,
			LastUsefulReceiveAt: now,
		}
		if conn.inbound {
			address := domain.PeerAddress("10.9.0." + string(rune('1'+i)) + ":64646")
			clientPipe, serverPipe := net.Pipe()
			t.Cleanup(func() { _ = clientPipe.Close() })
			t.Cleanup(func() { _ = serverPipe.Close() })
			core := netcore.New(domain.ConnID(9100+i), serverPipe, netcore.Inbound, netcore.Options{
				Address:         address,
				Identity:        peer,
				Caps:            []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1},
				ProtocolVersion: conn.version,
				Declarations:    &declarations,
			})
			t.Cleanup(core.Close)
			svc.peerMu.Lock()
			svc.setTestConnEntryLocked(clientPipe, &connEntry{
				core:    core,
				tracked: true,
			})
			svc.health[address] = health
			svc.peerMu.Unlock()
			addresses[true] = domain.PeerAddress("inbound:" + core.RemoteAddr())
			continue
		}

		address := domain.PeerAddress("10.9.1." + string(rune('1'+i)) + ":64646")
		session := &peerSession{
			address: address,
			// The datagram ingress keys its channel-relative state on this id
			// and refuses an arrival without one; the emitter ADDRESSES the
			// hand-over by it, so two sessions of one fixture must not share.
			connID:       domain.ConnID(7501 + i),
			peerIdentity: peer,
			capabilities: []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1},
			sendCh:       make(chan peerSendItem, 8),
			authOK:       true,
			version:      int(conn.version),
			declarations: declarations.Clone(),
		}
		svc.peerMu.Lock()
		svc.sessions[address] = session
		svc.health[address] = health
		svc.peerMu.Unlock()
		addresses[false] = address
	}
	return addresses
}

// ---------------------------------------------------------------------------
// Feature flag (§10)
// ---------------------------------------------------------------------------

// TestDatagramLayerAbsentWithFlagOff pins the rollout contract: with the flag
// off nothing is constructed, nothing is advertised and no allocation is made
// for the plane. This is the test the whole "no existing path changes
// behaviour" claim rests on.
func TestDatagramLayerAbsentWithFlagOff(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, false)

	if svc.datagramLayer() != nil {
		t.Fatal("the datagram layer was constructed with the feature flag off")
	}
	if advertise := svc.localDatagramAdvertise(); advertise.Endpoint || advertise.Transit {
		t.Fatalf("localDatagramAdvertise() = %+v with the flag off, want both false", advertise)
	}
	for _, capability := range localCapabilities(svc.cfg.EnableMeshRoutingV3, svc.localDatagramAdvertise()) {
		if capability == domain.CapMeshDatagramV1 || capability == domain.CapMeshDatagramTransitV1 {
			t.Fatalf("capability %q advertised with the flag off", capability)
		}
	}
	if names := svc.localDTypeStrings(svc.localDatagramAdvertise()); names != nil {
		t.Fatalf("localDTypeStrings = %v with the flag off, want nil", names)
	}
	// The inbound command limiter must keep covering datagrams: the exemption
	// is paid for by the layer's own §5 budget, and there is no layer. The
	// question is asked of the read loop's decision function, which is where the
	// exemption is really taken.
	authenticated := registerDatagramCommandConn(t, svc, domain.ConnID(8812), true)
	if svc.frameLineExemptFromCommandLimit(authenticated, mustDatagramLine(t, newNodeDatagram(t, nil))) {
		t.Fatal("datagram exempted from the command rate limiter without a layer to charge it")
	}
}

// TestDatagramIngressUnchangedWithFlagOff pins that a `datagram` frame
// reaching the ingress on a flag-off node is answered exactly as M9a answered
// it: parsed, counted as an unknown dtype, accepted, no ban, connection alive.
func TestDatagramIngressUnchangedWithFlagOff(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, false)

	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	result := svc.handleDatagramFrame(
		context.Background(),
		line,
		provenDatagramNeighbour(domain.PeerIdentityFromWire(datagramTestDstHex), datagramInbound),
	)
	if !result.Accepted() {
		t.Fatalf("flag-off ingress refused a well-formed datagram: %v", result.Err())
	}
	if result.BanWorthy() {
		t.Fatal("flag-off ingress charged ban points for a well-formed datagram")
	}
	if got := datagramDropCount(svc, datagram.DropUnknownDType); got != 1 {
		t.Fatalf("unknown_dtype drops = %d, want 1", got)
	}
}

// TestDatagramLayerBuiltWithFlagOn is the mirror image: the plane exists, both
// capabilities are advertised on a full node, and the exemption from the
// command limiter is live because there is now a budget that replaces it.
func TestDatagramLayerBuiltWithFlagOn(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)

	if svc.datagramLayer() == nil {
		t.Fatal("the datagram layer was not constructed with the feature flag on")
	}
	// BOTH halves are on immediately and neither reads the type registry
	// (§6): the endpoint name says the envelope is understood, the transit
	// name says other people's frames will be forwarded, and PR-0's empty
	// registry contradicts neither. What the registry decides is `dtypes`.
	if advertise := svc.localDatagramAdvertise(); !advertise.Endpoint || !advertise.Transit {
		t.Fatalf("localDatagramAdvertise() = %+v with an empty registry, want endpoint and transit", advertise)
	}
	registerFixtureDatagramTypes(t, svc)
	if advertise := svc.localDatagramAdvertise(); !advertise.Endpoint || !advertise.Transit {
		t.Fatalf("localDatagramAdvertise() = %+v with the fixture kit, want endpoint and transit", advertise)
	}
	// The exemption is asked of an AUTHENTICATED connection: it is a swap for
	// the layer's §5 budget, and that budget is charged on the identity the
	// neighbour proved (see datagram_command_budget_test.go for the boundary).
	authenticated := registerDatagramCommandConn(t, svc, domain.ConnID(8811), true)
	if !svc.frameLineExemptFromCommandLimit(authenticated, mustDatagramLine(t, newNodeDatagram(t, nil))) {
		t.Fatal("datagram not exempted from the command limiter although the layer charges its own budget")
	}
	// The layer's half of the exemption is for `datagram` and nothing else.
	if svc.datagramCarriesOwnBudget("push_message", svc.inboundDatagramBudgetKey(authenticated)) {
		t.Fatal("a non-datagram frame type was exempted from the command limiter")
	}
	// The transit gate reads the same set the wire carries. A mismatch would
	// let the node relay while telling the network it does not.
	advertised := svc.localAdvertisedCapabilities()
	if !advertised.Has(datagram.CapabilityDatagramV1) || !advertised.Has(datagram.CapabilityDatagramTransitV1) {
		t.Fatal("the advertised set handed to the pipeline is missing a datagram capability")
	}
}

// TestDatagramCapabilitiesHaveOneRepresentation is the anti-drift test.
//
// Three readers ask what this node claims: the WIRE (hello/welcome through
// localCapabilityStrings), the pipeline's transit gate
// (localAdvertisedCapabilities), and the CANDIDATE FILTER a peer runs over
// what it received (datagram.AdmitCandidate). They used to disagree — the
// wire withheld mesh_datagram_v1 while the gates asserted it — and the
// disagreement is invisible from inside any one of them.
//
// The test walks all three from the single source, localDatagramAdvertise, so
// breaking that one function reddens all three at once; and it walks them
// over BOTH node types, because transit is the one name that legitimately
// differs between them.
func TestDatagramCapabilitiesHaveOneRepresentation(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		nodeType    domain.NodeType
		wantTransit bool
	}{
		"full node":   {nodeType: config.NodeTypeFull, wantTransit: true},
		"client node": {nodeType: config.NodeTypeClient, wantTransit: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			svc := newDatagramLayerServiceOfType(t, tc.nodeType)
			advertise := svc.localDatagramAdvertise()

			// 1. The wire: what a peer receives in hello/welcome.
			onWire := localCapabilityStrings(svc.cfg.EnableMeshRoutingV3, advertise)
			// 2. The gates: what the pipeline enforces about itself.
			gates := svc.localAdvertisedCapabilities()
			// 3. The candidate filter: what a PEER decides about this node
			//    from exactly the names it read off the wire.
			asSeenByPeer := datagram.NewAdvertisedCapabilities(onWire)

			// Errorf, not Fatalf: breaking the one source must be VISIBLE in
			// all three readers at once, and a fatal on the first would hide
			// the other two behind it.
			for _, capability := range []domain.CapabilityName{
				datagram.CapabilityDatagramV1, datagram.CapabilityDatagramTransitV1,
			} {
				want := capability == datagram.CapabilityDatagramV1 || tc.wantTransit
				if got := containsString(onWire, capability.String()); got != want {
					t.Errorf("the wire advertises %q = %v, want %v", capability, got, want)
				}
				if got := gates.Has(capability); got != want {
					t.Errorf("the pipeline gates read %q = %v, want %v", capability, got, want)
				}
				if got := asSeenByPeer.Has(capability); got != want {
					t.Errorf("a peer reads %q = %v, want %v", capability, got, want)
				}
			}

			// And the filter really admits this node in the roles those names
			// promise — the decision the whole plane depends on.
			frame := newNodeDatagram(t, nil)
			transit := datagram.AdmitCandidate(frame, domain.PeerIdentity{}, asSeenByPeer)
			if transit.Admitted() != tc.wantTransit {
				t.Errorf("admitted as transit = %v, want %v (outcome %s)",
					transit.Admitted(), tc.wantTransit, transit.Outcome())
			}
			if lastHop := datagram.AdmitCandidate(frame, frame.Dst, asSeenByPeer); !lastHop.Admitted() {
				t.Errorf("a node advertising mesh_datagram_v1 was refused as the last hop: %s", lastHop.Outcome())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PeerMetadata (§4.3, §9 line 1088)
// ---------------------------------------------------------------------------

// TestDatagramPeerMetadataDescribesTheConnectionTheSendUses is §9 line 1088.
//
// The peer has TWO connections: an inbound one claiming a newer protocol
// version, and an outbound one claiming an older one. The send path prefers
// outbound, so the metadata the scheduler ranks by must report the OUTBOUND
// version. Aggregating — max(version) across sockets — is the exact bug the
// file router already shipped and fixed: the ranking promised a newer path
// while the bytes left over the older one.
func TestDatagramPeerMetadataDescribesTheConnectionTheSendUses(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	outboundConnectedAt := time.Now().UTC().Add(-time.Hour)
	installDatagramPeer(t, svc,
		peer,
		datagramPeerConn{version: 20, connectedAt: outboundConnectedAt},
		datagramPeerConn{version: 27, connectedAt: time.Now().UTC().Add(-time.Minute), inbound: true},
	)

	frame := newNodeDatagram(t, func(frame *protocol.DatagramFrame) { frame.Dst = peer })
	conn, ok := datagramPeerMetadata{service: svc}.SendableConnection(context.Background(), peer, frame)
	if !ok {
		t.Fatal("SendableConnection reported no usable connection for a live peer")
	}
	if conn.ReportedProtocolVersion != 20 {
		t.Fatalf("ReportedProtocolVersion = %d, want 20 (the OUTBOUND session the send would use), not the inbound 27",
			conn.ReportedProtocolVersion)
	}
	if !conn.ConnectedAt.Equal(outboundConnectedAt) {
		t.Fatalf("ConnectedAt = %s, want the outbound session's %s", conn.ConnectedAt, outboundConnectedAt)
	}

	// The emitter must reach for the SAME connection, or the plan and the
	// bytes would still disagree while the metadata looked right.
	svc.peerMu.RLock()
	targets := svc.datagramSendTargetsLocked(peer, time.Now().UTC())
	svc.peerMu.RUnlock()
	if len(targets) == 0 {
		t.Fatal("datagramSendTargetsLocked returned nothing for a live peer")
	}
	if targets[0].session == nil {
		t.Fatal("the emitter would try the inbound connection first; the metadata described the outbound one")
	}
}

// TestDatagramPeerMetadataRejectsStalledPeer pins the liveness half of the
// same contract: a peer the send path would refuse must have no metadata at
// all, rather than metadata with a zero timestamp — otherwise "stalled" and
// "connected at the zero time" become indistinguishable.
func TestDatagramPeerMetadataRejectsStalledPeer(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	addresses := installDatagramPeer(t, svc, peer, datagramPeerConn{version: 27, connectedAt: time.Now().UTC()})

	svc.peerMu.Lock()
	svc.health[addresses[false]].LastUsefulReceiveAt = time.Now().UTC().Add(-24 * time.Hour)
	svc.peerMu.Unlock()

	metadata := datagramPeerMetadata{service: svc}
	frame := newNodeDatagram(t, func(frame *protocol.DatagramFrame) { frame.Dst = peer })
	if _, ok := metadata.SendableConnection(context.Background(), peer, frame); ok {
		t.Fatal("SendableConnection described a stalled peer the send path would not use")
	}
}

// ---------------------------------------------------------------------------
// Reachability (§4.3, §9 line 1089)
// ---------------------------------------------------------------------------

// TestDatagramProbeAgreesWithSendOnUnchangedTable is §9 line 1089.
//
// Over a routing state that does not move between the two calls, the probe's
// one-way guarantee must hold: "unreachable" means a send performed at the
// same moment would not have been queued. The test drives both ends — the
// probe through the scheduler and the send through the pipeline — and checks
// them against each other in both directions, with and without a live
// neighbour.
func TestDatagramProbeAgreesWithSendOnUnchangedTable(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	// The fixture's own guard: with no layer BOTH ends of the comparison answer
	// "unreachable", so the agreement under test would hold vacuously.
	requireDatagramPlane(t, svc)
	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	dtype := domain.DType("push_identity")

	probe := func() bool {
		data, err := svc.DatagramReachable(context.Background(), peer, dtype)
		if err != nil {
			t.Fatalf("DatagramReachable: %v", err)
		}
		var answer struct {
			Reachable bool   `json:"reachable"`
			Reason    string `json:"reason"`
		}
		if err := json.Unmarshal(data, &answer); err != nil {
			t.Fatalf("decode the reachability answer: %v", err)
		}
		// A negative answer always names a reason: §6.1 acts on
		// unsupported_dtype and must not act on a plain missing route.
		if !answer.Reachable && answer.Reason == "" {
			t.Fatalf("an unreachable answer named no reason: %s", data)
		}
		return answer.Reachable
	}
	send := func() datagram.SendOutcome {
		frame := newServiceDatagram(t, svc, peer, dtype, domain.RoutePolicyBest)
		return svc.datagramLayer().pipeline.SendLocal(context.Background(), datagram.LocalSendOpts{Frame: frame})
	}

	// No neighbour, no route: the probe must say unreachable and the send
	// must refuse. `queued` here would be the contradiction.
	if probe() {
		t.Fatal("the probe reported a destination reachable with no session and no route")
	}
	if outcome := send(); outcome.Kind() == datagram.SendQueued {
		t.Fatalf("the send queued a frame the probe called unreachable: %s", outcome)
	}

	// A live direct session that passes the role gate: now both must agree
	// the other way.
	installDatagramPeer(t, svc, peer, datagramPeerConn{version: 27, connectedAt: time.Now().UTC().Add(-time.Hour)})
	if !probe() {
		t.Fatal("the probe reported a directly connected, datagram-capable destination unreachable")
	}
	if outcome := send(); outcome.Kind() != datagram.SendQueued {
		t.Fatalf("the send refused (%s) a destination the probe called reachable", outcome)
	}
}

// newServiceDatagram builds a routed datagram signed by svc's own identity, so
// the frame passes the signer/src binding the way a real local send does.
func newServiceDatagram(
	t *testing.T,
	svc *Service,
	dst domain.PeerIdentity,
	dtype domain.DType,
	policy domain.RoutePolicy,
) protocol.DatagramFrame {
	t.Helper()
	src, err := domain.ParsePeerIdentity(svc.identity.Address)
	if err != nil {
		t.Fatalf("ParsePeerIdentity(local): %v", err)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	for i := range salt {
		salt[i] = byte(0x40 + i)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         src,
		Dst:         dst,
		TTL:         domain.DatagramDefaultMaxHops,
		RoutePolicy: policy,
		DType:       dtype,
		Payload:     []byte("datagram-e2e-payload"),
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        salt,
			MaxTTL:      domain.DatagramDefaultMaxHops,
			Time:        time.Now().UTC().Unix(),
		},
	}
	signed, err := protocol.SignDatagram(frame, domain.NetworkID(networkName), svc.identity.PrivateKey)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return signed
}

// ---------------------------------------------------------------------------
// FrameEmitter (§4.2, §5)
// ---------------------------------------------------------------------------

// TestDatagramEmitterWritesTheLayersLine pins that the emitter hands the
// transport the bytes the layer already produced and does not serialize the
// frame a second time.
//
// The check is byte identity all the way to the wire: the queued frame's
// RawLine must equal OutboundFrame.Line, and MarshalFrameLine — the function
// the writer calls — must return exactly those bytes. A re-encode would be
// visible immediately, because a JSON round trip through the universal Frame
// drops every datagram-specific field.
func TestDatagramEmitterWritesTheLayersLine(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	addresses := installDatagramPeer(t, svc, peer, datagramPeerConn{version: 27, connectedAt: time.Now().UTC()})

	frame := newNodeDatagram(t, nil)
	line := mustDatagramLine(t, frame)
	out := datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(line),
		Class:     frame.Class,
		SendUntil: time.Now().UTC().Add(5 * time.Second),
	}

	requireDatagramPlane(t, svc)
	emitter := datagramEmitterOf(svc)
	if !emitter.EmitTo(context.Background(), out) {
		t.Fatal("EmitTo refused a frame for a live, datagram-capable peer")
	}

	svc.peerMu.RLock()
	session := svc.sessions[addresses[false]]
	svc.peerMu.RUnlock()

	select {
	case item := <-session.sendCh:
		if item.Type != protocol.DatagramFrameType {
			t.Fatalf("queued frame type = %q, want %q", item.Type, protocol.DatagramFrameType)
		}
		if item.RawLine != string(out.Line) {
			t.Fatalf("queued RawLine is not the layer's line:\n got %q\nwant %q", item.RawLine, string(out.Line))
		}
		encoded, err := protocol.MarshalFrameLine(item.Frame)
		if err != nil {
			t.Fatalf("MarshalFrameLine: %v", err)
		}
		if encoded != string(out.Line) {
			t.Fatalf("the writer would emit different bytes than the layer measured:\n got %q\nwant %q",
				encoded, string(out.Line))
		}
	default:
		t.Fatal("EmitTo reported acceptance but queued nothing on the outbound session")
	}
}

// TestDatagramEmitterRefusesUnknownPeer pins the honest negative: with no
// usable connection the emitter reports a refusal rather than swallowing the
// frame, so the scheduler moves on to the next candidate.
func TestDatagramEmitterRefusesUnknownPeer(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)

	frame := newNodeDatagram(t, nil)
	line := mustDatagramLine(t, frame)
	requireDatagramPlane(t, svc)
	emitted := datagramEmitterOf(svc).EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:  domain.PeerIdentityFromWire(datagramTestDstHex),
		Frame: frame,
		Line:  []byte(line),
		Class: frame.Class,
	})
	if emitted {
		t.Fatal("EmitTo claimed a frame was queued for a peer with no connection")
	}
}

// TestDatagramOutboundWriteCarriesClassWriteGrace pins §4.2 for every frame the
// layer emits.
//
// Before OutboundFrame carried a class and a deadline, a frame could reach the
// writer with the connection's 30 s default write deadline. The hop budget
// behind the 240 s reverse window is queue time PLUS write time, so a control
// frame with a 30 s write tail breaks the formula.
func TestDatagramOutboundWriteCarriesClassWriteGrace(t *testing.T) {
	t.Parallel()

	deadline := time.Now().UTC().Add(3 * time.Second)
	for _, tc := range []struct {
		class domain.DatagramClass
		grace time.Duration
	}{
		{domain.DatagramClassControl, 5 * time.Second},
		{domain.DatagramClassBulk, 30 * time.Second},
	} {
		write, err := datagramFrameEmitter{}.outboundWrite(
			datagram.OutboundFrame{Class: tc.class, SendUntil: deadline})
		if err != nil {
			t.Fatalf("outboundWrite(%s): %v", tc.class, err)
		}
		if write.WriteGrace != tc.grace {
			t.Fatalf("class %s write grace = %s, want %s", tc.class, write.WriteGrace, tc.grace)
		}
		if !write.SendUntil.Valid() || !write.SendUntil.Time().Equal(deadline) {
			t.Fatalf("class %s lost the send deadline: %+v", tc.class, write.SendUntil)
		}
		// A contract that produces a nil ticket would silently drop both the
		// deadline and the grace on the floor.
		if netcore.NewWriteTicket(write) == nil {
			t.Fatalf("class %s produced an inert write ticket: the deadline would never be re-checked", tc.class)
		}
	}

	if _, err := (datagramFrameEmitter{}).outboundWrite(
		datagram.OutboundFrame{Class: domain.DatagramClass("nonsense")}); err == nil {
		t.Fatal("an unknown class produced a write contract instead of a refusal")
	}
}

// TestDatagramNodeSecretIsStableAndDerived pins both halves of the
// node_local_secret contract: identical within a process (and across restarts
// of the same identity, which is the same derivation), and different between
// identities — the property that decorrelates the explore rotation.
func TestDatagramNodeSecretIsStableAndDerived(t *testing.T) {
	t.Parallel()

	first, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	second, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	a := newDatagramNodeSecret(first.PrivateKey).NodeLocalSecret()
	b := newDatagramNodeSecret(first.PrivateKey).NodeLocalSecret()
	c := newDatagramNodeSecret(second.PrivateKey).NodeLocalSecret()

	if len(a) == 0 {
		t.Fatal("the node local secret is empty; NewScheduler refuses it")
	}
	if string(a) != string(b) {
		t.Fatal("the same identity derived two different secrets: the explore offset would move for no reason")
	}
	if string(a) == string(c) {
		t.Fatal("two identities derived the same secret: the explore offset would not decorrelate between nodes")
	}
	if string(a) == string(first.PrivateKey) {
		t.Fatal("the secret is the private key itself")
	}
}

// TestForgetDatagramPeerReleasesOnlyASettledBucket is the honest contract of
// the call, and it exists because the previous comment promised something the
// code never did: Forget drops a bucket only when dropping it forgives
// nothing. A peer that was talking a moment ago KEEPS its bucket — handing a
// half-spent budget back at a moment the peer chooses is a free burst — while
// a peer silent past IdleRetention is released here instead of waiting for an
// eviction to notice it.
func TestForgetDatagramPeerReleasesOnlyASettledBucket(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	admission := svc.datagramLayer().admission
	peer := domain.PeerIdentityFromWire(datagramTestDstHex)

	if !admission.Admit(datagram.ProvenIdentityKey(peer), 64) {
		t.Fatal("the first frame of a fresh session must be admitted")
	}
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("tracked peers = %d, want 1", got)
	}

	// The session closes right after the traffic: the bucket is neither idle
	// nor refilled, so it stays.
	svc.forgetDatagramPeer(peer)
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("a freshly used bucket was released: tracked peers = %d, want 1", got)
	}

	// A nil layer and a zero identity are both no-ops, not panics.
	svc.forgetDatagramPeer(domain.PeerIdentity{})
	if got := admission.TrackedPeers(); got != 1 {
		t.Fatalf("a zero identity disturbed the map: tracked peers = %d", got)
	}
}
