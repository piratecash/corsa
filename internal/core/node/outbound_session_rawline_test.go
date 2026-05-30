package node

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// outbound_session_rawline_test.go pins the Round-6 fix for the
// outbound peer-session reader path. The session reader calls
// protocol.ParseFrameLine to get a Frame, then pushes it to
// session.inboxCh. ParseFrameLine intentionally does NOT populate
// Frame.RawLine. Phase 2+ / Phase 4 frames (route_sync_digest_v1,
// route_sync_summary_v1, route_announce_v3, route_poison_v1) use
// the RawLine bypass pattern — their wire payload lives outside the
// universal Frame fields and the dispatch re-parses it from
// Frame.RawLine. Without explicit population in the reader the
// dispatch sees RawLine == "" and silently drops the frame, which on
// the outbound session path effectively makes v3 / poison
// one-directional. The reader-side fix populates Frame.RawLine for
// the RawLine-backed types (via isRawLineBackedFrameType) right
// after ParseFrameLine; these tests exercise the helper and the
// end-to-end ParseFrameLine → dispatch contract.

// TestIsAnnouncePlaneFrameType_CoversV3AndPoison pins the Round-6
// extension of the announce-plane size guard. Before the fix, the
// outbound peer-session reader applied the strict 128 KiB MaxFrameLine
// budget only to announce_routes / routes_update / request_resync /
// route_announce_v3 — leaving route_poison_v1 in the 8 MiB
// response-plane budget. A buggy or hostile peer could pack a poison
// frame past 128 KiB and the receiver would JSON-parse + ed25519-verify
// it before hitting any size check. The cap-guard list now includes
// poison; this test fails immediately if the literal is dropped from
// the switch.
func TestIsAnnouncePlaneFrameType_CoversV3AndPoison(t *testing.T) {
	must := []string{
		"announce_routes",
		"routes_update",
		"request_resync",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
	}
	for _, ft := range must {
		if !isAnnouncePlaneFrameType(ft) {
			t.Errorf("isAnnouncePlaneFrameType(%q) = false, want true", ft)
		}
	}
	// Frame types outside the announce plane MUST be left to the wider
	// response-plane (8 MiB) budget — narrowing them to 128 KiB would
	// break legitimate batched responses (contacts/messages/inbox).
	mustNot := []string{
		"relay_message",
		"hello",
		"fetch_contacts",
		"",
	}
	for _, ft := range mustNot {
		if isAnnouncePlaneFrameType(ft) {
			t.Errorf("isAnnouncePlaneFrameType(%q) = true, want false", ft)
		}
	}
}

// TestIsRawLineBackedFrameType_CoversV3AndPoison pins the explicit
// set of types that use the RawLine bypass. A regression that adds a
// new bypass-using frame type but forgets to update the helper would
// silently re-introduce the outbound-session drop bug; this test
// fails immediately if either of the two production v3/poison
// constants drops off the list.
func TestIsRawLineBackedFrameType_CoversV3AndPoison(t *testing.T) {
	must := []string{
		"route_sync_digest_v1",
		"route_sync_summary_v1",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
	}
	for _, ft := range must {
		if !isRawLineBackedFrameType(ft) {
			t.Errorf("isRawLineBackedFrameType(%q) = false, want true", ft)
		}
	}
	// Universal-Frame types must NOT trigger the RawLine assignment
	// (avoids subtle marshal short-circuit issues elsewhere).
	mustNot := []string{
		"announce_routes",
		"routes_update",
		"request_resync",
		"relay_message",
		"hello",
		"",
	}
	for _, ft := range mustNot {
		if isRawLineBackedFrameType(ft) {
			t.Errorf("isRawLineBackedFrameType(%q) = true, want false", ft)
		}
	}
}

// TestOutboundSessionReader_PopulatesRawLineForV3 walks the
// outbound-session reader contract end-to-end at the parse boundary:
// ParseFrameLine on a wire-shaped route_announce_v3 line followed by
// the reader's targeted RawLine population must produce a Frame whose
// dispatcher can re-parse RawLine successfully and apply the entry.
// Before the fix, RawLine was empty here and the v3 dispatcher's
// UnmarshalRouteAnnounceV3Frame would fail with a JSON parse error,
// silently dropping the frame.
func TestOutboundSessionReader_PopulatesRawLineForV3(t *testing.T) {
	// Build a wire-shaped v3 frame byte string the same way the peer
	// would put it on the wire.
	v3 := protocol.RouteAnnounceV3Frame{
		Type:  protocol.RouteAnnounceV3FrameType,
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: 1,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: idTargetX, Hops: 1, SeqNo: 1},
		},
	}
	raw, err := protocol.MarshalRouteAnnounceV3Frame(v3)
	if err != nil {
		t.Fatalf("marshal v3 frame: %v", err)
	}
	line := string(raw) + "\n"
	trimmed := strings.TrimSpace(line)

	// Reproduce the reader's parse + targeted RawLine assignment.
	frame, err := protocol.ParseFrameLine(trimmed)
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if frame.RawLine != "" {
		t.Fatalf("precondition: ParseFrameLine must not populate RawLine; got %q", frame.RawLine)
	}
	if isRawLineBackedFrameType(frame.Type) {
		frame.RawLine = trimmed
	}
	if frame.RawLine == "" {
		t.Fatalf("RawLine must be populated for %s", frame.Type)
	}

	// Now feed the frame to the production dispatcher exactly as
	// readPeerSession would. With RawLine populated the
	// UnmarshalRouteAnnounceV3Frame call inside the dispatcher
	// succeeds, the cap triplet passes, and handleRouteAnnounceV3
	// applies the entry to the routing table.
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], frame)

	if got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)); len(got) == 0 {
		t.Fatalf("v3 frame must reach the routing table via the outbound session reader → dispatch path; got 0 entries")
	}
}

// TestOutboundSessionReader_DropsV3WhenRawLineEmpty proves the
// failure mode the fix prevents: dispatching a v3 frame with empty
// RawLine (the pre-fix reader output) silently drops it. Without
// this assertion the previous test could pass trivially even if
// dispatch had a fallback to the universal Frame fields.
func TestOutboundSessionReader_DropsV3WhenRawLineEmpty(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	// Same Type, but RawLine deliberately empty — what the old reader
	// pushed to inboxCh before the fix. Dispatcher's
	// UnmarshalRouteAnnounceV3Frame([]byte("")) fails and the frame
	// is dropped; nothing reaches the routing table.
	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], protocol.Frame{
		Type: protocol.RouteAnnounceV3FrameType,
	})

	if got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)); len(got) > 0 {
		t.Fatalf("v3 dispatch must drop a frame with empty RawLine; got %d entries", len(got))
	}
}

// TestOutboundSessionReader_PopulatesRawLineForPoison is the
// poison-side analogue: the reader's targeted RawLine population
// must let the dispatcher's UnmarshalRoutePoisonFrame succeed so
// Table.InvalidateUplinkClaim runs against the slot the poison
// targets.
func TestOutboundSessionReader_PopulatesRawLineForPoison(t *testing.T) {
	// Seed a live transit claim through idPeerB so the poison has
	// something to invalidate.
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	// Seed a transit claim keyed on (idTargetX, uplink=idPeerB) — the
	// poison handler invalidates ONLY the (identity, sender) slot, so
	// the uplink must equal the poison-sending peer for the invalidation
	// to bite.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim via idPeerB: %v", err)
	}

	senderAddr := domain.PeerAddress("addr-peerB-poison")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	poison := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-28T12:00:00Z",
	}
	raw, err := protocol.MarshalRoutePoisonFrame(poison)
	if err != nil {
		t.Fatalf("marshal poison: %v", err)
	}
	line := string(raw) + "\n"
	trimmed := strings.TrimSpace(line)

	frame, err := protocol.ParseFrameLine(trimmed)
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if isRawLineBackedFrameType(frame.Type) {
		frame.RawLine = trimmed
	}
	if frame.RawLine == "" {
		t.Fatalf("RawLine must be populated for %s", frame.Type)
	}

	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], frame)

	// Lookup filters withdrawn entries — the poisoned claim must
	// disappear from the result.
	for _, r := range svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)) {
		if r.NextHop == domain.PeerIdentity(idPeerB) {
			t.Fatalf("poison via outbound session reader path must invalidate the (idTargetX, idPeerB) slot; live entry %+v survived", r)
		}
	}
}
