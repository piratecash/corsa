package node

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
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
		protocol.RoutePoisonV2FrameType,
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
		protocol.RoutePoisonV2FrameType,
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
			{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
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
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], frame)

	if got := svc.routingTable.Lookup(idTargetX); len(got) == 0 {
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
		peerIdentity: idPeerB,
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

	if got := svc.routingTable.Lookup(idTargetX); len(got) > 0 {
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
	addDirectViaIdentity(t, svc, idPeerB)
	// Seed a transit claim keyed on (idTargetX, uplink=idPeerB) — the
	// poison handler invalidates ONLY the (identity, sender) slot, so
	// the uplink must equal the poison-sending peer for the invalidation
	// to bite.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idPeerB,
		NextHop:  idPeerB,
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
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	poison := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX.String(),
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
	for _, r := range svc.routingTable.Lookup(idTargetX) {
		if r.NextHop == idPeerB {
			t.Fatalf("poison via outbound session reader path must invalidate the (idTargetX, idPeerB) slot; live entry %+v survived", r)
		}
	}
}

// ---------------------------------------------------------------------------
// Outbound control-frame cmd-rate limit (regression for the
// inbound/outbound asymmetry: inbound cmd limiter covers
// request_resync / route_poison_v1 but the outbound peer-session
// dispatcher used to skip the limiter entirely, letting a peer
// flood control frames over an established outbound session and
// fall back only to the loose 200-tokens/s announceLimiter route
// bucket).
// ---------------------------------------------------------------------------

// TestDropOutboundControlFrameBucket_ForcesFreshBudgetAfterRemoval
// pins the lifecycle contract for dropOutboundControlFrameBucket:
// after a session's bucket is exhausted, calling the helper to
// drop it makes a subsequent allowCommand for the SAME connID key
// observe a fresh burst budget — proving the underlying
// removeConn ran. This is the property each session-removal site
// relies on to prevent stale buckets from accumulating in the
// cmdLimiter map (commandRateLimiter.cleanup is not wired
// anywhere).
//
// The helper is also expected to no-op safely on connID==0 and
// when cmdLimiter is nil — both are exercised here.
func TestDropOutboundControlFrameBucket_ForcesFreshBudgetAfterRemoval(t *testing.T) {
	t.Parallel()

	svc := &Service{cmdLimiter: newCommandRateLimiter()}
	const connID = domain.ConnID(31415)
	session := &peerSession{connID: connID}

	// Exhaust the bucket.
	for i := 0; i < cmdBurstPerConn; i++ {
		if !svc.outboundControlFrameAllowed(session) {
			t.Fatalf("burst budget should cover %d calls; failed at i=%d", cmdBurstPerConn, i)
		}
	}
	// Sanity: at least one immediate next call must be blocked.
	blocked := false
	for i := 0; i < 20; i++ {
		if !svc.outboundControlFrameAllowed(session) {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("post-burst call unexpectedly allowed; cmdLimiter never engaged")
	}

	// Drop the bucket — this is what every session-removal site
	// now calls.
	svc.dropOutboundControlFrameBucket(connID)

	// A fresh bucket: the very first call after removal must
	// succeed because removeConn deleted the per-conn entry and
	// the next allowCommand re-creates it at full burst.
	if !svc.outboundControlFrameAllowed(session) {
		t.Fatal("after dropOutboundControlFrameBucket the connID's bucket was not actually removed — call still blocked")
	}

	// Helper must no-op safely on the two degenerate inputs that
	// the cleanup fallback (ensurePeerSessions defer) can pass.
	svc.dropOutboundControlFrameBucket(0)
	svcNilLimiter := &Service{cmdLimiter: nil}
	svcNilLimiter.dropOutboundControlFrameBucket(connID)
}

// TestOutboundControlFrameAllowed_PerSessionBucket pins the
// predicate directly: the first cmdBurstPerConn calls return true,
// then the bucket is exhausted and at least one subsequent call
// returns false. This is the building block both outbound control-
// frame cases rely on; the integration tests below add the
// dispatcher layer on top.
func TestOutboundControlFrameAllowed_PerSessionBucket(t *testing.T) {
	t.Parallel()

	svc := &Service{cmdLimiter: newCommandRateLimiter()}
	session := &peerSession{connID: 9001}

	for i := 0; i < cmdBurstPerConn; i++ {
		if !svc.outboundControlFrameAllowed(session) {
			t.Fatalf("first %d calls must be allowed (burst budget); failed at i=%d", cmdBurstPerConn, i)
		}
	}

	// Bucket exhausted. Some immediate subsequent calls must fail.
	// Refill rate is 30/s, so in a tight loop we deterministically
	// see at least a handful of failures before any token recovers.
	blocked := 0
	for i := 0; i < 25; i++ {
		if !svc.outboundControlFrameAllowed(session) {
			blocked++
		}
	}
	if blocked == 0 {
		t.Fatal("post-burst calls all allowed; cmdLimiter did not engage on the outbound bucket")
	}
}

// TestDispatchPeerSessionFrame_RequestResync_CmdLimited is the
// end-to-end regression for the outbound side. It exhausts the
// per-session control-frame bucket and then dispatches one more
// request_resync; the handler MUST NOT fire. Without the
// outboundControlFrameAllowed gate added to the dispatcher,
// handleRequestResync would flip NeedsFullResync and call
// TriggerUpdate regardless of bucket state.
func TestDispatchPeerSessionFrame_RequestResync_CmdLimited(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	svc.cmdLimiter = newCommandRateLimiter()

	senderAddr := domain.PeerAddress("addr-peerB")
	const connID = domain.ConnID(424242)

	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: idPeerB,
		connID:       connID,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
	state := registry.Get(idPeerB)
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, time.Now())
	if state.View().NeedsFullResync {
		t.Fatalf("precondition: NeedsFullResync must be cleared")
	}
	svc.announceLoop.PendingTrigger() // drain any spurious trigger

	// Exhaust the per-session bucket so the dispatcher's gate
	// fires deterministically.
	key := outboundControlFrameLimitKey(connID)
	for i := 0; i < cmdBurstPerConn; i++ {
		svc.cmdLimiter.allowCommand(key)
	}

	// Dispatch request_resync — should be silently dropped.
	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], protocol.Frame{
		Type: "request_resync",
	})

	if state.View().NeedsFullResync {
		t.Fatal("request_resync passed the outbound cmd-limit gate: handleRequestResync was called despite an exhausted bucket")
	}
	if svc.announceLoop.PendingTrigger() {
		t.Fatal("request_resync bypassed the outbound cmd-limit gate: TriggerUpdate fired")
	}
}

// TestDispatchPeerSessionFrame_RoutePoison_CmdLimited is the
// route_poison_v1 counterpart. It seeds a transit claim, exhausts
// the per-session bucket, and dispatches a properly-signed poison
// frame; the uplink claim must SURVIVE because the gate aborts
// dispatch before InvalidateUplinkClaim runs.
func TestDispatchPeerSessionFrame_RoutePoison_CmdLimited(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	svc.cmdLimiter = newCommandRateLimiter()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	addDirectViaIdentity(t, svc, domain.PeerIdentityFromWire(peer.Address))

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   domain.PeerIdentityFromWire(peer.Address),
		NextHop:  domain.PeerIdentityFromWire(peer.Address),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	const connID = domain.ConnID(727272)
	senderAddr := domain.PeerAddress(peer.Address)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentityFromWire(peer.Address),
		connID:       connID,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	if svc.health == nil {
		svc.health = map[domain.PeerAddress]*peerHealth{}
	}
	svc.health[senderAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Build a properly signed poison frame the handler would
	// otherwise accept — we are testing the gate, not sig checks.
	poison := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX.String(),
		Reason:   protocol.RoutePoisonReasonHealthDead,
		IssuedAt: "2026-05-28T12:00:00Z",
	}
	sig := ed25519.Sign(peer.PrivateKey, poison.CanonicalSenderSigBytes())
	poison.SenderSig = base64.StdEncoding.EncodeToString(sig)
	raw, err := protocol.MarshalRoutePoisonFrame(poison)
	if err != nil {
		t.Fatalf("MarshalRoutePoisonFrame: %v", err)
	}
	frame := protocol.Frame{
		Type:    protocol.RoutePoisonFrameType,
		RawLine: string(raw),
	}

	// Exhaust the per-session bucket.
	key := outboundControlFrameLimitKey(connID)
	for i := 0; i < cmdBurstPerConn; i++ {
		svc.cmdLimiter.allowCommand(key)
	}

	svc.dispatchPeerSessionFrame(senderAddr, svc.sessions[senderAddr], frame)

	// The claim must still be live — gate fired before
	// InvalidateUplinkClaim ran.
	stillLive := false
	for _, r := range svc.routingTable.Lookup(idTargetX) {
		if r.NextHop == domain.PeerIdentityFromWire(peer.Address) {
			stillLive = true
			break
		}
	}
	if !stillLive {
		t.Fatal("route_poison_v1 passed the outbound cmd-limit gate: handleRoutePoison invalidated the claim despite an exhausted bucket")
	}
}
