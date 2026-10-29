//go:build interop

package node

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestReleasedV30OriginReplyThroughRelay keeps the published endpoint A and
// current client C apart: A -> B <- C. B is first the actual published full
// node, then an ordinary current full node (legacy A-B plus secure C-B).
// Current nodes explicitly select the default-on datagram, routing-v3 and
// hold-until-reachable flags without reading a user's environment/config.
// No direct A-C socket may substitute for the relay route. This is a real
// old-origin core/wire regression, not an old Desktop composer/codec test.
func TestReleasedV30OriginReplyThroughRelay(t *testing.T) {
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	binaries := interopBinaries(t, 30)
	if len(binaries) != 1 {
		t.Fatalf("relay reply fixture requires one published V30 binary, got %d", len(binaries))
	}
	release := describeRelease(t, fuse, binaries[0], 30)
	if release.release != "2.4.70" || release.sha256 != "5cf33cc61c7009be436a1eeed606cfb51ac279a264cc3a92a5e536fb0b74f6e5" {
		t.Fatalf("relay reply requires the qualified published Darwin arm64 v2.4.70 binary; got version=%q SHA256=%s", release.release, release.sha256)
	}
	for _, relayKind := range []string{"published_v30_relay", "current_v31_relay"} {
		t.Run(relayKind, func(t *testing.T) {
			var relayAddress string
			var relayIdentity string
			var currentRelay *Service
			var publishedRelay *releasedNode
			if relayKind == "published_v30_relay" {
				// The old relay's only egress target is an unused owned
				// loopback port. The two endpoints initiate its real links.
				publishedRelay = startReleasedNode(t, fuse, release, freeAddress(t))
				relayAddress = string(publishedRelay.listen)
				relayIdentity = publishedRelay.keys.Address
			} else {
				var stop func()
				currentRelay, stop = startTestNodeWithSetup(t, config.Node{
					ListenAddress: freeAddress(t), Type: config.NodeTypeFull,
					EnableDatagramV1: true, EnableMeshRoutingV3: true, HoldDMUntilReachable: true,
				}, withoutOwnDials(func(s *Service) { s.disableRateLimiting = false }))
				t.Cleanup(stop)
				relayAddress = string(currentRelay.cfg.ListenAddress)
				relayIdentity = currentRelay.Address()
			}
			old := startReleasedNode(t, fuse, release, relayAddress)
			// Let the genuine A-B session publish the old identity/key set
			// before C starts its original connect-time synchronization.
			waitForConditionMsg(t, 40*time.Second, "old endpoint never connected to the relay", func() bool {
				return releasedRelayConnection(t, old, relayIdentity, "", false)
			})
			dev, stop := startTestNodeWithSetup(t, config.Node{
				ListenAddress: freeAddress(t), Type: config.NodeTypeClient,
				BootstrapPeers: []string{relayAddress}, ConnectOnly: relayAddress,
				EnableDatagramV1: true, EnableMeshRoutingV3: true, HoldDMUntilReachable: true,
			}, func(s *Service) { s.disableRateLimiting = false })
			t.Cleanup(stop)
			var replyID string
			defer func() {
				observeReleasedReplyPresence(t, dev, old)
				if replyID != "" {
					observeReleasedReplyPending(t, old, replyID)
				}
				assertNoDirectContact(t, dev, old.peerIdentity(), "relay diagnostic final bookend")
				releasedRelayConnection(t, old, relayIdentity, dev.Address(), true)
				observeReleasedRelayRoutes(t, publishedRelay, currentRelay, old.keys.Address, dev.Address())
			}()
			waitForConditionMsg(t, 40*time.Second, "current client never authenticated to the relay", func() bool {
				kinds := dev.SessionKindStats()
				if publishedRelay != nil {
					return kinds.OutboundLegacy > 0
				}
				return kinds.OutboundV2 > 0
			})
			assertNoDirectContact(t, dev, old.peerIdentity(), "before relay messages")
			releasedRelayConnection(t, old, relayIdentity, dev.Address(), true)

			// Ordinary default-on identity discovery may intentionally skip
			// bulk contact gossip. Ask through the same public UI lookup
			// entrypoint; never put the fixture's known peer keys in maps.
			resolution := dev.HandleLocalFrame(protocol.Frame{Type: "resolve_identity", Address: old.keys.Address})
			if resolution.Type != "identity_resolution" {
				t.Fatalf("current public identity lookup: %q/%q", resolution.Type, resolution.Code)
			}
			var oldContact protocol.ContactFrame
			waitForConditionMsg(t, 40*time.Second, "current client never naturally learned the old endpoint's key set through the relay", func() bool {
				frame := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
				if frame.Type != "contacts" {
					t.Fatalf("current contacts returned %q/%q", frame.Type, frame.Code)
				}
				for _, c := range frame.Contacts {
					if c.Address == old.keys.Address {
						oldContact = c
						return true
					}
				}
				return false
			})
			requireReleasedRelayContact(t, oldContact, old.keys)
			// This is the original public user contact-acceptance path using
			// ONLY the row fetched from the running node, not supplied keys.
			imported := dev.HandleLocalFrame(protocol.Frame{Type: "import_contacts", Contacts: []protocol.ContactFrame{oldContact}})
			if imported.Type != "contacts_imported" {
				t.Fatalf("current public contact acceptance: %q/%q", imported.Type, imported.Code)
			}

			initialID := newMessageID(t)
			const initialBody = "current-client-through-relay-to-published-endpoint"
			sendDMToRelease(t, dev, old, initialID, initialBody)
			var initial protocol.MessageFrame
			waitForConditionMsg(t, 40*time.Second, "current message did not cross the relay to the published endpoint", func() bool {
				initial = releasedReplyMessage(t, old, initialID)
				return initial.ID != ""
			})
			requireReleasedReplyPlaintext(t, old.keys, initial, dev.Address(), identity.PublicKeyBase64(dev.identity.PublicKey), old.keys.Address, initialBody)
			assertNoDirectContact(t, dev, old.peerIdentity(), "after current-to-old relay delivery")
			releasedRelayConnection(t, old, relayIdentity, dev.Address(), true)

			var currentContact protocol.ContactFrame
			waitForConditionMsg(t, 40*time.Second, "published endpoint never naturally learned the current recipient's key set", func() bool {
				currentContact = releasedReplyContact(t, old, dev.Address())
				return currentContact.Address != ""
			})
			requireReleasedRelayContact(t, currentContact, dev.identity)
			raw, err := old.rpc("importContacts", map[string]any{"contacts": []protocol.ContactFrame{currentContact}})
			if err != nil {
				t.Fatalf("published public contact acceptance: %v", err)
			}
			var accepted protocol.Frame
			if err := json.Unmarshal(raw, &accepted); err != nil || accepted.Type != "contacts_imported" {
				t.Fatalf("published contact acceptance: type=%q decode=%v", accepted.Type, err)
			}
			requireReleasedRelayTrusted(t, dev, old)
			// Give both original two-second projector loops real time after
			// contact membership. No clock/projection/state is manufactured;
			// missing/offline/probing remain diagnostics, never send gates.
			time.Sleep(2 * presenceSnapshotInterval)
			observeReleasedReplyPresence(t, dev, old)
			observeReleasedRelayRoutes(t, publishedRelay, currentRelay, old.keys.Address, dev.Address())

			const replyBody = "published-endpoint-origin-through-relay-to-current-client"
			ciphertext, err := directmsg.EncryptForParticipants(old.keys, domain.DMRecipient{
				Address: domain.PeerIdentityFromWire(currentContact.Address), BoxKeyBase64: currentContact.BoxKey,
			}, domain.OutgoingDM{Body: replyBody, ReplyTo: domain.MessageID(initialID)})
			if err != nil {
				t.Fatalf("encrypt owned old-origin relay reply: %v", err)
			}
			replyID = newMessageID(t)
			frame := sendMessageFrame("dm", replyID, old.keys.Address, dev.Address(), "sender-delete", time.Now().UTC().Format(time.RFC3339), 0, ciphertext)
			encoded, err := json.Marshal(frame)
			if err != nil {
				t.Fatalf("encode relay reply: %v", err)
			}
			var args map[string]any
			if err := json.Unmarshal(encoded, &args); err != nil {
				t.Fatalf("decode relay reply arguments: %v", err)
			}
			raw, err = old.rpc("sendMessage", args)
			if err != nil {
				t.Fatalf("published endpoint's actual relay-origin sendMessage: %v", err)
			}
			var stored protocol.Frame
			if err := json.Unmarshal(raw, &stored); err != nil || stored.Type != "message_stored" {
				t.Fatalf("published relay-origin acceptance: type=%q code=%q decode=%v", stored.Type, stored.Code, err)
			}
			t.Logf("published relay-origin LOCAL acceptance: message=%s status=%s (not delivery)", replyID, stored.Status)
			observeReleasedReplyPending(t, old, replyID)
			var received protocol.MessageFrame
			waitForConditionMsg(t, 40*time.Second, "published-origin reply did not cross the relay to current client", func() bool {
				inbox := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
				if inbox.Type != "messages" {
					t.Fatalf("current inbox: %q/%q", inbox.Type, inbox.Code)
				}
				for _, item := range inbox.Messages {
					if item.ID == replyID {
						received = item
						return true
					}
				}
				return false
			})
			requireReleasedReplyPlaintext(t, dev.identity, received, old.keys.Address, identity.PublicKeyBase64(old.keys.PublicKey), dev.Address(), replyBody)
			plain, err := directmsg.DecryptForIdentity(dev.identity, received.Sender, identity.PublicKeyBase64(old.keys.PublicKey), received.Recipient, received.Body)
			if err != nil || plain.ReplyTo != initialID {
				t.Fatalf("relay reply reference failed: decrypt=%v", err)
			}
			// A successful DM is not proof that the old endpoint received a
			// target_proof. Its normal discovery/prober cadence outlives the
			// four-second initial diagnostic snapshot above.
			requireReleasedRelayPresenceProof(t, old, dev.Address())
			assertNoDirectContact(t, dev, old.peerIdentity(), "after old-to-current relay delivery")
			releasedRelayConnection(t, old, relayIdentity, dev.Address(), true)
		})
	}
}

func requireReleasedRelayContact(t *testing.T, row protocol.ContactFrame, owner *identity.Identity) {
	t.Helper()
	if row.Address != owner.Address || row.PubKey != identity.PublicKeyBase64(owner.PublicKey) || row.BoxKey != identity.BoxPublicKeyBase64(owner.BoxPublicKey) {
		t.Fatal("naturally learned relay contact does not match the original endpoint identity")
	}
	if err := identity.VerifyBoxKeyBinding(row.Address, row.PubKey, row.BoxKey, row.BoxSig); err != nil {
		t.Fatalf("naturally learned relay contact binding: %v", err)
	}
}

func releasedRelayConnection(t *testing.T, old *releasedNode, relay, forbidden string, require bool) bool {
	t.Helper()
	raw, err := old.rpc("getActiveConnections", nil)
	if err != nil {
		t.Fatalf("published connection observation: %v", err)
	}
	var snapshot struct {
		Connections []struct {
			Identity string `json:"identity"`
		} `json:"connections"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("published connection observation decode: %v", err)
	}
	found := false
	for _, c := range snapshot.Connections {
		if forbidden != "" && c.Identity == forbidden {
			t.Fatal("published endpoint has a forbidden direct socket to current endpoint")
		}
		if c.Identity == relay {
			found = true
		}
	}
	if require && !found {
		t.Error("published endpoint has no live connection to the required relay")
	}
	return found
}

func requireReleasedRelayTrusted(t *testing.T, dev *Service, old *releasedNode) {
	t.Helper()
	current := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if current.Type != "contacts" {
		t.Fatalf("current membership observation: %q/%q", current.Type, current.Code)
	}
	currentMember := false
	for _, c := range current.Contacts {
		if c.Address == old.keys.Address {
			currentMember = true
		}
	}
	raw, err := old.rpc("fetchTrustedContacts", nil)
	if err != nil {
		t.Fatalf("published membership observation: %v", err)
	}
	var previous protocol.Frame
	if err := json.Unmarshal(raw, &previous); err != nil || previous.Type != "contacts" {
		t.Fatalf("published membership observation: type=%q decode=%v", previous.Type, err)
	}
	oldMember := false
	for _, c := range previous.Contacts {
		if c.Address == dev.Address() {
			oldMember = true
		}
	}
	if !currentMember || !oldMember {
		t.Fatalf("public contact acceptance did not establish membership: current=%v published=%v", currentMember, oldMember)
	}
	t.Log("both endpoints are actual public-API accepted contacts; presence is observed independently")
}

func observeReleasedRelayRoutes(t *testing.T, oldRelay *releasedNode, currentRelay *Service, oldID, currentID string) {
	t.Helper()
	for _, target := range []string{oldID, currentID} {
		if oldRelay != nil {
			raw, err := oldRelay.rpc("fetchRouteLookup", map[string]any{"identity": target})
			if err != nil {
				t.Errorf("published relay lookup: %v", err)
				continue
			}
			var lookup struct {
				Identity string `json:"identity"`
				Routes   []struct {
					NextHop string  `json:"next_hop"`
					Hops    int     `json:"hops"`
					Source  string  `json:"source"`
					TTL     float64 `json:"ttl_seconds"`
				} `json:"routes"`
			}
			if err := json.Unmarshal(raw, &lookup); err != nil {
				t.Errorf("published relay lookup decode: %v", err)
				continue
			}
			for _, r := range lookup.Routes {
				t.Logf("published relay route: target=%s next_hop=%s hops=%d source=%s ttl=%g", lookup.Identity, r.NextHop, r.Hops, r.Source, r.TTL)
			}
			t.Logf("published relay route count: target=%s count=%d", target, len(lookup.Routes))
		} else {
			routes := currentRelay.routingTable.Lookup(domain.PeerIdentityFromWire(target))
			for _, r := range routes {
				address := currentRelay.resolveRouteNextHopAddress(r.NextHop, int(r.Hops))
				t.Logf("current relay route: target=%s next_hop=%s hops=%d source=%v usable_address=%s", target, r.NextHop.String(), r.Hops, r.Source, address)
			}
			t.Logf("current relay route count: target=%s count=%d", target, len(routes))
		}
	}
}

// requireReleasedRelayPresenceProof waits for the published endpoint's own
// projector to report target_proof evidence for this exact current identity.
// It drives no probe/clock/route and never substitutes the successful DM for
// proof. The poll uses the original bounded local RPC helper.
func requireReleasedRelayPresenceProof(t *testing.T, old *releasedNode, target string) {
	t.Helper()
	proven := false
	defer func() {
		if !proven {
			logReleasedRelayProbeFailure(t, old, target)
		}
	}()
	var previous protocol.PresenceFrame
	previousPresent, observed := false, false
	waitForConditionMsg(t, 45*time.Second, "published endpoint did not report current endpoint online/proof", func() bool {
		raw, err := old.rpc("fetchPresence", nil)
		if err != nil {
			t.Fatalf("published proof poll RPC: %v", err)
		}
		var frame protocol.Frame
		if err := json.Unmarshal(raw, &frame); err != nil || frame.Type != "presence" {
			t.Fatalf("published proof poll: type=%q decode=%v", frame.Type, err)
		}
		var current protocol.PresenceFrame
		present := false
		for _, row := range frame.Presence {
			if row.Identity == target {
				current, present = row, true
				break
			}
		}
		if !observed || present != previousPresent || current != previous {
			t.Logf("published target-proof transition: target=%s present=%v state=%s source=%s reason=%s generation=%d", target, present, current.State, current.Source, current.Reason, frame.PresenceGeneration)
			observed, previousPresent, previous = true, present, current
		}
		proven = present && current.State == "online" && current.Source == "proof"
		return proven
	})
}

func logReleasedRelayProbeFailure(t *testing.T, old *releasedNode, target string) {
	t.Helper()
	// Bound the emitted diagnostic to this target's last twelve original
	// old-process probe events. Keep the original full process logs via the
	// existing release fixture; this projection is not their replacement.
	lines := strings.Split(string(ansiEscape.ReplaceAll(old.stdout(t), nil)), "\n")
	emitted := 0
	for i := len(lines) - 1; i >= 0 && emitted < 12; i-- {
		if strings.Contains(lines[i], target) && strings.Contains(lines[i], "presence_probe_") {
			t.Logf("published matching probe diagnostic: %s", lines[i])
			emitted++
		}
	}
	t.Logf("published matching probe diagnostics emitted=%d target=%s (full original logs retained)", emitted, target)
}
