package node

import (
	"bufio"
	"encoding/json"
	"maps"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOwnedFindNodeMixedV2KeepsPeerExchange(t *testing.T) {
	relay, address := newOwnedBareV2PilotRelay(t, 2, 5)
	target := dialOwnedBareV2PilotClient(t, relay, address, true)
	// Seed ordinary discovery readout with a documentation address. This entry
	// is not a dial candidate or an authenticated FIND_NODE member.
	const knownAddress domain.PeerAddress = "198.51.100.7:5050"
	relay.peerMu.Lock()
	relay.health[knownAddress] = &peerHealth{Address: knownAddress, Connected: true, Direction: peerDirectionInbound}
	relay.peerMu.Unlock()
	relay.rebuildPeersExchangeSnapshot()

	for _, advertise := range []bool{false, true} {
		id, conn := dialOwnedBareV2PilotWire(t, relay, address, advertise, net.Dialer{}, nil)
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
		line, err := readFrameLine(reader, protocol.MaxFrameLine)
		if err != nil {
			t.Fatalf("get_peers with FIND_NODE advertised=%t: %v", advertise, err)
		}
		var response struct {
			Type  string   `json:"type"`
			Count *int     `json:"count"`
			Peers []string `json:"peers"`
		}
		if err := json.Unmarshal([]byte(line), &response); err != nil {
			t.Fatal(err)
		}
		if response.Type != "peers" || response.Count == nil || *response.Count != 1 ||
			len(response.Peers) != 1 || response.Peers[0] != string(knownAddress) {
			t.Fatalf("peer exchange changed with FIND_NODE advertised=%t: %s", advertise, line)
		}
		if advertise {
			bareV2PilotOriginalMembers(t, relay, target.id, domain.PeerIdentityFromWire(id.Address))
		} else {
			bareV2PilotOriginalMembers(t, relay, target.id)
		}
	}
}

func TestOwnedFindNodeMixedV2KeepsEncryptedDirectMessages(t *testing.T) {
	relay, address := newOwnedBareV2PilotRelay(t, 2, 5)
	target := dialOwnedBareV2PilotClient(t, relay, address, true)
	f1, f1Wire := dialOwnedBareV2PilotWire(t, relay, address, true, net.Dialer{}, nil)
	ordinary, ordinaryWire := dialOwnedBareV2PilotWire(t, relay, address, false, net.Dialer{}, nil)
	t.Cleanup(func() { _ = ordinaryWire.Close() })
	t.Cleanup(func() { _ = f1Wire.Close() })
	for _, wire := range []net.Conn{f1Wire, ordinaryWire} {
		if err := wire.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	f1Reader, ordinaryReader := bufio.NewReader(f1Wire), bufio.NewReader(ordinaryWire)
	originals := make(map[domain.PeerIdentity]findnode.Session)
	for _, member := range bareV2PilotOriginalMembers(t, relay, target.id, domain.PeerIdentityFromWire(f1.Address)) {
		originals[member.Identity()] = member
	}
	waitFor(t, 2*time.Second, "mixed peers did not publish their ordinary delivery routes", func() bool {
		return relay.SubscriberCount(f1.Address) == 1 && relay.SubscriberCount(ordinary.Address) == 1
	})

	request := protocol.FindNodeRequest{ID: 41, Target: target.id, Challenge: domain.FindNodeChallenge{41}}
	raw, err := protocol.MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f1Wire.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	var forwarded protocol.FindNodeRequest
	select {
	case forwarded = <-target.requests:
		if forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
			t.Fatal("mixed-peer lookup changed before reaching the target")
		}
	case err := <-target.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("mixed-peer lookup did not reach the original target")
	}
	// The target retains its original request until both ordinary DMs arrive.
	for _, peer := range []struct {
		sender, recipient *identity.Identity
		wire              net.Conn
		reader, receiver  *bufio.Reader
		messageID         string
	}{
		{f1, ordinary, f1Wire, f1Reader, ordinaryReader, "mixed-f1-to-ordinary"},
		{ordinary, f1, ordinaryWire, ordinaryReader, f1Reader, "mixed-ordinary-to-f1"},
	} {
		ciphertext, err := directmsg.EncryptForMessage(peer.sender, domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(peer.recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(peer.recipient.BoxPublicKey),
		}, domain.OutgoingDM{Body: peer.messageID}, "dm", peer.messageID)
		if err != nil {
			t.Fatal(err)
		}
		writeJSONFrame(t, peer.wire, protocol.Frame{
			Type: "push_message", Topic: "dm", Recipient: peer.recipient.Address,
			PubKey: identity.PublicKeyBase64(peer.sender.PublicKey),
			BoxKey: identity.BoxPublicKeyBase64(peer.sender.BoxPublicKey),
			BoxSig: identity.SignBoxKeyBinding(peer.sender),
			Item: &protocol.MessageFrame{
				ID: peer.messageID, Sender: peer.sender.Address, Recipient: peer.recipient.Address,
				Flag: "sender-delete", CreatedAt: time.Now().UTC().Format(time.RFC3339), Body: ciphertext,
			},
		})
		pushed := readJSONTestFrame(t, peer.receiver)
		if pushed.Type != "push_message" || pushed.Topic != "dm" || pushed.Item == nil ||
			pushed.Item.ID != peer.messageID || pushed.Item.Sender != peer.sender.Address ||
			pushed.Item.Recipient != peer.recipient.Address || pushed.Item.Body != ciphertext {
			t.Fatalf("ordinary DM changed in the mixed session path: %+v", pushed)
		}
		plain, err := directmsg.DecryptForMessage(peer.recipient, domain.MessageRef{
			Topic: "dm", ID: peer.messageID, Sender: peer.sender.Address, Recipient: peer.recipient.Address,
		}, identity.PublicKeyBase64(peer.sender.PublicKey), pushed.Item.Body)
		if err != nil || plain.Body != peer.messageID {
			t.Fatal("mixed-session recipient did not authenticate and decrypt the DM", plain, err)
		}
		ack := readJSONTestFrame(t, peer.reader)
		if ack.Type != "ack_delete" || ack.ID != peer.messageID {
			t.Fatalf("missing original-hop acknowledgement for the ordinary DM: %+v", ack)
		}
		pending := relay.bridgeCapture.intake.ResourceSnapshot().Intake
		if pending.Taken != 1 || pending.Answered != 0 {
			t.Fatal("FIND_NODE request did not remain pending during ordinary delivery", pending)
		}
	}
	target.replyTo(t, forwarded)
	line, err := readFrameLine(f1Reader, protocol.MaxFrameLine)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.ParseFindNodeAnswer([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil {
		t.Fatal("FIND_NODE return failed after mixed-peer DM exchange", err)
	}
	after := make(map[domain.PeerIdentity]findnode.Session)
	for _, member := range bareV2PilotOriginalMembers(t, relay, target.id, domain.PeerIdentityFromWire(f1.Address)) {
		after[member.Identity()] = member
	}
	if !maps.Equal(originals, after) {
		t.Fatal("ordinary DM delivery changed the original FIND_NODE membership")
	}
}
