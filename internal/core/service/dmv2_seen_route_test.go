//go:build s6_direct_dmv2_qa

package service

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

type seenTransportNode struct {
	db     *storage.Database
	node   *node.Service
	client *DesktopClient
}

func TestDirectDMV2SeenUsesNormalLocalAndRetryDispatch(t *testing.T) {
	t.Run("reader accepts", func(t *testing.T) { testDirectDMV2SeenRoute(t, false, false) })
	t.Run("reader dials", func(t *testing.T) { testDirectDMV2SeenRoute(t, true, false) })
	t.Run("reader waits offline", func(t *testing.T) { testDirectDMV2SeenRoute(t, true, true) })
}

func testDirectDMV2SeenRoute(t *testing.T, readerDials, offline bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	newIdentity := func() *identity.Identity {
		t.Helper()
		id, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	author, reader := newIdentity(), newIdentity()
	owner := domain.PeerIdentityFromWire(reader.Address)
	const network = "gazeta-devnet"
	app := config.App{Version: "test", Network: network}
	openFactory := func(id *identity.Identity) func() seenTransportNode {
		cfg := configtest.WithTempDataDirs(t, config.Node{Type: domain.NodeTypeFull, ListenAddress: "127.0.0.1:0", AllowPrivatePeers: true, DeliveryRetryMaxAttempts: 2})
		dbCfg := storage.Config{ExplicitPath: filepath.Join(t.TempDir(), "seen-transport.db"),
			Owner: domain.PeerIdentityFromWire(id.Address), Catalog: migrations.Catalog()}
		return func() seenTransportNode {
			t.Helper()
			db, err := storage.Open(ctx, dbCfg)
			if err != nil {
				t.Fatal(err)
			}
			svc := node.NewService(cfg, id, ebus.New())
			svc.EnableDirectDMV2InboundWakeTest()
			t.Cleanup(func() {
				svc.CloseDirectDMV2SeenJournalTest()
				_ = db.Close()
			})
			return seenTransportNode{db, svc, NewDesktopClient(app, cfg, id, svc, db)}
		}
	}
	openSender, openReader := openFactory(author), openFactory(reader)
	sender, receiver := openSender(), openReader()
	ref := domain.MessageRef{
		Topic: "dm", ID: "387238ec-7f0e-4b6b-88dd-346bc1bb1902",
		Sender: author.Address, Recipient: reader.Address,
	}
	created := time.Now().UTC().Truncate(time.Second)
	flag := protocol.MessageFlagAnyDelete
	sealed, err := directmsg.SealV2(author, domain.DMRecipient{
		Address: owner, BoxKeyBase64: identity.BoxPublicKeyBase64(reader.BoxPublicKey),
	}, directmsg.V2Metadata{
		V2Binding: directmsg.V2Binding{
			Network: network, Topic: ref.Topic, ID: ref.ID,
			Sender: ref.Sender, Recipient: ref.Recipient, Flag: string(flag),
		},
		CreatedAt: created.Unix(), ExpiresAt: created.Add(time.Hour).Unix(),
	}, directmsg.PlainMessage{Body: "normal message for seen exchange"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{
		Topic: ref.Topic, ID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient,
		Flag: flag, Payload: []byte(sealed), CreatedAt: created, TTLSeconds: 3600,
	}
	if sender.client.store.StoreMessage(envelope, true) != node.StoreInserted ||
		receiver.client.store.StoreMessage(envelope, false) != node.StoreInserted {
		t.Fatal("failed to seed sender and recipient rows")
	}
	if !sender.client.store.UpdateDeliveryStatus(protocol.DeliveryReceipt{
		MessageID: envelope.ID, Sender: ref.Recipient, Recipient: ref.Sender, Status: protocol.ReceiptStatusDelivered,
	}) {
		t.Fatal("failed to seed delivered sender row")
	}
	// Start the author again so its usual outbox registration learns the
	// delivered row and can accept a later seen after restart.
	sender.node.CloseDirectDMV2SeenJournalTest()
	if err := sender.db.Close(); err != nil {
		t.Fatal(err)
	}
	sender = openSender()

	if _, err := receiver.db.Executor().ExecContext(ctx, `CREATE TRIGGER fail_seen_transport_confirmation BEFORE INSERT ON seen_ack
		BEGIN SELECT RAISE(ABORT, 'fixture seen transport confirmation unavailable'); END`); err != nil {
		t.Fatal(err)
	}

	if offline {
		markDirectDMV2Seen(t, ctx, receiver, ref)
		receiver.node.WaitBackground()
		receipt, ok := receiver.node.DirectDMV2SeenRetryTest(ref)
		if !ok || receipt.Signature == "" {
			t.Fatal("offline seen did not enter the signed retry queue")
		}
		_, due, ok := receiver.node.DirectDMV2SeenScheduleTest(ref)
		if !ok {
			t.Fatal("missing schedule for the offline seen")
		}
		// Six due polls exceed the two-send budget while there is no route.
		for i := 0; i < 6; i++ {
			receiver.node.RunDirectDMV2RetryTickForTest(due)
			attempts, next, pending := receiver.node.DirectDMV2SeenScheduleTest(ref)
			if !pending || attempts != 0 || !next.Equal(due.Add(time.Minute)) {
				t.Fatalf("offline poll %d: pending=%v attempts=%d next=%s", i, pending, attempts, next)
			}
			due = next
		}
		current, pending := receiver.node.DirectDMV2SeenRetryTest(ref)
		rows, err := receiver.client.store.UnconfirmedSeen()
		if !pending || current != receipt || err != nil || len(rows) != 1 {
			t.Fatalf("offline receipt was not retained: pending=%v rows=%d err=%v", pending, len(rows), err)
		}
	}
	seenTransportRound(t, ctx, sender, receiver, author, reader, ref, true, readerDials, offline)
	if _, err := receiver.db.Executor().ExecContext(ctx, "DROP TRIGGER fail_seen_transport_confirmation"); err != nil {
		t.Fatal(err)
	}
	if err := sender.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.db.Close(); err != nil {
		t.Fatal(err)
	}
	sender, receiver = openSender(), openReader()
	seenTransportRound(t, ctx, sender, receiver, author, reader, ref, false, readerDials, false)
	if err := receiver.db.Close(); err != nil {
		t.Fatal(err)
	}
	finalReader := openReader()
	if _, pending := finalReader.node.DirectDMV2SeenRetryTest(ref); pending {
		t.Fatal("confirmed seen returned after opening SQLite")
	}
}

func markDirectDMV2Seen(t *testing.T, ctx context.Context, receiver seenTransportNode, ref domain.MessageRef) {
	t.Helper()
	author := domain.PeerIdentityFromWire(ref.Sender)
	owner := domain.PeerIdentityFromWire(ref.Recipient)
	if err := receiver.client.MarkConversationSeen(ctx, author, []DirectMessage{{
		ID: ref.ID, Sender: author, Recipient: owner, ReceiptStatus: protocol.ReceiptStatusDelivered,
	}}); err != nil {
		t.Fatal(err)
	}
}

func seenTransportRound(t *testing.T, ctx context.Context, sender, receiver seenTransportNode, author, reader *identity.Identity, ref domain.MessageRef, first, readerDials, offline bool) {
	t.Helper()
	if _, pending := receiver.node.DirectDMV2SeenRetryTest(ref); pending != (!first || offline) {
		t.Fatal("unexpected initial seen retry state")
	}
	listener, dialer := receiver.node, sender.node
	localID, remoteID := author, reader
	if readerDials {
		listener, dialer = sender.node, receiver.node
		localID, remoteID = reader, author
	}
	left, right := net.Pipe()
	serverAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 43001}
	peerAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 43002}
	server := seenTransportPipe{Conn: left, local: serverAddr, remote: peerAddr}
	raw := seenTransportPipe{Conn: right, local: peerAddr, remote: serverAddr}
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		listener.HandleDirectDMV2InboundWakeTest(server)
	}()
	t.Cleanup(func() {
		_ = raw.Close()
		_ = server.Close()
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Error("seen listener did not join")
		}
	})

	local, err := sessionv2.NewLocal(localID, domain.NetworkID("gazeta-devnet"))
	if err != nil {
		t.Fatal(err)
	}
	proven, err := sessionv2.Dial(ctx, raw, local, protocol.Frame{
		Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion,
		NodeType: string(domain.NodeTypeFull), Capabilities: []string{string(domain.CapDirectDMV2)},
	}, sessionv2.ExpectPeer(domain.PeerIdentityFromWire(remoteID.Address)), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := proven.Peer()
	if err != nil || !slices.Contains(peer.Intro.Capabilities, string(domain.CapDirectDMV2)) {
		t.Fatalf("direct capability missing: %v", err)
	}
	conn, err := proven.Conn()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	outbound, err := dialer.AttachDirectDMV2OutboxTestSession(proven)
	if err != nil {
		t.Fatal(err)
	}
	defer outbound.Close()
	wire := bufio.NewReader(conn)
	read := func(want string) protocol.Frame {
		t.Helper()
		for {
			line, err := wire.ReadBytes('\n')
			if err != nil {
				t.Fatalf("read %s: %v", want, err)
			}
			var frame protocol.Frame
			if err := json.Unmarshal(line, &frame); err != nil {
				t.Fatal(err)
			}
			if frame.Type == want {
				return frame
			}
			if frame.Type == "error" || frame.Type == "push_delivery_receipt" || frame.Type == "ack_delete" {
				t.Fatalf("unexpected %s while reading %s", frame.Type, want)
			}
		}
	}
	// A ping/pong first proves the ordinary listener completed auth and is
	// serving this connection. The final pong orders the ack behind its write.
	if err := outbound.SendBarrier(); err != nil {
		t.Fatal(err)
	}
	read("pong")
	if readerDials && !outbound.PublishForRetryTest() {
		t.Fatal("reader session publication failed")
	}
	if first && !offline {
		markDirectDMV2Seen(t, ctx, receiver, ref)
		receiver.node.WaitBackground()
	} else {
		_, due, ok := receiver.node.DirectDMV2SeenScheduleTest(ref)
		if !ok {
			t.Fatal("missing scheduled retry")
		}
		receiver.node.RunDirectDMV2RetryTickForTest(due)
		attempts, _, ok := receiver.node.DirectDMV2SeenScheduleTest(ref)
		if !ok || attempts != 1 {
			t.Fatalf("reconnected writer did not spend one attempt: pending=%v attempts=%d", ok, attempts)
		}
	}
	receipt, pending := receiver.node.DirectDMV2SeenRetryTest(ref)
	if !pending {
		t.Fatal("seen did not enter the exact retry journal")
	}
	if backlog, relay := receiver.node.DirectDMV2SeenGenericCopiesTest(receipt); backlog || relay {
		t.Fatal("direct seen also entered generic delivery state")
	}
	receiptFrame := read("push_delivery_receipt")
	wantStatus := protocol.ReceiptStatusSeen
	if readerDials {
		wantStatus = protocol.ReceiptStatusSeenAck
	}
	if r := receiptFrame.Receipt; r == nil || r.MessageID != ref.ID || r.Status != wantStatus || r.Signature == "" {
		t.Fatalf("missing signed %s on the TLSv2 stream", wantStatus)
	}
	outbound.Dispatch(receiptFrame)
	dialer.WaitBackground()
	if err := outbound.SendBarrier(); err != nil {
		t.Fatal(err)
	}
	read("pong")
	entry, found, err := sender.client.chatLog.EntryByRef(ctx, ref)
	if err != nil || !found || entry.DeliveryStatus != chatlog.StatusSeen {
		t.Fatalf("author did not persist seen: found=%v err=%v", found, err)
	}
	_, pending = receiver.node.DirectDMV2SeenRetryTest(ref)
	durable, err := receiver.client.store.UnconfirmedSeen()
	if err != nil || pending != first || (len(durable) == 1) != first || len(durable) > 1 {
		t.Fatalf("reader seen retry: pending=%v rows=%d err=%v", pending, len(durable), err)
	}
	outbound.Close()
	select {
	case <-handlerDone:
		listener.CloseDirectDMV2SeenJournalTest()
	case <-ctx.Done():
		t.Fatal("original accepted handler did not finish")
	}
}

type seenTransportPipe struct {
	net.Conn
	local, remote net.Addr
}

func (c seenTransportPipe) LocalAddr() net.Addr  { return c.local }
func (c seenTransportPipe) RemoteAddr() net.Addr { return c.remote }
