package sdk

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestSDKDirectMessageSelectsNegotiatedSignedReceiptPolicy(t *testing.T) {
	t.Setenv("CORSA_CONNECT_ONLY", "")
	ctx := t.Context()
	readerCfg := ownDMRuntimeConfig(t, []string{})
	authorCfg := ownDMRuntimeConfig(t, []string{readerCfg.Node.ListenAddress})
	ownDMRuntimeAssertLoopbackConfig(t, readerCfg, nil)
	ownDMRuntimeAssertLoopbackConfig(t, authorCfg, []string{readerCfg.Node.ListenAddress})
	reader, author := newReceiptSigningRuntime(t, readerCfg), newReceiptSigningRuntime(t, authorCfg)
	ownDMRuntimeContact(t, author, reader)
	refFor := func(id string) domain.MessageRef {
		return domain.MessageRef{Topic: "dm", ID: id, Sender: author.Address(), Recipient: reader.Address()}
	}
	send := func(body string) domain.MessageRef {
		t.Helper()
		msg, err := author.SendDirectMessage(ctx, reader.Address(), body)
		if err != nil || msg == nil {
			t.Fatalf("send direct message: %v", err)
		}
		return refFor(msg.ID)
	}
	authorStore := author.client.ChatlogGateway().Store()
	readerStore := reader.client.ChatlogGateway().Store()
	legacy := send("sealed before signed receipts were negotiated")
	row, found, err := authorStore.EntryByRef(ctx, legacy)
	if err != nil || !found || chatlog.NegotiatedSignedReceipts(row.Metadata) {
		t.Fatalf("queued message must keep legacy receipt policy, found=%v err=%v", found, err)
	}
	for _, runtime := range []*Runtime{reader, author} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ownDMRuntimeWait(t, "SDK peers did not negotiate signed receipts", func() bool {
		return author.nodeService.SupportsSignedReceipts(domain.PeerIdentityFromWire(reader.Address())) &&
			reader.nodeService.SupportsSignedReceipts(domain.PeerIdentityFromWire(author.Address()))
	})
	protected := send("sealed after signed receipts were negotiated")
	for _, store := range []*chatlog.Store{authorStore, readerStore} {
		ownDMRuntimeWait(t, "SDK did not persist the protected delivery", func() bool {
			row, found, err := store.EntryByRef(ctx, protected)
			if err != nil {
				t.Fatal(err)
			}
			return found && row.DeliveryStatus == chatlog.StatusDelivered && chatlog.NegotiatedSignedReceipts(row.Metadata)
		})
	}
}

func TestSDKFirstDirectMessageNegotiatesSignedReceiptsThroughRelay(t *testing.T) {
	t.Setenv("CORSA_CONNECT_ONLY", "")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	relayCfg := ownDMRuntimeConfig(t, []string{})
	relayAddress := relayCfg.Node.ListenAddress
	seeds := []string{relayAddress}
	authorCfg, readerCfg := ownDMRuntimeConfig(t, seeds), ownDMRuntimeConfig(t, seeds)
	if authorCfg.Node.ListenAddress == readerCfg.Node.ListenAddress ||
		authorCfg.Node.ListenAddress == relayAddress || readerCfg.Node.ListenAddress == relayAddress {
		t.Fatal("the fixture needs three distinct loopback addresses")
	}
	listen := false
	for _, cfg := range []*Config{&authorCfg, &readerCfg} {
		cfg.Node.Type = NodeTypeClient
		cfg.Node.ListenerEnabled = &listen
	}
	ownDMRuntimeAssertLoopbackConfig(t, relayCfg, nil)
	ownDMRuntimeAssertLoopbackConfig(t, authorCfg, seeds)
	ownDMRuntimeAssertLoopbackConfig(t, readerCfg, seeds)
	relay := newReceiptSigningRuntime(t, relayCfg)
	author := newReceiptSigningRuntime(t, authorCfg)
	reader := newReceiptSigningRuntime(t, readerCfg)
	// The saved contact supplies ordinary encryption keys. It has no
	// receipt capability assertion, and the clients have never connected.
	ownDMRuntimeContact(t, author, reader)
	received := reader.SubscribeDirectMessages(ctx)
	for _, runtime := range []*Runtime{relay, reader, author} {
		if err := runtime.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if runtime != relay {
			result := runtime.nodeService.ConnectOnly(ctx, relayAddress)
			if result.Type != "ok" || len(result.Peers) != 1 || result.Peers[0] != relayAddress {
				t.Fatalf("pin SDK client to relay: %s %s", result.Type, result.Status)
			}
		}
	}
	authorID := domain.PeerIdentityFromWire(author.Address())
	readerID := domain.PeerIdentityFromWire(reader.Address())
	relayID := domain.PeerIdentityFromWire(relay.Address())
	relayRoute := func(source *Runtime, target domain.PeerIdentity) bool {
		route := source.nodeService.RoutingSnapshot().BestRoute(target)
		return route != nil && route.NextHop == relayID && route.Hops == 2
	}
	ownDMRuntimeWait(t, "both clients need a route through the relay", func() bool {
		return relayRoute(author, readerID) && relayRoute(reader, authorID)
	})

	const body = "first SDK message through a relay"
	sent, err := author.SendDirectMessage(ctx, reader.Address(), body)
	if err != nil || sent == nil {
		t.Fatalf("first ordinary SDK send: %v", err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: sent.ID, Sender: author.Address(), Recipient: reader.Address()}
	select {
	case msg, ok := <-received:
		if !ok || msg.ID != ref.ID || msg.Sender != ref.Sender || msg.Recipient != ref.Recipient || msg.Body != body {
			t.Fatal("the reader did not receive the exact SDK message through the relay")
		}
	case <-ctx.Done():
		t.Fatalf("wait for the first relay message: %v", ctx.Err())
	}
	for _, runtime := range []*Runtime{author, reader} {
		row, found, err := runtime.client.ChatlogGateway().Store().EntryByRef(ctx, ref)
		if err != nil || !found || !chatlog.NegotiatedSignedReceipts(row.Metadata) {
			t.Fatalf("first relay message is missing its protected receipt policy on %s, found=%v err=%v", runtime.Address(), found, err)
		}
	}

	_, readerRecord := reader.nodeService.SelfIdentityRecord()
	network := domain.NetworkID(authorCfg.App.Network)
	waitForSDKRelayReceipt(t, author, network, ref, string(readerRecord.PubKey), protocol.ReceiptStatusDelivered)
	if err := reader.client.MarkConversationSeen(ctx, authorID, []service.DirectMessage{{
		ID: ref.ID, Sender: authorID, Recipient: readerID, ReceiptStatus: protocol.ReceiptStatusDelivered,
	}}); err != nil {
		t.Fatalf("mark first relay message seen: %v", err)
	}
	waitForSDKRelayReceipt(t, author, network, ref, string(readerRecord.PubKey), protocol.ReceiptStatusSeen)
	ownDMRuntimeWait(t, "signed seen_ack did not commit for the first relay message", func() bool {
		var confirmed int
		err := reader.database.Executor().QueryRowContext(ctx,
			"SELECT 1 FROM seen_ack WHERE topic=? AND id=? AND sender=? AND recipient=?",
			ref.Topic, ref.ID, ref.Sender, ref.Recipient).Scan(&confirmed)
		if errors.Is(err, sql.ErrNoRows) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		return confirmed == 1
	})
	if !relayRoute(author, readerID) || !relayRoute(reader, authorID) {
		t.Fatal("the endpoints did not retain their relay-only routes")
	}
}

func newReceiptSigningRuntime(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	return runtime
}

func waitForSDKRelayReceipt(t *testing.T, author *Runtime, network domain.NetworkID, ref domain.MessageRef, key, status string) {
	t.Helper()
	ownDMRuntimeWait(t, "missing signed "+status+" receipt", func() bool {
		reply := author.nodeService.HandleLocalFrame(protocol.Frame{Type: "fetch_delivery_receipts", Recipient: ref.Sender})
		for _, frame := range reply.Receipts {
			if frame.MessageID != ref.ID || frame.Status != status {
				continue
			}
			if frame.Sender != ref.Recipient || frame.Recipient != ref.Sender || frame.Signature == "" {
				t.Fatalf("unexpected %s receipt identity or signature", status)
			}
			stamp, err := time.Parse(time.RFC3339, frame.DeliveredAt)
			if err != nil {
				t.Fatal(err)
			}
			receipt := protocol.DeliveryReceipt{
				MessageID: protocol.MessageID(frame.MessageID), Sender: frame.Sender, Recipient: frame.Recipient,
				Status: frame.Status, DeliveredAt: stamp,
			}
			if err := protocol.VerifyDeliveryReceipt(network, ref, receipt, key, frame.Signature); err != nil {
				t.Fatalf("%s receipt signature: %v", status, err)
			}
			return true
		}
		return false
	})
}
