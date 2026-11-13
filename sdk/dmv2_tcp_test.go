//go:build s6_direct_dmv2_qa

package sdk

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestSDKDirectDMV2LifecycleOverLoopbackTCP(t *testing.T) {
	t.Setenv("CORSA_CONNECT_ONLY", "")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	readerCfg := directDMV2TCPConfig(t, []string{})
	authorCfg := directDMV2TCPConfig(t, []string{readerCfg.Node.ListenAddress})
	if authorCfg.Node.ListenAddress == readerCfg.Node.ListenAddress {
		t.Fatal("two distinct local listeners are required")
	}
	reader := newDirectDMV2TCPRuntime(t, readerCfg)
	author := newDirectDMV2TCPRuntime(t, authorCfg)
	ownDMRuntimeContact(t, author, reader)

	const body = "local TCP direct DM lifecycle"
	queued, err := author.SendDirectMessage(ctx, reader.Address(), body)
	if err != nil || queued == nil || queued.ReceiptStatus != protocol.MessageStatusQueued {
		t.Fatalf("SDK did not queue the message before connecting: %v", err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: queued.ID, Sender: author.Address(), Recipient: reader.Address()}
	authorStore := author.client.ChatlogGateway().Store()
	readerStore := reader.client.ChatlogGateway().Store()
	before, found, err := authorStore.EntryByRef(ctx, ref)
	if err != nil || !found || !chatlog.NeverEmitted(before.Metadata) ||
		!directmsg.HasV2EnvelopePrefix([]byte(before.Body)) {
		t.Fatalf("missing queued sealed outbox row: found=%v err=%v", found, err)
	}

	events := reader.SubscribeDirectMessages(ctx)
	if err := reader.Start(ctx); err != nil {
		t.Fatalf("start reader: %v", err)
	}
	if err := author.Start(ctx); err != nil {
		t.Fatalf("start author: %v", err)
	}
	select {
	case msg, ok := <-events:
		if !ok || msg.ID != ref.ID || msg.Sender != ref.Sender || msg.Recipient != ref.Recipient || msg.Body != body {
			t.Fatal("incoming SDK stream did not return the exact decrypted message")
		}
	case <-ctx.Done():
		t.Fatalf("wait for the SDK message: %v", ctx.Err())
	}
	inbox, found, err := readerStore.EntryByRef(ctx, ref)
	if err != nil || !found || inbox.Body != before.Body || inbox.DeliveryStatus != chatlog.StatusDelivered {
		t.Fatalf("recipient did not commit the original sealed message: found=%v err=%v", found, err)
	}
	waitDirectDMV2TCP(t, ctx, "author did not store delivered", func() (bool, error) {
		row, found, err := authorStore.EntryByRef(ctx, ref)
		onWire, known := chatlog.OnWire(row.Metadata)
		return found && row.DeliveryStatus == chatlog.StatusDelivered && known && onWire, err
	})
	if !author.nodeService.DirectDMV2TCPWriterTest(ref.Recipient, true) ||
		!reader.nodeService.DirectDMV2TCPWriterTest(ref.Sender, false) {
		t.Fatal("direct writer was not the expected published TLSv2 connection")
	}

	sender := domain.PeerIdentityFromWire(ref.Sender)
	recipient := domain.PeerIdentityFromWire(ref.Recipient)
	if err := reader.client.MarkConversationSeen(ctx, sender, []service.DirectMessage{{
		ID: ref.ID, Sender: sender, Recipient: recipient, ReceiptStatus: protocol.ReceiptStatusDelivered,
	}}); err != nil {
		t.Fatalf("mark seen: %v", err)
	}
	waitDirectDMV2TCP(t, ctx, "seen acknowledgement did not commit", func() (bool, error) {
		var acknowledged int
		err := reader.database.Executor().QueryRowContext(ctx,
			"SELECT 1 FROM seen_ack WHERE topic=? AND id=? AND sender=? AND recipient=?",
			ref.Topic, ref.ID, ref.Sender, ref.Recipient).Scan(&acknowledged)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		_, pending := reader.nodeService.DirectDMV2SeenRetryTest(ref)
		return acknowledged == 1 && !pending, err
	})
	for _, store := range []*chatlog.Store{authorStore, readerStore} {
		row, found, err := store.EntryByRef(ctx, ref)
		if err != nil || !found || row.DeliveryStatus != chatlog.StatusSeen {
			t.Fatalf("both persisted messages must be seen: found=%v err=%v", found, err)
		}
	}
	for _, runtime := range []*Runtime{author, reader} {
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func directDMV2TCPConfig(t *testing.T, seeds []string) Config {
	t.Helper()
	cfg := testRuntimeConfig(t)
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Node.ListenAddress = probe.Addr().String()
	advertise := uint16(probe.Addr().(*net.TCPAddr).Port)
	cfg.Node.AdvertisePort = &advertise
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.App.Network = "gazeta-devnet"
	cfg.Node.Type = NodeTypeFull
	cfg.Node.AllowPrivatePeers = true
	cfg.Node.MaxOutgoingPeers = 1
	cfg.Node.ProxyAddress = ""
	cfg.Node.DownloadDir = filepath.Join(cfg.Node.ChatLogDir, "downloads")
	cfg.Node.BootstrapPeers = append([]string{}, seeds...)
	internal := cfg.internal()
	if cfg.RPC.Enabled || internal.Node.ProxyAddress != "" ||
		internal.Node.ListenAddress != cfg.Node.ListenAddress || len(internal.Node.BootstrapPeers) != len(seeds) {
		t.Fatal("unexpected network configuration for loopback fixture")
	}
	for index, seed := range internal.Node.BootstrapPeers {
		host, port, err := net.SplitHostPort(seed)
		number, portErr := strconv.ParseUint(port, 10, 16)
		if err != nil || portErr != nil || number == 0 || host != "127.0.0.1" || seed != seeds[index] {
			t.Fatal("bootstrap peer escaped the fixture's loopback address")
		}
	}
	return cfg
}

func newDirectDMV2TCPRuntime(t *testing.T, cfg Config) *Runtime {
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
	runtime.nodeService.EnableDirectDMV2InboundWakeTest()
	if err := runtime.client.EnableDirectDMV2SendForTest(time.Hour); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func waitDirectDMV2TCP(t *testing.T, ctx context.Context, description string, ready func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := ready()
		if err != nil {
			t.Fatalf("%s: %v", description, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
