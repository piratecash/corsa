package sdk

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

func TestSDKDirectMessageSelectsNegotiatedSignedReceiptPolicy(t *testing.T) {
	t.Setenv("CORSA_CONNECT_ONLY", "")
	ctx := t.Context()
	readerCfg := ownDMRuntimeConfig(t, []string{})
	authorCfg := ownDMRuntimeConfig(t, []string{readerCfg.Node.ListenAddress})
	ownDMRuntimeAssertLoopbackConfig(t, readerCfg, nil)
	ownDMRuntimeAssertLoopbackConfig(t, authorCfg, []string{readerCfg.Node.ListenAddress})
	newRuntime := func(cfg Config) *Runtime {
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
	reader, author := newRuntime(readerCfg), newRuntime(authorCfg)
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
