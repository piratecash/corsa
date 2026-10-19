package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestGlobalPublicationRPCUsesRuntimeAndSQLiteWithoutRecursion(t *testing.T) {
	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, rpcCall := range []bool{false, true} {
		var id string
		if rpcCall {
			raw, err := runtime.ExecuteContext(t.Context(), "publishGlobal", map[string]interface{}{"body": "rpc global"})
			if err != nil {
				t.Fatal(err)
			}
			var result domain.GlobalPublication
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			id = string(result.Message.ID)
			if result.Outcome != domain.GlobalStoredLocally {
				t.Fatal("RPC overstated local result")
			}
		} else {
			result, err := runtime.PublishGlobalMessage(t.Context(), "own global")
			if err != nil {
				t.Fatal(err)
			}
			id = result.ID
			if result.ClaimedSender != runtime.Address() || result.Outcome != "stored_locally" {
				t.Fatal("publisher accepted foreign identity")
			}
		}
		row, found, err := runtime.client.ChatlogGateway().Store().EntryByID(t.Context(), domain.MessageID(id))
		if err != nil || !found || row.Sender != runtime.Address() || row.Recipient != "*" || row.Flag != "immutable" {
			t.Fatal("publisher did not commit real SQLite row", err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := runtime.PublishGlobalMessage(cancelled, "cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PublishGlobalMessage(t.Context(), "closed"); !errors.Is(err, errClosed) {
		t.Fatal("closed runtime accepted global", err)
	}
	if _, err := runtime.Execute("publishGlobal", map[string]interface{}{"body": "closed"}); !errors.Is(err, errClosed) {
		t.Fatal("closed RPC runtime accepted global", err)
	}
}

func TestGlobalPublicationRealSQLiteFailure(t *testing.T) {
	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := runtime.database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PublishGlobalMessage(t.Context(), "must not emit"); err == nil {
		t.Fatal("closed SQLite acknowledged own publication")
	}
}

type blockingGlobalStore struct{ entered, release chan struct{} }

func (s *blockingGlobalStore) StoreMessage(protocol.Envelope, bool) node.StoreResult {
	close(s.entered)
	<-s.release
	return node.StoreInserted
}
func (*blockingGlobalStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }
func TestGlobalRPCOperationLeaseDrainsBeforeClose(t *testing.T) {
	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingGlobalStore{entered: make(chan struct{}), release: make(chan struct{})}
	runtime.nodeService.RegisterMessageStore(store)
	publishDone := make(chan error, 1)
	go func() {
		_, err := runtime.ExecuteContext(t.Context(), "publishGlobal", map[string]interface{}{"body": "leased"})
		publishDone <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("RPC did not enter direct typed publisher")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- runtime.Close() }()
	select {
	case <-closeDone:
		close(store.release)
		t.Fatal("Close bypassed admitted publisher")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	if err := <-publishDone; err != nil {
		t.Fatal("committed publication result lost during cancellation", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}
