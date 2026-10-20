package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// No exported test bridge: package service directly composes the INTERNAL
// node constructor and its own private DMCrypto constructor. Real SQLite and
// Run/CM/TLS/writer remain in the path; the receiver gate delays only storage.
type ownDMReceiveStore struct {
	client  *DesktopClient
	entered chan protocol.Envelope
	release chan struct{}
	once    sync.Once
}

func (g *ownDMReceiveStore) unblock() { g.once.Do(func() { close(g.release) }) }
func (g *ownDMReceiveStore) StoreMessage(e protocol.Envelope, outgoing bool) node.StoreResult {
	if !outgoing && e.Topic == "dm" {
		copy := e
		copy.Payload = append([]byte(nil), e.Payload...)
		select {
		case g.entered <- copy:
		default:
		}
		<-g.release
	}
	return g.client.StoreMessage(e, outgoing)
}
func (g *ownDMReceiveStore) UpdateDeliveryStatus(r protocol.DeliveryReceipt) bool {
	return g.client.UpdateDeliveryStatus(r)
}

type ownDMCancelStore struct {
	client *DesktopClient
	cancel context.CancelFunc
}

func (g ownDMCancelStore) StoreMessage(e protocol.Envelope, outgoing bool) node.StoreResult {
	result := g.client.StoreMessage(e, outgoing)
	if outgoing {
		g.cancel()
	}
	return result
}
func (g ownDMCancelStore) UpdateDeliveryStatus(r protocol.DeliveryReceipt) bool {
	return g.client.UpdateDeliveryStatus(r)
}

func ownDMRunningClient(t *testing.T, candidate bool, peers []string, receiveGate bool, cancelAfterStore context.CancelFunc) (*runningClient, node.OwnDMProvenancePort, *ownDMReceiveStore) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeLoopbackAddress(t), BootstrapPeers: peers, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	var svc *node.Service
	var port node.OwnDMProvenancePort
	if candidate {
		svc, port = node.NewInactiveOwnDMProvenanceCandidate(cfg, id, nil)
	} else {
		svc = node.NewService(cfg, id, nil)
	}
	db := newTestStateDB(t, domain.PeerIdentityFromWire(id.Address))
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}, nodeCfg: cfg, localNode: svc, chatLog: testChatlogStore(db.Executor(), domain.PeerIdentityFromWire(id.Address))}
	c.wireSubServices()
	var gate *ownDMReceiveStore
	if receiveGate {
		gate = &ownDMReceiveStore{client: c, entered: make(chan protocol.Envelope, 1), release: make(chan struct{})}
		svc.RegisterMessageStore(gate)
	} else if cancelAfterStore != nil {
		svc.RegisterMessageStore(ownDMCancelStore{client: c, cancel: cancelAfterStore})
	} else {
		svc.RegisterMessageStore(c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		if gate != nil {
			gate.unblock()
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("actual own-DM Run return: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("actual own-DM Run did not return")
			return
		}
		svc.WaitBackground()
	})
	return &runningClient{DesktopClient: c, address: cfg.ListenAddress}, port, gate
}

// Health publication precedes onPeerSessionEstablished; do not race the
// first directed action against that remaining actual CM setup work.
func ownDMWaitDirectRoute(t *testing.T, s *node.Service, peer domain.PeerIdentity) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		route := s.RoutingSnapshot().BestRoute(peer)
		if route != nil && route.NextHop == peer && route.Hops == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual CM direct route never appeared")
}

// ownDMRequirePersistedCiphertext checks the raw SQLite representation, not
// the plaintext echo returned by SendDirectMessage. The authenticated sender
// key comes from the original running identity, never from the stored row.
func ownDMRequirePersistedCiphertext(t *testing.T, sender *runningClient, sent *DirectMessage, recipient domain.PeerIdentity) string {
	t.Helper()
	if sent == nil || sent.ID == "" || sent.Sender != sender.Address() || sent.Recipient != recipient {
		t.Fatal("ordinary echo identity/participants differ from the original sender and recipient")
	}
	row, err := sender.chatLog.ReadLastEntry(t.Context(), "dm", recipient)
	if err != nil {
		t.Fatal("read real sender SQLite record", err)
	}
	if row == nil {
		t.Fatal("real sender SQLite record missing")
	}
	if row.ID != sent.ID || row.RowID <= 0 {
		t.Fatalf("persisted SQLite message/row identity mismatch: id=%q rowid=%d want=%q", row.ID, row.RowID, sent.ID)
	}
	if row.Sender != sender.id.Address || row.Recipient != recipient.String() {
		t.Fatalf("persisted SQLite participants mismatch: sender=%q recipient=%q", row.Sender, row.Recipient)
	}
	if row.Body == "" || row.Body == sent.Body {
		t.Fatal("persisted SQLite body must be nonempty ciphertext rather than plaintext echo")
	}
	senderKey := identity.PublicKeyBase64(sender.id.PublicKey)
	if err := directmsg.VerifyEnvelope(sender.id.Address, senderKey, recipient.String(), row.Body); err != nil {
		t.Fatal("persisted SQLite envelope failed original sender authentication", err)
	}
	plain, err := directmsg.DecryptForIdentity(sender.id, sender.id.Address, senderKey, recipient.String(), row.Body)
	if err != nil {
		t.Fatal("persisted SQLite own-sender copy did not decrypt", err)
	}
	if plain == nil || plain.Body != sent.Body {
		t.Fatal("persisted SQLite decrypted body differs from plaintext echo")
	}
	return row.Body
}

func TestOwnDMCryptoCandidateActualCMWriteAndEcho(t *testing.T) {
	receiver, _, gate := ownDMRunningClient(t, false, nil, true, nil)
	sender, port, _ := ownDMRunningClient(t, true, []string{receiver.address}, false, nil)
	trustEachOther(t, sender, receiver)
	waitForSession(t, sender, receiver.Address())
	ownDMWaitDirectRoute(t, sender.localNode, receiver.Address())
	var receipt node.OwnDMSubmission
	observed := 0
	crypto, err := newDMCryptoWithOwnDMProvenance(sender.rpc, sender.chatlog, sender.id, port, func(r node.OwnDMSubmission) { observed++; receipt = r })
	if err != nil {
		t.Fatal(err)
	}
	sent, err := crypto.SendDirectMessage(t.Context(), receiver.Address(), domain.OutgoingDM{Body: "actual encrypted own-DM"})
	if err != nil || sent == nil || sent.Body != "actual encrypted own-DM" || sent.Recipient != receiver.Address() || sent.Sender != sender.Address() || sent.ID == "" || observed != 1 {
		t.Fatalf("ordinary echo/receipt lost: %+v %v count=%d", sent, err, observed)
	}
	var wire protocol.Envelope
	select {
	case wire = <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("real receiver never reached storage after socket write")
	}
	if wire.ID != protocol.MessageID(sent.ID) || string(wire.Payload) == sent.Body {
		t.Fatal("typed producer bypassed actual encryption or changed message")
	}
	if err := directmsg.VerifyEnvelope(sender.id.Address, identity.PublicKeyBase64(sender.id.PublicKey), receiver.id.Address, string(wire.Payload)); err != nil {
		t.Fatal("actual encrypted envelope signature", err)
	}
	var evidence node.OwnDMWriteEvidence
	deadline := time.Now().Add(3 * time.Second)
	for {
		evidence, err = receipt.TrySettle(t.Context())
		if err == nil {
			break
		}
		if !errors.Is(err, node.ErrOwnDMNotWritten) || time.Now().After(deadline) {
			t.Fatalf("genuine CM first write never settled: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if evidence.RecipientIdentity() != receiver.Address() || evidence.NearestHopIdentity() != receiver.Address() {
		t.Fatal("genuine direct X/Y identity mismatch")
	}
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, node.ErrOwnDMProvenanceUnavailable) {
		t.Fatal("own receipt settled twice", err)
	}
	persistedCiphertext := ownDMRequirePersistedCiphertext(t, sender, sent, receiver.Address())
	if persistedCiphertext != string(wire.Payload) {
		t.Fatal("persisted SQLite ciphertext differs from the genuine CM wire payload")
	}
	gate.unblock()
}

func TestOwnDMCryptoCandidateOwnerAndDefaultConstruction(t *testing.T) {
	receiver, _, gate := ownDMRunningClient(t, false, nil, true, nil)
	sender, port, _ := ownDMRunningClient(t, true, []string{receiver.address}, false, nil)
	if _, err := newDMCryptoWithOwnDMProvenance(receiver.rpc, receiver.chatlog, receiver.id, port, nil); err == nil {
		t.Fatal("foreign node capability accepted")
	}
	if _, err := newDMCryptoWithOwnDMProvenance(sender.rpc, sender.chatlog, receiver.id, port, nil); err == nil {
		t.Fatal("foreign identity capability accepted")
	}
	if _, err := newDMCryptoWithOwnDMProvenance(sender.rpc, sender.chatlog, sender.id, node.OwnDMProvenancePort{}, nil); err == nil {
		t.Fatal("zero capability accepted")
	}
	if sender.dm.ownDMPort.ValidFor(sender.localNode, sender.id) || sender.dm.ownDMObserver != nil {
		t.Fatal("ordinary DMCrypto construction enabled provenance")
	}
	trustEachOther(t, sender, receiver)
	waitForSession(t, sender, receiver.Address())
	ownDMWaitDirectRoute(t, sender.localNode, receiver.Address())
	message, err := sender.SendDirectMessage(t.Context(), receiver.Address(), domain.OutgoingDM{Body: "default path"})
	if err != nil || message == nil {
		t.Fatal("default ordinary send", err)
	}
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("default ordinary write did not reach actual receiver")
	}
	if _, err := (node.OwnDMSubmission{}).TrySettle(t.Context()); !errors.Is(err, node.ErrOwnDMProvenanceUnavailable) {
		t.Fatal("zero receipt gained authority", err)
	}
	gate.unblock()
}

func TestOwnDMCryptoCancellationAfterRealStorageKeepsEcho(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sender, port, _ := ownDMRunningClient(t, true, nil, false, cancel)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	trust(t, sender.DesktopClient, recipient)
	var receipt node.OwnDMSubmission
	crypto, err := newDMCryptoWithOwnDMProvenance(sender.rpc, sender.chatlog, sender.id, port, func(r node.OwnDMSubmission) { receipt = r })
	if err != nil {
		t.Fatal(err)
	}
	sent, err := crypto.SendDirectMessage(ctx, domain.PeerIdentityFromWire(recipient.Address), domain.OutgoingDM{Body: "stored before cancellation"})
	if err != nil || sent == nil || ctx.Err() == nil {
		t.Fatalf("performed storage masked as failed send: %+v %v ctx=%v", sent, err, ctx.Err())
	}
	row, err := sender.chatLog.ReadLastEntry(t.Context(), "dm", domain.PeerIdentityFromWire(recipient.Address))
	if err != nil || row == nil || row.ID != sent.ID {
		t.Fatal("performed SQLite storage missing", err)
	}
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, node.ErrOwnDMProvenanceUnavailable) {
		t.Fatal("cancelled optional provenance survived", err)
	}
}
