package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
)

// Real service/SQLite construction, no exported test bridge/clock/record port.
// This bounded slice has a node Run/CM/ACK/strict positive, plus this genuine
// service optional-refusal/echo check. It does NOT claim a combined successful
// service R stage: import_contacts establishes K; there is no existing public
// ordinary business API to inject a non-K signed min2 record or private clock.
func ownDMAdmissionRunningClient(t *testing.T, peers []string, cancelAfterStore context.CancelFunc) (*runningClient, node.OwnDMProvenancePort) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeLoopbackAddress(t), BootstrapPeers: peers, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	svc, port, err := node.NewInactiveOwnDMAdmissionCandidate(cfg, id, nil, node.OwnDMResidenceProfile{MaxPrestage: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	db := newTestStateDB(t, domain.PeerIdentityFromWire(id.Address))
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}, nodeCfg: cfg, localNode: svc, chatLog: testChatlogStore(db.Executor(), domain.PeerIdentityFromWire(id.Address))}
	c.wireSubServices()
	if cancelAfterStore != nil {
		svc.RegisterMessageStore(ownDMCancelStore{client: c, cancel: cancelAfterStore})
	} else {
		svc.RegisterMessageStore(c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("actual admission candidate Run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("actual admission candidate Run did not return")
			return
		}
		svc.WaitBackground()
	})
	return &runningClient{DesktopClient: c, address: cfg.ListenAddress}, port
}
func TestOwnDMCryptoAdmissionOptionalCancellationKeepsSQLiteEcho(t *testing.T) {
	receiver, _, gate := ownDMRunningClient(t, false, nil, true, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sender, port := ownDMAdmissionRunningClient(t, []string{receiver.address}, cancel)
	trustEachOther(t, sender, receiver)
	waitForSession(t, sender, receiver.Address())
	ownDMWaitDirectRoute(t, sender.localNode, receiver.Address())
	var receipt node.OwnDMSubmission
	observed := 0
	crypto, err := newDMCryptoWithOwnDMProvenance(sender.rpc, sender.chatlog, sender.id, port, func(r node.OwnDMSubmission) { observed++; receipt = r })
	if err != nil {
		t.Fatal(err)
	}
	sent, err := crypto.SendDirectMessage(ctx, receiver.Address(), domain.OutgoingDM{Body: "real SQLite before optional R refusal"})
	if err != nil || sent == nil || sent.Body != "real SQLite before optional R refusal" || observed != 1 || ctx.Err() == nil {
		t.Fatalf("performed storage/echo hidden by optional admission: %+v %v", sent, err)
	}
	ownDMRequirePersistedCiphertext(t, sender, sent, receiver.Address())
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, node.ErrOwnDMProvenanceUnavailable) {
		t.Fatal("canceled original escaped as evidence", err)
	}
	if sender.localNode.SecureSessionStoreStats().RelationAllocations != 0 {
		t.Fatal("canceled optional service original staged R")
	}
	if sender.dm.ownDMPort.ValidFor(sender.localNode, sender.id) || sender.dm.ownDMObserver != nil {
		t.Fatal("normal service constructor activated admission")
	}
	gate.unblock()
}
