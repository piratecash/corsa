package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type globalPublicationRouter struct{ target domain.PeerAddress }

func (r globalPublicationRouter) Route(protocol.Envelope) RoutingDecision {
	return RoutingDecision{GossipTargets: []domain.PeerAddress{r.target}}
}
func TestClientTypedGlobalPublishesThroughNormalStoreAndGossip(t *testing.T) {
	svc := newTestService(t, config.NodeTypeClient)
	svc.cfg.HoldDMUntilReachable = true
	store, _ := newRecordingMockMessageStore(t)
	svc.RegisterMessageStore(store)
	target := domain.PeerAddress("192.0.2.120:64646")
	svc.router = globalPublicationRouter{target: target}
	result, err := svc.PublishGlobalMessage(t.Context(), domain.OutgoingGlobalMessage{Body: "own global"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != domain.GlobalStoredLocally || !result.Message.ID.IsValid() {
		t.Fatal("local publication missing")
	}
	svc.WaitBackground()
	svc.deliveryMu.RLock()
	count := len(svc.pending[target])
	svc.deliveryMu.RUnlock()
	if count != 1 {
		t.Fatalf("client stored global locally but normal gossip queue has %d frames", count)
	}
}

func TestGlobalPublicationRefusesStoreFailureAndUntrustedSelf(t *testing.T) {
	for _, kind := range []string{"failed", "deferred", "raw-self", "wire-self", "import-self"} {
		t.Run(kind, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeClient)
			result := StoreInserted
			if kind == "failed" {
				result = StoreFailed
			}
			if kind == "deferred" {
				result = StoreDeferred
			}
			svc.RegisterMessageStore(newCustomStoreMock(t, func(protocol.Envelope, bool) StoreResult { return result }))
			target := domain.PeerAddress("192.0.2.121:64646")
			svc.router = globalPublicationRouter{target: target}
			if kind == "failed" || kind == "deferred" {
				if _, err := svc.PublishGlobalMessage(t.Context(), domain.OutgoingGlobalMessage{Body: "not persisted"}); err == nil {
					t.Fatal("failed SQL acknowledged publication")
				}
			} else {
				frame := protocol.Frame{Type: "send_message", ID: "11111111-1111-4111-8111-111111111111", Topic: "global", Address: svc.Address(), Recipient: "*", Body: "forged self", Flag: "immutable", CreatedAt: svc.deliveryClock().UTC().Format(time.RFC3339)}
				msg, err := incomingMessageFromFrame(frame)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "wire-self" {
					msg.Via = "192.0.2.123:64646"
				}
				if msg.origin != messageOriginUntrusted {
					t.Fatal("raw frame supplied trusted origin")
				}
				if _, _, code := svc.storeIncomingMessage(msg, kind != "import-self"); code != "" {
					t.Fatal(code)
				}
			}
			svc.WaitBackground()
			svc.deliveryMu.RLock()
			n := len(svc.pending[target])
			svc.deliveryMu.RUnlock()
			if n != 0 {
				t.Fatal("failed/untrusted global emitted")
			}
		})
	}
}
func TestGlobalPublicationValidationAndMissingGateway(t *testing.T) {
	svc := newTestService(t, config.NodeTypeClient)
	store, rec := newRecordingMockMessageStore(t)
	svc.RegisterMessageStore(store)
	for _, body := range []string{"", " \t\n", string([]byte{255}), strings.Repeat("x", maxPeerCommandBodyBytes+1)} {
		if _, err := svc.PublishGlobalMessage(t.Context(), domain.OutgoingGlobalMessage{Body: domain.GlobalMessageBody(body)}); !errors.Is(err, domain.ErrInvalidGlobalMessage) {
			t.Fatal("invalid body accepted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := svc.PublishGlobalMessage(ctx, domain.OutgoingGlobalMessage{Body: "cancelled"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	rec.mu.Lock()
	n := len(rec.stored)
	rec.mu.Unlock()
	if n != 0 {
		t.Fatal("invalid body stored")
	}
	// No gateway does not prevent the local SQL acknowledgement, or imply remote delivery.
	result, err := svc.PublishGlobalMessage(t.Context(), domain.OutgoingGlobalMessage{Body: " canonical "})
	if err != nil {
		t.Fatal(err)
	}
	if result.Message.Body != "canonical" || result.Outcome != domain.GlobalStoredLocally {
		t.Fatal("canonical/local outcome lost")
	}
}

func TestGlobalHoldRepairKeepsUnreachableDMAndControlHeld(t *testing.T) {
	for _, topic := range []string{"dm", protocol.TopicControlDM} {
		t.Run(topic, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeClient)
			svc.cfg.HoldDMUntilReachable = true
			target := domain.PeerAddress("192.0.2.124:64646")
			svc.router = globalPublicationRouter{target: target}
			recipient, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			svc.pubKeys[svc.Address()] = identity.PublicKeyBase64(svc.identity.PublicKey)
			body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))
			if topic == protocol.TopicControlDM {
				body = sealControlDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))
			}
			msg := incomingMessage{ID: "11111111-1111-4111-8111-111111111111", Topic: topic, Sender: svc.Address(), Recipient: recipient.Address, Flag: protocol.MessageFlagImmutable, Body: body, CreatedAt: time.Now().UTC()}
			stored, _, code := svc.storeIncomingMessage(msg, true)
			if !stored || code != "" {
				t.Fatal("legitimate own DM/control not admitted", code)
			}
			svc.WaitBackground()
			svc.deliveryMu.RLock()
			n := len(svc.pending[target])
			entry := svc.awaitingDelivered[msg.ID]
			svc.deliveryMu.RUnlock()
			if n != 0 {
				t.Fatal("unreachable DM/control escaped hold after global fix")
			}
			if topic == "dm" && (entry == nil || entry.Hold != holdUnreachable) {
				t.Fatal("DM hold retry state not retained")
			}
		})
	}
}
