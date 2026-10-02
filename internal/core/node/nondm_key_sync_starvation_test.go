package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// fabricatedAuthor is a well-formed address nobody holds a key for.
func fabricatedAuthor(i int) string { return fmt.Sprintf("%040x", i+1) }

// stalledOutboundSession is an outbound session whose owner loop never
// answers a contact-sync request, so a pass that asks it stays in flight until
// the node's lifecycle context ends.
func stalledOutboundSession(t *testing.T, svc *Service, n int) *peerSession {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	address := domain.PeerAddress(fmt.Sprintf("10.0.1.%d:64646", n+1))
	session := &peerSession{
		address:       address,
		connID:        domain.ConnID(9100 + n),
		peerIdentity:  domain.PeerIdentityFromWire(owner.Address),
		sendCh:        make(chan peerSendItem, 4),
		authOK:        true,
		contactSyncCh: make(chan chan int),
	}
	svc.peerMu.Lock()
	svc.sessions[address] = session
	svc.health[address] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()
	return session
}

// TestNonDMKeySyncCannotStarveKeylessDMRecovery: three attacker sessions keep
// pushing non-DM messages of fabricated authors, and every pass they cause
// hangs on its owner loop. The keyless-DM recovery of an honest sender through
// a fourth neighbour must still get a pass: the two triggers draw from
// separate pools, so non-DM traffic can never hold the DM slots.
func TestNonDMKeySyncCannotStarveKeylessDMRecovery(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	svc.runCtx = ctx
	t.Cleanup(cancel) // runs before WaitBackground: the stalled passes end with it

	for n := 0; n < maxConcurrentSenderKeySyncPasses; n++ {
		session := stalledOutboundSession(t, svc, n)
		for i := 0; i < 5; i++ {
			author := fabricatedAuthor(n*100 + i)
			svc.dispatchPeerSessionFrame(session.address, session, protocol.Frame{
				Type:  "push_message",
				Topic: "global",
				Item: &protocol.MessageFrame{
					ID:         fmt.Sprintf("attacker-%d-%d", n, i),
					Sender:     author,
					Recipient:  "*",
					Flag:       string(protocol.MessageFlagImmutable),
					CreatedAt:  time.Now().UTC().Format(time.RFC3339),
					TTLSeconds: 300,
					Body:       "noise",
				},
			})
		}
	}

	honest, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("10.0.2.1:64646"), honest.Address, nil)
	waitSenderKeySyncScheduled(t, svc, honest.Address)
}
