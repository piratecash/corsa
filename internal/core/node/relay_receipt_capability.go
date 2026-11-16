package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const (
	maxRelayReceiptProbes = 16
	relayReceiptProbeWait = 750 * time.Millisecond
)

type relayReceiptProbe struct {
	target domain.PeerIdentity
	boxKey string
	hash   [sha256.Size]byte
	done   chan struct{}
}

type relayReceiptProbes struct {
	mu      sync.Mutex
	pending map[domain.PeerIdentity]relayReceiptProbe
}

// ProveRelaySignedReceipts asks the final recipient once over the current
// datagram route. A proof applies only to this send and the recipient box key
// it will use. The question waits at most relayReceiptProbeWait.
func (s *Service) ProveRelaySignedReceipts(ctx context.Context, to domain.PeerIdentity, boxKey string) bool {
	layer := s.datagramLayer()
	if layer == nil || layer.pipeline == nil || to.IsZero() || boxKey == "" || ctx.Err() != nil || s.runCtx.Err() != nil {
		return false
	}
	requestCtx, cancel := context.WithTimeout(ctx, relayReceiptProbeWait)
	defer cancel()
	var label domain.PeerIdentity
	if _, err := rand.Read(label[:]); err != nil || label.IsZero() {
		return false
	}
	payload, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V: domain.IdentityLookupSchemaVersion, TargetProof: true,
		Required: []string{domain.LookupRequirementSignedDirectReceipts},
	})
	if err != nil {
		return false
	}
	probe := relayReceiptProbe{target: to, boxKey: boxKey, hash: sha256.Sum256(payload), done: make(chan struct{})}
	probes := &layer.relayReceiptProbes
	probes.mu.Lock()
	if _, busy := probes.pending[label]; busy || len(probes.pending) >= maxRelayReceiptProbes {
		probes.mu.Unlock()
		return false
	}
	if probes.pending == nil {
		probes.pending = make(map[domain.PeerIdentity]relayReceiptProbe)
	}
	// Register before enqueue: the response can overtake the send return.
	probes.pending[label] = probe
	probes.mu.Unlock()
	defer func() {
		probes.mu.Lock()
		delete(probes.pending, label)
		probes.mu.Unlock()
	}()

	frame := protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRequest, Class: domain.DatagramClassControl,
		Src: label, Dst: to, TTL: domain.DatagramDefaultMaxHops, RoutePolicy: domain.RoutePolicyBest,
		DType: domain.DTypeGetIdentity, Payload: payload,
	}
	outcome := layer.pipeline.SendLocal(requestCtx, datagram.LocalSenderIdentityResolver, datagram.LocalSendOpts{
		Frame: frame, FirstHop: s.preferredFirstHops(), CancelRequestOnContextDone: true,
	})
	if outcome.Kind() != datagram.SendQueued {
		return false
	}
	select {
	case <-probe.done:
		return requestCtx.Err() == nil && s.runCtx.Err() == nil
	case <-requestCtx.Done():
		return false
	case <-s.runCtx.Done():
		return false
	}
}

// postIdentityAuthorizer verifies the reply before it can claim a reverse
// slot. Ordinary post_identity consumers retain their existing admission.
type postIdentityAuthorizer struct{ svc *Service }

func (a *postIdentityAuthorizer) Authorize(_ context.Context, delivery datagram.DeliveryContext, payload []byte) datagram.AuthorizationDecision {
	layer := a.svc.datagramLayer()
	label, ok := delivery.Header().Label()
	if !ok || layer == nil {
		return datagram.Accept()
	}
	probes := &layer.relayReceiptProbes
	probes.mu.Lock()
	probe, ours := probes.pending[label.Raw()]
	probes.mu.Unlock()
	if ours && !probe.verifies(layer.network, label.Raw(), payload) {
		return datagram.Reject(errors.New("identity lookup: signed receipt capability proof invalid"))
	}
	return datagram.Accept()
}

func (p relayReceiptProbe) verifies(network domain.NetworkID, label domain.PeerIdentity, payload []byte) bool {
	answer, err := protocol.ParsePostIdentityPayload(payload)
	if err != nil || len(answer.TargetProof) == 0 {
		return false
	}
	body, err := protocol.VerifyIdentityRecord(answer.Record, network, p.target)
	return err == nil && body.DM && string(body.BoxKey) == p.boxKey &&
		protocol.VerifyTargetProof(answer.TargetProof, body, network, label, p.hash, answer.Record) == nil
}

func (p *relayReceiptProbes) complete(label domain.PeerIdentity) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	probe, ok := p.pending[label]
	if ok {
		delete(p.pending, label)
		close(probe.done)
	}
	return ok
}
