package node

import (
	"bufio"
	"context"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Private one-shot ingress evidence, never an intake reservation or R/T permit.
// The only production mint sites are immediately after successful
// bounded inbound/outbound frame reads, before parsing or Node lock waits.
// Both candidate ingress read loops call the bounded wrappers below.
type ownedFindNodeReceive struct{ cell *ownedFindNodeReceiveCell }
type ownedFindNodeReceiveCell struct {
	capture   *findNodeBridgeCaptureContext
	transport *proofTransportOwner
	stamp     findnodebridge.CaptureStamp
	received  findnodebridge.OwnedInstant
	wire      string
	consumed  atomic.Bool
}
type ownedFindNodeIngress struct {
	capture   *findNodeBridgeCaptureContext
	transport *proofTransportOwner
	source    findnode.Session
	stamp     findnodebridge.CaptureStamp
	received  findnodebridge.OwnedInstant
	request   protocol.FindNodeRequest
	wire      string
}

// Caller owns the immutable wire string delivered by the original read lane.
// Point LP is this ONE genuine Sample. No later wait may refresh received.
func (s *Service) recordOwnedFindNodeReceive(owner *proofTransportOwner, wire string) (ownedFindNodeReceive, error) {
	if s == nil || s.bridgeCapture == nil {
		return ownedFindNodeReceive{}, findnode.ErrNotReady
	}
	c := s.bridgeCapture
	if owner == nil || owner.life == nil || owner.id == 0 || owner.token == nil || owner.token != s.proofToken || c.terminal == nil || c.terminal != s.bridgeTerminal || c.terminal.token != owner.token {
		return ownedFindNodeReceive{}, findnode.ErrUnauthenticated
	}
	if len(wire) > protocol.MaxFindNodeFrameBytes {
		return ownedFindNodeReceive{}, protocol.ErrFindNodeMalformed
	}
	stamp, err := c.observer.BeginCapture()
	if err != nil {
		return ownedFindNodeReceive{}, err
	}
	reservation, err := c.consumer.TryReadClock(stamp)
	if err != nil {
		return ownedFindNodeReceive{}, err
	}
	received, err := reservation.Sample()
	reservation.Release()
	if err != nil {
		return ownedFindNodeReceive{}, err
	}
	if !c.consumer.Matches(stamp, received) {
		return ownedFindNodeReceive{}, findnode.ErrUnauthenticated
	}
	return ownedFindNodeReceive{cell: &ownedFindNodeReceiveCell{capture: c, transport: owner, stamp: stamp, received: received, wire: wire}}, nil
}

// Resolve the SAME original transport/F1 after all receive-point work. A ConnID
// lookup never substitutes a replacement common lifetime or published Session.
// Returned evidence remains historical. OfferReceived checks original
// membership and source.opened <= received under its own original claim.
func (s *Service) takeOwnedFindNodeReceive(ctx context.Context, receipt ownedFindNodeReceive) (ownedFindNodeIngress, error) {
	cell := receipt.cell
	if s == nil || s.bridgeCapture == nil || cell == nil || cell.capture != s.bridgeCapture || cell.transport == nil || cell.transport.life == nil || cell.transport.token != s.proofToken || !cell.capture.consumer.Matches(cell.stamp, cell.received) {
		return ownedFindNodeIngress{}, findnode.ErrUnauthenticated
	}
	// Every attempt retires this original cell. Copies cannot replay after a
	// transient registry refusal, cancellation or success; no hidden retry lane.
	if !cell.consumed.CompareAndSwap(false, true) {
		return ownedFindNodeIngress{}, findnode.ErrStaleWork
	}
	if ctx == nil {
		return ownedFindNodeIngress{}, findnode.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ownedFindNodeIngress{}, err
	}
	// Actual ingress calls this only after its original raw/command admission.
	// The precharge cell is historical raw bytes + point, never a parsed request.
	request, err := protocol.ParseFindNodeRequest([]byte(cell.wire))
	if err != nil {
		return ownedFindNodeIngress{}, err
	}
	s.peerMu.RLock()
	entry := s.connEntryByIDLocked(cell.transport.id)
	if entry == nil || entry.common != cell.transport.life {
		s.peerMu.RUnlock()
		return ownedFindNodeIngress{}, findnode.ErrUnauthenticated
	}
	source, found, err := s.collectOwnedFindNodeRawMemberLocked(ctx, cell.transport.id)
	s.peerMu.RUnlock()
	if err != nil {
		return ownedFindNodeIngress{}, err
	}
	if !found || source.ConnID() != cell.transport.id {
		return ownedFindNodeIngress{}, findnode.ErrUnauthenticated
	}
	return ownedFindNodeIngress{capture: cell.capture, transport: cell.transport, source: source, stamp: cell.stamp, received: cell.received, request: request, wire: cell.wire}, nil
}

// Called by the two ORIGINAL bounded read lanes before capture/parse/Node waits.
// Ordinary constructors and the older observation-only candidates do not bind
// an intake and retain their exact old command paths.
func (s *Service) recordOwnedFindNodeLine(owner *proofTransportOwner, wire string) (ownedFindNodeReceive, bool, error) {
	if s == nil || s.bridgeCapture == nil || s.bridgeCapture.intake == (findnode.DormantIntake{}) || s.bridgeCapture.sink == (findnode.ObservationSink{}) {
		return ownedFindNodeReceive{}, false, nil
	}
	kind, class := classifyFrameLine(wire)
	if class != frameLineNamed || (kind != protocol.FindNodeFrameType && kind != protocol.FindNodeAnswerFrameType) {
		return ownedFindNodeReceive{}, false, nil
	}
	receipt, err := s.recordOwnedFindNodeReceive(owner, wire)
	return receipt, true, err
}

// A successful return means an original provisional row was retained by the
// SAME dormant owner. No dummy live call/drop, frame write or Ready transition.
func (s *Service) dispatchOwnedFindNodeReceive(receipt ownedFindNodeReceive) error {
	if receipt.cell != nil {
		kind, class := classifyFrameLine(receipt.cell.wire)
		if class == frameLineNamed && kind == protocol.FindNodeAnswerFrameType {
			return s.dispatchOwnedFindNodeAnswer(receipt)
		}
	}
	ingress, err := s.takeOwnedFindNodeReceive(s.runCtx, receipt)
	if err != nil {
		return err
	}
	observation, err := s.captureOwnedFindNodeObservation(s.runCtx)
	if err != nil {
		return err
	}
	if observation.owner != ingress.capture {
		return findnode.ErrUnauthenticated
	}
	if s.identity != nil && ingress.request.Target == domain.PeerIdentityFromWire(s.identity.Address) {
		return s.dispatchOwnedFindNodeLocalTarget(ingress, observation)
	}
	target := findnode.Session{}
	for _, original := range observation.facts.allOriginal {
		if original != ingress.source && original.Identity() == ingress.request.Target && (target == (findnode.Session{}) || original.ConnID() < target.ConnID()) {
			target = original
		}
	}
	// D1 forwards only to a known exact original neighbor. Unknown targets
	// remain silent; this path does not perform D2 recursive discovery.
	if target == (findnode.Session{}) {
		return findnode.ErrNotReady
	}
	cut, err := sealOwnedFindNodeObservation(observation)
	if err != nil {
		return err
	}
	input, err := ingress.capture.sink.SealReceived(ingress.stamp, ingress.received, ingress.source, []byte(ingress.wire))
	if err != nil {
		return err
	}
	err = ingress.capture.intake.OfferReceived(input, target, cut)
	s.signalOwnedFindNodeRound() // also wake a lawful refusal that opened an empty collection
	return err
}

// These wrappers are called by the actual two read loops. The old bounded
// read/error behavior completes first; the one point is minted next,
// before capture taps, chargeLine, general dispatch or later Node/class waits.
func (s *Service) readInboundOwnedFindNodeLine(reader *bufio.Reader, id domain.ConnID, owner *proofTransportOwner) (string, ownedFindNodeReceive, bool, error, error) {
	wire, err := s.readInboundCommandLineOwned(reader, id, owner)
	if err != nil {
		return wire, ownedFindNodeReceive{}, false, nil, err
	}
	receipt, claimed, receiveErr := s.recordOwnedFindNodeLine(owner, wire)
	return wire, receipt, claimed, receiveErr, nil
}
func (s *Service) readOutboundOwnedFindNodeBeforeCharge(reader *bufio.Reader, session *peerSession, owner *proofTransportOwner) (string, ownedFindNodeReceive, bool, error, error) {
	wire, err := s.readSessionLineBeforeCharge(reader, session)
	if err != nil {
		return wire, ownedFindNodeReceive{}, false, nil, err
	}
	receipt, claimed, receiveErr := s.recordOwnedFindNodeLine(owner, wire)
	return wire, receipt, claimed, receiveErr, nil
}
func (s *Service) readOutboundOwnedFindNodeLine(reader *bufio.Reader, session *peerSession, owner *proofTransportOwner) (string, ownedFindNodeReceive, bool, error, error) {
	wire, receipt, claimed, receiveErr, readErr := s.readOutboundOwnedFindNodeBeforeCharge(reader, session, owner)
	if readErr != nil {
		return wire, ownedFindNodeReceive{}, false, nil, readErr
	}
	admitted, err := s.admitReadSessionLine(session, wire)
	if err != nil {
		return admitted, ownedFindNodeReceive{}, false, nil, err
	}
	return admitted, receipt, claimed, receiveErr, nil
}
