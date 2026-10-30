package node

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Internal-package visibility permits service-package composition. Only the
// unused candidate constructor mints a nonzero port. Normal construction and
// raw frames do not enable this provenance branch or any record admission.
type OwnDMProvenancePort struct{ r2Capability *ownDMProvenanceOwner }
type ownDMProvenanceOwner struct {
	r2Service   *Service
	r2Identity  *identity.Identity
	r2Admission *ownDMRecordAdmission
}
type OwnDMSubmission struct{ r2Submission *ownDMSubmission }
type ownDMSubmission struct {
	r2Owner   *ownDMProvenanceOwner
	r2Result  r2aSubmissionResult
	r2Refusal error
}
type OwnDMWriteEvidence struct {
	r2Recipient r2aRecipientEvidence
	r2Hop       r2aNearestHopEvidence
}

var ErrOwnDMProvenanceUnavailable = errR2aOrigin
var ErrOwnDMNotWritten = errR2aNotWritten

// NewInactiveOwnDMProvenanceCandidate is deliberately unused by normal roots.
// It creates the Service and its capability together; there is no port getter
// or late enabling operation on an existing Service.
func NewInactiveOwnDMProvenanceCandidate(cfg config.Node, id *identity.Identity, bus *ebus.Bus) (*Service, OwnDMProvenancePort) {
	s := newService(cfg, id, bus, nil)
	owner := &ownDMProvenanceOwner{r2Service: s, r2Identity: id}
	s.r2OwnDMOwner = owner
	return s, OwnDMProvenancePort{r2Capability: owner}
}

func (p OwnDMProvenancePort) ValidFor(s *Service, id *identity.Identity) bool {
	return p.r2Capability != nil && s != nil && id != nil && p.r2Capability.r2Service == s && p.r2Capability.r2Identity == id && s.identity == id && s.r2OwnDMOwner == p.r2Capability
}

// Submit preserves the ordinary reply, including a performed storage effect
// when optional provenance is refused after I/O. It never waits for wire ACK.
func (p OwnDMProvenancePort) Submit(ctx context.Context, input protocol.Envelope) (protocol.Frame, OwnDMSubmission, error) {
	return p.submitWithResidence(ctx, ctx, input)
}

// Only the typed SDK/service path supplies a distinct original caller lifetime.
// The operation context still owns all prepare/store/send work and its refusal.
func (p OwnDMProvenancePort) SubmitWithResidence(ctx, residence context.Context, input protocol.Envelope) (protocol.Frame, OwnDMSubmission, error) {
	return p.submitWithResidence(ctx, residence, input)
}

func (p OwnDMProvenancePort) submitWithResidence(ctx, residence context.Context, input protocol.Envelope) (protocol.Frame, OwnDMSubmission, error) {
	if p.r2Capability == nil || !p.ValidFor(p.r2Capability.r2Service, p.r2Capability.r2Identity) {
		return protocol.Frame{}, OwnDMSubmission{}, errR2aOrigin
	}
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, OwnDMSubmission{}, err
	}
	s := p.r2Capability.r2Service
	originalResidence := p.captureOwnDMResidence(residence)
	prepared, err := s.prepareR2aOwnDM(ctx, input)
	if err != nil {
		if ctx.Err() != nil {
			return protocol.Frame{}, OwnDMSubmission{}, ctx.Err()
		}
		// Optional provenance may be unavailable, but ordinary admission still
		// owns its own validation/result. No second dispatch follows this call.
		return s.storeMessageFrame(ownDMFrame(input)), OwnDMSubmission{}, nil
	}
	prepared = s.bindOwnDMResidence(prepared, originalResidence)
	result, refusal := s.submitR2aOwnDM(ctx, prepared, r2aHopPublication{})
	if !result.r2Dispatched {
		return protocol.Frame{}, OwnDMSubmission{}, refusal
	}
	if refusal == nil {
		s.registerOwnDMRecordAdmission(ctx, result)
	}
	reply := protocol.Frame{Type: "message_stored", Topic: prepared.sealed.message.Topic, ID: string(prepared.sealed.message.ID), Count: result.count}
	if result.code != "" {
		reply = protocol.Frame{Type: "error", Code: result.code}
	} else {
		if !result.stored {
			reply.Type = "message_known"
		}
		reply.Status = s.storedMessageStatus(admissionRef(prepared.sealed.message))
	}
	if result.stamp == nil {
		return reply, OwnDMSubmission{}, nil
	}
	return reply, OwnDMSubmission{r2Submission: &ownDMSubmission{r2Owner: p.r2Capability, r2Result: result, r2Refusal: refusal}}, nil
}

func ownDMFrame(e protocol.Envelope) protocol.Frame {
	return protocol.Frame{Type: "send_message", Topic: e.Topic, ID: string(e.ID), Address: e.Sender, Recipient: e.Recipient, Flag: string(e.Flag), CreatedAt: e.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), TTLSeconds: e.TTLSeconds, Body: string(e.Payload)}
}

// TrySettle is nonblocking: only the existing writer's same first-item ACK
// can qualify. No supplied channel/proof, waiter, history, stage or R budget.
func (receipt OwnDMSubmission) TrySettle(ctx context.Context) (OwnDMWriteEvidence, error) {
	v := receipt.r2Submission
	if v == nil || v.r2Owner == nil || v.r2Owner.r2Service == nil || v.r2Owner.r2Service.r2OwnDMOwner != v.r2Owner || v.r2Owner.r2Service.identity != v.r2Owner.r2Identity {
		return OwnDMWriteEvidence{}, errR2aOrigin
	}
	if v.r2Refusal != nil {
		return OwnDMWriteEvidence{}, errors.Join(errR2aOrigin, v.r2Refusal)
	}
	x, y, err := v.r2Owner.r2Service.settleR2aOwnHop(ctx, v.r2Result)
	if err != nil {
		return OwnDMWriteEvidence{}, err
	}
	return OwnDMWriteEvidence{r2Recipient: x, r2Hop: y}, nil
}
func (e OwnDMWriteEvidence) RecipientIdentity() domain.PeerIdentity {
	return e.r2Recipient.original.identity
}
func (e OwnDMWriteEvidence) NearestHopIdentity() domain.PeerIdentity {
	return e.r2Hop.original.identity
}

// The original capsule was captured before finishPeerSessionForCM waits.
// Its ready bit belongs to peerMu and is set only after real CM registration,
// pin refusal checks and SessionInitReady. A stale/closed session cannot mint.
func (s *Service) publishR2OwnDMHop(session *peerSession) {
	if s.r2OwnDMOwner == nil || session == nil {
		return
	}
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	hop := session.r2OwnDMHop
	if hop == nil || hop.owner != s || hop.session != session || hop.connID != session.connID || s.sessions[session.address] != session || session.selectedGeneration == 0 || session.eligibilityEnded.Load() || session.closedBy.Load() != nil || s.runCtx.Err() != nil {
		return
	}
	hop.r2PublishedReady = true
}
