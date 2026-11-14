package loadstand

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
)

// FinalAccountingResult is a value-only post-Done reduction. It is distinct
// from the sole waiter's exit result and never delays publication of Done.
var ErrFinalAccountingPending = errors.New("confirmed process exit; final accounting ownership retained")

type FinalAccountingReason string

const (
	FinalExitUnclean           FinalAccountingReason = "unclean_owned_exit"
	FinalReportRejected        FinalAccountingReason = "invalid_final_report"
	FinalCapabilityCloseFailed FinalAccountingReason = "capability_close_failed"
	FinalArtifactsReleased     FinalAccountingReason = "explicitly_released"
)

type FinalAccountingResult struct {
	Finality Finality
	Traffic  domain.TransportTrafficStats
	Reason   FinalAccountingReason
}

type FinalRouteWireResult struct {
	FinalAccountingResult
	Endpoints []RouteWireEndpoint
	Seal      RouteWireFinalSeal
}

type boundRouteWireWindow struct {
	binding    CompletedStartBinding
	commitment RouteWireCommitment
}

// finalReduction has one context-aware I/O owner. No terminal/member mutex is
// held while opening or reading the retained root. Cancellation retains it.
type finalReduction struct {
	once          sync.Once
	gate          chan struct{}
	sealed        bool
	result        FinalAccountingResult
	binding       CompletedStartBinding
	wireWindow    *boundRouteWireWindow
	wireEndpoints []RouteWireEndpoint
	wireSeal      RouteWireFinalSeal
}

func (r *finalReduction) acquire(ctx context.Context) error {
	r.once.Do(func() { r.gate = make(chan struct{}, 1); r.gate <- struct{}{} })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.gate:
	}
	if err := ctx.Err(); err != nil {
		r.release()
		return err
	}
	return nil
}
func (r *finalReduction) release() { r.gate <- struct{}{} }

// CompletedStart binds only an independently completed member receipt, while
// this launch is still owned and running. Failed starts have no such binding.
func (p *Process) CompletedStart(receipt StartReceipt) (CompletedStartBinding, error) {
	if p == nil || p.artifacts == nil {
		return CompletedStartBinding{}, ErrFinalAccountingUnavailable
	}
	select {
	case <-p.done:
		return CompletedStartBinding{}, ErrFinalAccountingUnavailable
	default:
	}
	return completedStartBinding(p.artifacts.binding, receipt)
}
func (p *Process) FinalAccounting(ctx context.Context, binding CompletedStartBinding, prior TransportHighWater) (FinalAccountingResult, error) {
	result, err := p.reduceFinalAccounting(ctx, binding, prior, false)
	return result.FinalAccountingResult, err
}

// BindRouteWireWindow attaches the independently observed census while the
// FullNode launch is still owned. The window may be bound once per launch.
func (p *Process) BindRouteWireWindow(ctx context.Context, binding CompletedStartBinding, expected RouteWireExpectation) error {
	if p == nil || p.finalRole != MemberFull || expected.Window == (LaunchNonce{}) {
		return ErrFinalAccountingUnavailable
	}
	commitment, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
	if err != nil {
		return err
	}
	return p.bindRouteWireCommitment(ctx, binding, commitment)
}

func (p *Process) bindRouteWireCommitment(ctx context.Context, binding CompletedStartBinding, commitment RouteWireCommitment) error {
	if p == nil || p.finalRole != MemberFull || !commitment.valid() {
		return ErrFinalAccountingUnavailable
	}
	if err := p.reduction.acquire(ctx); err != nil {
		return err
	}
	defer p.reduction.release()
	select {
	case <-p.done:
		return ErrFinalAccountingUnavailable
	default:
	}
	if p.artifacts == nil || binding.launch != p.artifacts.binding || binding.receipt.Incarnation.Ordinal == 0 {
		return ErrFinalAccountingUnavailable
	}
	if bound := p.reduction.wireWindow; bound != nil {
		if sameCompletedStart(bound.binding, binding) && bound.commitment == commitment {
			return nil
		}
		return ErrFinalAccountingUnavailable
	}
	p.reduction.wireWindow = &boundRouteWireWindow{binding: binding, commitment: commitment}
	return nil
}

func (p *Process) FinalRouteWireAccounting(ctx context.Context, binding CompletedStartBinding, prior TransportHighWater) (FinalRouteWireResult, error) {
	return p.reduceFinalAccounting(ctx, binding, prior, true)
}

func sameCompletedStart(a, b CompletedStartBinding) bool {
	return a.launch == b.launch && a.receipt.Public == b.receipt.Public &&
		a.receipt.Incarnation.Ordinal == b.receipt.Incarnation.Ordinal &&
		a.receipt.Incarnation.StartedAt.Equal(b.receipt.Incarnation.StartedAt)
}

func (p *Process) reduceFinalAccounting(ctx context.Context, binding CompletedStartBinding, prior TransportHighWater, routeWire bool) (FinalRouteWireResult, error) {
	if p == nil {
		return FinalRouteWireResult{}, ErrFinalAccountingUnavailable
	}
	select {
	case <-p.done:
	default:
		return FinalRouteWireResult{}, ErrFinalAccountingUnavailable
	}
	if err := p.reduction.acquire(ctx); err != nil {
		return FinalRouteWireResult{}, err
	}
	defer p.reduction.release()
	// An incorrect caller cannot poison the cache for the rightful launch owner.
	if p.artifacts == nil || binding.launch != p.artifacts.binding || binding.receipt.Incarnation.Ordinal == 0 {
		return FinalRouteWireResult{}, ErrFinalAccountingUnavailable
	}
	wireWindow := p.reduction.wireWindow
	if routeWire != (wireWindow != nil) || (wireWindow != nil && !sameCompletedStart(wireWindow.binding, binding)) {
		return FinalRouteWireResult{}, ErrFinalAccountingUnavailable
	}
	expected := binding.RuntimeBinding()
	priorValid := !prior.invalid && prior.binding.Identity == expected.Identity && prior.binding.StartedAt.Equal(expected.StartedAt)
	rejected := FinalRouteWireResult{FinalAccountingResult: FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalReportRejected}}
	if wireWindow != nil && !priorValid {
		return rejected, ErrFinalReportInvalid
	}
	if p.reduction.sealed {
		if p.reduction.result.Reason == FinalArtifactsReleased {
			return FinalRouteWireResult{FinalAccountingResult: p.reduction.result}, ErrFinalAccountingUnavailable
		}
		sealed := p.reduction.binding
		if !sameCompletedStart(sealed, binding) || !priorValid {
			return rejected, ErrFinalReportInvalid
		}
		r := p.reduction.result
		if r.Reason == FinalReportRejected || r.Reason == FinalCapabilityCloseFailed || (r.Finality == FinalExact && prior.seen && (r.Traffic.BytesSent < prior.sent || r.Traffic.BytesReceived < prior.received)) {
			return rejected, ErrFinalReportInvalid
		}
		return FinalRouteWireResult{FinalAccountingResult: r, Endpoints: slices.Clone(p.reduction.wireEndpoints), Seal: p.reduction.wireSeal}, nil
	}
	result := FinalRouteWireResult{FinalAccountingResult: FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalExitUnclean}}
	var validationErr error
	if !priorValid {
		validationErr = ErrFinalReportInvalid
	}
	if p.exit.Clean() && priorValid {
		var raw []byte
		var err error
		if wireWindow != nil {
			raw, err = p.artifacts.readBounded(ctx, routeWireReportLimit(wireWindow.commitment.Count))
		} else {
			raw, err = p.artifacts.read(ctx)
		}
		if err == nil {
			if !p.rpc.credentials.valid() {
				err = ErrInvalidConfiguration
			} else {
				for _, pattern := range secretPatterns([][]byte{p.rpc.credentials.password.raw, []byte(p.rpc.credentials.passwordText())}) {
					if bytes.Contains(raw, pattern) {
						err = ErrSecretLeak
						break
					}
				}
			}
		}
		if err == nil {
			if wireWindow != nil {
				var wire HeadlessRouteWireAccounting
				wire, err = decodeCommittedHeadlessRouteWireReport(raw, binding, prior, wireWindow.commitment)
				result.Traffic, result.Endpoints = wire.Traffic, wire.Endpoints
				if err == nil {
					result.Seal = RouteWireFinalSeal{Census: wireWindow.commitment, Bytes: len(raw), SHA256: sha256.Sum256(raw)}
				}
			} else if p.finalRole == MemberFull {
				result.Traffic, err = DecodeHeadlessFinalReport(raw, binding, prior)
			} else {
				result.Traffic, err = DecodeFinalReport(raw, binding, prior)
			}
			if err == nil {
				result.Finality = FinalExact
				result.Reason = ""
			}
		}
		if ctx.Err() != nil {
			return FinalRouteWireResult{}, ctx.Err()
		}
		validationErr = err
	}
	if err := ctx.Err(); err != nil {
		return FinalRouteWireResult{}, err
	}
	closeErr := p.artifacts.close()
	if closeErr != nil {
		result = FinalRouteWireResult{FinalAccountingResult: FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalCapabilityCloseFailed}}
	}
	if validationErr != nil && closeErr == nil {
		result.Reason = FinalReportRejected
	}
	p.reduction.binding = binding
	p.reduction.result = result.FinalAccountingResult
	p.reduction.wireEndpoints = slices.Clone(result.Endpoints)
	p.reduction.wireSeal = result.Seal
	p.reduction.sealed = true
	return result, errors.Join(validationErr, closeErr)
}

// ReleaseArtifacts is explicit confirmed-exit cleanup for failed starts or raw
// process fixtures. It cannot close a capability still used by an owned child.
func (p *Process) ReleaseArtifacts(ctx context.Context) error {
	if p == nil {
		return nil
	}
	select {
	case <-p.done:
	default:
		return ErrStopUnconfirmed
	}
	if err := p.reduction.acquire(ctx); err != nil {
		return err
	}
	defer p.reduction.release()
	if p.reduction.sealed {
		if p.reduction.result.Reason == FinalCapabilityCloseFailed {
			return ErrParentProcessClose
		}
		return nil
	}
	err := p.artifacts.close()
	reason := FinalArtifactsReleased
	if err != nil {
		reason = FinalCapabilityCloseFailed
	}
	p.reduction.result = FinalAccountingResult{Finality: FinalUnavailable, Reason: reason}
	p.reduction.sealed = true
	return err
}
