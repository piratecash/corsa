package loadstand

import (
	"bytes"
	"context"
	"errors"
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

// finalReduction has one context-aware I/O owner. No terminal/member mutex is
// held while opening or reading the retained root. Cancellation retains it.
type finalReduction struct {
	once    sync.Once
	gate    chan struct{}
	sealed  bool
	result  FinalAccountingResult
	binding CompletedStartBinding
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
	if p == nil {
		return FinalAccountingResult{}, ErrFinalAccountingUnavailable
	}
	select {
	case <-p.done:
	default:
		return FinalAccountingResult{}, ErrFinalAccountingUnavailable
	}
	if err := p.reduction.acquire(ctx); err != nil {
		return FinalAccountingResult{}, err
	}
	defer p.reduction.release()
	// An incorrect caller cannot poison the cache for the rightful launch owner.
	if p.artifacts == nil || binding.launch != p.artifacts.binding || binding.receipt.Incarnation.Ordinal == 0 {
		return FinalAccountingResult{}, ErrFinalAccountingUnavailable
	}
	expected := binding.RuntimeBinding()
	priorValid := !prior.invalid && prior.binding.Identity == expected.Identity && prior.binding.StartedAt.Equal(expected.StartedAt)
	if p.reduction.sealed {
		if p.reduction.result.Reason == FinalArtifactsReleased {
			return p.reduction.result, ErrFinalAccountingUnavailable
		}
		sealed := p.reduction.binding
		if sealed.launch != binding.launch || sealed.receipt.Public != binding.receipt.Public || sealed.receipt.Incarnation.Ordinal != binding.receipt.Incarnation.Ordinal || !sealed.receipt.Incarnation.StartedAt.Equal(binding.receipt.Incarnation.StartedAt) || !priorValid {
			return FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalReportRejected}, ErrFinalReportInvalid
		}
		r := p.reduction.result
		if r.Reason == FinalReportRejected || r.Reason == FinalCapabilityCloseFailed || (r.Finality == FinalExact && prior.seen && (r.Traffic.BytesSent < prior.sent || r.Traffic.BytesReceived < prior.received)) {
			return FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalReportRejected}, ErrFinalReportInvalid
		}
		return r, nil
	}
	result := FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalExitUnclean}
	var validationErr error
	if !priorValid {
		validationErr = ErrFinalReportInvalid
	}
	if p.exit.Clean() && priorValid {
		raw, err := p.artifacts.read(ctx)
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
			result.Traffic, err = DecodeFinalReport(raw, binding, prior)
			if err == nil {
				result.Finality = FinalExact
				result.Reason = ""
			}
		}
		if ctx.Err() != nil {
			return FinalAccountingResult{}, ctx.Err()
		}
		validationErr = err
	}
	if err := ctx.Err(); err != nil {
		return FinalAccountingResult{}, err
	}
	closeErr := p.artifacts.close()
	if closeErr != nil {
		result = FinalAccountingResult{Finality: FinalUnavailable, Reason: FinalCapabilityCloseFailed}
	}
	if validationErr != nil && closeErr == nil {
		result.Reason = FinalReportRejected
	}
	p.reduction.binding = binding
	p.reduction.result = result
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
