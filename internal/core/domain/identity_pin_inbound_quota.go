package domain

import "time"

// IdentityPinInboundQuotaReason describes the inactive N0 storage/admission core.
// It is not a declaration that a production inbound producer is installed.
type IdentityPinInboundQuotaReason string

const (
	InboundQuotaNotInstalled     IdentityPinInboundQuotaReason = "journal_not_installed"
	InboundQuotaUnavailable      IdentityPinInboundQuotaReason = "store_unavailable"
	InboundQuotaReady            IdentityPinInboundQuotaReason = "ready"
	InboundQuotaHistoryUnknown   IdentityPinInboundQuotaReason = "history_unknown"
	InboundQuotaUnconfirmed      IdentityPinInboundQuotaReason = "journal_unconfirmed"
	InboundQuotaClockUnavailable IdentityPinInboundQuotaReason = "clock_unavailable"
	InboundQuotaClockFault       IdentityPinInboundQuotaReason = "clock_fault"
	InboundQuotaExhausted        IdentityPinInboundQuotaReason = "counter_exhausted"
	InboundQuotaGroupFull        IdentityPinInboundQuotaReason = "source_group_full"
	InboundQuotaHistoryFull      IdentityPinInboundQuotaReason = "group_history_full"
	InboundQuotaClassFull        IdentityPinInboundQuotaReason = "inbound_class_full"
	InboundQuotaInvalidGroup     IdentityPinInboundQuotaReason = "source_group_invalid"
)

type IdentityPinInboundQuotaStatus struct {
	Reason                      IdentityPinInboundQuotaReason
	ProducerInstalled           bool
	RemainingObserved           *time.Duration
	Groups, Events, Allocations int
	ConfirmedAllocations        int
	OverCapacity                bool
}

func (r IdentityPinInboundQuotaReason) Error() string { return string(r) }
