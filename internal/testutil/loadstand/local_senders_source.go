package loadstand

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
)

// DecodeOptionalLocalSenders applies identical exact-key semantics at both
// process-RPC and original-Service summary boundaries. Omission is optional;
// a case-folded present field must not masquerade as legacy absence.
func DecodeOptionalLocalSenders(fields map[string]json.RawMessage, started, now time.Time) (*datagram.LocalSendersSnapshot, error) {
	if err := canonicalFields(fields, "local_senders"); err != nil {
		return nil, err
	}
	return DecodeLocalSendersSnapshot(fields["local_senders"], started, now)
}

// DecodeLocalSendersSnapshot retains the existing optional diagnostic block.
// Its independently loaded atomics are not a coherent transaction: arithmetic
// equalities belong only to an explicitly controlled, quiescent C2 epoch.
func DecodeLocalSendersSnapshot(raw json.RawMessage, started, now time.Time) (*datagram.LocalSendersSnapshot, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, ErrFamilyUnavailable
	}
	fields, err := object(raw, "by_sender", "started_at", "read_at")
	if err != nil {
		return nil, err
	}
	if len(fields) != 3 {
		return nil, ErrInvalidSample
	}
	start, err := timestamp(fields, "started_at")
	if err != nil {
		return nil, err
	}
	read, err := timestamp(fields, "read_at")
	if err != nil {
		return nil, err
	}
	if !start.Equal(started) || read.Before(start) || read.After(now) {
		return nil, ErrInvalidSample
	}
	rows, err := object(fields["by_sender"])
	if err != nil {
		return nil, err
	}
	names := []string{"unattributed"}
	for _, sender := range datagram.LocalSenders() {
		names = append(names, sender.String())
	}
	if len(rows) != len(names) {
		return nil, ErrInvalidSample
	}
	out := &datagram.LocalSendersSnapshot{BySender: make(map[string]datagram.LocalSendCounts, len(names)), StartedAt: &start, ReadAt: &read}
	for _, name := range names {
		row, err := object(rows[name], "attempts", "queued", "no_route", "failed", "rejected", "gate_refused", "quota_capped", "slot_busy", "expired", "other_rejected")
		if err != nil {
			return nil, err
		}
		if len(row) != 10 {
			return nil, ErrInvalidSample
		}
		// Decode each required uint64: null/missing/negative/fractional/overflow
		// values cannot silently become an observed zero.
		var counts datagram.LocalSendCounts
		if counts.Attempts, err = value[uint64](row, "attempts"); err != nil {
			return nil, err
		}
		if counts.Queued, err = value[uint64](row, "queued"); err != nil {
			return nil, err
		}
		if counts.NoRoute, err = value[uint64](row, "no_route"); err != nil {
			return nil, err
		}
		if counts.Failed, err = value[uint64](row, "failed"); err != nil {
			return nil, err
		}
		if counts.Rejected, err = value[uint64](row, "rejected"); err != nil {
			return nil, err
		}
		if counts.GateRefused, err = value[uint64](row, "gate_refused"); err != nil {
			return nil, err
		}
		if counts.QuotaCapped, err = value[uint64](row, "quota_capped"); err != nil {
			return nil, err
		}
		if counts.SlotBusy, err = value[uint64](row, "slot_busy"); err != nil {
			return nil, err
		}
		if counts.Expired, err = value[uint64](row, "expired"); err != nil {
			return nil, err
		}
		if counts.OtherRejected, err = value[uint64](row, "other_rejected"); err != nil {
			return nil, err
		}
		out.BySender[name] = counts
	}
	return out, nil
}

// CloneLocalSendersSnapshot freezes report ownership, including both stamps.
// Nil remains not-measured for older providers.
func CloneLocalSendersSnapshot(in *datagram.LocalSendersSnapshot) *datagram.LocalSendersSnapshot {
	if in == nil {
		return nil
	}
	out := *in
	if in.StartedAt != nil {
		at := *in.StartedAt
		out.StartedAt = &at
	}
	if in.ReadAt != nil {
		at := *in.ReadAt
		out.ReadAt = &at
	}
	if in.BySender != nil {
		out.BySender = make(map[string]datagram.LocalSendCounts, len(in.BySender))
		for name, row := range in.BySender {
			out.BySender[name] = row
		}
	}
	return &out
}
