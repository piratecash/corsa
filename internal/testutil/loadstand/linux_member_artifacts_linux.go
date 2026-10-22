//go:build linux

package loadstand

import (
	"context"
	"time"
)

type linuxMemberReceiptsOutcome struct {
	receipts []Receipt
	err      error
}

func (m *linuxSDKMember) Receipts(ctx context.Context, limit int) ([]Receipt, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return nil, err
	}
	if limit < 1 || limit > int(^uint(0)>>1)/1024-1 {
		return nil, ErrInvalidConfiguration
	}
	o := m.fixture
	o.mu.Lock()
	if o.workBusy || o.stopping || o.correlation.poisoned {
		o.mu.Unlock()
		return nil, ErrLinuxOwnerBusy
	}
	id, err := o.correlation.allocate()
	if err != nil {
		o.mu.Unlock()
		return nil, err
	}
	o.workBusy = true
	o.mu.Unlock()
	x := linuxSampleExchange{ctx: ctx, id: id, receiptLimit: limit, receiptsResult: make(chan linuxMemberReceiptsOutcome)}
	select {
	case o.exchanges <- x:
	case <-ctx.Done():
		o.mu.Lock()
		o.workBusy = false
		o.mu.Unlock()
		return nil, ctx.Err()
	case <-o.stoppingDone:
		return nil, ErrLinuxOwnerPoisoned
	}
	select {
	case result := <-x.receiptsResult:
		if result.err != nil {
			return nil, result.err
		}
		if len(result.receipts) > limit {
			return nil, ErrInvalidSample
		}
		for _, receipt := range result.receipts {
			if !receipt.ID.IsValid() || receipt.Recipient != m.current.Public.Identity {
				return nil, ErrInvalidSample
			}
		}
		return result.receipts, nil
	case <-ctx.Done():
		o.poison()
		return nil, ctx.Err()
	case <-o.stoppingDone:
		return nil, ErrLinuxOwnerPoisoned
	}
}
func (o *LinuxFixture) receiptsOnWorkOwner(x linuxSampleExchange) {
	deadline, ok := x.ctx.Deadline()
	var err error
	if !ok {
		err = ErrLinuxOwnerBound
	}
	var raw []byte
	if err == nil {
		raw, err = encodeLinuxPacket(linuxMemberReceipts, x.id, 1, linuxMemberReceiptsRequest{Nonce: o.correlation.nonce, BudgetNanos: int64(time.Until(deadline)), Limit: x.receiptLimit})
	}
	if err == nil {
		err = o.work.write(raw, deadline)
	}
	receipts := make([]Receipt, 0)
	sequence := uint64(1)
	for err == nil {
		packet, readErr := o.work.read(deadline)
		if readErr != nil {
			err = readErr
			break
		}
		reply, decodeErr := decodeLinuxBody[linuxMemberReceiptsReply](packet)
		if decodeErr != nil || packet.kind != linuxMemberReceiptsResult || packet.id != x.id || packet.ordinal != 1 || reply.Nonce != o.correlation.nonce || reply.Code != linuxResultReady || reply.Sequence != sequence || reply.More && len(reply.Receipts) == 0 || len(reply.Receipts) > x.receiptLimit-len(receipts) {
			err = ErrLinuxOwnerProtocol
			break
		}
		receipts = append(receipts, reply.Receipts...)
		if !reply.More {
			break
		}
		sequence++
	}
	if err != nil {
		o.poison()
		receipts = nil
	}
	o.mu.Lock()
	o.workBusy = false
	o.mu.Unlock()
	select {
	case x.receiptsResult <- linuxMemberReceiptsOutcome{receipts: receipts, err: err}:
	case <-x.ctx.Done():
		o.poison()
	case <-o.stoppingDone:
	}
}
