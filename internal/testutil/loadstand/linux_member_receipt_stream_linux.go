//go:build linux

package loadstand

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
)

type linuxMemberReceiptsRequest struct {
	Nonce       LaunchNonce
	BudgetNanos int64
	Limit       int
}
type linuxMemberReceiptsReply struct {
	Sequence uint64
	More     bool
	Nonce    LaunchNonce
	Code     linuxResultCode
	Receipts []Receipt `json:",omitempty"`
}

func (r *linuxHelperRuntime) writeMemberReceipts(ctx context.Context, p linuxPacket, request linuxMemberReceiptsRequest) error {
	sequence := uint64(1)
	write := func(rows []Receipt, more bool, code linuxResultCode) error {
		if rows == nil {
			rows = []Receipt{}
		}
		raw, err := encodeLinuxPacket(linuxMemberReceiptsResult, p.id, 1, linuxMemberReceiptsReply{Nonce: r.init.Nonce, Code: code, Sequence: sequence, More: more, Receipts: rows})
		if err != nil {
			return err
		}
		deadline, _ := ctx.Deadline()
		if err = r.work.write(raw, minLinuxPacketDeadline(deadline)); err != nil {
			return err
		}
		sequence++
		return nil
	}
	unavailable := func() error { return write(nil, false, linuxResultUnavailable) }
	if r.init.Planned == nil || r.init.Planned.Plan.Role != MemberSDK || r.memberBinding == nil || request.Limit < 1 || request.Limit > int(^uint(0)>>1)/1024-1 {
		return unavailable()
	}
	root, file, size, err := openMemberPublicSnapshot(ctx, filepath.Join(r.init.Config.DataDir, "receipts.jsonl"), int64(request.Limit)*1024)
	if err != nil {
		return unavailable()
	}
	closed := false
	closeSnapshot := func() error {
		if closed {
			return nil
		}
		closed = true
		return errors.Join(file.Close(), root.Close())
	}
	defer func() { _ = closeSnapshot() }()
	scanner := bufio.NewScanner(io.NewSectionReader(file, 0, size))
	rows := make([]Receipt, 0)
	seen := 0
	for scanner.Scan() {
		if ctx.Err() != nil || seen >= request.Limit {
			_ = closeSnapshot()
			return unavailable()
		}
		var row Receipt
		if json.Unmarshal(scanner.Bytes(), &row) != nil || !row.ID.IsValid() || row.Recipient != r.memberBinding.Receipt().Public.Identity {
			_ = closeSnapshot()
			return unavailable()
		}
		seen++
		candidate := append(rows, row)
		_, encodeErr := encodeLinuxPacket(linuxMemberReceiptsResult, p.id, 1, linuxMemberReceiptsReply{Nonce: r.init.Nonce, Code: linuxResultReady, Sequence: sequence, More: true, Receipts: candidate})
		if encodeErr != nil {
			if len(rows) == 0 {
				_ = closeSnapshot()
				return unavailable()
			}
			if err = write(rows, true, linuxResultReady); err != nil {
				return err
			}
			rows = []Receipt{row}
			if _, encodeErr = encodeLinuxPacket(linuxMemberReceiptsResult, p.id, 1, linuxMemberReceiptsReply{Nonce: r.init.Nonce, Code: linuxResultReady, Sequence: sequence, More: true, Receipts: rows}); encodeErr != nil {
				_ = closeSnapshot()
				return unavailable()
			}
		} else {
			rows = candidate
		}
	}
	if err = errors.Join(scanner.Err(), ctx.Err(), closeSnapshot()); err != nil {
		return unavailable()
	}
	return write(rows, false, linuxResultReady)
}
