//go:build linux

package loadstand

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"

	"github.com/piratecash/corsa/internal/core/domain"
	"golang.org/x/sys/unix"
)

type linuxMemberMembershipDescriptor struct {
	SHA256 string
	Size   int64
	Count  int
}
type linuxMemberMembershipRow struct {
	ID       MemberID
	Endpoint domain.PeerAddress
}
type linuxMemberMembership struct {
	file       *os.File
	descriptor linuxMemberMembershipDescriptor
}
type linuxMemberMembershipView struct {
	descriptor linuxMemberMembershipDescriptor
	endpoints  map[domain.PeerAddress]bool
}

func newLinuxMemberMembership(ctx context.Context, plan plannedNetworkPlan) (_ *linuxMemberMembership, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([]linuxMemberMembershipRow, len(plan.members))
	for i, member := range plan.members {
		rows[i] = linuxMemberMembershipRow{ID: member.id, Endpoint: member.endpoint}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	fd, err := unix.MemfdCreate("original-loadstand-membership", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "original-loadstand-membership")
	keep := false
	defer func() {
		if !keep {
			result = errors.Join(result, file.Close())
		}
	}()
	if count, err := file.Write(raw); err != nil || count != len(raw) {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	if _, err = unix.FcntlInt(file.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	keep = true
	return &linuxMemberMembership{file: file, descriptor: linuxMemberMembershipDescriptor{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(raw)), Count: len(rows)}}, nil
}
func (m *linuxMemberMembership) close() error {
	if m == nil || m.file == nil {
		return nil
	}
	file := m.file
	m.file = nil
	return file.Close()
}
func decodeLinuxMemberMembership(ctx context.Context, reader io.Reader, expected linuxMemberMembershipDescriptor) (*linuxMemberMembershipView, error) {
	if expected.Count < 1 || expected.Size < 1 || expected.Size == 1<<63-1 {
		return nil, ErrInvalidConfiguration
	}
	digest := sha256.New()
	bounded := &io.LimitedReader{R: reader, N: expected.Size + 1}
	decoder := json.NewDecoder(io.TeeReader(bounded, digest))
	decoder.DisallowUnknownFields()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('[') {
		return nil, ErrInvalidSample
	}
	view := &linuxMemberMembershipView{descriptor: expected, endpoints: make(map[domain.PeerAddress]bool, expected.Count)}
	ids := make(map[MemberID]bool, expected.Count)
	for decoder.More() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var row linuxMemberMembershipRow
		if decoder.Decode(&row) != nil || !plannedLiteralName(string(row.ID)) || ids[row.ID] || view.endpoints[row.Endpoint] || len(ids) >= expected.Count {
			return nil, ErrInvalidSample
		}
		address, parseErr := netip.ParseAddrPort(string(row.Endpoint))
		if parseErr != nil || !plannedApprovedAddress(address.Addr()) || address.Port() == 0 || domain.PeerAddress(address.String()) != row.Endpoint {
			return nil, ErrInvalidSample
		}
		ids[row.ID] = true
		view.endpoints[row.Endpoint] = true
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') || len(ids) != expected.Count {
		return nil, ErrInvalidSample
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, ErrInvalidSample
	}
	if bounded.N != 1 || hex.EncodeToString(digest.Sum(nil)) != expected.SHA256 {
		return nil, ErrInvalidSample
	}
	return view, ctx.Err()
}
func readLinuxMemberMembership(ctx context.Context, file *os.File, expected linuxMemberMembershipDescriptor) (*linuxMemberMembershipView, error) {
	if file == nil {
		return nil, ErrLinuxOwnerProtocol
	}
	var info unix.Stat_t
	if unix.Fstat(int(file.Fd()), &info) != nil || info.Mode&unix.S_IFMT != unix.S_IFREG || info.Uid != 0 || info.Size != expected.Size {
		return nil, ErrLinuxOwnerProtocol
	}
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	required := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if err != nil || seals&required != required {
		return nil, ErrLinuxOwnerProtocol
	}
	return decodeLinuxMemberMembership(ctx, io.NewSectionReader(file, 0, expected.Size), expected)
}
