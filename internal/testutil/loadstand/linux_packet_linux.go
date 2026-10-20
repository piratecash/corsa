//go:build linux

package loadstand

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Only the poller-managed UnixConn owns the endpoint after bootstrap. raw pins
// that same net owner for the one nonblocking Stop; it is not another FD alias.
type linuxPacketConn struct {
	conn      *net.UnixConn
	raw       syscall.RawConn
	closeOnce sync.Once
	closeErr  error
}

func (p *linuxPacketConn) close() error {
	p.closeOnce.Do(func() { p.closeErr = p.conn.Close() })
	return p.closeErr
}

func newLinuxPacketPair() (*linuxPacketConn, *os.File, error) {
	ends, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, nil, ErrLinuxOwnerUnsupported
	}
	original := os.NewFile(uintptr(ends[0]), "linux-owner-bootstrap")
	child := os.NewFile(uintptr(ends[1]), "linux-child-bootstrap")
	managed, err := checkedLinuxPacketFile(original)
	if err != nil {
		return nil, nil, errors.Join(err, child.Close())
	}
	return managed, child, nil
}
func checkedLinuxPacketFile(original *os.File) (*linuxPacketConn, error) {
	if original == nil {
		return nil, ErrLinuxOwnerProtocol
	}
	converted, convertErr := net.FileConn(original)
	aliasErr := original.Close() // exactly once, before protocol publication
	if convertErr != nil || aliasErr != nil {
		if converted != nil {
			_ = converted.Close() // partial conversion is never published
		}
		return nil, ErrLinuxOwnerProtocol
	}
	packet, ok := converted.(*net.UnixConn)
	if !ok || packet.LocalAddr().Network() != "unixpacket" {
		return nil, errors.Join(ErrLinuxOwnerProtocol, converted.Close())
	}
	raw, err := packet.SyscallConn()
	if err != nil {
		return nil, errors.Join(ErrLinuxOwnerProtocol, packet.Close())
	}
	valid := false
	err = raw.Control(func(fd uintptr) {
		kind, kindErr := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		flags, flagsErr := unix.FcntlInt(fd, unix.F_GETFL, 0)
		valid = kindErr == nil && flagsErr == nil && kind == unix.SOCK_SEQPACKET && flags&unix.O_NONBLOCK != 0
	})
	if err != nil || !valid {
		return nil, errors.Join(ErrLinuxOwnerProtocol, packet.Close())
	}
	return &linuxPacketConn{conn: packet, raw: raw}, nil
}

func (p *linuxPacketConn) read(deadline time.Time) (linuxPacket, error) {
	if err := p.conn.SetReadDeadline(deadline); err != nil {
		return linuxPacket{}, ErrLinuxOwnerProtocol
	}
	var payload [linuxPacketLimit]byte
	ancillary := make([]byte, unix.CmsgSpace(16*4))
	n, oobn, flags, _, readErr := p.conn.ReadMsgUnix(payload[:], ancillary)
	// Even a truncated, malformed or failed packet can install received rights.
	// Dispose the actual kernel-returned prefix BEFORE every other verdict.
	disposeErr := disposeLinuxAncillary(ancillary[:oobn])
	if disposeErr != nil || oobn != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n > len(payload) {
		return linuxPacket{}, ErrLinuxOwnerProtocol
	}
	if readErr != nil {
		return linuxPacket{}, readErr
	}
	if n == 0 {
		return linuxPacket{}, io.EOF
	}
	return decodeLinuxPacket(payload[:n])
}
func disposeLinuxAncillary(oob []byte) error {
	var defect error
	for len(oob) != 0 {
		if len(oob) < unix.CmsgLen(0) {
			return errors.Join(defect, ErrLinuxOwnerProtocol)
		}
		header, data, remaining, err := unix.ParseOneSocketControlMessage(oob)
		if err != nil {
			return errors.Join(defect, ErrLinuxOwnerProtocol)
		}
		if header.Level != unix.SOL_SOCKET || header.Type != unix.SCM_RIGHTS {
			defect = ErrLinuxOwnerProtocol
		} else {
			if len(data)%4 != 0 {
				defect = ErrLinuxOwnerProtocol
			}
			for len(data) >= 4 {
				fd := int(int32(binary.NativeEndian.Uint32(data[:4])))
				flags, flagErr := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
				closeErr := unix.Close(fd) // never retry a potentially recycled number
				if flagErr != nil || flags&unix.FD_CLOEXEC == 0 || closeErr != nil {
					defect = ErrLinuxOwnerProtocol
				}
				data = data[4:]
			}
		}
		oob = remaining
	}
	return defect
}
func (p *linuxPacketConn) write(packet []byte, deadline time.Time) error {
	if len(packet) > linuxPacketLimit || len(packet) < linuxHeaderSize || deadline.IsZero() {
		return ErrLinuxOwnerBound
	}
	if p.conn.SetWriteDeadline(deadline) != nil {
		return ErrLinuxOwnerProtocol
	}
	n, oobn, err := p.conn.WriteMsgUnix(packet, nil, nil)
	if err != nil || n != len(packet) || oobn != 0 {
		return ErrLinuxOwnerProtocol
	}
	return nil
}
func (p *linuxPacketConn) sendStop(packet []byte) (LinuxStopDelivery, error) {
	delivery := LinuxStopUnknown
	var sendErr error
	err := p.raw.Control(func(fd uintptr) {
		n, err := unix.SendmsgN(int(fd), packet, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
		switch {
		case err == nil && n == len(packet):
			delivery = LinuxStopSent
		case errors.Is(err, unix.EAGAIN), errors.Is(err, unix.EWOULDBLOCK):
			delivery = LinuxStopNoPacket
		default:
			sendErr = ErrLinuxOwnerProtocol
		}
	})
	if err != nil {
		sendErr = ErrLinuxOwnerProtocol
	}
	// A successful local send is not an acknowledgment. EOF is always an
	// independent stop intent, preserving READ for actual terminal observations.
	if err := p.conn.CloseWrite(); err != nil {
		sendErr = ErrLinuxOwnerProtocol
	}
	return delivery, sendErr
}
