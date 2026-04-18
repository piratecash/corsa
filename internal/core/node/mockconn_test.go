package node

import (
	"bytes"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/testutil/netmocks"
)

// connBuffer is a thread-safe write buffer used alongside netmocks.MockConn.
type connBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (cb *connBuffer) Written() []byte {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.buf.Bytes()
}

// newBufferedMockConn creates a MockConn that captures all Write calls into a
// shared connBuffer. The caller inspects cb.Written() instead of accessing
// the connection directly.
func newBufferedMockConn(t *testing.T) (*netmocks.MockConn, *connBuffer) {
	t.Helper()
	cb := &connBuffer{}
	m := netmocks.NewMockConn(t)
	m.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		return cb.buf.Write(b)
	}).Maybe()
	m.On("Read", mock.Anything).Return(0, nil).Maybe()
	m.On("Close").Return(nil).Maybe()
	m.On("LocalAddr").Return(&net.TCPAddr{}).Maybe()
	m.On("RemoteAddr").Return(&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 64646}).Maybe()
	m.On("SetDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetReadDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetWriteDeadline", mock.Anything).Return(nil).Maybe()
	return m, cb
}

// newSimpleMockConn creates a MockConn with default addresses suitable for
// tests that only need a net.Conn without inspecting written data.
func newSimpleMockConn(t *testing.T) *netmocks.MockConn {
	t.Helper()
	return newMockConnWithAddr(t,
		&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 12345},
		&net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 54321},
	)
}

// newMockConnWithAddr creates a MockConn with explicit local and remote
// addresses. Write returns len(b), nil by default.
func newMockConnWithAddr(t *testing.T, local, remote net.Addr) *netmocks.MockConn {
	t.Helper()
	m := netmocks.NewMockConn(t)
	m.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		return len(b), nil
	}).Maybe()
	m.On("Read", mock.Anything).Return(0, nil).Maybe()
	m.On("Close").Return(nil).Maybe()
	m.On("LocalAddr").Return(local).Maybe()
	m.On("RemoteAddr").Return(remote).Maybe()
	m.On("SetDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetReadDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetWriteDeadline", mock.Anything).Return(nil).Maybe()
	return m
}
