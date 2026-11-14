package sessionv2

import (
	"crypto/sha256"
	"crypto/tls"
	"net"
)

// LoadstandSessionID labels an original TLS connection within one explicitly
// enabled measurement window. Its exporter label is separate from the proof
// and only the digest leaves this function.
func LoadstandSessionID(conn net.Conn, window [32]byte) ([32]byte, bool) {
	if window == ([32]byte{}) {
		return [32]byte{}, false
	}
	session, ok := conn.(*sessionConn)
	if !ok {
		// The node can retain a peekedConn over the original proven transport.
		if wrapper, wrapped := conn.(interface{ NetConn() net.Conn }); wrapped {
			session, ok = wrapper.NetConn().(*sessionConn)
		}
	}
	if !ok || session == nil {
		return [32]byte{}, false
	}
	state := session.ConnectionState()
	if !state.HandshakeComplete || state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != ALPN {
		return [32]byte{}, false
	}
	material, err := state.ExportKeyingMaterial("EXPORTER-corsa-loadstand-route-wire-v1", window[:], sha256.Size)
	if err != nil {
		return [32]byte{}, false
	}
	id := sha256.Sum256(material)
	clear(material)
	return id, id != ([32]byte{})
}
