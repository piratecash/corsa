package protocol

// SessionAuthPayload is the byte payload a v1 (legacy) initiator signs with
// its Ed25519 key in auth_session. It names neither the verifier nor the
// connection, which is why the signature can be relayed and why a v1 session
// proves no identity this node may attribute anything to (see
// docs/protocol/handshake.md and docs/protocol/network_security.md §13).
//
// It lives in protocol rather than with the verifier because it is wire
// format: the v1 verifier (connauth) and the v2 tests that show a v1
// signature cannot be carried into a v2 session both need the exact bytes.
func SessionAuthPayload(challenge, address string) []byte {
	return []byte("corsa-session-auth-v1|" + challenge + "|" + address)
}
