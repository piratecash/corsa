package sessionv2

import "crypto/tls"

// config.go builds the TLS parameters of the v2 session
// (docs/protocol/session_v2.md, "TLS parameters"). A config is built per
// handshake and never stored in a field: a stored tls.Config is one print
// away from showing whatever certificate someone later puts into it.

// ALPN is the application protocol both ends require, checked explicitly on
// both sides: a TLS stack that skips negotiation must not pass for v2.
const ALPN = "corsa/2"

func listenerConfig(certificates CertificateSource) *tls.Config {
	return &tls.Config{
		MinVersion:                  tls.VersionTLS13,
		MaxVersion:                  tls.VersionTLS13,
		NextProtos:                  []string{ALPN},
		GetCertificate:              certificates.GetCertificate,
		ClientAuth:                  tls.NoClientCert,
		SessionTicketsDisabled:      true,
		DynamicRecordSizingDisabled: true,
	}
}

func dialerConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		NextProtos: []string{ALPN},
		// The certificate proves nothing (certificate.go): identity is
		// proven by session_proof over this session's exporter. Skipping
		// verification is therefore the design, and it is allowed in this
		// package only (a guard test pins that).
		InsecureSkipVerify:          true, //nolint:gosec // identity is proven by session_proof, not by the certificate
		ClientSessionCache:          nil,
		SessionTicketsDisabled:      true,
		DynamicRecordSizingDisabled: true,
	}
}
