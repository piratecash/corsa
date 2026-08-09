package datagram

import (
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// header.go is the READ-ONLY view of a signed header the layer computes its
// timing from.
//
// It is a value copy with no payload and no mutable aliases: byte fields are
// handed out as copies, so nothing downstream can reach back into the frame
// whose transcript the layer has already built, or is about to build. Payload
// is absent entirely — the timing rules of §2.2 are a function of the HEADER,
// and a type that cannot express payload access cannot be talked into it by a
// later change.
//
// Reference: docs/protocol/datagram.md §2.1, §2.2, §3.3.

// ErrHeaderNotSigned marks an attempt to build the header view from a frame
// that carries no auth block. The view exists only on the routed plane (§2.1),
// so it is constructible only there and every accessor on it is total — no
// "auth may be missing" branch can leak into the timing arithmetic.
var ErrHeaderNotSigned = errors.New("datagram: the header view requires a routed frame with auth")

// Header is the header of a routed datagram as the layer's timing and logging
// see it.
type Header struct {
	version     domain.DatagramVersion
	mode        domain.DatagramMode
	class       domain.DatagramClass
	src         domain.PeerIdentity
	dst         domain.PeerIdentity
	ttl         uint8
	routePolicy domain.RoutePolicy
	dtype       domain.DType
	authVersion domain.AuthVersion
	authTime    time.Time
	maxTTL      uint8
	salt        []byte
	pubKey      []byte
}

// NewHeader builds the view of a routed frame. The frame is validated first:
// the timing arithmetic must never observe a header the wire layer would have
// rejected, or its "total, pure, O(1)" contract would be asked to cope with
// structurally impossible values.
func NewHeader(frame protocol.DatagramFrame) (Header, error) {
	if err := frame.Validate(); err != nil {
		return Header{}, err
	}
	if frame.Mode != domain.DatagramModeRouted || frame.Auth == nil {
		return Header{}, fmt.Errorf("%w: mode %q", ErrHeaderNotSigned, frame.Mode.String())
	}
	return Header{
		version:     frame.Version,
		mode:        frame.Mode,
		class:       frame.Class,
		src:         frame.Src,
		dst:         frame.Dst,
		ttl:         frame.TTL,
		routePolicy: frame.RoutePolicy,
		dtype:       frame.DType,
		authVersion: frame.Auth.AuthVersion,
		authTime:    time.Unix(frame.Auth.Time, 0).UTC(),
		maxTTL:      frame.Auth.MaxTTL,
		salt:        append([]byte(nil), frame.Auth.Salt...),
		pubKey:      append([]byte(nil), frame.Auth.PubKey...),
	}, nil
}

// Version returns the header version `v`.
func (h Header) Version() domain.DatagramVersion { return h.version }

// Mode returns the routing plane. Always DatagramModeRouted for a Header.
func (h Header) Mode() domain.DatagramMode { return h.mode }

// Class returns the traffic class, which selects the queue residence and
// write grace used by the deadline arithmetic (§4.2).
func (h Header) Class() domain.DatagramClass { return h.class }

// Src returns the signer of a routed frame (§2.1.1).
func (h Header) Src() domain.PeerIdentity { return h.src }

// Dst returns the destination address.
func (h Header) Dst() domain.PeerIdentity { return h.dst }

// TTL returns the RAW incoming hop budget, before any clamp or decrement.
func (h Header) TTL() uint8 { return h.ttl }

// RoutePolicy returns the candidate picking strategy.
func (h Header) RoutePolicy() domain.RoutePolicy { return h.routePolicy }

// DType returns the datagram protocol name carried in payload.
func (h Header) DType() domain.DType { return h.dtype }

// AuthVersion returns `auth.av`.
func (h Header) AuthVersion() domain.AuthVersion { return h.authVersion }

// AuthTime returns the SIGNED timestamp. Every window in §2.2 and §3.3 is
// measured from it rather than from the moment of arrival, so a frame delayed
// in transit does not occupy a replay slot longer than a prompt one.
func (h Header) AuthTime() time.Time { return h.authTime }

// MaxTTL returns the signed hop budget the raw ttl is checked against.
func (h Header) MaxTTL() uint8 { return h.maxTTL }

// Salt returns a copy of the 16 random bytes that make each attempt unique.
func (h Header) Salt() []byte { return append([]byte(nil), h.salt...) }

// PubKey returns a copy of the Ed25519 key carried in the frame. It is
// exposed for logging and policy only: the signature is verified by the layer.
func (h Header) PubKey() []byte { return append([]byte(nil), h.pubKey...) }

// enumName renders a metric label for a closed enumeration, without letting
// an out-of-range value reach a log as a bare number.
func enumName[T comparable](names map[T]string, value T) string {
	if name, ok := names[value]; ok {
		return name
	}
	return "invalid"
}
