package datagram

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// frame_builder.go closes the last thing §8 leaves to every migrating
// protocol: assembling and signing a `routed` datagram.
//
// LocalSendOpts takes a frame "already signed when the mode requires it", so
// without this type each protocol holds the node's Ed25519 key and its network
// id and hand-writes the same nine header fields — Version, Mode, Class, Src,
// TTL, RoutePolicy and the whole auth block. That is not a stylistic
// duplication: the failure mode is silent. A forgotten `MaxTTL = OriginTTL()`
// makes `ttl > max_ttl` and the FIRST relay drops the frame without a word,
// which is invisible to the sender — it sees a perfectly successful `queued`.
//
// So the builder owns exactly the fields whose value is fixed by the layer, and
// the caller supplies only what the protocol really decides: destination, type,
// class, policy and payload. The node's private key stops travelling into every
// adapter as a side effect.
//
// Reference: docs/refactoring/datagram-transport.md §2.1, §3.1, §8.

// ErrFrameBuilderConfig marks a builder that cannot be constructed.
var ErrFrameBuilderConfig = errors.New("datagram: invalid routed frame builder configuration")

// RoutedFrameBuilderConfig wires the builder. An opts struct because four of
// its five fields are mandatory and a forgotten one must be a constructor
// error, not a frame the network drops (CLAUDE.md).
type RoutedFrameBuilderConfig struct {
	// Network is the network id bound into the transcript, so a frame of one
	// network can never verify on another (§3.1).
	Network domain.NetworkID
	// LocalID is this node's identity; it becomes header.src and must be the
	// fingerprint of PrivateKey, which the transcript check enforces.
	LocalID domain.PeerIdentity
	// PrivateKey signs the transcript. It lives HERE and not in every
	// protocol adapter, which is the point of the type.
	PrivateKey ed25519.PrivateKey
	// Clock is the injectable time source behind auth.time, following the
	// package convention.
	Clock func() time.Time
	// Entropy is the source of the per-frame salt. Optional: crypto/rand by
	// default. A test injects a deterministic reader; nothing else should.
	Entropy io.Reader
}

// RoutedFrameBuilder assembles and signs routed datagrams for one node.
//
// Safe for concurrent use: it holds no mutable state, and the two things it
// draws per frame — the salt and the clock reading — are drawn locally.
type RoutedFrameBuilder struct {
	clock   func() time.Time
	entropy io.Reader
	private ed25519.PrivateKey
	public  ed25519.PublicKey
	network domain.NetworkID
	localID domain.PeerIdentity
}

// NewRoutedFrameBuilder builds the builder.
func NewRoutedFrameBuilder(cfg RoutedFrameBuilderConfig) (*RoutedFrameBuilder, error) {
	switch {
	case cfg.Network == "":
		return nil, fmt.Errorf("%w: a network id is required", ErrFrameBuilderConfig)
	case cfg.LocalID.IsZero():
		return nil, fmt.Errorf("%w: a local identity is required", ErrFrameBuilderConfig)
	case len(cfg.PrivateKey) != ed25519.PrivateKeySize:
		return nil, fmt.Errorf("%w: a %d-byte private key is required, got %d",
			ErrFrameBuilderConfig, ed25519.PrivateKeySize, len(cfg.PrivateKey))
	}
	if _, err := domain.ParseNetworkID(cfg.Network.String()); err != nil {
		return nil, err
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	entropy := cfg.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	return &RoutedFrameBuilder{
		clock:   clock,
		entropy: entropy,
		private: cfg.PrivateKey,
		public:  cfg.PrivateKey.Public().(ed25519.PublicKey),
		network: cfg.Network,
		localID: cfg.LocalID,
	}, nil
}

// RoutedFrameOpts is everything the PROTOCOL decides about one frame. Every
// other header field is fixed by the layer and is not offered here — there is
// no way to ask for a wrong `max_ttl` or a wrong `mode`.
type RoutedFrameOpts struct {
	// Dst is the destination identity.
	Dst domain.PeerIdentity
	// DType is the registered type name.
	DType domain.DType
	// Class picks the size cap, the queue lane and the write grace (§2.1).
	Class domain.DatagramClass
	// RoutePolicy is `best` or `explore` (§4.3). The zero value is refused
	// rather than defaulted: "which policy did this retry use" is exactly the
	// question §8 makes the adapter answer, and a silent default would hide
	// it.
	RoutePolicy domain.RoutePolicy
	// Payload is the already-encrypted body, raw bytes. The serializer does
	// the base64url encoding, so handing it a string here is the double
	// encoding of §2.3.
	Payload []byte
}

// Build assembles the frame, signs it and validates the whole structural
// contract before returning it.
//
// The validation is not belt-and-braces: SignDatagram runs Validate on the
// signed frame, so an oversize payload or a mode/class combination the matrix
// forbids is reported to the caller SYNCHRONOUSLY and permanently, instead of
// arriving later as a `failed` outcome it would retry with backoff.
func (b *RoutedFrameBuilder) Build(opts RoutedFrameOpts) (protocol.DatagramFrame, error) {
	if opts.Dst.IsZero() {
		return protocol.DatagramFrame{}, fmt.Errorf("%w: a destination is required", ErrFrameBuilderConfig)
	}
	if opts.RoutePolicy == "" {
		return protocol.DatagramFrame{}, fmt.Errorf("%w: a route policy is required", ErrFrameBuilderConfig)
	}
	salt, err := b.salt()
	if err != nil {
		return protocol.DatagramFrame{}, err
	}

	frame := protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion,
		Mode:    domain.DatagramModeRouted,
		Class:   opts.Class,
		Src:     b.localID,
		Dst:     opts.Dst,
		// The origin does NOT decrement: the first hop receives the full
		// budget, and max_ttl mirrors it so `ttl > max_ttl` cannot be true of
		// a frame this node produced (§4.1 rule 2).
		TTL:         OriginTTL(),
		RoutePolicy: opts.RoutePolicy,
		DType:       opts.DType,
		Payload:     opts.Payload,
		Auth: &protocol.DatagramAuth{
			// The base profile is the only one this build implements, and the
			// wire refuses every other value as an unimplemented version — so
			// it is fixed here rather than offered as an option nobody can
			// legally set.
			AuthVersion: domain.AuthVersionBase,
			PubKey:      b.public,
			Salt:        salt,
			MaxTTL:      OriginTTL(),
			Time:        b.clock().UTC().Unix(),
		},
	}
	signed, err := protocol.SignDatagram(frame, b.network, b.private)
	if err != nil {
		return protocol.DatagramFrame{}, fmt.Errorf("datagram: sign the %s frame: %w", opts.DType, err)
	}
	return signed, nil
}

// salt draws the per-frame salt. A fresh one per frame is what lets the SAME
// ciphertext be resent without hitting the anti-replay cache (§3.1), so it is
// drawn here and never carried over from a previous attempt.
func (b *RoutedFrameBuilder) salt() ([]byte, error) {
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := io.ReadFull(b.entropy, salt); err != nil {
		return nil, fmt.Errorf("datagram: draw the frame salt: %w", err)
	}
	return salt, nil
}
