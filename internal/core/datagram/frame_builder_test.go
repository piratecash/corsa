package datagram

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// frame_builder_test.go pins the two things the builder exists for: the header
// fields a migrating protocol must not be able to get wrong, and the fresh
// salt that lets one ciphertext be resent without hitting anti-replay.

func newTestFrameBuilder(t *testing.T, clock func() time.Time) (*RoutedFrameBuilder, ed25519.PrivateKey, domain.PeerIdentity) {
	t.Helper()
	private, signer := newSigner(t)
	builder, err := NewRoutedFrameBuilder(RoutedFrameBuilderConfig{
		Network:    testNetwork,
		LocalID:    signer,
		PrivateKey: private,
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("NewRoutedFrameBuilder: %v", err)
	}
	return builder, private, signer
}

// TestBuiltFrameCarriesTheLayerFixedHeader is the regression that matters:
// every field the layer — not the protocol — decides is correct by
// construction, and there is no opts field through which a caller could get it
// wrong. A `max_ttl` below `ttl` or the wrong `av` is a SILENT drop at the
// first relay, invisible to a sender that saw a perfectly successful `queued`.
func TestBuiltFrameCarriesTheLayerFixedHeader(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	builder, _, signer := newTestFrameBuilder(t, func() time.Time { return now })
	dst := domaintest.ID("destination")

	frame, err := builder.Build(RoutedFrameOpts{
		Dst:         dst,
		DType:       domain.DType("file_transfer"),
		Class:       domain.DatagramClassBulk,
		RoutePolicy: domain.RoutePolicyExplore,
		Payload:     []byte("sealed-bytes"),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if frame.Version != domain.DatagramHeaderVersion {
		t.Fatalf("version = %d", frame.Version)
	}
	if frame.Mode != domain.DatagramModeRouted {
		t.Fatalf("mode = %s, want routed", frame.Mode)
	}
	if frame.Src != signer {
		t.Fatalf("src = %v, want the local identity %v", frame.Src, signer)
	}
	if frame.TTL != OriginTTL() {
		t.Fatalf("ttl = %d, want the full origin budget %d", frame.TTL, OriginTTL())
	}
	if frame.Auth == nil {
		t.Fatal("a routed frame without auth is refused by every relay")
	}
	if frame.Auth.MaxTTL != OriginTTL() {
		t.Fatalf("max_ttl = %d, want %d — a lower one makes ttl > max_ttl and the FIRST hop drops the frame",
			frame.Auth.MaxTTL, OriginTTL())
	}
	if frame.Auth.AuthVersion != domain.AuthVersionBase {
		t.Fatalf("av = %d, want the base profile %d", frame.Auth.AuthVersion, domain.AuthVersionBase)
	}
	if frame.Auth.Time != now.Unix() {
		t.Fatalf("auth.time = %d, want the injected clock %d", frame.Auth.Time, now.Unix())
	}
	if len(frame.Auth.Salt) != domain.DatagramSaltBytes {
		t.Fatalf("salt = %d bytes, want %d", len(frame.Auth.Salt), domain.DatagramSaltBytes)
	}

	// The signature really verifies against the network the builder was wired
	// with, which is what makes the frame usable at all.
	if err := protocol.VerifyDatagramSignature(frame, testNetwork); err != nil {
		t.Fatalf("VerifyDatagramSignature: %v", err)
	}
	// The payload travels as RAW bytes: the serializer owns the base64url, and
	// pre-encoding here is the double encoding of §2.3.
	if !bytes.Equal(frame.Payload, []byte("sealed-bytes")) {
		t.Fatalf("payload = %q, want the raw sealed bytes", frame.Payload)
	}
}

// A fresh salt per frame is what lets the SAME ciphertext be resent without
// hitting the anti-replay cache — the whole reason §3.1 puts a salt in auth.
func TestBuiltFramesOfOnePayloadCarryDifferentSalts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	builder, _, _ := newTestFrameBuilder(t, func() time.Time { return now })
	opts := RoutedFrameOpts{
		Dst:         domaintest.ID("destination"),
		DType:       domain.DType("file_transfer"),
		Class:       domain.DatagramClassControl,
		RoutePolicy: domain.RoutePolicyBest,
		Payload:     []byte("identical"),
	}

	first, err := builder.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	second, err := builder.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if bytes.Equal(first.Auth.Salt, second.Auth.Salt) {
		t.Fatal("two frames of one payload shared a salt: the resend would be dropped as a replay")
	}
	if replayKeyOf(t, first) == replayKeyOf(t, second) {
		t.Fatal("two frames of one payload produced one replay key")
	}
}

// The builder refuses everything the layer would refuse LATER and reports it
// synchronously, so an adapter learns "this can never work" instead of
// retrying a `failed` outcome with backoff.
func TestBuilderRefusesWhatTheLayerWouldRefuse(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	builder, private, signer := newTestFrameBuilder(t, func() time.Time { return now })

	tests := map[string]RoutedFrameOpts{
		"no destination": {
			DType: "file_transfer", Class: domain.DatagramClassControl,
			RoutePolicy: domain.RoutePolicyBest,
		},
		"no route policy": {
			Dst: domaintest.ID("destination"), DType: "file_transfer",
			Class: domain.DatagramClassControl,
		},
		"payload above the class cap": {
			Dst: domaintest.ID("destination"), DType: "file_transfer",
			Class: domain.DatagramClassBulk, RoutePolicy: domain.RoutePolicyBest,
			Payload: make([]byte, domain.DatagramBulkPayloadCap+1),
		},
		"malformed dtype": {
			Dst: domaintest.ID("destination"), DType: "File Transfer",
			Class: domain.DatagramClassControl, RoutePolicy: domain.RoutePolicyBest,
		},
	}
	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := builder.Build(opts); err == nil {
				t.Fatal("the builder produced a frame the layer refuses")
			}
		})
	}

	// And the constructor refuses an incomplete wiring rather than producing
	// frames nothing can verify.
	for name, cfg := range map[string]RoutedFrameBuilderConfig{
		"no network":  {LocalID: signer, PrivateKey: private},
		"no identity": {Network: testNetwork, PrivateKey: private},
		"no key":      {Network: testNetwork, LocalID: signer},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewRoutedFrameBuilder(cfg); err == nil {
				t.Fatal("an incompletely wired builder was accepted")
			}
		})
	}
}

// TestBuiltFrameGoesThroughTheLayerUnchanged is the end-to-end half: a frame
// this builder produced is accepted by the layer's own send path, which is the
// only claim a migrating protocol actually needs.
func TestBuiltFrameGoesThroughTheLayerUnchanged(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	origin := newPipelineNode(t, net, nodeOpts{id: signer})
	dst := domaintest.ID("far")
	hop := newPipelineNode(t, net, nodeOpts{name: "builder-hop", transit: true})
	link(origin, hop, true, true)
	route(origin, dst, hop.id, 2)

	builder, err := NewRoutedFrameBuilder(RoutedFrameBuilderConfig{
		Network:    testNetwork,
		LocalID:    signer,
		PrivateKey: private,
		Clock:      origin.clock,
	})
	if err != nil {
		t.Fatalf("NewRoutedFrameBuilder: %v", err)
	}
	frame, err := builder.Build(RoutedFrameOpts{
		Dst:         dst,
		DType:       dtypePush,
		Class:       domain.DatagramClassControl,
		RoutePolicy: domain.RoutePolicyBest,
		Payload:     []byte("payload"),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{Frame: frame})
	if outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal = %s (%v)", outcome, outcome.Err())
	}
	journal := net.journal()
	if len(journal) != 1 {
		t.Fatalf("the wire saw %d frames, want 1", len(journal))
	}
	if journal[0].frame.TTL != OriginTTL() {
		t.Fatalf("the first hop saw ttl %d, want the full origin budget %d",
			journal[0].frame.TTL, OriginTTL())
	}
}
