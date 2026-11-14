package loadstand

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// HeadlessRouteWireReport is the sealed O-1a record from a FullNode. The
// ordinary headless shutdown report stays independently verifiable inside it.
// Window and the expected endpoints come from the controller's census.
type HeadlessRouteWireReport struct {
	Version   uint8                 `json:"version"`
	Window    LaunchNonce           `json:"window_nonce"`
	Bounds    RouteWireWindowBounds `json:"boot_window,omitzero"`
	Transport HeadlessFinalReport   `json:"transport"`
	Endpoints []RouteWireEndpoint   `json:"endpoints"`
}

type RouteWireEndpointKey struct {
	Interval  domain.RouteWireSessionInterval `json:"session_interval,omitzero"`
	ConnID    domain.ConnID                   `json:"conn_id"`
	Peer      domain.PeerIdentity             `json:"peer"`
	Direction domain.PeerDirection            `json:"direction"`
	SessionID LaunchFingerprint               `json:"session_id,omitzero"`
}

type RouteWireEndpoint struct {
	RouteWireEndpointKey
	Complete     bool                         `json:"complete"`
	Sent         RouteWireCounts              `json:"sent"`
	Received     RouteWireCounts              `json:"received"`
	SentSpan     domain.RouteWireFrameSpan    `json:"sent_span,omitzero"`
	ReceivedSpan domain.RouteWireFrameSpan    `json:"received_span,omitzero"`
	FullReceive  *domain.RouteWireFullReceipt `json:"full_receive,omitempty"`
	FullSend     *domain.RouteWireFullSend    `json:"full_send,omitempty"`
}

// FramesWithin checks the callback evidence for all selected frame classes.
func (r RouteWireEndpoint) FramesWithin(begin, end time.Duration) bool {
	return routeWireFrameSpanFits(r.Sent, r.SentSpan, begin, end) && routeWireFrameSpanFits(r.Received, r.ReceivedSpan, begin, end)
}

func routeWireFrameSpanFits(counts RouteWireCounts, span domain.RouteWireFrameSpan, begin, end time.Duration) bool {
	if counts == (RouteWireCounts{}) {
		return span == (domain.RouteWireFrameSpan{})
	}
	return span.Within(begin, end)
}

// RouteWireCounts groups the exact plaintext bytes, including the original
// newline, and a digest of their concatenation in observed order.
type RouteWireCounts struct {
	Probe   RouteWireCount `json:"probe"`
	Ack     RouteWireCount `json:"ack"`
	Digest  RouteWireCount `json:"digest"`
	Summary RouteWireCount `json:"summary"`
	FullV3  RouteWireCount `json:"full_v3"`
	DeltaV3 RouteWireCount `json:"delta_v3"`
}

type RouteWireCount struct {
	Frames uint64 `json:"frames"`
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type RouteWireExpectation struct {
	Window    LaunchNonce
	Bounds    RouteWireWindowBounds
	Endpoints []RouteWireEndpointKey
}

// RouteWireWindowBounds uses the Linux CLOCK_BOOTTIME coordinate. The zero
// value selects lifetime diagnostics. A controller must attest the clock
// domain before interpreting bounds across processes.
type RouteWireWindowBounds struct {
	Begin time.Duration `json:"begin_ns"`
	End   time.Duration `json:"end_ns"`
}

func (b RouteWireWindowBounds) valid() bool { return b.Begin > 0 && b.End > b.Begin }

func (b RouteWireWindowBounds) validOrZero() bool {
	return b == (RouteWireWindowBounds{}) || b.valid()
}

// RouteWireCommitment is the fixed-size projection of a controller-owned
// census. It can cross the Linux control channel independently of row count.
type RouteWireCommitment struct {
	Window LaunchNonce           `json:"window_nonce"`
	Bounds RouteWireWindowBounds `json:"boot_window,omitzero"`
	Count  int                   `json:"endpoint_count"`
	SHA256 LaunchFingerprint     `json:"census_sha256"`
}

// RouteWireFinalSeal identifies bytes read from the original launch-owned FD.
// A controller can use it to verify a separately transported report.
type RouteWireFinalSeal struct {
	Census RouteWireCommitment `json:"census"`
	Bytes  int                 `json:"report_bytes"`
	SHA256 LaunchFingerprint   `json:"report_sha256"`
}

func (s RouteWireFinalSeal) matches(raw []byte, expected RouteWireCommitment) bool {
	return s.validFor(expected) && s.Bytes == len(raw) && s.SHA256 == sha256.Sum256(raw)
}

func (s RouteWireFinalSeal) validFor(expected RouteWireCommitment) bool {
	return expected.valid() && s.Census == expected && s.Bytes > 0 &&
		ArtifactReadLimit(s.Bytes) <= routeWireReportLimit(expected.Count) && s.SHA256 != (LaunchFingerprint{})
}

type HeadlessRouteWireAccounting struct {
	Traffic   domain.TransportTrafficStats
	Bounds    RouteWireWindowBounds
	Endpoints []RouteWireEndpoint
}

// Use the census size, never the untrusted row count, to bound decoding.
func routeWireReportLimit(endpoints int) ArtifactReadLimit {
	if endpoints < 0 || uint64(endpoints) > (math.MaxInt64-1-uint64(FinalReportLimit))/4096 {
		return 0
	}
	return FinalReportLimit + ArtifactReadLimit(endpoints)*4096
}

func validateRouteWireCensus(keys []RouteWireEndpointKey, identity domain.PeerIdentity, bounds RouteWireWindowBounds) error {
	bounded := bounds.valid()
	connections := make(map[domain.ConnID]bool, len(keys))
	sessions := make(map[LaunchFingerprint]bool)
	for _, key := range keys {
		if key.ConnID == 0 || key.Peer.IsZero() || key.Peer == identity ||
			(key.Direction != domain.PeerDirectionInbound && key.Direction != domain.PeerDirectionOutbound) || connections[key.ConnID] ||
			bounded != (key.SessionID != (LaunchFingerprint{})) || sessions[key.SessionID] {
			return ErrFinalReportInvalid
		}
		if (bounded && !key.Interval.Within(bounds.Begin, bounds.End)) || (!bounded && key.Interval != (domain.RouteWireSessionInterval{})) {
			return ErrFinalReportInvalid
		}
		connections[key.ConnID] = true
		if bounded {
			sessions[key.SessionID] = true
		}
	}
	return nil
}

func commitRouteWireWindow(expected RouteWireExpectation, identity domain.PeerIdentity) (RouteWireCommitment, error) {
	var out RouteWireCommitment
	if expected.Window == (LaunchNonce{}) || !expected.Bounds.validOrZero() || identity.IsZero() || routeWireReportLimit(len(expected.Endpoints)) == 0 {
		return out, ErrFinalReportInvalid
	}
	if err := validateRouteWireCensus(expected.Endpoints, identity, expected.Bounds); err != nil {
		return out, err
	}
	keys := append(make([]RouteWireEndpointKey, 0, len(expected.Endpoints)), expected.Endpoints...)
	slices.SortFunc(keys, func(a, b RouteWireEndpointKey) int { return cmp.Compare(a.ConnID, b.ConnID) })
	canonical, err := json.Marshal(struct {
		Version   int
		Identity  domain.PeerIdentity
		Window    LaunchNonce
		Endpoints []RouteWireEndpointKey
		Bounds    RouteWireWindowBounds `json:",omitzero"`
	}{1, identity, expected.Window, keys, expected.Bounds})
	if err != nil {
		return out, err
	}
	return RouteWireCommitment{Window: expected.Window, Bounds: expected.Bounds, Count: len(keys), SHA256: sha256.Sum256(canonical)}, nil
}

func (c RouteWireCommitment) valid() bool {
	return c.Window != (LaunchNonce{}) && c.Bounds.validOrZero() && c.SHA256 != (LaunchFingerprint{}) && routeWireReportLimit(c.Count) != 0
}

func (r HeadlessRouteWireReport) Encode() ([]byte, error) {
	if !r.Bounds.validOrZero() {
		return nil, ErrFinalReportInvalid
	}
	if r.Endpoints == nil {
		r.Endpoints = []RouteWireEndpoint{}
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if ArtifactReadLimit(len(raw)) > routeWireReportLimit(len(r.Endpoints)) {
		return nil, ErrArtifactTooLarge
	}
	return raw, nil
}

func DecodeHeadlessRouteWireReport(raw []byte, binding CompletedStartBinding, prior TransportHighWater, expected RouteWireExpectation) (HeadlessRouteWireAccounting, error) {
	commitment, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
	if err != nil {
		return HeadlessRouteWireAccounting{}, err
	}
	return decodeCommittedHeadlessRouteWireReport(raw, binding, prior, commitment)
}

// DecodeSealedHeadlessRouteWireReport verifies both the independently captured
// census and the report bytes identified by the joined helper.
func DecodeSealedHeadlessRouteWireReport(raw []byte, binding CompletedStartBinding, prior TransportHighWater, expected RouteWireExpectation, seal RouteWireFinalSeal) (HeadlessRouteWireAccounting, error) {
	commitment, err := commitRouteWireWindow(expected, binding.RuntimeBinding().Identity)
	if err != nil || !seal.matches(raw, commitment) {
		return HeadlessRouteWireAccounting{}, ErrFinalReportInvalid
	}
	return decodeCommittedHeadlessRouteWireReport(raw, binding, prior, commitment)
}

func decodeCommittedHeadlessRouteWireReport(raw []byte, binding CompletedStartBinding, prior TransportHighWater, commitment RouteWireCommitment) (HeadlessRouteWireAccounting, error) {
	var empty HeadlessRouteWireAccounting
	if len(raw) == 0 || !commitment.valid() || ArtifactReadLimit(len(raw)) > routeWireReportLimit(commitment.Count) {
		return empty, ErrFinalReportInvalid
	}
	fields, err := object(raw, "version", "window_nonce", "transport", "endpoints")
	if err != nil {
		return empty, ErrFinalReportInvalid
	}
	var bounds RouteWireWindowBounds
	if rawBounds, present := fields["boot_window"]; present {
		parts, err := object(rawBounds, "begin_ns", "end_ns")
		if err != nil || len(fields) != 5 || len(parts) != 2 || json.Unmarshal(rawBounds, &bounds) != nil || !bounds.valid() {
			return empty, ErrFinalReportInvalid
		}
	} else if len(fields) != 4 {
		return empty, ErrFinalReportInvalid
	}
	if bounds != commitment.Bounds {
		return empty, ErrFinalReportInvalid
	}
	var envelope struct {
		Version   uint8             `json:"version"`
		Window    LaunchNonce       `json:"window_nonce"`
		Endpoints []json.RawMessage `json:"endpoints"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Version != 1 || envelope.Window != commitment.Window || len(envelope.Endpoints) != commitment.Count {
		return empty, ErrFinalReportInvalid
	}
	traffic, err := DecodeHeadlessFinalReport(fields["transport"], binding, prior)
	if err != nil {
		return empty, err
	}
	rows := make([]RouteWireEndpoint, 0, len(envelope.Endpoints))
	keys := make([]RouteWireEndpointKey, 0, len(envelope.Endpoints))
	sent, received := traffic.BytesSent, traffic.BytesReceived
	required := []string{"conn_id", "peer", "direction", "complete", "sent", "received"}
	if bounds.valid() {
		required = append(required, "session_id", "session_interval")
	}
	for _, data := range envelope.Endpoints {
		fields, err := object(data, required...)
		if err != nil {
			return empty, ErrFinalReportInvalid
		}
		if bounds.valid() {
			interval, err := object(fields["session_interval"], "begin_before_ns", "begin_after_ns", "end_before_ns", "end_after_ns")
			if err != nil || len(interval) != 4 {
				return empty, ErrFinalReportInvalid
			}
		}
		var row RouteWireEndpoint
		if err := json.Unmarshal(data, &row); err != nil || !row.Complete {
			return empty, ErrFinalReportInvalid
		}
		if !routeWireCountsFit(fields["sent"], &sent) || !routeWireCountsFit(fields["received"], &received) {
			return empty, ErrFinalReportInvalid
		}
		fieldCount := len(required)
		if rawReceipt, present := fields["full_receive"]; present {
			parts, err := object(rawReceipt, "destinations", "identity_sha256", "epoch", "snapshot_after_ns")
			if err != nil || len(parts) != 4 || !bounds.valid() || row.Received.FullV3.Frames == 0 ||
				row.FullReceive == nil || !validRouteWireFullReceipt(*row.FullReceive, bounds) {
				return empty, ErrFinalReportInvalid
			}
			fieldCount++
		}
		if rawSend, present := fields["full_send"]; present {
			parts, err := object(rawSend, "destinations", "identity_sha256", "epoch", "passes", "periodic_passes", "triggered_passes", "connect_passes")
			p := row.FullSend
			if err != nil || len(parts) != 7 || !bounds.valid() || p == nil || !p.ValidPasses() || p.Passes > row.Sent.FullV3.Frames ||
				!validRouteWireFullIdentitySet(p.Destinations, p.Epoch, p.IdentitySHA256) {
				return empty, ErrFinalReportInvalid
			}
			fieldCount++
		}
		if bounds.valid() {
			for _, direction := range []struct {
				key    string
				counts RouteWireCounts
			}{{"sent_span", row.Sent}, {"received_span", row.Received}} {
				span, present := fields[direction.key]
				if present != (direction.counts != (RouteWireCounts{})) {
					return empty, ErrFinalReportInvalid
				}
				if present {
					parts, err := object(span, "first_ns", "last_ns")
					if err != nil || len(parts) != 2 {
						return empty, ErrFinalReportInvalid
					}
					fieldCount++
				}
			}
			if !row.FramesWithin(row.Interval.BeginAfter, row.Interval.EndBefore) {
				return empty, ErrFinalReportInvalid
			}
		}
		if len(fields) != fieldCount {
			return empty, ErrFinalReportInvalid
		}
		keys = append(keys, row.RouteWireEndpointKey)
		rows = append(rows, row)
	}
	actual, err := commitRouteWireWindow(RouteWireExpectation{Window: envelope.Window, Bounds: bounds, Endpoints: keys}, binding.RuntimeBinding().Identity)
	if err != nil || actual != commitment {
		return empty, ErrFinalReportInvalid
	}
	return HeadlessRouteWireAccounting{Traffic: traffic, Bounds: bounds, Endpoints: rows}, nil
}

func validRouteWireFullReceipt(receipt domain.RouteWireFullReceipt, bounds RouteWireWindowBounds) bool {
	return receipt.SnapshotAfterNS > bounds.End && validRouteWireFullIdentitySet(receipt.Destinations, receipt.Epoch, receipt.IdentitySHA256)
}

func validRouteWireFullIdentitySet(destinations int, epoch uint64, sha string) bool {
	if destinations <= 0 || destinations > domain.RouteWireFullMaxDestinations || epoch == 0 ||
		len(sha) != 64 || sha != strings.ToLower(sha) {
		return false
	}
	var digest [32]byte
	_, err := hex.Decode(digest[:], []byte(sha))
	return err == nil
}

func routeWireCountsFit(raw json.RawMessage, remaining *uint64) bool {
	fields, err := object(raw, "probe", "ack", "digest", "summary", "full_v3", "delta_v3")
	if err != nil || len(fields) != 6 {
		return false
	}
	for _, data := range fields {
		counters, err := object(data, "frames", "bytes", "sha256")
		if err != nil || len(counters) != 3 {
			return false
		}
		var value RouteWireCount
		if err := json.Unmarshal(data, &value); err != nil || value.Bytes > *remaining {
			return false
		}
		if value.Frames == 0 {
			if value.Bytes != 0 || value.SHA256 != "" {
				return false
			}
			continue
		}
		// (bytes-1)/MaxFrameLine+1 is ceil(bytes/MaxFrameLine), without
		// an overflowing multiplication. A complete frame has at least one byte.
		if value.Bytes < value.Frames || (value.Bytes-1)/protocol.MaxFrameLine+1 > value.Frames ||
			len(value.SHA256) != 64 || strings.ToLower(value.SHA256) != value.SHA256 {
			return false
		}
		var digest [32]byte
		if _, err := hex.Decode(digest[:], []byte(value.SHA256)); err != nil {
			return false
		}
		*remaining -= value.Bytes
	}
	return true
}
