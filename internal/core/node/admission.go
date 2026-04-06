package node

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// admission.go centralizes relay frame admission control and documents the
// relay subsystem invariants. The admitRelayFrame function is called from both
// handleJSONCommand (inbound TCP) and handlePeerSessionFrame (peer sessions)
// to enforce uniform capability and frame-size checks.

// Relay invariants — canonical definitions live in docs/protocol/relay.md.
// This file references the canonical IDs so code and docs stay in sync.
// Tests reference invariant IDs in their names (e.g. TestINV4_...).
//
// INV-3  (relay.md): Gossip always runs unconditionally for stored transit DMs.
// INV-4  (relay.md): Client nodes never act as transit relay hops.
// INV-5  (relay.md): Exactly one relay_hop_ack per relay_message with semantic status.
// INV-6  (relay.md): Final recipient stores relay state for receipt reverse path.
// INV-7  (relay.md): Hop-ack status reflects actual delivery outcome.
// INV-9  (relay.md): Relay frames require an authenticated session / capability gate.
// INV-10 (relay.md): Relay is DM-only — non-DM relay frames are dropped.
// INV-11 (relay.md): On the origin node, ReceiptForwardTo is empty.

// relayAdmitResult describes the outcome of a relay frame admission check.
type relayAdmitResult int

const (
	// relayAdmitOK means the frame passed admission and should be processed.
	relayAdmitOK relayAdmitResult = iota

	// relayAdmitRejectCapability means the sender lacks the required capability.
	relayAdmitRejectCapability

	// relayAdmitRejectFrameSize means the frame exceeds the maximum allowed size.
	relayAdmitRejectFrameSize
)

// maxCommandLineBytes is the transport-level limit for inbound client
// commands read by handleConn. Client commands (send_message, relay_message,
// fetch_messages, etc.) are single-object JSON lines. The largest legitimate
// command is relay_message with a 64 KiB sealed DM body (base64 ≈ 87 KiB
// plus JSON overhead). 128 KiB is a safe upper bound.
//
// This limit does NOT apply to peer-session or handshake reads because
// response frames (messages, inbox, fetch_contacts) can contain multiple
// full DM bodies and legitimately exceed 128 KiB.
const maxCommandLineBytes = 128 * 1024

// maxResponseLineBytes is the transport-level limit for frames read from
// peer sessions (readPeerSession) and outbound handshake exchanges
// (connectToPeer, startPeerSession). Response frames such as messages and
// inbox can serialize many DMs into a single JSON line. With DM bodies up
// to 64 KiB each, a batch of ~100 messages could reach several megabytes.
// 8 MiB provides headroom without allowing unbounded allocation.
const maxResponseLineBytes = 8 * 1024 * 1024

// maxPeerCommandBodyBytes is the post-parse body size limit for inbound
// command frames on peer sessions. The transport-level maxResponseLineBytes
// (8 MiB) must be large to accommodate multi-message response frames, but
// that creates an asymmetry: an authenticated peer could send a single
// command with a multi-megabyte body. This limit closes that gap.
//
// Applied to the largest body-carrying field in each command frame type:
//   - push_message:  frame.Item.Body (sealed DM envelope)
//   - relay_message: frame.Body      (checked separately via admitRelayFrame)
//
// 128 KiB matches maxCommandLineBytes — a single command should never need
// more than what the inbound TCP path allows.
const maxPeerCommandBodyBytes = 128 * 1024

// maxAnnouncePeers is the maximum number of peer addresses accepted in a
// single announce_peer frame. A legitimate node announces a handful of
// peers per exchange; larger lists are either misbehaving or malicious.
// Excess entries are silently truncated — the frame is not rejected.
const maxAnnouncePeers = 64

// maxRelayBodyBytes is the maximum allowed length of the Body field in a
// relay_message frame. Applied after parsing as a relay-specific admission
// check on top of the transport-level frame line limits.
//
// DM bodies are X25519+ChaCha20-Poly1305 sealed envelopes. A reasonable
// upper bound for DM text + overhead is 64 KiB.
const maxRelayBodyBytes = 65536

// maxRelayStates is the hard cap on the total number of in-flight
// relayForwardState entries across all peers. Each entry is ~200 bytes;
// 10 000 entries ≈ 2 MiB — safe on any hardware. Under normal load a node
// processes a few hundred relay messages per TTL window (180 s). 10 000
// provides ample headroom while preventing unbounded growth from relay floods.
const maxRelayStates = 10_000

// maxRelayStatesPerPeer limits how many relay forward states any single
// peer (identified by PreviousHop transport address) can contribute.
// A legitimate peer sends a handful of relay messages per window.
// 500 allows spikes without letting one peer fill the global budget.
const maxRelayStatesPerPeer = 500

// maxRelayRetryEntries caps the total number of entries in the relay retry
// map. Each entry is a small (relayAttempt, ~48 bytes) timing record.
// 5 000 entries keeps the queue-{port}.json file under reasonable size.
const maxRelayRetryEntries = 5_000

// maxPendingFramesPerPeer limits the number of queued frames for any single
// peer address. Prevents a single unreachable peer from consuming all
// pending queue memory.
const maxPendingFramesPerPeer = 200

// maxPendingFramesTotal caps the total pending frame count across all peers.
// Prevents unbounded growth when many peers are simultaneously unreachable.
const maxPendingFramesTotal = 2_000

// admitRelayFrame validates a frame against relay admission rules. Called
// from both handleJSONCommand (inbound TCP) and handlePeerSessionFrame
// (peer sessions) to enforce uniform capability and size checks.
//
// hasCapability abstracts the transport-specific capability check:
// connHasCapability for inbound connections, sessionHasCapability for
// peer sessions.
//
// bodyLen is len(frame.Body) — the only field that can realistically
// exceed the size limit. relay_hop_ack frames have no body, so callers
// pass 0 for them.
func admitRelayFrame(hasCapability bool, bodyLen int) relayAdmitResult {
	if !hasCapability {
		return relayAdmitRejectCapability
	}

	if bodyLen > maxRelayBodyBytes {
		return relayAdmitRejectFrameSize
	}

	return relayAdmitOK
}

// isRelayFrame returns true if the frame type is a relay protocol frame
// (relay_message or relay_hop_ack). Used by admission control and frame
// classification.
func isRelayFrame(frameType string) bool {
	return frameType == "relay_message" || frameType == "relay_hop_ack"
}

// peekFrameType extracts the "type" field value from a raw JSON line without
// full parsing. Returns an empty string if the type field is not found or the
// line is not valid JSON. This is used in the inbound read loop to classify
// frames before applying rate limiters — avoiding the cost of json.Unmarshal
// for every inbound frame just to decide which rate bucket to charge.
//
// The scanner looks for the first occurrence of `"type":"<value>"` allowing
// optional whitespace around the colon. This covers all well-formed frames
// produced by Corsa peers. If `"type"` appears inside a string value before
// the actual field, the scanner may return "" — this fails safely toward
// applying rate limiting (not exempting).
func peekFrameType(line string) string {
	const key = `"type"`
	idx := strings.Index(line, key)
	if idx < 0 {
		return ""
	}

	// Skip past `"type"` and any whitespace + colon.
	pos := idx + len(key)
	for pos < len(line) && (line[pos] == ' ' || line[pos] == '\t') {
		pos++
	}
	if pos >= len(line) || line[pos] != ':' {
		return ""
	}
	pos++ // skip ':'
	for pos < len(line) && (line[pos] == ' ' || line[pos] == '\t') {
		pos++
	}
	if pos >= len(line) || line[pos] != '"' {
		return ""
	}
	pos++ // skip opening quote

	// Read until closing quote.
	end := strings.IndexByte(line[pos:], '"')
	if end < 0 {
		return ""
	}
	return line[pos : pos+end]
}

// errFrameTooLarge is returned by readFrameLine when the accumulated line
// exceeds the caller-specified limit before a newline is found.
var errFrameTooLarge = fmt.Errorf("frame line exceeds size limit")

// readFrameLine reads a newline-terminated JSON frame line from reader,
// enforcing limitBytes incrementally during the read. The limit is checked
// before copying each chunk into the result buffer, so a malicious peer
// cannot force allocation beyond limitBytes regardless of how large a line
// it sends.
//
// Callers choose the appropriate limit for their context:
//   - handleConn (inbound commands):    maxCommandLineBytes  (128 KiB)
//   - readPeerSession (peer frames):    maxResponseLineBytes (8 MiB)
//   - connectToPeer / startPeerSession: maxResponseLineBytes (8 MiB)
//
// Implementation: ReadSlice returns a slice of bufio's internal buffer
// without allocating. When the delimiter is not found within the buffer,
// it returns bufio.ErrBufferFull and we loop. We track total bytes seen
// and reject as soon as the running total exceeds limitBytes — before
// copying the oversized chunk.
func readFrameLine(reader *bufio.Reader, limitBytes int) (string, error) {
	var total int
	var parts [][]byte

	for {
		chunk, err := reader.ReadSlice('\n')

		// Check limit BEFORE copying — reject without allocating the
		// oversized chunk into our result buffer.
		total += len(chunk)
		if total > limitBytes {
			return "", errFrameTooLarge
		}

		// ReadSlice returns a view into the internal buffer that will be
		// overwritten on the next read, so we must copy.
		saved := make([]byte, len(chunk))
		copy(saved, chunk)
		parts = append(parts, saved)

		if err == nil {
			// Found the newline delimiter — line is complete.
			return string(bytes.Join(parts, nil)), nil
		}

		if errors.Is(err, bufio.ErrBufferFull) {
			// Delimiter not found within the buffer — keep reading.
			continue
		}

		// Real I/O error (including io.EOF).
		if total > 0 && errors.Is(err, io.EOF) {
			return string(bytes.Join(parts, nil)), err
		}
		return "", err
	}
}

// ---------------------------------------------------------------------------
// Handshake and session timeout constants. Centralized here so every
// transport path uses the same values and they can be tuned in one place.
// ---------------------------------------------------------------------------

// dialTimeout is the TCP connect timeout for outbound peer sessions
// (openPeerSession) and peer sync dials (syncPeer / startPeerSession).
const dialTimeout = 2 * time.Second

// handshakeTimeout is the deadline for the full handshake sequence after
// dial: welcome + auth + subscribe. Applied as conn.SetDeadline and cleared
// once the handshake completes. Used by openPeerSession.
const handshakeTimeout = 2 * time.Second

// syncHandshakeTimeout is the tighter deadline for sync-only connections
// (syncPeer, startPeerSession) which do less work than a full session.
const syncHandshakeTimeout = 1500 * time.Millisecond

// sessionWriteTimeout is the per-frame write deadline for fire-and-forget
// frames on an established peer session (relay_message, relay_hop_ack, and
// peerSessionRequest).
const sessionWriteTimeout = 3 * time.Second

// inboundReadTimeout is the maximum time an inbound connection may remain
// idle (no complete frame received) before the server closes it. This
// prevents Slowloris-style attacks where a peer opens a connection and
// trickles data to hold the connection slot indefinitely.
//
// Legitimate peers send heartbeat pings every 30 seconds. A 120-second
// timeout allows for 4 missed heartbeats before disconnection, which is
// generous even on high-latency links.
const inboundReadTimeout = 120 * time.Second

// NOTE: relay-specific frame validation (ID, recipient, topic checks) is
// handled inside handleRelayMessage itself. A separate validateRelayMessage
// function was considered but deferred — handleRelayMessage already performs
// robust validation with appropriate state cleanup on each rejection path.
// When future iterations add rate limiting or overload hooks, a pre-check
// function can be introduced here without changing the processing pipeline.
