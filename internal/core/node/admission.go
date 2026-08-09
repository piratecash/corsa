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
// dispatchNetworkFrame (inbound TCP) and dispatchPeerSessionFrame (peer sessions)
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
// 5 000 entries bounds the in-memory map at a few hundred KB.
const maxRelayRetryEntries = 5_000

// maxPendingFramesPerPeer is the DEFAULT per-peer pending ring capacity —
// the bound on frames queued for any single peer address, preventing one
// unreachable peer from consuming all pending memory. It is the fallback
// when CORSA_PENDING_RING_SIZE / config.Node.PendingRingSize is unset; the
// queuePeerFrame ring evicts the oldest frame at this cap rather than
// rejecting new ones (see queuePeerFrame).
const maxPendingFramesPerPeer = 200

// maxPendingFramesTotal caps the total pending frame count across all peers.
// Prevents unbounded growth when many peers are simultaneously unreachable.
const maxPendingFramesTotal = 2_000

// admitRelayFrame validates a frame against relay admission rules. Called
// from both dispatchNetworkFrame (inbound TCP) and dispatchPeerSessionFrame
// (peer sessions) to enforce uniform capability and size checks.
//
// hasCapability abstracts the transport-specific capability check:
// connHasCapability for inbound connections, sessionHasCapability for
// peer sessions. BOTH are keyed on the connection the frame arrived on —
// the inbound one by ConnID, the outbound one by the session object the
// read loop owns — because a capability set belongs to a connection and a
// reconnect can put a different set under the same peer address.
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

// peekFrameType extracts the FIRST `"type":"<value>"` a raw JSON line contains,
// wherever it sits, and is DIAGNOSTIC ONLY: a log field or the best-effort
// attribution of a line that already lost its right to be dispatched. It must
// never decide anything — not a route, not a budget, not an exemption.
//
// The rule is not stylistic. The first `"type"` in a line and the one
// encoding/json binds to Frame.Type are two different answers, and a sender
// picks which reader gets which: a NESTED `{"a":{"type":"file_command"}}` or a
// duplicated top-level key names one type to this scan and another to the
// parser. Deciding on that answer is how an exemption meant for one frame type
// was granted to another — see frameLineExemptFromCommandLimit for the bypass
// that cost, and classifyFrameLine for the full argument and for the classifier
// every decision on a raw line takes instead.
//
// The remaining callers all label something already refused
// (oversizeRefusalAttribution, dropAmbiguousFrameLine,
// countAmbiguousDatagramRefusal): the drop happened on the classification, and
// the peek only says which plane's counter or log line it lands on. Being wrong
// there costs an attribution, never an admission.
//
// The scanner allows optional whitespace around the colon. If `"type"` appears
// inside a string value before the actual field, it may return "" — which as an
// attribution means "nothing claimed", the honest answer for such a line.
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

// frameLineClass is what a pre-parse scan is ALLOWED to conclude about a line,
// and the three answers are not degrees of one another. Two of them mean the
// scan and encoding/json return the same verdict; the third means they can
// differ, and a line on which they can differ cannot be routed before it is
// parsed — which is why it is the one class §4.1 step 1 refuses outright.
type frameLineClass uint8

const (
	// frameLineNamed: the line names its type ONCE, at the top level, spelled
	// literally, with a plain string value. protocol.ParseFrameLine will read
	// the same key and hand back the same value, so this classification may
	// decide where the line goes.
	frameLineNamed frameLineClass = iota

	// frameLineUnnamed: nothing at the top level can name the frame. Either the
	// object carries no key encoding/json would bind to Frame.Type, or the line
	// is structurally broken and the parser will refuse it. Both ways the parser
	// cannot answer `datagram` — or any other dispatchable type — so the line
	// keeps the ordinary path and takes whatever verdict the parser gives it.
	frameLineUnnamed

	// frameLineAmbiguous: a top-level key encoding/json WILL bind to Frame.Type
	// exists, and this scan cannot say which value the parser will take from it.
	// A duplicate key, an escaped key or value, a case-variant spelling, a
	// non-string value: in every one of them the scan and the parser can name
	// two different types for one line, and the sender picks which reader gets
	// which answer.
	frameLineAmbiguous
)

// classifyFrameLine reads the TOP-LEVEL `type` of a raw JSON frame line and
// says how far that reading may be trusted.
//
// # Why peekFrameType is not enough
//
// peekFrameType takes the FIRST `"type"` it finds anywhere in the line, and
// encoding/json takes the LAST top-level one. Those are two different answers,
// and a sender chooses which one each reader gets:
//
//   - `{"type":"messages", …, "type":"datagram"}` — duplicate top-level keys.
//     The scan says `messages`, the parser says `datagram`. On the peer-session
//     reader that is the difference between the 8 MiB response budget and the
//     strict 128 KiB one, so the line is decoded in full before anything looks
//     at the real type — exactly the work §4.1 step 1 exists to prevent;
//   - `{"a":{"type":"messages"},"type":"datagram"}` — a NESTED key seen first.
//     Same divergence, without a duplicate to notice;
//   - `{"TYPE":"datagram"}` — encoding/json prefers an exact tag match but
//     ACCEPTS a case-insensitive one, so the parser reads a datagram off a key
//     a byte comparison calls unrelated. That is the same blind spot spelled
//     differently, and it is why the candidate test here is case-insensitive
//     while the ACCEPTANCE test is not.
//
// So the question this answers is not "what does the line look like" but "can
// this scan and the parser be made to disagree", and everything on which they
// can is frameLineAmbiguous:
//
//   - more than one top-level `type` candidate;
//   - a candidate key or a `type` value carrying a backslash escape. `"type"`
//     decodes to `type` for encoding/json, and comparing raw bytes would miss
//     it; refusing every escaped top-level key costs nothing, because no frame
//     this node or its peers marshal has one;
//   - a candidate key that is not literally `type` but which encoding/json
//     still binds to the field;
//   - a `type` whose value is not a plain string. `{"type":null,"type":"datagram"}`
//     is the shape that makes this necessary rather than tidy: JSON `null`
//     leaves a string field untouched, so the parser reads `datagram` from a
//     line whose first candidate named nothing at all.
//
// Everything that is neither of those — no candidate, a broken object, a
// truncated string — is frameLineUnnamed, because encoding/json cannot get a
// dispatchable type out of it either.
//
// The scan allocates nothing and walks the line once. It is not a validator:
// it reads only as much structure as the question needs, and
// protocol.ParseFrameLine remains the authority on everything else.
func classifyFrameLine(line string) (string, frameLineClass) {
	pos := skipJSONSpace(line, 0)
	if pos >= len(line) || line[pos] != '{' {
		return "", frameLineUnnamed
	}
	pos++

	var (
		frameType string
		found     bool
		depth     = 1
	)
	for pos < len(line) && depth > 0 {
		switch line[pos] {
		case '"':
			key, escaped, next, ok := scanJSONString(line, pos)
			if !ok {
				return "", frameLineUnnamed
			}
			colon := skipJSONSpace(line, next)
			if colon >= len(line) || line[colon] != ':' {
				// A string in value position; nothing here names a key.
				pos = next
				continue
			}
			// An escaped key stays a candidate on purpose: this scan cannot
			// tell whether it spells `type`, and that ambiguity is exactly
			// what must disqualify the line a few lines below. Skipping it
			// here would hand the wide budget back to the decoy.
			if depth != 1 || (!escaped && !bindsToFrameTypeField(key)) {
				pos = colon + 1
				continue
			}
			if found || escaped || key != frameTypeKey {
				// A second candidate, one this scan cannot read, or one the
				// parser binds while this scan would not: the line does not
				// name its type unambiguously.
				return "", frameLineAmbiguous
			}
			value, valueEscaped, after, ok := scanJSONStringValue(line, colon+1)
			if !ok || valueEscaped {
				return "", frameLineAmbiguous
			}
			frameType, found, pos = value, true, after
		case '{', '[':
			depth++
			pos++
		case '}', ']':
			depth--
			pos++
		default:
			pos++
		}
	}
	if depth != 0 || !found {
		return "", frameLineUnnamed
	}
	return frameType, frameLineNamed
}

// topLevelFrameType is classifyFrameLine for the callers that only need to know
// whether the line named itself, and is kept as its own name because "did this
// line name its type" reads at the call site while a class comparison does not.
func topLevelFrameType(line string) (string, bool) {
	frameType, class := classifyFrameLine(line)
	return frameType, class == frameLineNamed
}

// bindsToFrameTypeField reports whether encoding/json would bind a top-level key
// to protocol.Frame.Type. It is CASE-INSENSITIVE because the decoder is: it
// prefers an exact tag match and falls back to a case-insensitive one, so
// `{"TYPE":"datagram"}` arrives at the dispatcher as a datagram. The scan cannot
// reproduce the decoder's preference order across duplicates, so every key the
// decoder might bind is treated as a candidate and anything but the literal
// spelling is refused above.
func bindsToFrameTypeField(key string) bool {
	return strings.EqualFold(key, frameTypeKey)
}

// frameTypeKey is the top-level key every frame names itself with.
const frameTypeKey = "type"

// skipJSONSpace advances past the whitespace JSON allows between tokens.
func skipJSONSpace(line string, pos int) int {
	for pos < len(line) {
		switch line[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
		default:
			return pos
		}
	}
	return pos
}

// scanJSONString reads one string token starting at the opening quote. It
// returns the RAW content, whether that content carried a backslash escape,
// and the index just past the closing quote.
//
// The content is deliberately left undecoded: the only caller compares it
// against a fixed ASCII name, and a caller that sees `escaped` refuses rather
// than guessing what the escape meant.
func scanJSONString(line string, pos int) (string, bool, int, bool) {
	if pos >= len(line) || line[pos] != '"' {
		return "", false, pos, false
	}
	escaped := false
	for i := pos + 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			escaped = true
			i++
		case '"':
			return line[pos+1 : i], escaped, i + 1, true
		}
	}
	return "", false, pos, false
}

// scanJSONStringValue reads the string value that follows a colon, skipping
// the whitespace between them. A non-string value reports false: a `type` that
// is not a string names nothing.
func scanJSONStringValue(line string, pos int) (string, bool, int, bool) {
	return scanJSONString(line, skipJSONSpace(line, pos))
}

// wireLineBudget is the size of one frame line as BOTH ends count it: the
// bytes of the line plus the terminating newline when the caller no longer
// carries one.
//
// It lives here, beside readFrameLine and peekFrameType, because it is the
// unit every line-size gate on this node is expressed in — the strict 128 KiB
// budget of the announce plane and of the datagram plane alike — and because
// the receive paths hand a line over in different shapes: a reader passes the
// line as read, newline included, while a caller working from an already
// stripped line passes it without. Without one helper the two would disagree
// with each other, and with the sender's MarshalFrameLineWithLimit budget, by
// exactly one byte at the boundary — the one place where the answer changes.
func wireLineBudget(line string) int {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		return len(line)
	}
	return len(line) + 1
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
	read, err := readFrameLineStaged(reader, limitBytes, nil)
	return read.line, err
}

// frameLineExtension is asked, ONCE per line and at the exact moment the soft
// limit is first crossed, how many further bytes the sender has earned for
// THIS line. It returns that allowance in bytes; zero or less refuses the
// line, and the reader stops without ever reading or copying the remainder.
//
// It takes the line PREFIX read so far — bounded by frameLinePrefixBytes, so
// the decision never grows with the line — because the only question a
// pre-read gate can answer is "what does this line claim to be", and the claim
// is at the front: protocol.Frame declares Type first and encoding/json emits
// struct fields in declaration order, so every frame this node's peers marshal
// names itself in its first few dozen bytes.
//
// The claim is NOT trusted as a classification. It only buys the right to keep
// reading; the authoritative verdict is taken from the complete line by
// admitFrameLinePreParse, which sees the duplicate keys and escapes a prefix
// cannot.
type frameLineExtension func(prefix string) int

// frameLinePrefixBytes is how much of a line's head the staged reader keeps
// for frameLineExtension.
//
// It is a fixed 1 KiB rather than "the first buffer fill" because the fill
// boundary is the SENDER's choice: a peer that trickles its line one TCP
// segment at a time would hand the gate ten bytes and have every legitimate
// wide reply refused. Accumulating a fixed prefix makes the gate see the same
// bytes whatever the segmentation, and 1 KiB is two orders of magnitude more
// than the `{"type":"push_message"` a decision needs.
const frameLinePrefixBytes = 1024

// frameLineRead is one staged read: what it produced AND what it cost.
//
// The cost is a field rather than something a caller works out afterwards
// because only the reader knows it. The read stops on a BUFFER FILL and not on
// the limit, so a refused line has consumed the limit plus however much of the
// crossing chunk the fill happened to carry — a number no caller can
// reconstruct from the constants it passed in. Every caller that charges a
// neighbour for the bytes it made this node read charges this field.
type frameLineRead struct {
	// line is the complete line, newline included, and is empty for a refusal.
	line string

	// consumed is how many bytes this call took out of the reader, refusal
	// included. It equals len(line) whenever a line was produced.
	consumed int

	// delimited reports whether the newline was CONSUMED. It exists for the
	// refusal path and is load-bearing there: a refused line leaves its
	// remainder in the stream for the caller to discard, but the chunk that
	// tripped the limit may already have carried the delimiter — and a caller
	// that discarded unconditionally would then eat the NEXT frame, turning one
	// oversize line into a silent hole in the peer's traffic.
	delimited bool
}

// readFrameLineStaged reads a newline-terminated JSON frame line, enforcing
// softLimit incrementally during the read, and — when extend is non-nil —
// asking it at the soft limit how much further the line may grow.
//
// With extend == nil it is exactly readFrameLine: softLimit is the only limit.
//
// # Why the extension is a callback and not a second limit
//
// The gate has to run BETWEEN the two reads, because "admission before any
// decoding" (§4.1 step 1) is worth nothing if the node has already read and
// copied eight megabytes to find out what the line claimed to be. A caller
// cannot pick the right limit up front — the answer depends on the claim,
// which is inside the bytes it has not read yet — so the decision is handed
// back to the caller at the one point where it is both informed and still
// cheap.
func readFrameLineStaged(reader *bufio.Reader, softLimit int, extend frameLineExtension) (frameLineRead, error) {
	limit := softLimit
	// A nil extension is "no staging": the soft limit is final, and the gate
	// must never fire — hence extended starts true and is never consulted.
	extended := extend == nil

	// Fast path: the overwhelming majority of frames fit inside bufio's
	// buffer, so the very first ReadSlice returns the whole line. In that
	// case a single string() conversion is the only allocation needed — the
	// previous unconditional saved-copy + bytes.Join + string sequence cost
	// three copies of every line on this hottest of receive paths. Nothing
	// here touches the prefix buffer: a line that fits one fill never reaches
	// the gate, and paying for its prefix would tax every frame on the node
	// for a decision only oversize lines ask for.
	chunk, err := reader.ReadSlice('\n')
	total := len(chunk)
	if total > limit && !extended {
		limit, extended = softLimit+extend(string(appendFrameLinePrefix(nil, chunk))), true
	}
	if total > limit {
		return frameLineRead{consumed: total, delimited: err == nil}, errFrameTooLarge
	}
	if err == nil {
		// chunk is a view into bufio's internal buffer; string() copies it
		// out before any subsequent read can overwrite it.
		return frameLineRead{line: string(chunk), consumed: total, delimited: true}, nil
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		// Real I/O error (including io.EOF) before the delimiter was seen.
		if total > 0 && errors.Is(err, io.EOF) {
			return frameLineRead{line: string(chunk), consumed: total}, err
		}
		return frameLineRead{consumed: total}, err
	}

	// Slow path: the line spans multiple buffer fills. Preserve the first
	// chunk (it will be overwritten on the next read) and keep reading.
	first := make([]byte, total)
	copy(first, chunk)
	parts := [][]byte{first}
	prefix := appendFrameLinePrefix(nil, first)

	for {
		chunk, err = reader.ReadSlice('\n')

		// The prefix is grown from the buffer view BEFORE anything else:
		// the extension gate below may need this very chunk to see the
		// claim, and after the limit check there may be no copy at all.
		prefix = appendFrameLinePrefix(prefix, chunk)

		// Check limit BEFORE copying — reject without allocating the
		// oversized chunk into our result buffer.
		total += len(chunk)
		if total > limit && !extended {
			limit, extended = softLimit+extend(string(prefix)), true
		}
		if total > limit {
			return frameLineRead{consumed: total, delimited: err == nil}, errFrameTooLarge
		}

		// ReadSlice returns a view into the internal buffer that will be
		// overwritten on the next read, so we must copy.
		saved := make([]byte, len(chunk))
		copy(saved, chunk)
		parts = append(parts, saved)

		if err == nil {
			// Found the newline delimiter — line is complete.
			return frameLineRead{line: string(bytes.Join(parts, nil)), consumed: total, delimited: true}, nil
		}

		if errors.Is(err, bufio.ErrBufferFull) {
			// Delimiter not found within the buffer — keep reading.
			continue
		}

		// Real I/O error (including io.EOF).
		if total > 0 && errors.Is(err, io.EOF) {
			return frameLineRead{line: string(bytes.Join(parts, nil)), consumed: total}, err
		}
		return frameLineRead{consumed: total}, err
	}
}

// appendFrameLinePrefix grows the retained line head up to frameLinePrefixBytes
// and stops. It copies out of the caller's buffer view, which is what makes the
// prefix safe to hold across the next ReadSlice.
func appendFrameLinePrefix(prefix, chunk []byte) []byte {
	room := frameLinePrefixBytes - len(prefix)
	if room <= 0 {
		return prefix
	}
	if len(chunk) > room {
		chunk = chunk[:room]
	}
	return append(prefix, chunk...)
}

// discardFrameLineRemainder consumes and THROWS AWAY the rest of a line the
// staged reader refused, so the bytes after the refusal cannot be read as
// frames of their own. It reports how many bytes it dropped — including the
// last chunk, which stops on a buffer fill and so may carry the count past
// limitBytes. The count is what was READ, never what was allowed: a caller
// charges the neighbour with it, and a charge that stopped at the limit would
// hand the overshoot away for free on every refusal.
//
// Discarding rather than tearing the session down is what makes a single size
// violation cost the peer one frame instead of a reconnect: the punishment for
// repetition lives in the violation ledger (peerSessionAdmission), not here.
// The discard allocates nothing — ReadSlice hands back a view that is never
// copied — and is bounded by limitBytes, past which the line stops being a
// misbehaving frame and becomes an unterminated stream, which is fatal.
//
// The bound is checked BEFORE the delimiter is honoured, and the order is the
// whole rule rather than a detail: the newline arrives inside a chunk, and the
// chunk carrying it can be the very one that crosses the window. Answering
// "terminated, therefore fine" on that chunk would let a sender place its
// newline just past the boundary and buy the entire overshoot for one byte —
// which is exactly the unbounded read the window exists to refuse.
func discardFrameLineRemainder(reader *bufio.Reader, limitBytes int) (int, error) {
	discarded := 0
	for {
		chunk, err := reader.ReadSlice('\n')
		discarded += len(chunk)
		if discarded > limitBytes {
			return discarded, errFrameTooLarge
		}
		if err == nil {
			return discarded, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return discarded, err
		}
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

// sessionWriteTimeout is the per-write deadline applied by NetCore.writerLoop
// for outbound (dialled) connections — fire-and-forget frames, request-reply
// handshake and steady-state traffic all share this 3-second window.
// writeDeadlineFor(Outbound) returns this value; inbound uses connWriteTimeout.
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
