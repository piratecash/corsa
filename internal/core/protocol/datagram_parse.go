package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_parse.go is the strict datagram parser. It works on the RAW
// wire bytes, never on an already-decoded generic Frame: ParseFrameLine
// collapses duplicate keys, drops unknown fields and does not keep the
// original bytes, so a datagram that arrives as a Frame can no longer
// honour a single promise of §3.4.
//
// The parser is pure: no clock, no trust store, no per-peer state. It
// returns ttl exactly as received — clamping and the single per-hop
// decrement belong to the pipeline (§4.1.1), and `ttl <= auth.max_ttl` is
// checked against the raw value.

// maxDatagramJSONDepth bounds nesting during the raw scan. A legal frame
// reaches depth 2 (frame object → auth object); the slack keeps the bound
// from encoding the schema, while still refusing to grow a scan stack from a
// hostile "[[[[…" line.
//
// It is a VERSION-2 number and is applied only once `v` has matched this build
// (see ParseDatagramFrame): the shape of a version this build never implemented
// is not ours to bound, and pricing a lawful future structure as
// ErrDatagramMalformed would charge ban points to the neighbour that merely
// relayed it (§2, §4.4).
const maxDatagramJSONDepth = 4

// datagramTypeKey and datagramVersionKey are the two top-level keys whose
// meaning is fixed for EVERY header version: the type names the plane, and the
// version says whose schema the remaining keys obey. They are the only thing a
// receiver may read out of a frame before it knows the version.
const (
	datagramTypeKey    = "type"
	datagramVersionKey = "v"
)

// datagramHeaderFields and datagramAuthFields are the closed key sets of
// §3.4 FOR HEADER VERSION 2, and they are consulted only once `v` has
// matched this build. Under a known version an unknown key is rejected
// instead of ignored: extension goes through `v` and `av`, never through a
// field a receiver may skip. Under an unknown version they say nothing at
// all — the key set of a version this build never implemented is not ours to
// close, and a frame naming one is dropped as an unknown version (§2).
var (
	datagramHeaderFields = map[string]struct{}{
		"type": {}, "v": {}, "mode": {}, "class": {}, "src": {}, "dst": {},
		"ttl": {}, "route_policy": {}, "dtype": {}, "payload": {},
		"auth": {},
	}
	datagramAuthFields = map[string]struct{}{
		"av": {}, "pubkey": {}, "salt": {}, "max_ttl": {}, "time": {}, "sig": {},
	}
)

// ParseDatagramFrameLine parses one wire line under the command-plane
// budget. The size is measured on the full line INCLUDING the terminating
// newline — with or without it actually present in the argument — so the
// sender-side budget of MarshalDatagramFrameLineWithLimit and this gate
// agree byte for byte (§2.3).
func ParseDatagramFrameLine(line string) (DatagramFrame, error) {
	budget := len(line)
	if !strings.HasSuffix(line, "\n") {
		budget++
	}
	if budget > MaxFrameLine {
		return DatagramFrame{}, fmt.Errorf("ParseDatagramFrameLine: line size %d exceeds %d: %w", budget, MaxFrameLine, ErrFrameTooLarge)
	}
	return ParseDatagramFrame([]byte(strings.TrimSuffix(line, "\n")))
}

// ParseDatagram decodes and validates a datagram from its raw JSON bytes.
//
// The stable header is read FIRST, off the raw bytes and by a pass that knows
// no version-specific rule (peekDatagramStableHeader). Everything below it
// judges version 2 — the closed key sets, the nesting bound, the type of every
// value — so none of it may run for a frame this build cannot claim to
// understand: judging a future frame by this version's schema prices the
// extension mechanism as a stable-header violation, and the neighbour that
// merely relayed the frame pays the ban points (§2, §4.4).
//
// `type` and `v` are then read a SECOND time, out of the decoded field map.
// That is the divergence guard, not a leftover: if the pre-pass and
// encoding/json ever read the two keys differently, the frame is refused by the
// stricter of the two answers instead of being routed on one and validated by
// the other (§3.4).
func ParseDatagramFrame(raw []byte) (DatagramFrame, error) {
	if _, err := peekDatagramStableHeader(raw); err != nil {
		return DatagramFrame{}, err
	}
	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
		return DatagramFrame{}, err
	}
	fields, err := decodeJSONObject("frame", raw)
	if err != nil {
		return DatagramFrame{}, err
	}
	if err := requireDatagramFrameType(fields[datagramTypeKey]); err != nil {
		return DatagramFrame{}, err
	}
	version, err := parseDatagramVersion(fields[datagramVersionKey])
	if err != nil {
		return DatagramFrame{}, err
	}
	if err := requireKnownFields("frame", fields, datagramHeaderFields); err != nil {
		return DatagramFrame{}, err
	}

	datagram := DatagramFrame{Version: version}
	if err := fillDatagramHeader(&datagram, fields); err != nil {
		return DatagramFrame{}, err
	}
	if err := fillDatagramAuth(&datagram, fields); err != nil {
		return DatagramFrame{}, err
	}
	if err := datagram.Validate(); err != nil {
		return DatagramFrame{}, err
	}
	return datagram, nil
}

// requireDatagramFrameType judges the `type` of the stable header. A nil value
// is an ABSENT key, which is how both of its readers spell absence: the raw
// pre-pass leaves the slot untouched, and a map lookup of a missing key yields
// the same nil.
func requireDatagramFrameType(raw json.RawMessage) error {
	if raw == nil {
		return fmt.Errorf("%w: missing type", ErrDatagramMalformed)
	}
	frameType, err := wireString(datagramTypeKey, raw)
	if err != nil {
		return err
	}
	if frameType != DatagramFrameType {
		return fmt.Errorf("%w: type %q, want %q", ErrDatagramMalformed, frameType, DatagramFrameType)
	}
	return nil
}

// parseDatagramVersion separates two rejects that look alike but are
// acted on differently: a syntactically broken `v` is a malformed frame,
// while a well-formed version this build does not implement is dropped
// without forwarding and must stay distinguishable (§2).
func parseDatagramVersion(raw json.RawMessage) (domain.DatagramVersion, error) {
	if raw == nil {
		return 0, fmt.Errorf("%w: missing v", ErrDatagramMalformed)
	}
	number, err := wireInt64(datagramVersionKey, raw)
	if err != nil {
		return 0, err
	}
	version, err := domain.ParseDatagramVersion(number)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDatagramMalformed, err)
	}
	if version != domain.DatagramHeaderVersion {
		return 0, fmt.Errorf("%w: %d", ErrDatagramUnknownVersion, version)
	}
	return version, nil
}

func fillDatagramHeader(datagram *DatagramFrame, fields map[string]json.RawMessage) error {
	mode, err := requiredWireString(fields, "mode")
	if err != nil {
		return err
	}
	if datagram.Mode, err = domain.ParseDatagramMode(mode); err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramModeMatrix, err)
	}
	class, err := requiredWireString(fields, "class")
	if err != nil {
		return err
	}
	if datagram.Class, err = domain.ParseDatagramClass(class); err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramModeMatrix, err)
	}
	src, err := requiredWireString(fields, "src")
	if err != nil {
		return err
	}
	if datagram.Src, err = decodeDatagramHex("src", src); err != nil {
		return err
	}
	dst, err := requiredWireString(fields, "dst")
	if err != nil {
		return err
	}
	if datagram.Dst, err = decodeDatagramHex("dst", dst); err != nil {
		return err
	}
	ttlRaw, ok := fields["ttl"]
	if !ok {
		return fmt.Errorf("%w: missing ttl", ErrDatagramMalformed)
	}
	if datagram.TTL, err = wireByte("ttl", ttlRaw); err != nil {
		return err
	}
	if policyRaw, ok := fields["route_policy"]; ok {
		policy, err := wireString("route_policy", policyRaw)
		if err != nil {
			return err
		}
		if datagram.RoutePolicy, err = domain.ParseRoutePolicy(policy); err != nil {
			return fmt.Errorf("%w: %w", ErrDatagramModeMatrix, err)
		}
	}
	dtype, err := requiredWireString(fields, "dtype")
	if err != nil {
		return err
	}
	if datagram.DType, err = domain.ParseDType(dtype); err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramMalformed, err)
	}
	payload, err := requiredWireString(fields, "payload")
	if err != nil {
		return err
	}
	datagram.Payload, err = decodeDatagramBase64("payload", payload)
	return err
}

func fillDatagramAuth(datagram *DatagramFrame, fields map[string]json.RawMessage) error {
	raw, ok := fields["auth"]
	if !ok {
		return nil
	}
	authFields, err := decodeJSONObject("auth", raw)
	if err != nil {
		return err
	}
	if err := requireKnownFields("auth", authFields, datagramAuthFields); err != nil {
		return err
	}
	auth := DatagramAuth{}
	avRaw, err := requiredAuthField(authFields, "av")
	if err != nil {
		return err
	}
	avNumber, err := wireInt64("auth.av", avRaw)
	if err != nil {
		return err
	}
	if auth.AuthVersion, err = domain.ParseAuthVersion(avNumber); err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramAuth, err)
	}
	if auth.PubKey, err = requiredAuthBinary(authFields, "pubkey"); err != nil {
		return err
	}
	if auth.Salt, err = requiredAuthBinary(authFields, "salt"); err != nil {
		return err
	}
	maxTTLRaw, err := requiredAuthField(authFields, "max_ttl")
	if err != nil {
		return err
	}
	if auth.MaxTTL, err = wireByte("auth.max_ttl", maxTTLRaw); err != nil {
		return err
	}
	timeRaw, err := requiredAuthField(authFields, "time")
	if err != nil {
		return err
	}
	if auth.Time, err = wireInt64("auth.time", timeRaw); err != nil {
		return err
	}
	if auth.Sig, err = requiredAuthBinary(authFields, "sig"); err != nil {
		return err
	}
	datagram.Auth = &auth
	return nil
}

// requiredAuthField reads a mandatory auth key. Every field of the block is
// mandatory, and every absence reports the SAME sentinel: the pipeline
// decides metrics and ban on the class of reject, and "auth without a
// pubkey" and "auth without a time" are one class (§3.1).
func requiredAuthField(fields map[string]json.RawMessage, key string) (json.RawMessage, error) {
	raw, ok := fields[key]
	if !ok {
		return nil, fmt.Errorf("%w: missing auth.%s", ErrDatagramAuth, key)
	}
	return raw, nil
}

// requiredAuthBinary reads a mandatory base64url auth field into its raw
// bytes. Length is checked later by Validate, against the one place that
// knows the expected size of each field.
func requiredAuthBinary(fields map[string]json.RawMessage, key string) ([]byte, error) {
	raw, err := requiredAuthField(fields, key)
	if err != nil {
		return nil, err
	}
	label := "auth." + key
	encoded, err := wireString(label, raw)
	if err != nil {
		return nil, err
	}
	return decodeDatagramBase64(label, encoded)
}

// ---------------------------------------------------------------------------
// The stable header: the one pass that runs before the version is known
// ---------------------------------------------------------------------------

// stableDatagramKeys holds the RAW values of the two version-independent keys,
// nil for a key the document does not carry.
type stableDatagramKeys struct {
	frameType json.RawMessage
	version   json.RawMessage
}

// slotFor returns where a top-level key's value belongs, or nil for a key this
// pass is not allowed to have an opinion about. Every other key belongs to some
// version's schema, and which version that is has not been decided yet.
func (k *stableDatagramKeys) slotFor(key []byte) *json.RawMessage {
	switch string(key) {
	case datagramTypeKey:
		return &k.frameType
	case datagramVersionKey:
		return &k.version
	default:
		return nil
	}
}

// peekDatagramStableHeader reads `type` and `v` out of an UNTRUSTED document
// and judges nothing else.
//
// It exists because the two verdicts a receiver can reach about a frame it will
// not process cost the sender different things: a stable-header violation is
// ban-worthy, a header version this build does not implement is a silent drop
// (§2, §4.4). Deciding which one applies requires the version, so every rule
// that a version owns — the closed key set, the nesting bound, the type of each
// value — has to run BELOW this function and never above it.
func peekDatagramStableHeader(raw []byte) (domain.DatagramVersion, error) {
	keys, err := scanStableDatagramKeys(raw)
	if err != nil {
		return 0, err
	}
	if err := requireDatagramFrameType(keys.frameType); err != nil {
		return 0, err
	}
	return parseDatagramVersion(keys.version)
}

// scanStableDatagramKeys walks the raw bytes ONCE and returns the two stable
// keys of the top-level object.
//
// # What bounds it
//
// Not a schema — it may not have one yet — but its own shape:
//
//   - one forward pass, so the cost is O(len(raw)), on a line MaxFrameLine has
//     already capped at 128 KiB (ParseDatagramFrameLine);
//   - an integer depth counter instead of a stack, so a "[[[[…" document makes
//     this pass COUNT to a million rather than allocate a million scopes. That
//     is why it needs no depth limit of its own, and why it must not borrow the
//     version-2 one;
//   - it stops at the byte that closes the top-level value, so trailing content
//     is left for decodeJSONObject to name;
//   - the returned values ALIAS raw. Nothing here allocates, and nothing here
//     builds a JSON value.
//
// # What it deliberately does not do
//
// It validates no structure. A document that is broken in a way this pass walks
// straight past reaches the strict parser below with its verdict intact; a
// document broken in a way that hides `v` is refused as malformed, which is the
// same answer it would have got from the parser. The pass can only ever move a
// frame from "judged by this version's schema" to "dropped as an unknown
// version", and never the other way.
func scanStableDatagramKeys(raw []byte) (stableDatagramKeys, error) {
	keys := stableDatagramKeys{}
	depth := 0
	for i := 0; i < len(raw); {
		switch raw[i] {
		case '{', '[':
			depth++
			i++
		case '}', ']':
			depth--
			if depth < 0 {
				return stableDatagramKeys{}, fmt.Errorf("%w: unbalanced JSON container", ErrDatagramMalformed)
			}
			if depth == 0 {
				// The top-level value is closed; whatever follows is trailing
				// content and belongs to the strict parser's verdict, not here.
				return keys, nil
			}
			i++
		case '"':
			key, next, err := scanJSONStringToken(raw, i)
			if err != nil {
				return stableDatagramKeys{}, err
			}
			colon := skipDatagramJSONSpace(raw, next)
			if depth != 1 || colon >= len(raw) || raw[colon] != ':' {
				// A value, or a key of a nested object: not the stable header.
				i = next
				continue
			}
			if err := keys.record(raw, key, colon+1); err != nil {
				return stableDatagramKeys{}, err
			}
			// The value itself is NOT skipped: leaving it to the loop is what
			// keeps this a single pass with no value parser in it.
			i = colon + 1
		default:
			i++
		}
	}
	return keys, nil
}

// record stores one top-level key's value, refusing a second spelling of the
// same key. A duplicate stable key is a stable-header violation in every
// version — two readers would route two different frames — so it is refused
// here rather than left to the version-2 duplicate scan, which never runs for a
// future version.
func (k *stableDatagramKeys) record(raw []byte, key []byte, valueAt int) error {
	slot := k.slotFor(key)
	if slot == nil {
		return nil
	}
	if *slot != nil {
		return fmt.Errorf("%w: %q", ErrDatagramDuplicateKey, key)
	}
	value, err := stableDatagramValueToken(raw, valueAt)
	if err != nil {
		return err
	}
	*slot = value
	return nil
}

// stableDatagramValueToken returns the raw token a stable key was given: a
// complete JSON string, or the run of bytes up to the next structural
// delimiter.
//
// It never walks a container, and it does not have to: `type` is a string and
// `v` an integer in every version, so a value that opens an object or an array
// yields a token wireString and wireInt64 both refuse — which is the verdict
// such a frame has earned whatever version it claims.
func stableDatagramValueToken(raw []byte, pos int) (json.RawMessage, error) {
	pos = skipDatagramJSONSpace(raw, pos)
	if pos >= len(raw) {
		return nil, fmt.Errorf("%w: a stable header key with no value", ErrDatagramMalformed)
	}
	if raw[pos] == '"' {
		_, next, err := scanJSONStringToken(raw, pos)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(raw[pos:next]), nil
	}
	end := pos
	for end < len(raw) && !isJSONStructural(raw[end]) {
		end++
	}
	return json.RawMessage(bytes.TrimRight(raw[pos:end], " \t\r\n")), nil
}

// isJSONStructural reports whether a byte ends a scalar token.
func isJSONStructural(b byte) bool {
	switch b {
	case ',', '}', ']':
		return true
	default:
		return false
	}
}

// skipDatagramJSONSpace advances past the whitespace JSON allows between
// tokens.
func skipDatagramJSONSpace(raw []byte, pos int) int {
	for pos < len(raw) {
		switch raw[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
		default:
			return pos
		}
	}
	return pos
}

// ---------------------------------------------------------------------------
// Raw JSON helpers
// ---------------------------------------------------------------------------

// decodeJSONObject decodes one JSON object into its raw key/value pairs and
// refuses trailing content. Decoding into a map instead of a tagged struct
// is deliberate: it puts the closed key set of §3.4 under our own control
// (a typed reject, not a text-matched encoding/json message) and keeps
// every value raw, so each field is validated by the rule that owns it.
func decodeJSONObject(scope string, raw []byte) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return nil, fmt.Errorf("%w: %s is not a JSON object", ErrDatagramMalformed, scope)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrDatagramMalformed, scope, err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: %s has trailing content", ErrDatagramMalformed, scope)
	}
	return fields, nil
}

func requireKnownFields(scope string, fields map[string]json.RawMessage, allowed map[string]struct{}) error {
	for name := range fields {
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("%w: %s.%s", ErrDatagramUnknownField, scope, name)
		}
	}
	return nil
}

// requiredWireString reads a mandatory JSON string field of the header.
func requiredWireString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("%w: missing %s", ErrDatagramMalformed, name)
	}
	return wireString(name, raw)
}

// wireString requires a JSON string. A JSON null decodes into a Go string
// without error, so the token kind is checked explicitly instead.
func wireString(label string, raw json.RawMessage) (string, error) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", fmt.Errorf("%w: %s must be a JSON string", ErrDatagramMalformed, label)
	}
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("%w: %s: %v", ErrDatagramMalformed, label, err)
	}
	return out, nil
}

// wireByte reads a JSON integer in 0…255 (ttl and max_ttl are hop
// counters, where zero is a legal "no hops left" value).
func wireByte(label string, raw json.RawMessage) (uint8, error) {
	number, err := wireInt64(label, raw)
	if err != nil {
		return 0, err
	}
	if number < 0 || number > 255 {
		return 0, fmt.Errorf("%w: %s %d outside 0..255", ErrDatagramMalformed, label, number)
	}
	return uint8(number), nil
}

// wireInt64 requires a JSON INTEGER literal. Fractions, exponents, plus
// signs and quoted numbers are rejected: "1.0", "1e0" and "1" would each
// have to become one transcript byte, and letting two parsers pick
// differently is exactly the ambiguity §3.4 closes.
func wireInt64(label string, raw json.RawMessage) (int64, error) {
	token := string(raw)
	if err := requireIntegerLiteral(label, token); err != nil {
		return 0, err
	}
	var number int64
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("%w: %s: %v", ErrDatagramMalformed, label, err)
	}
	return number, nil
}

func requireIntegerLiteral(label, token string) error {
	digits := strings.TrimPrefix(token, "-")
	if digits == "" {
		return fmt.Errorf("%w: %s must be a JSON integer, got %q", ErrDatagramMalformed, label, token)
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return fmt.Errorf("%w: %s must be a JSON integer, got %q", ErrDatagramMalformed, label, token)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Duplicate-key scan
// ---------------------------------------------------------------------------

// jsonScanInlineKeys is how many keys one object keeps in a linear slice
// before the scan switches that object to a map. Header objects have a
// handful of keys, so the slice wins on both time and allocations; the map is
// there so a wide object in the payload stays linear instead of quadratic.
const jsonScanInlineKeys = 16

// jsonScanScope is one open container during the raw scan. expectKey marks
// the position where the next string token is an object key rather than a
// value, which is the only way to tell them apart without parsing.
//
// The key storage is REUSED as the scan pushes and pops: the scan runs before
// authentication (§2.2), so a frame full of tiny objects must not translate
// into an allocation per object for anyone who can open a socket.
type jsonScanScope struct {
	// keys are the raw key bytes seen in this object. For an unescaped,
	// valid-UTF-8 key the entry aliases `raw` and costs nothing.
	keys [][]byte
	// large takes over past jsonScanInlineKeys.
	large     map[string]struct{}
	isObject  bool
	expectKey bool
}

// reset re-arms a scope for a newly opened container, keeping its storage.
func (s *jsonScanScope) reset(isObject bool) {
	s.keys = s.keys[:0]
	clear(s.large)
	s.isObject = isObject
	s.expectKey = isObject
}

// scanDuplicateJSONKeys walks the raw bytes once and rejects a key that
// repeats inside any object — the frame itself, auth, ext, or any nested
// object. encoding/json silently keeps the LAST occurrence even with
// DisallowUnknownFields, so without this scan one implementation could
// verify the signature over one spelling and route the other (§3.4).
//
// Keys are compared after the SAME normalisation encoding/json applies, so
// `"ab"` and `"ab"` collide as they must, and so do a raw invalid UTF-8 byte
// and the U+FFFD it decodes to — otherwise the scan would see two keys where
// the decoder sees one, which is precisely the two-parser divergence this
// scan exists to close.
func scanDuplicateJSONKeys(raw []byte, maxDepth int) error {
	stack := make([]jsonScanScope, 0, 8)
	depth := 0
	for i := 0; i < len(raw); {
		switch raw[i] {
		case '{', '[':
			if depth >= maxDepth {
				return fmt.Errorf("%w: JSON nesting deeper than %d", ErrDatagramMalformed, maxDepth)
			}
			if depth == len(stack) {
				stack = append(stack, jsonScanScope{})
			}
			stack[depth].reset(raw[i] == '{')
			depth++
			i++
		case '}', ']':
			if depth == 0 {
				return fmt.Errorf("%w: unbalanced JSON container", ErrDatagramMalformed)
			}
			depth--
			i++
		case '"':
			key, next, err := scanJSONStringToken(raw, i)
			if err != nil {
				return err
			}
			if depth > 0 {
				scope := &stack[depth-1]
				if scope.isObject && scope.expectKey {
					if err := scope.recordKey(key); err != nil {
						return err
					}
				}
			}
			i = next
		case ',':
			if depth > 0 {
				scope := &stack[depth-1]
				scope.expectKey = scope.isObject
			}
			i++
		default:
			i++
		}
	}
	return nil
}

func (s *jsonScanScope) recordKey(key []byte) error {
	if s.large != nil {
		if _, duplicate := s.large[string(key)]; duplicate {
			return fmt.Errorf("%w: %q", ErrDatagramDuplicateKey, key)
		}
		s.large[string(key)] = struct{}{}
		s.expectKey = false
		return nil
	}
	for _, known := range s.keys {
		if bytes.Equal(known, key) {
			return fmt.Errorf("%w: %q", ErrDatagramDuplicateKey, key)
		}
	}
	if len(s.keys) == jsonScanInlineKeys {
		s.large = make(map[string]struct{}, 2*jsonScanInlineKeys)
		for _, known := range s.keys {
			s.large[string(known)] = struct{}{}
		}
		s.keys = s.keys[:0]
		s.large[string(key)] = struct{}{}
		s.expectKey = false
		return nil
	}
	s.keys = append(s.keys, key)
	s.expectKey = false
	return nil
}

// scanJSONStringToken reads the string token starting at the opening quote
// raw[start] and returns its NORMALISED value plus the index just past the
// closing quote.
//
// The fast path aliases `raw` and allocates nothing, and it is taken only
// when the bytes need no normalisation at all — no escapes and valid UTF-8.
// Everything else goes through encoding/json itself rather than through a
// second implementation of its rules: invalid UTF-8 and lone surrogates both
// decode to U+FFFD there, and a hand-written approximation of that is exactly
// how the two parsers would drift apart again (§3.4).
func scanJSONStringToken(raw []byte, start int) ([]byte, int, error) {
	escaped := false
	for i := start + 1; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			escaped = true
			i++ // the escaped byte can be a quote and must not close the token
		case '"':
			token := raw[start : i+1]
			body := token[1 : len(token)-1]
			if !escaped && utf8.Valid(body) {
				return body, i + 1, nil
			}
			var value string
			if err := json.Unmarshal(token, &value); err != nil {
				return nil, 0, fmt.Errorf("%w: bad JSON string: %v", ErrDatagramMalformed, err)
			}
			return []byte(value), i + 1, nil
		}
	}
	return nil, 0, fmt.Errorf("%w: unterminated JSON string", ErrDatagramMalformed)
}
